use super::{Backend, cache_tests, connection};
use crate::authorization::{Action, Permit, Principal};
use crate::config::Config;
use crate::model::bucket::CreateBucket;
use crate::model::multipart::{PartStitch, SeedConsumption};
use crate::model::{
    AccountStore, BucketStore, ChunkStore, MultipartStore, PhysicalStore, PublishStore, QuotaStore,
    StreamCreate, StreamStore, TaskControlStore,
};
use anyhow::Result;
use serde_json::json;
use uuid::Uuid;

async fn publish(backend: &Backend, stream: Uuid, size: i64) -> Result<()> {
    backend.reserve(stream, size, true, None).await?;
    backend.finish(stream, size, "fixture", json!({})).await?;
    assert!(backend.publish(stream, None, None, None).await?);
    Ok(())
}

async fn queue(backend: &Backend, stream: Uuid) -> Result<Option<(i64, i64, String)>> {
    Ok(sqlx::query_as(
        "SELECT generation,progress_cursor,reason FROM pack_maintenance WHERE stream_id=?",
    )
    .bind(stream)
    .fetch_optional(&backend.pool)
    .await?)
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_pack_queue_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    backend.initialize_admin("admin", "password", false).await?;
    let storage = super::storage_tests::fixture(&backend).await?;
    let bucket = BucketStore::create(
        &backend,
        &Principal::Local,
        CreateBucket {
            name: "queues".into(),
            project_id: None,
            storage_id: Some(storage),
        },
    )
    .await?
    .id;
    let source = cache_tests::stream(&backend, bucket, "source").await?;
    let c0 = cache_tests::chunk(&backend, storage, source, 1).await?;
    let c1 = cache_tests::chunk(&backend, storage, source, 2).await?;
    backend.register_encoding(&c0).await?;
    backend.register_encoding(&c1).await?;
    ChunkStore::complete(&backend, source, 0, &c0).await?;
    ChunkStore::complete(&backend, source, 32, &c1).await?;
    assert!(queue(&backend, source).await?.is_none());
    publish(&backend, source, 64).await?;
    assert_eq!(queue(&backend, source).await?, Some((1, 0, "pack".into())));

    // A concurrent copy invalidates an old queue generation, even before its publication.
    sqlx::query("UPDATE pack_maintenance SET progress_cursor=32,next_check_at=TIMESTAMPADD(DAY,1,CURRENT_TIMESTAMP(6)) WHERE stream_id=?").bind(source).execute(db).await?;
    let copy = cache_tests::stream(&backend, bucket, "copy").await?;
    backend
        .copy_references(copy, source, 0, 64, &[c0.id, c1.id])
        .await?;
    assert_eq!(queue(&backend, source).await?, Some((2, 0, "pack".into())));
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT next_check_at<=CURRENT_TIMESTAMP(6) FROM pack_maintenance WHERE stream_id=?"
        )
        .bind(source)
        .fetch_one(db)
        .await?
    );
    assert!(queue(&backend, copy).await?.is_none());
    publish(&backend, copy, 64).await?;
    assert_eq!(queue(&backend, copy).await?, Some((1, 0, "pack".into())));
    assert!(backend.reference(copy, 64, c0.id, 32).await.is_err());
    assert_eq!(queue(&backend, source).await?.unwrap().0, 2);

    let owner = TaskControlStore::enqueue(&backend, "pack", None, json!({"policy":"pack"})).await?;
    let pack = cache_tests::pack(&backend, storage, owner).await?;
    sqlx::query("UPDATE packs SET state='ready',stored_size=64 WHERE id=?")
        .bind(pack)
        .execute(db)
        .await?;
    sqlx::query("UPDATE chunks SET pack_id=? WHERE id IN (?,?)")
        .bind(pack)
        .bind(c0.id)
        .bind(c1.id)
        .execute(db)
        .await?;
    let target = cache_tests::stream(&backend, bucket, "target").await?;
    backend.reference(target, 0, c0.id, 32).await?;
    assert_eq!(
        sqlx::query_as::<_, (i64, String)>(
            "SELECT generation,reason FROM pack_changes WHERE pack_id=?"
        )
        .bind(pack)
        .fetch_one(db)
        .await?,
        (1, "reuse".into())
    );
    let generation = queue(&backend, source).await?.unwrap().0;
    assert!(backend.reference(target, 0, c1.id, 32).await.is_err());
    assert_eq!(queue(&backend, source).await?.unwrap().0, generation);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT generation FROM pack_changes WHERE pack_id=?")
            .bind(pack)
            .fetch_one(db)
            .await?,
        1
    );
    publish(&backend, target, 32).await?;
    assert!(queue(&backend, target).await?.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT generation FROM pack_changes WHERE pack_id=?")
            .bind(pack)
            .fetch_one(db)
            .await?,
        2
    );

    // Multipart replacement changes both the removed and surviving references.
    let authority = Permit::for_action(Principal::Local, bucket, Action::Write);
    let left = StreamStore::create(
        &backend,
        StreamCreate {
            authority: authority.clone(),
            bucket,
            key: "parts".into(),
            stream_type: "part".into(),
            metadata: json!({}),
            public: false,
            claim_object: false,
            completion: None,
            owner: None,
        },
    )
    .await?;
    let right = StreamStore::create(
        &backend,
        StreamCreate {
            authority: authority.clone(),
            bucket,
            key: "parts".into(),
            stream_type: "part".into(),
            metadata: json!({}),
            public: false,
            claim_object: false,
            completion: None,
            owner: None,
        },
    )
    .await?;
    backend.reference(left, 0, c0.id, 32).await?;
    backend.reference(right, 0, c1.id, 32).await?;
    let seed = cache_tests::stream(&backend, bucket, "seed").await?;
    let replacement = cache_tests::chunk(&backend, storage, seed, 3).await?;
    backend.register_encoding(&replacement).await?;
    ChunkStore::complete(&backend, seed, 0, &replacement).await?;
    let before = queue(&backend, source).await?.unwrap().0;
    sqlx::query("UPDATE chunks SET reference_changed_at='2000-01-01' WHERE id IN (?,?)")
        .bind(c0.id)
        .bind(c1.id)
        .execute(db)
        .await?;
    backend
        .stitch(PartStitch {
            left,
            right,
            offset: 16,
            left_length: 16,
            right_length: 16,
            chunk: replacement.id,
        })
        .await?;
    assert!(queue(&backend, source).await?.unwrap().0 > before);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM chunks WHERE id IN (?,?) AND reference_changed_at>'2000-01-01'"
        )
        .bind(c0.id)
        .bind(c1.id)
        .fetch_one(db)
        .await?,
        2
    );
    assert!(queue(&backend, left).await?.is_none());
    assert!(queue(&backend, right).await?.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT reason FROM pack_changes WHERE pack_id=?")
            .bind(pack)
            .fetch_one(db)
            .await?,
        "reuse"
    );

    let upload = MultipartStore::create(
        &backend,
        crate::model::multipart::MultipartCreate {
            authority: &authority,
            bucket,
            key: "seed",
            access: "fixture",
            metadata: json!({}),
            public: false,
            algorithm: None,
            checksum_type: None,
            web: None,
            max_active: None,
        },
    )
    .await?
    .0;
    let mut spans = vec![(left, 16, 16)];
    backend
        .consume_seed(SeedConsumption {
            upload,
            stream: seed,
            offset: 0,
            chunk: replacement.id,
            spans: &mut spans,
            prefix: 16,
            consumed: 32,
        })
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT length FROM extents WHERE stream_id=?")
            .bind(seed)
            .fetch_one(db)
            .await?,
        16
    );
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT unreferenced_at IS NULL FROM chunks WHERE id=?")
            .bind(replacement.id)
            .fetch_one(db)
            .await?
    );

    // A cached publication enters the same queue and rolls back hooks with a failed extent.
    let cached = cache_tests::stream(&backend, bucket, "cache").await?;
    let cached0 = cache_tests::chunk(&backend, storage, cached, 4).await?;
    let cached1 = cache_tests::chunk(&backend, storage, cached, 5).await?;
    cache_tests::stage(&backend, &cached0, cached, 0, true).await?;
    cache_tests::stage(&backend, &cached1, cached, 32, true).await?;
    publish(&backend, cached, 64).await?;
    assert_eq!(queue(&backend, cached).await?, Some((1, 0, "pack".into())));
    backend.pool.close().await;
    Ok(())
}
