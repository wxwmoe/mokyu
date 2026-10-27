use super::{Backend, connection};
use crate::authorization::{Action, Permit, Principal};
use crate::config::Config;
use crate::content::codec::Chunk;
use crate::model::Id;
use crate::model::bucket::CreateBucket;
use crate::model::cache::{CacheBatch, CacheStage};
use crate::model::task::Execution;
use crate::model::{
    AccountStore, BucketStore, ChunkCreate, ChunkStore, PhysicalStore, PublishStore, QuotaStore,
    StreamCreate, StreamStore, TaskControlStore, UploadCacheStore,
};
use anyhow::Result;
use serde_json::json;
use uuid::Uuid;

pub(super) async fn stream(backend: &Backend, bucket: Uuid, key: &str) -> Result<Id> {
    StreamStore::create(
        backend,
        StreamCreate {
            authority: Permit::for_action(Principal::Local, bucket, Action::Write),
            bucket,
            key: key.into(),
            stream_type: "object".into(),
            metadata: json!({"content_type":"text/plain"}),
            public: false,
            claim_object: true,
            completion: None,
            owner: None,
        },
    )
    .await
}
pub(super) async fn chunk(backend: &Backend, storage: Uuid, stream: Id, hash: u8) -> Result<Chunk> {
    let (mut chunk, reused) = backend
        .allocate(ChunkCreate {
            storage,
            stream,
            hash: [hash; 32],
            raw_size: 32,
            algorithm: "none".into(),
            key: None,
        })
        .await?;
    assert!(!reused);
    chunk.location_id = Uuid::new_v4();
    chunk.stored_size = Some(32);
    backend.start_upload(stream, &chunk).await?;
    Ok(chunk)
}
pub(super) async fn stage(
    backend: &Backend,
    chunk: &Chunk,
    stream: Id,
    offset: i64,
    new: bool,
) -> Result<String> {
    let key = format!(
        "{}.raw",
        if chunk.location_id.is_nil() {
            Uuid::new_v4()
        } else {
            chunk.location_id
        }
    );
    assert!(
        backend
            .stage(CacheStage {
                buffered: false,
                boot_id: None,
                encoding: None,
                node: None,
                chunk,
                stream,
                offset,
                new,
                size: 32,
                storage: chunk.storage_id.unwrap(),
                key: &key,
                timeout: 3600
            })
            .await?
    );
    Ok(key)
}
async fn execute(backend: &Backend, id: Uuid) -> Result<Execution> {
    let task = Execution {
        id,
        token: Uuid::new_v4(),
    };
    sqlx::query("UPDATE tasks SET state='running',execution_token=? WHERE id=?")
        .bind(task.token)
        .bind(id)
        .execute(&backend.pool)
        .await?;
    Ok(task)
}
async fn durable(backend: &Backend, chunk: &Chunk) -> Result<()> {
    backend.register_encoding(chunk).await?;
    sqlx::query("UPDATE chunk_encodings SET state='ready' WHERE encoding_id=?")
        .bind(chunk.encoding_id)
        .execute(&backend.pool)
        .await?;
    Ok(())
}
pub(super) async fn pack(backend: &Backend, storage: Uuid, owner: Uuid) -> Result<i64> {
    let id=sqlx::query("INSERT INTO packs(location_id,storage_id,encryption_algorithm,raw_size,member_count,state,owner_task,pack_fingerprint) VALUES(?,?,'none',64,2,'preparing',?,?)")
        .bind(Uuid::new_v4()).bind(storage).bind(owner).bind([0u8;32].as_slice()).execute(&backend.pool).await?.last_insert_id();
    Ok(i64::try_from(id)?)
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_upload_cache_contract() -> Result<()> {
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
            name: "cache".into(),
            project_id: None,
            storage_id: Some(storage),
        },
    )
    .await?
    .id;
    sqlx::query("UPDATE storage_profiles SET pack_max_size='96B',pack_upload_flush_timeout='60s' WHERE storage_id=?").bind(storage).execute(db).await?;
    let first = stream(&backend, bucket, "cached-\u{732b}%").await?;
    assert!(!backend.bypass(first).await?);
    assert_eq!(backend.status().await?.0["entries"], 0);
    assert!(backend.next_attempt().await?.is_none());
    assert!(!UploadCacheStore::enqueue(&backend, &[]).await?);
    backend.reserve(first, 96, false, None).await?;
    let mut chunks = Vec::new();
    let mut names = Vec::new();
    for n in 0..3 {
        let value = chunk(&backend, storage, first, n).await?;
        names.push(stage(&backend, &value, first, i64::from(n) * 32, true).await?);
        chunks.push(value);
        if n == 1 {
            assert!(!backend.seal(first, false).await?);
            assert!(!UploadCacheStore::enqueue(&backend, &[]).await?);
        }
    }
    let ids: Vec<_> = chunks.iter().map(|c| c.id).collect();
    sqlx::query("UPDATE pending_uploads SET encoding_policy=? WHERE chunk_id=?")
        .bind(json!({"compression":{"enabled":false}}))
        .bind(ids[1])
        .execute(db)
        .await?;
    let segment = super::queries::pending_upload_segment(ids[0], Some(first), None)
        .fetch_all(db)
        .await?;
    assert_eq!(
        segment.iter().map(|r| r.0).collect::<Vec<_>>(),
        vec![ids[0], ids[2]]
    );
    let selected =
        super::queries::pending_upload_mappings(ids[0], Some(first), 0, Uuid::new_v4(), false)
            .build_query_as::<crate::model::Mapping>()
            .fetch_all(db)
            .await?;
    assert_eq!(
        selected.iter().map(|r| r.chunk.id).collect::<Vec<_>>(),
        vec![ids[0], ids[2]]
    );
    sqlx::query("UPDATE pending_uploads SET encoding_policy=NULL WHERE chunk_id=?")
        .bind(ids[1])
        .execute(db)
        .await?;
    assert!(backend.next_attempt().await?.unwrap() > chrono::Utc::now());
    let (summary, pins) = backend.status().await?;
    assert_eq!(summary["entries"], 3);
    assert_eq!(summary["bytes"], 96);
    assert_eq!(pins.len(), 3);
    assert!(
        pins.iter()
            .all(|pin| pin["created_at"].as_str().unwrap().ends_with('Z'))
    );
    assert_eq!(backend.pins(&names, Some(storage)).await?, names);
    assert!(backend.pins(&names, Some(Uuid::new_v4())).await?.is_empty());
    assert!(backend.pins(&[], None).await?.is_empty());
    let restored = backend.upload_pins(0.into(), None).await?;
    assert_eq!(restored.len(), 3);
    for (index, (id, target, name, size, charged_storage)) in restored.iter().enumerate() {
        assert_eq!(
            (*id, *target, name.as_ref(), *size, *charged_storage),
            (ids[index], Some(storage), Some(&names[index]), 32, storage)
        );
        assert_eq!(
            backend.pending_source(*id).await?,
            Some((storage, names[index].clone()))
        );
        assert!(backend.pending_only(*id).await?);
    }
    assert_eq!(
        backend.metadata(first).await?,
        Some((
            json!({"content_type":"text/plain"}),
            "cached-\u{732b}%".into()
        ))
    );
    assert!(backend.metadata(Id::from(i64::MAX)).await?.is_none());
    StreamStore::finish(&backend, first, 96, "etag", json!({})).await?;
    assert!(backend.publish(first, None, None, None).await?);
    assert!(backend.release(&ids, false).await?.keys.is_empty());
    assert!(backend.seal(first, false).await?);
    sqlx::query("UPDATE storage_profiles SET pack_max_size='64B' WHERE storage_id=?")
        .bind(storage)
        .execute(db)
        .await?;
    assert!(UploadCacheStore::enqueue(&backend, &[]).await?);
    let owner: Uuid = sqlx::query_scalar("SELECT owner_task FROM pending_uploads WHERE chunk_id=?")
        .bind(ids[0])
        .fetch_one(db)
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pending_uploads WHERE owner_task=?")
            .bind(owner)
            .fetch_one(db)
            .await?,
        2
    );
    let task = execute(&backend, owner).await?;
    let detail = backend.raw(owner).await?.unwrap()["detail"].clone();
    let stale = Execution {
        id: owner,
        token: Uuid::new_v4(),
    };
    assert!(backend.batch(stale, &detail, false).await.is_err());
    let batch = backend.batch(task, &detail, false).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pack_inputs WHERE task_id=?")
            .bind(owner)
            .fetch_one(db)
            .await?,
        2
    );
    assert_eq!(
        backend.raw(owner).await?.unwrap()["detail"]["source_inputs"]
            .as_object()
            .unwrap()
            .len(),
        2
    );
    assert!(
        matches!(batch,CacheBatch::Encode{stream:Some(id),expired:false,rows,..} if id==first && rows.len()==2)
    );
    let copy = stream(&backend, bucket, "copy").await?;
    backend.reference(copy, 0, ids[0], 32).await?;
    assert_eq!(backend.status().await?.1.len(), 4);
    assert!(
        matches!(backend.batch(task,&detail,false).await?,CacheBatch::Encode{rows,..} if rows.len()==1)
    );
    sqlx::query("DELETE FROM extents WHERE stream_id=?")
        .bind(copy)
        .execute(db)
        .await?;
    sqlx::query(
        "UPDATE pending_uploads SET created_at=TIMESTAMPADD(SECOND,-120,CURRENT_TIMESTAMP(6))",
    )
    .execute(db)
    .await?;
    assert!(matches!(
        backend.batch(task, &detail, false).await?,
        CacheBatch::Encode { expired: true, .. }
    ));
    backend.processed(stale).await?;
    assert_eq!(backend.raw(owner).await?.unwrap()["processed"], 0);
    backend.processed(task).await?;
    assert_eq!(backend.raw(owner).await?.unwrap()["processed"], 1);
    durable(&backend, &chunks[0]).await?;
    assert!(
        matches!(backend.batch(task,&detail,false).await?,CacheBatch::Release(id) if id==ids[0])
    );
    let previous_generation: i64 =
        sqlx::query_scalar("SELECT generation FROM pack_maintenance WHERE stream_id=?")
            .bind(first)
            .fetch_one(db)
            .await?;
    let release = backend.release(&ids, false).await?;
    assert_eq!(
        release.keys,
        vec![(names[0].clone(), Some(chunks[0].location_id))]
    );
    assert!(release.staged.is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT generation FROM pack_maintenance WHERE stream_id=?")
            .bind(first)
            .fetch_one(db)
            .await?,
        previous_generation + 1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM cache_pins WHERE chunk_id=?")
            .bind(ids[0])
            .fetch_one(db)
            .await?,
        0
    );

    let output = pack(&backend, storage, owner).await?;
    sqlx::query("UPDATE chunk_encodings SET owner_task=? WHERE encoding_id=?")
        .bind(owner)
        .bind(chunks[1].encoding_id)
        .execute(db)
        .await?;
    assert!(UploadCacheStore::finish(&backend, stale).await.is_err());
    UploadCacheStore::finish(&backend, task).await?;
    assert_eq!(backend.raw(owner).await?.unwrap()["state"], "completed");
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=?")
            .bind(output)
            .fetch_one(db)
            .await?,
        "retired"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM chunk_encodings WHERE encoding_id=?")
            .bind(chunks[1].encoding_id)
            .fetch_one(db)
            .await?,
        "retired"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pack_inputs WHERE task_id=?")
            .bind(owner)
            .fetch_one(db)
            .await?,
        0
    );
    sqlx::query("DELETE FROM extents WHERE chunk_id=?")
        .bind(ids[1])
        .execute(db)
        .await?;
    assert_eq!(backend.unused(Some(storage)).await?, vec![ids[1]]);
    assert!(backend.unused(Some(Uuid::new_v4())).await?.is_empty());
    assert_eq!(
        backend.release(&[ids[1]], false).await?.keys,
        vec![(names[1].clone(), None)]
    );
    assert!(backend.release(&[], false).await?.keys.is_empty());

    let (left, right) = tokio::join!(backend.flush(None), backend.flush(None));
    let left = left?;
    let right = right?;
    assert_ne!(left, right);
    let flush = execute(&backend, left).await?;
    let snapshot = backend.raw(flush.id).await?.unwrap()["detail"].clone();
    let blocker = TaskControlStore::enqueue(&backend, "pack", None, json!({})).await?;
    sqlx::query("INSERT INTO pack_inputs(task_id,chunk_id) VALUES(?,?)")
        .bind(blocker)
        .bind(ids[2])
        .execute(db)
        .await?;
    assert!(matches!(
        backend.batch(flush, &snapshot, true).await?,
        CacheBatch::Wait
    ));
    sqlx::query("DELETE FROM pack_inputs WHERE task_id=?")
        .bind(blocker)
        .execute(db)
        .await?;
    sqlx::query("UPDATE tasks SET state='failed' WHERE id=?")
        .bind(blocker)
        .execute(db)
        .await?;
    sqlx::query("UPDATE pending_uploads SET owner_task=? WHERE chunk_id=?")
        .bind(blocker)
        .bind(ids[2])
        .execute(db)
        .await?;
    assert!(backend.batch(flush, &json!({}), true).await.is_err());
    assert!(
        matches!(backend.batch(flush,&snapshot,true).await?,CacheBatch::Encode{rows,..} if rows.len()==1)
    );
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>("SELECT owner_task FROM pending_uploads WHERE chunk_id=?")
            .bind(ids[2])
            .fetch_one(db)
            .await?,
        flush.id
    );
    durable(&backend, &chunks[2]).await?;
    assert_eq!(
        backend.release(&[ids[2]], false).await?.keys,
        vec![(names[2].clone(), Some(chunks[2].location_id))]
    );
    let later = stream(&backend, bucket, "later").await?;
    let late_chunk = chunk(&backend, storage, later, 9).await?;
    let late_name = stage(&backend, &late_chunk, later, 0, true).await?;
    assert!(matches!(
        backend.batch(flush, &snapshot, true).await?,
        CacheBatch::Finished
    ));
    UploadCacheStore::finish(&backend, flush).await?;
    assert!(backend.pending_only(late_chunk.id).await?);
    let retry =
        TaskControlStore::enqueue(&backend, "upload", None, json!({"first":late_chunk.id})).await?;
    sqlx::query("UPDATE tasks SET state='failed',error='test' WHERE id=?")
        .bind(retry)
        .execute(db)
        .await?;
    sqlx::query("UPDATE pending_uploads SET owner_task=?,next_retry_at=TIMESTAMPADD(SECOND,-1,CURRENT_TIMESTAMP(6)),attempts=1,last_error='test' WHERE chunk_id=?").bind(retry).bind(late_chunk.id).execute(db).await?;
    assert!(backend.next_attempt().await?.unwrap() < chrono::Utc::now());
    assert_eq!(backend.status().await?.0["failed_entries"], 1);
    assert!(UploadCacheStore::enqueue(&backend, &[]).await?);
    assert_eq!(backend.raw(retry).await?.unwrap()["state"], "queued");
    assert!(backend.raw(retry).await?.unwrap()["error"].is_null());
    assert!(!UploadCacheStore::enqueue(&backend, &[]).await?);
    let retry = execute(&backend, retry).await?;
    UploadCacheStore::pause(&backend, retry).await?;
    assert!(UploadCacheStore::finish(&backend, retry).await.is_err());
    assert!(backend.change_state(retry.id, false, true).await?);
    let current = execute(&backend, retry.id).await?;
    assert!(
        backend
            .batch(retry, &json!({"first":late_chunk.id}), false)
            .await
            .is_err()
    );
    assert!(matches!(
        backend.batch(current, &json!({}), false).await?,
        CacheBatch::Finished
    ));
    assert!(matches!(
        backend
            .batch(current, &json!({"first":late_chunk.id}), false)
            .await?,
        CacheBatch::Encode { .. }
    ));
    durable(&backend, &late_chunk).await?;
    assert_eq!(
        backend.release(&[late_chunk.id], false).await?.keys,
        vec![(late_name, Some(late_chunk.location_id))]
    );
    UploadCacheStore::finish(&backend, current).await?;

    let reused = stream(&backend, bucket, "reused").await?;
    let ready_pack = pack(&backend, storage, blocker).await?;
    sqlx::query("UPDATE packs SET state='ready',stored_size=64 WHERE id=?")
        .bind(ready_pack)
        .execute(db)
        .await?;
    sqlx::query("UPDATE chunks SET pack_id=? WHERE id IN (?,?)")
        .bind(ready_pack)
        .bind(ids[0])
        .bind(late_chunk.id)
        .execute(db)
        .await?;
    sqlx::query("UPDATE chunk_encodings SET state='retired' WHERE chunk_id IN (?,?)")
        .bind(ids[0])
        .bind(late_chunk.id)
        .execute(db)
        .await?;
    for (offset, id) in [ids[0], late_chunk.id].into_iter().enumerate() {
        assert!(backend.can_stage_reuse(id).await?);
        let source = backend.ready_chunk(id).await?.unwrap();
        assert!(source.location_id.is_nil());
        stage(&backend, &source, reused, offset as i64 * 32, false).await?;
    }
    assert!(
        backend
            .release(&[ids[0], late_chunk.id], false)
            .await?
            .keys
            .is_empty()
    );
    backend.seal(reused, true).await?;
    assert!(UploadCacheStore::enqueue(&backend, &[]).await?);
    let owner: Uuid = sqlx::query_scalar("SELECT owner_task FROM pending_uploads WHERE chunk_id=?")
        .bind(ids[0])
        .fetch_one(db)
        .await?;
    let task = execute(&backend, owner).await?;
    let detail = backend.raw(owner).await?.unwrap()["detail"].clone();
    assert!(
        matches!(backend.batch(task,&detail,false).await?,CacheBatch::Rewrite{pack,ids,..} if pack==ready_pack.into() && ids.len()==2)
    );
    assert_eq!(
        backend
            .release(&[ids[0], late_chunk.id], true)
            .await?
            .keys
            .len(),
        2
    );
    UploadCacheStore::finish(&backend, task).await?;
    assert_eq!(backend.status().await?.0["entries"], 0);
    assert!(backend.status().await?.1.is_empty());
    assert!(backend.next_attempt().await?.is_none());
    let third = backend.ready_chunk(ids[2]).await?.unwrap();
    assert!(
        !backend
            .stage(CacheStage {
                buffered: false,
                boot_id: None,
                encoding: None,
                node: None,
                chunk: &third,
                stream: reused,
                offset: 64,
                new: false,
                size: 32,
                storage,
                key: &names[2],
                timeout: 1
            })
            .await?
    );
    assert!(
        backend
            .stage(CacheStage {
                buffered: false,
                boot_id: None,
                encoding: None,
                node: None,
                chunk: &third,
                stream: reused,
                offset: 64,
                new: false,
                size: 32,
                storage,
                key: "__mokyu/multipart/remote",
                timeout: 1
            })
            .await
            .is_err()
    );
    assert!(
        backend
            .stage(CacheStage {
                buffered: false,
                boot_id: None,
                encoding: None,
                node: None,
                chunk: &third,
                stream: first,
                offset: 96,
                new: false,
                size: 32,
                storage,
                key: &names[2],
                timeout: 1
            })
            .await
            .is_err()
    );
    let rollback = chunk(&backend, storage, reused, 42).await?;
    assert!(stage(&backend, &rollback, reused, 0, true).await.is_err());
    assert!(backend.pending_source(rollback.id).await?.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM chunks WHERE id=?")
            .bind(rollback.id)
            .fetch_one(db)
            .await?,
        "uploading"
    );
    let mut stale_source = rollback.clone();
    stale_source.encoding_id = i64::MAX.into();
    assert!(
        stage(&backend, &stale_source, reused, 64, true)
            .await
            .is_err()
    );
    let other_storage = super::storage_tests::fixture_named(&backend, "other").await?;
    let other_bucket = BucketStore::create(
        &backend,
        &Principal::Local,
        CreateBucket {
            name: "other".into(),
            project_id: None,
            storage_id: Some(other_storage),
        },
    )
    .await?
    .id;
    let other_stream = stream(&backend, other_bucket, "other").await?;
    assert!(
        stage(&backend, &rollback, other_stream, 0, true)
            .await
            .is_err()
    );
    assert_eq!(backend.status().await?.0["entries"], 0);
    assert!(backend.status().await?.1.is_empty());
    backend.disable(reused).await?;
    assert!(backend.bypass(reused).await?);
    Ok(())
}
