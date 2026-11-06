use super::{cache_tests, connection, queries, storage_tests, transactions};
use crate::authorization::Principal;
use crate::config::Config;
use crate::model::bucket::CreateBucket;
use crate::model::{AccountStore, BucketStore, Id};
use anyhow::Result;
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_raw_claims_lock_only_the_selected_stream() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    backend.initialize_admin("admin", "password", false).await?;
    let pool = &backend.pool;
    let storage = storage_tests::fixture(&backend).await?;
    let bucket = BucketStore::create(
        &backend,
        &Principal::Local,
        CreateBucket {
            name: "raw-locks".into(),
            project_id: None,
            storage_id: Some(storage),
        },
    )
    .await?
    .id;
    let uploading = cache_tests::stream(&backend, bucket, "uploading").await?;
    let mut held = pool.begin().await?;
    sqlx::query("SELECT id FROM streams WHERE id=? FOR UPDATE")
        .bind(uploading)
        .fetch_one(&mut *held)
        .await?;
    assert!(
        tokio::time::timeout(
            Duration::from_secs(2),
            transactions::claim_raw_processing(pool, false, Uuid::new_v4())
        )
        .await??
        .is_none()
    );
    held.rollback().await?;

    let stream = cache_tests::stream(&backend, bucket, "ready").await?;
    sqlx::query("UPDATE streams SET state='ready',payload_type='raw',size=32 WHERE id=?")
        .bind(stream)
        .execute(pool)
        .await?;
    assert_eq!(
        sqlx::query("UPDATE objects SET stream_id=? WHERE bucket_id=? AND object_key='ready'")
            .bind(stream)
            .bind(bucket)
            .execute(pool)
            .await?
            .rows_affected(),
        1
    );
    let source = Id::try_from(sqlx::query("INSERT INTO raw_sources(location_id,storage_id,state,size,digest,profile_revision) VALUES(?,?,'ready',32,?,1)")
        .bind(Uuid::new_v4()).bind(storage).bind(&[2u8; 16][..]).execute(pool).await?.last_insert_id())?;
    queries::raw_enqueue_processing(stream, source)
        .execute(pool)
        .await?;
    let mut held = pool.begin().await?;
    sqlx::query("SELECT id FROM streams WHERE id=? FOR UPDATE")
        .bind(uploading)
        .fetch_one(&mut *held)
        .await?;
    let work = tokio::time::timeout(
        Duration::from_secs(2),
        transactions::claim_raw_processing(pool, false, Uuid::new_v4()),
    )
    .await??
    .unwrap();
    assert_eq!(work.stream, stream);
    assert_eq!(work.source, source);
    held.rollback().await?;
    assert!(
        transactions::claim_raw_processing(pool, false, Uuid::new_v4())
            .await?
            .is_none()
    );

    sqlx::query("UPDATE raw_processing SET token=NULL,lease_expires_at=NULL WHERE stream_id=?")
        .bind(stream)
        .execute(pool)
        .await?;
    assert_eq!(
        queries::raw_processing_due(false, None)
            .build_query_scalar::<Id>()
            .fetch_optional(pool)
            .await?,
        Some(stream)
    );
    sqlx::query("UPDATE streams SET state='writing' WHERE id=?")
        .bind(stream)
        .execute(pool)
        .await?;
    assert!(
        queries::raw_processing_due(false, Some(stream))
            .build_query_scalar::<Id>()
            .fetch_optional(pool)
            .await?
            .is_none()
    );
    sqlx::query("UPDATE streams SET state='ready' WHERE id=?")
        .bind(stream)
        .execute(pool)
        .await?;
    let (first, second) = tokio::join!(
        transactions::claim_raw_processing(pool, false, Uuid::new_v4()),
        transactions::claim_raw_processing(pool, false, Uuid::new_v4()),
    );
    assert_ne!(first?.is_some(), second?.is_some());
    raw_cleanup_locks(pool, stream, uploading, source).await?;
    Ok(())
}

async fn raw_cleanup_locks(
    pool: &sqlx::MySqlPool,
    stream: Id,
    other: Id,
    source: Id,
) -> Result<()> {
    for id in [stream, other] {
        queries::raw_attach(id, source, 16).execute(pool).await?;
    }
    sqlx::query("INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id,source_offset) VALUES(?,16,16,?,16)")
        .bind(stream).bind(source).execute(pool).await?;
    let mut gc = pool.begin().await?;
    assert_eq!(
        queries::raw_cleanup_ranges(stream, 1)
            .fetch_all(&mut *gc)
            .await?,
        [(0, source)]
    );
    // The collector owns the selected range before it locks the shared physical source.
    let selected: Option<Id> = sqlx::query_scalar(
        "SELECT source_id FROM raw_ranges WHERE stream_id=? AND offset_bytes=0 FOR UPDATE SKIP LOCKED",
    ).bind(stream).fetch_optional(pool).await?;
    assert_eq!(selected, None);
    for (id, offset) in [(stream, 16), (other, 0)] {
        let rows = tokio::time::timeout(
            Duration::from_secs(2),
            queries::node_read_raw(id, offset, offset + 16, 1).fetch_all(pool),
        )
        .await??;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].range.source.id, source);
    }
    queries::raw_source(source).fetch_one(&mut *gc).await?;
    assert_eq!(
        queries::raw_detach(stream, 0)
            .execute(&mut *gc)
            .await?
            .rows_affected(),
        1
    );
    gc.commit().await?;
    assert!(
        queries::node_read_raw(stream, 0, 16, 1)
            .fetch_all(pool)
            .await?
            .is_empty()
    );
    assert_eq!(
        queries::node_read_raw(other, 0, 16, 1)
            .fetch_one(pool)
            .await?
            .range
            .source
            .id,
        source
    );
    Ok(())
}
