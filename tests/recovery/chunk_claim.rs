use super::{Backend, migration, queries};
use crate::model::{ChunkCreate, ChunkStore, Error, Id, cluster::ClusterStore};
use anyhow::Result;
use sqlx::{Connection, PgConnection, PgPool, postgres::PgPoolOptions};
use std::{sync::Arc, time::Duration};
use tokio::time::timeout;
use uuid::Uuid;

async fn stream(pool: &PgPool, bucket: Uuid, key: &str) -> Result<Id> {
    let id = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state) VALUES($1,$2,'object','writing') RETURNING id")
        .bind(bucket).bind(key).fetch_one(pool).await?;
    queries::create_write_lease(id, 300).execute(pool).await?;
    Ok(id)
}

fn allocation(storage: Uuid, stream: Id, hash: u8) -> ChunkCreate {
    ChunkCreate {
        storage,
        stream,
        hash: [hash; 32],
        raw_size: 32,
        algorithm: "none".into(),
        key: None,
    }
}

#[tokio::test]
#[ignore = "requires a fresh isolated PostgreSQL database"]
async fn postgres_chunk_claims_release_connections_and_fence_stale_writers() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(url.trim())
        .await?;
    migration::run(&mut *pool.acquire().await?).await?;
    sqlx::query("UPDATE mokyu_meta SET deployment_mode='cluster'")
        .execute(&pool)
        .await?;
    let first = Arc::new(Backend::new(
        pool.clone(),
        PgConnection::connect(url.trim()).await?,
        false,
    ));
    let other_pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(url.trim())
        .await?;
    let other = Backend::new(
        other_pool.clone(),
        PgConnection::connect(url.trim()).await?,
        false,
    );
    let _first_profile = first.profile_changes().await?;
    let _other_profile = other.profile_changes().await?;
    let mut first_tasks = sqlx::postgres::PgListener::connect_with(&pool).await?;
    first_tasks.listen("mokyu_tasks").await?;
    let mut other_tasks = sqlx::postgres::PgListener::connect_with(&other_pool).await?;
    other_tasks.listen("mokyu_tasks").await?;
    let storage = Uuid::new_v4();
    let bucket = Uuid::new_v4();
    sqlx::query("INSERT INTO storages(id,name,type,endpoint_url,remote_bucket_name,region,access_key_encrypted,secret_key_encrypted) VALUES($1,'claims','s3','http://backend:8333','claims','us-east-1','fixture','fixture')")
        .bind(storage).execute(&pool).await?;
    sqlx::query("INSERT INTO buckets(id,project_id,name,storage_id) VALUES($1,'00000000-0000-0000-0000-000000000001','claims',$2)")
        .bind(bucket).bind(storage).execute(&pool).await?;
    let left = stream(&pool, bucket, "left").await?;
    let right = stream(&pool, bucket, "right").await?;
    let (mut old, reused) = first.allocate(allocation(storage, left, 1)).await?;
    assert!(!reused);
    let error = other
        .allocate(allocation(storage, right, 1))
        .await
        .err()
        .expect("the live owner must retain its allocation");
    assert_eq!(crate::model::error(&error), Some(Error::ChunkBusy));
    let (independent, reused) = other.allocate(allocation(storage, right, 2)).await?;
    assert!(!reused && independent.id != old.id);
    for pool in [&pool, &other_pool] {
        assert_eq!(
            timeout(
                Duration::from_secs(2),
                sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(pool)
            )
            .await??,
            1
        );
        let mut probe = pool.begin().await?;
        assert!(sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock(hashtextextended($1::uuid::text || ':' || encode($2::bytea,'hex'),734922709851001))")
            .bind(storage).bind([1u8;32].as_slice()).fetch_one(&mut *probe).await?);
        probe.rollback().await?;
    }

    old.stored_size = Some(32);
    assert!(first.start_upload(right, &old).await.is_err());
    let mut invalid = old.clone();
    invalid.stored_size = Some(-1);
    assert!(first.start_upload(left, &invalid).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM chunks WHERE id=$1")
            .bind(old.id)
            .fetch_one(&pool)
            .await?,
        "preparing"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM chunk_encodings WHERE encoding_id=$1")
            .bind(old.encoding_id)
            .fetch_one(&pool)
            .await?,
        0
    );
    first.start_upload(left, &old).await?;
    sqlx::query("UPDATE stream_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE stream_id=$1")
        .bind(left).execute(&pool).await?;
    assert!(first.renew_write(left, 300).await.is_err());
    assert!(first.complete(left, 0, &old).await.is_err());
    let (mut current, reused) = other.allocate(allocation(storage, right, 1)).await?;
    assert!(!reused && current.id != old.id && current.encoding_id != old.encoding_id);
    assert!(first.start_upload(left, &old).await.is_err());
    assert!(first.complete(left, 0, &old).await.is_err());
    current.stored_size = Some(32);
    other.start_upload(right, &current).await?;
    other.complete(right, 0, &current).await?;
    let next = stream(&pool, bucket, "reuse").await?;
    let (reused, existing) = first.allocate(allocation(storage, next, 1)).await?;
    assert!(existing && reused.id == current.id);
    assert!(
        first
            .reference(left, 0, current.id, current.raw_size)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM extents WHERE stream_id=$1")
            .bind(left)
            .fetch_one(&pool)
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM chunks WHERE id=$1")
            .bind(old.id)
            .fetch_one(&pool)
            .await?,
        "failed"
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT recovery_pending AND owner_stream=$2 FROM chunk_encodings WHERE encoding_id=$1"
        )
        .bind(old.encoding_id)
        .bind(left)
        .fetch_one(&pool)
        .await?
    );
    queries::retire_stream_encodings(left, 10)
        .execute(&pool)
        .await?;
    assert!(sqlx::query_scalar::<_, bool>("SELECT recovery_pending AND owner_stream IS NULL AND state='retired' FROM chunk_encodings WHERE encoding_id=$1")
        .bind(old.encoding_id).fetch_one(&pool).await?);

    let task = Uuid::new_v4();
    let token = Uuid::new_v4();
    sqlx::query("INSERT INTO tasks(id,task_type,state,lease_token,lease_expires_at) VALUES($1,'upload','running',$2,clock_timestamp()+interval '5 minutes')")
        .bind(task).bind(token).execute(&pool).await?;
    let owned = stream(&pool, bucket, "task").await?;
    sqlx::query("UPDATE streams SET owner_task_id=$1,owner_lease_token=$2 WHERE id=$3")
        .bind(task)
        .bind(token)
        .bind(owned)
        .execute(&pool)
        .await?;
    let (mut stale, _) = first.allocate(allocation(storage, owned, 3)).await?;
    stale.stored_size = Some(32);
    first.start_upload(owned, &stale).await?;
    sqlx::query("UPDATE tasks SET lease_token=$1 WHERE id=$2")
        .bind(Uuid::new_v4())
        .bind(task)
        .execute(&pool)
        .await?;
    assert!(first.complete(owned, 0, &stale).await.is_err());
    assert!(first.allocate(allocation(storage, owned, 4)).await.is_err());
    let (replacement, reused) = other.allocate(allocation(storage, next, 3)).await?;
    assert!(!reused && replacement.id != stale.id);

    let cancelled = stream(&pool, bucket, "cancelled").await?;
    let mut blocker = PgConnection::connect(url.trim()).await?;
    let mut lock = blocker.begin().await?;
    queries::claim_chunk_hash(storage, &[9; 32])
        .execute(&mut *lock)
        .await?;
    let busy = timeout(
        Duration::from_secs(2),
        first.allocate(allocation(storage, cancelled, 9)),
    )
    .await?
    .err()
    .expect("a held hash lock must report contention");
    assert_eq!(crate::model::error(&busy), Some(Error::ChunkBusy));
    lock.rollback().await?;
    let mut lock = blocker.begin().await?;
    queries::source_writing_stream(cancelled)
        .fetch_one(&mut *lock)
        .await?;
    let mut pending = tokio::spawn({
        let backend = first.clone();
        async move { backend.allocate(allocation(storage, cancelled, 9)).await }
    });
    assert!(
        timeout(Duration::from_millis(100), &mut pending)
            .await
            .is_err()
    );
    pending.abort();
    assert!(matches!(pending.await, Err(error) if error.is_cancelled()));
    lock.rollback().await?;
    let (recovered, _) = timeout(
        Duration::from_secs(2),
        first.allocate(allocation(storage, cancelled, 9)),
    )
    .await??;
    assert_eq!(recovered.hash, vec![9; 32]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM chunks WHERE storage_id=$1 AND hash=$2 AND state='preparing'"
        )
        .bind(storage)
        .bind([9u8; 32].as_slice())
        .fetch_one(&pool)
        .await?,
        1
    );
    Ok(())
}
