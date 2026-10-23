use super::{migration, queries};
use crate::model::Id;
use anyhow::Result;
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_DATABASE_URL_FILE pointing to a new isolated PostgreSQL database"]
async fn postgres_raw_allocation_preserves_settings_without_blocking_writers() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = PgPool::connect(url.trim()).await?;
    assert!(queries::metadata_table().fetch_one(&pool).await?.is_none());
    migration::run(&mut *pool.acquire().await?).await?;
    let storage = Uuid::new_v4();
    let bucket = Uuid::new_v4();
    let stream = Id::from(Uuid::now_v7());
    sqlx::query(
        "INSERT INTO storages(id,name,type,path) VALUES($1,'raw-locks','local','/data/raw-locks')",
    )
    .bind(storage)
    .execute(&pool)
    .await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id) VALUES($1)")
        .bind(storage)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO buckets(id,name,storage_id) VALUES($1,'raw-locks',$2)")
        .bind(bucket)
        .bind(storage)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO streams(id,bucket_id,object_key,stream_type,state) VALUES($1,$2,'object','object','writing')")
        .bind(stream).bind(bucket).execute(&pool).await?;
    let mut writer = pool.begin().await?;
    queries::lock_bucket_uploads(bucket)
        .fetch_one(&mut *writer)
        .await?;
    let mut raw = pool.begin().await?;
    tokio::time::timeout(
        Duration::from_secs(2),
        queries::raw_target(stream, storage).fetch_one(&mut *raw),
    )
    .await??;
    writer.rollback().await?;
    for (statement, id) in [
        (
            "SELECT id FROM buckets WHERE id=$1 FOR UPDATE NOWAIT",
            bucket,
        ),
        (
            "SELECT id FROM storages WHERE id=$1 FOR UPDATE NOWAIT",
            storage,
        ),
        (
            "SELECT storage_id FROM storage_profiles WHERE storage_id=$1 FOR UPDATE NOWAIT",
            storage,
        ),
    ] {
        let error = sqlx::query(statement)
            .bind(id)
            .execute(&pool)
            .await
            .unwrap_err();
        assert_eq!(
            error.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("55P03")
        );
    }
    raw.rollback().await?;
    queries::raw_target(stream, storage)
        .fetch_one(&pool)
        .await?;
    Ok(())
}
