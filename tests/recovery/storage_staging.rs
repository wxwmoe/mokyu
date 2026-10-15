use super::{Backend, migration};
use crate::model::PhysicalStore;
use anyhow::Result;
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_DATABASE_URL_FILE pointing to a new isolated PostgreSQL database"]
async fn postgres_staging_claim_is_atomic() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = PgPool::connect(url.trim()).await?;
    assert!(
        super::queries::metadata_table()
            .fetch_one(&pool)
            .await?
            .is_none()
    );
    migration::run(&pool, &mut *pool.acquire().await?, false).await?;
    let storage = Uuid::new_v4();
    sqlx::query("INSERT INTO storages(id,name,type,path) VALUES($1,'staging-contract','local','/data/staging-contract')").bind(storage).execute(&pool).await?;
    let first = Backend::new(pool.clone());
    let second = Backend::new(pool.clone());
    let (a, b) = tokio::join!(
        first.claim_staging(storage, "same", 8, 8),
        second.claim_staging(storage, "same", 8, 8)
    );
    a?;
    b?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT sum(bytes)::bigint FROM staging_objects WHERE storage_id=$1"
        )
        .bind(storage)
        .fetch_one(&pool)
        .await?,
        8
    );
    assert!(first.claim_staging(storage, "same", 7, 8).await.is_err());
    assert!(second.claim_staging(storage, "excess", 1, 8).await.is_err());
    first.release_staging(storage, "same").await?;
    first.release_staging(storage, "same").await?;
    let (a, b) = tokio::join!(
        first.claim_staging(storage, "left", 8, 8),
        second.claim_staging(storage, "right", 8, 8)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let names: Vec<String> =
        sqlx::query_scalar("SELECT object_key FROM staging_objects WHERE storage_id=$1")
            .bind(storage)
            .fetch_all(&pool)
            .await?;
    assert_eq!(names.len(), 1);
    first.release_staging(storage, &names[0]).await?;
    assert!(
        first
            .claim_staging(storage, "overflow", u64::MAX, u64::MAX)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM staging_objects WHERE storage_id=$1")
            .bind(storage)
            .fetch_one(&pool)
            .await?,
        0
    );
    Ok(())
}
