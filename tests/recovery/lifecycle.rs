use super::{Backend, migration};
use crate::model::maintenance::MaintenanceStore;
use anyhow::Result;
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_DATABASE_URL_FILE pointing to a new isolated PostgreSQL database"]
async fn postgres_fragment_deletion_holds_lock() -> Result<()> {
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
    let backend = Backend::new(pool.clone(), pool.acquire().await?.detach(), false);
    let storage = Uuid::new_v4();
    let bucket = Uuid::new_v4();
    let stream = Uuid::new_v4();
    let fragment = Uuid::new_v4();
    sqlx::query("INSERT INTO storages(id,name,type,path) VALUES($1,'fragment-lock','local','/data/fragment-lock')")
        .bind(storage).execute(&pool).await?;
    sqlx::query("INSERT INTO buckets(id,name,storage_id) VALUES($1,'fragment-lock',$2)")
        .bind(bucket)
        .bind(storage)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO streams(id,bucket_id,object_key,stream_type,state) VALUES($1,$2,'fragment-lock','part','writing')")
        .bind(stream).bind(bucket).execute(&pool).await?;
    sqlx::query("INSERT INTO fragments(id,owner_stream,sealed,size,hash) VALUES($1,$2,true,1,$3)")
        .bind(fragment)
        .bind(stream)
        .bind([3u8; 32].as_slice())
        .execute(&pool)
        .await?;
    let reference =
        "INSERT INTO extents(stream_id,offset_bytes,length,fragment_id) VALUES($1,0,1,$2)";

    let guard = backend.claim_fragment(fragment).await?.unwrap();
    let mut writer = pool.begin().await?;
    sqlx::query("SET LOCAL lock_timeout='100ms'")
        .execute(&mut *writer)
        .await?;
    let blocked = sqlx::query(reference)
        .bind(stream)
        .bind(fragment)
        .execute(&mut *writer)
        .await
        .unwrap_err();
    assert_eq!(
        blocked.as_database_error().unwrap().code().as_deref(),
        Some("55P03")
    );
    writer.rollback().await?;
    drop(guard);
    sqlx::query(reference)
        .bind(stream)
        .bind(fragment)
        .execute(&pool)
        .await?;
    assert!(backend.claim_fragment(fragment).await?.is_none());

    sqlx::query("DELETE FROM extents WHERE stream_id=$1")
        .bind(stream)
        .execute(&pool)
        .await?;
    backend
        .claim_fragment(fragment)
        .await?
        .unwrap()
        .deletion
        .finish()
        .await?;
    assert!(!backend.fragment_exists(fragment).await?);
    let deleted = sqlx::query(reference)
        .bind(stream)
        .bind(fragment)
        .execute(&pool)
        .await
        .unwrap_err();
    assert_eq!(
        deleted.as_database_error().unwrap().code().as_deref(),
        Some("23503")
    );
    assert!(backend.claim_fragment(fragment).await?.is_none());
    Ok(())
}
