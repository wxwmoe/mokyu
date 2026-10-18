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
    migration::run(&mut *pool.acquire().await?).await?;
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

#[tokio::test]
#[ignore = "requires MOKYU_TEST_DATABASE_URL_FILE pointing to a new isolated PostgreSQL database"]
async fn postgres_source_validation_allows_references() -> Result<()> {
    use anyhow::Context;
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
    migration::run(&mut *pool.acquire().await?).await?;
    let storage = Uuid::new_v4();
    let bucket = Uuid::new_v4();
    let stream = Uuid::new_v4();
    let other = Uuid::new_v4();
    sqlx::query("INSERT INTO storages(id,name,type,path) VALUES($1,'source-lock','local','/data/source-lock')")
        .bind(storage).execute(&pool).await?;
    sqlx::query("INSERT INTO buckets(id,name,storage_id) VALUES($1,'source-lock',$2)")
        .bind(bucket)
        .bind(storage)
        .execute(&pool)
        .await?;
    for id in [stream, other] {
        sqlx::query("INSERT INTO streams(id,bucket_id,object_key,stream_type,state) VALUES($1,$2,$1::text,'object','writing')")
            .bind(id).bind(bucket).execute(&pool).await?;
    }
    let chunk: i64 = sqlx::query_scalar("INSERT INTO chunks(storage_id,hash,raw_size,encryption_algorithm,state) VALUES($1,$2,1,'none','ready') RETURNING id")
        .bind(storage).bind(vec![3u8;32]).fetch_one(&pool).await?;
    let encoding: i64 = sqlx::query_scalar("INSERT INTO chunk_encodings(encoding_id,chunk_id,location_id,storage_id,standalone_size,standalone_compressed,state,encryption_algorithm,created_at) SELECT current_encoding_id,id,$2,storage_id,1,false,'ready','none',now() FROM chunks WHERE id=$1 RETURNING encoding_id")
        .bind(chunk).bind(Uuid::new_v4()).fetch_one(&pool).await?;
    super::queries::insert_chunk_extent(stream, 0, 1, chunk)
        .execute(&pool)
        .await?;
    let ids = [chunk];
    for scope in [Some(stream), None] {
        let mut validation = pool.begin().await?;
        for phase in 0..3 {
            super::queries::source_locks(scope, &ids, phase)
                .build_query_scalar::<i64>()
                .fetch_all(&mut *validation)
                .await?;
        }
        assert!(
            !super::queries::unavailable_sources(scope, &ids)
                .build_query_scalar::<bool>()
                .fetch_one(&mut *validation)
                .await?
        );
        let mut writer = pool.begin().await?;
        sqlx::query("SET LOCAL lock_timeout='100ms'")
            .execute(&mut *writer)
            .await?;
        assert_eq!(
            super::queries::lock_chunk(chunk)
                .fetch_one(&mut *writer)
                .await
                .context("reference lock")?,
            "ready"
        );
        assert_eq!(
            super::queries::lock_ready_chunks(&ids)
                .fetch_all(&mut *writer)
                .await
                .context("copy reference lock")?,
            ids
        );
        sqlx::query("UPDATE chunks SET unreferenced_at=now() WHERE id=$1")
            .bind(chunk)
            .execute(&mut *writer)
            .await
            .context("liveness timestamp")?;
        assert_eq!(
            super::queries::reference_chunk(chunk)
                .execute(&mut *writer)
                .await
                .context("reference update")?
                .rows_affected(),
            1
        );
        assert_eq!(
            super::queries::reference_chunk(chunk)
                .execute(&mut *writer)
                .await
                .context("reference update")?
                .rows_affected(),
            0
        );
        assert_eq!(
            super::queries::reference_chunks(&ids)
                .execute(&mut *writer)
                .await
                .context("copy liveness update")?
                .rows_affected(),
            0
        );
        super::queries::insert_chunk_extent(other, 0, 1, chunk)
            .execute(&mut *writer)
            .await
            .context("reference insertion")?;
        writer.rollback().await?;

        let mut mapping = pool.begin().await?;
        sqlx::query("SET LOCAL lock_timeout='100ms'")
            .execute(&mut *mapping)
            .await?;
        let blocked = super::queries::lock_pack_layout(&ids)
            .fetch_all(&mut *mapping)
            .await
            .unwrap_err();
        assert_eq!(
            blocked.as_database_error().unwrap().code().as_deref(),
            Some("55P03")
        );
        mapping.rollback().await?;
        let mut quarantine = pool.begin().await?;
        sqlx::query("SET LOCAL lock_timeout='100ms'")
            .execute(&mut *quarantine)
            .await?;
        let blocked = sqlx::query("UPDATE chunk_encodings SET quarantined_at=now(),quarantine_code='hash_mismatch',quarantine_revision=quarantine_revision+1 WHERE encoding_id=$1")
            .bind(encoding).execute(&mut *quarantine).await.unwrap_err();
        assert_eq!(
            blocked.as_database_error().unwrap().code().as_deref(),
            Some("55P03")
        );
        quarantine.rollback().await?;
        validation.rollback().await?;
    }
    sqlx::query("UPDATE chunk_encodings SET quarantined_at=now(),quarantine_code='hash_mismatch',quarantine_revision=quarantine_revision+1 WHERE encoding_id=$1")
        .bind(encoding).execute(&pool).await?;
    assert!(
        super::queries::unavailable_sources(Some(stream), &[])
            .build_query_scalar::<bool>()
            .fetch_one(&pool)
            .await?
    );
    Ok(())
}
