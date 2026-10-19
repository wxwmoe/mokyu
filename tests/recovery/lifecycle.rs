use super::{Backend, migration};
use crate::model::maintenance::MaintenanceStore;
use anyhow::Result;
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a fresh isolated PostgreSQL database"]
async fn postgres_storage_policy_expiration() -> Result<()> {
    use crate::model::{MultipartStore, StorageProfileStore};
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = PgPool::connect(url.trim()).await?;
    migration::run(&mut *pool.acquire().await?).await?;
    let backend = Backend::new(pool.clone(), pool.acquire().await?.detach(), false);
    let storage = Uuid::new_v4();
    let bucket = Uuid::new_v4();
    let upload = Uuid::new_v4();
    let stream = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO storages(id,name,type,path) VALUES($1,'policy','local','/data/policy')",
    )
    .bind(storage)
    .execute(&pool)
    .await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id,multipart_idle_timeout) VALUES($1,'1s')")
        .bind(storage)
        .execute(&pool)
        .await?;
    StorageProfileStore::initialize(&backend, "64MiB").await?;
    let profile = backend.load(storage).await?;
    StorageProfileStore::initialize(&backend, "2GiB").await?;
    assert_eq!(backend.load(storage).await?.revision, profile.revision);
    assert_eq!(profile.min_free_space.as_deref(), Some("64MiB"));
    sqlx::query("INSERT INTO buckets(id,name,storage_id) VALUES($1,'policy',$2)")
        .bind(bucket)
        .bind(storage)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO uploads(id,bucket_id,object_key,access_key,touched_at) VALUES($1,$2,'policy','fixture',now()-interval '1 hour')").bind(upload).bind(bucket).execute(&pool).await?;
    assert_eq!(
        backend.expired_uploads(10, Some(storage)).await?,
        vec![upload]
    );
    backend.touch(upload).await?;
    assert!(!backend.expire(upload).await?);
    sqlx::query("UPDATE uploads SET touched_at=now()-interval '1 hour' WHERE id=$1")
        .bind(upload)
        .execute(&pool)
        .await?;
    sqlx::query("UPDATE storage_profiles SET multipart_idle_timeout='2h' WHERE storage_id=$1")
        .bind(storage)
        .execute(&pool)
        .await?;
    assert!(backend.expired_uploads(10, None).await?.is_empty());
    assert!(!backend.expire(upload).await?);
    sqlx::query("UPDATE storage_profiles SET multipart_idle_timeout='1s' WHERE storage_id=$1")
        .bind(storage)
        .execute(&pool)
        .await?;

    // Expiry must see a part writer that committed while it waited for the parent lock.
    let mut writer = pool.begin().await?;
    super::queries::lock_upload_state(upload)
        .fetch_one(&mut *writer)
        .await?;
    let cleanup = tokio::spawn({
        let pool = pool.clone();
        async move {
            super::transactions::abort_multipart(
                &pool,
                &crate::authorization::Permit::system(),
                upload,
                true,
            )
            .await
        }
    });
    sqlx::query("INSERT INTO streams(id,bucket_id,object_key,stream_type,state) VALUES($1,$2,'policy','part','writing')").bind(stream).bind(bucket).execute(&mut *writer).await?;
    sqlx::query("INSERT INTO stream_leases(id,stream_id,lease_type,expires_at) VALUES($1,$1,'write',now()+interval '1 minute')").bind(stream).execute(&mut *writer).await?;
    super::queries::begin_upload_part(upload, 1, stream)
        .execute(&mut *writer)
        .await?;
    writer.commit().await?;
    assert!(!cleanup.await??);
    assert!(!backend.expire(upload).await?);
    sqlx::query("UPDATE stream_leases SET expires_at=now()-interval '1 second' WHERE stream_id=$1")
        .bind(stream)
        .execute(&pool)
        .await?;
    assert!(backend.expire(upload).await?);
    assert!(!backend.expire(upload).await?);
    assert!(!backend.expire(Uuid::new_v4()).await?);
    Ok(())
}

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
    async fn available(
        db: &mut sqlx::PgConnection,
        stream: Option<Uuid>,
        ids: &[i64],
    ) -> Result<bool> {
        match super::transactions::require_sources(db, stream, ids).await {
            Ok(()) => Ok(true),
            Err(error)
                if crate::model::error(&error) == Some(crate::model::Error::SourceUnavailable) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }
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
        assert!(available(&mut validation, scope, &ids).await?);
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
    assert!(!available(&mut *pool.acquire().await?, Some(stream), &[]).await?);

    let pack: i64 = sqlx::query_scalar("INSERT INTO packs(location_id,storage_id,encryption_algorithm,raw_size,stored_size,member_count,state,digest) VALUES($1,$2,'none',2,2,2,'ready',$3) RETURNING id")
        .bind(Uuid::new_v4()).bind(storage).bind(vec![0u8;32]).fetch_one(&pool).await?;
    for scope in [Some(stream), None] {
        for phase in 0..3 {
            sqlx::query("UPDATE chunks SET state='ready',pack_id=$2 WHERE id=$1")
                .bind(chunk)
                .bind(if phase == 2 { Some(pack) } else { None })
                .execute(&pool)
                .await?;
            sqlx::query("UPDATE chunk_encodings SET state=$2,quarantined_at=NULL,quarantine_code=NULL WHERE encoding_id=$1")
                .bind(encoding).bind(if phase==2 { "retired" } else { "ready" }).execute(&pool).await?;
            sqlx::query("UPDATE packs SET quarantined_at=NULL,quarantine_code=NULL WHERE id=$1")
                .bind(pack)
                .execute(&pool)
                .await?;
            let mut changing = pool.begin().await?;
            match phase {
                0 => {
                    sqlx::query("SELECT id FROM chunks WHERE id=$1 FOR UPDATE")
                        .bind(chunk)
                        .execute(&mut *changing)
                        .await?;
                    sqlx::query("UPDATE chunks SET state='failed' WHERE id=$1")
                        .bind(chunk)
                        .execute(&mut *changing)
                        .await?;
                }
                1 => {
                    sqlx::query("UPDATE chunk_encodings SET quarantined_at=now(),quarantine_code='hash_mismatch',quarantine_revision=quarantine_revision+1 WHERE encoding_id=$1")
                    .bind(encoding).execute(&mut *changing).await?;
                }
                _ => {
                    sqlx::query("UPDATE packs SET quarantined_at=now(),quarantine_code='hash_mismatch',quarantine_revision=quarantine_revision+1 WHERE id=$1")
                    .bind(pack).execute(&mut *changing).await?;
                }
            }
            let mut validation = pool.begin().await?;
            assert_eq!(
                sqlx::query_scalar::<_, String>("SHOW transaction_isolation")
                    .fetch_one(&mut *validation)
                    .await?,
                "read committed"
            );
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *validation)
                .await?;
            let release = async {
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    loop {
                        let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')")
                            .bind(pid).fetch_one(&pool).await?;
                        if waiting { break; }
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    anyhow::Ok(())
                }).await??;
                changing.commit().await?;
                anyhow::Ok(())
            };
            let (healthy, released) =
                tokio::join!(available(&mut validation, scope, &ids), release);
            released?;
            assert!(!healthy?, "source changed while waiting in phase {phase}");
            assert_eq!(
                sqlx::query_scalar::<_, i32>("SELECT 1")
                    .fetch_one(&mut *validation)
                    .await?,
                1
            );
            let mut mapping = pool.begin().await?;
            sqlx::query("SET LOCAL lock_timeout='100ms'")
                .execute(&mut *mapping)
                .await?;
            let blocked = sqlx::query("SELECT id FROM chunks WHERE id=$1 FOR UPDATE")
                .bind(chunk)
                .execute(&mut *mapping)
                .await
                .unwrap_err();
            assert_eq!(
                blocked.as_database_error().unwrap().code().as_deref(),
                Some("55P03")
            );
            mapping.rollback().await?;
            validation.rollback().await?;
        }
    }

    for (scope, ids) in [(Some(other), vec![]), (None, vec![]), (None, vec![-1])] {
        assert!(available(&mut *pool.acquire().await?, scope, &ids).await?);
    }
    let fragment = Uuid::new_v4();
    sqlx::query("INSERT INTO fragments(id,size,hash) VALUES($1,1,$2)")
        .bind(fragment)
        .bind(vec![0u8; 32])
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,fragment_id) VALUES($1,0,1,$2)")
        .bind(other)
        .bind(fragment)
        .execute(&pool)
        .await?;
    assert!(available(&mut *pool.acquire().await?, Some(other), &[]).await?);
    sqlx::query("DELETE FROM extents WHERE stream_id=$1")
        .bind(other)
        .execute(&pool)
        .await?;

    let many: Vec<i64> = sqlx::query_scalar("INSERT INTO chunks(storage_id,hash,raw_size,encryption_algorithm,state) SELECT $1,digest('source-check-'||i::text,'sha256'),1,'none','ready' FROM generate_series(1,4097) i RETURNING id")
        .bind(storage).fetch_all(&pool).await?;
    sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,location_id,storage_id,standalone_size,standalone_compressed,state,encryption_algorithm,created_at) SELECT current_encoding_id,id,uuidv7(),storage_id,1,false,'ready','none',now() FROM chunks WHERE id=ANY($1)")
        .bind(&many).execute(&pool).await?;
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) SELECT $1,row_number() OVER(ORDER BY id)-1,1,id FROM chunks WHERE id=ANY($2)")
        .bind(other).bind(&many).execute(&pool).await?;
    for mode in [
        "SET LOCAL plan_cache_mode=force_generic_plan",
        "SET LOCAL plan_cache_mode=force_custom_plan",
    ] {
        let mut tx = pool.begin().await?;
        sqlx::query(mode).execute(&mut *tx).await?;
        assert!(available(&mut tx, Some(other), &[]).await?);
        assert!(available(&mut tx, None, &many).await?);
        tx.rollback().await?;
    }
    sqlx::query("UPDATE chunk_encodings SET quarantined_at=now(),quarantine_code='hash_mismatch' WHERE chunk_id=$1")
        .bind(many.last().unwrap()).execute(&pool).await?;
    assert!(!available(&mut *pool.acquire().await?, Some(other), &[]).await?);
    assert!(!available(&mut *pool.acquire().await?, None, &many).await?);
    Ok(())
}
