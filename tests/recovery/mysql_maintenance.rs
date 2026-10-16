use super::{Backend, cache_tests, connection, queries};
use crate::authorization::Principal;
use crate::config::Config;
use crate::model::bucket::CreateBucket;
use crate::model::maintenance::MaintenanceStore;
use crate::model::{
    AccountStore, BucketStore, ChunkStore, PhysicalStore, QuotaStore, StreamStore, StreamTail,
    TaskControlStore,
};
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use serde_json::json;
use uuid::Uuid;

async fn history(backend: &Backend, section: &str, batch: i64) -> Result<u64> {
    backend
        .cleanup_history(section, 0., batch, &[], 1000)
        .await
        .with_context(|| format!("cleanup {section}"))
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_maintenance_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    let admin = backend.initialize_admin("admin", "password", false).await?;
    let storage = super::storage_tests::fixture(&backend).await?;
    let bucket = BucketStore::create(
        &backend,
        &Principal::Local,
        CreateBucket {
            name: "maintenance".into(),
            project_id: None,
            storage_id: Some(storage),
        },
    )
    .await?
    .id;
    for section in [
        "maintenance_previews",
        "media_operations",
        "audit_events",
        "chunks",
        "packs",
        "chunk_encodings",
        "uploads",
        "integrity_issues",
        "tasks",
        "sessions",
        "refresh_tokens",
    ] {
        assert_eq!(
            backend
                .cleanup_history(section, 86400., 10, &[], 1000)
                .await?,
            0,
            "{section}"
        );
    }
    assert!(history(&backend, "invalid", 1).await.is_err());
    assert_eq!(backend.storages().await?, vec![storage]);
    let stream = cache_tests::stream(&backend, bucket, "abandoned").await?;
    backend.reserve(stream, 72, false, None).await?;
    let mut chunks = Vec::new();
    for n in 0..2u8 {
        let chunk = cache_tests::chunk(&backend, storage, stream, n + 1).await?;
        backend.register_encoding(&chunk).await?;
        ChunkStore::complete(&backend, stream, i64::from(n) * 32, &chunk).await?;
        chunks.push(chunk);
    }
    let fragment = Uuid::new_v4();
    backend
        .append_tail(StreamTail {
            fragment,
            stream,
            offset: 64,
            size: 8,
            hash: [9; 32],
            staging_storage: None,
            staging_key: None,
        })
        .await?;
    assert!(backend.fragment_exists(fragment).await?);
    let recovery = backend.recovery_fragments(Uuid::nil()).await?;
    assert_eq!(recovery.len(), 1);
    assert_eq!(
        (recovery[0].0, recovery[0].1, recovery[0].2.clone()),
        (fragment, 8, vec![9; 32])
    );
    assert!(recovery[0].3.is_none() && recovery[0].4.is_none());
    assert!(backend.recovery_fragments(fragment).await?.is_empty());
    assert!(backend.claim_fragment(fragment).await?.is_none());
    assert!(
        backend
            .stale_streams(&[stream], 10, None, None)
            .await?
            .is_empty()
    );
    assert!(
        !backend
            .claim_stream(stream, Some(Uuid::new_v4()), None)
            .await?
    );
    assert_eq!(backend.cleanup_stream(stream, 10).await?, 0);
    let task = TaskControlStore::enqueue(&backend, "pack", None, json!({})).await?;
    let token = Uuid::new_v4();
    queries::claim_task(task, token).execute(db).await?;
    sqlx::query("UPDATE streams SET owner_task_id=?,owner_execution_token=? WHERE id=?")
        .bind(task)
        .bind(token)
        .bind(stream)
        .execute(db)
        .await?;
    assert!(!backend.claim_stream(stream, None, None).await?);
    let pack = cache_tests::pack(&backend, storage, task).await?;
    for (ordinal, chunk) in chunks.iter().enumerate() {
        sqlx::query(
            "INSERT INTO pack_members(pack_id,ordinal,chunk_id,offset_bytes) VALUES(?,?,?,?)",
        )
        .bind(pack)
        .bind(ordinal as i32)
        .bind(chunk.id)
        .bind(ordinal as i64 * 32)
        .execute(db)
        .await?;
        sqlx::query("UPDATE chunks SET pack_id=? WHERE id=?")
            .bind(pack)
            .bind(chunk.id)
            .execute(db)
            .await?;
    }
    sqlx::query("UPDATE packs SET state='ready',stored_size=64 WHERE id=?")
        .bind(pack)
        .execute(db)
        .await?;
    let interrupted_pack = cache_tests::pack(&backend, storage, task).await?;
    let incomplete = cache_tests::chunk(&backend, storage, stream, 20).await?;
    backend.register_encoding(&incomplete).await?;
    sqlx::query("INSERT INTO pack_inputs(task_id,chunk_id) VALUES(?,?)")
        .bind(task)
        .bind(chunks[0].id)
        .execute(db)
        .await?;
    let upload = Uuid::new_v4();
    sqlx::query("INSERT INTO uploads(id,bucket_id,object_key,access_key,state,output_stream,durable_at) VALUES(?,?,'completed','fixture','completed',?,CURRENT_TIMESTAMP(6))").bind(upload).bind(bucket).bind(stream).execute(db).await?;
    let pending = Uuid::new_v4();
    sqlx::query("INSERT INTO uploads(id,bucket_id,object_key,access_key,state,touched_at) VALUES(?,?,'pending','fixture','completing','2000-01-01')").bind(pending).bind(bucket).execute(db).await?;
    sqlx::query("INSERT INTO parts(upload_id,part_number,stream_id,write_token) VALUES(?,1,?,?)")
        .bind(pending)
        .bind(stream)
        .bind(stream)
        .execute(db)
        .await?;
    assert_eq!(backend.fragment_uploads(fragment).await?, vec![pending]);
    sqlx::query("DELETE FROM parts WHERE upload_id=?")
        .bind(pending)
        .execute(db)
        .await?;
    assert!(!backend.upload_expired(pending, 60., None).await?);
    let gc = TaskControlStore::enqueue(&backend, "gc", None, json!({})).await?;
    queries::claim_task(gc, Uuid::new_v4()).execute(db).await?;
    backend.recover(true).await?;
    assert_eq!(backend.raw(task).await?.unwrap()["state"], "paused");
    assert_eq!(backend.raw(gc).await?.unwrap()["state"], "queued");
    assert!(backend.raw(task).await?.unwrap()["execution_token"].is_null());
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM chunks WHERE id=?")
            .bind(incomplete.id)
            .fetch_one(db)
            .await?,
        "failed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=?")
            .bind(interrupted_pack)
            .fetch_one(db)
            .await?,
        "retired"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pack_inputs")
            .fetch_one(db)
            .await?,
        0
    );
    assert!(backend.upload_expired(pending, 60., Some(storage)).await?);
    assert!(
        !backend
            .upload_expired(pending, 60., Some(Uuid::new_v4()))
            .await?
    );
    assert_eq!(
        backend.expired_uploads(60., 1, Some(storage)).await?,
        vec![pending]
    );
    assert!(!backend.claim_stream(stream, None, Some(86400.)).await?);
    let mut held = db.begin().await?;
    sqlx::query("SELECT id FROM streams WHERE id=? FOR UPDATE")
        .bind(stream)
        .execute(&mut *held)
        .await?;
    assert!(!backend.claim_stream(stream, None, None).await?);
    held.rollback().await?;
    assert!(backend.claim_stream(stream, Some(storage), None).await?);
    assert_eq!(backend.cleanup_stream(stream, 1).await?, 2);
    let quota = backend.read(&Principal::Local, "bucket", bucket).await?;
    assert_eq!(
        (quota.reserved_bytes, quota.inflight_bytes),
        (Some("0".into()), Some("0".into()))
    );
    assert_eq!(backend.extents(stream, 0, 100).await?.len(), 2);
    assert_eq!(backend.cleanup_stream(stream, 1).await?, 1);
    assert_eq!(backend.cleanup_stream(stream, 1).await?, 1);
    assert_eq!(
        backend
            .stale_fragments(10, &[stream], Some(storage))
            .await?,
        vec![fragment]
    );
    let deletion = backend.claim_fragment(fragment).await?.unwrap();
    assert!(deletion.sealed && deletion.storage.is_none() && deletion.key.is_none());
    assert!(backend.claim_fragment(fragment).await?.is_none());
    drop(deletion);
    let deletion = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let Some(deletion) = backend.claim_fragment(fragment).await? {
                break Ok::<_, anyhow::Error>(deletion);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;
    deletion.deletion.finish().await?;
    assert!(!backend.fragment_exists(fragment).await?);
    assert_eq!(backend.cleanup_stream(stream, 1).await?, 1);
    assert_eq!(backend.cleanup_stream(stream, 1).await?, 0);
    assert!(
        sqlx::query_scalar::<_, Option<Uuid>>("SELECT output_stream FROM uploads WHERE id=?")
            .bind(upload)
            .fetch_one(db)
            .await?
            .is_none()
    );
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT durable_at IS NOT NULL FROM uploads WHERE id=?")
            .bind(upload)
            .fetch_one(db)
            .await?
    );
    assert!(
        backend
            .chunk_candidates(Uuid::new_v4(), 0., 10)
            .await?
            .is_empty()
    );
    assert_eq!(
        backend.chunk_candidates(storage, 86400., 10).await?.len(),
        0
    );
    assert_eq!(backend.chunk_candidates(storage, 0., 1).await?.len(), 1);
    for chunk in &chunks {
        let locations = backend
            .retire_chunk(chunk.id, storage, 0., &[])
            .await?
            .unwrap();
        assert!(locations.contains(&chunk.location_id));
        backend.delete_chunk(chunk.id, storage).await?;
    }
    backend.retire_packs(storage).await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=?")
            .bind(pack)
            .fetch_one(db)
            .await?,
        "retired"
    );
    assert_eq!(history(&backend, "chunks", 10).await?, 0);
    let physical = backend
        .physical_candidates(false, 0., 10, 86400., storage)
        .await?;
    assert!(!physical.iter().any(|r| r.0 == chunks[0].encoding_id));
    sqlx::query("UPDATE chunk_encodings SET stored_at=NULL WHERE encoding_id=?")
        .bind(chunks[0].encoding_id)
        .execute(db)
        .await?;
    let physical = backend
        .physical_candidates(false, 0., 10, 86400., storage)
        .await?;
    assert!(physical.iter().any(|r| r.0 == chunks[0].encoding_id && r.2));
    backend
        .physical_age(false, chunks[0].encoding_id, Utc::now())
        .await?;
    backend
        .physical_age(
            false,
            chunks[0].encoding_id,
            Utc::now() - Duration::days(10),
        )
        .await?;
    assert!(
        !backend
            .claim_physical(false, chunks[0].encoding_id, 0., 86400.)
            .await?
    );
    for chunk in &chunks {
        assert!(
            backend
                .claim_physical(false, chunk.encoding_id, 0., 0.)
                .await?
        );
        backend.delete_physical(false, chunk.encoding_id).await?;
    }
    assert_eq!(history(&backend, "chunks", 10).await?, 0);
    backend
        .physical_age(true, pack, Utc::now() - Duration::days(10))
        .await?;
    assert!(
        backend
            .physical_candidates(true, 0., 10, 86400., storage)
            .await?
            .iter()
            .any(|r| r.0 == pack && !r.2)
    );
    assert!(backend.claim_physical(true, pack, 0., 86400.).await?);
    backend.delete_physical(true, pack).await?;
    assert_eq!(history(&backend, "packs", 1).await?, 1);
    assert_eq!(history(&backend, "chunks", 1).await?, 1);
    assert_eq!(history(&backend, "chunks", 10).await?, 1);
    assert_eq!(history(&backend, "chunk_encodings", 10).await?, 0);
    assert_eq!(
        backend
            .cleanup_history("uploads", 0., 10, &[upload], 1000)
            .await?,
        0
    );
    assert_eq!(history(&backend, "uploads", 10).await?, 1);
    assert!(!backend.gc_status().await?.0.is_empty());
    assert!(!backend.gc_paused().await?);
    backend.mode(true).await?;
    assert!(backend.gc_paused().await?);
    backend.mode(false).await?;

    let cache_stream = cache_tests::stream(&backend, bucket, "cache").await?;
    let cached = cache_tests::chunk(&backend, storage, cache_stream, 30).await?;
    cache_tests::stage(&backend, &cached, cache_stream, 0, true).await?;
    backend.recover(false).await?;
    assert!(
        !backend
            .chunk_candidates(storage, 0., 50)
            .await?
            .iter()
            .any(|r| r.0 == cached.id)
    );
    assert!(
        backend
            .retire_chunk(cached.id, storage, 0., &[])
            .await?
            .is_none()
    );
    let live = cache_tests::stream(&backend, bucket, "live").await?;
    let orphan = cache_tests::chunk(&backend, storage, live, 31).await?;
    sqlx::query("UPDATE chunks SET state='failed',unreferenced_at='2000-01-01' WHERE id=?")
        .bind(orphan.id)
        .execute(db)
        .await?;
    assert!(
        backend
            .retire_chunk(orphan.id, storage, 0., &[live])
            .await?
            .is_none()
    );
    sqlx::query(
        "UPDATE chunks SET state='ready',owner_stream=NULL,unreferenced_at=NULL WHERE id=?",
    )
    .bind(orphan.id)
    .execute(db)
    .await?;
    assert_eq!(backend.mark_orphans().await?, 1);
    assert_eq!(backend.mark_orphans().await?, 0);
    sqlx::query("INSERT INTO pack_inputs(task_id,chunk_id) VALUES(?,?)")
        .bind(task)
        .bind(orphan.id)
        .execute(db)
        .await?;
    assert!(
        backend
            .retire_chunk(orphan.id, storage, 0., &[])
            .await?
            .is_none()
    );
    sqlx::query("DELETE FROM pack_inputs WHERE chunk_id=?")
        .bind(orphan.id)
        .execute(db)
        .await?;
    assert!(
        backend
            .retire_chunk(orphan.id, storage, 0., &[])
            .await?
            .is_some()
    );
    let unsealed = Uuid::new_v4();
    sqlx::query("INSERT INTO fragments(id,owner_stream,sealed,size,hash) VALUES(?,?,FALSE,8,?)")
        .bind(unsealed)
        .bind(live)
        .bind([0u8; 32].as_slice())
        .execute(db)
        .await?;
    assert!(
        !backend
            .stale_fragments(100, &[], Some(storage))
            .await?
            .contains(&unsealed)
    );
    let cleanup = backend.claim_fragment(unsealed).await?.unwrap();
    assert!(!cleanup.sealed && cleanup.owner == Some(live));
    drop(cleanup);
    assert_eq!(
        backend
            .cleanup_tombstones(Some(Uuid::new_v4()), 100)
            .await?,
        0
    );
    assert!(backend.cleanup_tombstones(Some(storage), 1).await? > 0);
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM objects WHERE bucket_id=? AND object_key='live')"
        )
        .bind(bucket)
        .fetch_one(db)
        .await?
    );

    let issue_task = TaskControlStore::enqueue(&backend, "integrity", None, json!({})).await?;
    sqlx::query("UPDATE tasks SET state='completed',updated_at='2000-01-01' WHERE id=?")
        .bind(issue_task)
        .execute(db)
        .await?;
    sqlx::query("INSERT INTO integrity_issues(task_id,subject,code) VALUES(?,'subject','fixture')")
        .bind(issue_task)
        .execute(db)
        .await?;
    assert_eq!(history(&backend, "tasks", 100).await?, 0);
    assert_eq!(history(&backend, "integrity_issues", 1).await?, 1);
    assert_eq!(history(&backend, "tasks", 1).await?, 1);
    for n in 0..2 {
        sqlx::query("INSERT INTO maintenance_previews(id,actor_id,action,parameters,fingerprint,expires_at) VALUES(?,?,'sweep',JSON_OBJECT(),REPEAT('0',64),'2000-01-01')").bind(Uuid::new_v4()).bind(admin).execute(db).await?;
        sqlx::query("INSERT INTO media_operations(id,user_id,client_id,request_hash,created_at) VALUES(?,?,?,?, '2000-01-01')").bind(Uuid::new_v4()).bind(admin).bind(Uuid::new_v4()).bind([n as u8;32].as_slice()).execute(db).await?;
    }
    let mut held = db.begin().await?;
    sqlx::query("SELECT id FROM maintenance_previews FOR UPDATE")
        .execute(&mut *held)
        .await?;
    assert_eq!(history(&backend, "maintenance_previews", 1).await?, 0);
    held.rollback().await?;
    for section in ["maintenance_previews", "media_operations"] {
        assert_eq!(history(&backend, section, 1).await?, 1);
        assert_eq!(history(&backend, section, 10).await?, 1);
    }
    assert_eq!(history(&backend, "audit_events", 1).await?, 1);
    let session = Uuid::new_v4();
    let family = Uuid::new_v4();
    let refresh = Uuid::new_v4();
    sqlx::query("INSERT INTO sessions(id,token_hash,user_id,csrf_hash,expires_at) VALUES(?,?,?,?, '2000-01-01')").bind(session).bind([1u8;32].as_slice()).bind(admin).bind([2u8;32].as_slice()).execute(db).await?;
    sqlx::query("INSERT INTO session_refresh_tokens(id,session_id,family_id,token_hash,expires_at) VALUES(?,?,?,?,CURRENT_TIMESTAMP(6)+INTERVAL 1 DAY)").bind(refresh).bind(session).bind(family).bind([3u8;32].as_slice()).execute(db).await?;
    sqlx::query("INSERT INTO session_refresh_tokens(id,session_id,family_id,token_hash,expires_at,revoked_at,replaced_by) VALUES(?,?,?,?,'2000-01-01','2000-01-01',?)").bind(Uuid::new_v4()).bind(session).bind(family).bind([4u8;32].as_slice()).bind(refresh).execute(db).await?;
    assert_eq!(history(&backend, "sessions", 10).await?, 0);
    assert_eq!(history(&backend, "refresh_tokens", 10).await?, 0);
    sqlx::query("UPDATE session_refresh_tokens SET revoked_at='2000-01-01' WHERE id=?")
        .bind(refresh)
        .execute(db)
        .await?;
    assert_eq!(history(&backend, "refresh_tokens", 1).await?, 1);
    assert_eq!(history(&backend, "refresh_tokens", 10).await?, 1);
    assert_eq!(history(&backend, "sessions", 10).await?, 1);
    let mut held = db.acquire().await?;
    sqlx::raw_sql("LOCK TABLES audit_events WRITE")
        .execute(&mut *held)
        .await?;
    let started = std::time::Instant::now();
    let error = backend
        .cleanup_history("audit_events", 0., 1, &[], 50)
        .await
        .unwrap_err();
    assert_eq!(
        crate::model::error(&error),
        Some(crate::model::Error::QueryCancelled)
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(20),
            backend.cleanup_history("audit_events", 0., 1, &[], 1000)
        )
        .await
        .is_err()
    );
    let available: i32 = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        sqlx::query_scalar("SELECT 7").fetch_one(db),
    )
    .await??;
    assert_eq!(available, 7);
    sqlx::raw_sql("UNLOCK TABLES").execute(&mut *held).await?;
    drop(held);
    history(&backend, "audit_events", 1).await?;
    let mut connections = Vec::new();
    for _ in 0..budget.database_connections {
        connections.push(db.acquire().await?);
    }
    let error = backend
        .cleanup_history("audit_events", 0., 1, &[], 20)
        .await
        .unwrap_err();
    assert_eq!(
        crate::model::error(&error),
        Some(crate::model::Error::QueryCancelled)
    );
    drop(connections);
    backend.pool.close().await;
    Ok(())
}
