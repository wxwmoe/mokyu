use super::connection;
use crate::authorization::Principal;
use crate::config::Config;
use crate::content::codec;
use crate::model::key::ManagedKeyCreate;
use crate::model::runtime::RuntimeStore;
use crate::model::{ManagedKeyStore, StorageRecordStore};
use anyhow::Result;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_key_and_content_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    let state = backend.initialize(false).await?;
    assert_eq!((state.mode.as_str(), state.mode_revision), ("single", 0));
    assert!(!state.maintenance && !state.gc_paused && !state.pack_creation_paused);
    assert!(backend.initialize(true).await?.maintenance);
    assert!(backend.initialize(false).await?.maintenance);
    backend.health().await?;
    assert!(backend.pool().0 > 0);
    assert!(!backend.pending_uploads().await?);

    let storage = super::storage_tests::fixture(&backend).await?;
    let key = Uuid::new_v4();
    let bytes = [23u8; 32];
    let input = || -> Result<ManagedKeyCreate> {
        Ok(ManagedKeyCreate {
            id: key,
            name: "  content-key  ".into(),
            purpose: "block".into(),
            algorithm: "aes-256-gcm".into(),
            protected: codec::protect(
                &bytes,
                &secrets.credential_key,
                format!("managed-key-{key}").as_bytes(),
            )?,
            fingerprint: *blake3::hash(&bytes).as_bytes(),
        })
    };
    assert!(
        crate::model::audit::scope(-1, backend.create(&Principal::Local, input()?))
            .await
            .is_err()
    );
    assert!(backend.resolve_block("content-key").await?.is_none());
    let view = backend.create(&Principal::Local, input()?).await?;
    assert_eq!(view.name, "content-key");
    assert_eq!(view.fingerprint, blake3::hash(&bytes).to_hex().as_str());
    assert!(!view.in_use && view.revoked_at.is_none());
    assert!(backend.create(&Principal::Local, input()?).await.is_err());
    assert_eq!(backend.active_block(key).await?.unwrap().1, "aes-256-gcm");
    assert!(backend.resolve_block("Content-key").await?.is_none());
    assert!(backend.resolve_block("content-key ").await?.is_none());
    backend.ensure_loaded(&secrets, "content-key").await?;
    assert_eq!(
        codec::unprotect_bytes(
            &backend.material("content-key").await?.unwrap().encrypted,
            &secrets.credential_key,
            format!("managed-key-{key}").as_bytes()
        )?,
        bytes
    );

    backend.current_key("fixture", Some("content-key")).await?;
    assert_eq!(backend.for_storage(storage).await?.unwrap().id, key);
    assert!(ManagedKeyStore::list(&backend, &Principal::Local).await?[0].in_use);
    assert!(backend.revoke(&Principal::Local, key).await.is_err());
    backend.current_key("fixture", None).await?;

    let chunk = sqlx::query("INSERT INTO chunks(storage_id,hash,raw_size,encryption_algorithm,block_key_id,state) VALUES(?,?,32,'aes-256-gcm',?,'preparing')").bind(storage).bind(vec![1u8;32]).bind(key).execute(db).await?.last_insert_id() as i64;
    assert!(sqlx::query("INSERT INTO chunks(storage_id,hash,raw_size,encryption_algorithm,state) VALUES(?,?,32,'none','preparing')").bind(storage).bind(vec![1u8;32]).execute(db).await.is_err());
    let encoding = sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,state,encryption_algorithm,block_key_id,encoding_fingerprint) VALUES(?,?,?,'uploading','aes-256-gcm',?,?)").bind(chunk).bind(Uuid::new_v4()).bind(storage).bind(key).bind(vec![2u8;32]).execute(db).await?.last_insert_id() as i64;
    sqlx::query("UPDATE chunks SET current_encoding_id=? WHERE id=?")
        .bind(encoding)
        .bind(chunk)
        .execute(db)
        .await?;
    let nonce_id = backend
        .reserve_nonce(key, crate::content::nonce::CHUNK, 1)
        .await?
        .0;
    sqlx::query("UPDATE chunk_encodings SET nonce=?,nonce_id=? WHERE encoding_id=?")
        .bind(
            codec::typed_nonce(chrono::Utc::now(), nonce_id, crate::content::nonce::CHUNK)?
                .to_vec(),
        )
        .bind(nonce_id)
        .bind(encoding)
        .execute(db)
        .await?;
    for state in ["uploading", "ready", "retired", "deleting"] {
        sqlx::query("UPDATE chunk_encodings SET state=? WHERE encoding_id=?")
            .bind(state)
            .bind(encoding)
            .execute(db)
            .await?;
        assert!(backend.revoke(&Principal::Local, key).await.is_err());
    }
    sqlx::query("UPDATE chunk_encodings SET state='ready' WHERE encoding_id=?")
        .bind(encoding)
        .execute(db)
        .await?;
    assert!(sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,state,encryption_algorithm,encoding_fingerprint) VALUES(?,?,?,'ready','none',?)").bind(chunk).bind(Uuid::new_v4()).bind(storage).bind(vec![3u8;32]).execute(db).await.is_err());
    sqlx::query("UPDATE chunk_encodings SET state='deleted' WHERE encoding_id=?")
        .bind(encoding)
        .execute(db)
        .await?;
    assert!(!ManagedKeyStore::list(&backend, &Principal::Local).await?[0].in_use);
    sqlx::query("INSERT INTO pending_uploads(chunk_id,offset_bytes,staged_size,next_retry_at) VALUES(?,0,32,CURRENT_TIMESTAMP(6))").bind(chunk).execute(db).await?;
    assert!(backend.pending_uploads().await?);
    assert!(backend.revoke(&Principal::Local, key).await.is_err());
    sqlx::query("DELETE FROM pending_uploads WHERE chunk_id=?")
        .bind(chunk)
        .execute(db)
        .await?;

    let pack = sqlx::query("INSERT INTO packs(location_id,storage_id,encryption_algorithm,block_key_id,raw_size,member_count,state,pack_fingerprint) VALUES(?,?,'aes-256-gcm',?,64,2,'preparing',?)").bind(Uuid::new_v4()).bind(storage).bind(key).bind(vec![4u8;32]).execute(db).await?.last_insert_id() as i64;
    assert!(backend.revoke(&Principal::Local, key).await.is_err());
    sqlx::query("UPDATE packs SET state='deleted' WHERE id=?")
        .bind(pack)
        .execute(db)
        .await?;
    let task = Uuid::new_v4();
    sqlx::query("INSERT INTO tasks(id,task_type,state,detail) VALUES(?,'rewrite','queued',?)")
        .bind(task)
        .bind(serde_json::json!({"target_key_id":key}))
        .execute(db)
        .await?;
    for state in ["queued", "running", "paused", "failed"] {
        sqlx::query("UPDATE tasks SET state=? WHERE id=?")
            .bind(state)
            .bind(task)
            .execute(db)
            .await?;
        assert!(backend.revoke(&Principal::Local, key).await.is_err());
    }
    sqlx::query("UPDATE tasks SET state='completed' WHERE id=?")
        .bind(task)
        .execute(db)
        .await?;
    assert!(
        crate::model::audit::scope(-1, backend.revoke(&Principal::Local, key))
            .await
            .is_err()
    );
    assert!(backend.material("content-key").await?.is_some());
    backend.revoke(&Principal::Local, key).await?;
    backend.revoke(&Principal::Local, key).await?;
    assert!(backend.resolve_block("content-key").await?.is_none());
    assert!(backend.active_block(key).await?.is_none());
    assert!(backend.material("content-key").await?.is_none());
    assert!(
        ManagedKeyStore::list(&backend, &Principal::Local).await?[0]
            .revoked_at
            .is_some()
    );

    // Deleted physical identities must not be reassigned to new sources.
    sqlx::query("DELETE FROM chunk_encodings WHERE encoding_id=?")
        .bind(encoding)
        .execute(db)
        .await?;
    let next = sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,state,encryption_algorithm,encoding_fingerprint) VALUES(?,?,?,'ready','none',?)").bind(chunk).bind(Uuid::new_v4()).bind(storage).bind(vec![5u8;32]).execute(db).await?.last_insert_id() as i64;
    assert!(next > encoding);
    assert!(sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,state,encryption_algorithm,encoding_fingerprint) VALUES(?,?,?,'uploading','none',?)").bind(chunk).bind(Uuid::new_v4()).bind(Uuid::new_v4()).bind(vec![5u8;32]).execute(db).await.is_err());
    sqlx::query("UPDATE chunks SET state='failed' WHERE id=?")
        .bind(chunk)
        .execute(db)
        .await?;
    sqlx::query("INSERT INTO chunks(storage_id,hash,raw_size,encryption_algorithm,state) VALUES(?,?,32,'none','preparing')").bind(storage).bind(vec![1u8;32]).execute(db).await?;
    backend.check_owner().await?;
    backend.pool.close().await;
    Ok(())
}
