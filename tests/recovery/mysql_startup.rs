use super::{connection, storage_tests};
use crate::authorization::Principal;
use crate::config::Config;
use crate::content::codec;
use crate::model::key::ManagedKeyCreate;
use crate::model::{ManagedKeyStore, StorageRecordStore};
use anyhow::Result;
use sqlx::{Connection, MySqlConnection, mysql::MySqlConnectOptions};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_production_startup_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let path = std::path::Path::new(&path);
    let (config, mut secrets, budget) = Config::load(path)?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let block = Uuid::new_v4();
    let signing = Uuid::new_v4();
    let revoked = Uuid::new_v4();
    for (id, name, purpose, algorithm, byte) in [
        (block, "startup-block", "block", "aes-256-gcm", 23u8),
        (signing, "startup-signing", "signing", "raw", 24),
        (revoked, "startup-revoked", "block", "chacha20-poly1305", 25),
    ] {
        backend
            .create(
                &Principal::Local,
                ManagedKeyCreate {
                    id,
                    name: name.into(),
                    purpose: purpose.into(),
                    algorithm: algorithm.into(),
                    protected: codec::protect(
                        &[byte; 32],
                        &secrets.credential_key,
                        format!("managed-key-{id}").as_bytes(),
                    )?,
                    fingerprint: *blake3::hash(&[byte; 32]).as_bytes(),
                },
            )
            .await?;
    }
    backend.revoke(&Principal::Local, revoked).await?;
    let storage = storage_tests::fixture_named(&backend, "startup").await?;
    backend
        .current_key("startup", Some("startup-block"))
        .await?;
    let chunk=sqlx::query("INSERT INTO chunks(storage_id,hash,raw_size,encryption_algorithm,block_key_id,state,current_encoding_id) VALUES(?,?,32,'aes-256-gcm',?,'ready',1)").bind(storage).bind([0u8;32].as_slice()).bind(block).execute(&backend.pool).await?.last_insert_id() as i64;
    sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,standalone_size,state,encryption_algorithm,block_key_id,encoding_fingerprint) VALUES(?,?,?,48,'ready','aes-256-gcm',?,?)").bind(chunk).bind(Uuid::new_v4()).bind(storage).bind(block).bind([0u8;32].as_slice()).execute(&backend.pool).await?;
    backend.pool.close().await;
    backend.owner.into_inner().close().await?;

    let stale = Uuid::new_v4();
    secrets.publish_key_with_id(
        Some(stale),
        "stale".into(),
        codec::Key::new("aes-256-gcm", [26; 32])?,
    )?;
    let (stores, mut owner) =
        crate::model::connect(&config, &mut secrets, &budget, "single").await?;
    assert!(stores.cluster.is_none());
    assert_eq!(stores.runtime.initialize(false).await?.mode, "single");
    owner.check().await?;
    assert_eq!(secrets.key_for_id(Some(block)).unwrap().material, [23; 32]);
    assert!(secrets.key_for_id(Some(signing)).is_none());
    assert!(secrets.key_for_id(Some(revoked)).is_none());
    assert!(secrets.key_for_id(Some(stale)).is_none() && secrets.key("stale").is_none());
    assert_eq!(
        stores.managed_keys.for_storage(storage).await?.unwrap().id,
        block
    );
    let (_, mut other, _) = Config::load(path)?;
    assert!(
        crate::model::connect(&config, &mut other, &budget, "single")
            .await
            .is_err()
    );
    for role in ["control", "data", "worker"] {
        assert!(
            crate::model::connect(&config, &mut other, &budget, role)
                .await
                .is_err()
        );
    }

    let options = MySqlConnectOptions::new()
        .host(&config.database.host)
        .port(config.database.effective_port().unwrap())
        .database(&config.database.name)
        .username(&config.database.user)
        .password(&secrets.database_password);
    let mut control = MySqlConnection::connect_with(&options).await?;
    let runtime: u64 = sqlx::query_scalar(
        "SELECT CAST(IS_USED_LOCK(SHA2(CONCAT('mokyu:runtime:',DATABASE()),256)) AS UNSIGNED)",
    )
    .fetch_one(&mut control)
    .await?;
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("KILL CONNECTION {runtime}")))
        .execute(&mut control)
        .await?;
    assert!(owner.check().await.is_err());
    drop(owner);
    drop(stores);

    let (_, mut wrong, _) = Config::load(path)?;
    wrong.credential_key = codec::Key::new("aes-256-gcm", [28; 32])?;
    assert!(
        crate::model::connect(&config, &mut wrong, &budget, "single")
            .await
            .is_err()
    );
    let encrypted: Vec<u8> =
        sqlx::query_scalar("SELECT secret_encrypted FROM managed_keys WHERE id=?")
            .bind(block)
            .fetch_one(&mut control)
            .await?;
    sqlx::query("UPDATE managed_keys SET fingerprint=? WHERE id=?")
        .bind([0u8; 32].as_slice())
        .bind(block)
        .execute(&mut control)
        .await?;
    let (_, mut fresh, _) = Config::load(path)?;
    let error = crate::model::connect(&config, &mut fresh, &budget, "single")
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("fingerprint mismatch"));
    sqlx::query("UPDATE managed_keys SET fingerprint=?,secret_encrypted=NULL WHERE id=?")
        .bind(blake3::hash(&[23u8; 32]).as_bytes().as_slice())
        .bind(block)
        .execute(&mut control)
        .await?;
    let error = crate::model::connect(&config, &mut fresh, &budget, "single")
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("missing a historical chunk key"));
    sqlx::query("UPDATE managed_keys SET secret_encrypted=? WHERE id=?")
        .bind(encrypted)
        .bind(block)
        .execute(&mut control)
        .await?;
    let (stores, mut owner) = crate::model::connect(&config, &mut fresh, &budget, "single").await?;
    owner.check().await?;
    assert_eq!(fresh.key_for_id(Some(block)).unwrap().material, [23; 32]);

    // Native server errors remain recognizable through context at the common API boundary.
    let mut waiter = MySqlConnection::connect_with(&options).await?;
    sqlx::query("SET SESSION innodb_lock_wait_timeout=1")
        .execute(&mut waiter)
        .await?;
    let mut tx = control.begin().await?;
    sqlx::query("SELECT id FROM mokyu_meta WHERE id=1 FOR UPDATE")
        .execute(&mut *tx)
        .await?;
    let error = sqlx::query("UPDATE mokyu_meta SET gc_paused=FALSE WHERE id=1")
        .execute(&mut waiter)
        .await
        .unwrap_err();
    assert_eq!(
        crate::model::error(&anyhow::Error::from(error).context("native lock")),
        Some(crate::model::Error::OperationAborted)
    );
    tx.rollback().await?;
    let error=sqlx::query("/*M!100603 SET STATEMENT max_statement_time=0.005 FOR */ WITH RECURSIVE seq AS (SELECT 0 AS n UNION ALL SELECT n+1 FROM seq WHERE n<999) SELECT /*+ MAX_EXECUTION_TIME(5) */ SUM(a.n*b.n+c.n) FROM seq a CROSS JOIN seq b CROSS JOIN seq c").execute(&mut waiter).await.unwrap_err();
    assert_eq!(
        crate::model::error(&anyhow::Error::from(error).context("native timeout")),
        Some(crate::model::Error::QueryCancelled)
    );
    let error = sqlx::query("INSERT INTO projects(id,name) SELECT id,name FROM projects LIMIT 1")
        .execute(&mut waiter)
        .await
        .unwrap_err();
    assert_eq!(crate::model::error(&error.into()), None);
    drop(owner);
    drop(stores);
    control.close().await?;
    waiter.close().await?;
    Ok(())
}
