use super::{connection, queries};
use crate::config::Config;
use anyhow::Result;
use sqlx::Connection;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_SQLITE_CONFIG pointing to a fresh isolated SQLite database"]
async fn sqlite_synchronous_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, mut budget) = Config::load(std::path::Path::new(&path))?;
    budget.database_connections = 4;
    let mut backend = connection::open(&config, &secrets, &budget, "single").await?;
    assert_eq!(backend.database_info.synchronous.as_deref(), Some("full"));
    sqlx::query(
        "INSERT INTO runtime_settings(section,value) VALUES('preserved','{\"saved\":true}')",
    )
    .execute(&backend.write)
    .await?;
    for (name, expected) in [("normal", 1), ("off", 0), ("full", 2)] {
        let previous = queries::synchronous().fetch_one(&backend.write).await?;
        sqlx::query("INSERT INTO runtime_settings(section,value) VALUES('io',?) ON CONFLICT(section) DO UPDATE SET value=excluded.value")
            .bind(serde_json::json!({"sqlite_synchronous": name})).execute(&backend.write).await?;
        assert_eq!(
            queries::synchronous().fetch_one(&backend.write).await?,
            previous
        );
        backend.read.close().await;
        backend.write.close().await;
        drop(backend);
        backend = connection::open(&config, &secrets, &budget, "single").await?;
        assert_eq!(backend.database_info.synchronous.as_deref(), Some(name));
        assert_eq!(backend.database_info.journal_mode.as_deref(), Some("wal"));
        assert_eq!(
            queries::synchronous().fetch_one(&backend.write).await?,
            expected
        );
        let mut held = Vec::new();
        for _ in 0..3 {
            held.push(backend.read.acquire().await?);
        }
        for mut conn in held {
            assert_eq!(
                queries::synchronous().fetch_one(&mut *conn).await?,
                expected
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
                    .fetch_one(&mut *conn)
                    .await?,
                1
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>("PRAGMA query_only")
                    .fetch_one(&mut *conn)
                    .await?,
                1
            );
            conn.close().await?;
        }
        assert_eq!(
            queries::synchronous().fetch_one(&backend.read).await?,
            expected
        );
        let mut tx = backend.write.begin().await?;
        sqlx::query("UPDATE runtime_settings SET value='{}' WHERE section='preserved'")
            .execute(&mut *tx)
            .await?;
        tx.rollback().await?;
        assert_eq!(
            queries::runtime_setting("preserved")
                .fetch_one(&backend.read)
                .await?,
            serde_json::json!({"saved": true})
        );
    }
    sqlx::query("UPDATE runtime_settings SET value='{\"sqlite_synchronous\":\"invalid\"}' WHERE section='io'")
        .execute(&backend.write).await?;
    backend.read.close().await;
    backend.write.close().await;
    drop(backend);
    assert!(
        connection::open(&config, &secrets, &budget, "single")
            .await
            .is_err()
    );
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(
            config
                .database
                .sqlite_path(&config.instance.data_path)
                .unwrap(),
        )
        .synchronous(sqlx::sqlite::SqliteSynchronous::Full);
    let mut admin = sqlx::SqliteConnection::connect_with(&options).await?;
    assert_eq!(
        queries::runtime_setting("preserved")
            .fetch_one(&mut admin)
            .await?,
        serde_json::json!({"saved": true})
    );
    sqlx::query(
        "UPDATE runtime_settings SET value='{\"sqlite_synchronous\":\"full\"}' WHERE section='io'",
    )
    .execute(&mut admin)
    .await?;
    admin.close().await?;
    budget.database_connections = 1;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    assert!(backend.shared_pool);
    assert_eq!(queries::synchronous().fetch_one(&backend.read).await?, 2);
    backend.read.close().await;
    backend.write.close().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_SQLITE_CONFIG pointing to a fresh isolated SQLite database"]
async fn sqlite_nonce_durability_contract() -> Result<()> {
    use crate::authorization::Principal;
    use crate::content::{codec, nonce};
    use crate::model::ManagedKeyStore;
    use crate::model::key::ManagedKeyCreate;
    use crate::model::runtime::RuntimeStore;
    use crate::model::settings::RuntimeSettingsStore;
    use serde_json::json;
    use uuid::Uuid;

    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let mut backend = connection::open(&config, &secrets, &budget, "single").await?;
    backend.initialize(false).await?;
    backend
        .ensure(&crate::model::runtime_defaults(&config))
        .await?;
    let key = Uuid::new_v4();
    let material = [173u8; 32];
    backend
        .create(
            &Principal::Local,
            ManagedKeyCreate {
                id: key,
                name: "durability-key".into(),
                purpose: "block".into(),
                algorithm: "aes-256-gcm".into(),
                protected: codec::protect(
                    &material,
                    &secrets.credential_key,
                    format!("managed-key-{key}").as_bytes(),
                )?,
                fingerprint: *blake3::hash(&material).as_bytes(),
            },
        )
        .await?;

    backend
        .save(
            &Principal::Local,
            "io",
            None,
            json!({"sqlite_synchronous":"off"}),
        )
        .await?;
    assert!(backend.reserve_nonce(key, nonce::CHUNK, 1).await.is_err());
    assert_eq!(
        queries::nonce_counter(key, nonce::CHUNK)
            .fetch_one(&backend.read)
            .await?,
        0
    );
    backend.read.close().await;
    backend.write.close().await;
    drop(backend);
    backend = connection::open(&config, &secrets, &budget, "single").await?;
    assert_eq!(backend.database_info.synchronous.as_deref(), Some("off"));
    assert!(backend.reserve_nonce(key, nonce::CHUNK, 1).await.is_err());
    backend
        .save(
            &Principal::Local,
            "io",
            None,
            json!({"sqlite_synchronous":"full"}),
        )
        .await?;
    assert!(backend.reserve_nonce(key, nonce::CHUNK, 1).await.is_err());
    backend.read.close().await;
    backend.write.close().await;
    drop(backend);
    backend = connection::open(&config, &secrets, &budget, "single").await?;

    let (setting, reservation) = tokio::join!(
        backend.save(
            &Principal::Local,
            "io",
            None,
            json!({"sqlite_synchronous":"off"})
        ),
        backend.reserve_nonce(key, nonce::CHUNK, 1)
    );
    assert_ne!(
        setting.is_ok(),
        reservation.is_ok(),
        "OFF and a first nonce reservation must exclude each other"
    );
    backend
        .save(
            &Principal::Local,
            "io",
            None,
            json!({"sqlite_synchronous":"normal"}),
        )
        .await?;
    backend.read.close().await;
    backend.write.close().await;
    drop(backend);
    backend = connection::open(&config, &secrets, &budget, "single").await?;
    assert_eq!(backend.database_info.synchronous.as_deref(), Some("normal"));
    let previous = queries::nonce_counter(key, nonce::CHUNK)
        .fetch_one(&backend.read)
        .await?;
    assert_eq!(
        backend.reserve_nonce(key, nonce::CHUNK, 1).await?,
        (previous + 1, previous + 64)
    );
    assert_eq!(backend.reserve_nonce(key, nonce::PACK, 1).await?, (1, 64));
    assert_eq!(queries::synchronous().fetch_one(&backend.write).await?, 1);
    assert_eq!(queries::synchronous().fetch_one(&backend.read).await?, 1);
    assert!(
        backend
            .reserve_nonce(Uuid::new_v4(), nonce::CHUNK, 1)
            .await
            .is_err()
    );
    assert_eq!(queries::synchronous().fetch_one(&backend.write).await?, 1);
    let (first, second) = tokio::join!(
        backend.reserve_nonce(key, nonce::CHUNK, 1),
        backend.reserve_nonce(key, nonce::CHUNK, 1)
    );
    let (first, second) = (first?, second?);
    assert!(first.1 < second.0 || second.1 < first.0);

    #[cfg(feature = "fault-injection")]
    for (boundary, committed) in [("nonce-before-commit", false), ("nonce-after-commit", true)] {
        let dir = std::path::PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?);
        std::fs::create_dir_all(&dir)?;
        let fault = dir.join(boundary);
        std::fs::write(&fault, b"pause")?;
        let previous = queries::nonce_counter(key, nonce::CHUNK)
            .fetch_one(&backend.read)
            .await?;
        let mut pending = Box::pin(backend.reserve_nonce(key, nonce::CHUNK, 1));
        tokio::select! {
            result = &mut pending => panic!("nonce reservation did not pause: {result:?}"),
            result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while !fault.with_extension("hit").exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }) => { result?; }
        }
        drop(pending);
        std::fs::remove_file(fault)?;
        assert_eq!(queries::synchronous().fetch_one(&backend.write).await?, 1);
        assert_eq!(
            queries::nonce_counter(key, nonce::CHUNK)
                .fetch_one(&backend.read)
                .await?,
            previous + if committed { 64 } else { 0 }
        );
    }
    let revision = backend.row("io").await?.revision;
    let error = backend
        .save(
            &Principal::Local,
            "io",
            Some(revision),
            json!({"sqlite_synchronous":"off"}),
        )
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("nonce reservations"));
    assert_eq!(backend.row("io").await?.revision, revision);
    let high = queries::nonce_counter(key, nonce::CHUNK)
        .fetch_one(&backend.read)
        .await?;
    backend.read.close().await;
    backend.write.close().await;
    drop(backend);
    backend = connection::open(&config, &secrets, &budget, "single").await?;
    assert_eq!(
        backend.reserve_nonce(key, nonce::CHUNK, 1).await?.0,
        high + 1
    );
    backend.revoke(&Principal::Local, key).await?;
    assert!(
        backend
            .save(
                &Principal::Local,
                "io",
                None,
                json!({"sqlite_synchronous":"off"})
            )
            .await
            .is_err()
    );
    sqlx::query(
        "UPDATE runtime_settings SET value='{\"sqlite_synchronous\":\"off\"}' WHERE section='io'",
    )
    .execute(&backend.write)
    .await?;
    backend.read.close().await;
    backend.write.close().await;
    drop(backend);
    let result = connection::open(&config, &secrets, &budget, "single").await;
    assert!(result.is_err_and(|error| error.to_string().contains("nonce reservations")));
    Ok(())
}
