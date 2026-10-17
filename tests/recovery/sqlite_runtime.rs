use super::{Backend, connection, queries};
use crate::config::Config;
use anyhow::{Context, Result};
use sqlx::{Connection, SqliteConnection, migrate::Migrate, sqlite::SqliteConnectOptions};
use std::{
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

async fn close(backend: Backend) {
    backend.read.close().await;
    backend.write.close().await;
}

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_SQLITE_CONFIG pointing to a fresh isolated directory"]
async fn sqlite_runtime_and_migration_recovery() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let config_file = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, mut budget) = Config::load(Path::new(&config_file))?;
    let path = config
        .database
        .sqlite_path(&config.instance.data_path)
        .unwrap();
    let marker = config.instance.data_path.join("sqlite-crash-ready");
    if std::env::var_os("MOKYU_TEST_SQLITE_CHILD").is_some() {
        let backend = connection::open(&config, &secrets, &budget, "single").await?;
        sqlx::query("INSERT INTO runtime_settings(section,value) VALUES('committed','89')")
            .execute(&backend.write)
            .await?;
        let mut tx = backend.write.begin_with(queries::BEGIN_WRITE).await?;
        sqlx::query("INSERT INTO runtime_settings(section,value) VALUES('uncommitted','90')")
            .execute(&mut *tx)
            .await?;
        std::fs::write(&marker, b"ready")?;
        std::future::pending::<()>().await;
        tx.rollback().await?;
        return Ok(());
    }
    assert!(!path.exists(), "fixture database must be new");
    assert!(!marker.exists());
    for role in ["control", "data", "worker"] {
        assert!(
            connection::open(&config, &secrets, &budget, role)
                .await
                .is_err()
        );
    }
    assert!(!path.exists());
    std::fs::create_dir_all(path.parent().unwrap())?;
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true);
    let mut admin = SqliteConnection::connect_with(&options).await?;
    admin.ensure_migrations_table("_sqlx_migrations").await?;
    sqlx::raw_sql("CREATE TRIGGER test_migration BEFORE INSERT ON _sqlx_migrations BEGIN SELECT RAISE(ABORT,'isolated interruption'); END").execute(&mut admin).await?;
    assert!(
        connection::open(&config, &secrets, &budget, "single")
            .await
            .is_err()
    );
    assert_eq!(
        queries::tables().fetch_all(&mut admin).await?,
        ["_sqlx_migrations"]
    );
    assert!(
        queries::migrations()
            .fetch_all(&mut admin)
            .await?
            .is_empty()
    );
    sqlx::raw_sql("DROP TRIGGER test_migration")
        .execute(&mut admin)
        .await?;
    sqlx::raw_sql("CREATE TRIGGER test_migration BEFORE INSERT ON _sqlx_migrations WHEN NEW.version=6 BEGIN SELECT RAISE(ABORT,'isolated late interruption'); END").execute(&mut admin).await?;
    assert!(
        connection::open(&config, &secrets, &budget, "single")
            .await
            .is_err()
    );
    assert_eq!(queries::schema_version().fetch_one(&mut admin).await?, 5);
    assert_eq!(queries::migrations().fetch_all(&mut admin).await?.len(), 5);
    assert!(
        !queries::tables()
            .fetch_all(&mut admin)
            .await?
            .iter()
            .any(|name| name == "tasks")
    );
    sqlx::raw_sql("DROP TRIGGER test_migration")
        .execute(&mut admin)
        .await?;
    admin.close().await?;

    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    backend.check_owner()?;
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&backend.read)
            .await?
            .is_empty()
    );
    assert_eq!(
        queries::schema_version().fetch_one(&backend.read).await?,
        18
    );
    assert_eq!(
        queries::journal_mode().fetch_one(&backend.read).await?,
        "wal"
    );
    for pool in [&backend.read, &backend.write] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
                .fetch_one(pool)
                .await?,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA synchronous")
                .fetch_one(pool)
                .await?,
            2
        );
    }
    let history = queries::migrations().fetch_all(&backend.read).await?;
    assert!(history.iter().all(|(_, success, _)| *success));
    let deployment: uuid::Uuid = sqlx::query_scalar("SELECT deployment_id FROM mokyu_meta")
        .fetch_one(&backend.read)
        .await?;
    assert!(
        connection::open(&config, &secrets, &budget, "single")
            .await
            .is_err()
    );
    let mut alias = config.clone();
    alias.instance.data_path = config.instance.data_path.join("different-data");
    alias.database.path = Some(path.clone());
    assert!(
        connection::open(&alias, &secrets, &budget, "single")
            .await
            .is_err()
    );
    #[cfg(unix)]
    {
        let link = config.instance.data_path.join("alias.db");
        std::os::unix::fs::symlink(&path, &link)?;
        alias.database.path = Some(link);
        assert!(
            connection::open(&alias, &secrets, &budget, "single")
                .await
                .is_err()
        );
    }
    #[cfg(unix)]
    {
        let hardlink = config.instance.data_path.join("hardlink.db");
        std::fs::hard_link(&path, &hardlink)?;
        assert!(backend.check_owner().is_err());
        alias.database.path = Some(hardlink.clone());
        let error = connection::open(&alias, &secrets, &budget, "single")
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("hard-linked"));
        std::fs::remove_file(hardlink)?;
        backend.check_owner()?;
        let held = path.with_extension("db-held");
        std::fs::rename(&path, &held)?;
        std::fs::File::create_new(&path)?;
        assert!(backend.check_owner().is_err());
        std::fs::remove_file(&path)?;
        std::fs::rename(&held, &path)?;
        backend.check_owner()?;
    }
    assert!(
        sqlx::query("UPDATE mokyu_meta SET gc_paused=2")
            .execute(&backend.write)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("INSERT INTO runtime_settings(section,value) VALUES('invalid','no json')")
            .execute(&backend.write)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE mokyu_meta SET gc_paused=1")
            .execute(&backend.read)
            .await
            .is_err()
    );
    sqlx::query(
        "INSERT INTO runtime_settings(section,value) VALUES('test','{\"preserved\":true}')",
    )
    .execute(&backend.write)
    .await?;

    let mut snapshot = backend.read.begin().await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT revision FROM runtime_settings WHERE section='test'")
            .fetch_one(&mut *snapshot)
            .await?,
        1
    );
    let mut tx = backend.write.begin_with(queries::BEGIN_WRITE).await?;
    sqlx::query("UPDATE runtime_settings SET revision=2 WHERE section='test'")
        .execute(&mut *tx)
        .await?;
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(1),
            sqlx::query_scalar::<_, i64>(
                "SELECT revision FROM runtime_settings WHERE section='test'"
            )
            .fetch_one(&mut *snapshot)
        )
        .await??,
        1
    );
    tx.commit().await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT revision FROM runtime_settings WHERE section='test'")
            .fetch_one(&mut *snapshot)
            .await?,
        1
    );
    snapshot.commit().await?;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let write = backend.write.clone();
        tasks.spawn(async move {
            let mut tx = write.begin_with(queries::BEGIN_WRITE).await?;
            sqlx::query("UPDATE runtime_settings SET revision=revision+1 WHERE section='test'")
                .execute(&mut *tx)
                .await?;
            tx.commit().await
        });
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT revision FROM runtime_settings WHERE section='test'")
            .fetch_one(&backend.read)
            .await?,
        34
    );
    assert!(backend.read.size() + backend.write.size() <= budget.database_connections);
    let mut competing =
        SqliteConnection::connect_with(&options.clone().busy_timeout(Duration::from_millis(50)))
            .await?;
    let tx = backend.write.begin_with(queries::BEGIN_WRITE).await?;
    let busy = sqlx::query("UPDATE mokyu_meta SET gc_paused=1")
        .execute(&mut competing)
        .await
        .unwrap_err();
    assert!(matches!(
        super::error(&busy.into()),
        Some(crate::model::Error::OperationAborted)
    ));
    tx.rollback().await?;
    competing.close().await?;
    close(backend).await;

    budget.database_connections = 1;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    assert_eq!(
        queries::migrations().fetch_all(&backend.read).await?,
        history
    );
    assert_eq!(
        sqlx::query_scalar::<_, uuid::Uuid>("SELECT deployment_id FROM mokyu_meta")
            .fetch_one(&backend.read)
            .await?,
        deployment
    );
    assert_eq!(
        sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT value FROM runtime_settings WHERE section='test'"
        )
        .fetch_one(&backend.read)
        .await?,
        serde_json::json!({"preserved":true})
    );
    sqlx::query("UPDATE runtime_settings SET revision=35 WHERE section='test'")
        .execute(&backend.write)
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT revision FROM runtime_settings WHERE section='test'")
            .fetch_one(&backend.read)
            .await?,
        35
    );
    close(backend).await;

    let mut process = Process(
        Command::new(std::env::current_exe()?)
            .args([
                "sqlite_runtime_and_migration_recovery",
                "--ignored",
                "--nocapture",
            ])
            .env("MOKYU_TEST_SQLITE_CHILD", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        while !marker.exists() {
            anyhow::ensure!(
                process.0.try_wait()?.is_none(),
                "SQLite crash fixture exited early"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("SQLite crash fixture did not reach its transaction")??;
    process.0.kill()?;
    process.0.wait()?;
    assert!(
        path.with_file_name(format!(
            "{}-wal",
            path.file_name().unwrap().to_string_lossy()
        ))
        .metadata()?
        .len()
            > 0
    );
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>("PRAGMA integrity_check")
            .fetch_one(&backend.read)
            .await?,
        "ok"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT value FROM runtime_settings WHERE section='committed'"
        )
        .fetch_one(&backend.read)
        .await?,
        "89"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM runtime_settings WHERE section='uncommitted'"
        )
        .fetch_one(&backend.read)
        .await?,
        0
    );
    close(backend).await;

    let mut admin = SqliteConnection::connect_with(&options).await?;
    for change in [
        "UPDATE _sqlx_migrations SET checksum=zeroblob(48) WHERE version=1",
        "UPDATE _sqlx_migrations SET success=0 WHERE version=1",
        "UPDATE _sqlx_migrations SET version=999 WHERE version=1",
    ] {
        sqlx::query(sqlx::AssertSqlSafe(change))
            .execute(&mut admin)
            .await?;
        assert!(
            connection::open(&config, &secrets, &budget, "single")
                .await
                .is_err()
        );
        sqlx::query(
            "UPDATE _sqlx_migrations SET version=1,checksum=?,success=1 WHERE version IN (1,999)",
        )
        .bind(&history[0].2)
        .execute(&mut admin)
        .await?;
    }
    sqlx::query("UPDATE mokyu_meta SET schema_version=999")
        .execute(&mut admin)
        .await?;
    assert!(
        connection::open(&config, &secrets, &budget, "single")
            .await
            .is_err()
    );
    sqlx::query("UPDATE mokyu_meta SET schema_version=(SELECT MAX(version) FROM _sqlx_migrations)")
        .execute(&mut admin)
        .await?;
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version=1")
        .execute(&mut admin)
        .await?;
    assert!(
        connection::open(&config, &secrets, &budget, "single")
            .await
            .is_err()
    );
    sqlx::query("INSERT INTO _sqlx_migrations(version,description,success,checksum,execution_time) VALUES(1,'runtime',1,?,0)").bind(&history[0].2).execute(&mut admin).await?;
    sqlx::query("UPDATE mokyu_meta SET deployment_key_fingerprint=zeroblob(32)")
        .execute(&mut admin)
        .await?;
    assert!(
        connection::open(&config, &secrets, &budget, "single")
            .await
            .is_err()
    );
    sqlx::query("UPDATE mokyu_meta SET deployment_key_fingerprint=?")
        .bind(
            blake3::hash(&secrets.credential_key.material)
                .as_bytes()
                .as_slice(),
        )
        .execute(&mut admin)
        .await?;
    admin.close().await?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    backend.check_owner()?;
    let replaced = backend.lock_path.with_extension("replaced");
    std::fs::rename(&backend.lock_path, &replaced)?;
    std::fs::write(&backend.lock_path, b"")?;
    assert!(backend.check_owner().is_err());
    close(backend).await;

    let mut foreign = config.clone();
    foreign.database.path = Some(config.instance.data_path.join("foreign.db"));
    let mut connection = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(foreign.database.path.as_ref().unwrap())
            .create_if_missing(true),
    )
    .await?;
    sqlx::query("CREATE TABLE sqlitex_data(id INTEGER PRIMARY KEY)")
        .execute(&mut connection)
        .await?;
    assert!(
        connection::open(&foreign, &secrets, &budget, "single")
            .await
            .is_err()
    );
    assert_eq!(
        queries::tables().fetch_all(&mut connection).await?,
        ["sqlitex_data"]
    );
    connection.close().await?;
    let mut fresh = config.clone();
    fresh.instance.data_path = config.instance.data_path.join("private");
    fresh.database.path = None;
    let backend = connection::open(&fresh, &secrets, &budget, "single").await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for file in [
            fresh.instance.data_path.join("mokyu.db"),
            backend.lock_path.clone(),
        ] {
            assert_eq!(file.metadata()?.permissions().mode() & 0o777, 0o600);
        }
        assert_eq!(
            fresh.instance.data_path.metadata()?.permissions().mode() & 0o777,
            0o700
        );
    }
    close(backend).await;
    Ok(())
}
