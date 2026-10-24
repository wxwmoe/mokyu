use super::{Backend, connection, queries};
use crate::config::Config;
use anyhow::Result;
use sqlx::{Connection, MySqlConnection, mysql::MySqlConnectOptions};

async fn close(backend: Backend) -> Result<()> {
    backend.pool.close().await;
    backend.owner.into_inner().close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_runtime_and_migration_recovery() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let migrations = sqlx::migrate!("./migrations/mysql");
    let latest = migrations.iter().last().unwrap().version;
    let admin_path = std::env::var("MOKYU_TEST_MYSQL_ADMIN_CONFIG")?;
    let (admin_config, admin_secrets, _) = Config::load(std::path::Path::new(&admin_path))?;
    let options = MySqlConnectOptions::new()
        .host(&config.database.host)
        .port(config.database.effective_port().unwrap())
        .database(&config.database.name)
        .username(&admin_config.database.user)
        .password(&admin_secrets.database_password);
    let mut admin = MySqlConnection::connect_with(&options).await?;
    assert!(queries::tables().fetch_all(&mut admin).await?.is_empty());
    for role in ["control", "data", "worker"] {
        assert!(
            connection::open(&config, &secrets, &budget, role)
                .await
                .is_err()
        );
    }
    assert!(queries::tables().fetch_all(&mut admin).await?.is_empty());
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    backend.check_owner().await?;
    assert_eq!(
        i64::from(queries::schema_version().fetch_one(&backend.pool).await?),
        latest
    );
    let deployment: uuid::Uuid =
        sqlx::query_scalar("SELECT deployment_id FROM mokyu_meta WHERE id=1")
            .fetch_one(&backend.pool)
            .await?;
    assert!(
        queries::migrations()
            .fetch_all(&backend.pool)
            .await?
            .iter()
            .all(|row| row.1)
    );
    let (isolation, time_zone, strict): (String, String, bool) = sqlx::query_as("SELECT /*!80000 @@SESSION.transaction_isolation */ /*M!100603 @@SESSION.tx_isolation */,@@SESSION.time_zone,FIND_IN_SET('STRICT_ALL_TABLES',@@SESSION.sql_mode)>0")
        .fetch_one(&backend.pool).await?;
    assert_eq!(
        (isolation.as_str(), time_zone.as_str(), strict),
        ("READ-COMMITTED", "+00:00", true)
    );
    let tables = queries::tables().fetch_all(&backend.pool).await?;
    for excluded in [
        "nodes",
        "stream_leases",
        "storage_permits",
        "storage_rate_state",
        "staging_objects",
    ] {
        assert!(!tables.iter().any(|name| name == excluded));
    }
    assert!(
        connection::open(&config, &secrets, &budget, "single")
            .await
            .is_err()
    );
    let history = queries::migrations().fetch_all(&backend.pool).await?;
    sqlx::query(
        "INSERT INTO runtime_settings(section,value) VALUES('test',JSON_OBJECT('preserved',TRUE))",
    )
    .execute(&backend.pool)
    .await?;
    close(backend).await?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    assert_eq!(
        queries::migrations().fetch_all(&backend.pool).await?,
        history
    );
    assert_eq!(
        sqlx::query_scalar::<_, uuid::Uuid>("SELECT deployment_id FROM mokyu_meta WHERE id=1")
            .fetch_one(&backend.pool)
            .await?,
        deployment
    );
    assert_eq!(
        sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT value FROM runtime_settings WHERE section='test'"
        )
        .fetch_one(&backend.pool)
        .await?,
        serde_json::json!({"preserved": true})
    );
    close(backend).await?;

    sqlx::query("UPDATE _sqlx_migrations SET checksum=REPEAT(CHAR(0),48) WHERE version=1")
        .execute(&mut admin)
        .await?;
    assert!(
        connection::open(&config, &secrets, &budget, "single")
            .await
            .is_err()
    );
    sqlx::query("UPDATE _sqlx_migrations SET checksum=? WHERE version=1")
        .bind(&history[0].2)
        .execute(&mut admin)
        .await?;
    sqlx::query("UPDATE mokyu_meta SET schema_version=999 WHERE id=1")
        .execute(&mut admin)
        .await?;
    assert!(
        connection::open(&config, &secrets, &budget, "single")
            .await
            .is_err()
    );
    sqlx::query("UPDATE mokyu_meta SET schema_version=? WHERE id=1")
        .bind(latest)
        .execute(&mut admin)
        .await?;
    sqlx::query("UPDATE _sqlx_migrations SET version=999 WHERE version=1")
        .execute(&mut admin)
        .await?;
    assert!(
        connection::open(&config, &secrets, &budget, "single")
            .await
            .is_err()
    );
    sqlx::query("UPDATE _sqlx_migrations SET version=1 WHERE version=999")
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
    sqlx::query("INSERT INTO _sqlx_migrations(version,description,success,checksum,execution_time) VALUES(1,'runtime',TRUE,?,0)")
        .bind(&history[0].2).execute(&mut admin).await?;
    sqlx::query("UPDATE mokyu_meta SET deployment_key_fingerprint=REPEAT(CHAR(0),32) WHERE id=1")
        .execute(&mut admin)
        .await?;
    assert!(
        connection::open(&config, &secrets, &budget, "single")
            .await
            .is_err()
    );
    sqlx::query("UPDATE mokyu_meta SET deployment_key_fingerprint=? WHERE id=1")
        .bind(
            blake3::hash(&secrets.credential_key.material)
                .as_bytes()
                .as_slice(),
        )
        .execute(&mut admin)
        .await?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    backend.check_owner().await?;
    sqlx::query("SELECT RELEASE_LOCK(SHA2(CONCAT('mokyu:runtime:',DATABASE()),256))")
        .execute(&mut *backend.owner.lock().await)
        .await?;
    assert!(backend.check_owner().await.is_err());
    close(backend).await?;
    Ok(())
}
