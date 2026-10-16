use super::{connection, queries};
use crate::config::Config;
use anyhow::Result;
use sqlx::Connection;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_SQLITE_CONFIG pointing to a fresh isolated SQLite database"]
async fn sqlite_file_recovery_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let mut writer = backend.write.acquire().await?;
    sqlx::query("INSERT INTO runtime_settings(section,value) VALUES('backup','{\"saved\":true}')")
        .execute(&mut *writer)
        .await?;
    let pages: i64 = sqlx::query_scalar("PRAGMA page_count")
        .fetch_one(&mut *writer)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "PRAGMA max_page_count={pages}"
    )))
    .execute(&mut *writer)
    .await?;
    let mut tx = writer.begin_with(queries::BEGIN_WRITE).await?;
    sqlx::query("UPDATE runtime_settings SET revision=2 WHERE section='backup'")
        .execute(&mut *tx)
        .await?;
    let error = sqlx::query("UPDATE runtime_settings SET value=? WHERE section='backup'")
        .bind(serde_json::json!("x".repeat(2 * 1024 * 1024)))
        .execute(&mut *tx)
        .await
        .unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("13")
    );
    drop(tx);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT revision FROM runtime_settings WHERE section='backup'"
        )
        .fetch_one(&mut *writer)
        .await?,
        1
    );
    sqlx::query("PRAGMA max_page_count=4294967294")
        .execute(&mut *writer)
        .await?;

    let backup = config.instance.data_path.join("backup.db");
    assert!(!backup.exists());
    sqlx::query("VACUUM INTO ?")
        .bind(backup.to_str().unwrap())
        .execute(&mut *writer)
        .await?;
    sqlx::query("UPDATE runtime_settings SET revision=3 WHERE section='backup'")
        .execute(&mut *writer)
        .await?;
    drop(writer);
    backend.read.close().await;
    backend.write.close().await;
    drop(backend);

    let mut restored = config.clone();
    restored.database.path = Some(backup);
    let backend = connection::open(&restored, &secrets, &budget, "single").await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>("PRAGMA integrity_check")
            .fetch_one(&backend.read)
            .await?,
        "ok"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT revision FROM runtime_settings WHERE section='backup'"
        )
        .fetch_one(&backend.read)
        .await?,
        1
    );
    let value: serde_json::Value =
        sqlx::query_scalar("SELECT value FROM runtime_settings WHERE section='backup'")
            .fetch_one(&backend.read)
            .await?;
    assert_eq!(value, serde_json::json!({"saved":true}));
    backend.read.close().await;
    backend.write.close().await;
    drop(backend);

    let corrupt = config.instance.data_path.join("corrupt.db");
    assert!(!corrupt.exists());
    let original = b"not a SQLite database";
    std::fs::write(&corrupt, original)?;
    restored.database.path = Some(corrupt.clone());
    assert!(
        connection::open(&restored, &secrets, &budget, "single")
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(corrupt)?, original);
    Ok(())
}
