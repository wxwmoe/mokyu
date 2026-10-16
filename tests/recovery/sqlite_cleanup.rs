use super::try_cleanup_transaction;
use anyhow::Result;
use sqlx::Connection;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::time::Duration;

#[tokio::test]
async fn cleanup_reacquires_returning_connection() -> Result<()> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    sqlx::query("CREATE TABLE reclaimed (id INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await?;
    for id in 0..64 {
        // No caller-side yield: every commit must permit the next candidate.
        let mut tx = try_cleanup_transaction(&pool)
            .await?
            .expect("cleanup skipped an available writer");
        sqlx::query("INSERT INTO reclaimed VALUES (?)")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM reclaimed")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 64);
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn cleanup_yields_to_busy_writer_and_recovers() -> Result<()> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    let writer = pool.begin().await?;
    let claim =
        tokio::time::timeout(Duration::from_secs(1), try_cleanup_transaction(&pool)).await??;
    assert!(claim.is_none(), "cleanup acquired an occupied writer");
    writer.rollback().await?;
    let tx = try_cleanup_transaction(&pool)
        .await?
        .expect("cancelled acquisition blocked the next claim");
    tx.commit().await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn cleanup_propagates_pool_errors() -> Result<()> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    pool.close().await;
    let error = try_cleanup_transaction(&pool)
        .await
        .expect_err("closed pool was treated as busy");
    assert!(matches!(
        error.downcast_ref::<sqlx::Error>(),
        Some(sqlx::Error::PoolClosed)
    ));
    Ok(())
}

#[tokio::test]
async fn cleanup_cancels_busy_database_transaction() -> Result<()> {
    let path = std::env::temp_dir().join(format!("mokyu-cleanup-{}.db", uuid::Uuid::new_v4()));
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .busy_timeout(Duration::from_secs(2));
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await?;
    let mut connection = sqlx::SqliteConnection::connect_with(&options).await?;
    let writer = connection.begin_with("BEGIN IMMEDIATE").await?;
    let claim =
        tokio::time::timeout(Duration::from_secs(1), try_cleanup_transaction(&pool)).await??;
    assert!(
        claim.is_none(),
        "cleanup did not yield to the database writer"
    );
    writer.rollback().await?;
    let mut tx = try_cleanup_transaction(&pool)
        .await?
        .expect("cancelled BEGIN blocked the next transaction");
    sqlx::query("CREATE TABLE reclaimed (id INTEGER PRIMARY KEY)")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    connection.close().await?;
    pool.close().await;
    std::fs::remove_file(path)?;
    Ok(())
}
