use super::{cache_tests, connection, queries, storage_tests, transactions};
use crate::authorization::Principal;
use crate::config::Config;
use crate::model::bucket::CreateBucket;
use crate::model::{AccountStore, BucketStore, ChunkStore, PhysicalStore};
use anyhow::Result;
use std::time::Duration;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_sources_lock_only_referenced_chunks() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    backend.initialize_admin("admin", "password", false).await?;
    let storage = storage_tests::fixture(&backend).await?;
    let bucket = BucketStore::create(
        &backend,
        &Principal::Local,
        CreateBucket {
            name: "source-locks".into(),
            project_id: None,
            storage_id: Some(storage),
        },
    )
    .await?
    .id;
    let mut chunks = Vec::new();
    let mut streams = Vec::new();
    for (name, hash) in [("a", 1), ("b", 2)] {
        let stream = cache_tests::stream(&backend, bucket, name).await?;
        let chunk = cache_tests::chunk(&backend, storage, stream, hash).await?;
        backend.register_encoding(&chunk).await?;
        ChunkStore::complete(&backend, stream, 0, &chunk).await?;
        streams.push(stream);
        chunks.push(chunk);
    }
    let mut held = backend.pool.begin().await?;
    queries::lock_chunk(chunks[0].id)
        .fetch_one(&mut *held)
        .await?;
    let mut checked = backend.pool.begin().await?;
    // Exercise the non-semijoin plan without relying on table-size estimates.
    sqlx::query("SET SESSION optimizer_switch='semijoin=off,materialization=off'")
        .execute(&mut *checked)
        .await?;
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        transactions::require_sources(&mut checked, Some(streams[1]), &[]).await?;
        transactions::require_sources(&mut checked, None, &[chunks[1].id]).await
    })
    .await;
    held.rollback().await?;
    result??;
    checked.rollback().await?;
    Ok(())
}
