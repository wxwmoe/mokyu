use super::{connection, queries::notices};
use crate::config::Config;
use crate::model::notification::Consumer;
use anyhow::Result;
use std::time::Duration;
use uuid::Uuid;

async fn acknowledge(pool: &sqlx::MySqlPool, consumer: Consumer, event: Uuid) -> Result<u64> {
    let mut tx = pool.begin().await?;
    let ids = serde_json::to_string(&[event.simple().to_string()])?;
    let count = notices::acknowledge(&mut tx, consumer, &ids).await?;
    tx.commit().await?;
    Ok(count)
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_notice_receipts_follow_publication_locks() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let pool = &backend.pool;
    let consumer = Consumer {
        node: Uuid::new_v4(),
        generation: Uuid::new_v4(),
    };
    let event = Uuid::new_v4();
    let replacement = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO nodes(id,name,role,credential_generation) VALUES(?,'receipt','data',1)",
    )
    .bind(consumer.node)
    .execute(pool)
    .await?;
    notices::activate(consumer, "data").execute(pool).await?;
    sqlx::query("INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key,retry_at) VALUES(?,0,?,?,'reset',?,'',UTC_TIMESTAMP(6))")
        .bind(consumer.node).bind(consumer.generation).bind(event).bind(consumer.node).execute(pool).await?;

    let mut publish = pool.begin().await?;
    sqlx::query("SELECT id FROM nodes WHERE id=? FOR UPDATE")
        .bind(consumer.node)
        .fetch_one(&mut *publish)
        .await?;
    let receipt = acknowledge(pool, consumer, event);
    tokio::pin!(receipt);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut receipt)
            .await
            .is_err()
    );
    tokio::time::timeout(
        Duration::from_secs(2),
        sqlx::query("UPDATE node_notices SET event_id=?,retry_at=NULL WHERE node_id=? AND slot=0")
            .bind(replacement)
            .bind(consumer.node)
            .execute(&mut *publish),
    )
    .await??;
    publish.commit().await?;
    assert_eq!(receipt.await?, 0);

    assert_eq!(acknowledge(pool, consumer, replacement).await?, 0);
    sqlx::query("UPDATE node_notices SET retry_at=UTC_TIMESTAMP(6) WHERE node_id=?")
        .bind(consumer.node)
        .execute(pool)
        .await?;
    assert_eq!(
        acknowledge(
            pool,
            Consumer {
                generation: Uuid::new_v4(),
                ..consumer
            },
            replacement
        )
        .await?,
        0
    );
    assert_eq!(
        acknowledge(
            pool,
            Consumer {
                node: Uuid::new_v4(),
                ..consumer
            },
            replacement
        )
        .await?,
        0
    );
    sqlx::query("UPDATE nodes SET enabled=FALSE WHERE id=?")
        .bind(consumer.node)
        .execute(pool)
        .await?;
    assert_eq!(acknowledge(pool, consumer, replacement).await?, 0);
    sqlx::query("UPDATE nodes SET enabled=TRUE WHERE id=?")
        .bind(consumer.node)
        .execute(pool)
        .await?;
    sqlx::query("UPDATE notification_consumers SET expires_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE node_id=?")
        .bind(consumer.node).execute(pool).await?;
    assert_eq!(acknowledge(pool, consumer, replacement).await?, 0);
    notices::renew(consumer).execute(pool).await?;
    assert_eq!(acknowledge(pool, consumer, replacement).await?, 1);
    assert_eq!(acknowledge(pool, consumer, replacement).await?, 0);
    Ok(())
}
