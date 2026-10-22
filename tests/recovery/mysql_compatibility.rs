use super::{connection, queries};
use crate::config::Config;
use crate::model::TaskControlStore;
use anyhow::Result;
use serde_json::{Value, json};
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_dialect_and_lock_scope_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    for name in ["Case", "case", "case ", "\u{732b}", "\u{732b} "] {
        sqlx::query("INSERT INTO projects(id,name) VALUES(?,?)")
            .bind(Uuid::new_v4())
            .bind(name)
            .execute(db)
            .await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM projects WHERE name=?")
                .bind(name)
                .fetch_one(db)
                .await?,
            1
        );
    }
    let first = backend.enqueue("integrity", None, json!({})).await?;
    let second = backend.enqueue("integrity", None, json!({})).await?;
    let detail = json!({"object":{"ok":true},"array":[false,null,"true"],"null":null});
    queries::record_pack_rewrite(first, detail.clone())
        .execute(db)
        .await?;
    let saved: Value = sqlx::query_scalar("SELECT detail FROM tasks WHERE id=?")
        .bind(first)
        .fetch_one(db)
        .await?;
    assert_eq!(saved["last_rewrite"], detail);

    let mut a = db.begin().await?;
    let locked: Uuid = queries::claim_task_candidate(0, &[], false)
        .build_query_scalar()
        .fetch_one(&mut *a)
        .await?;
    let mut b = db.begin().await?;
    let other: Uuid = tokio::time::timeout(
        Duration::from_secs(2),
        queries::claim_task_candidate(0, &[], false)
            .build_query_scalar()
            .fetch_one(&mut *b),
    )
    .await??;
    assert_ne!(locked, other);
    assert!([first, second].contains(&locked) && [first, second].contains(&other));
    // Claiming tasks must not lock the global maintenance or policy rows.
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut metadata = db.begin().await?;
        sqlx::query("SELECT id FROM mokyu_meta FOR UPDATE")
            .execute(&mut *metadata)
            .await?;
        sqlx::query("SELECT policy FROM maintenance_controls FOR UPDATE")
            .execute(&mut *metadata)
            .await?;
        metadata.rollback().await
    })
    .await??;
    b.rollback().await?;
    a.rollback().await?;
    Ok(())
}
