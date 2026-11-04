use super::{connection, queries};
use crate::config::Config;
use crate::model::{TaskControlStore, maintenance::MaintenanceStore};
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
    backend.recover(false).await?;
    let db = &backend.pool;
    let node = Uuid::new_v4();
    queries::node_registry::insert(node, "pulse-race", "data", &[1])
        .execute(db)
        .await?;
    let storage = super::storage_tests::fixture(&backend).await?;
    let file = Uuid::from_u128(0x0180ff00_0000_7000_8000_000000000001);
    let missing = Uuid::from_u128(file.as_u128() + 1);
    let mut inventory = db.begin().await?;
    let source = sqlx::query("INSERT INTO raw_sources(location_id,storage_id,state,size,digest,profile_revision,pending_upload) VALUES(?,?,'ready',4,?,1,TRUE)")
        .bind(file).bind(storage).bind(&[7u8; 16][..]).execute(&mut *inventory).await?.last_insert_id();
    sqlx::query(
        "INSERT INTO raw_uploads(source_id,deduplicate,background,node_id) VALUES(?,FALSE,TRUE,?)",
    )
    .bind(source)
    .bind(node)
    .execute(&mut *inventory)
    .await?;
    let (target, size, pending, ..) = queries::node_files::snapshot(file, node)
        .fetch_one(&mut *inventory)
        .await?;
    assert_eq!((target, size, pending), (Some(storage), 4, true));
    let (target, size, pending, ..) = queries::node_files::snapshot(missing, node)
        .fetch_one(&mut *inventory)
        .await?;
    assert_eq!((target, size, pending), (None, 0, false));
    inventory.rollback().await?;
    let mut pulse = db.begin().await?;
    queries::node_registry::seen(node)
        .execute(&mut *pulse)
        .await?;
    let mut request = db.begin().await?;
    let mut lookup = Box::pin(queries::node_registry::node(node).fetch_one(&mut *request));
    tokio::select! {
        _ = &mut lookup => anyhow::bail!("node lookup did not wait for the active pulse"),
        _ = tokio::time::sleep(Duration::from_millis(50)) => {}
    }
    pulse.commit().await?;
    tokio::time::timeout(Duration::from_secs(2), lookup).await??;
    request.rollback().await?;
    sqlx::query("DELETE FROM nodes WHERE id=?")
        .bind(node)
        .execute(db)
        .await?;
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

    assert_ne!(first, second);
    for class in [0, 2] {
        if class == 2 {
            sqlx::query("UPDATE tasks SET task_type='upload',detail=?,state=IF(id=?,'failed','queued'),updated_at=TIMESTAMPADD(SECOND,-31,CURRENT_TIMESTAMP(6)) WHERE id IN (?,?)")
                .bind(json!({"operation":"cdc"})).bind(first).bind(first).bind(second).execute(db).await?;
        }
        let mut a = db.begin().await?;
        let locked: Uuid = queries::claim_task_candidate(class, &[], false)
            .build_query_scalar()
            .fetch_one(&mut *a)
            .await?;
        let mut b = db.begin().await?;
        let other: Uuid = tokio::time::timeout(
            Duration::from_secs(2),
            queries::claim_task_candidate(class, &[], false)
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
    }
    Ok(())
}
