use super::{Backend, migration};
use crate::{
    content::codec::Key,
    model::{
        cluster::ClusterStore,
        node::{NodeFilter, NodeReport},
    },
};
use anyhow::Result;
use chrono::Utc;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a fresh isolated PostgreSQL database"]
async fn postgres_node_reports_fence_identity_boot_and_sequence() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = PgPool::connect(url.trim()).await?;
    assert!(
        super::queries::metadata_table()
            .fetch_one(&pool)
            .await?
            .is_none()
    );
    migration::run(&mut *pool.acquire().await?).await?;
    let backend = Backend::new(pool.clone(), pool.acquire().await?.detach(), false);
    let key = Key::new("aes-256-gcm", [7; 32])?;
    let state = backend
        .register("data", "data", "credential".into(), &key)
        .await?
        .unwrap();
    let other = backend
        .register("control", "control", "control-key".into(), &key)
        .await?
        .unwrap();
    let desired = json!({"data":{"web":{"revision":1,"value":{}},"requests":{"revision":1,"value":{"read_idle_timeout":"30s"}}},"control":{"web":{"revision":1,"value":{"origin":"https://example.test"}}}});
    let mut report = NodeReport {
        boot_id: Uuid::new_v4(),
        sequence: 1,
        sampled_at: Utc::now(),
        metadata: Some(
            json!({"format":1,"revisions":{"requests":1},"loaded":{"requests":{"read_idle_timeout":"30s"}}}),
        ),
        status: json!({"work":"normal"}),
    };
    backend
        .begin_report(&state, "data", &key, report.boot_id, Utc::now())
        .await?;
    assert!(backend.report(&state, "data", &key, &report).await?);
    assert!(!backend.report(&state, "data", &key, &report).await?);
    assert!(
        backend
            .report(&state, "control", &key, &report)
            .await
            .is_err()
    );
    let mut forged = state.clone();
    forged.credential = "wrong".into();
    assert!(
        backend
            .report(&forged, "data", &key, &report)
            .await
            .is_err()
    );
    let node = backend.node(state.node_id, &desired).await?.unwrap();
    assert_eq!(
        (
            node.contact.as_str(),
            node.work.as_str(),
            node.configuration.as_str()
        ),
        ("online", "normal", "applied")
    );
    assert_eq!(node.settings["web"], "not_applicable");
    assert_eq!(
        backend
            .node(other.node_id, &desired)
            .await?
            .unwrap()
            .configuration,
        "unknown"
    );
    let mut newer = desired.clone();
    newer["data"]["requests"]["revision"] = json!(2);
    assert_eq!(
        backend
            .node(state.node_id, &newer)
            .await?
            .unwrap()
            .configuration,
        "equivalent"
    );
    newer["data"]["requests"]["value"]["read_idle_timeout"] = json!("31s");
    assert_eq!(
        backend
            .node(state.node_id, &newer)
            .await?
            .unwrap()
            .configuration,
        "restart_required"
    );
    newer["data"]["requests"]["value"]["new_setting"] = json!(true);
    assert_eq!(
        backend
            .node(state.node_id, &newer)
            .await?
            .unwrap()
            .configuration,
        "unknown"
    );
    let mut filter = NodeFilter {
        limit: 1,
        ..Default::default()
    };
    let first = backend.nodes(&filter, &desired).await?;
    assert_eq!(first.items.len(), 1);
    filter.after = first.next_cursor;
    let second = backend.nodes(&filter, &desired).await?;
    assert_eq!(second.items.len(), 1);
    assert_ne!(first.items[0].id, second.items[0].id);
    assert!(second.next_cursor.is_none());
    assert_eq!(
        backend
            .node_counts(&desired)
            .await?
            .iter()
            .map(|row| row.count.parse::<u64>().unwrap())
            .sum::<u64>(),
        2
    );
    assert!(
        backend
            .setting_counts(&desired)
            .await?
            .iter()
            .any(|row| row.state == "unknown")
    );
    report.sequence = 2;
    report.metadata = None;
    report.status = json!({"work":"maintenance"});
    assert!(backend.report(&state, "data", &key, &report).await?);
    assert_eq!(
        backend.node(state.node_id, &desired).await?.unwrap().work,
        "maintenance"
    );
    assert_eq!(
        backend
            .node(state.node_id, &desired)
            .await?
            .unwrap()
            .configuration,
        "applied"
    );
    sqlx::query("UPDATE node_reports SET received_at=now()-interval '1 minute' WHERE node_id=$1")
        .bind(state.node_id)
        .execute(&pool)
        .await?;
    let stale = backend.node(state.node_id, &desired).await?.unwrap();
    assert_eq!(stale.contact, "online");
    assert_eq!(stale.configuration, "unknown");
    assert!(!stale.report_fresh);
    sqlx::query("UPDATE nodes SET last_seen_at=now()-interval '1 minute' WHERE id=$1")
        .bind(state.node_id)
        .execute(&pool)
        .await?;
    assert_eq!(
        backend
            .node(state.node_id, &desired)
            .await?
            .unwrap()
            .contact,
        "unresponsive"
    );
    backend.heartbeat(&state, "data", &key).await?;
    let current_boot = Uuid::new_v4();
    backend
        .begin_report(&state, "data", &key, current_boot, Utc::now())
        .await?;
    report.sequence = 999;
    assert!(!backend.report(&state, "data", &key, &report).await?);
    assert_eq!(
        backend.node(state.node_id, &desired).await?.unwrap().work,
        "starting"
    );
    report.boot_id = current_boot;
    report.status = json!({"oversize":"x".repeat(32768)});
    assert!(backend.report(&state, "data", &key, &report).await.is_err());
    report.status = json!({"work":"normal"});
    report.metadata = Some(json!({"format":99}));
    assert!(backend.report(&state, "data", &key, &report).await?);
    assert_eq!(
        backend
            .node(state.node_id, &desired)
            .await?
            .unwrap()
            .configuration,
        "unknown"
    );
    backend.release("data").await?;
    report.sequence += 1;
    assert!(backend.report(&state, "data", &key, &report).await.is_err());
    assert_eq!(
        backend
            .node(state.node_id, &desired)
            .await?
            .unwrap()
            .contact,
        "released"
    );
    Ok(())
}
