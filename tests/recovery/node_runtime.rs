use super::{Backend, migration, queries};
use crate::{
    content::codec::Key,
    model::{
        cluster::ClusterStore,
        metrics::{NodeSample, RuntimeQuery},
        node::NodeReport,
    },
};
use anyhow::Result;
use chrono::Utc;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a fresh isolated PostgreSQL database"]
async fn postgres_node_runtime_bounds_fences_and_aggregation() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = PgPool::connect(url.trim()).await?;
    assert!(queries::metadata_table().fetch_one(&pool).await?.is_none());
    migration::run(&mut *pool.acquire().await?).await?;
    let backend = Backend::new(pool.clone(), pool.acquire().await?.detach(), false);
    let key = Key::new("aes-256-gcm", [8; 32])?;
    let a = backend
        .register("data-a", "data", "a".into(), &key)
        .await?
        .unwrap();
    let b = backend
        .register("data-b", "data", "b".into(), &key)
        .await?
        .unwrap();
    let boot = Uuid::new_v4();
    let mut report = NodeReport {
        boot_id: boot,
        sequence: 1,
        sampled_at: Utc::now(),
        metadata: Some(json!({"format":1})),
        status: json!({}),
    };
    for node in [&a, &b] {
        backend.heartbeat(node, "data", &key).await?;
        backend
            .begin_report(node, "data", &key, boot, Utc::now())
            .await?;
        for sequence in [1, 2] {
            report.sequence = sequence;
            report.status = json!({"runtime":{"started_at":Utc::now(),"uptime_seconds":sequence*60,"http":{"web":{"started":(sequence-1)*60}}},"cache":{"hits":sequence-1},"io":{"cache_lookups":(sequence-1)*10}});
            assert!(backend.report(node, "data", &key, &report).await?);
            let sample = NodeSample::from_report(&report, 60)?;
            assert!(
                backend
                    .sample_node_runtime(node, "data", &key, &sample)
                    .await?
            );
            assert!(
                !backend
                    .sample_node_runtime(node, "data", &key, &sample)
                    .await?
            );
            assert!(
                backend
                    .sample_node_runtime(node, "worker", &key, &sample)
                    .await
                    .is_err()
            );
        }
    }
    sqlx::query("UPDATE node_runtime_samples SET received_at=now()-(CASE WHEN sequence=1 THEN 61 ELSE 1 END)*interval '1 second'").execute(&pool).await?;
    backend.aggregate_runtime(60, 86400).await?;
    let history = backend.cluster_runtime(&RuntimeQuery::default()).await?;
    assert_eq!(history.len(), 1);
    assert_eq!(
        (
            history[0].expected_nodes,
            history[0].reported_nodes,
            history[0].rate_nodes
        ),
        (2, 2, 2)
    );
    assert_eq!(history[0].rates.as_ref().unwrap().requests, 2.0);
    assert_eq!(
        history[0].rates.as_ref().unwrap().cache_hit_ratio,
        Some(0.1)
    );
    backend.aggregate_runtime(60, 86400).await?;
    assert_eq!(
        backend
            .cluster_runtime(&RuntimeQuery::default())
            .await?
            .len(),
        1
    );
    assert_eq!(
        backend
            .node_runtime(
                a.node_id,
                &RuntimeQuery {
                    hours: 24,
                    limit: 1
                }
            )
            .await?
            .len(),
        1
    );
    assert!(
        backend
            .node_runtime(
                a.node_id,
                &RuntimeQuery {
                    hours: 169,
                    limit: 1
                }
            )
            .await
            .is_err()
    );

    // Old boot samples remain historical but cannot be accepted or become current.
    backend
        .begin_report(&a, "data", &key, Uuid::new_v4(), Utc::now())
        .await?;
    let sample = NodeSample::from_report(&report, 60)?;
    assert!(
        !backend
            .sample_node_runtime(&a, "data", &key, &sample)
            .await?
    );
    sqlx::query("UPDATE runtime_samples SET sampled_at=now()-interval '2 minutes' WHERE data->>'scope'='cluster'").execute(&pool).await?;
    backend.aggregate_runtime(60, 86400).await?;
    let history = backend.cluster_runtime(&RuntimeQuery::default()).await?;
    assert_eq!(history.last().unwrap().reported_nodes, 1);
    assert_eq!(history.first().unwrap().reported_nodes, 2);
    backend.release("data-b").await?;
    assert!(
        backend
            .sample_node_runtime(&b, "data", &key, &sample)
            .await
            .is_err()
    );

    // Both the history endpoint and retention work have fixed upper bounds.
    sqlx::query("INSERT INTO node_runtime_samples(node_id,boot_id,sequence,uptime_seconds,data,received_at) SELECT $1,$2,n,n,jsonb_set($3,'{sequence}',to_jsonb(n)),now()-n*interval '1 second' FROM generate_series(3,2052) n").bind(a.node_id).bind(boot).bind(sqlx::types::Json(&sample)).execute(&pool).await?;
    assert_eq!(
        backend
            .node_runtime(a.node_id, &RuntimeQuery::default())
            .await?
            .len(),
        2048
    );
    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM node_runtime_samples")
        .fetch_one(&pool)
        .await?;
    let rows = queries::node_runtime_expire(0.0)
        .execute(&pool)
        .await?
        .rows_affected();
    assert_eq!(rows, 1000);
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM node_runtime_samples")
        .fetch_one(&pool)
        .await?;
    assert_eq!(before - after, 1000);
    let mut lock = pool.begin().await?;
    sqlx::raw_sql("LOCK node_runtime_samples IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await?;
    let start = std::time::Instant::now();
    assert!(
        backend
            .node_runtime(a.node_id, &RuntimeQuery::default())
            .await
            .is_err()
    );
    assert!(start.elapsed() < std::time::Duration::from_secs(6));
    lock.rollback().await?;
    Ok(())
}
