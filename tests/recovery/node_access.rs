use super::*;
use crate::{
    cluster::NodeState,
    model::{
        metrics::{AccessBatch, ChunkCounts, MetricsStore},
        node_observation::{Access, Batch},
    },
};
use chrono::{Timelike, Utc};

async fn send(
    backend: &Backend,
    node: &NodeState,
    key: &Key,
    run: Run,
    access: Access,
) -> Result<()> {
    assert!(
        backend
            .report_observations(
                node,
                key,
                Batch {
                    run,
                    compression: Vec::new(),
                    credentials: Vec::new(),
                    access: Some(access),
                }
            )
            .await?
            .is_empty()
    );
    Ok(())
}

pub(super) async fn verify(
    backend: &Backend,
    pool: &PgPool,
    key: &Key,
    (node, run): (&NodeState, Run),
    (peer, peer_run): (&NodeState, Run),
    (worker, worker_run): (&NodeState, Run),
) -> Result<()> {
    let chunk: Uuid = sqlx::query_scalar("SELECT id FROM chunks ORDER BY id LIMIT 1")
        .fetch_one(pool)
        .await?;
    let at = Utc::now()
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let mut pending = AccessBatch::default();
    pending.chunks.insert(
        (chunk.into(), at),
        ChunkCounts {
            reads: 2,
            ranges: 1,
            bytes: 100,
            origins: 0,
        },
    );
    let access = Access::new(1, pending);
    assert!(
        send(backend, worker, key, worker_run, access.clone())
            .await
            .is_err()
    );
    assert!(
        send(
            backend,
            node,
            key,
            Run {
                fence: Uuid::new_v4(),
                ..run
            },
            access.clone()
        )
        .await
        .is_err()
    );
    tokio::try_join!(
        send(backend, node, key, run, access.clone()),
        send(backend, node, key, run, access.clone())
    )?;
    let mut changed = access.clone();
    changed.chunks[0].1.bytes += 1;
    assert!(send(backend, node, key, run, changed).await.is_err());
    let counts = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT reads,range_reads,bytes FROM chunk_access_stats WHERE chunk_id=$1",
    )
    .bind(chunk)
    .fetch_one(pool)
    .await?;
    assert_eq!(counts, (2, 1, 100));
    send(
        backend,
        node,
        key,
        run,
        Access::new(2, AccessBatch::default()),
    )
    .await?;
    send(backend, node, key, run, access.clone()).await?;
    let mut future = access.clone();
    future.sequence = 3;
    future.chunks[0].0.1 += chrono::Duration::days(2);
    assert!(send(backend, node, key, run, future).await.is_err());
    let sequence: i64 = sqlx::query_scalar("SELECT sequence FROM node_access WHERE node_id=$1")
        .bind(node.node_id)
        .fetch_one(pool)
        .await?;
    assert_eq!(sequence, 2);
    send(
        backend,
        peer,
        key,
        peer_run,
        Access::new(1, AccessBatch::default()),
    )
    .await?;
    backend.flush_access(AccessBatch::default(), 120.0).await?;
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT access_flushed_at >= now()-interval '120 seconds' FROM mokyu_meta"
        )
        .fetch_one(pool)
        .await?
    );
    sqlx::query("UPDATE node_access SET received_at=now()-interval '1 hour' WHERE node_id=$1")
        .bind(peer.node_id)
        .execute(pool)
        .await?;
    send(
        backend,
        node,
        key,
        run,
        Access::new(3, AccessBatch::default()),
    )
    .await?;
    backend.flush_access(AccessBatch::default(), 120.0).await?;
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT access_flushed_at < now()-interval '120 seconds' FROM mokyu_meta"
        )
        .fetch_one(pool)
        .await?
    );
    let lost = AccessBatch {
        dropped: true,
        ..AccessBatch::default()
    };
    send(backend, peer, key, peer_run, Access::new(2, lost)).await?;
    backend.flush_access(AccessBatch::default(), 120.0).await?;
    assert!(sqlx::query_scalar::<_, bool>("SELECT access_flushed_at >= now()-interval '120 seconds' AND access_coverage_since >= (SELECT gap_since FROM node_access WHERE node_id=$1) FROM mokyu_meta").bind(peer.node_id).fetch_one(pool).await?);
    let count: i64 = sqlx::query_scalar("SELECT reads FROM chunk_access_stats WHERE chunk_id=$1")
        .bind(chunk)
        .fetch_one(pool)
        .await?;
    assert_eq!(count, 2);
    sqlx::query("UPDATE mokyu_meta SET maintenance=true")
        .execute(pool)
        .await?;
    send(
        backend,
        node,
        key,
        run,
        Access::new(4, AccessBatch::default()),
    )
    .await?;
    sqlx::query("UPDATE mokyu_meta SET maintenance=false")
        .execute(pool)
        .await?;
    Ok(())
}
