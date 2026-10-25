use super::*;
use crate::{
    cluster::NodeState,
    model::{maintenance::MaintenanceStore, session::Run},
};
use serde_json::{Value, json};

async fn enqueue(backend: &Backend, storage: Uuid, dry: bool) -> Result<Uuid> {
    Ok(sqlx::query_scalar("INSERT INTO tasks(task_type,state,detail) VALUES('sweep','queued',$1) RETURNING id")
        .bind(json!({"storage_id":storage,"dry_run":dry,"prefix":"","cutoff":chrono::Utc::now()-chrono::Duration::hours(1),"candidates":0,"bytes":0,"unrecognized":0,"samples":[]}))
        .fetch_one(&backend.pool).await?)
}

async fn indexed(backend: &Backend, storage: Uuid, id: Uuid) -> Result<()> {
    sqlx::query("INSERT INTO packs(id,storage_id,encryption_algorithm,raw_size,stored_size,member_count,state,pack_fingerprint) VALUES($1,$2,'none',2,2,2,'preparing',$3)")
        .bind(id).bind(storage).bind(vec![6u8;32]).execute(&backend.pool).await?;
    Ok(())
}

async fn state(backend: &Backend, task: Uuid) -> Result<(String, i64, Value)> {
    Ok(
        sqlx::query_as("SELECT state,processed,detail FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(&backend.pool)
            .await?,
    )
}

pub(super) async fn check(
    backend: &Backend,
    peer: &Backend,
    node: &NodeState,
    key: &Key,
    run: Run,
    storage: Uuid,
    bucket: Uuid,
) -> Result<()> {
    let pool = &backend.pool;
    let now = chrono::Utc::now();
    let old = now - chrono::Duration::hours(2);
    let orphans = [Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7()];
    let protected = Uuid::now_v7();
    indexed(backend, storage, protected).await?;
    let mut items: Vec<work::ListedObject> = orphans
        .iter()
        .map(|id| work::ListedObject {
            path: crate::storage::chunk_name(*id),
            size: 3,
            modified: old,
        })
        .collect();
    items.extend([
        work::ListedObject {
            path: format!("packs/{}", crate::storage::chunk_name(protected)),
            size: 4,
            modified: old,
        },
        work::ListedObject {
            path: crate::storage::chunk_name(Uuid::now_v7()),
            size: 5,
            modified: now,
        },
        work::ListedObject {
            path: "meta.json".into(),
            size: 6,
            modified: old,
        },
    ]);
    items.sort_by(|a, b| a.path.cmp(&b.path));
    let request = |operation, completion| work::Exchange {
        run,
        operation,
        completion,
        class: 0,
        max_items: 64,
        cached: vec![],
    };
    let listing = |operation, items| work::Completion {
        operation,
        outcomes: vec![work::Outcome::Listed { items }],
    };
    let task = enqueue(backend, storage, true).await?;
    let operation = Uuid::now_v7();
    let first = backend
        .exchange_work(node, key, request(operation, None))
        .await?;
    assert!(matches!(
        first.plan,
        Some(work::Plan::SweepScan { limit: 32, .. })
    ));
    let mut wrong = items.clone();
    wrong.reverse();
    assert!(
        backend
            .exchange_work(
                node,
                key,
                request(Uuid::now_v7(), Some(listing(operation, wrong)))
            )
            .await
            .is_err()
    );
    let receipt = Uuid::now_v7();
    let next = backend
        .exchange_work(
            node,
            key,
            request(receipt, Some(listing(operation, items.clone()))),
        )
        .await?;
    let replay = peer
        .exchange_work(
            node,
            key,
            request(receipt, Some(listing(operation, items.clone()))),
        )
        .await?;
    assert_eq!(
        serde_json::to_value(&next.plan)?,
        serde_json::to_value(&replay.plan)?
    );
    assert!(matches!(
        next.plan,
        Some(work::Plan::SweepScan { after: Some(_), .. })
    ));
    let (_, processed, detail) = state(backend, task).await?;
    assert_eq!(processed, 6);
    assert_eq!(detail["candidates"], 3);
    assert_eq!(detail["bytes"], 9);
    assert_eq!(detail["unrecognized"], 1);
    assert!(
        backend
            .exchange_work(
                node,
                key,
                request(Uuid::now_v7(), Some(listing(receipt, vec![])))
            )
            .await?
            .plan
            .is_none()
    );
    assert_eq!(state(backend, task).await?.0, "completed");

    let task = enqueue(backend, storage, false).await?;
    sqlx::query("UPDATE mokyu_meta SET maintenance=false")
        .execute(pool)
        .await?;
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), None))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(state(backend, task).await?.0, "failed");
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT error FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(pool)
            .await?,
        "sweep_requires_maintenance"
    );
    MaintenanceStore::mode(backend, true).await?;
    // Earlier child contracts deliberately retained grants; expire those isolated fixtures.
    sqlx::query("UPDATE stream_leases SET expires_at=clock_timestamp()-interval '1 second'")
        .execute(pool)
        .await?;
    sqlx::query("UPDATE node_native_work SET expires_at=clock_timestamp()-interval '1 second' WHERE result_hash IS NULL").execute(pool).await?;
    let stream: Uuid = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state) VALUES($1,'sweep-read','object','ready') RETURNING id").bind(bucket).fetch_one(pool).await?;
    let lease: Uuid = sqlx::query_scalar("INSERT INTO stream_leases(stream_id,lease_type,expires_at) VALUES($1,'read',clock_timestamp()+interval '1 minute') RETURNING id").bind(stream).fetch_one(pool).await?;
    sqlx::query("UPDATE tasks SET state='queued',error=NULL WHERE id=$1")
        .bind(task)
        .execute(pool)
        .await?;
    assert!(MaintenanceStore::mode(peer, false).await.is_err());
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), None))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(state(backend, task).await?.0, "queued");
    sqlx::query("DELETE FROM stream_leases WHERE id=$1")
        .bind(lease)
        .execute(pool)
        .await?;
    let operation = Uuid::now_v7();
    assert!(matches!(
        backend
            .exchange_work(node, key, request(operation, None))
            .await?
            .plan,
        Some(work::Plan::SweepScan { .. })
    ));
    let deletion = Uuid::now_v7();
    let reply = backend
        .exchange_work(
            node,
            key,
            request(deletion, Some(listing(operation, items.clone()))),
        )
        .await?;
    let work::Plan::SweepDelete {
        items: selected, ..
    } = reply.plan.unwrap()
    else {
        panic!("expected deletion plan")
    };
    assert_eq!(selected.len(), 3);
    assert!(selected.iter().all(|row| {
        orphans.contains(&crate::storage::Storage::parse_physical_path(&row.path).unwrap())
    }));
    let results = work::Completion {
        operation: deletion,
        outcomes: vec![
            work::Outcome::Deleted {
                stored_at: Some(old),
            },
            work::Outcome::Retry,
            work::Outcome::Retained { stored_at: now },
        ],
    };
    let receipt = Uuid::now_v7();
    assert!(
        backend
            .exchange_work(node, key, request(receipt, Some(results.clone())))
            .await?
            .plan
            .is_none()
    );
    assert!(
        peer.exchange_work(node, key, request(receipt, Some(results)))
            .await?
            .plan
            .is_none()
    );
    let (status, processed, detail) = state(backend, task).await?;
    assert_eq!(status, "failed");
    assert_eq!(processed, 0);
    assert_eq!(detail["candidates"], 1);
    assert_eq!(detail["unrecognized"], 1);
    assert_eq!(detail["sweep_page"]["items"].as_array().unwrap().len(), 1);
    // A candidate becoming indexed before a resumed grant must be retained.
    indexed(
        backend,
        storage,
        crate::storage::Storage::parse_physical_path(&selected[1].path).unwrap(),
    )
    .await?;
    sqlx::query("UPDATE tasks SET state='queued',error=NULL WHERE id=$1")
        .bind(task)
        .execute(pool)
        .await?;
    let operation = Uuid::now_v7();
    assert!(matches!(
        backend
            .exchange_work(node, key, request(operation, None))
            .await?
            .plan,
        Some(work::Plan::SweepScan { .. })
    ));
    let (_, processed, detail) = state(backend, task).await?;
    assert_eq!(processed, 6);
    assert!(detail.get("sweep_page").is_none());
    assert_eq!(detail["candidates"], 1);
    backend
        .exchange_work(
            node,
            key,
            request(Uuid::now_v7(), Some(listing(operation, vec![]))),
        )
        .await?;
    assert_eq!(state(backend, task).await?.0, "completed");

    let task = enqueue(backend, storage, false).await?;
    let operation = Uuid::now_v7();
    backend
        .exchange_work(node, key, request(operation, None))
        .await?;
    let deletion = Uuid::now_v7();
    let reply = backend
        .exchange_work(
            node,
            key,
            request(
                deletion,
                Some(listing(operation, vec![selected[0].clone()])),
            ),
        )
        .await?;
    assert!(matches!(reply.plan, Some(work::Plan::SweepDelete { .. })));
    sqlx::query("UPDATE tasks SET state='paused' WHERE id=$1")
        .bind(task)
        .execute(pool)
        .await?;
    assert!(MaintenanceStore::mode(peer, false).await.is_err());
    sqlx::query("UPDATE maintenance_controls SET paused=false WHERE policy='cleanup'")
        .execute(pool)
        .await?;
    let cleanup: Uuid = sqlx::query_scalar(
        "INSERT INTO tasks(task_type,state,detail) VALUES('cleanup','queued',$1) RETURNING id",
    )
    .bind(json!({"storage_id":storage}))
    .fetch_one(pool)
    .await?;
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), None))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(state(backend, cleanup).await?.0, "queued");
    sqlx::query("UPDATE node_work SET expires_at=clock_timestamp()-interval '1 second' WHERE node_id=$1 AND operation_id=$2").bind(node.node_id).bind(deletion).execute(pool).await?;
    MaintenanceStore::mode(peer, false).await?;
    assert!(
        backend
            .exchange_work(
                node,
                key,
                request(
                    Uuid::now_v7(),
                    Some(work::Completion {
                        operation: deletion,
                        outcomes: vec![work::Outcome::Deleted { stored_at: None }],
                    })
                )
            )
            .await
            .is_err()
    );
    Ok(())
}
