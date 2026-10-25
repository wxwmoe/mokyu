use super::*;
use crate::{
    cluster::NodeState,
    model::{
        session::Run,
        source::{SourceOperation, SourceScope, SourceType},
    },
};
use serde_json::json;

async fn enqueue(backend: &Backend, scope: &SourceScope, policy: &str) -> Result<Uuid> {
    let snapshot =
        queries::source_snapshot(&mut *backend.pool.acquire().await?, scope, false).await?;
    let operation = SourceOperation {
        principal: Principal::System,
        mode: "cluster".into(),
        snapshot,
        code: (policy != "recheck").then(|| "remote_missing".into()),
        node: None,
    };
    Ok(sqlx::query_scalar("INSERT INTO tasks(task_type,state,detail) VALUES('integrity','queued',$1) RETURNING id")
        .bind(json!({"policy":policy,"operation":policy,"storage_id":scope.storage_id,"source_operation":operation})).fetch_one(&backend.pool).await?)
}

pub(super) async fn check(
    backend: &Backend,
    peer: &Backend,
    node: &NodeState,
    key: &Key,
    run: Run,
    storage: Uuid,
) -> Result<()> {
    let pool = &backend.pool;
    let raw = Uuid::now_v7();
    sqlx::query("INSERT INTO raw_sources(id,storage_id,medium,state,size,digest,profile_revision) VALUES($1,$2,'shared_s3','ready',1,$3,1)")
        .bind(raw).bind(storage).bind(vec![7u8;16]).execute(pool).await?;
    let scope = SourceScope {
        storage_id: storage,
        source: SourceType::Raw,
        id: raw.to_string(),
    };
    let request = |operation, completion| work::Exchange {
        run,
        operation,
        completion,
        class: 0,
        max_items: 64,
        cached: vec![],
    };
    // Ordinary integrity work is not consumed by the exact-source executor.
    let ordinary: Uuid = sqlx::query_scalar("INSERT INTO tasks(task_type,state,detail,updated_at) VALUES('integrity','queued',jsonb_build_object('storage_id',$1::uuid,'mode','full'),now()-interval '1 day') RETURNING id").bind(storage).fetch_one(pool).await?;
    let task = enqueue(backend, &scope, "quarantine").await?;
    let operation = Uuid::now_v7();
    let reply = backend
        .exchange_work(node, key, request(operation, None))
        .await?;
    let work::Plan::Inspect {
        snapshot,
        inspect,
        members,
        location,
    } = reply.plan.unwrap()
    else {
        panic!("expected source plan")
    };
    assert!(!inspect && members.is_empty());
    assert_eq!(location, raw);
    assert_eq!(snapshot.location(), raw);
    assert!(queries::work::physical_active(raw).fetch_one(pool).await?);
    let completion = work::Completion {
        operation,
        outcomes: vec![work::Outcome::Inspected { code: None }],
    };
    let receipt = Uuid::now_v7();
    let result = backend
        .exchange_work(node, key, request(receipt, Some(completion.clone())))
        .await?;
    assert!(result.plan.is_none());
    assert_eq!(
        sqlx::query_as::<_, (Option<String>, i64)>(
            "SELECT quarantine_code,revision FROM raw_sources WHERE id=$1"
        )
        .bind(raw)
        .fetch_one(pool)
        .await?,
        (Some("remote_missing".into()), 2)
    );
    assert_eq!(
        sqlx::query_as::<_, (String, i64)>("SELECT state,processed FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(pool)
            .await?,
        ("completed".into(), 1)
    );
    assert!(!queries::work::physical_active(raw).fetch_one(pool).await?);
    assert!(
        peer.exchange_work(node, key, request(receipt, Some(completion)))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT revision FROM raw_sources WHERE id=$1")
            .bind(raw)
            .fetch_one(pool)
            .await?,
        2
    );

    let task = enqueue(backend, &scope, "recheck").await?;
    // Saturation must not consume the task or change the source.
    let permit = Uuid::new_v4();
    sqlx::query("UPDATE storage_profiles SET integrity_concurrency=1 WHERE storage_id=$1")
        .bind(storage)
        .execute(pool)
        .await?;
    queries::insert_storage_permit(permit, storage, "integrity", node.node_id)
        .execute(pool)
        .await?;
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), None))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(pool)
            .await?,
        "queued"
    );
    queries::release_storage_permit(permit)
        .execute(pool)
        .await?;
    let operation = Uuid::now_v7();
    let reply = backend
        .exchange_work(node, key, request(operation, None))
        .await?;
    assert!(matches!(
        reply.plan,
        Some(work::Plan::Inspect { inspect: true, .. })
    ));
    let bad = work::Completion {
        operation,
        outcomes: vec![work::Outcome::Deleted { stored_at: None }],
    };
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), Some(bad)))
            .await
            .is_err()
    );
    let completion = work::Completion {
        operation,
        outcomes: vec![work::Outcome::Inspected { code: None }],
    };
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), Some(completion)))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(
        sqlx::query_as::<_, (Option<String>, i64)>(
            "SELECT quarantine_code,revision FROM raw_sources WHERE id=$1"
        )
        .bind(raw)
        .fetch_one(pool)
        .await?,
        (None, 3)
    );

    for (outcome, expected, changed) in [
        (work::Outcome::Retry, "backend_io_failed", false),
        (
            work::Outcome::Inspected { code: None },
            "source_changed",
            true,
        ),
    ] {
        let task = enqueue(backend, &scope, "verify").await?;
        let operation = Uuid::now_v7();
        assert!(
            backend
                .exchange_work(node, key, request(operation, None))
                .await?
                .plan
                .is_some()
        );
        if changed {
            sqlx::query("UPDATE raw_sources SET revision=revision+1 WHERE id=$1")
                .bind(raw)
                .execute(pool)
                .await?;
        }
        let completion = work::Completion {
            operation,
            outcomes: vec![outcome],
        };
        assert!(
            backend
                .exchange_work(node, key, request(Uuid::now_v7(), Some(completion)))
                .await?
                .plan
                .is_none()
        );
        assert_eq!(
            sqlx::query_as::<_, (String, Option<String>)>(
                "SELECT state,error FROM tasks WHERE id=$1"
            )
            .bind(task)
            .fetch_one(pool)
            .await?,
            ("failed".into(), Some(expected.into()))
        );
        assert!(
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT quarantine_code FROM raw_sources WHERE id=$1"
            )
            .bind(raw)
            .fetch_one(pool)
            .await?
            .is_none()
        );
    }
    let task = enqueue(backend, &scope, "verify").await?;
    let operation = Uuid::now_v7();
    assert!(
        backend
            .exchange_work(node, key, request(operation, None))
            .await?
            .plan
            .is_some()
    );
    let invalid = work::Completion {
        operation,
        outcomes: vec![work::Outcome::Inspected {
            code: Some("missing_key".into()),
        }],
    };
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), Some(invalid)))
            .await
            .is_err()
    );
    let checked = work::Completion {
        operation,
        outcomes: vec![work::Outcome::Inspected {
            code: Some("hash_mismatch".into()),
        }],
    };
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), Some(checked)))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT quarantine_code FROM raw_sources WHERE id=$1")
            .bind(raw)
            .fetch_one(pool)
            .await?,
        "hash_mismatch"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(pool)
            .await?,
        "completed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM tasks WHERE id=$1")
            .bind(ordinary)
            .fetch_one(pool)
            .await?,
        "queued"
    );
    sqlx::query("UPDATE storage_profiles SET integrity_rps=2,integrity_bandwidth='2MiB' WHERE storage_id=$1").bind(storage).execute(pool).await?;
    sqlx::query("INSERT INTO storage_rate_state(storage_id,metric,tokens,replenished_at,updated_at) VALUES($1,'integrity_rps',2,now()+interval '1 day',now()),($1,'integrity_bandwidth',0,now()+interval '1 day',now()) ON CONFLICT(storage_id,metric) DO UPDATE SET tokens=excluded.tokens,replenished_at=excluded.replenished_at").bind(storage).execute(pool).await?;
    let task = enqueue(backend, &scope, "recheck").await?;
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), None))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(sqlx::query_scalar::<_,f64>("SELECT tokens::double precision FROM storage_rate_state WHERE storage_id=$1 AND metric='integrity_rps'").bind(storage).fetch_one(pool).await?,2.0);
    sqlx::query("UPDATE storage_rate_state SET tokens=100 WHERE storage_id=$1 AND metric='integrity_bandwidth'").bind(storage).execute(pool).await?;
    let operation = Uuid::now_v7();
    assert!(
        backend
            .exchange_work(node, key, request(operation, None))
            .await?
            .plan
            .is_some()
    );
    let completion = work::Completion {
        operation,
        outcomes: vec![work::Outcome::Inspected { code: None }],
    };
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), Some(completion)))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(pool)
            .await?,
        "completed"
    );
    sqlx::query("UPDATE storage_profiles SET integrity_rps=NULL,integrity_bandwidth=NULL WHERE storage_id=$1").bind(storage).execute(pool).await?;
    sqlx::query("UPDATE tasks SET state='paused' WHERE id=$1")
        .bind(ordinary)
        .execute(pool)
        .await?;
    Ok(())
}
