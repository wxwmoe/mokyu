use super::*;
use crate::model::{node_read as read, node_work as work};
use anyhow::Context;

pub(super) async fn check(
    backend: &Backend,
    admin: &Principal,
    node: &crate::cluster::NodeState,
    key: &Key,
    run: Run,
    epoch: i64,
    storage: Uuid,
) -> Result<()> {
    let pool = &backend.pool;
    sqlx::query("UPDATE storage_profiles SET write_processing='inline',upload_cache=true WHERE storage_id=$1")
        .bind(storage).execute(pool).await?;
    let mut sources = Vec::new();
    for index in 0..70u8 {
        sources.push(cached(backend, node, key, run, index, 3).await?);
    }
    let request = |completion| work::Exchange {
        run,
        operation: Uuid::now_v7(),
        completion,
        class: 2,
        max_items: 64,
        cached: vec![],
    };
    let mut outsiders = Vec::new();
    for role in ["data", "worker", "node"] {
        let other = backend
            .provision_node(admin, &format!("cache-outsider-{role}"), role, key)
            .await?;
        let session = backend
            .open_session(
                &other,
                key,
                Open {
                    boot: Uuid::new_v4(),
                    connection: Uuid::new_v4(),
                    previous_epoch: 0,
                },
            )
            .await?;
        outsiders.push((other, session.run));
    }
    check_owner(backend, key, &outsiders).await?;
    let mut hash_lock = pool.begin().await?;
    queries::claim_chunk_hash(storage, &[95; 32])
        .execute(&mut *hash_lock)
        .await?;
    let blocked = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        backend.exchange_work(node, key, request(None)),
    )
    .await
    .context("cache batch waited for a content hash")?;
    assert_eq!(
        model::error(&blocked.err().unwrap()),
        Some(model::Error::NodeWriteBusy)
    );
    hash_lock.rollback().await?;
    let first = backend.exchange_work(node, key, request(None)).await?;
    let work::Plan::UploadRaw { items, .. } = first.plan.as_ref().unwrap() else {
        panic!("expected upload batch")
    };
    assert_eq!(items.len(), 64);
    assert!(
        items
            .iter()
            .all(|item| !item.hashing && sources.contains(&item.source.location_id))
    );
    let token = first.grants[0].id;
    let reference = || read::GrantRef {
        r#type: read::GrantType::Work,
        id: token,
    };
    sqlx::query("UPDATE tasks SET lease_expires_at=clock_timestamp()+interval '5 seconds' WHERE lease_token=$1").bind(token).execute(pool).await?;
    let renewal = backend
        .pulse_session(
            node,
            key,
            run,
            epoch,
            2,
            false,
            read::KeepAlive {
                renew: vec![reference()],
                release: vec![],
            },
        )
        .await?;
    assert!(renewal.grants[0].accepted);
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM tasks WHERE lease_token=$1 AND lease_expires_at>clock_timestamp()+interval '50 seconds'").bind(token).fetch_one(pool).await?,64);
    let paused = items[1].task;
    sqlx::query("UPDATE tasks SET state='paused' WHERE id=$1")
        .bind(paused)
        .execute(pool)
        .await?;
    let stale = work::Completion {
        operation: first.operation,
        outcomes: vec![work::Outcome::Uploaded; 64],
    };
    assert!(
        backend
            .exchange_work(node, key, request(Some(stale.clone())))
            .await
            .is_err()
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE r.storage_id=$1").bind(storage).fetch_one(pool).await?,70);
    let rejected = backend
        .pulse_session(
            node,
            key,
            run,
            epoch,
            3,
            false,
            read::KeepAlive {
                renew: vec![reference()],
                release: vec![],
            },
        )
        .await?;
    assert!(!rejected.grants[0].accepted);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM tasks WHERE lease_token=$1 AND lease_expires_at>clock_timestamp()"
        )
        .bind(token)
        .fetch_one(pool)
        .await?,
        0
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM storage_permits WHERE storage_id=$1 AND expires_at>clock_timestamp()").bind(storage).fetch_one(pool).await?,0);
    sqlx::query("UPDATE tasks SET state='queued' WHERE id=$1")
        .bind(paused)
        .execute(pool)
        .await?;
    check_owner(backend, key, &outsiders).await?;
    let replacement = backend.exchange_work(node, key, request(None)).await?;
    let work::Plan::UploadRaw { items, .. } = replacement.plan.as_ref().unwrap() else {
        panic!("expected recovered upload batch")
    };
    assert_eq!(items.len(), 64);
    assert_ne!(replacement.grants[0].id, token);
    assert!(
        backend
            .exchange_work(node, key, request(Some(stale)))
            .await
            .is_err()
    );
    backend
        .pulse_session(
            node,
            key,
            run,
            epoch,
            4,
            false,
            read::KeepAlive {
                renew: vec![],
                release: vec![reference()],
            },
        )
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM tasks WHERE lease_token=$1 AND lease_expires_at>clock_timestamp()"
        )
        .bind(replacement.grants[0].id)
        .fetch_one(pool)
        .await?,
        64
    );
    let mut outcomes = vec![work::Outcome::Uploaded; 64];
    outcomes[63] = work::Outcome::Retry;
    let failed = items[63].task;
    let completion = work::Completion {
        operation: replacement.operation,
        outcomes,
    };
    let receipt = Uuid::now_v7();
    let hash: Vec<u8> =
        sqlx::query_scalar("SELECT content_hash FROM raw_uploads WHERE source_id=$1")
            .bind(items[63].source.id)
            .fetch_one(pool)
            .await?;
    let mut hash_lock = pool.begin().await?;
    queries::claim_chunk_hash(storage, &hash)
        .execute(&mut *hash_lock)
        .await?;
    let mut blocked = request(Some(completion.clone()));
    blocked.operation = receipt;
    blocked.max_items = 0;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        backend.exchange_work(node, key, blocked),
    )
    .await
    .context("cache completion waited for a content hash")?;
    assert_eq!(
        model::error(&result.err().unwrap()),
        Some(model::Error::NodeWriteBusy)
    );
    hash_lock.rollback().await?;
    for _ in 0..2 {
        let mut finish = request(Some(completion.clone()));
        finish.operation = receipt;
        finish.max_items = 0;
        assert!(
            backend
                .exchange_work(node, key, finish)
                .await?
                .plan
                .is_none()
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM tasks WHERE id=$1")
            .bind(failed)
            .fetch_one(pool)
            .await?,
        "failed"
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE r.storage_id=$1").bind(storage).fetch_one(pool).await?,7);
    sqlx::query("UPDATE tasks SET updated_at=clock_timestamp()-interval '31 seconds' WHERE id=$1")
        .bind(failed)
        .execute(pool)
        .await?;
    sqlx::query("UPDATE raw_uploads SET next_attempt_at=clock_timestamp() WHERE source_id=$1")
        .bind(items[63].source.id)
        .execute(pool)
        .await?;
    let last = backend.exchange_work(node, key, request(None)).await?;
    let work::Plan::UploadRaw { items, .. } = last.plan.as_ref().unwrap() else {
        panic!("expected remaining uploads")
    };
    assert_eq!(items.len(), 7);
    let mut finish = request(Some(work::Completion {
        operation: last.operation,
        outcomes: vec![work::Outcome::Uploaded; 7],
    }));
    finish.max_items = 0;
    backend.exchange_work(node, key, finish).await?;
    let inventory = backend
        .check_raw_files(
            node,
            key,
            model::node_native::Inventory {
                run,
                files: sources,
                recover: false,
            },
        )
        .await?;
    assert!(
        inventory
            .iter()
            .all(|file| matches!(file.state, raw::FileState::Cached))
    );
    let mut remaining = std::collections::HashSet::new();
    for (index, size) in [
        (120, 33 * 1024 * 1024),
        (121, 33 * 1024 * 1024),
        (122, 65 * 1024 * 1024),
        (123, 3),
    ] {
        remaining.insert(cached(backend, node, key, run, index, size).await?);
    }
    let mut oversized = false;
    while !remaining.is_empty() {
        let batch = backend.exchange_work(node, key, request(None)).await?;
        let work::Plan::UploadRaw { items, .. } = batch.plan.as_ref().unwrap() else {
            panic!("expected byte-bounded uploads")
        };
        let bytes: i64 = items.iter().map(|item| item.source.size).sum();
        assert!(bytes <= 64 * 1024 * 1024 || items.len() == 1);
        oversized |= bytes > 64 * 1024 * 1024;
        for item in items {
            assert!(remaining.remove(&item.source.location_id));
        }
        let mut finish = request(Some(work::Completion {
            operation: batch.operation,
            outcomes: vec![work::Outcome::Uploaded; items.len()],
        }));
        finish.max_items = 0;
        backend.exchange_work(node, key, finish).await?;
    }
    assert!(oversized);
    Ok(())
}

async fn check_owner(
    backend: &Backend,
    key: &Key,
    outsiders: &[(crate::cluster::NodeState, Run)],
) -> Result<()> {
    for (node, run) in outsiders {
        let reply = backend
            .exchange_work(
                node,
                key,
                work::Exchange {
                    run: *run,
                    operation: Uuid::now_v7(),
                    completion: None,
                    class: 2,
                    max_items: 64,
                    cached: vec![],
                },
            )
            .await?;
        assert!(
            reply.plan.is_none(),
            "local uploads were offered to {}",
            node.role
        );
    }
    Ok(())
}

async fn cached(
    backend: &Backend,
    node: &crate::cluster::NodeState,
    key: &Key,
    run: Run,
    index: u8,
    size: i64,
) -> Result<Uuid> {
    let pool = &backend.pool;
    let mut opening = begin(run, &format!("cache-batch-{index}"));
    opening.intent.credential_revision = queries::node_read_credential("writer")
        .fetch_one(pool)
        .await?
        .0;
    let operation = opening.operation;
    backend.begin_write(node, key, opening).await?;
    let input = raw::Input {
        local: true,
        size,
        digest: [index + 32; 16],
        hash: Some([index + 32; 32]),
    };
    let source = backend
        .prepare_raw(
            node,
            key,
            raw::Prepare {
                run,
                operation,
                input: input.clone(),
            },
        )
        .await?
        .source;
    backend
        .commit_raw(
            node,
            key,
            raw::Commit {
                run,
                operation,
                source,
            },
        )
        .await?;
    backend
        .publish_write(
            node,
            key,
            write::Publish {
                run,
                operation,
                size,
                etag: hex::encode(input.digest),
                checksums: Default::default(),
                if_match: None,
                if_none: None,
            },
        )
        .await?;
    Ok(source)
}
