use super::*;
use crate::{
    cluster::NodeState,
    model::{integrity::Detail, session::Run},
    worker::integrity::Mode,
};
use serde_json::Value;

async fn enqueue(backend: &Backend, storage: Uuid, bucket: Uuid, phase: &str) -> Result<Uuid> {
    let detail = Detail {
        storage_id: Some(storage),
        mode: Mode::Full,
        phase: phase.into(),
        bucket_id: Some(bucket),
        bucket: None,
        key: None,
        upper_chunk_id: Uuid::from_u128(u128::MAX).to_string(),
        upper_object: Some((bucket, "z".into())),
        objects_checked: 3,
        chunks_checked: 0,
        bytes_checked: 0,
        issues: 0,
        skipped: 0,
        finished_at: None,
        storages: vec![],
        storage_index: 0,
    };
    Ok(sqlx::query_scalar(
        "INSERT INTO tasks(task_type,state,detail) VALUES('integrity','queued',$1) RETURNING id",
    )
    .bind(serde_json::to_value(detail)?)
    .fetch_one(&backend.pool)
    .await?)
}

async fn state(backend: &Backend, task: Uuid) -> Result<(String, Value)> {
    Ok(sqlx::query_as("SELECT state,detail FROM tasks WHERE id=$1")
        .bind(task)
        .fetch_one(&backend.pool)
        .await?)
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
    let bucket: Uuid = sqlx::query_scalar(
        "INSERT INTO buckets(name,storage_id) VALUES('work-audit',$1) RETURNING id",
    )
    .bind(storage)
    .fetch_one(pool)
    .await?;
    let mut sources = vec![];
    for name in ["a", "b", "c"] {
        let raw = Uuid::now_v7();
        sqlx::query("INSERT INTO raw_sources(id,storage_id,medium,state,size,digest,profile_revision) VALUES($1,$2,'shared_s3','ready',2,$3,1)")
            .bind(raw).bind(storage).bind(vec![1u8;16]).execute(pool).await?;
        let stream: Uuid = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,payload_type) VALUES($1,$2,'object','ready',2,'raw') RETURNING id")
            .bind(bucket).bind(name).fetch_one(pool).await?;
        sqlx::query("INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_offset,source_id) VALUES($1,0,2,0,$2)").bind(stream).bind(raw).execute(pool).await?;
        sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,$2,$3,uuidv7())")
            .bind(bucket).bind(name).bind(stream).execute(pool).await?;
        sources.push(raw);
    }
    let request = |operation, completion, max_items| work::Exchange {
        run,
        operation,
        completion,
        class: 0,
        max_items,
        cached: vec![],
    };
    let task = enqueue(backend, storage, bucket, "raw").await?;
    let operation = Uuid::now_v7();
    let first = backend
        .exchange_work(node, key, request(operation, None, 2))
        .await?;
    let work::Plan::Audit { items, mode, .. } = first.plan.unwrap() else {
        panic!("expected audit")
    };
    assert_eq!(mode, Mode::Full);
    assert_eq!(items.len(), 2);
    for item in &items {
        assert!(
            queries::work::physical_active(item.location)
                .fetch_one(pool)
                .await?
        );
        assert!(item.previous.is_none() && item.targets.len() == 1);
    }
    let invalid = work::Completion {
        operation,
        outcomes: vec![work::Outcome::Deleted { stored_at: None }; 2],
    };
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), Some(invalid), 2))
            .await
            .is_err()
    );
    let completion = work::Completion {
        operation,
        outcomes: vec![
            work::Outcome::Inspected {
                code: Some("hash_mismatch".into()),
            },
            work::Outcome::Inspected { code: None },
        ],
    };
    let receipt = Uuid::now_v7();
    let next = backend
        .exchange_work(node, key, request(receipt, Some(completion.clone()), 2))
        .await?;
    let replay = peer
        .exchange_work(node, key, request(receipt, Some(completion), 2))
        .await?;
    assert_eq!(
        serde_json::to_value(&next.plan)?,
        serde_json::to_value(&replay.plan)?
    );
    let work::Plan::Audit { items, .. } = next.plan.unwrap() else {
        panic!("expected next audit")
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].location, sources[2]);
    let (_, detail) = state(backend, task).await?;
    assert_eq!(detail["issues"], 1);
    assert_eq!(detail["bytes_checked"], 2);
    // Removing an object while its immutable source is being checked is not corruption.
    sqlx::query("DELETE FROM objects WHERE bucket_id=$1 AND object_key='c'")
        .bind(bucket)
        .execute(pool)
        .await?;
    assert!(
        backend
            .exchange_work(
                node,
                key,
                request(
                    Uuid::now_v7(),
                    Some(work::Completion {
                        operation: receipt,
                        outcomes: vec![work::Outcome::Inspected {
                            code: Some("remote_missing".into())
                        }],
                    }),
                    2
                )
            )
            .await?
            .plan
            .is_none()
    );
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), None, 2))
            .await?
            .plan
            .is_none()
    );
    let (status, detail) = state(backend, task).await?;
    assert_eq!(status, "completed");
    assert_eq!(detail["issues"], 1);
    assert_eq!(detail["skipped"], 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM integrity_issues WHERE task_id=$1")
            .bind(task)
            .fetch_one(pool)
            .await?,
        1
    );

    // A failed batch cannot advance the cursor; resume observes the same source and byte budget.
    sqlx::query("UPDATE raw_sources SET size=700000 WHERE id=ANY($1)")
        .bind(&sources)
        .execute(pool)
        .await?;
    sqlx::query("UPDATE storage_profiles SET integrity_byte_budget='1MiB' WHERE storage_id=$1")
        .bind(storage)
        .execute(pool)
        .await?;
    let task = enqueue(backend, storage, bucket, "raw").await?;
    let operation = Uuid::now_v7();
    let first = backend
        .exchange_work(node, key, request(operation, None, 64))
        .await?;
    let work::Plan::Audit { items, .. } = first.plan.unwrap() else {
        panic!("expected bounded audit")
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].location, sources[0]);
    backend
        .exchange_work(
            node,
            key,
            request(
                Uuid::now_v7(),
                Some(work::Completion {
                    operation,
                    outcomes: vec![work::Outcome::Retry],
                }),
                64,
            ),
        )
        .await?;
    assert_eq!(state(backend, task).await?.0, "failed");
    assert!(
        sqlx::query_scalar::<_, Option<String>>("SELECT cursor FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(pool)
            .await?
            .is_none()
    );
    sqlx::query("UPDATE tasks SET state='queued',error=NULL WHERE id=$1")
        .bind(task)
        .execute(pool)
        .await?;
    let mut completion = None;
    for source in &sources[..2] {
        let operation = Uuid::now_v7();
        let reply = peer
            .exchange_work(node, key, request(operation, completion, 64))
            .await?;
        let work::Plan::Audit { items, .. } = reply.plan.unwrap() else {
            panic!("expected resumed audit")
        };
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].location, *source);
        completion = Some(work::Completion {
            operation,
            outcomes: vec![work::Outcome::Inspected { code: None }],
        });
    }
    assert!(
        peer.exchange_work(node, key, request(Uuid::now_v7(), completion, 64))
            .await?
            .plan
            .is_none()
    );
    assert!(
        peer.exchange_work(node, key, request(Uuid::now_v7(), None, 64))
            .await?
            .plan
            .is_none()
    );
    let (status, detail) = state(backend, task).await?;
    assert_eq!(status, "completed");
    assert_eq!(detail["bytes_checked"], 1_400_000);

    let pack: Uuid = sqlx::query_scalar("INSERT INTO packs(storage_id,encryption_algorithm,raw_size,stored_size,member_count,state,pack_fingerprint) VALUES($1,'none',4,120,2,'ready',$2) RETURNING id")
        .bind(storage).bind(vec![3u8;32]).fetch_one(pool).await?;
    let stream: Uuid = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,size) VALUES($1,'z','object','ready',4) RETURNING id").bind(bucket).fetch_one(pool).await?;
    sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,'z',$2,uuidv7())").bind(bucket).bind(stream).execute(pool).await?;
    for ordinal in 0..2i32 {
        let chunk: Uuid = sqlx::query_scalar("INSERT INTO chunks(storage_id,hash,raw_size,encryption_algorithm,state,pack_id) VALUES($1,$2,2,'none','ready',$3) RETURNING id")
            .bind(storage).bind(vec![ordinal as u8;32]).bind(pack).fetch_one(pool).await?;
        sqlx::query(
            "INSERT INTO pack_members(pack_id,chunk_id,ordinal,offset_bytes) VALUES($1,$2,$3,$4)",
        )
        .bind(pack)
        .bind(chunk)
        .bind(ordinal)
        .bind(i64::from(ordinal) * 2)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES($1,$2,2,$3)",
        )
        .bind(stream)
        .bind(i64::from(ordinal) * 2)
        .bind(chunk)
        .execute(pool)
        .await?;
    }
    let task = enqueue(backend, storage, bucket, "chunks").await?;
    let operation = Uuid::now_v7();
    let first = backend
        .exchange_work(node, key, request(operation, None, 1))
        .await?;
    let work::Plan::Audit { items, .. } = first.plan.unwrap() else {
        panic!("expected pack audit")
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].members.len(), 2);
    assert!(items[0].previous.is_none());
    let receipt = Uuid::now_v7();
    let completion = work::Completion {
        operation,
        outcomes: vec![work::Outcome::Inspected { code: None }],
    };
    let next = backend
        .exchange_work(node, key, request(receipt, Some(completion.clone()), 1))
        .await?;
    let replay = peer
        .exchange_work(node, key, request(receipt, Some(completion), 1))
        .await?;
    assert_eq!(
        serde_json::to_value(&next.plan)?,
        serde_json::to_value(&replay.plan)?
    );
    let work::Plan::Audit { items, .. } = replay.plan.unwrap() else {
        panic!("expected reused pack audit")
    };
    assert!(items[0].members.is_empty());
    assert!(matches!(
        &items[0].previous,
        Some(work::Outcome::Inspected { code: None })
    ));
    assert!(
        backend
            .exchange_work(
                node,
                key,
                request(
                    Uuid::now_v7(),
                    Some(work::Completion {
                        operation: receipt,
                        outcomes: vec![work::Outcome::Inspected {
                            code: Some("hash_mismatch".into())
                        }],
                    }),
                    1
                )
            )
            .await
            .is_err()
    );
    assert!(
        backend
            .exchange_work(
                node,
                key,
                request(
                    Uuid::now_v7(),
                    Some(work::Completion {
                        operation: receipt,
                        outcomes: vec![work::Outcome::Inspected { code: None }],
                    }),
                    1
                )
            )
            .await?
            .plan
            .is_none()
    );
    let (status, detail) = state(backend, task).await?;
    assert_eq!(status, "completed");
    assert_eq!(detail["chunks_checked"], 2);
    assert_eq!(detail["bytes_checked"], 120);
    assert_eq!(detail["issues"], 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM integrity_packs WHERE task_id=$1")
            .bind(task)
            .fetch_one(pool)
            .await?,
        1
    );
    Ok(())
}
