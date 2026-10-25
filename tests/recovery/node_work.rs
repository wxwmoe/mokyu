use super::*;
use crate::model::{node_read as read, node_work as work};

#[path = "node_audit.rs"]
mod audit;
#[path = "node_cleanup.rs"]
mod cleanup;
#[path = "node_conversion.rs"]
mod conversion;
#[path = "node_inspection.rs"]
mod inspection;
#[path = "node_packing.rs"]
mod packing;
#[path = "node_processing.rs"]
mod processing;
#[path = "node_rewrite.rs"]
mod rewrite;
#[path = "node_sweep.rs"]
mod sweep;

pub(super) async fn check(
    backend: &Backend,
    peer: &Backend,
    admin: &Principal,
    key: &Key,
    bucket: Uuid,
) -> Result<()> {
    let pool = &backend.pool;
    let node = backend
        .provision_node(admin, "work-owner", "worker", key)
        .await?;
    let other = backend
        .provision_node(admin, "work-other", "data", key)
        .await?;
    let session = backend
        .open_session(
            &node,
            key,
            Open {
                boot: Uuid::new_v4(),
                connection: Uuid::new_v4(),
                previous_epoch: 0,
            },
        )
        .await?;
    let run = session.run;
    let storage = Uuid::now_v7();
    sqlx::query("INSERT INTO storages(id,name,type,endpoint_url,remote_bucket_name,region,access_key_encrypted,secret_key_encrypted,identity_marker) VALUES($1,'work-fixture','s3','http://backend:8333','work-fixture','us-east-1',$2,$3,$4)")
        .bind(storage).bind(codec::protect(b"work-access", key, format!("storage-{storage}-access").as_bytes())?)
        .bind(codec::protect(b"work-secret", key, format!("storage-{storage}-secret").as_bytes())?)
        .bind(crate::storage::model::identity_marker(node.deployment_id, storage)?).execute(pool).await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id,unreferenced_grace,min_storage_duration,control_concurrency,maintenance_concurrency) VALUES($1,'1s','0s',1,1)").bind(storage).execute(pool).await?;
    let packs: Vec<Uuid> = sqlx::query_scalar("INSERT INTO packs(encryption_algorithm,raw_size,stored_size,member_count,state,unreferenced_at,storage_id,pack_fingerprint) SELECT 'none',2,2,2,'retired',clock_timestamp()-interval '2 days',$1,$2 FROM generate_series(1,70) RETURNING id")
        .bind(storage).bind(vec![4u8;32]).fetch_all(pool).await?;
    let stream: Uuid = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state) VALUES($1,'work-pinned','object','ready') RETURNING id").bind(bucket).fetch_one(pool).await?;
    let lease: Uuid = sqlx::query_scalar("INSERT INTO stream_leases(stream_id,lease_type,expires_at) VALUES($1,'read',clock_timestamp()+interval '1 minute') RETURNING id").bind(stream).fetch_one(pool).await?;
    sqlx::query("INSERT INTO node_reads(read_id,node_id,credential_generation,boot_id,fence,operation_id,request_hash,payload_revision,offset_bytes,end_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,1,0,2)")
        .bind(lease).bind(node.node_id).bind(node.credential_generation).bind(run.boot).bind(run.fence).bind(Uuid::now_v7()).bind(vec![1u8;32]).execute(pool).await?;
    let window: Uuid = sqlx::query_scalar("INSERT INTO node_read_windows(read_id,offset_bytes,end_bytes,mapping) VALUES($1,0,2,'{}') RETURNING id").bind(lease).fetch_one(pool).await?;
    sqlx::query("INSERT INTO node_read_sources(window_id,kind,location_id) VALUES($1,'pack',$2)")
        .bind(window)
        .bind(packs[0])
        .execute(pool)
        .await?;
    let raw = Uuid::now_v7();
    sqlx::query("INSERT INTO raw_sources(id,storage_id,medium,state,size,digest,profile_revision,created_at,unreferenced_at,upload_id) VALUES($1,$2,'shared_s3','ready',2,$3,1,clock_timestamp()-interval '2 days',clock_timestamp()-interval '2 days','unfinished-upload')")
        .bind(raw).bind(storage).bind(vec![2u8;16]).execute(pool).await?;
    sqlx::query("INSERT INTO staging_objects(storage_id,object_key,bytes,created_at) VALUES($1,'staging/work-fixture',3,clock_timestamp()-interval '10 minutes')").bind(storage).execute(pool).await?;
    let task: Uuid = sqlx::query_scalar("INSERT INTO tasks(task_type,state,detail,execution_scope,target_node_id) VALUES('gc','queued',jsonb_build_object('storage_id',$1::uuid),'node',$2) RETURNING id").bind(storage).bind(other.node_id).fetch_one(pool).await?;
    let request = |operation, completion| work::Exchange {
        run,
        operation,
        completion,
        class: 0,
        max_items: 64,
        cached: vec![],
    };
    assert!(
        backend
            .exchange_work(&node, key, request(Uuid::now_v7(), None))
            .await?
            .plan
            .is_none()
    );
    sqlx::query("UPDATE tasks SET target_node_id=$2 WHERE id=$1")
        .bind(task)
        .bind(node.node_id)
        .execute(pool)
        .await?;
    let mut wrong = request(Uuid::now_v7(), None);
    wrong.run.fence = Uuid::new_v4();
    assert!(backend.exchange_work(&node, key, wrong).await.is_err());
    let occupied = Uuid::new_v4();
    queries::insert_storage_permit(occupied, storage, "control", other.node_id)
        .execute(pool)
        .await?;
    assert!(
        backend
            .exchange_work(&node, key, request(Uuid::now_v7(), None))
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
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM packs WHERE storage_id=$1 AND state='retired'"
        )
        .bind(storage)
        .fetch_one(pool)
        .await?,
        70
    );
    queries::release_storage_permit(occupied)
        .execute(pool)
        .await?;
    let first = Uuid::now_v7();
    let plan = backend
        .exchange_work(&node, key, request(first, None))
        .await?;
    let work::Plan::Collect { items, .. } = plan.plan.as_ref().unwrap() else {
        panic!("expected collection plan")
    };
    assert_eq!(items.len(), 64);
    assert!(items.iter().all(|item| item.location != packs[0]));
    let mut cached = request(first, None);
    cached.cached = plan
        .materials
        .iter()
        .map(|value| value.version.clone())
        .collect();
    let repeated = peer.exchange_work(&node, key, cached).await?;
    assert!(repeated.materials.iter().all(|value| value.json.is_none()));
    assert_eq!(
        serde_json::to_value(&plan.plan)?,
        serde_json::to_value(&repeated.plan)?
    );
    let mut changed = request(first, None);
    changed.max_items = 32;
    assert!(peer.exchange_work(&node, key, changed).await.is_err());
    let renewed = peer
        .pulse_session(
            &node,
            key,
            run,
            session.epoch,
            1,
            false,
            read::KeepAlive {
                renew: plan
                    .grants
                    .iter()
                    .map(|grant| read::GrantRef {
                        r#type: grant.r#type,
                        id: grant.id,
                    })
                    .collect(),
                release: vec![],
            },
        )
        .await?;
    assert_eq!(renewed.grants.len(), 1);
    assert!(renewed.grants[0].accepted);
    let completion = work::Completion {
        operation: first,
        outcomes: vec![work::Outcome::Deleted { stored_at: None }; 64],
    };
    let second = Uuid::now_v7();
    let next = peer
        .exchange_work(&node, key, request(second, Some(completion.clone())))
        .await?;
    let work::Plan::Collect { items, .. } = next.plan.as_ref().unwrap() else {
        panic!("expected collection plan")
    };
    assert_eq!(items.len(), 7);
    assert!(items.iter().any(|item| matches!(&item.source,work::Physical::Raw{upload:Some(upload)} if upload=="unfinished-upload")));
    assert!(
        items
            .iter()
            .any(|item| matches!(item.source, work::Physical::Staging { .. }))
    );
    backend
        .exchange_work(&node, key, request(second, Some(completion.clone())))
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT processed FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(pool)
            .await?,
        64
    );
    let mut bad = completion.clone();
    bad.outcomes[0] = work::Outcome::Retry;
    assert!(
        backend
            .exchange_work(&node, key, request(second, Some(bad)))
            .await
            .is_err()
    );
    let done = work::Completion {
        operation: second,
        outcomes: vec![work::Outcome::Deleted { stored_at: None }; 7],
    };
    let third = Uuid::now_v7();
    assert!(
        backend
            .exchange_work(&node, key, request(third, Some(done.clone())))
            .await?
            .plan
            .is_none()
    );
    peer.exchange_work(&node, key, request(third, Some(done)))
        .await?;
    let (state, processed): (String, i64) =
        sqlx::query_as("SELECT state,processed FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(pool)
            .await?;
    assert_eq!((state.as_str(), processed), ("completed", 71));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=$1")
            .bind(packs[0])
            .fetch_one(pool)
            .await?,
        "retired"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM storage_permits WHERE storage_id=$1")
            .bind(storage)
            .fetch_one(pool)
            .await?,
        0
    );
    // An unknown physical age defers deletion until the backend's retention is satisfied.
    sqlx::query("DELETE FROM stream_leases WHERE id=$1")
        .bind(lease)
        .execute(pool)
        .await?;
    sqlx::query("UPDATE storage_profiles SET min_storage_duration='1d' WHERE storage_id=$1")
        .bind(storage)
        .execute(pool)
        .await?;
    sqlx::query("UPDATE tasks SET state='queued' WHERE id=$1")
        .bind(task)
        .execute(pool)
        .await?;
    let fourth = Uuid::now_v7();
    let plan = backend
        .exchange_work(&node, key, request(fourth, None))
        .await?;
    let work::Plan::Collect { items, .. } = plan.plan.unwrap() else {
        panic!("expected collection plan")
    };
    assert_eq!(items.len(), 1);
    assert!(items[0].inspect_age);
    let retained = work::Completion {
        operation: fourth,
        outcomes: vec![work::Outcome::Retained {
            stored_at: chrono::Utc::now(),
        }],
    };
    assert!(
        peer.exchange_work(&node, key, request(Uuid::now_v7(), Some(retained)))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=$1")
            .bind(packs[0])
            .fetch_one(pool)
            .await?,
        "deleting"
    );
    // A late result cannot complete a replacement execution, and an expired grant cannot revive.
    sqlx::query("UPDATE storage_profiles SET min_storage_duration='0s' WHERE storage_id=$1")
        .bind(storage)
        .execute(pool)
        .await?;
    sqlx::query("UPDATE tasks SET state='queued' WHERE id=$1")
        .bind(task)
        .execute(pool)
        .await?;
    let expired = Uuid::now_v7();
    let old = backend
        .exchange_work(&node, key, request(expired, None))
        .await?;
    sqlx::query("UPDATE node_work SET expires_at=clock_timestamp()-interval '1 second' WHERE node_id=$1 AND operation_id=$2").bind(node.node_id).bind(expired).execute(pool).await?;
    sqlx::query(
        "UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(task)
    .execute(pool)
    .await?;
    sqlx::query("UPDATE storage_permits SET expires_at=clock_timestamp()-interval '1 second' WHERE storage_id=$1").bind(storage).execute(pool).await?;
    let stale = work::Completion {
        operation: expired,
        outcomes: vec![work::Outcome::Deleted { stored_at: None }],
    };
    assert!(
        peer.exchange_work(&node, key, request(Uuid::now_v7(), Some(stale)))
            .await
            .is_err()
    );
    let renewed = peer
        .pulse_session(
            &node,
            key,
            run,
            session.epoch,
            2,
            false,
            read::KeepAlive {
                renew: old
                    .grants
                    .iter()
                    .map(|grant| read::GrantRef {
                        r#type: grant.r#type,
                        id: grant.id,
                    })
                    .collect(),
                release: vec![],
            },
        )
        .await?;
    assert!(!renewed.grants[0].accepted);
    let recovery = Uuid::now_v7();
    let replacement = peer
        .exchange_work(&node, key, request(recovery, None))
        .await?;
    assert_ne!(replacement.grants[0].id, old.grants[0].id);
    let result = work::Completion {
        operation: recovery,
        outcomes: vec![work::Outcome::Deleted { stored_at: None }],
    };
    assert!(
        backend
            .exchange_work(&node, key, request(Uuid::now_v7(), Some(result)))
            .await?
            .plan
            .is_none()
    );
    // Metadata progress commits with its changes even when no worker result survives.
    let metadata_bucket: Uuid = sqlx::query_scalar(
        "INSERT INTO buckets(name,storage_id) VALUES('work-metadata',$1) RETURNING id",
    )
    .bind(storage)
    .fetch_one(pool)
    .await?;
    let orphan: Uuid = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,touched_at) VALUES($1,'expired','object','retired',clock_timestamp()-interval '2 days') RETURNING id").bind(metadata_bucket).fetch_one(pool).await?;
    sqlx::query("UPDATE storage_profiles SET upload_retention='1s' WHERE storage_id=$1")
        .bind(storage)
        .execute(pool)
        .await?;
    sqlx::query("UPDATE tasks SET state='queued',processed=0 WHERE id=$1")
        .bind(task)
        .execute(pool)
        .await?;
    let metadata = Uuid::now_v7();
    let plan = backend
        .exchange_work(&node, key, request(metadata, None))
        .await?;
    let work::Plan::Collect {
        items,
        metadata_count,
        ..
    } = plan.plan.unwrap()
    else {
        panic!("expected collection plan")
    };
    assert!(items.is_empty());
    assert_eq!(metadata_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT processed FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM streams WHERE id=$1")
            .bind(orphan)
            .fetch_one(pool)
            .await?,
        0
    );
    sqlx::query("UPDATE node_work SET expires_at=clock_timestamp()-interval '1 second' WHERE node_id=$1 AND operation_id=$2").bind(node.node_id).bind(metadata).execute(pool).await?;
    sqlx::query(
        "UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(task)
    .execute(pool)
    .await?;
    sqlx::query("UPDATE storage_permits SET expires_at=clock_timestamp()-interval '1 second' WHERE storage_id=$1").bind(storage).execute(pool).await?;
    assert!(
        peer.exchange_work(&node, key, request(Uuid::now_v7(), None))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(
        sqlx::query_as::<_, (String, i64)>("SELECT state,processed FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(pool)
            .await?,
        ("completed".into(), 1)
    );
    // Partial failure keeps committed progress and releases both shared permits.
    let failures: Vec<Uuid> = sqlx::query_scalar("INSERT INTO packs(encryption_algorithm,raw_size,stored_size,member_count,state,unreferenced_at,storage_id,pack_fingerprint) SELECT 'none',2,2,2,'retired',clock_timestamp()-interval '2 days',$1,$2 FROM generate_series(1,2) RETURNING id").bind(storage).bind(vec![5u8;32]).fetch_all(pool).await?;
    sqlx::query("UPDATE tasks SET state='queued',processed=0,execution_scope='cluster',target_node_id=NULL WHERE id=$1").bind(task).execute(pool).await?;
    let partial = Uuid::now_v7();
    let plan = backend
        .exchange_work(&node, key, request(partial, None))
        .await?;
    let work::Plan::Collect { items, .. } = plan.plan.unwrap() else {
        panic!("expected collection plan")
    };
    assert_eq!(items.len(), 2);
    assert!(items.iter().all(|item| failures.contains(&item.location)));
    let result = work::Completion {
        operation: partial,
        outcomes: vec![
            work::Outcome::Deleted { stored_at: None },
            work::Outcome::Retry,
        ],
    };
    assert!(
        peer.exchange_work(&node, key, request(Uuid::now_v7(), Some(result)))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(
        sqlx::query_as::<_, (String, i64)>("SELECT state,processed FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(pool)
            .await?,
        ("failed".into(), 1)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM storage_permits WHERE storage_id=$1")
            .bind(storage)
            .fetch_one(pool)
            .await?,
        0
    );
    // Shared work resumes on a data node without the previous worker's identity.
    sqlx::query("UPDATE tasks SET state='queued' WHERE id=$1")
        .bind(task)
        .execute(pool)
        .await?;
    let other_session = backend
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
    let id = Uuid::now_v7();
    let mut input = request(id, None);
    input.run = other_session.run;
    let plan = peer.exchange_work(&other, key, input).await?;
    let work::Plan::Collect { items, .. } = plan.plan.unwrap() else {
        panic!("expected collection plan")
    };
    assert_eq!(items.len(), 1);
    let mut input = request(
        Uuid::now_v7(),
        Some(work::Completion {
            operation: id,
            outcomes: vec![work::Outcome::Deleted { stored_at: None }],
        }),
    );
    input.run = other_session.run;
    assert!(
        backend
            .exchange_work(&other, key, input)
            .await?
            .plan
            .is_none()
    );
    assert_eq!(
        sqlx::query_as::<_, (String, i64)>("SELECT state,processed FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(pool)
            .await?,
        ("completed".into(), 2)
    );
    cleanup::check(
        backend,
        &other,
        key,
        other_session.run,
        storage,
        node.node_id,
    )
    .await?;
    inspection::check(backend, peer, &other, key, other_session.run, storage).await?;
    audit::check(backend, peer, &other, key, other_session.run, storage).await?;
    sweep::check(
        backend,
        peer,
        &other,
        key,
        other_session.run,
        storage,
        bucket,
    )
    .await?;
    rewrite::check(backend, peer, &other, key, other_session.run, admin).await?;
    packing::check(backend, peer, &other, key, other_session.run).await?;
    processing::check(backend, peer, &other, key, other_session.run).await?;
    conversion::check(backend, peer, admin, key).await?;
    Ok(())
}
