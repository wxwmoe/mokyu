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

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_owner_continues_after_local_task_cleanup() -> Result<()> {
    use crate::authorization::Principal;
    use crate::model::cluster::{NodeSessions, NodeWork};
    use crate::model::{RuntimeSettingsStore, node_work, session};
    use sqlx::Connection;

    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let setup = connection::open(&config, &secrets, &budget, "single").await?;
    sqlx::query("UPDATE mokyu_meta SET deployment_mode='single_control' WHERE id=1")
        .execute(&setup.pool)
        .await?;
    setup.pool.close().await;
    setup.owner.into_inner().close().await?;

    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    RuntimeSettingsStore::ensure(&backend, &crate::model::runtime_defaults(&config)).await?;
    let key = &secrets.credential_key;
    let owner = backend
        .provision_node(&Principal::Local, "owner", "data", key)
        .await?;
    let peer = backend
        .provision_node(&Principal::Local, "peer", "data", key)
        .await?;
    let session = backend
        .open_session(
            &owner,
            key,
            session::Open {
                boot: Uuid::new_v4(),
                connection: Uuid::new_v4(),
                previous_epoch: 0,
            },
        )
        .await?;
    let request = |operation| node_work::Exchange {
        run: session.run,
        operation,
        completion: None,
        class: 2,
        max_items: node_work::MAX_ITEMS,
        cached: vec![],
    };
    let storage = super::storage_tests::fixture(&backend).await?;
    let mut tasks = Vec::new();
    for node in [peer.node_id, owner.node_id, owner.node_id] {
        let task = backend.enqueue("upload", None, json!({
            "operation":"cache", "source_id":"100", "source_node":node, "storage_id":storage,
        })).await?;
        queries::node_files::target(task, node)
            .execute(&backend.pool)
            .await?;
        tasks.push(task);
    }
    let first = Uuid::now_v7();
    for operation in [first, first, Uuid::now_v7()] {
        let reply = backend
            .exchange_work(&owner, key, request(operation))
            .await?;
        assert!(
            reply.plan.is_none()
                && reply.grants.is_empty()
                && reply.check_again
                && !reply.wake_shared
        );
    }
    for operation in [Uuid::now_v7(), first] {
        let reply = backend
            .exchange_work(&owner, key, request(operation))
            .await?;
        assert!(reply.plan.is_none() && reply.grants.is_empty() && !reply.check_again);
    }
    for (id, expected) in tasks.into_iter().zip(["queued", "completed", "completed"]) {
        assert_eq!(
            queries::task_record(id)
                .fetch_one(&backend.pool)
                .await?
                .state,
            expected
        );
    }
    let task = backend
        .enqueue("upload", None, json!({"operation":"cdc"}))
        .await?;
    queries::node_files::target(task, owner.node_id)
        .execute(&backend.pool)
        .await?;
    for conversion in [Some(task), None] {
        let mut tx = backend.pool.begin().await?;
        let source = sqlx::query("INSERT INTO raw_sources(location_id,storage_id,state,size,digest,profile_revision,medium,pending_upload,processing_task) VALUES(?,?,'ready',4,?,1,'local',TRUE,?)")
            .bind(Uuid::new_v4()).bind(storage).bind(&[1u8;16][..]).bind(conversion)
            .execute(&mut *tx).await?.last_insert_id() as i64;
        sqlx::query("INSERT INTO raw_uploads(source_id,deduplicate,background,node_id,token) VALUES(?,FALSE,TRUE,?,?)")
            .bind(source).bind(owner.node_id).bind(Uuid::new_v4()).execute(&mut *tx).await?;
        let upload = queries::raw_upload_record(crate::model::Id::Integer(source))
            .fetch_one(&mut *tx)
            .await?;
        let shared = super::transactions::complete_raw_upload_in(&mut tx, &upload, None).await?;
        assert_eq!(shared, conversion.is_some());
        tx.commit().await?;
    }
    let source_node =
        sqlx::query_scalar::<_, Option<Uuid>>("SELECT source_node FROM tasks WHERE id=?")
            .bind(task)
            .fetch_one(&backend.pool)
            .await?;
    assert!(source_node.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_write_cleanup_skips_busy_receipts() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    let node = Uuid::new_v4();
    queries::node_registry::insert(node, "receipt-owner", "data", &[1])
        .execute(db)
        .await?;
    let ops = [
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
    ];
    for (op, phase) in ops
        .into_iter()
        .zip(["active", "active", "published", "published"])
    {
        sqlx::query("INSERT INTO node_writes(node_id,operation_id,lease_id,boot_id,fence,credential_generation,request_hash,bucket_id,storage_id,profile,intent,phase,expires_at,updated_at) VALUES(?,?,?,?,?,1,?,?,?,'{}','{}',?,UTC_TIMESTAMP(6)-INTERVAL 1 SECOND,UTC_TIMESTAMP(6)-INTERVAL 2 DAY)")
            .bind(node).bind(op).bind(Uuid::new_v4()).bind(Uuid::new_v4()).bind(Uuid::new_v4())
            .bind(&[0u8;32][..]).bind(Uuid::new_v4()).bind(Uuid::new_v4()).bind(phase).execute(db).await?;
    }
    let mut writer = db.begin().await?;
    for op in [ops[0], ops[2]] {
        sqlx::query("SELECT 1 FROM node_writes WHERE node_id=? AND operation_id=? FOR UPDATE")
            .bind(node)
            .bind(op)
            .fetch_one(&mut *writer)
            .await?;
    }
    let mut sweep = db.begin().await?;
    tokio::time::timeout(
        Duration::from_secs(2),
        super::node_writes::expire(&mut sweep),
    )
    .await??;
    sweep.commit().await?;
    sqlx::query("UPDATE node_writes SET expires_at=UTC_TIMESTAMP(6)+INTERVAL 1 HOUR,updated_at=UTC_TIMESTAMP(6) WHERE node_id=? AND operation_id IN (?,?)")
        .bind(node).bind(ops[0]).bind(ops[2]).execute(&mut *writer).await?;
    writer.commit().await?;
    let mut sweep = db.begin().await?;
    super::node_writes::expire(&mut sweep).await?;
    sweep.commit().await?;
    for (op, phase) in
        ops.into_iter()
            .zip([Some("active"), Some("expired"), Some("published"), None])
    {
        let row = sqlx::query_scalar::<_, String>(
            "SELECT phase FROM node_writes WHERE node_id=? AND operation_id=?",
        )
        .bind(node)
        .bind(op)
        .fetch_optional(db)
        .await?;
        assert_eq!(row.as_deref(), phase);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_work_recovery_skips_live_tasks() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    let node = Uuid::new_v4();
    queries::node_registry::insert(node, "work-recovery", "data", &[1])
        .execute(db)
        .await?;
    let mut tasks = Vec::new();
    for (expires, replaced) in [(60, false), (-1, false), (-1, true)] {
        let task = backend.enqueue("upload", None, json!({})).await?;
        let token = Uuid::new_v4();
        queries::claim_task(task, token).execute(db).await?;
        sqlx::query("INSERT INTO node_work(node_id,operation_id,boot_id,fence,request_hash,task_id,token,expires_at) VALUES(?,?,?,?,?,?,?,TIMESTAMPADD(SECOND,?,UTC_TIMESTAMP(6)))")
            .bind(node).bind(Uuid::now_v7()).bind(Uuid::new_v4()).bind(Uuid::new_v4())
            .bind(&[0u8; 32][..]).bind(task).bind(if replaced { Uuid::new_v4() } else { token })
            .bind(expires).execute(db).await?;
        tasks.push(task);
    }
    let mut writer = db.begin().await?;
    sqlx::query("SELECT id FROM tasks WHERE id=? FOR UPDATE")
        .bind(tasks[0])
        .fetch_one(&mut *writer)
        .await?;
    let mut sweep = db.begin().await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), queries::work::requeue(&mut sweep)).await??,
        1
    );
    sweep.commit().await?;
    writer.commit().await?;
    for (task, expected) in tasks.into_iter().zip(["running", "queued", "running"]) {
        assert_eq!(
            queries::task_record(task).fetch_one(db).await?.state,
            expected
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_read_cleanup_preserves_busy_grants() -> Result<()> {
    use crate::authorization::{Action, Permit, Principal};
    use crate::model::{BucketStore, bucket::CreateBucket};

    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let storage = super::storage_tests::fixture(&backend).await?;
    let bucket = BucketStore::create(
        &backend,
        &Principal::Local,
        CreateBucket {
            name: "read-grants".into(),
            project_id: None,
            storage_id: Some(storage),
        },
    )
    .await?;
    let db = &backend.pool;
    let stream = sqlx::query("INSERT INTO streams(bucket_id,object_key,stream_type,state,write_authorization) VALUES(?,'test','object','ready',?)")
        .bind(bucket.id).bind(sqlx::types::Json(Permit::for_action(Principal::Local, bucket.id, Action::Write))).execute(db).await?.last_insert_id();
    let node = Uuid::new_v4();
    queries::node_registry::insert(node, "read-owner", "data", &[1])
        .execute(db)
        .await?;
    let reads = [Uuid::new_v4(), Uuid::new_v4()];
    for id in reads {
        sqlx::query("INSERT INTO node_reads(read_id,node_id,boot_id,fence,credential_generation,operation_id,request_hash,stream_id,payload_revision,offset_bytes,end_bytes,expires_at) VALUES(?,?,?,?,1,?,?,?,1,0,0,UTC_TIMESTAMP(6)-INTERVAL 1 SECOND)")
            .bind(id).bind(node).bind(Uuid::new_v4()).bind(Uuid::new_v4()).bind(Uuid::new_v4()).bind(&[1u8;32][..]).bind(stream).execute(db).await?;
    }
    let mut reader = db.begin().await?;
    sqlx::query("SELECT read_id FROM node_reads WHERE read_id=? FOR UPDATE")
        .bind(reads[0])
        .fetch_one(&mut *reader)
        .await?;
    let mut sweep = db.begin().await?;
    tokio::time::timeout(
        Duration::from_secs(2),
        queries::read_grants::expire(&mut sweep),
    )
    .await??;
    sweep.commit().await?;
    sqlx::query(
        "UPDATE node_reads SET expires_at=UTC_TIMESTAMP(6)+INTERVAL 1 HOUR WHERE read_id=?",
    )
    .bind(reads[0])
    .execute(&mut *reader)
    .await?;
    reader.commit().await?;
    let mut sweep = db.begin().await?;
    queries::read_grants::expire(&mut sweep).await?;
    sweep.commit().await?;
    let retained = sqlx::query_scalar::<_, Uuid>("SELECT read_id FROM node_reads")
        .fetch_all(db)
        .await?;
    assert_eq!(retained, vec![reads[0]]);
    Ok(())
}
