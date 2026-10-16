use super::{Backend, cache_tests, connection};
use crate::authorization::Principal;
use crate::config::Config;
use crate::model::bucket::CreateBucket;
use crate::model::task::Execution;
use crate::model::{
    AccountStore, BucketStore, TaskClaim, TaskControlStore, TaskExecutionStore, TaskFinish,
    UploadCacheStore,
};
use anyhow::Result;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

fn claim(class: i32, token: Uuid) -> TaskClaim<'static> {
    TaskClaim {
        class,
        active: &[],
        token,
    }
}
async fn claimed(backend: &Backend, id: Uuid, class: i32) -> Result<Execution> {
    let token = Uuid::new_v4();
    assert_eq!(backend.claim(claim(class, token)).await?, Some(id));
    Ok(Execution { id, token })
}
async fn finish(backend: &Backend, execution: Execution, error: Option<&str>) -> Result<()> {
    TaskExecutionStore::finish(
        backend,
        TaskFinish {
            execution,
            reuse_seconds: 60,
            error: error.map(str::to_owned),
        },
    )
    .await
}
async fn claim_all(backend: &Backend, class: i32) -> Result<HashSet<Uuid>> {
    let mut ids = HashSet::new();
    while let Some(id) = backend.claim(claim(class, Uuid::new_v4())).await? {
        assert!(ids.insert(id));
    }
    Ok(ids)
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_task_execution_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    backend.initialize_admin("admin", "password", false).await?;
    let storage = super::storage_tests::fixture(&backend).await?;
    assert!(backend.claim(claim(0, Uuid::new_v4())).await?.is_none());
    let first = TaskControlStore::enqueue(&backend, "integrity", None, json!({})).await?;
    let tokens = [Uuid::new_v4(), Uuid::new_v4()];
    let (a, b) = tokio::join!(
        backend.claim(claim(0, tokens[0])),
        backend.claim(claim(0, tokens[1]))
    );
    let results = [a?, b?];
    assert_eq!(results.iter().filter(|id| id.is_some()).count(), 1);
    let winner = results.iter().position(|id| id.is_some()).unwrap();
    let execution = Execution {
        id: first,
        token: tokens[winner],
    };
    let stale = Execution {
        id: first,
        token: tokens[1 - winner],
    };
    let record = TaskExecutionStore::load(&backend, first).await?;
    assert_eq!(
        (
            record.task_type.as_str(),
            record.state.as_str(),
            record.execution_token
        ),
        ("integrity", "running", Some(execution.token))
    );
    let started = backend.raw(first).await?.unwrap()["started_at"].clone();
    assert!(!started.is_null());
    sqlx::query("UPDATE tasks SET updated_at=TIMESTAMPADD(DAY,-1,CURRENT_TIMESTAMP(6)) WHERE id=?")
        .bind(first)
        .execute(db)
        .await?;
    assert!(backend.claim(claim(0, Uuid::new_v4())).await?.is_none());
    assert!(!backend.confirm(first, stale.token).await?);
    assert!(!backend.fence("single", first, stale.token).await?);
    assert!(backend.fence("single", first, execution.token).await?);
    finish(&backend, stale, Some("stale")).await?;
    assert!(backend.confirm(first, execution.token).await?);
    backend
        .progress(execution, "cursor", 3, json!({"checked":3}))
        .await?;
    let record = TaskExecutionStore::load(&backend, first).await?;
    assert_eq!(record.cursor.as_deref(), Some("cursor"));
    assert_eq!(record.detail, json!({"checked":3}));
    assert_eq!(record.bucket_id, None);
    finish(&backend, execution, None).await?;
    assert_eq!(
        TaskExecutionStore::load(&backend, first)
            .await?
            .execution_token,
        None
    );
    assert!(
        backend
            .claim(TaskClaim {
                class: 0,
                active: &[first],
                token: Uuid::new_v4()
            })
            .await?
            .is_none()
    );
    let next = claimed(&backend, first, 0).await?;
    assert_eq!(backend.raw(first).await?.unwrap()["started_at"], started);
    assert!(!backend.confirm(first, execution.token).await?);
    backend.complete(next).await?;
    finish(&backend, next, Some("late failure")).await?;
    assert_eq!(
        TaskExecutionStore::load(&backend, first).await?.state,
        "completed"
    );
    assert!(backend.raw(first).await?.unwrap()["error"].is_null());
    assert!(
        !backend
            .fence("single", Uuid::new_v4(), Uuid::new_v4())
            .await?
    );

    let mut tasks = HashMap::new();
    for (name, task_type, detail) in [
        ("pack", "pack", json!({"policy":"pack"})),
        ("repack", "pack", json!({"policy":"repack"})),
        ("reuse", "pack", json!({"policy":"reuse"})),
        ("unpack", "unpack", json!({})),
        ("gc", "gc", json!({})),
        ("cleanup", "cleanup", json!({})),
        ("purge", "purge", json!({})),
        ("usage", "usage", json!({})),
        ("sweep", "sweep", json!({})),
        ("integrity", "integrity", json!({})),
        ("upload", "upload", json!({})),
        ("flush", "cache_flush", json!({})),
        ("migration", "storage_migration", json!({})),
        ("rewrite", "rewrite", json!({})),
    ] {
        tasks.insert(
            name,
            TaskControlStore::enqueue(&backend, task_type, None, detail).await?,
        );
    }
    sqlx::query("UPDATE mokyu_meta SET maintenance=TRUE,gc_paused=TRUE,pack_creation_paused=TRUE")
        .execute(db)
        .await?;
    sqlx::query("UPDATE maintenance_controls SET paused=TRUE WHERE policy IN ('cleanup','reuse')")
        .execute(db)
        .await?;
    assert_eq!(
        claim_all(&backend, 0).await?,
        HashSet::from([tasks["sweep"], tasks["integrity"]])
    );
    assert!(claim_all(&backend, 1).await?.is_empty());
    assert_eq!(
        claim_all(&backend, 2).await?,
        HashSet::from([tasks["upload"], tasks["flush"]])
    );
    sqlx::query("UPDATE mokyu_meta SET maintenance=FALSE")
        .execute(db)
        .await?;
    assert_eq!(
        claim_all(&backend, 1).await?,
        HashSet::from([tasks["unpack"]])
    );
    sqlx::query("UPDATE maintenance_controls SET paused=FALSE WHERE policy='reuse'")
        .execute(db)
        .await?;
    assert_eq!(
        claim_all(&backend, 1).await?,
        HashSet::from([tasks["reuse"]])
    );
    sqlx::query("UPDATE mokyu_meta SET gc_paused=FALSE")
        .execute(db)
        .await?;
    assert_eq!(
        claim_all(&backend, 0).await?,
        HashSet::from([tasks["gc"], tasks["purge"], tasks["usage"]])
    );
    sqlx::query("UPDATE mokyu_meta SET pack_creation_paused=FALSE")
        .execute(db)
        .await?;
    assert_eq!(
        claim_all(&backend, 1).await?,
        HashSet::from([tasks["pack"], tasks["repack"]])
    );
    sqlx::query("UPDATE maintenance_controls SET paused=FALSE WHERE policy='cleanup'")
        .execute(db)
        .await?;
    assert_eq!(
        claim_all(&backend, 0).await?,
        HashSet::from([tasks["cleanup"]])
    );
    assert_eq!(
        claim_all(&backend, 2).await?,
        HashSet::from([tasks["migration"], tasks["rewrite"]])
    );
    assert!(backend.claim(claim(3, Uuid::new_v4())).await?.is_none());
    sqlx::query("UPDATE tasks SET state='completed',execution_token=NULL")
        .execute(db)
        .await?;

    let revision: i64 =
        sqlx::query_scalar("SELECT revision FROM storage_profiles WHERE storage_id=?")
            .bind(storage)
            .fetch_one(db)
            .await?;
    let rewrite = TaskControlStore::enqueue(
        &backend,
        "rewrite",
        None,
        json!({"storage_id":storage,"profile_revision":revision}),
    )
    .await?;
    let task = claimed(&backend, rewrite, 2).await?;
    assert!(backend.fence("single", task.id, task.token).await?);
    sqlx::query("UPDATE storage_profiles SET revision=revision+1 WHERE storage_id=?")
        .bind(storage)
        .execute(db)
        .await?;
    assert!(!backend.fence("single", task.id, Uuid::new_v4()).await?);
    assert_eq!(
        TaskExecutionStore::load(&backend, rewrite).await?.state,
        "running"
    );
    assert!(!backend.fence("single", task.id, task.token).await?);
    assert_eq!(
        TaskExecutionStore::load(&backend, rewrite).await?.state,
        "paused"
    );
    assert_eq!(
        TaskExecutionStore::load(&backend, rewrite)
            .await?
            .execution_token,
        Some(task.token)
    );
    assert!(
        backend.raw(rewrite).await?.unwrap()["error"]
            .as_str()
            .unwrap()
            .contains("profile changed")
    );
    finish(&backend, task, None).await?;
    assert_eq!(
        TaskExecutionStore::load(&backend, rewrite)
            .await?
            .execution_token,
        None
    );
    assert!(backend.change_state(rewrite, false, true).await?);
    let task = claimed(&backend, rewrite, 2).await?;
    assert!(backend.fence("single", task.id, task.token).await?);
    assert!(!backend.fence("cluster", task.id, task.token).await?);
    finish(&backend, task, None).await?;
    assert!(backend.change_state(rewrite, false, true).await?);
    let task = claimed(&backend, rewrite, 2).await?;
    sqlx::query("UPDATE tasks SET detail=? WHERE id=?")
        .bind(json!({"storage_id":Uuid::new_v4(),"profile_revision":1}))
        .bind(rewrite)
        .execute(db)
        .await?;
    assert!(!backend.fence("single", task.id, task.token).await?);
    finish(&backend, task, None).await?;
    sqlx::query("UPDATE tasks SET state='completed' WHERE id=?")
        .bind(rewrite)
        .execute(db)
        .await?;

    let bucket = BucketStore::create(
        &backend,
        &Principal::Local,
        CreateBucket {
            name: "execution".into(),
            project_id: None,
            storage_id: Some(storage),
        },
    )
    .await?
    .id;
    let stream = cache_tests::stream(&backend, bucket, "pending").await?;
    let c0 = cache_tests::chunk(&backend, storage, stream, 1).await?;
    let c1 = cache_tests::chunk(&backend, storage, stream, 2).await?;
    cache_tests::stage(&backend, &c0, stream, 0, true).await?;
    cache_tests::stage(&backend, &c1, stream, 32, true).await?;
    let old_pack = cache_tests::pack(&backend, storage, first).await?;
    sqlx::query("UPDATE packs SET state='ready',stored_size=64 WHERE id=?")
        .bind(old_pack)
        .execute(db)
        .await?;
    sqlx::query("UPDATE chunks SET pack_id=? WHERE id=?")
        .bind(old_pack)
        .bind(c0.id)
        .execute(db)
        .await?;
    sqlx::query("INSERT INTO pack_changes(pack_id) VALUES(?)")
        .bind(old_pack)
        .execute(db)
        .await?;
    sqlx::query("INSERT INTO pack_maintenance(stream_id) VALUES(?)")
        .bind(stream)
        .execute(db)
        .await?;
    let packing =
        TaskControlStore::enqueue(&backend, "pack", None, json!({"policy":"pack"})).await?;
    let task = claimed(&backend, packing, 1).await?;
    let output = cache_tests::pack(&backend, storage, packing).await?;
    sqlx::query("UPDATE chunk_encodings SET owner_task=? WHERE encoding_id=?")
        .bind(packing)
        .bind(c0.encoding_id)
        .execute(db)
        .await?;
    sqlx::query("INSERT INTO pack_inputs(task_id,chunk_id) VALUES(?,?)")
        .bind(packing)
        .bind(c0.id)
        .execute(db)
        .await?;
    finish(
        &backend,
        Execution {
            id: packing,
            token: Uuid::new_v4(),
        },
        Some("stale"),
    )
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=?")
            .bind(output)
            .fetch_one(db)
            .await?,
        "preparing"
    );
    finish(&backend, task, Some("pack failure")).await?;
    assert_eq!(
        TaskExecutionStore::load(&backend, packing).await?.state,
        "failed"
    );
    assert_eq!(
        TaskExecutionStore::load(&backend, packing)
            .await?
            .execution_token,
        None
    );
    assert_eq!(
        backend.raw(packing).await?.unwrap()["error"],
        "pack failure"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=?")
            .bind(output)
            .fetch_one(db)
            .await?,
        "retired"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=?")
            .bind(old_pack)
            .fetch_one(db)
            .await?,
        "ready"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM chunk_encodings WHERE encoding_id=?")
            .bind(c0.encoding_id)
            .fetch_one(db)
            .await?,
        "retired"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pack_inputs WHERE task_id=?")
            .bind(packing)
            .fetch_one(db)
            .await?,
        0
    );
    assert!(sqlx::query_scalar::<_,bool>("SELECT next_check_at>TIMESTAMPADD(SECOND,58,CURRENT_TIMESTAMP(6)) FROM pack_changes WHERE pack_id=?").bind(old_pack).fetch_one(db).await?);
    assert!(sqlx::query_scalar::<_,bool>("SELECT next_check_at>TIMESTAMPADD(SECOND,58,CURRENT_TIMESTAMP(6)) FROM pack_maintenance WHERE stream_id=?").bind(stream).fetch_one(db).await?);
    assert!(backend.change_state(packing, false, true).await?);
    let task = claimed(&backend, packing, 1).await?;
    let held = cache_tests::pack(&backend, storage, packing).await?;
    sqlx::query("INSERT INTO pack_inputs(task_id,chunk_id) VALUES(?,?)")
        .bind(packing)
        .bind(c1.id)
        .execute(db)
        .await?;
    finish(&backend, task, None).await?;
    assert_eq!(
        TaskExecutionStore::load(&backend, packing).await?.state,
        "queued"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=?")
            .bind(held)
            .fetch_one(db)
            .await?,
        "preparing"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pack_inputs WHERE task_id=?")
            .bind(packing)
            .fetch_one(db)
            .await?,
        1
    );
    let task = claimed(&backend, packing, 1).await?;
    TaskControlStore::pause(&backend, task).await?;
    finish(&backend, task, Some("pause is not failure")).await?;
    assert_eq!(
        TaskExecutionStore::load(&backend, packing).await?.state,
        "paused"
    );
    assert!(backend.raw(packing).await?.unwrap()["error"].is_null());
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=?")
            .bind(held)
            .fetch_one(db)
            .await?,
        "retired"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pack_inputs WHERE task_id=?")
            .bind(packing)
            .fetch_one(db)
            .await?,
        0
    );

    assert!(backend.change_state(packing, false, true).await?);
    let task = claimed(&backend, packing, 1).await?;
    let resumed_output = cache_tests::pack(&backend, storage, packing).await?;
    sqlx::query("INSERT INTO pack_inputs(task_id,chunk_id) VALUES(?,?)")
        .bind(packing)
        .bind(c1.id)
        .execute(db)
        .await?;
    TaskControlStore::pause(&backend, task).await?;
    assert!(backend.change_state(packing, false, true).await?);
    finish(&backend, task, Some("late paused executor")).await?;
    assert_eq!(
        TaskExecutionStore::load(&backend, packing).await?.state,
        "queued"
    );
    assert!(backend.raw(packing).await?.unwrap()["error"].is_null());
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=?")
            .bind(resumed_output)
            .fetch_one(db)
            .await?,
        "retired"
    );
    let task = claimed(&backend, packing, 1).await?;
    let profile_output = cache_tests::pack(&backend, storage, packing).await?;
    sqlx::query("INSERT INTO pack_inputs(task_id,chunk_id) VALUES(?,?)")
        .bind(packing)
        .bind(c1.id)
        .execute(db)
        .await?;
    sqlx::query("UPDATE tasks SET detail=? WHERE id=?")
        .bind(json!({"policy":"pack","storage_id":storage,"profile_revision":-1}))
        .bind(packing)
        .execute(db)
        .await?;
    assert!(!backend.fence("single", packing, task.token).await?);
    finish(&backend, task, None).await?;
    assert_eq!(
        TaskExecutionStore::load(&backend, packing).await?.state,
        "paused"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=?")
            .bind(profile_output)
            .fetch_one(db)
            .await?,
        "retired"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pack_inputs WHERE task_id=?")
            .bind(packing)
            .fetch_one(db)
            .await?,
        0
    );

    let upload =
        TaskControlStore::enqueue(&backend, "upload", None, json!({"first":c1.id})).await?;
    let task = claimed(&backend, upload, 2).await?;
    sqlx::query("UPDATE pending_uploads SET owner_task=?")
        .bind(upload)
        .execute(db)
        .await?;
    let output = cache_tests::pack(&backend, storage, upload).await?;
    sqlx::query("INSERT INTO pack_inputs(task_id,chunk_id) VALUES(?,?)")
        .bind(upload)
        .bind(c1.id)
        .execute(db)
        .await?;
    finish(&backend, task, Some("upload failure")).await?;
    let (attempts,owner,delay,error): (i32,Option<Uuid>,i64,String)=sqlx::query_as("SELECT attempts,owner_task,TIMESTAMPDIFF(MICROSECOND,CURRENT_TIMESTAMP(6),next_retry_at),last_error FROM pending_uploads WHERE chunk_id=?").bind(c1.id).fetch_one(db).await?;
    assert_eq!(
        (attempts, owner, error.as_str()),
        (1, Some(upload), "upload failure")
    );
    assert!(delay > 0 && delay <= 1_000_000, "{delay}");
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=?")
            .bind(output)
            .fetch_one(db)
            .await?,
        "retired"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pack_inputs WHERE task_id=?")
            .bind(upload)
            .fetch_one(db)
            .await?,
        0
    );
    sqlx::query(
        "UPDATE pending_uploads SET next_retry_at=TIMESTAMPADD(SECOND,-1,CURRENT_TIMESTAMP(6))",
    )
    .execute(db)
    .await?;
    assert!(UploadCacheStore::enqueue(&backend, 64).await?);
    let task = claimed(&backend, upload, 2).await?;
    finish(&backend, task, Some("again")).await?;
    let (attempts,delay): (i32,i64)=sqlx::query_as("SELECT attempts,TIMESTAMPDIFF(MICROSECOND,CURRENT_TIMESTAMP(6),next_retry_at) FROM pending_uploads WHERE chunk_id=?").bind(c1.id).fetch_one(db).await?;
    assert_eq!(attempts, 2);
    assert!(delay > 1_000_000 && delay <= 2_000_000, "{delay}");
    sqlx::query("UPDATE pending_uploads SET attempts=30")
        .execute(db)
        .await?;
    assert!(backend.change_state(upload, false, true).await?);
    let task = claimed(&backend, upload, 2).await?;
    finish(&backend, task, Some("bounded")).await?;
    let (attempts,delay): (i32,i64)=sqlx::query_as("SELECT attempts,TIMESTAMPDIFF(SECOND,CURRENT_TIMESTAMP(6),next_retry_at) FROM pending_uploads WHERE chunk_id=?").bind(c1.id).fetch_one(db).await?;
    assert_eq!(attempts, 30);
    assert!((250..=256).contains(&delay));
    let flush = TaskControlStore::enqueue(
        &backend,
        "cache_flush",
        None,
        json!({"upper_time":chrono::Utc::now()}),
    )
    .await?;
    let task = claimed(&backend, flush, 2).await?;
    sqlx::query("UPDATE pending_uploads SET owner_task=?,attempts=1")
        .bind(flush)
        .execute(db)
        .await?;
    let output = cache_tests::pack(&backend, storage, flush).await?;
    sqlx::query("INSERT INTO pack_inputs(task_id,chunk_id) VALUES(?,?)")
        .bind(flush)
        .bind(c1.id)
        .execute(db)
        .await?;
    finish(&backend, task, Some("flush failure")).await?;
    let (attempts, owner): (i32, Option<Uuid>) =
        sqlx::query_as("SELECT attempts,owner_task FROM pending_uploads WHERE chunk_id=?")
            .bind(c1.id)
            .fetch_one(db)
            .await?;
    assert_eq!((attempts, owner), (2, None));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=?")
            .bind(output)
            .fetch_one(db)
            .await?,
        "retired"
    );
    assert_eq!(backend.status().await?.0["entries"], 2);
    assert!(backend.change_state(flush, false, true).await?);
    let task = claimed(&backend, flush, 2).await?;
    UploadCacheStore::finish(&backend, task).await?;
    finish(&backend, task, Some("late failure")).await?;
    assert_eq!(
        TaskExecutionStore::load(&backend, flush).await?.state,
        "completed"
    );
    assert!(backend.raw(flush).await?.unwrap()["error"].is_null());
    Ok(())
}
