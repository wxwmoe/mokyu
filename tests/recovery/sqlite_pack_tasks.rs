use super::{Backend, cache_tests, connection, queries, transactions};
use crate::authorization::Principal;
use crate::config::Config;
use crate::model::bucket::CreateBucket;
use crate::model::task::Execution;
use crate::model::{AccountStore, BucketStore, TaskControlStore, TaskExecutionStore};
use anyhow::Result;
use serde_json::{Value, json};
use uuid::Uuid;

fn id(value: &Value) -> Uuid {
    value["task_id"].as_str().unwrap().parse().unwrap()
}
async fn execute(backend: &Backend, id: Uuid) -> Result<Execution> {
    let token = Uuid::new_v4();
    assert_eq!(
        queries::claim_task(id, token)
            .execute(&backend.write)
            .await?
            .rows_affected(),
        1
    );
    Ok(Execution { id, token })
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_SQLITE_CONFIG pointing to a fresh isolated SQLite database"]
async fn sqlite_pack_task_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.write;
    backend.initialize_admin("admin", "password", false).await?;
    assert!(
        transactions::start_pack(db, "invalid", false)
            .await
            .is_err()
    );
    let empty = id(&transactions::start_pack(db, "pack", false).await?);
    let row = backend.raw(empty).await?.unwrap();
    assert_eq!(row["state"], "completed");
    assert_eq!(row["detail"]["storages"], json!([]));
    assert!(row["detail"].get("role").is_none());
    let first = super::storage_tests::fixture_named(&backend, "first").await?;
    let second = super::storage_tests::fixture_named(&backend, "second").await?;
    sqlx::query("UPDATE storages SET created_at=IIF(id=?,946684800000000,946771200000000)")
        .bind(first)
        .execute(db)
        .await?;
    sqlx::query(
        "UPDATE storage_profiles SET pack_enabled=(storage_id=?),pack_range_enabled=(storage_id=?)",
    )
    .bind(first)
    .bind(second)
    .execute(db)
    .await?;
    let (a, b) = tokio::join!(
        transactions::start_pack(db, "pack", false),
        transactions::start_pack(db, "pack", false)
    );
    let a = a?;
    let b = b?;
    assert_eq!(id(&a), id(&b));
    assert!(a["existing"] == true || b["existing"] == true);
    let pack_task = id(&a);
    let detail = backend.raw(pack_task).await?.unwrap()["detail"].clone();
    assert_eq!(detail["execution_mode"], "single");
    assert_eq!(detail["storages"].as_array().unwrap().len(), 1);
    assert_eq!(detail["storage_id"], first.to_string());
    assert_eq!(
        queries::pack_storages(Some("range"))
            .fetch_all(db)
            .await?
            .iter()
            .map(|v| v.0)
            .collect::<Vec<_>>(),
        vec![second]
    );
    assert_eq!(
        queries::pack_storages(Some("reuse"))
            .fetch_all(db)
            .await?
            .len(),
        2
    );
    sqlx::query("UPDATE tasks SET state='failed' WHERE id=?")
        .bind(pack_task)
        .execute(db)
        .await?;
    let retry = transactions::start_pack(db, "pack", true).await?;
    assert_eq!(retry["idle"], true);
    let manual = id(&transactions::start_pack(db, "pack", false).await?);
    assert_ne!(manual, pack_task);
    sqlx::query("UPDATE tasks SET state='completed' WHERE id=?")
        .bind(manual)
        .execute(db)
        .await?;
    sqlx::query("UPDATE tasks SET updated_at=946684800000000 WHERE id=?")
        .bind(pack_task)
        .execute(db)
        .await?;
    sqlx::query("UPDATE storage_maintenance SET last_scheduled_at=946684800000000")
        .execute(db)
        .await?;
    assert_ne!(
        id(&transactions::start_pack(db, "pack", true).await?),
        pack_task
    );

    let (a, b) = tokio::join!(
        transactions::start_unpack(db, None, true),
        transactions::start_unpack(db, None, true)
    );
    let a = a?;
    let b = b?;
    assert_eq!(id(&a), id(&b));
    let unpack = id(&a);
    let detail = backend.raw(unpack).await?.unwrap()["detail"].clone();
    assert_eq!(detail["storages"].as_array().unwrap().len(), 2);
    assert_eq!(detail["storage_id"], first.to_string());
    assert!(backend.change_state(unpack, false, false).await?);
    assert_eq!(
        id(&transactions::start_unpack(db, None, true).await?),
        unpack
    );
    assert!(backend.change_state(unpack, false, true).await?);
    let task = execute(&backend, unpack).await?;
    let stale = Execution {
        id: task.id,
        token: Uuid::new_v4(),
    };
    assert!(
        transactions::advance_pack_storage(db, stale, 1)
            .await
            .is_err()
    );
    backend.progress(task, "cursor", 4, detail.clone()).await?;
    transactions::advance_pack_storage(db, task, 2).await?;
    let row = backend.raw(unpack).await?.unwrap();
    assert_eq!(row["detail"]["storage_index"], 1);
    assert_eq!(row["detail"]["storage_id"], second.to_string());
    assert_eq!(row["processed"], 6);
    assert!(row["cursor"].is_null());
    sqlx::query("UPDATE storage_profiles SET revision=revision+1 WHERE storage_id=?")
        .bind(second)
        .execute(db)
        .await?;
    assert!(!TaskExecutionStore::fence(&backend, "single", task.id, task.token).await?);
    assert!(
        transactions::advance_pack_storage(db, task, 1)
            .await
            .is_err()
    );
    assert!(backend.change_state(unpack, false, true).await?);
    let task = execute(&backend, unpack).await?;
    assert!(TaskExecutionStore::fence(&backend, "single", task.id, task.token).await?);
    transactions::advance_pack_storage(db, task, 3).await?;
    let row = backend.raw(unpack).await?.unwrap();
    assert_eq!(row["state"], "completed");
    assert_eq!(row["processed"], 9);

    // Empty storage snapshots still run a cleanup history phase before completion.
    let history = TaskControlStore::enqueue(
        &backend,
        "cleanup",
        None,
        json!({"execution_mode":"single","phase":"history","storages":[],"storage_index":0}),
    )
    .await?;
    let history = execute(&backend, history).await?;
    transactions::advance_pack_storage(db, history, 0).await?;
    assert_eq!(
        backend.raw(history.id).await?.unwrap()["state"],
        "completed"
    );

    let bucket = BucketStore::create(
        &backend,
        &Principal::Local,
        CreateBucket {
            name: "queues".into(),
            project_id: None,
            storage_id: Some(first),
        },
    )
    .await?
    .id;
    let stream = cache_tests::stream(&backend, bucket, "fixture").await?;
    sqlx::query("INSERT INTO pack_maintenance(stream_id,generation) VALUES(?,2)")
        .bind(stream)
        .execute(db)
        .await?;
    let task = TaskControlStore::enqueue(&backend, "pack", None, json!({"policy":"range"})).await?;
    let task = execute(&backend, task).await?;
    let stale = Execution {
        id: task.id,
        token: Uuid::new_v4(),
    };
    let pack = cache_tests::pack(&backend, first, task.id).await?;
    let specific = id(&transactions::start_unpack(db, Some(pack.into()), false).await?);
    assert_eq!(
        backend.raw(specific).await?.unwrap()["detail"]["storages"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        id(&transactions::start_unpack(db, Some(pack.into()), false).await?),
        specific
    );
    assert_ne!(
        id(&transactions::start_unpack(db, Some(pack.into()), true).await?),
        specific
    );
    sqlx::query("INSERT INTO pack_changes(pack_id,generation) VALUES(?,2)")
        .bind(pack)
        .execute(db)
        .await?;
    assert!(
        transactions::finish_pack_change(db, stale, pack.into(), 2)
            .await
            .is_err()
    );
    transactions::finish_pack_change(db, task, pack.into(), 1).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT generation FROM pack_changes WHERE pack_id=?")
            .bind(pack)
            .fetch_one(db)
            .await?,
        2
    );
    transactions::advance_pack_queue(db, task, stream, 1, 99).await?;
    assert!(
        transactions::advance_pack_queue(db, stale, stream, 2, 99)
            .await
            .is_err()
    );
    transactions::advance_pack_queue(db, task, stream, 2, 64).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT progress_cursor FROM pack_maintenance WHERE stream_id=?"
        )
        .bind(stream)
        .fetch_one(db)
        .await?,
        64
    );
    let later = chrono::Utc::now() + chrono::Duration::hours(1);
    transactions::defer_pack_queue(db, task, stream, 1, later).await?;
    assert!(
        transactions::defer_pack_queue(db, stale, stream, 2, later)
            .await
            .is_err()
    );
    transactions::defer_pack_queue(db, task, stream, 2, later).await?;
    let queued: (i64,String,bool)=sqlx::query_as("SELECT progress_cursor,reason,next_check_at>CAST(unixepoch('subsec')*1000000 AS INTEGER) FROM pack_maintenance WHERE stream_id=?").bind(stream).fetch_one(db).await?;
    assert_eq!(queued, (0, "repack".into(), true));
    transactions::finish_pack_queue(db, task, stream, 1).await?;
    assert!(
        transactions::finish_pack_queue(db, stale, stream, 2)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pack_maintenance WHERE stream_id=?")
            .bind(stream)
            .fetch_one(db)
            .await?,
        1
    );
    assert!(
        transactions::record_range(db, stale, pack.into(), 2, json!({"decision":"split"}))
            .await
            .is_err()
    );
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT range_checked_at IS NULL FROM packs WHERE id=?")
            .bind(pack)
            .fetch_one(db)
            .await?
    );
    transactions::record_range(db, task, pack.into(), 2, json!({"decision":"split"})).await?;
    transactions::record_range(db, task, pack.into(), 1, json!({"decision":"keep"})).await?;
    let row = backend.raw(task.id).await?.unwrap();
    assert_eq!(row["processed"], 3);
    assert_eq!(row["detail"]["evaluated"], 2);
    assert_eq!(row["detail"]["last_range"], json!({"decision":"keep"}));
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT range_checked_at IS NOT NULL FROM packs WHERE id=?")
            .bind(pack)
            .fetch_one(db)
            .await?
    );
    transactions::finish_pack_queue(db, task, stream, 2).await?;
    transactions::finish_pack_change(db, task, pack.into(), 2).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pack_maintenance WHERE stream_id=?")
            .bind(stream)
            .fetch_one(db)
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pack_changes WHERE pack_id=?")
            .bind(pack)
            .fetch_one(db)
            .await?,
        0
    );

    backend.check_owner()?;
    backend.read.close().await;
    backend.write.close().await;
    Ok(())
}
