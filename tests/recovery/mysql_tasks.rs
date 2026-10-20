use super::connection;
use crate::authorization::Principal;
use crate::config::Config;
use crate::model::bucket::CreateBucket;
use crate::model::task::{Execution, JobQuery};
use crate::model::{AccountStore, BucketStore, TaskControlStore};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use uuid::Uuid;

fn filter(value: Value) -> JobQuery {
    serde_json::from_value(value).unwrap()
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_task_control_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    let admin = backend.initialize_admin("admin", "password", false).await?;
    let storage = super::storage_tests::fixture(&backend).await?;
    let bucket = BucketStore::create(
        &backend,
        &Principal::Local,
        CreateBucket {
            name: "tasks".into(),
            project_id: None,
            storage_id: Some(storage),
        },
    )
    .await?
    .id;
    let request = Uuid::new_v4();
    let unpack = crate::model::REQUEST_ID
        .scope(
            request,
            backend.enqueue("unpack", Some(bucket), json!({"pack_id":7})),
        )
        .await?;
    assert_eq!(unpack, request);
    let rewrite = backend
        .enqueue(
            "rewrite",
            None,
            json!({"storage_id":storage,"profile_revision":0}),
        )
        .await?;
    let pack = backend
        .enqueue(
            "pack",
            None,
            json!({"policy":"repack","storages":[{"storage_id":storage}]}),
        )
        .await?;
    let gc = backend.enqueue("gc", Some(bucket), json!({})).await?;
    let integrity = backend.enqueue("integrity", None, json!({})).await?;
    let sweep = backend
        .enqueue("sweep", None, json!({"preview":true}))
        .await?;
    assert!(backend.enqueue("invalid", None, json!({})).await.is_err());
    sqlx::query("UPDATE tasks SET created_at='2026-01-01 00:00:00.123456'")
        .execute(db)
        .await?;
    sqlx::query("UPDATE tasks SET created_by_label='My Auto%_Scheduler' WHERE id=?")
        .bind(pack)
        .execute(db)
        .await?;

    assert_eq!(
        backend.unpack_status(7.into()).await?.unwrap()["id"],
        json!(unpack)
    );
    assert!(backend.unpack_status(8.into()).await?.is_none());
    assert_eq!(
        backend.rewrite_status(storage).await?.unwrap()["id"],
        json!(rewrite)
    );
    assert_eq!(backend.rewrite_task(storage).await?, Some(rewrite));
    assert!(backend.rewrite_task(Uuid::new_v4()).await?.is_none());
    assert_eq!(
        backend.load(pack).await?.unwrap(),
        (
            false,
            "pack".into(),
            json!({"policy":"repack","storages":[{"storage_id":storage}]})
        )
    );
    assert!(backend.load(Uuid::new_v4()).await?.is_none());
    assert!(!backend.other_integrity(integrity).await?);
    assert!(backend.other_integrity(sweep).await?);
    assert!(backend.sweep_preview(sweep).await?.is_none());
    assert_eq!(TaskControlStore::all(&backend).await?.len(), 6);

    let mut collected = Vec::new();
    let mut cursor = None;
    loop {
        let page = backend
            .page(&Principal::Local, filter(json!({})), cursor, 2)
            .await?;
        if page.is_empty() {
            break;
        }
        for task in &page {
            let id: Uuid = serde_json::from_value(task["id"].clone())?;
            let time: DateTime<Utc> = serde_json::from_value(task["created_at"].clone())?;
            assert_eq!(time.timestamp_subsec_micros(), 123456);
            for field in [
                "execution_token",
                "lease_token",
                "lease_node_id",
                "parent_task_id",
                "execution_scope",
            ] {
                assert!(task.get(field).is_none());
            }
            collected.push(id);
            cursor = Some((time, id));
        }
    }
    let mut expected = vec![unpack, rewrite, pack, gc, integrity, sweep];
    expected.sort_by(|a, b| b.cmp(a));
    assert_eq!(collected, expected);
    for (input, count) in [
        (json!({"bucket":bucket}), 2),
        (json!({"storage":storage}), 4),
        (json!({"storage":Uuid::new_v4()}), 0),
        (json!({"policy":"repack"}), 1),
        (json!({"actor":"aUtO%_"}), 1),
        (json!({"actor":"' OR 1=1 --"}), 0),
        (json!({"state":"failed"}), 0),
    ] {
        assert_eq!(
            backend
                .page(&Principal::Local, filter(input), None, 20)
                .await?
                .len(),
            count
        );
    }
    assert!(
        backend
            .view(&Principal::Local, Uuid::new_v4())
            .await
            .is_err()
    );
    let value = backend.view(&Principal::Local, gc).await?;
    assert_eq!(value["bucket_name"], "tasks");
    assert!(value["blocked"].is_null());
    sqlx::query("UPDATE maintenance_controls SET paused=TRUE WHERE policy='repack'")
        .execute(db)
        .await?;
    assert_eq!(
        backend.view(&Principal::Local, pack).await?["blocked"],
        "PolicyPaused"
    );
    sqlx::query("UPDATE maintenance_controls SET paused=FALSE WHERE policy='repack'")
        .execute(db)
        .await?;
    sqlx::query("UPDATE mokyu_meta SET gc_paused=TRUE,pack_creation_paused=TRUE")
        .execute(db)
        .await?;
    assert_eq!(
        backend.view(&Principal::Local, gc).await?["blocked"],
        "PolicyPaused"
    );
    assert_eq!(
        backend.view(&Principal::Local, pack).await?["blocked"],
        "PackCreationStopped"
    );
    sqlx::query("UPDATE mokyu_meta SET maintenance=TRUE")
        .execute(db)
        .await?;
    assert_eq!(
        backend.view(&Principal::Local, pack).await?["blocked"],
        "MaintenanceMode"
    );
    assert_eq!(
        backend.view(&Principal::Local, rewrite).await?["blocked"],
        "MaintenanceMode"
    );
    assert!(backend.view(&Principal::Local, integrity).await?["blocked"].is_null());

    let token = Uuid::new_v4();
    sqlx::query("INSERT INTO api_tokens(id,user_id,label,token_hash,prefix,`system_access`,auth_revision) VALUES(?,?,'test',?,'test',FALSE,0)")
        .bind(token).bind(admin).bind([3u8;32].as_slice()).execute(db).await?;
    let principal = Principal::Token {
        id: token,
        user: admin,
        revision: 0,
        user_revision: 0,
    };
    assert!(
        backend
            .page(&principal, filter(json!({})), None, 20)
            .await
            .is_err()
    );
    assert!(backend.view(&principal, pack).await.is_err());
    sqlx::query("UPDATE api_tokens SET `system_access`=TRUE WHERE id=?")
        .bind(token)
        .execute(db)
        .await?;
    assert_eq!(
        backend
            .page(&principal, filter(json!({})), None, 20)
            .await?
            .len(),
        6
    );
    sqlx::query("UPDATE api_tokens SET auth_revision=1 WHERE id=?")
        .bind(token)
        .execute(db)
        .await?;
    assert!(backend.view(&principal, pack).await.is_err());

    let execution = Execution {
        id: rewrite,
        token: Uuid::new_v4(),
    };
    let stale = Execution {
        id: rewrite,
        token: Uuid::new_v4(),
    };
    sqlx::query("UPDATE tasks SET state='running',execution_token=?,started_at=CURRENT_TIMESTAMP(6) WHERE id=?").bind(execution.token).bind(rewrite).execute(db).await?;
    assert!(backend.running(execution).await?);
    assert!(!backend.running(stale).await?);
    backend.pause(stale).await?;
    backend.complete(stale).await?;
    backend.progress(stale, "wrong", 20, json!({})).await?;
    assert!(backend.raw(rewrite).await?.unwrap()["cursor"].is_null());
    backend
        .progress(
            execution,
            "10",
            4,
            json!({"storage_id":storage,"profile_revision":0}),
        )
        .await?;
    let value = backend.raw(rewrite).await?.unwrap();
    assert_eq!(value["cursor"], "10");
    assert_eq!(value["processed"], 4);
    assert!(value.get("execution_token").is_none());
    assert!(value["started_at"].as_str().unwrap().ends_with('Z'));
    backend.pause(execution).await?;
    backend.progress(execution, "wrong", 20, json!({})).await?;
    backend.complete(execution).await?;
    assert_eq!(backend.raw(rewrite).await?.unwrap()["state"], "paused");
    assert!(!backend.running(execution).await?);
    assert!(backend.change_state(rewrite, true, true).await.is_err());
    sqlx::query("UPDATE storage_profiles SET revision=revision+1 WHERE storage_id=?")
        .bind(storage)
        .execute(db)
        .await?;
    let revision: i64 =
        sqlx::query_scalar("SELECT revision FROM storage_profiles WHERE storage_id=?")
            .bind(storage)
            .fetch_one(db)
            .await?;
    assert!(backend.change_state(rewrite, false, true).await?);
    assert_eq!(
        backend.raw(rewrite).await?.unwrap()["detail"]["profile_revision"],
        revision
    );
    assert!(!backend.change_state(rewrite, false, true).await?);
    backend.complete(execution).await?;
    assert_eq!(backend.raw(rewrite).await?.unwrap()["state"], "queued");
    assert!(backend.change_state(rewrite, false, false).await?);
    assert!(!backend.change_state(rewrite, false, false).await?);
    assert!(!backend.change_state(Uuid::new_v4(), false, true).await?);
    sqlx::query("UPDATE tasks SET state='running',execution_token=? WHERE id=?")
        .bind(stale.token)
        .bind(rewrite)
        .execute(db)
        .await?;
    backend.progress(execution, "wrong", 20, json!({})).await?;
    assert_eq!(backend.raw(rewrite).await?.unwrap()["processed"], 4);
    backend.complete(stale).await?;
    assert_eq!(backend.raw(rewrite).await?.unwrap()["state"], "completed");
    assert!(!backend.change_state(rewrite, false, true).await?);

    sqlx::query("UPDATE tasks SET state='completed' WHERE id IN (?,?)")
        .bind(sweep)
        .bind(integrity)
        .execute(db)
        .await?;
    assert_eq!(
        backend.sweep_preview(sweep).await?,
        Some(json!({"preview":true}))
    );
    assert!(!backend.other_integrity(sweep).await?);
    assert!(backend.sweep_preview(rewrite).await?.is_none());
    Ok(())
}
