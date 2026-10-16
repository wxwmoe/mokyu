use super::{Backend, connection, queries, transactions};
use crate::authorization::Principal;
use crate::config::Config;
use crate::model::account::SessionLogin;
use crate::model::audit;
use crate::model::runtime::RuntimeStore;
use crate::model::task::Execution;
use crate::model::{AccountStore, AuditStore, TaskControlStore};
use anyhow::Result;
use serde_json::json;
use uuid::Uuid;

async fn execute(backend: &Backend, id: Uuid) -> Result<Execution> {
    let token = Uuid::new_v4();
    assert_eq!(
        queries::claim_task(id, token)
            .execute(&backend.pool)
            .await?
            .rows_affected(),
        1
    );
    Ok(Execution { id, token })
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_maintenance_control_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    let admin = backend.initialize_admin("admin", "password", false).await?;
    let verified = backend.login_user("admin").await?.unwrap();
    backend
        .login(SessionLogin {
            verified: &verified,
            access_hash: &[1; 32],
            csrf_hash: &[1; 32],
            refresh_hash: &[1; 32],
            seconds: 3600,
            user_agent: "fixture",
            request_id: "fixture",
        })
        .await?;
    let session = backend.authenticate_session(&[1; 32]).await?.unwrap();
    let user = Principal::User {
        id: admin,
        session: session.session_id,
        revision: session.authorization_revision,
    };
    assert!(
        transactions::start_lifecycle_policy(db, "unknown")
            .await
            .is_err()
    );
    let (empty, _) = transactions::start_lifecycle_policy(db, "gc").await?;
    assert_eq!(backend.raw(empty).await?.unwrap()["state"], "completed");
    let (empty, _) = transactions::start_lifecycle_policy(db, "cleanup").await?;
    let execution = execute(&backend, empty).await?;
    transactions::finish_lifecycle_policy(db, execution, 0, false, json!({"deleted":0})).await?;
    let row = backend.raw(empty).await?.unwrap();
    assert_eq!(row["state"], "completed");
    assert_eq!(row["detail"]["report"]["history"], json!({"deleted":0}));
    let first = super::storage_tests::fixture_named(&backend, "first").await?;
    let second = super::storage_tests::fixture_named(&backend, "second").await?;
    sqlx::query("UPDATE storages SET created_at=IF(id=?,'2000-01-01','2000-01-02')")
        .bind(first)
        .execute(db)
        .await?;
    let (a, b) = tokio::join!(
        transactions::start_lifecycle_policy(db, "gc"),
        transactions::start_lifecycle_policy(db, "gc")
    );
    let (a, b) = (a?, b?);
    assert_eq!(a.0, b.0);
    assert_ne!(a.1, b.1);
    let id = a.0;
    assert!(backend.change_state(id, false, false).await?);
    assert_eq!(
        transactions::start_lifecycle_policy(db, "gc").await?,
        (id, true)
    );
    assert!(backend.change_state(id, false, true).await?);
    let task = execute(&backend, id).await?;
    assert_eq!(
        backend.raw(id).await?.unwrap()["detail"]["storage_id"],
        first.to_string()
    );
    transactions::finish_lifecycle_policy(
        db,
        task,
        2,
        true,
        json!({"chunks":2,"status":"ignored"}),
    )
    .await?;
    transactions::finish_lifecycle_policy(db, task, 3, false, json!({"chunks":3})).await?;
    let row = backend.raw(id).await?.unwrap();
    assert_eq!(row["processed"], 5);
    assert_eq!(row["detail"]["storage_id"], second.to_string());
    assert_eq!(
        row["detail"]["report"]["storages"][first.to_string()]["chunks"],
        5
    );
    assert!(
        row["detail"]["report"]["storages"][first.to_string()]
            .get("status")
            .is_none()
    );
    let stale = Execution {
        id,
        token: Uuid::new_v4(),
    };
    assert!(
        transactions::finish_lifecycle_policy(db, stale, 1, false, json!({"chunks":1}))
            .await
            .is_err()
    );
    assert!(
        transactions::finish_lifecycle_policy(db, task, 1, false, json!([]))
            .await
            .is_err()
    );
    transactions::finish_lifecycle_policy(db, task, 1, true, json!({"chunks":1})).await?;
    assert!(
        transactions::finish_lifecycle_policy(db, task, 1, true, json!({"chunks":i64::MAX}))
            .await
            .is_err()
    );
    transactions::finish_lifecycle_policy(db, task, 0, false, json!({"chunks":0})).await?;
    assert_eq!(backend.raw(id).await?.unwrap()["state"], "completed");
    let controls = queries::maintenance_controls(&mut *db.acquire().await?).await?;
    assert_eq!(controls.len(), 7);
    let gc = controls.iter().find(|c| c["policy"] == "gc").unwrap();
    assert_eq!(gc["latest"]["id"], id.to_string());
    assert_eq!(gc["latest"]["processed"], 6);
    assert!(gc["active"].is_null());
    assert!(gc["latest"].get("execution_token").is_none());
    assert!(gc["last_scheduled_at"].as_str().unwrap().ends_with('Z'));
    let paused = transactions::start_lifecycle_policy(db, "gc").await?.0;
    backend.change_state(paused, false, false).await?;
    let controls = queries::maintenance_controls(&mut *db.acquire().await?).await?;
    let gc = controls.iter().find(|c| c["policy"] == "gc").unwrap();
    assert_eq!(gc["active"]["id"], paused.to_string());
    transactions::change_maintenance_policy(db, "gc", true).await?;
    assert!(
        queries::maintenance_policy_paused("gc")
            .fetch_one(db)
            .await?
    );
    assert!(backend.state().await?.gc_paused);
    transactions::change_maintenance_policy(db, "gc", false).await?;
    transactions::change_maintenance_policy(db, "cleanup", true).await?;
    assert!(
        queries::maintenance_policy_paused("cleanup")
            .fetch_one(db)
            .await?
    );
    assert!(
        transactions::change_maintenance_policy(db, "unknown", true)
            .await
            .is_err()
    );
    sqlx::query("UPDATE maintenance_controls SET last_scheduled_at='2000-01-01' WHERE policy='gc'")
        .execute(db)
        .await?;
    queries::scheduled_maintenance("gc").execute(db).await?;
    let schedule = queries::maintenance_schedule().fetch_all(db).await?;
    assert_eq!(schedule.len(), 7);
    assert!(schedule.iter().find(|v| v.0 == "gc").unwrap().2.unwrap() < 5.);
    assert!(!queries::due_pack_reuse().fetch_one(db).await?);
    assert_eq!(queries::preparing_pack_count().fetch_one(db).await?, "0");
    transactions::change_pack_creation(db, true).await?;
    let unpack = TaskControlStore::enqueue(&backend, "unpack", None, json!({"all":true})).await?;
    assert!(transactions::change_pack_creation(db, false).await.is_err());
    let unpack_task = execute(&backend, unpack).await?;
    TaskControlStore::complete(&backend, unpack_task).await?;
    transactions::change_pack_creation(db, false).await?;
    assert!(!backend.state().await?.pack_creation_paused);
    let mut pause = Vec::new();
    for name in ["purge", "pack", "unpack", "storage_migration", "rewrite"] {
        pause.push(TaskControlStore::enqueue(&backend, name, None, json!({})).await?);
    }
    let running = execute(&backend, pause[0]).await?;
    transactions::maintenance_mode(db, true).await?;
    assert!(backend.state().await?.maintenance);
    for id in pause {
        assert_eq!(backend.raw(id).await?.unwrap()["state"], "paused");
    }
    assert_eq!(
        queries::task_record(running.id)
            .fetch_one(db)
            .await?
            .execution_token,
        Some(running.token)
    );
    transactions::maintenance_mode(db, false).await?;
    assert!(!backend.state().await?.maintenance);
    TaskControlStore::enqueue(&backend, "sweep", None, json!({"dry_run":true})).await?;
    assert!(!queries::destructive_sweep().fetch_one(db).await?);
    let sweep =
        TaskControlStore::enqueue(&backend, "sweep", None, json!({"dry_run":false})).await?;
    assert!(queries::destructive_sweep().fetch_one(db).await?);
    TaskControlStore::complete(&backend, execute(&backend, sweep).await?).await?;
    assert!(!queries::destructive_sweep().fetch_one(db).await?);

    let input = json!({"all":true});
    let impact = json!({"packs":7,"bytes":"400"});
    let first = transactions::create_maintenance_preview(
        db,
        &user,
        "unpack",
        input.clone(),
        impact.clone(),
        "UNPACK".into(),
    )
    .await?;
    let second = transactions::create_maintenance_preview(
        db,
        &user,
        "unpack",
        input.clone(),
        impact.clone(),
        "UNPACK".into(),
    )
    .await?;
    assert!(
        transactions::maintenance_execution(db, &user, first.id, "unpack")
            .await?
            .is_none()
    );
    assert!(second.expires_at > chrono::Utc::now() + chrono::Duration::minutes(9));
    let receipt = transactions::maintenance_execution(db, &user, second.id, "unpack")
        .await?
        .unwrap();
    assert!(receipt.task.is_none());
    assert_eq!(
        receipt.parameters,
        json!({"input":input,"confirmation":"UNPACK","impact":impact})
    );
    assert_eq!(
        receipt.fingerprint,
        blake3::hash(&serde_json::to_vec(&impact)?)
            .to_hex()
            .to_string()
    );
    drop(receipt);
    assert!(
        transactions::maintenance_execution(db, &user, second.id, "sweep")
            .await?
            .is_none()
    );
    let mut tokens = Vec::new();
    for seed in [2u8, 3] {
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO api_tokens(id,user_id,label,token_hash,prefix,system_access,auth_revision) VALUES(?,?,'test',?,'test',TRUE,0)").bind(id).bind(admin).bind([seed;32].as_slice()).execute(db).await?;
        tokens.push(Principal::Token {
            id,
            user: admin,
            revision: 0,
            user_revision: 0,
        });
    }
    let preview = transactions::create_maintenance_preview(
        db,
        &tokens[0],
        "sweep",
        json!({}),
        impact.clone(),
        "SWEEP".into(),
    )
    .await?;
    assert!(
        transactions::maintenance_execution(db, &tokens[1], preview.id, "sweep")
            .await?
            .is_none()
    );
    assert!(
        transactions::maintenance_execution(db, &user, preview.id, "sweep")
            .await?
            .is_none()
    );
    assert!(
        transactions::create_maintenance_preview(
            db,
            &user,
            "invalid",
            json!({}),
            json!({}),
            "".into()
        )
        .await
        .is_err()
    );
    let receipt = transactions::maintenance_execution(db, &tokens[0], preview.id, "sweep")
        .await?
        .unwrap();
    let task = crate::model::REQUEST_ID
        .scope(
            preview.id,
            TaskControlStore::enqueue(&backend, "sweep", None, json!({"dry_run":true})),
        )
        .await?;
    assert_eq!(task, preview.id);
    drop(receipt);
    let receipt = transactions::maintenance_execution(db, &tokens[0], preview.id, "sweep")
        .await?
        .unwrap();
    assert_eq!(receipt.task, Some(task));
    let event = backend
        .begin(&tokens[0], "maintenance.sweep", Some("fixture"))
        .await?;
    audit::scope(event, receipt.completion.finish(task, true)).await?;
    let saved = backend.event(event, true, admin).await?.unwrap();
    assert_eq!(saved.outcome, "succeeded");
    let receipt = transactions::maintenance_execution(db, &tokens[0], preview.id, "sweep")
        .await?
        .unwrap();
    assert_eq!(receipt.task, Some(task));
    receipt.completion.finish(task, false).await?;
    sqlx::query("UPDATE maintenance_previews SET expires_at='2000-01-01' WHERE id=?")
        .bind(second.id)
        .execute(db)
        .await?;
    assert!(
        transactions::maintenance_execution(db, &user, second.id, "unpack")
            .await?
            .is_none()
    );
    let (a, b) = tokio::join!(
        transactions::create_maintenance_preview(
            db,
            &user,
            "unpack",
            json!({}),
            json!({}),
            "A".into()
        ),
        transactions::create_maintenance_preview(
            db,
            &user,
            "unpack",
            json!({}),
            json!({}),
            "B".into()
        )
    );
    let (a, b) = (a?, b?);
    assert_ne!(a.id, b.id);
    let count:i64 = sqlx::query_scalar("SELECT COUNT(*) FROM maintenance_previews WHERE actor_id=? AND token_id IS NULL AND action='unpack' AND task_id IS NULL").bind(admin).fetch_one(db).await?;
    assert_eq!(count, 1);
    let existing = if transactions::maintenance_execution(db, &user, a.id, "unpack")
        .await?
        .is_some()
    {
        a.id
    } else {
        b.id
    };
    sqlx::query("UPDATE web_users SET authorization_revision=authorization_revision+1 WHERE id=?")
        .bind(admin)
        .execute(db)
        .await?;
    assert!(
        transactions::maintenance_execution(db, &user, existing, "unpack")
            .await
            .is_err()
    );
    assert!(
        transactions::create_maintenance_preview(
            db,
            &tokens[0],
            "sweep",
            json!({}),
            json!({}),
            "SWEEP".into()
        )
        .await
        .is_err()
    );
    Ok(())
}
