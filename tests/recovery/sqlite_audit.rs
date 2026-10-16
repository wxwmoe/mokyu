use super::{connection, transactions};
use crate::authorization::{DEFAULT_PROJECT, Principal};
use crate::config::Config;
use crate::model::audit::{self, Filter};
use crate::model::{AccountStore, AuditStore};
use anyhow::Result;
use serde_json::{Value, json};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_SQLITE_CONFIG pointing to a fresh isolated SQLite database"]
async fn sqlite_audit_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.write;
    let storage = super::storage_tests::fixture(&backend).await?;
    assert!(backend.setup_required().await?);
    let admin = backend.initialize_admin("admin", "password", false).await?;
    let member = Uuid::new_v4();
    sqlx::query("INSERT INTO web_users(id,username,password_hash) VALUES(?,'Member_%','password')")
        .bind(member)
        .execute(db)
        .await?;
    let user = Principal::User {
        id: member,
        session: Uuid::new_v4(),
        revision: 0,
    };
    let token_id = Uuid::new_v4();
    let token = Principal::Token {
        id: token_id,
        user: member,
        revision: 0,
        user_revision: 0,
    };
    let bucket = Uuid::new_v4();
    sqlx::query("INSERT INTO buckets(id,name,project_id,storage_id) VALUES(?,'audit',?,?)")
        .bind(bucket)
        .bind(DEFAULT_PROJECT)
        .bind(storage)
        .execute(db)
        .await?;
    sqlx::query(
        "INSERT INTO project_members(user_id,project_id,role,scope) VALUES(?,?,'reader','all')",
    )
    .bind(member)
    .bind(DEFAULT_PROJECT)
    .execute(db)
    .await?;

    let pending = backend
        .begin(&user, "account.update", Some("pending"))
        .await?;
    let event = backend.event(pending, false, member).await?.unwrap();
    assert_eq!(event.id, pending.to_string());
    assert_eq!(event.actor_id, Some(member));
    assert_eq!(event.actor_label, "Member_%");
    assert_eq!(event.source, "web");
    assert_eq!(event.outcome, "unknown");
    assert!(event.finished_at.is_none());
    assert!(backend.event(pending, false, admin).await?.is_none());
    assert!(backend.event(pending, true, admin).await?.is_some());
    let failed = backend
        .begin(&token, "credential.create", Some("failed"))
        .await?;
    backend.finish(failed, 403, false).await?;
    let event = backend.event(failed, false, member).await?.unwrap();
    assert_eq!(event.source, "token");
    assert_eq!(event.token_id, Some(token_id));
    assert_eq!(event.outcome, "failed");
    let finished = event.finished_at;
    backend.finish(failed, 200, false).await?;
    let event = backend.event(failed, true, admin).await?.unwrap();
    assert_eq!(event.outcome, "failed");
    assert_eq!(event.finished_at, finished);
    assert_eq!(event.status, Some(200));
    let private = backend
        .begin(&user, "credential.create", Some("private"))
        .await?;
    let mut tx = db.begin_with(super::queries::BEGIN_WRITE).await?;
    transactions::checkpoint_audit(
        &mut tx,
        private,
        "credential.create",
        "secret-scope",
        None,
        None,
        json!({"changed": true}),
    )
    .await?;
    tx.commit().await?;
    backend.finish(private, 403, false).await?;
    assert!(backend.event(private, false, member).await?.is_none());
    assert_eq!(
        backend.event(private, true, admin).await?.unwrap().outcome,
        "succeeded"
    );
    let partial = backend
        .begin(&Principal::Local, "system.update", None)
        .await?;
    backend.finish(partial, 200, true).await?;
    let event = backend.event(partial, true, admin).await?.unwrap();
    assert_eq!(event.outcome, "partial");
    assert_eq!(event.source, "cli");
    assert_eq!(event.actor_label, "Local CLI");
    assert!(event.actor_id.is_none());

    let visible = backend
        .begin(&user, "bucket.update", Some("visible"))
        .await?;
    let detail = json!({"project_id":DEFAULT_PROJECT,"bucket_id":bucket,"value":"x".repeat(9000)});
    let mut tx = db.begin_with(super::queries::BEGIN_WRITE).await?;
    audit::scope(
        visible,
        transactions::checkpoint_event(&mut tx, "bucket.update", "audit", detail.clone()),
    )
    .await?;
    tx.commit().await?;
    backend.finish(visible, 200, false).await?;
    let event = backend.event(visible, false, member).await?.unwrap();
    assert_eq!(event.project_id, Some(DEFAULT_PROJECT));
    assert_eq!(event.bucket_id, Some(bucket));
    assert_eq!(event.detail, detail);
    let listed = backend
        .events(false, Some(member), None, Filter::default(), 100, false)
        .await?;
    assert!(listed.iter().any(|e| e.id == pending.to_string()));
    assert!(listed.iter().any(|e| e.id == failed.to_string()));
    assert!(!listed.iter().any(|e| e.id == private.to_string()));
    assert!(!listed.iter().any(|e| e.id == partial.to_string()));
    assert_eq!(
        listed
            .iter()
            .find(|e| e.id == visible.to_string())
            .unwrap()
            .detail,
        json!({"details_available":true})
    );
    let export = backend
        .events(false, Some(member), None, Filter::default(), 100, true)
        .await?;
    assert_eq!(
        export
            .iter()
            .find(|e| e.id == visible.to_string())
            .unwrap()
            .detail,
        detail
    );
    let page = backend
        .events(true, None, None, Filter::default(), 1, false)
        .await?;
    assert_eq!(page.len(), 2);
    assert!(page[0].id.parse::<i64>()? > page[1].id.parse::<i64>()?);
    let next = backend
        .events(
            true,
            None,
            Some(page[1].id.parse()?),
            Filter::default(),
            1,
            false,
        )
        .await?;
    assert!(
        next.iter()
            .all(|e| e.id.parse::<i64>().unwrap() < page[1].id.parse().unwrap())
    );
    assert!(
        backend
            .events(false, None, None, Filter::default(), 100, false)
            .await?
            .is_empty()
    );
    let row = backend.event(visible, true, admin).await?.unwrap();
    let filter = Filter {
        actor: Some("MEMBER_%".into()),
        action: Some("bucket.".into()),
        source: Some("web".into()),
        outcome: Some("succeeded".into()),
        project: Some(DEFAULT_PROJECT),
        bucket: Some(bucket),
        since: Some(row.created_at),
        until: Some(row.created_at),
        ..Default::default()
    };
    let filtered = backend.events(true, None, None, filter, 100, false).await?;
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].id, visible.to_string());
    for action in ["%", "bucket_", "bucket.update ", "Bucket."] {
        assert!(
            backend
                .events(
                    true,
                    None,
                    None,
                    Filter {
                        action: Some(action.into()),
                        ..Default::default()
                    },
                    100,
                    false
                )
                .await?
                .is_empty()
        );
    }
    sqlx::query("DELETE FROM project_members WHERE user_id=?")
        .bind(member)
        .execute(db)
        .await?;
    assert!(backend.event(visible, false, member).await?.is_none());
    assert!(
        !backend
            .events(false, Some(member), None, Filter::default(), 100, false)
            .await?
            .iter()
            .any(|e| e.id == visible.to_string())
    );

    let rollback = backend.begin(&user, "token.update", None).await?;
    let mut tx = db.begin_with(super::queries::BEGIN_WRITE).await?;
    transactions::checkpoint_audit(
        &mut tx,
        rollback,
        "token.update",
        "changed",
        None,
        None,
        json!({"changed":true}),
    )
    .await?;
    tx.rollback().await?;
    let event = backend.event(rollback, false, member).await?.unwrap();
    assert_eq!(event.outcome, "unknown");
    assert_eq!(event.detail, json!({}));
    let mut tx = db.begin_with(super::queries::BEGIN_WRITE).await?;
    assert!(
        audit::scope(
            rollback,
            transactions::checkpoint_event(
                &mut tx,
                "token.update",
                "too-large",
                json!({"value":"x".repeat(262144)})
            )
        )
        .await
        .is_err()
    );
    tx.rollback().await?;
    backend.finish(rollback, 500, false).await?;
    assert_eq!(
        backend
            .event(rollback, false, member)
            .await?
            .unwrap()
            .outcome,
        "failed"
    );
    sqlx::query("UPDATE audit_events SET detail=JSON_ARRAY() WHERE id=?")
        .bind(failed)
        .execute(db)
        .await?;
    assert!(backend.event(failed, false, member).await?.is_none());
    sqlx::query("DELETE FROM web_users WHERE id=?")
        .bind(member)
        .execute(db)
        .await?;
    let missing = backend.begin(&user, "session.logout", None).await?;
    let event = backend.event(missing, true, admin).await?.unwrap();
    assert_eq!(event.actor_label, "Deleted account");
    assert_eq!(event.detail, Value::Object(Default::default()));
    assert_eq!(
        backend
            .event(pending, true, admin)
            .await?
            .unwrap()
            .actor_label,
        "Member_%"
    );
    let mut batch = db.begin_with(super::queries::BEGIN_WRITE).await?;
    for n in 0..130 {
        sqlx::query("INSERT INTO audit_events(actor_label,source,action,target) VALUES(?,'cli','unicode.test','')")
            .bind(if n == 0 { "\u{c5}lice" } else { "test" }).execute(&mut *batch).await?;
    }
    batch.commit().await?;
    let found = backend
        .events(
            true,
            None,
            None,
            Filter {
                actor: Some("\u{e5}LICE".into()),
                ..Default::default()
            },
            1,
            false,
        )
        .await?;
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].actor_label, "\u{c5}lice");
    assert!(
        backend
            .events(true, None, None, Filter::default(), i64::MAX, false)
            .await
            .is_err()
    );
    backend.read.close().await;
    backend.write.close().await;
    Ok(())
}
