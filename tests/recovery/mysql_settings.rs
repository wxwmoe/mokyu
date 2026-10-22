use super::{connection, transactions};
use crate::authorization::Principal;
use crate::config::Config;
use crate::model::Id;
use crate::model::account::{AccountStore, SessionLogin};
use crate::model::settings::{ControlSettingsPatch, RuntimeSettingsStore};
use anyhow::Result;
use serde_json::json;
use sqlx::Connection;
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_runtime_settings_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    assert!(
        budget.database_connections >= 2,
        "lock regression requires two pool connections"
    );
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    assert!(backend.setup_required().await?);
    let defaults = crate::model::runtime_defaults(&config);
    backend.ensure(&defaults).await?;
    backend
        .ensure(&[("maintenance", json!({"wrong":true}))])
        .await?;
    assert_eq!(backend.load().await?.len(), defaults.len());
    assert_eq!(backend.revisions().await?.len(), defaults.len());
    assert!(
        backend
            .inspect()
            .await?
            .iter()
            .all(|row| row.2 == 1 && row.3)
    );
    assert!(backend.value("maintenance").await?.get("wrong").is_none());
    let uploads = backend.value("uploads").await?;
    assert!(uploads.get("staging_storage_id").is_none());
    assert!(uploads.get("staging_bytes_limit").is_none());
    let user = backend
        .initialize_admin("admin", "test-password", false)
        .await?;
    let verified = backend.login_user("admin").await?.unwrap();
    backend
        .login(SessionLogin {
            verified: &verified,
            access_hash: &[1; 32],
            csrf_hash: &[2; 32],
            refresh_hash: &[3; 32],
            seconds: 3600,
            user_agent: "test",
            request_id: "test",
        })
        .await?;
    let auth = backend.authenticate_session(&[1; 32]).await?.unwrap();
    let admin = Principal::User {
        id: user,
        session: auth.session_id,
        revision: auth.authorization_revision,
    };
    assert_eq!(backend.sections(&admin).await?.len(), defaults.len());
    assert_eq!(backend.section(&admin, "maintenance").await?.revision, 1);
    for principal in [
        Principal::Node {
            id: Uuid::new_v4(),
            role: "control".into(),
        },
        Principal::Service {
            access_key: "none".into(),
            revision: 0,
        },
        Principal::User {
            id: user,
            session: auth.session_id,
            revision: 99,
        },
        Principal::User {
            id: user,
            session: Uuid::new_v4(),
            revision: 0,
        },
    ] {
        assert!(backend.section(&principal, "maintenance").await.is_err());
        assert!(
            backend
                .save(&principal, "maintenance", Some(1), json!({}))
                .await
                .is_err()
        );
    }
    for principal in [Principal::Local, Principal::System, Principal::Bootstrap] {
        assert_eq!(
            backend.section(&principal, "maintenance").await?.revision,
            1
        );
    }

    let token = Uuid::new_v4();
    sqlx::query("INSERT INTO api_tokens(id,user_id,label,token_hash,prefix,system_access,auth_revision) VALUES(?,?,'test',?,'test',FALSE,0)")
        .bind(token).bind(user).bind([4u8;32].as_slice()).execute(db).await?;
    let token_admin = Principal::Token {
        id: token,
        user,
        revision: 0,
        user_revision: 0,
    };
    assert!(backend.sections(&token_admin).await.is_err());
    sqlx::query("UPDATE api_tokens SET system_access=TRUE WHERE id=?")
        .bind(token)
        .execute(db)
        .await?;
    assert_eq!(backend.sections(&token_admin).await?.len(), defaults.len());
    let stale_token = Principal::Token {
        id: token,
        user,
        revision: 1,
        user_revision: 0,
    };
    assert!(backend.sections(&stale_token).await.is_err());
    sqlx::query("UPDATE api_tokens SET revoked_at=CURRENT_TIMESTAMP(6) WHERE id=?")
        .bind(token)
        .execute(db)
        .await?;
    assert!(backend.sections(&token_admin).await.is_err());
    sqlx::query("UPDATE api_tokens SET revoked_at=NULL,expires_at=CURRENT_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id=?").bind(token).execute(db).await?;
    assert!(backend.sections(&token_admin).await.is_err());
    sqlx::query("UPDATE api_tokens SET expires_at=NULL WHERE id=?")
        .bind(token)
        .execute(db)
        .await?;

    let snapshot = backend.row("maintenance").await?;
    let mut changed = snapshot.value.clone();
    changed["interval"] = json!("61s");
    let (first, second) = tokio::join!(
        backend.save(
            &admin,
            "maintenance",
            Some(snapshot.revision),
            changed.clone()
        ),
        backend.save(
            &admin,
            "maintenance",
            Some(snapshot.revision),
            changed.clone()
        )
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let saved = first.or(second)?;
    assert_eq!(saved.revision, 2);
    assert_eq!(saved.value, changed);
    assert!(
        !backend
            .inspect()
            .await?
            .iter()
            .find(|row| row.0 == "maintenance")
            .unwrap()
            .3
    );
    assert!(
        crate::model::audit::scope(
            (-1).into(),
            backend.save(&admin, "maintenance", Some(2), snapshot.value.clone())
        )
        .await
        .is_err()
    );
    assert_eq!(backend.row("maintenance").await?.revision, 2);
    assert_eq!(backend.value("maintenance").await?, changed);
    let audit: Id = sqlx::query(
        "INSERT INTO audit_events(actor_label,source,action,target) VALUES('test','web','test','')",
    )
    .execute(db)
    .await?
    .last_insert_id()
    .try_into()?;
    let saved = crate::model::audit::scope(
        audit,
        backend.save(&admin, "maintenance", None, snapshot.value.clone()),
    )
    .await?;
    assert_eq!(saved.revision, 3);
    let detail: serde_json::Value =
        sqlx::query_scalar("SELECT detail FROM audit_events WHERE id=?")
            .bind(audit)
            .fetch_one(db)
            .await?;
    assert_eq!(detail, json!({"revision":3}));
    let reset = backend
        .reset(&Principal::Local, "maintenance", snapshot.value.clone())
        .await?;
    assert_eq!(reset.revision, 4);
    assert!(
        backend
            .inspect()
            .await?
            .iter()
            .find(|row| row.0 == "maintenance")
            .unwrap()
            .3
    );
    assert!(backend.reset(&admin, "absent", json!({})).await.is_err());
    assert!(
        backend
            .save(&admin, "absent", None, json!({}))
            .await
            .is_err()
    );

    let patch = |lifetime: &str, revision| ControlSettingsPatch {
        origin: None,
        secure_cookie: None,
        session_lifetime: Some(lifetime.into()),
        gravatar_base_url: None,
        revision: Some(revision),
    };
    let web = backend.row("web").await?;
    assert!(
        backend
            .update_web(&admin, patch("bad", web.revision))
            .await
            .is_err()
    );
    assert_eq!(backend.row("web").await?.revision, web.revision);
    let (web, revision) = backend
        .update_web(&admin, patch("2h", web.revision))
        .await?;
    assert_eq!(web.session_lifetime, "2h");
    assert_eq!(revision, 2);
    assert!(backend.update_web(&admin, patch("3h", 1)).await.is_err());
    assert!(
        crate::model::audit::scope((-1).into(), backend.update_web(&admin, patch("3h", 2)))
            .await
            .is_err()
    );
    assert_eq!(backend.row("web").await?.revision, 2);
    sqlx::query("UPDATE runtime_settings SET revision=? WHERE section='maintenance'")
        .bind(i64::MAX)
        .execute(db)
        .await?;
    assert!(
        backend
            .save(&admin, "maintenance", None, changed)
            .await
            .is_err()
    );
    assert_eq!(backend.row("maintenance").await?.revision, i64::MAX);
    assert_eq!(backend.value("maintenance").await?, snapshot.value);

    // Authorization stays locked until the settings transaction commits.
    let guard = transactions::admin_transaction(db, &admin).await?;
    let mut independent = db.begin().await?;
    sqlx::query("SELECT id FROM sessions WHERE id=? FOR UPDATE NOWAIT")
        .bind(auth.session_id)
        .fetch_one(&mut *independent)
        .await?;
    independent.commit().await?;
    let mut password_change =
        Box::pin(backend.change_password(&verified, &[1; 32], "next-password", &[5; 32]));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut password_change)
            .await
            .is_err()
    );
    guard.commit().await?;
    password_change.await?;
    assert!(backend.sections(&admin).await.is_err());
    assert!(backend.sections(&token_admin).await.is_err());
    let next = backend.authenticate_session(&[1; 32]).await?.unwrap();
    let admin = Principal::User {
        id: user,
        session: next.session_id,
        revision: next.authorization_revision,
    };
    assert_eq!(next.authorization_revision, auth.authorization_revision + 1);
    assert_eq!(backend.sections(&admin).await?.len(), defaults.len());
    sqlx::query("UPDATE web_users SET must_change_password=TRUE WHERE id=?")
        .bind(user)
        .execute(db)
        .await?;
    assert!(backend.sections(&admin).await.is_err());
    sqlx::query("UPDATE web_users SET must_change_password=FALSE,role='member' WHERE id=?")
        .bind(user)
        .execute(db)
        .await?;
    assert!(backend.sections(&admin).await.is_err());
    sqlx::query("UPDATE web_users SET role='admin',enabled=FALSE WHERE id=?")
        .bind(user)
        .execute(db)
        .await?;
    assert!(backend.sections(&admin).await.is_err());
    backend.pool.close().await;
    backend.owner.into_inner().close().await?;
    Ok(())
}
