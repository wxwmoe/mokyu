use super::{connection, queries, transactions};
use crate::authorization::{Action, DEFAULT_PROJECT, Permit, Principal};
use crate::config::Config;
use crate::model::Id;
use crate::model::account::SessionLogin;
use crate::model::{AccountStore, AuthorizationStore, RuntimeSettingsStore};
use anyhow::Result;
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_SQLITE_CONFIG pointing to a fresh isolated SQLite database"]
async fn sqlite_authorization_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    assert!(budget.database_connections >= 2);
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.write;
    let storage = Uuid::new_v4();
    sqlx::query("INSERT INTO storages(id,name,type,path,identity_marker) VALUES(?,'test','local','/data/test','{}')")
        .bind(storage).execute(db).await?;
    assert!(backend.setup_required().await?);
    let admin = backend.initialize_admin("admin", "password", false).await?;
    let member = Uuid::new_v4();
    sqlx::query("INSERT INTO web_users(id,username,password_hash) VALUES(?,'member','password')")
        .bind(member)
        .execute(db)
        .await?;
    let mut users = Vec::new();
    for (name, seed) in [("admin", 1u8), ("member", 2)] {
        let verified = backend.login_user(name).await?.unwrap();
        backend
            .login(SessionLogin {
                verified: &verified,
                access_hash: &[seed; 32],
                csrf_hash: &[seed; 32],
                refresh_hash: &[seed; 32],
                seconds: 3600,
                user_agent: "test",
                request_id: "test",
            })
            .await?;
        let session = backend.authenticate_session(&[seed; 32]).await?.unwrap();
        users.push(Principal::User {
            id: session.user_id,
            session: session.session_id,
            revision: session.authorization_revision,
        });
    }
    let bucket = Uuid::new_v4();
    let other = Uuid::new_v4();
    let project = Uuid::new_v4();
    sqlx::query("INSERT INTO projects(id,name) VALUES(?,'Other')")
        .bind(project)
        .execute(db)
        .await?;
    for (id, name, project) in [
        (bucket, "first", DEFAULT_PROJECT),
        (other, "second", project),
    ] {
        sqlx::query("INSERT INTO buckets(id,name,project_id,storage_id) VALUES(?,?,?,?)")
            .bind(id)
            .bind(name)
            .bind(project)
            .bind(storage)
            .execute(db)
            .await?;
    }
    for principal in [
        &users[0],
        &Principal::Local,
        &Principal::System,
        &Principal::Bootstrap,
    ] {
        assert_eq!(backend.actions(principal, bucket).await?, Action::ALL);
        backend.admin(principal).await?.finish().await?;
    }
    assert!(backend.admin(&users[1]).await.is_err());
    assert!(backend.actions(&users[1], bucket).await?.is_empty());
    let node = Principal::Node {
        id: Uuid::new_v4(),
        role: "control".into(),
    };
    assert!(backend.actions(&node, bucket).await.is_err());
    assert!(backend.admin(&node).await.is_err());
    assert!(backend.service("missing").await.is_err());

    assert_eq!(queries::action_bits(&Action::ALL), 127);
    assert_eq!(queries::action_bits(&Action::role("reader")), 67);
    assert_eq!(queries::action_bits(&Action::role("writer")), 95);
    for action in Action::ALL {
        assert_eq!(queries::actions(queries::action_bit(action)), vec![action]);
    }
    sqlx::query(
        "INSERT INTO project_members(user_id,project_id,role,scope) VALUES(?,?,'reader','all')",
    )
    .bind(member)
    .bind(DEFAULT_PROJECT)
    .execute(db)
    .await?;
    for role in ["reader", "writer", "maintainer"] {
        sqlx::query("UPDATE project_members SET role=? WHERE user_id=?")
            .bind(role)
            .bind(member)
            .execute(db)
            .await?;
        assert_eq!(
            backend.actions(&users[1], bucket).await?,
            Action::role(role)
        );
        assert!(backend.actions(&users[1], other).await?.is_empty());
    }
    sqlx::query("UPDATE project_members SET role='reader',scope='selected' WHERE user_id=?")
        .bind(member)
        .execute(db)
        .await?;
    assert!(backend.actions(&users[1], bucket).await?.is_empty());
    sqlx::query(
        "INSERT INTO member_bucket_grants(user_id,project_id,bucket_id,actions) VALUES(?,?,?,127)",
    )
    .bind(member)
    .bind(DEFAULT_PROJECT)
    .bind(bucket)
    .execute(db)
    .await?;
    assert_eq!(
        backend.actions(&users[1], bucket).await?,
        Action::role("reader")
    );
    assert!(
        sqlx::query("UPDATE member_bucket_grants SET actions=128 WHERE user_id=?")
            .bind(member)
            .execute(db)
            .await
            .is_err()
    );
    assert!(sqlx::query("INSERT INTO member_bucket_grants(user_id,project_id,bucket_id,actions) VALUES(?,?,?,127)").bind(member).bind(DEFAULT_PROJECT).bind(other).execute(db).await.is_err());

    let token = Uuid::new_v4();
    sqlx::query("INSERT INTO api_tokens(id,user_id,label,token_hash,prefix,system_access,auth_revision) VALUES(?,?,'test',?,'test',TRUE,0)").bind(token).bind(member).bind([8u8;32].as_slice()).execute(db).await?;
    sqlx::query("INSERT INTO token_bucket_grants(token_id,bucket_id,actions) VALUES(?,?,127)")
        .bind(token)
        .bind(bucket)
        .execute(db)
        .await?;
    let member_token = Principal::Token {
        id: token,
        user: member,
        revision: 0,
        user_revision: 0,
    };
    assert_eq!(
        backend.actions(&member_token, bucket).await?,
        Action::role("reader")
    );
    assert!(backend.admin(&member_token).await.is_err());
    sqlx::query("UPDATE member_bucket_grants SET actions=? WHERE user_id=?")
        .bind(queries::action_bits(&[Action::List, Action::Inspect]))
        .bind(member)
        .execute(db)
        .await?;
    assert_eq!(
        backend.actions(&member_token, bucket).await?,
        vec![Action::List, Action::Inspect]
    );
    sqlx::query("UPDATE api_tokens SET authorization_revision=1 WHERE id=?")
        .bind(token)
        .execute(db)
        .await?;
    assert!(backend.actions(&member_token, bucket).await.is_err());
    sqlx::query("UPDATE api_tokens SET authorization_revision=0 WHERE id=?")
        .bind(token)
        .execute(db)
        .await?;
    sqlx::query("UPDATE web_users SET authorization_revision=1 WHERE id=?")
        .bind(member)
        .execute(db)
        .await?;
    assert!(backend.actions(&member_token, bucket).await.is_err());
    assert!(backend.actions(&users[1], bucket).await.is_err());
    sqlx::query("UPDATE web_users SET authorization_revision=0 WHERE id=?")
        .bind(member)
        .execute(db)
        .await?;
    sqlx::query("UPDATE api_tokens SET user_id=?,system_access=FALSE WHERE id=?")
        .bind(admin)
        .bind(token)
        .execute(db)
        .await?;
    let admin_token = Principal::Token {
        id: token,
        user: admin,
        revision: 0,
        user_revision: 0,
    };
    assert_eq!(backend.actions(&admin_token, bucket).await?, Action::ALL);
    assert!(backend.actions(&admin_token, other).await?.is_empty());
    assert!(backend.admin(&admin_token).await.is_err());
    sqlx::query("UPDATE api_tokens SET system_access=TRUE WHERE id=?")
        .bind(token)
        .execute(db)
        .await?;
    assert_eq!(backend.actions(&admin_token, other).await?, Action::ALL);
    backend.admin(&admin_token).await?.finish().await?;

    sqlx::query(
        "INSERT INTO s3_credentials(access_key_id,secret_encrypted,project_id) VALUES('test',?,?)",
    )
    .bind([9u8; 32].as_slice())
    .bind(DEFAULT_PROJECT)
    .execute(db)
    .await?;
    for id in [bucket, other] {
        sqlx::query(
            "INSERT INTO s3_credential_grants(access_key,bucket_id,actions) VALUES('test',?,?)",
        )
        .bind(id)
        .bind(queries::action_bits(&[Action::Read]))
        .execute(db)
        .await?;
    }
    let service = backend.service("test").await?;
    assert_eq!(backend.actions(&service, bucket).await?, vec![Action::Read]);
    assert!(backend.actions(&service, other).await?.is_empty());
    assert!(backend.admin(&service).await.is_err());
    let permit = service.require(&backend, bucket, Action::Read).await?;
    assert!(
        service
            .require(&backend, bucket, Action::Write)
            .await
            .is_err()
    );
    let mut guard = db.begin_with(queries::BEGIN_WRITE).await?;
    transactions::lock_permit(&mut guard, &permit).await?;
    assert_eq!(backend.actions(&service, bucket).await?, vec![Action::Read]);
    let mut revoke = Box::pin(sqlx::query("UPDATE s3_credentials SET enabled=FALSE,authorization_revision=authorization_revision+1 WHERE access_key_id='test'").execute(db));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut revoke)
            .await
            .is_err()
    );
    guard.commit().await?;
    revoke.await?;
    assert!(backend.actions(&service, bucket).await.is_err());
    let mut guard = db.begin_with(queries::BEGIN_WRITE).await?;
    assert!(
        transactions::lock_permit(&mut guard, &permit)
            .await
            .is_err()
    );
    guard.rollback().await?;
    assert!(backend.service("test").await.is_err());
    sqlx::query("UPDATE s3_credentials SET enabled=TRUE,expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)-1000000 WHERE access_key_id='test'").execute(db).await?;
    assert!(backend.service("test").await.is_err());

    let missing = Permit::for_action(Principal::System, Uuid::new_v4(), Action::Write);
    let mut guard = db.begin_with(queries::BEGIN_WRITE).await?;
    assert!(
        transactions::lock_permit(&mut guard, &missing)
            .await
            .is_err()
    );
    guard.rollback().await?;
    let audit: Id = (sqlx::query(
        "INSERT INTO audit_events(actor_label,source,action,target) VALUES('test','cli','test','')",
    )
    .execute(db)
    .await?
    .last_insert_rowid())
    .into();
    let mut guard = backend.admin(&Principal::Local).await?;
    crate::model::audit::scope(
        audit,
        guard.checkpoint("test.allowed", "target", serde_json::json!({"ok":true})),
    )
    .await?;
    guard.finish().await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT outcome FROM audit_events WHERE id=?")
            .bind(audit)
            .fetch_one(db)
            .await?,
        "succeeded"
    );
    let mut guard = backend.admin(&Principal::Local).await?;
    assert!(
        crate::model::audit::scope(
            (-1).into(),
            guard.checkpoint("test.denied", "target", serde_json::json!({}))
        )
        .await
        .is_err()
    );
    drop(guard);
    let verified = backend.login_user("admin").await?.unwrap();
    let mut guard = backend.admin(&users[0]).await?;
    let mut password = Box::pin(backend.change_password(&verified, &[1; 32], "new", &[42; 32]));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut password)
            .await
            .is_err()
    );
    // The waiting identity change must not reserve the writer ahead of this operation.
    tokio::time::timeout(Duration::from_secs(2), async {
        backend.ensure(&[("guard", serde_json::json!({}))]).await?;
        assert_eq!(backend.row("guard").await?.revision, 1);
        guard
            .checkpoint("test.nested", "", serde_json::json!({}))
            .await?;
        Result::<()>::Ok(())
    })
    .await??;
    guard.finish().await?;
    password.await?;
    assert!(backend.admin(&users[0]).await.is_err());
    backend.read.close().await;
    backend.write.close().await;
    drop(backend);

    let mut one = budget.clone();
    one.database_connections = 1;
    let backend = connection::open(&config, &secrets, &one, "single").await?;
    let mut guard = backend.admin(&Principal::Local).await?;
    tokio::time::timeout(Duration::from_secs(2), async {
        backend.ensure(&[("one", serde_json::json!({}))]).await?;
        assert_eq!(backend.row("one").await?.revision, 1);
        guard
            .checkpoint("test.one", "", serde_json::json!({}))
            .await?;
        Result::<()>::Ok(())
    })
    .await??;
    guard.finish().await?;
    backend.read.close().await;
    backend.write.close().await;
    Ok(())
}
