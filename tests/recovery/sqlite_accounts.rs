use super::{Backend, connection};
use crate::config::Config;
use crate::model::account::{
    AccountStore, Preferences, RefreshedSession, SessionLogin, SessionRefresh, VerifiedAccount,
};
use anyhow::Result;
use chrono::Utc;
use uuid::Uuid;

fn hash(secret: &str) -> [u8; 32] {
    *blake3::hash(secret.as_bytes()).as_bytes()
}

async fn login(
    backend: &Backend,
    verified: &VerifiedAccount,
    access: &str,
    refresh: &str,
) -> Result<()> {
    backend
        .login(SessionLogin {
            verified,
            access_hash: &hash(access),
            csrf_hash: &hash("test-csrf"),
            refresh_hash: &hash(refresh),
            seconds: 3600,
            user_agent: "native-sqlite-contract",
            request_id: "native-sqlite-test",
        })
        .await
}

async fn refresh(backend: &Backend, supplied: &str) -> Result<RefreshedSession> {
    backend
        .refresh(SessionRefresh {
            supplied,
            access: None,
            key: &[9; 32],
            seconds: 3600,
            request_id: "native-sqlite-test",
        })
        .await
}

async fn audit_intent(backend: &Backend) -> Result<i64> {
    Ok(sqlx::query(
        "INSERT INTO audit_events(actor_label,source,action,target) VALUES('test','web','test','')",
    )
    .execute(&backend.write)
    .await?
    .last_insert_rowid())
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_SQLITE_CONFIG pointing to a fresh isolated SQLite database"]
async fn sqlite_account_security_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.write;
    assert!(backend.setup_required().await?);
    backend.initialize().await?;
    assert!(backend.setup_hash().await?.is_none());
    backend.setup_token(&hash("expired-setup")).await?;
    sqlx::query(
        "UPDATE setup_tokens SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)-1000000",
    )
    .execute(db)
    .await?;
    assert!(backend.setup_hash().await?.is_none());
    assert!(
        backend
            .setup("admin", "password-a", &hash("expired-setup"), "test")
            .await
            .is_err()
    );
    backend.setup_token(&hash("old-setup")).await?;
    backend.setup_token(&hash("setup")).await?;
    assert_eq!(backend.setup_hash().await?.unwrap(), hash("setup"));
    assert!(
        backend
            .setup("admin", "password-a", &hash("old-setup"), "test")
            .await
            .is_err()
    );
    let setup_hash = hash("setup");
    let (first, second) = tokio::join!(
        backend.setup("admin", "password-a", &setup_hash, "test"),
        backend.setup("admin", "password-a", &setup_hash, "test")
    );
    assert_ne!(first.is_ok(), second.is_ok());
    first.or(second)?;
    assert!(!backend.setup_required().await?);
    assert!(backend.setup_hash().await?.is_none());
    assert!(backend.setup_token(&hash("later-setup")).await.is_err());
    assert!(
        backend
            .initialize_admin("other", "password-a", false)
            .await
            .is_err()
    );

    let verified = backend.login_user("admin").await?.unwrap();
    let user = verified.0;
    assert!(backend.login_user("Admin").await?.is_none());
    assert!(backend.login_user("admin ").await?.is_none());
    assert_eq!(
        backend.password(user).await?.unwrap(),
        ("password-a".into(), 0)
    );
    login(&backend, &verified, "access-a", "refresh-a").await?;
    login(&backend, &verified, "access-b", "refresh-b").await?;
    let current = hash("access-a");
    let session = backend.authenticate_session(&current).await?.unwrap();
    assert_eq!(session.user_id, user);
    assert_eq!(session.csrf_hash, hash("test-csrf"));
    assert!(backend.recent(session.session_id, user).await?);
    assert!(!backend.recent(session.session_id, Uuid::new_v4()).await?);
    let listed = backend.sessions(user, &current).await?;
    assert_eq!(listed.len(), 2);
    assert_eq!(listed.iter().filter(|row| row.current).count(), 1);
    assert_eq!(
        backend.session(&current).await?.unwrap().0,
        session.session_id
    );
    sqlx::query(
        "UPDATE sessions SET last_seen_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)-600000000 WHERE id=?",
    )
    .bind(session.session_id)
    .execute(db)
    .await?;
    backend.authenticate_session(&current).await?.unwrap();
    let seen: i64 = sqlx::query_scalar("SELECT last_seen_at FROM sessions WHERE id=?")
        .bind(session.session_id)
        .fetch_one(db)
        .await?;
    assert!((Utc::now() - super::queries::datetime(seen)?).num_seconds() < 10);

    let (first, second) = tokio::join!(
        backend.failed_login(user, "failed-a"),
        backend.failed_login(user, "failed-b")
    );
    first?;
    second?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM audit_events WHERE action='session.login' AND outcome='failed'"
        )
        .fetch_one(db)
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT detail FROM audit_events WHERE action='session.login' AND outcome='failed'"
        )
        .fetch_one(db)
        .await?,
        serde_json::json!({"identity_verified":false})
    );

    let preferences = || Preferences {
        display_name: "  Native user  ".into(),
        locale: Some("ja".into()),
        theme: Some("dark".into()),
        avatar_email: " USER@example.test ".into(),
        avatar_enabled: true,
    };
    assert!(
        crate::model::audit::scope(
            (-1).into(),
            backend.preferences(user, &current, preferences())
        )
        .await
        .is_err()
    );
    assert_eq!(backend.profile(user).await?.unwrap().display_name, "");
    let audit = audit_intent(&backend).await?;
    crate::model::audit::scope(
        audit.into(),
        backend.preferences(user, &current, preferences()),
    )
    .await?;
    let profile = backend.profile(user).await?.unwrap();
    assert_eq!(
        (profile.display_name.as_str(), profile.avatar_email.as_str()),
        ("Native user", "user@example.test")
    );
    assert_eq!(profile.locale.as_deref(), Some("ja"));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT outcome FROM audit_events WHERE id=?")
            .bind(audit)
            .fetch_one(db)
            .await?,
        "succeeded"
    );
    assert!(
        backend
            .preferences(Uuid::new_v4(), &current, preferences())
            .await
            .is_err()
    );

    let token_hash = hash("api-token");
    sqlx::query("INSERT INTO api_tokens(id,user_id,label,token_hash,prefix,system_access,auth_revision) VALUES(?,?,'test',?,'test',TRUE,0)")
        .bind(Uuid::new_v4()).bind(user).bind(token_hash.as_slice()).execute(db).await?;
    assert_eq!(
        backend.authenticate_token(&token_hash).await?.unwrap().1,
        user
    );
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT last_used_at IS NOT NULL FROM api_tokens")
            .fetch_one(db)
            .await?
    );
    let writer = backend
        .write
        .begin_with(super::queries::BEGIN_WRITE)
        .await?;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        assert!(backend.authenticate_token(&token_hash).await?.is_some());
        assert!(backend.authenticate_session(&current).await?.is_some());
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    writer.rollback().await?;
    sqlx::query(
        "UPDATE api_tokens SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)-1000000",
    )
    .execute(db)
    .await?;
    assert!(backend.authenticate_token(&token_hash).await?.is_none());
    sqlx::query("UPDATE api_tokens SET expires_at=NULL")
        .execute(db)
        .await?;

    // Missing audit intent must roll back the password, revision, and revoked credentials.
    let next_refresh = hash("password-refresh");
    assert!(
        crate::model::audit::scope(
            (-1).into(),
            backend.change_password(&verified, &current, "password-b", &next_refresh)
        )
        .await
        .is_err()
    );
    assert_eq!(
        backend.password(user).await?.unwrap(),
        ("password-a".into(), 0)
    );
    assert!(
        backend
            .authenticate_session(&hash("access-b"))
            .await?
            .is_some()
    );
    assert!(backend.authenticate_token(&token_hash).await?.is_some());
    let (first, second) = tokio::join!(
        backend.change_password(&verified, &current, "password-b", &next_refresh),
        backend.change_password(&verified, &current, "password-b", &next_refresh)
    );
    assert_ne!(first.is_ok(), second.is_ok());
    first.or(second)?;
    assert_eq!(
        backend.password(user).await?.unwrap(),
        ("password-b".into(), 1)
    );
    assert!(backend.authenticate_session(&current).await?.is_some());
    assert!(
        backend
            .authenticate_session(&hash("access-b"))
            .await?
            .is_none()
    );
    assert!(backend.authenticate_token(&token_hash).await?.is_none());
    assert!(refresh(&backend, "refresh-a").await.is_err());
    assert!(refresh(&backend, "refresh-b").await.is_err());
    assert!(
        login(&backend, &verified, "stale-access", "stale-refresh")
            .await
            .is_err()
    );
    assert!(
        backend
            .reauthenticate(&verified, &current, &hash("stale-reauth"))
            .await
            .is_err()
    );
    let verified = backend.login_user("admin").await?.unwrap();
    backend
        .reauthenticate(&verified, &current, &hash("reauth-refresh"))
        .await?;
    assert!(refresh(&backend, "password-refresh").await.is_err());

    // Both concurrent refresh callers must receive the same successor credentials.
    sqlx::query("UPDATE session_refresh_tokens SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+259200000000 WHERE revoked_at IS NULL").execute(db).await?;
    let (first, second) = tokio::join!(
        refresh(&backend, "reauth-refresh"),
        refresh(&backend, "reauth-refresh")
    );
    let first = first?;
    let second = second?;
    assert_eq!(
        (&first.access, &first.refresh, &first.csrf),
        (&second.access, &second.refresh, &second.csrf)
    );
    assert_eq!(first.user, user);
    assert_eq!(first.session, session.session_id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM session_refresh_tokens WHERE revoked_at IS NULL"
        )
        .fetch_one(db)
        .await?,
        1
    );
    assert!(backend.authenticate_session(&current).await?.is_none());
    assert!(
        backend
            .authenticate_session(&hash(&first.access))
            .await?
            .is_some()
    );
    sqlx::query("UPDATE sessions SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)-1000000 WHERE id=?")
        .bind(first.session)
        .execute(db)
        .await?;
    assert!(
        backend
            .authenticate_session(&hash(&first.access))
            .await?
            .is_none()
    );
    let renewed = refresh(&backend, &first.refresh).await?;
    assert!(renewed.expires > Utc::now());
    assert!(
        backend
            .authenticate_session(&hash(&renewed.access))
            .await?
            .is_some()
    );
    sqlx::query("UPDATE session_refresh_tokens SET revoked_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)-31000000 WHERE token_hash=?").bind(hash("reauth-refresh").as_slice()).execute(db).await?;
    assert!(refresh(&backend, "reauth-refresh").await.is_err());
    assert!(
        backend
            .authenticate_session(&hash(&renewed.access))
            .await?
            .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM session_refresh_tokens")
            .fetch_one(db)
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM audit_events WHERE action='session.refresh_reuse'"
        )
        .fetch_one(db)
        .await?,
        1
    );

    login(&backend, &verified, "access-c", "refresh-c").await?;
    login(&backend, &verified, "access-d", "refresh-d").await?;
    let current = hash("access-c");
    let other = backend.session(&hash("access-d")).await?.unwrap().0;
    backend.revoke_sessions(user, &current, Some(other)).await?;
    assert!(refresh(&backend, "refresh-d").await.is_err());
    login(&backend, &verified, "access-e", "refresh-e").await?;
    backend.revoke_sessions(user, &current, None).await?;
    assert!(refresh(&backend, "refresh-e").await.is_err());
    assert_eq!(backend.sessions(user, &current).await?.len(), 1);
    sqlx::query("UPDATE web_users SET enabled=FALSE WHERE id=?")
        .bind(user)
        .execute(db)
        .await?;
    assert!(backend.authenticate_session(&current).await?.is_none());
    assert!(refresh(&backend, "refresh-c").await.is_err());
    assert!(backend.login_user("admin").await?.is_none());
    assert!(backend.profile(user).await?.is_none());
    sqlx::query("UPDATE web_users SET enabled=TRUE WHERE id=?")
        .bind(user)
        .execute(db)
        .await?;
    backend.logout(user, &current).await?;
    assert!(backend.authenticate_session(&current).await?.is_none());
    assert!(refresh(&backend, "refresh-c").await.is_err());

    // CLI initialization shares the setup lock and refuses a second administrator.
    sqlx::query("DELETE FROM web_users WHERE id=?")
        .bind(user)
        .execute(db)
        .await?;
    let admin = backend
        .initialize_admin("cli-admin", "password-c", true)
        .await?;
    assert!(backend.profile(admin).await?.unwrap().must_change_password);
    assert!(
        backend
            .initialize_admin("cli-other", "password-c", false)
            .await
            .is_err()
    );
    backend.initialize().await?;
    backend.check_owner()?;
    backend.read.close().await;
    backend.write.close().await;
    Ok(())
}
