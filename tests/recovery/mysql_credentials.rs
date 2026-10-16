use super::connection;
use crate::authorization::{Action, DEFAULT_PROJECT, Principal};
use crate::config::Config;
use crate::model::account::SessionLogin;
use crate::model::administration::{BucketGrant, Membership, UserCreate};
use crate::model::credential::{
    CredentialInput, CredentialMaterial, CredentialSettings, RotateCredential, TokenInput,
    TokenMaterial,
};
use crate::model::{AccountStore, AdministrationStore, AuthorizationStore, CredentialStore};
use anyhow::Result;
use uuid::Uuid;

fn grants(bucket: Uuid) -> Vec<BucketGrant> {
    vec![BucketGrant {
        bucket_id: bucket,
        actions: vec![Action::List, Action::Read],
    }]
}

fn credential_input(bucket: Uuid) -> CredentialInput {
    CredentialInput {
        project_id: DEFAULT_PROJECT,
        label: " service ".into(),
        expires_in: Some("1h".into()),
        grants: grants(bucket),
    }
}

fn material(access: &str) -> CredentialMaterial {
    CredentialMaterial {
        access: access.into(),
        encrypted: vec![1, 2, 3],
    }
}

fn token_input(bucket: Uuid) -> TokenInput {
    TokenInput {
        label: " token ".into(),
        system: false,
        expires_in: Some("1h".into()),
        keep_expiry: false,
        grants: grants(bucket),
    }
}

fn token_material(seed: u8) -> TokenMaterial {
    TokenMaterial {
        prefix: format!("test{seed}"),
        hash: [seed; 32],
    }
}

async fn token_principal(backend: &super::Backend, seed: u8) -> Result<Principal> {
    let (id, user, _, _, revision, user_revision) =
        backend.authenticate_token(&[seed; 32]).await?.unwrap();
    Ok(Principal::Token {
        id,
        user,
        revision,
        user_revision,
    })
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_credential_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    let storage = super::storage_tests::fixture(&backend).await?;
    let local = Principal::Local;
    assert!(backend.setup_required().await?);
    let admin = backend.initialize_admin("admin", "password", false).await?;
    let member = backend
        .create_user(
            &local,
            UserCreate {
                username: "member".into(),
                password_hash: "password".into(),
                role: "member".into(),
                must_change_password: false,
            },
        )
        .await?;
    let project = Uuid::new_v4();
    sqlx::query("INSERT INTO projects(id,name) VALUES(?,'Other')")
        .bind(project)
        .execute(db)
        .await?;
    let bucket = Uuid::new_v4();
    let second = Uuid::new_v4();
    let foreign = Uuid::new_v4();
    for (id, name, project) in [
        (bucket, "bucket-a", DEFAULT_PROJECT),
        (second, "bucket-b", DEFAULT_PROJECT),
        (foreign, "foreign", project),
    ] {
        sqlx::query("INSERT INTO buckets(id,name,project_id,storage_id) VALUES(?,?,?,?)")
            .bind(id)
            .bind(name)
            .bind(project)
            .bind(storage)
            .execute(db)
            .await?;
    }
    let credential = backend
        .create_credential(&local, credential_input(bucket), material("KEY-A"))
        .await?;
    assert_eq!(credential.label, "service");
    assert_eq!(credential.grants.len(), 1);
    assert_eq!(
        backend.encrypted_secret("KEY-A").await?,
        Some(vec![1, 2, 3])
    );
    assert!(backend.encrypted_secret("key-a").await?.is_none());
    assert!(backend.encrypted_secret("KEY-A ").await?.is_none());
    assert!(backend.service("KEY-A ").await.is_err());
    assert!(backend.credential("KEY-A ").await.is_err());
    assert!(
        backend
            .create_credential(&local, credential_input(bucket), material("KEY-A"))
            .await
            .is_err()
    );
    assert!(
        backend
            .create_credential(&local, credential_input(foreign), material("INVALID"))
            .await
            .is_err()
    );
    assert!(backend.credential("INVALID").await.is_err());
    assert!(
        crate::model::audit::scope(
            -1,
            backend.create_credential(&local, credential_input(bucket), material("ROLLBACK"))
        )
        .await
        .is_err()
    );
    assert!(backend.credential("ROLLBACK").await.is_err());
    let first = backend.service("KEY-A").await?;
    assert_eq!(backend.actions(&first, bucket).await?.len(), 2);
    let before = backend.credential("KEY-A").await?.expires_at;
    backend.touch_credential("KEY-A").await?;
    let touched = backend.credential("KEY-A").await?.last_used_at;
    assert!(touched.is_some());
    backend.touch_credential("KEY-A").await?;
    assert_eq!(backend.credential("KEY-A").await?.last_used_at, touched);
    let updated = backend
        .update_credential(
            &local,
            "KEY-A",
            CredentialSettings {
                label: "next".into(),
                enabled: true,
                expires_in: None,
                keep_expiry: true,
                grants: None,
            },
        )
        .await?;
    assert_eq!(updated.expires_at, before);
    assert!(backend.actions(&first, bucket).await.is_err());
    assert!(
        backend
            .update_credential(
                &local,
                "KEY-A",
                CredentialSettings {
                    label: "bad".into(),
                    enabled: true,
                    expires_in: Some("1h".into()),
                    keep_expiry: true,
                    grants: None
                }
            )
            .await
            .is_err()
    );
    assert!(
        backend
            .set_grants(&local, "KEY-A", grants(foreign))
            .await
            .is_err()
    );
    let mut duplicate = grants(bucket);
    duplicate.extend(grants(bucket));
    assert!(
        backend
            .set_grants(&local, "KEY-A", duplicate)
            .await
            .is_err()
    );
    assert_eq!(backend.credential("KEY-A").await?.grants.len(), 1);
    let service = backend.service("KEY-A").await?;
    backend
        .change_grant(&local, "KEY-A", second, vec![Action::List])
        .await?;
    assert!(backend.actions(&service, bucket).await.is_err());
    assert_eq!(
        backend
            .buckets("KEY-A", "bucket-", "", None, "region", 10)
            .await?
            .len(),
        2
    );
    assert_eq!(
        backend
            .buckets("KEY-A", "", "bucket-a", Some("region"), "region", 1)
            .await?[0]
            .id,
        second
    );
    for prefix in ["bucket-a ", "%", "bucket_"] {
        assert!(
            backend
                .buckets("KEY-A", prefix, "", None, "region", 10)
                .await?
                .is_empty()
        );
    }
    assert!(
        backend
            .buckets("KEY-A", "", "", Some("wrong"), "region", 10)
            .await?
            .is_empty()
    );
    backend
        .change_grant(&local, "KEY-A", second, vec![])
        .await?;
    assert_eq!(backend.credential("KEY-A").await?.grants.len(), 1);
    let (replacement, old_expiry) = backend
        .rotate_credential(
            &local,
            "KEY-A",
            RotateCredential {
                overlap: "10m".into(),
                expires_in: Some("2h".into()),
            },
            material("KEY-B"),
        )
        .await?;
    assert_eq!(replacement.grants[0].bucket_id, bucket);
    assert!(old_expiry <= before.unwrap());
    assert_eq!(
        backend.credentials(Some(DEFAULT_PROJECT), "", 1).await?[0].access_key,
        "KEY-A"
    );
    assert_eq!(
        backend.credentials(None, "KEY-A", 10).await?[0].access_key,
        "KEY-B"
    );
    assert!(backend.credentials(Some(project), "", 10).await?.is_empty());
    backend.enable_credential(&local, "KEY-B", false).await?;
    assert!(backend.encrypted_secret("KEY-B").await?.is_none());
    assert!(
        backend
            .buckets("KEY-B", "", "", None, "region", 10)
            .await?
            .is_empty()
    );
    assert!(
        backend
            .rotate_credential(
                &local,
                "KEY-B",
                RotateCredential {
                    overlap: "1m".into(),
                    expires_in: None
                },
                material("KEY-C")
            )
            .await
            .is_err()
    );
    backend.enable_credential(&local, "KEY-B", true).await?;
    let service = backend.service("KEY-B").await?;
    assert!(
        crate::model::audit::scope(-1, backend.set_grants(&local, "KEY-B", vec![]))
            .await
            .is_err()
    );
    assert_eq!(backend.actions(&service, bucket).await?.len(), 2);
    assert!(
        crate::model::audit::scope(-1, backend.revoke_credential(&local, "KEY-B"))
            .await
            .is_err()
    );
    assert!(backend.service("KEY-B").await.is_ok());
    backend.revoke_credential(&local, "KEY-B").await?;
    assert!(backend.service("KEY-B").await.is_err());
    backend
        .rotate_credential(
            &local,
            "KEY-A",
            RotateCredential {
                overlap: "1s".into(),
                expires_in: None,
            },
            material("KEY-C"),
        )
        .await?;
    sqlx::query("UPDATE s3_credentials SET expires_at=CURRENT_TIMESTAMP(6)-INTERVAL 1 MICROSECOND WHERE access_key_id='KEY-A'").execute(db).await?;
    assert!(backend.encrypted_secret("KEY-A").await?.is_none());
    assert!(
        backend
            .buckets("KEY-A", "", "", None, "region", 10)
            .await?
            .is_empty()
    );

    backend
        .save_member(
            &local,
            DEFAULT_PROJECT,
            member.id,
            Some(Membership {
                role: "reader".into(),
                scope: "all".into(),
                grants: vec![],
            }),
        )
        .await?;
    let verified = backend.login_user("member").await?.unwrap();
    backend
        .login(SessionLogin {
            verified: &verified,
            access_hash: &[10; 32],
            csrf_hash: &[10; 32],
            refresh_hash: &[10; 32],
            seconds: 3600,
            user_agent: "test",
            request_id: "test",
        })
        .await?;
    let session = backend.authenticate_session(&[10; 32]).await?.unwrap();
    let user = Principal::User {
        id: member.id,
        session: session.session_id,
        revision: session.authorization_revision,
    };
    assert!(
        backend
            .create_credential(&user, credential_input(bucket), material("DENIED"))
            .await
            .is_err()
    );
    assert!(
        backend
            .create_token(&user, admin, token_input(bucket), token_material(21))
            .await
            .is_err()
    );
    let mut excessive = token_input(bucket);
    excessive.grants[0].actions.push(Action::Write);
    assert!(
        backend
            .create_token(&user, member.id, excessive, token_material(21))
            .await
            .is_err()
    );
    assert!(
        backend
            .create_token(&user, member.id, token_input(foreign), token_material(21))
            .await
            .is_err()
    );
    let mut system = token_input(bucket);
    system.system = true;
    system.grants.clear();
    assert!(
        backend
            .create_token(&user, member.id, system, token_material(21))
            .await
            .is_err()
    );
    let token = backend
        .create_token(&user, member.id, token_input(bucket), token_material(21))
        .await?;
    assert!(token.active);
    assert_eq!(token.label, "token");
    assert_eq!(backend.token_user(token.id).await?, Some(member.id));
    assert_eq!(backend.tokens(member.id, None, 1).await?.len(), 1);
    assert!(
        backend
            .tokens(member.id, Some(token.id), 1)
            .await?
            .is_empty()
    );
    let principal = token_principal(&backend, 21).await?;
    assert_eq!(backend.actions(&principal, bucket).await?.len(), 2);
    assert!(backend.admin(&principal).await.is_err());
    let mut input = token_input(bucket);
    input.expires_in = None;
    input.keep_expiry = true;
    input.grants[0].actions = vec![Action::Read];
    let updated = backend
        .update_token(&user, member.id, token.id, input)
        .await?;
    assert_eq!(updated.expires_at, token.expires_at);
    assert!(backend.actions(&principal, bucket).await.is_err());
    let principal = token_principal(&backend, 21).await?;
    assert_eq!(
        backend.actions(&principal, bucket).await?,
        vec![Action::Read]
    );
    assert!(
        crate::model::audit::scope(
            -1,
            backend.update_token(&user, member.id, token.id, token_input(bucket))
        )
        .await
        .is_err()
    );
    assert_eq!(
        backend.actions(&principal, bucket).await?,
        vec![Action::Read]
    );
    assert!(
        crate::model::audit::scope(
            -1,
            backend.create_token(&user, member.id, token_input(bucket), token_material(22))
        )
        .await
        .is_err()
    );
    assert!(backend.authenticate_token(&[22; 32]).await?.is_none());
    let mut system = token_input(bucket);
    system.system = true;
    system.grants.clear();
    let system = backend
        .create_token(&local, admin, system, token_material(23))
        .await?;
    assert!(system.system && system.active);
    let system_principal = token_principal(&backend, 23).await?;
    backend.admin(&system_principal).await?.finish().await?;
    assert!(backend.revoke_token(&user, system.id).await.is_err());
    backend.revoke_token(&system_principal, token.id).await?;
    assert!(backend.authenticate_token(&[21; 32]).await?.is_none());
    assert!(!backend.tokens(member.id, None, 1).await?[0].active);
    assert!(
        backend
            .update_token(&user, member.id, token.id, token_input(bucket))
            .await
            .is_err()
    );
    backend.revoke_token(&local, token.id).await?;
    backend
        .create_token(&local, member.id, token_input(bucket), token_material(24))
        .await?;
    backend
        .reset_password(&local, member.id, "replacement", false)
        .await?;
    assert!(backend.authenticate_token(&[24; 32]).await?.is_none());
    assert!(
        backend
            .tokens(member.id, None, 10)
            .await?
            .iter()
            .all(|token| !token.active)
    );
    backend.pool.close().await;
    Ok(())
}
