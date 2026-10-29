use super::connection;
use crate::authorization::{Action, Principal};
use crate::config::Config;
use crate::model::account::SessionLogin;
use crate::model::administration::{BucketGrant, Membership, ProjectInput, UserCreate, UserPatch};
use crate::model::{AccountStore, AdministrationStore, AuthorizationStore};
use anyhow::Result;
use sqlx::Connection;
use uuid::Uuid;

fn user_input(username: &str, role: &str) -> UserCreate {
    UserCreate {
        username: username.into(),
        password_hash: "password".into(),
        role: role.into(),
        must_change_password: false,
    }
}

fn project_input(name: &str) -> ProjectInput {
    ProjectInput {
        name: name.into(),
        description: "test".into(),
        allow_bucket_create: true,
    }
}

fn membership(bucket: Uuid) -> Membership {
    Membership {
        external_grants: Vec::new(),
        role: "reader".into(),
        scope: "selected".into(),
        grants: vec![BucketGrant {
            bucket_id: bucket,
            actions: Action::ALL.to_vec(),
        }],
    }
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_administration_contract() -> Result<()> {
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
    let error = backend
        .create_user(&local, user_input("member", "member"))
        .await
        .err()
        .unwrap();
    assert_eq!(
        crate::model::error(&error),
        Some(crate::model::Error::LastAdministrator)
    );
    assert!(!backend.project_mode().await?);
    let admin = backend
        .create_user(&local, user_input("admin", "admin"))
        .await?;
    assert!(!backend.setup_required().await?);
    assert!(backend.delete_user(&local, admin.id).await.is_err());
    assert!(
        backend
            .update_user(
                &local,
                admin.id,
                UserPatch {
                    role: Some("member".into()),
                    enabled: None
                }
            )
            .await
            .is_err()
    );
    assert!(
        backend
            .update_user(
                &local,
                admin.id,
                UserPatch {
                    role: None,
                    enabled: Some(false)
                }
            )
            .await
            .is_err()
    );
    let second_admin = backend
        .create_user(&local, user_input("admin2", "admin"))
        .await?;
    assert!(
        crate::model::audit::scope(
            (-1).into(),
            backend.create_user(&local, user_input("rollback", "member"))
        )
        .await
        .is_err()
    );
    assert!(backend.find_user("rollback").await?.is_none());
    assert!(!backend.project_mode().await?);
    let (first, second) = tokio::join!(
        backend.create_user(&local, user_input("member", "member")),
        backend.create_user(&local, user_input("member", "member"))
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let member = first.or(second)?;
    assert!(backend.project_mode().await?);
    assert!(backend.set_project_mode(&local, false).await.is_err());
    assert_eq!(backend.find_user("member").await?.unwrap().id, member.id);
    assert!(backend.user(Uuid::new_v4()).await?.is_none());
    assert_eq!(backend.all_users().await?.len(), 3);
    assert_eq!(
        backend.users("MEMBER", "", 10, Some("member")).await?.len(),
        1
    );
    let literal = backend
        .create_user(&local, user_input("a_%", "member"))
        .await?;
    assert_eq!(backend.users("_%", "", 10, None).await?.len(), 1);
    assert_eq!(backend.users("", "", 1, None).await?.len(), 1);
    backend.delete_user(&local, literal.id).await?;

    let project = backend
        .save_project(&local, None, project_input(" Test "))
        .await?;
    assert_eq!(project.name, "Test");
    assert!(
        backend
            .save_project(&local, None, project_input("Test"))
            .await
            .is_err()
    );
    assert_eq!(
        backend
            .save_project(&local, Some(project.id), project_input("Test"))
            .await?
            .id,
        project.id
    );
    assert!(
        backend
            .save_project(&local, Some(Uuid::new_v4()), project_input("Missing"))
            .await
            .is_err()
    );
    assert_eq!(backend.projects().await?.len(), 2);
    assert_eq!(
        backend.visible_projects(member.id, true, None).await?.len(),
        2
    );
    assert!(
        backend
            .visible_projects(member.id, false, None)
            .await?
            .is_empty()
    );
    let bucket = Uuid::new_v4();
    sqlx::query("INSERT INTO buckets(id,name,project_id,storage_id) VALUES(?,'test',?,?)")
        .bind(bucket)
        .bind(project.id)
        .bind(storage)
        .execute(db)
        .await?;
    assert!(backend.delete_project(&local, project.id).await.is_err());
    assert!(
        backend
            .save_member(&local, project.id, admin.id, Some(membership(bucket)))
            .await
            .is_err()
    );
    assert!(
        backend
            .save_member(&local, Uuid::new_v4(), member.id, None)
            .await
            .is_err()
    );
    let verified = backend.login_user("member").await?.unwrap();
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
    let session = backend.authenticate_session(&[1; 32]).await?.unwrap();
    let stale = Principal::User {
        id: member.id,
        session: session.session_id,
        revision: session.authorization_revision,
    };
    assert!(backend.actions(&stale, bucket).await?.is_empty());
    backend
        .save_member(&local, project.id, member.id, Some(membership(bucket)))
        .await?;
    assert!(backend.actions(&stale, bucket).await.is_err());
    let session = backend.authenticate_session(&[1; 32]).await?.unwrap();
    let current = Principal::User {
        id: member.id,
        session: session.session_id,
        revision: session.authorization_revision,
    };
    assert_eq!(
        backend.actions(&current, bucket).await?,
        Action::role("reader")
    );
    assert_eq!(
        backend
            .visible_projects(member.id, false, None)
            .await?
            .len(),
        1
    );
    let rows = backend.members(project.id).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].user_id, member.id);
    assert_eq!(rows[0].grants[0].bucket_id, bucket);
    assert_eq!(rows[0].grants[0].actions, Action::ALL);
    assert!(
        crate::model::audit::scope(
            (-1).into(),
            backend.save_member(&local, project.id, member.id, None)
        )
        .await
        .is_err()
    );
    assert_eq!(
        backend.actions(&current, bucket).await?,
        Action::role("reader")
    );
    assert!(
        backend
            .save_member(
                &local,
                project.id,
                member.id,
                Some(membership(Uuid::new_v4()))
            )
            .await
            .is_err()
    );
    let mut duplicate = membership(bucket);
    duplicate.grants.push(BucketGrant {
        bucket_id: bucket,
        actions: vec![Action::Read],
    });
    assert!(
        backend
            .save_member(&local, project.id, member.id, Some(duplicate))
            .await
            .is_err()
    );
    assert_eq!(
        backend.actions(&current, bucket).await?,
        Action::role("reader")
    );
    let token = Uuid::new_v4();
    sqlx::query("INSERT INTO api_tokens(id,user_id,label,token_hash,prefix,auth_revision) VALUES(?,?,'test',?,'test',0)").bind(token).bind(member.id).bind([4u8;32].as_slice()).execute(db).await?;
    sqlx::query("INSERT INTO token_bucket_grants(token_id,bucket_id,actions) VALUES(?,?,1)")
        .bind(token)
        .bind(bucket)
        .execute(db)
        .await?;
    assert_eq!(
        backend
            .visible_projects(member.id, false, Some(token))
            .await?
            .len(),
        1
    );
    sqlx::query("UPDATE token_bucket_grants SET actions=2 WHERE token_id=?")
        .bind(token)
        .execute(db)
        .await?;
    assert!(
        backend
            .visible_projects(member.id, false, Some(token))
            .await?
            .is_empty()
    );

    backend
        .save_member(&local, project.id, member.id, None)
        .await?;
    assert!(backend.members(project.id).await?.is_empty());
    assert!(backend.actions(&current, bucket).await.is_err());
    backend
        .save_member(
            &local,
            project.id,
            member.id,
            Some(Membership {
                external_grants: Vec::new(),
                role: "writer".into(),
                scope: "all".into(),
                grants: Vec::new(),
            }),
        )
        .await?;
    assert!(backend.members(project.id).await?[0].grants.is_empty());
    let promoted = backend
        .update_user(
            &local,
            member.id,
            UserPatch {
                role: Some("admin".into()),
                enabled: None,
            },
        )
        .await?;
    assert_eq!(promoted.role, "admin");
    assert!(backend.members(project.id).await?.is_empty());
    assert!(backend.authenticate_session(&[1; 32]).await?.is_none());
    assert!(backend.authenticate_token(&[4; 32]).await?.is_none());
    assert!(
        crate::model::audit::scope(
            (-1).into(),
            backend.reset_password(&local, member.id, "next-password", true)
        )
        .await
        .is_err()
    );
    assert_eq!(backend.password(member.id).await?.unwrap().0, "password");
    backend
        .reset_password(&local, member.id, "next-password", true)
        .await?;
    assert_eq!(
        backend.password(member.id).await?.unwrap().0,
        "next-password"
    );
    assert!(
        backend
            .profile(member.id)
            .await?
            .unwrap()
            .must_change_password
    );
    assert!(
        backend
            .reset_password(&local, Uuid::new_v4(), "missing", false)
            .await
            .is_err()
    );
    assert!(
        crate::model::audit::scope((-1).into(), backend.delete_user(&local, member.id))
            .await
            .is_err()
    );
    assert!(backend.user(member.id).await?.is_some());
    backend.delete_user(&local, member.id).await?;
    assert!(backend.user(member.id).await?.is_none());
    sqlx::query("DELETE FROM buckets WHERE id=?")
        .bind(bucket)
        .execute(db)
        .await?;
    backend.delete_project(&local, project.id).await?;
    assert!(backend.delete_project(&local, project.id).await.is_err());
    backend.set_project_mode(&local, false).await?;
    assert!(!backend.project_mode().await?);
    backend.set_project_mode(&local, true).await?;
    assert!(backend.project_mode().await?);
    let (first, second) = tokio::join!(
        backend.delete_user(&local, admin.id),
        backend.delete_user(&local, second_admin.id)
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let error = first.err().or_else(|| second.err()).unwrap();
    assert_eq!(
        crate::model::error(&error),
        Some(crate::model::Error::LastAdministrator)
    );
    assert_eq!(backend.all_users().await?.len(), 1);
    backend.pool.close().await;
    backend.owner.into_inner().close().await?;
    Ok(())
}
