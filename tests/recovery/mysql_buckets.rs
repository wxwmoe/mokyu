use super::{Backend, connection};
use crate::app::Bucket;
use crate::authorization::{Action, DEFAULT_PROJECT, Permit, Principal};
use crate::config::Config;
use crate::model::PhysicalStore;
use crate::model::administration::ProjectInput;
use crate::model::bucket::{BucketUpdate, CreateBucket, TransferInput, Website, fingerprint};
use crate::model::quota::Limits;
use crate::model::{
    AccountStore, AdministrationStore, BucketStore, ChunkStore, MultipartStore, PublishStore,
    QuotaStore, StreamStore,
};
use anyhow::Result;
use serde_json::json;
use uuid::Uuid;

fn create(name: &str) -> CreateBucket {
    CreateBucket {
        name: name.into(),
        project_id: None,
        storage_id: None,
    }
}
fn settings(bucket: &Bucket, paused: bool, domains: Option<Vec<String>>) -> BucketUpdate {
    BucketUpdate {
        revision: bucket.settings_revision,
        cors: bucket.cors.clone(),
        website_enabled: bucket.website_enabled,
        index_document: bucket.index_document.clone(),
        error_document: bucket.error_document.clone(),
        public_base_url: bucket.public_base_url.clone(),
        uploads_paused: paused,
        domains,
    }
}
async fn count(backend: &Backend, project: Uuid, expected: &str) -> Result<()> {
    assert_eq!(
        backend
            .read(&Principal::Local, "project", project)
            .await?
            .bucket_count
            .as_deref(),
        Some(expected)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_bucket_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    let storage = super::storage_tests::fixture(&backend).await?;
    let admin = backend.initialize_admin("admin", "password", false).await?;
    assert!(
        BucketStore::create(&backend, &Principal::Local, create("InvalidName"))
            .await
            .is_err()
    );
    backend
        .save(
            &Principal::Local,
            "project",
            DEFAULT_PROJECT,
            Limits {
                logical_bytes_limit: None,
                inflight_bytes_limit: None,
                bucket_count_limit: Some("1".into()),
            },
        )
        .await?;
    let (a, b) = tokio::join!(
        BucketStore::create(&backend, &Principal::Local, create("first")),
        BucketStore::create(&backend, &Principal::Local, create("second"))
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let bucket = a.ok().or_else(|| b.ok()).unwrap();
    count(&backend, DEFAULT_PROJECT, "1").await?;
    assert!(
        BucketStore::create(&backend, &Principal::Local, create(&bucket.name))
            .await
            .is_err()
    );
    backend
        .save(
            &Principal::Local,
            "project",
            DEFAULT_PROJECT,
            Limits {
                logical_bytes_limit: None,
                inflight_bytes_limit: None,
                bucket_count_limit: Some("10".into()),
            },
        )
        .await?;
    assert!(
        crate::model::audit::scope(
            (-1).into(),
            BucketStore::create(&backend, &Principal::Local, create("rollback"))
        )
        .await
        .is_err()
    );
    assert!(backend.by_name("rollback", false).await?.is_none());
    count(&backend, DEFAULT_PROJECT, "1").await?;
    let spare = BucketStore::create(&backend, &Principal::Local, create("spare")).await?;
    assert_eq!(
        backend.by_name(&bucket.name, true).await?.unwrap().id,
        bucket.id
    );
    assert!(
        backend
            .creation_options(true, admin, true)
            .await?
            .1
            .iter()
            .any(|s| s.id == storage)
    );
    assert_eq!(backend.visible(admin, true, None).await?.len(), 2);
    backend
        .change_domain(&Principal::Local, "b.example", Some(bucket.id))
        .await?;
    backend
        .change_domain(&Principal::Local, "a.example", Some(bucket.id))
        .await?;
    assert!(
        backend
            .change_domain(&Principal::Local, "a.example", Some(spare.id))
            .await
            .is_err()
    );
    let (row, domains) = backend.settings(bucket.id).await?;
    assert_eq!(domains, vec!["a.example", "b.example"]);
    assert_eq!(row.settings_revision, 2);
    assert_eq!(backend.by_host("a.example").await?.unwrap().id, bucket.id);
    assert_eq!(backend.domain_names().await?.len(), 2);
    assert_eq!(
        backend
            .domains(&Principal::Local, Some(bucket.id))
            .await?
            .len(),
        2
    );
    assert!(
        backend
            .save_settings(&Principal::Local, bucket.id, settings(&bucket, false, None))
            .await
            .is_err()
    );
    backend
        .save_settings(
            &Principal::Local,
            bucket.id,
            settings(&row, false, Some(vec!["c.example".into()])),
        )
        .await?;
    assert!(backend.by_host("a.example").await?.is_none());
    backend
        .set_cors(&Principal::Local, bucket.id, json!([{"id":"test"}]))
        .await?;
    assert_eq!(backend.cors(bucket.id).await?, json!([{"id":"test"}]));
    backend
        .set_website(
            &Principal::Local,
            bucket.id,
            Website {
                website_enabled: true,
                index_document: "home.html".into(),
                error_document: "missing.html".into(),
            },
        )
        .await?;
    assert_eq!(
        backend.website(bucket.id).await?.index_document,
        "home.html"
    );
    let before = backend.settings(bucket.id).await?.0;
    assert!(
        crate::model::audit::scope(
            (-1).into(),
            backend.save_settings(&Principal::Local, bucket.id, settings(&before, true, None))
        )
        .await
        .is_err()
    );
    assert_eq!(
        backend.settings(bucket.id).await?.0.settings_revision,
        before.settings_revision
    );

    let member = Uuid::new_v4();
    sqlx::query("INSERT INTO web_users(id,username,password_hash) VALUES(?,'member','password')")
        .bind(member)
        .execute(db)
        .await?;
    sqlx::query("INSERT INTO project_members(user_id,project_id,role,scope) VALUES(?,?,'maintainer','selected')").bind(member).bind(DEFAULT_PROJECT).execute(db).await?;
    sqlx::query("UPDATE projects SET allow_bucket_create=TRUE WHERE id=?")
        .bind(DEFAULT_PROJECT)
        .execute(db)
        .await?;
    let verified = backend.login_user("member").await?.unwrap();
    backend
        .login(crate::model::account::SessionLogin {
            verified: &verified,
            access_hash: &[4; 32],
            csrf_hash: &[4; 32],
            refresh_hash: &[4; 32],
            seconds: 3600,
            user_agent: "test",
            request_id: "test",
        })
        .await?;
    let session = backend.authenticate_session(&[4; 32]).await?.unwrap();
    let mut principal = Principal::User {
        id: member,
        session: session.session_id,
        revision: session.authorization_revision,
    };
    assert_eq!(
        backend.creation_options(false, member, true).await?.0.len(),
        1
    );
    let owned = BucketStore::create(&backend, &principal, create("owned")).await?;
    assert!(
        backend
            .set_cors(&principal, owned.id, json!([]))
            .await
            .is_err()
    );
    principal = Principal::User {
        id: member,
        session: session.session_id,
        revision: session.authorization_revision + 1,
    };
    backend.set_cors(&principal, owned.id, json!([])).await?;
    let row = backend.settings(owned.id).await?.0;
    assert!(
        backend
            .save_settings(&principal, owned.id, settings(&row, true, None))
            .await
            .is_err()
    );
    assert!(
        backend
            .set_cors(&principal, bucket.id, json!([]))
            .await
            .is_err()
    );
    assert_eq!(backend.visible(member, false, None).await?.len(), 1);

    let target = backend
        .save_project(
            &Principal::Local,
            None,
            ProjectInput {
                name: "Target".into(),
                description: "".into(),
                allow_bucket_create: false,
            },
        )
        .await?
        .id;
    let authority = Permit::for_action(Principal::Local, bucket.id, Action::Write);
    let key = "literal%_\\\u{732b} ";
    let stream = StreamStore::create(
        &backend,
        crate::model::StreamCreate {
            authority: authority.clone(),
            bucket: bucket.id,
            key: key.into(),
            stream_type: "object".into(),
            metadata: json!({}),
            public: false,
            claim_object: true,
            completion: None,
            owner: None,
        },
    )
    .await?;
    backend.reserve(stream, 32, false, None).await?;
    let (mut chunk, _) = backend
        .allocate(crate::model::ChunkCreate {
            storage,
            stream,
            hash: [5; 32],
            raw_size: 32,
            algorithm: "none".into(),
            key: None,
        })
        .await?;
    chunk.stored_size = Some(32);
    backend.mark_uploading(&chunk).await?;
    backend.register_encoding(&chunk).await?;
    ChunkStore::complete(&backend, stream, 0, &chunk).await?;
    backend.finish(stream, 32, "etag", json!({})).await?;
    assert!(backend.publish(stream, None, None, None).await?);
    assert_eq!(
        backend.list_objects(bucket.id, key, "", true).await?[0].id,
        stream
    );
    assert!(
        backend
            .list_objects(bucket.id, key, key, false)
            .await?
            .is_empty()
    );
    assert!(
        BucketStore::delete(&backend, &Principal::Local, bucket.id, &bucket.name, None)
            .await
            .is_err()
    );
    let row = backend.settings(bucket.id).await?.0;
    backend
        .save_settings(&Principal::Local, bucket.id, settings(&row, true, None))
        .await?;
    sqlx::query(
        "INSERT INTO member_bucket_grants(user_id,project_id,bucket_id,actions) VALUES(?,?,?,127)",
    )
    .bind(member)
    .bind(DEFAULT_PROJECT)
    .bind(bucket.id)
    .execute(db)
    .await?;
    sqlx::query("INSERT INTO s3_credentials(access_key_id,secret_encrypted,project_id) VALUES('transfer',?,?)")
        .bind([1u8;32].as_slice()).bind(DEFAULT_PROJECT).execute(db).await?;
    sqlx::query(
        "INSERT INTO s3_credential_grants(access_key,bucket_id,actions) VALUES('transfer',?,127)",
    )
    .bind(bucket.id)
    .execute(db)
    .await?;
    let transfer_token = Uuid::new_v4();
    sqlx::query("INSERT INTO api_tokens(id,user_id,label,token_hash,prefix,system_access,auth_revision) VALUES(?,?,'transfer',?,'transfer',FALSE,0)")
        .bind(transfer_token).bind(member).bind([7u8;32].as_slice()).execute(db).await?;
    sqlx::query("INSERT INTO token_bucket_grants(token_id,bucket_id,actions) VALUES(?,?,127)")
        .bind(transfer_token)
        .bind(bucket.id)
        .execute(db)
        .await?;
    let (impact, preview) = backend
        .transfer_preview(&Principal::Local, bucket.id, target)
        .await?;
    assert_eq!(impact.logical_bytes, "32");
    assert_eq!(preview.bucket_count, "0");
    let transfer = || TransferInput {
        target_project: target,
        confirm_name: bucket.name.clone(),
        confirmation: fingerprint(&impact, Some(target)).unwrap(),
    };
    backend
        .save(
            &Principal::Local,
            "project",
            target,
            Limits {
                logical_bytes_limit: Some("31".into()),
                inflight_bytes_limit: None,
                bucket_count_limit: None,
            },
        )
        .await?;
    assert!(
        backend
            .transfer(&Principal::Local, bucket.id, transfer())
            .await
            .is_err()
    );
    assert_eq!(
        backend
            .by_name(&bucket.name, false)
            .await?
            .unwrap()
            .project_id,
        DEFAULT_PROJECT
    );
    backend
        .save(
            &Principal::Local,
            "project",
            target,
            Limits {
                logical_bytes_limit: Some("32".into()),
                inflight_bytes_limit: None,
                bucket_count_limit: None,
            },
        )
        .await?;
    assert!(
        crate::model::audit::scope(
            (-1).into(),
            backend.transfer(&Principal::Local, bucket.id, transfer())
        )
        .await
        .is_err()
    );
    count(&backend, target, "0").await?;
    let moved = backend
        .transfer(&Principal::Local, bucket.id, transfer())
        .await?;
    assert_eq!(moved.project_id, target);
    assert!(!moved.uploads_paused);
    let access: (i64,i64,i64)=sqlx::query_as("SELECT (SELECT authorization_revision FROM web_users WHERE id=?),(SELECT auth_revision FROM api_tokens WHERE id=?),(SELECT authorization_revision FROM s3_credentials WHERE access_key_id='transfer')")
        .bind(member).bind(transfer_token).fetch_one(db).await?;
    assert_eq!(access, (2, 1, 1));
    let impact = backend.impact(&Principal::Local, bucket.id).await?;
    assert_eq!(
        (
            impact.member_bucket_grants.as_str(),
            impact.service_grants.as_str(),
            impact.token_bucket_grants.as_str()
        ),
        ("0", "0", "0")
    );
    assert_eq!(
        backend
            .read(&Principal::Local, "project", DEFAULT_PROJECT)
            .await?
            .used_bytes
            .as_deref(),
        Some("0")
    );
    assert_eq!(
        backend
            .read(&Principal::Local, "project", target)
            .await?
            .used_bytes
            .as_deref(),
        Some("32")
    );
    count(&backend, DEFAULT_PROJECT, "2").await?;
    count(&backend, target, "1").await?;
    assert!(
        backend
            .transfer(&Principal::Local, bucket.id, transfer())
            .await
            .is_err()
    );

    let token = Uuid::new_v4();
    sqlx::query("INSERT INTO api_tokens(id,user_id,label,token_hash,prefix,system_access,auth_revision) VALUES(?,?,'test',?,'test',FALSE,0)").bind(token).bind(member).bind([8u8;32].as_slice()).execute(db).await?;
    sqlx::query("INSERT INTO token_bucket_grants(token_id,bucket_id,actions) VALUES(?,?,127)")
        .bind(token)
        .bind(owned.id)
        .execute(db)
        .await?;
    assert_eq!(backend.visible(member, false, Some(token)).await?.len(), 1);
    assert!(
        BucketStore::delete(&backend, &Principal::Local, owned.id, "wrong", None)
            .await
            .is_err()
    );
    let impact = backend.impact(&Principal::Local, owned.id).await?;
    BucketStore::delete(
        &backend,
        &Principal::Local,
        owned.id,
        &owned.name,
        Some(&fingerprint(&impact, None)?),
    )
    .await?;
    let revision: i64 = sqlx::query_scalar("SELECT auth_revision FROM api_tokens WHERE id=?")
        .bind(token)
        .fetch_one(db)
        .await?;
    assert_eq!(revision, 1);
    assert!(
        backend
            .visible(member, false, Some(token))
            .await?
            .is_empty()
    );
    count(&backend, DEFAULT_PROJECT, "1").await?;

    let upload = MultipartStore::create(
        &backend,
        crate::model::multipart::MultipartCreate {
            authority: &authority,
            bucket: bucket.id,
            key: "pending",
            access: "fixture",
            metadata: json!({}),
            public: false,
            algorithm: None,
            checksum_type: None,
            web: None,
            max_active: None,
        },
    )
    .await?
    .0;
    assert_eq!(backend.purge_counts(bucket.id).await?, (1, 1));
    assert_eq!(backend.purge_uploads(bucket.id, 5).await?.1, vec![upload]);
    backend
        .save(
            &Principal::Local,
            "project",
            target,
            Limits {
                logical_bytes_limit: None,
                inflight_bytes_limit: None,
                bucket_count_limit: None,
            },
        )
        .await?;
    let pending = StreamStore::create(
        &backend,
        crate::model::StreamCreate {
            authority: authority.clone(),
            bucket: bucket.id,
            key: key.into(),
            stream_type: "object".into(),
            metadata: json!({}),
            public: false,
            claim_object: true,
            completion: None,
            owner: None,
        },
    )
    .await?;
    backend.reserve(pending, 64, false, None).await?;
    let result = backend
        .start_purge(&Principal::Local, &bucket.name, bucket.id, None)
        .await?;
    let task = Uuid::parse_str(result["task_id"].as_str().unwrap())?;
    assert_eq!(
        backend
            .start_purge(&Principal::Local, &bucket.name, bucket.id, None)
            .await?["task_id"],
        result["task_id"]
    );
    assert!(backend.by_name(&bucket.name, true).await?.is_none());
    backend.abort(&authority, upload).await?;
    let execution = crate::model::task::Execution {
        id: task,
        token: Uuid::new_v4(),
    };
    assert!(
        backend
            .purge_batch(execution, bucket.id, None, 10, &[])
            .await
            .is_err()
    );
    sqlx::query("UPDATE tasks SET state='running',execution_token=? WHERE id=?")
        .bind(execution.token)
        .bind(task)
        .execute(db)
        .await?;
    assert!(
        !backend
            .purge_batch(execution, bucket.id, None, 10, &[])
            .await?
    );
    assert_eq!(
        backend
            .read(&Principal::Local, "project", target)
            .await?
            .used_bytes
            .as_deref(),
        Some("0")
    );
    assert!(
        !backend
            .purge_batch(execution, bucket.id, Some(key), 10, &[upload])
            .await?
    );
    sqlx::query("DELETE FROM extents WHERE stream_id=?")
        .bind(stream)
        .execute(db)
        .await?;
    let quota = backend.read(&Principal::Local, "bucket", bucket.id).await?;
    assert_eq!(
        (
            quota.reserved_bytes.as_deref(),
            quota.inflight_bytes.as_deref()
        ),
        (Some("0"), Some("64"))
    );
    backend.retire_work(pending).await?;
    sqlx::query("DELETE FROM streams WHERE id=?")
        .bind(pending)
        .execute(db)
        .await?;
    sqlx::query("DELETE FROM streams WHERE id=?")
        .bind(stream)
        .execute(db)
        .await?;
    assert!(
        backend
            .purge_batch(execution, bucket.id, Some(key), 10, &[])
            .await?
    );
    assert!(backend.by_name(&bucket.name, false).await?.is_none());
    assert!(backend.by_host("c.example").await?.is_none());
    count(&backend, target, "0").await?;
    let state: String = sqlx::query_scalar("SELECT state FROM tasks WHERE id=?")
        .bind(task)
        .fetch_one(db)
        .await?;
    assert_eq!(state, "completed");
    BucketStore::delete(&backend, &Principal::Local, spare.id, &spare.name, None).await?;
    count(&backend, DEFAULT_PROJECT, "0").await?;
    backend.check_owner().await?;
    backend.pool.close().await;
    Ok(())
}
