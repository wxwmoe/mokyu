use super::{Backend, connection, queries};
use crate::authorization::{Action, DEFAULT_PROJECT, Permit, Principal};
use crate::config::Config;
use crate::model::Id;
use crate::model::multipart::{
    MultipartCreate, PartStitch, SeedConsumption, TransferFilter, WebUpload,
};
use crate::model::{
    AccountStore, ChunkCreate, ChunkStore, MultipartStore, PublishStore, QuotaStore, StreamCreate,
    StreamStore, StreamTail,
};
use anyhow::Result;
use serde_json::json;
use uuid::Uuid;

fn upload<'a>(authority: &'a Permit, key: &'a str) -> MultipartCreate<'a> {
    MultipartCreate {
        authority,
        bucket: *authority.resources.keys().next().unwrap(),
        key,
        access: "fixture",
        metadata: json!({}),
        public: false,
        algorithm: Some("SHA256".into()),
        checksum_type: Some("COMPOSITE".into()),
        web: None,
        max_active: None,
    }
}
async fn part(backend: &Backend, authority: &Permit, key: &str) -> Result<Id> {
    StreamStore::create(
        backend,
        StreamCreate {
            authority: authority.clone(),
            bucket: *authority.resources.keys().next().unwrap(),
            key: key.into(),
            stream_type: "part".into(),
            metadata: json!({}),
            public: false,
            claim_object: false,
            completion: None,
            owner: None,
        },
    )
    .await
}
async fn usage(backend: &Backend, bucket: Uuid, reserved: i64, inflight: i64) -> Result<()> {
    let q = backend.read(&Principal::Local, "bucket", bucket).await?;
    assert_eq!(
        (q.reserved_bytes, q.inflight_bytes),
        (Some(reserved.to_string()), Some(inflight.to_string()))
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_multipart_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    let storage = super::storage_tests::fixture(&backend).await?;
    let bucket = Uuid::new_v4();
    sqlx::query("INSERT INTO buckets(id,name,project_id,storage_id) VALUES(?,'multipart',?,?)")
        .bind(bucket)
        .bind(DEFAULT_PROJECT)
        .bind(storage)
        .execute(db)
        .await?;
    queries::insert_quota_scope("bucket", bucket)
        .execute(db)
        .await?;
    let authority = Permit::for_action(Principal::Local, bucket, Action::Write);
    let user = backend.initialize_admin("admin", "password", false).await?;
    let client = Uuid::new_v4();
    let web = || {
        let mut input = upload(&authority, "web");
        input.web = Some(WebUpload {
            user,
            client_id: client,
            request_hash: vec![1; 32],
            file_name: "web.bin".into(),
            size: 32,
            part_size: 5242880,
            modified_at: Some(1),
            overwrite: false,
        });
        input
    };
    let (first, replay) = tokio::join!(
        MultipartStore::create(&backend, web()),
        MultipartStore::create(&backend, web())
    );
    let (first, replay) = (first?, replay?);
    assert_eq!(first.0, replay.0);
    assert_ne!(first.1, replay.1);
    let mut wrong = web();
    wrong.web.as_mut().unwrap().request_hash[0] = 2;
    assert!(MultipartStore::create(&backend, wrong).await.is_err());
    sqlx::query("UPDATE buckets SET uploads_paused=TRUE WHERE id=?")
        .bind(bucket)
        .execute(db)
        .await?;
    assert!(MultipartStore::create(&backend, web()).await?.1);
    assert!(
        MultipartStore::create(&backend, upload(&authority, "paused"))
            .await
            .is_err()
    );
    sqlx::query("UPDATE buckets SET uploads_paused=FALSE WHERE id=?")
        .bind(bucket)
        .execute(db)
        .await?;
    let limited = |key| {
        let mut input = upload(&authority, key);
        input.max_active = Some(2);
        input
    };
    let (a, b) = tokio::join!(
        MultipartStore::create(&backend, limited("a")),
        MultipartStore::create(&backend, limited("b"))
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let admitted = a.ok().or_else(|| b.ok()).unwrap().0;
    backend.abort(&authority, admitted).await?;
    let key = "literal%_\u{732b} ";
    let id = MultipartStore::create(&backend, upload(&authority, key))
        .await?
        .0;
    assert!(backend.upload(id, bucket, key, "fixture").await.is_ok());
    assert!(backend.upload(id, bucket, key, "Fixture").await.is_err());
    assert!(
        backend
            .upload(id, bucket, key.trim_end(), "fixture")
            .await
            .is_err()
    );
    let listing = backend
        .uploads(bucket, "fixture", "literal%_", "", false, None)
        .await?;
    assert_eq!(listing.len(), 1);
    assert_eq!(listing[0].id, id);
    assert!(
        backend
            .uploads(bucket, "fixture", "literal%_", key, false, Some(id))
            .await?
            .is_empty()
    );
    backend.touch(id).await?;
    assert_eq!(backend.bucket_name(bucket).await?, "multipart");

    let original = part(&backend, &authority, key).await?;
    assert!(
        backend
            .begin_part(&authority, id, 10001, original)
            .await
            .is_err()
    );
    backend.begin_part(&authority, id, 1, original).await?;
    backend.reserve(original, 32, false, Some((id, 1))).await?;
    let (mut chunk, _) = backend
        .allocate(ChunkCreate {
            storage,
            stream: original,
            hash: [1; 32],
            raw_size: 32,
            algorithm: "none".into(),
            key: None,
        })
        .await?;
    chunk.stored_size = Some(32);
    chunk.location_id = Uuid::new_v4();
    backend.start_upload(original, &chunk).await?;
    ChunkStore::complete(&backend, original, 0, &chunk).await?;
    backend
        .finish(original, 32, "part", json!({"sha256":"checksum"}))
        .await?;
    assert!(
        crate::model::audit::scope(
            (-1).into(),
            backend.commit_part(&authority, bucket, id, 1, original)
        )
        .await
        .is_err()
    );
    assert!(backend.part(id, 1).await?.is_none());
    usage(&backend, bucket, 32, 32).await?;
    backend
        .commit_part(&authority, bucket, id, 1, original)
        .await?;
    backend
        .commit_part(&authority, bucket, id, 1, original)
        .await?;
    assert_eq!(
        backend.web_parts(id, 0).await?[0].sha256.as_deref(),
        Some("checksum")
    );
    assert_eq!(backend.parts(id, 0, 10).await?[0].1, 32);
    assert_eq!(backend.pair(id, 1).await?.unwrap().1.len(), 1);
    let losing = part(&backend, &authority, key).await?;
    backend.begin_part(&authority, id, 1, losing).await?;
    backend.reserve(losing, 64, false, Some((id, 1))).await?;
    usage(&backend, bucket, 64, 96).await?;
    let winning = part(&backend, &authority, key).await?;
    backend.begin_part(&authority, id, 1, winning).await?;
    usage(&backend, bucket, 32, 96).await?;
    backend.reserve(winning, 16, false, Some((id, 1))).await?;
    backend.reference(winning, 0, chunk.id, 16).await?;
    backend
        .finish(winning, 16, "replacement", json!({}))
        .await?;
    assert!(
        backend
            .commit_part(&authority, bucket, id, 1, losing)
            .await
            .is_err()
    );
    backend
        .commit_part(&authority, bucket, id, 1, winning)
        .await?;
    usage(&backend, bucket, 16, 80).await?;
    backend.retire_work(losing).await?;
    usage(&backend, bucket, 16, 16).await?;
    assert_eq!(backend.part(id, 1).await?.unwrap().id, winning);
    let pending = part(&backend, &authority, key).await?;
    backend.begin_part(&authority, id, 2, pending).await?;
    backend.reserve(pending, 8, false, Some((id, 2))).await?;
    backend.abort(&authority, id).await?;
    usage(&backend, bucket, 0, 8).await?;
    assert!(!backend.active(id).await?);
    assert!(backend.upload(id, bucket, key, "fixture").await.is_err());
    assert!(backend.abort(&authority, id).await.is_err());
    assert!(backend.touch(id).await.is_err());
    backend.retire_work(pending).await?;
    usage(&backend, bucket, 0, 0).await?;

    let completed = MultipartStore::create(&backend, upload(&authority, "completed"))
        .await?
        .0;
    let data = part(&backend, &authority, "completed").await?;
    backend.begin_part(&authority, completed, 1, data).await?;
    backend
        .reserve(data, 32, false, Some((completed, 1)))
        .await?;
    backend.reference(data, 0, chunk.id, 32).await?;
    backend.finish(data, 32, "data", json!({})).await?;
    backend
        .commit_part(&authority, bucket, completed, 1, data)
        .await?;
    let completion_token = Uuid::new_v4();
    backend
        .begin_complete(crate::model::multipart::Completion {
            authority: &authority,
            upload: completed,
            hash: "manifest",
            token: completion_token,
            parts: &[(1, data)],
            node: None,
        })
        .await?;
    assert!(!backend.active(completed).await?);
    let competing = Uuid::new_v4();
    assert!(
        !backend
            .begin_complete(crate::model::multipart::Completion {
                authority: &authority,
                upload: completed,
                hash: "manifest",
                token: competing,
                parts: &[(1, data)],
                node: None
            })
            .await?
    );
    assert!(
        backend
            .begin_complete(crate::model::multipart::Completion {
                authority: &authority,
                upload: completed,
                hash: "changed",
                token: competing,
                parts: &[(1, data)],
                node: None
            })
            .await
            .is_err()
    );
    assert!(!backend.renew_complete(completed, competing, None).await?);
    backend.retry_complete(completed, competing).await?;
    assert!(!backend.active(completed).await?);
    assert!(
        backend
            .renew_complete(completed, completion_token, None)
            .await?
    );
    assert!(backend.pair(completed, 1).await?.is_none());
    backend.retry_complete(completed, completion_token).await?;
    assert!(backend.active(completed).await?);
    assert!(
        backend
            .begin_complete(crate::model::multipart::Completion {
                authority: &authority,
                upload: completed,
                hash: "manifest",
                token: competing,
                parts: &[(2, data)],
                node: None
            })
            .await
            .is_err()
    );
    let completion_token = Uuid::new_v4();
    backend
        .begin_complete(crate::model::multipart::Completion {
            authority: &authority,
            upload: completed,
            hash: "manifest",
            token: completion_token,
            parts: &[(1, data)],
            node: None,
        })
        .await?;
    let output = StreamStore::create(
        &backend,
        StreamCreate {
            authority: authority.clone(),
            bucket,
            key: "completed".into(),
            stream_type: "object".into(),
            metadata: json!({}),
            public: false,
            claim_object: true,
            completion: Some(completed),
            owner: None,
        },
    )
    .await?;
    QuotaStore::complete(&backend, completed, output, 32).await?;
    backend
        .copy_references(output, data, 0, 32, &[chunk.id])
        .await?;
    backend.finish(output, 32, "complete", json!({})).await?;
    sqlx::query("UPDATE uploads SET completion_expires_at='2000-01-01 00:00:00' WHERE id=?")
        .bind(completed)
        .execute(&backend.pool)
        .await?;
    assert!(
        !backend
            .renew_complete(completed, completion_token, None)
            .await?
    );
    assert!(
        !backend
            .publish(
                output,
                None,
                None,
                Some((completed, "manifest".into(), json!({}), completion_token))
            )
            .await?
    );
    let expired_token = completion_token;
    let completion_token = Uuid::new_v4();
    assert!(
        backend
            .begin_complete(crate::model::multipart::Completion {
                authority: &authority,
                upload: completed,
                hash: "manifest",
                token: completion_token,
                parts: &[(1, data)],
                node: None
            })
            .await?
    );
    backend.retry_complete(completed, expired_token).await?;
    assert!(!backend.active(completed).await?);
    assert!(
        !backend
            .publish(
                output,
                None,
                None,
                Some((completed, "manifest".into(), json!({}), expired_token))
            )
            .await?
    );
    assert!(
        backend
            .publish(
                output,
                None,
                None,
                Some((
                    completed,
                    "manifest".into(),
                    json!({"etag":"complete"}),
                    completion_token
                ))
            )
            .await?
    );
    usage(&backend, bucket, 0, 0).await?;
    let (rows, more) = backend
        .transfers(
            &Principal::Local,
            user,
            &TransferFilter {
                state: Some("all".into()),
                ..Default::default()
            },
            None,
            Some(completed),
            10,
        )
        .await?;
    assert!(!more);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].received_bytes, "32");
    assert_eq!(rows[0].remote_state, "stored");
    let durable: bool = sqlx::query_scalar("SELECT durable_at IS NOT NULL FROM uploads WHERE id=?")
        .bind(completed)
        .fetch_one(db)
        .await?;
    assert!(durable);
    let (page, more) = backend
        .transfers(
            &Principal::Local,
            user,
            &TransferFilter {
                state: Some("all".into()),
                ..Default::default()
            },
            None,
            None,
            1,
        )
        .await?;
    assert!(more && page.len() == 1);
    let (next, _) = backend
        .transfers(
            &Principal::Local,
            user,
            &TransferFilter {
                state: Some("all".into()),
                ..Default::default()
            },
            Some((page[0].created_at, page[0].id)),
            None,
            20,
        )
        .await?;
    assert!(next.iter().all(|row| row.id != page[0].id));

    let member = Uuid::new_v4();
    sqlx::query("INSERT INTO web_users(id,username,password_hash) VALUES(?,'member','password')")
        .bind(member)
        .execute(db)
        .await?;
    sqlx::query(
        "INSERT INTO project_members(user_id,project_id,role,scope) VALUES(?,?,'writer','all')",
    )
    .bind(member)
    .bind(DEFAULT_PROJECT)
    .execute(db)
    .await?;
    let verified = backend.login_user("member").await?.unwrap();
    backend
        .login(crate::model::account::SessionLogin {
            verified: &verified,
            access_hash: &[9; 32],
            csrf_hash: &[9; 32],
            refresh_hash: &[9; 32],
            seconds: 3600,
            user_agent: "test",
            request_id: "test",
        })
        .await?;
    let session = backend.authenticate_session(&[9; 32]).await?.unwrap();
    let principal = Principal::User {
        id: member,
        session: session.session_id,
        revision: session.authorization_revision,
    };
    let mut own = web();
    own.web.as_mut().unwrap().user = member;
    let owned = MultipartStore::create(&backend, own).await?.0;
    let all = TransferFilter {
        state: Some("all".into()),
        ..Default::default()
    };
    let visible = backend
        .transfers(&principal, member, &all, None, None, 20)
        .await?
        .0;
    assert_eq!(
        visible.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![owned]
    );
    assert!(
        backend
            .transfers(&principal, member, &all, None, Some(completed), 20)
            .await?
            .0
            .is_empty()
    );
    sqlx::query("UPDATE project_members SET role='maintainer' WHERE user_id=?")
        .bind(member)
        .execute(db)
        .await?;
    assert_eq!(
        backend
            .transfers(&principal, member, &all, None, Some(completed), 20)
            .await?
            .0
            .len(),
        1
    );
    let token = Uuid::new_v4();
    sqlx::query("INSERT INTO api_tokens(id,user_id,label,token_hash,prefix,system_access,auth_revision) VALUES(?,?,'test',?,'test',FALSE,0)")
        .bind(token).bind(member).bind([8u8;32].as_slice()).execute(db).await?;
    sqlx::query("INSERT INTO token_bucket_grants(token_id,bucket_id,actions) VALUES(?,?,4)")
        .bind(token)
        .bind(bucket)
        .execute(db)
        .await?;
    let token_principal = Principal::Token {
        id: token,
        user: member,
        revision: 0,
        user_revision: session.authorization_revision,
    };
    assert!(
        backend
            .transfers(&token_principal, member, &all, None, Some(completed), 20)
            .await?
            .0
            .is_empty()
    );
    assert_eq!(
        backend
            .transfers(&token_principal, member, &all, None, Some(owned), 20)
            .await?
            .0
            .len(),
        1
    );
    sqlx::query("UPDATE token_bucket_grants SET actions=36 WHERE token_id=?")
        .bind(token)
        .execute(db)
        .await?;
    assert_eq!(
        backend
            .transfers(&token_principal, member, &all, None, Some(completed), 20)
            .await?
            .0
            .len(),
        1
    );
    sqlx::query("UPDATE project_members SET role='reader' WHERE user_id=?")
        .bind(member)
        .execute(db)
        .await?;
    assert!(
        backend
            .transfers(&token_principal, member, &all, None, None, 20)
            .await?
            .0
            .is_empty()
    );

    let seed = MultipartStore::create(&backend, upload(&authority, "seed"))
        .await?
        .0;
    let left = part(&backend, &authority, "seed").await?;
    let right = part(&backend, &authority, "seed").await?;
    for stream in [left, right] {
        backend
            .append_tail(StreamTail {
                fragment: Uuid::new_v4(),
                stream,
                offset: 0,
                size: 8,
                hash: [2; 32],
                staging_storage: None,
                staging_key: None,
                file: None,
                file_offset: None,
            })
            .await?;
    }
    backend
        .stitch(PartStitch {
            left,
            offset: 4,
            left_length: 4,
            right,
            right_length: 5,
            chunk: chunk.id,
        })
        .await?;
    let l = backend.extents(left, 0, 8).await?;
    let r = backend.extents(right, 0, 8).await?;
    assert_eq!(
        (l[1].offset_bytes, l[1].length, l[1].source_offset),
        (4, 4, 0)
    );
    assert_eq!(
        (r[0].length, r[0].source_offset, r[1].source_offset),
        (5, 4, 5)
    );
    assert!(
        backend
            .stitch(PartStitch {
                left,
                offset: 0,
                left_length: 8,
                right,
                right_length: 9,
                chunk: chunk.id
            })
            .await
            .is_err()
    );
    assert_eq!(
        serde_json::to_value(backend.extents(left, 0, 8).await?)?,
        serde_json::to_value(l)?
    );
    let temporary = part(&backend, &authority, "seed").await?;
    backend.reference(temporary, 0, chunk.id, 8).await?;
    let mut spans = vec![(left, 0, 4)];
    backend
        .consume_seed(SeedConsumption {
            upload: seed,
            stream: temporary,
            offset: 0,
            chunk: chunk.id,
            prefix: 4,
            consumed: 8,
            spans: &mut spans,
        })
        .await?;
    assert!(spans.is_empty());
    let tail = backend.tail(temporary).await?.unwrap();
    assert_eq!((tail.length, tail.source_offset), (4, 4));
    assert_eq!(
        backend.extents(left, 0, 8).await?[0].chunk_id,
        Some(chunk.id)
    );
    backend.check_owner().await?;
    backend.pool.close().await;
    Ok(())
}
