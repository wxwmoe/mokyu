use super::{Backend, connection};
use crate::authorization::{Action, DEFAULT_PROJECT, Permit, Principal};
use crate::config::Config;
use crate::model::Id;
use crate::model::bucket::CreateBucket;
use crate::model::object::{
    CatalogCursor, CatalogFilter, MediaAction, ObjectChange, ObjectTransfer, Outcome, RECEIPT,
    Receipt, StorageMigration,
};
use crate::model::quota::Limits;
use crate::model::{
    AccountStore, BucketStore, ChunkStore, ObjectStore, PublishStore, QuotaStore, StreamStore,
};
use anyhow::Result;
use serde_json::json;
use uuid::Uuid;

async fn stream(
    backend: &Backend,
    bucket: Uuid,
    key: &str,
    size: i64,
    chunk: Id,
    claim: bool,
    mime: &str,
) -> Result<Id> {
    let id = StreamStore::create(
        backend,
        crate::model::StreamCreate {
            authority: Permit::for_action(Principal::Local, bucket, Action::Write),
            bucket,
            key: key.into(),
            stream_type: "object".into(),
            metadata: json!({"content_type":mime}),
            public: false,
            claim_object: claim,
            completion: None,
            owner: None,
        },
    )
    .await?;
    if claim {
        backend.reserve(id, size, false, None).await?;
    }
    if size > 0 {
        backend.reference(id, 0, chunk, size as i32).await?;
    }
    backend.finish(id, size, "etag", json!({})).await?;
    if claim {
        assert!(backend.publish(id, None, None, None).await?);
    }
    Ok(id)
}
async fn catalog(
    backend: &Backend,
    bucket: Uuid,
    filter: &mut CatalogFilter,
) -> Result<Vec<crate::model::object::MediaItem>> {
    backend
        .browse(
            &Permit::for_action(Principal::Local, bucket, Action::List),
            bucket,
            filter,
            None,
            (None, None),
        )
        .await
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_SQLITE_CONFIG pointing to a fresh isolated SQLite database"]
async fn sqlite_object_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.write;
    let storage = super::storage_tests::fixture(&backend).await?;
    let admin = backend.initialize_admin("admin", "password", false).await?;
    let a = BucketStore::create(
        &backend,
        &Principal::Local,
        CreateBucket {
            name: "source".into(),
            project_id: None,
            storage_id: Some(storage),
        },
    )
    .await?
    .id;
    let b = BucketStore::create(
        &backend,
        &Principal::Local,
        CreateBucket {
            name: "target".into(),
            project_id: None,
            storage_id: Some(storage),
        },
    )
    .await?
    .id;
    let bootstrap = StreamStore::create(
        &backend,
        crate::model::StreamCreate {
            authority: Permit::for_action(Principal::Local, a, Action::Write),
            bucket: a,
            key: "bootstrap".into(),
            stream_type: "part".into(),
            metadata: json!({}),
            public: false,
            claim_object: false,
            completion: None,
            owner: None,
        },
    )
    .await?;
    let (mut chunk, _) = backend
        .allocate(crate::model::ChunkCreate {
            storage,
            stream: bootstrap,
            hash: [1; 32],
            raw_size: 64,
            algorithm: "none".into(),
            key: None,
        })
        .await?;
    chunk.stored_size = Some(64);
    backend.start_upload(bootstrap, &chunk).await?;
    ChunkStore::complete(&backend, bootstrap, 0, &chunk).await?;
    let data = [
        ("folder/cute%_heart.png", 11, "image/png"),
        ("folder/cuteZZheart.png", 12, "image/png"),
        ("folder/HELLO.txt", 22, "text/plain"),
        ("folder/sub/hello.webm", 22, "video/webm"),
        ("a.txt", 1, "text/plain"),
        ("b.zip", 30, "application/zip"),
        ("folder/\u{53ef}\u{7231}\u{732b}.png", 41, "image/png"),
        ("odd/../a +%_#.txt", 43, "text/plain"),
        ("zero", 0, "application/octet-stream"),
    ];
    let mut ids = std::collections::BTreeMap::new();
    for (key, size, mime) in data {
        ids.insert(
            key,
            stream(&backend, a, key, size, chunk.id, true, mime).await?,
        );
    }
    assert_eq!(backend.storage(a).await?, Some(storage));
    assert_eq!(
        catalog(&backend, a, &mut CatalogFilter::default())
            .await?
            .len(),
        9
    );
    for (query, search_in, count) in [
        ("cute%_heart", "name", 1),
        ("hello", "name", 2),
        ("folder", "name", 0),
        ("folder", "path", 5),
        ("\u{53ef}\u{7231}\u{732b}", "name", 1),
    ] {
        let mut filter = CatalogFilter {
            q: query.into(),
            search_in: search_in.into(),
            ..Default::default()
        };
        assert_eq!(catalog(&backend, a, &mut filter).await?.len(), count);
    }
    let mut short = CatalogFilter {
        prefix: "folder/".into(),
        q: "HE".into(),
        ..Default::default()
    };
    assert_eq!(
        catalog(&backend, a, &mut short).await?[0].object_key,
        "folder/HELLO.txt"
    );
    assert_eq!(short.mode, "prefix");
    let mut exact = CatalogFilter {
        q: "odd/../a +%_#.txt".into(),
        mode: "exact".into(),
        ..Default::default()
    };
    assert_eq!(catalog(&backend, a, &mut exact).await?.len(), 1);
    let authority = Permit::for_action(Principal::Local, a, Action::List);
    assert_eq!(
        backend
            .browse(
                &authority,
                a,
                &mut CatalogFilter::default(),
                None,
                (Some(22), Some(22))
            )
            .await?
            .len(),
        2
    );
    for sort in ["name", "size", "modified"] {
        for order in ["asc", "desc"] {
            let mut filter = CatalogFilter {
                sort: sort.into(),
                order: order.into(),
                limit: 2,
                ..Default::default()
            };
            let mut cursor = None;
            let mut rows = Vec::new();
            loop {
                let mut page = backend
                    .browse(&authority, a, &mut filter, cursor, (None, None))
                    .await?;
                let more = page.len() > 2;
                page.truncate(2);
                if let Some(last) = page.last() {
                    cursor = Some(CatalogCursor {
                        scope: "".into(),
                        key: last.object_key.clone(),
                        size: last.size.clone(),
                        modified: last.modified_at,
                        folder: None,
                    });
                } else {
                    break;
                }
                rows.extend(page);
                if !more {
                    break;
                }
            }
            assert_eq!(rows.len(), 9);
            let values: Vec<_> = rows
                .iter()
                .map(|r| {
                    (
                        if sort == "size" {
                            r.size.parse::<i64>().unwrap()
                        } else if sort == "modified" {
                            r.modified_at.timestamp_micros()
                        } else {
                            0
                        },
                        r.object_key.clone(),
                    )
                })
                .collect();
            let mut expected = values.clone();
            expected.sort();
            if order == "desc" {
                expected.reverse();
            }
            assert_eq!(values, expected);
        }
    }
    let held = backend
        .write
        .begin_with(super::queries::BEGIN_WRITE)
        .await?;
    let mut filter = CatalogFilter {
        q: "hello".into(),
        ..Default::default()
    };
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            catalog(&backend, a, &mut filter)
        )
        .await??
        .len(),
        2
    );
    held.rollback().await?;
    let mut occupied = Vec::new();
    for _ in 0..backend.read.options().get_max_connections() {
        occupied.push(backend.read.acquire().await?);
    }
    let error = backend
        .browse(&authority, a, &mut filter, None, (None, None))
        .await
        .err()
        .expect("catalog acquisition must time out");
    assert_eq!(
        crate::model::error(&error),
        Some(crate::model::Error::QueryCancelled)
    );
    drop(occupied);
    let mut timed = connection::DeadlineConnection::acquire(
        &backend.read,
        tokio::time::Instant::now() + std::time::Duration::from_millis(50),
    )
    .await?;
    let error = sqlx::query_scalar::<_,i64>("WITH RECURSIVE n(v) AS (VALUES(0) UNION ALL SELECT v+1 FROM n WHERE v<1000000000) SELECT SUM(v) FROM n").fetch_one(&mut *timed).await.unwrap_err();
    assert_eq!(
        super::error(&error.into()),
        Some(crate::model::Error::QueryCancelled)
    );
    timed.release().await?;
    let total:i64 = sqlx::query_scalar("WITH RECURSIVE n(v) AS (VALUES(0) UNION ALL SELECT v+1 FROM n WHERE v<10000) SELECT SUM(v) FROM n").fetch_one(&backend.read).await?;
    assert_eq!(total, 50_005_000);
    assert!(tokio::time::timeout(std::time::Duration::from_millis(30),async {
        let mut timed = connection::DeadlineConnection::acquire(&backend.read,tokio::time::Instant::now()+std::time::Duration::from_secs(2)).await?;
        sqlx::query_scalar::<_,i64>("WITH RECURSIVE n(v) AS (VALUES(0) UNION ALL SELECT v+1 FROM n WHERE v<1000000000) SELECT SUM(v) FROM n").fetch_one(&mut *timed).await?;
        Ok::<_,anyhow::Error>(())
    }).await.is_err());
    let available: i64 = tokio::time::timeout(
        std::time::Duration::from_millis(400),
        sqlx::query_scalar("SELECT 7").fetch_one(&backend.read),
    )
    .await??;
    assert_eq!(available, 7);
    assert_eq!(catalog(&backend, a, &mut filter).await?.len(), 2);

    let member = Uuid::new_v4();
    sqlx::query("INSERT INTO web_users(id,username,password_hash) VALUES(?,'reader','password')")
        .bind(member)
        .execute(db)
        .await?;
    sqlx::query("INSERT INTO project_members(user_id,project_id,role,scope) VALUES(?,?,'reader','selected')").bind(member).bind(DEFAULT_PROJECT).execute(db).await?;
    sqlx::query(
        "INSERT INTO member_bucket_grants(user_id,project_id,bucket_id,actions) VALUES(?,?,?,127)",
    )
    .bind(member)
    .bind(DEFAULT_PROJECT)
    .bind(a)
    .execute(db)
    .await?;
    let verified = backend.login_user("reader").await?.unwrap();
    backend
        .login(crate::model::account::SessionLogin {
            verified: &verified,
            access_hash: &[8; 32],
            csrf_hash: &[8; 32],
            refresh_hash: &[8; 32],
            seconds: 3600,
            user_agent: "test",
            request_id: "test",
        })
        .await?;
    let session = backend.authenticate_session(&[8; 32]).await?.unwrap();
    let reader = Principal::User {
        id: member,
        session: session.session_id,
        revision: session.authorization_revision,
    };
    let permit = Permit::for_action(reader.clone(), a, Action::List);
    assert_eq!(
        backend
            .browse(
                &permit,
                a,
                &mut CatalogFilter::default(),
                None,
                (None, None)
            )
            .await?
            .len(),
        9
    );
    assert!(
        backend
            .browse(
                &permit,
                b,
                &mut CatalogFilter::default(),
                None,
                (None, None)
            )
            .await
            .is_err()
    );
    assert!(
        backend
            .change(ObjectChange {
                principal: reader,
                bucket: a,
                key: "a.txt".into(),
                expected: None,
                public: Some(true)
            })
            .await
            .is_err()
    );
    let original = ids["a.txt"];
    assert!(
        backend
            .change(ObjectChange {
                principal: Principal::Local,
                bucket: a,
                key: "a.txt".into(),
                expected: Some(Id::from(i64::MAX)),
                public: None
            })
            .await
            .is_err()
    );
    let client = Uuid::new_v4();
    let (first, second) = tokio::join!(
        backend.begin_operation(&Principal::Local, admin, client, &[1; 32]),
        backend.begin_operation(&Principal::Local, admin, client, &[1; 32])
    );
    let operation = first?.0;
    assert_eq!(operation, second?.0);
    assert_eq!(
        backend
            .begin_operation(&Principal::Local, admin, client, &[2; 32])
            .await?
            .1,
        vec![1; 32]
    );
    let receipt = || Receipt {
        id: operation,
        bucket: a,
        key: "a.txt".into(),
        action: MediaAction::PublicRead,
        count: 2,
    };
    let change = || ObjectChange {
        principal: Principal::Local,
        bucket: a,
        key: "a.txt".into(),
        expected: Some(original),
        public: Some(true),
    };
    assert!(
        crate::model::audit::scope(
            (-1).into(),
            RECEIPT.scope(receipt(), backend.change(change()))
        )
        .await
        .is_err()
    );
    assert!(backend.operation(operation).await?.is_none());
    assert!(!backend.current(a, "a.txt").await?.public_read);
    let audit =
        crate::model::AuditStore::begin(&backend, &Principal::Local, "object.change", Some("test"))
            .await?;
    assert!(
        !crate::model::audit::scope(audit, RECEIPT.scope(receipt(), backend.change(change())))
            .await?
    );
    assert_eq!(
        backend.operation(operation).await?.unwrap().output_version,
        Some(original)
    );
    let outcome: String = sqlx::query_scalar("SELECT outcome FROM audit_events WHERE id=?")
        .bind(audit)
        .fetch_one(db)
        .await?;
    assert_eq!(outcome, "partial");
    assert_eq!(
        catalog(
            &backend,
            a,
            &mut CatalogFilter {
                public: Some(true),
                ..Default::default()
            }
        )
        .await?[0]
            .id,
        original
    );
    backend
        .fail_operation(
            operation,
            &Outcome {
                status: 500,
                code: Some("ignored".into()),
                output_version: None,
                task_id: None,
            },
        )
        .await?;
    assert_eq!(backend.operation(operation).await?.unwrap().status, 200);
    let token = Uuid::nil();
    sqlx::query("INSERT INTO api_tokens(id,user_id,label,token_hash,prefix,system_access,auth_revision) VALUES(?,?,'test',?,'test',FALSE,0)").bind(token).bind(admin).bind([2u8;32].as_slice()).execute(db).await?;
    let token_principal = Principal::Token {
        id: token,
        user: admin,
        revision: 0,
        user_revision: 0,
    };
    assert_ne!(
        backend
            .begin_operation(&token_principal, admin, client, &[1; 32])
            .await?
            .0,
        operation
    );

    let source = ids["folder/HELLO.txt"];
    let copy = stream(&backend, b, "copy", 22, chunk.id, false, "text/plain").await?;
    let mut permission = Permit::for_action(Principal::Local, a, Action::Read);
    permission.add(b, Action::Write);
    let transfer = || ObjectTransfer {
        source_bucket: a,
        source_key: "folder/HELLO.txt",
        source_version: source,
        target_bucket: b,
        target_key: "copy",
        target_version: None,
    };
    backend
        .save(
            &Principal::Local,
            "bucket",
            b,
            Limits {
                logical_bytes_limit: Some("21".into()),
                inflight_bytes_limit: None,
                bucket_count_limit: None,
            },
        )
        .await?;
    assert!(
        backend
            .publish_copy(&permission, transfer(), copy, false, None)
            .await
            .is_err()
    );
    assert!(backend.current(b, "copy").await.is_err());
    backend
        .save(
            &Principal::Local,
            "bucket",
            b,
            Limits {
                logical_bytes_limit: Some("22".into()),
                inflight_bytes_limit: None,
                bucket_count_limit: None,
            },
        )
        .await?;
    backend
        .publish_copy(&permission, transfer(), copy, false, None)
        .await?;
    assert_eq!(backend.current(b, "copy").await?.id, copy);
    let detail = backend.chunks(copy, -1, 10).await?;
    assert_eq!(detail[0]["source"], "chunk");
    assert_eq!(detail[0]["reads"], "0");
    backend
        .move_object(
            &permission,
            ObjectTransfer {
                source_bucket: a,
                source_key: "folder/HELLO.txt",
                source_version: source,
                target_bucket: b,
                target_key: "copy",
                target_version: Some(copy),
            },
        )
        .await?;
    assert_eq!(backend.current(b, "copy").await?.id, source);
    assert!(backend.current(a, "folder/HELLO.txt").await.is_err());
    assert_eq!(
        backend
            .read(&Principal::Local, "bucket", b)
            .await?
            .used_bytes
            .as_deref(),
        Some("22")
    );
    let metadata = stream(&backend, a, "b.zip", 30, chunk.id, false, "video/mp4").await?;
    let transfer = || ObjectTransfer {
        source_bucket: a,
        source_key: "b.zip",
        source_version: ids["b.zip"],
        target_bucket: a,
        target_key: "b.zip",
        target_version: Some(ids["b.zip"]),
    };
    assert!(
        backend
            .publish_copy(&permission, transfer(), metadata, false, Some(true))
            .await
            .is_err()
    );
    backend
        .publish_copy(&permission, transfer(), metadata, false, Some(false))
        .await?;
    assert!(
        catalog(
            &backend,
            a,
            &mut CatalogFilter {
                category: Some("video".into()),
                ..Default::default()
            }
        )
        .await?
        .iter()
        .any(|r| r.id == metadata)
    );

    let pending = StreamStore::create(
        &backend,
        crate::model::StreamCreate {
            authority: Permit::for_action(Principal::Local, a, Action::Write),
            bucket: a,
            key: "a.txt".into(),
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
    assert!(
        backend
            .change(ObjectChange {
                principal: Principal::Local,
                bucket: a,
                key: "a.txt".into(),
                expected: Some(original),
                public: None
            })
            .await?
    );
    let quota = backend.read(&Principal::Local, "bucket", a).await?;
    assert_eq!(quota.reserved_bytes.as_deref(), Some("0"));
    assert_eq!(quota.inflight_bytes.as_deref(), Some("64"));
    backend.abandon_copy(pending).await?;
    assert_eq!(
        backend
            .read(&Principal::Local, "bucket", a)
            .await?
            .inflight_bytes
            .as_deref(),
        Some("0")
    );
    assert!(
        backend
            .change(ObjectChange {
                principal: Principal::Local,
                bucket: a,
                key: "missing".into(),
                expected: None,
                public: None
            })
            .await?
    );

    let storage2 = super::storage_tests::fixture_named(&backend, "second").await?;
    let c = BucketStore::create(
        &backend,
        &Principal::Local,
        CreateBucket {
            name: "migration".into(),
            project_id: Some(DEFAULT_PROJECT),
            storage_id: Some(storage2),
        },
    )
    .await?
    .id;
    let job = StorageMigration {
        source_bucket: b,
        source_key: "copy".into(),
        source_version: source,
        target_bucket: c,
        target_key: "migrated".into(),
        target_version: None,
        action: "move".into(),
        public_read: false,
    };
    let operation = backend
        .begin_operation(&Principal::Local, admin, Uuid::new_v4(), &[3; 32])
        .await?
        .0;
    let receipt = Receipt {
        id: operation,
        bucket: b,
        key: "copy".into(),
        action: MediaAction::Move,
        count: 1,
    };
    RECEIPT
        .scope(receipt, backend.queue_migration(&permission, &job))
        .await?;
    let result = backend.operation(operation).await?.unwrap();
    assert_eq!(result.status, 202);
    let task = result.task_id.unwrap();
    let token = Uuid::new_v4();
    sqlx::query("UPDATE tasks SET state='running',execution_token=? WHERE id=?")
        .bind(token)
        .bind(task)
        .execute(db)
        .await?;
    let destination = StreamStore::create(
        &backend,
        crate::model::StreamCreate {
            authority: Permit::system(),
            bucket: c,
            key: "migrated".into(),
            stream_type: "object".into(),
            metadata: json!({"content_type":"text/plain"}),
            public: false,
            claim_object: false,
            completion: None,
            owner: Some((task, token)),
        },
    )
    .await?;
    let (mut migrated, _) = backend
        .allocate(crate::model::ChunkCreate {
            storage: storage2,
            stream: destination,
            hash: [2; 32],
            raw_size: 22,
            algorithm: "none".into(),
            key: None,
        })
        .await?;
    migrated.stored_size = Some(22);
    backend.start_upload(destination, &migrated).await?;
    ChunkStore::complete(&backend, destination, 0, &migrated).await?;
    backend.finish(destination, 22, "etag", json!({})).await?;
    assert!(
        backend
            .complete_migration(task, Uuid::new_v4(), destination, &job)
            .await
            .is_err()
    );
    sqlx::query("UPDATE tasks SET state='paused' WHERE id=?")
        .bind(task)
        .execute(db)
        .await?;
    assert!(
        backend
            .complete_migration(task, token, destination, &job)
            .await
            .is_err()
    );
    sqlx::query("UPDATE tasks SET state='running' WHERE id=?")
        .bind(task)
        .execute(db)
        .await?;
    backend
        .complete_migration(task, token, destination, &job)
        .await?;
    assert!(backend.current(b, "copy").await.is_err());
    assert_eq!(backend.current(c, "migrated").await?.id, destination);
    assert_eq!(
        backend
            .read(&Principal::Local, "bucket", b)
            .await?
            .used_bytes
            .as_deref(),
        Some("0")
    );
    assert_eq!(
        backend
            .read(&Principal::Local, "bucket", c)
            .await?
            .used_bytes
            .as_deref(),
        Some("22")
    );
    let state: (String, Option<Uuid>) =
        sqlx::query_as("SELECT state,execution_token FROM tasks WHERE id=?")
            .bind(task)
            .fetch_one(db)
            .await?;
    assert_eq!(state, ("completed".into(), None));
    let unicode = BucketStore::create(
        &backend,
        &Principal::Local,
        CreateBucket {
            name: "unicode".into(),
            project_id: None,
            storage_id: Some(storage),
        },
    )
    .await?
    .id;
    for index in 0..130 {
        stream(
            &backend,
            unicode,
            &format!("folder/a{index:03}"),
            0,
            chunk.id,
            true,
            "text/plain",
        )
        .await?;
    }
    for name in ["folder/\u{c5}ngstr\u{f6}m1", "folder/\u{c5}ngstr\u{f6}m2"] {
        stream(&backend, unicode, name, 0, chunk.id, true, "text/plain").await?;
    }
    let mut filter = CatalogFilter {
        q: "\u{e5}ngstr\u{f6}m".into(),
        limit: 1,
        ..Default::default()
    };
    let matches = catalog(&backend, unicode, &mut filter).await?;
    assert_eq!(matches.len(), 2);
    let first = &matches[0];
    let after = CatalogCursor {
        scope: String::new(),
        key: first.object_key.clone(),
        size: first.size.clone(),
        modified: first.modified_at,
        folder: None,
    };
    assert_eq!(
        backend
            .browse(
                &Permit::for_action(Principal::Local, unicode, Action::List),
                unicode,
                &mut filter,
                Some(after),
                (None, None)
            )
            .await?
            .len(),
        1
    );
    backend.check_owner()?;
    backend.read.close().await;
    backend.write.close().await;
    Ok(())
}
