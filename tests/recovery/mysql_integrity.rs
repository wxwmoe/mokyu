use super::{Backend, cache_tests, connection, queries};
use crate::authorization::Principal;
use crate::config::Config;
use crate::model::bucket::CreateBucket;
use crate::model::integrity::{Cursor, Detail, Finding, IntegrityStore};
use crate::model::task::Execution;
use crate::model::{
    AccountStore, BucketStore, ChunkStore, PhysicalStore, PublishStore, QuotaStore, StreamStore,
    TaskControlStore,
};
use crate::worker::integrity::{Mode, Request};
use anyhow::Result;
use serde_json::json;
use uuid::Uuid;

fn request() -> Request {
    Request {
        mode: Mode::Full,
        bucket: None,
        key: None,
    }
}
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
async fn detail(backend: &Backend, id: Uuid) -> Result<Detail> {
    Ok(serde_json::from_value(
        backend.raw(id).await?.unwrap()["detail"].clone(),
    )?)
}
async fn publish(backend: &Backend, stream: Uuid) -> Result<()> {
    backend.reserve(stream, 32, true, None).await?;
    StreamStore::finish(backend, stream, 32, "fixture", json!({})).await?;
    assert!(backend.publish(stream, None, None, None).await?);
    Ok(())
}
fn finding(chunk: i64, code: &str) -> Finding {
    Finding {
        subject: format!("chunk:{chunk}"),
        code: code.into(),
        chunk_id: Some(chunk),
        location_id: Some(Uuid::nil()),
        stream_id: None,
        bucket_id: None,
        key: None,
        detail: json!({"fixture":true}),
    }
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_integrity_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    backend.initialize_admin("admin", "password", false).await?;
    let (empty, children) = backend.start(request(), None, None).await?;
    assert_eq!(children, 0);
    let mut d = detail(&backend, empty).await?;
    assert!(d.storage_id.is_none() && d.upper_object.is_none());
    assert_eq!(d.storages.len(), 1);
    let execution = execute(&backend, empty).await?;
    d.phase = "done".into();
    backend
        .checkpoint(execution, "{}".into(), d, vec![])
        .await?;
    assert_eq!(backend.raw(empty).await?.unwrap()["state"], "completed");
    let first = super::storage_tests::fixture_named(&backend, "first").await?;
    let second = super::storage_tests::fixture_named(&backend, "second").await?;
    sqlx::query("UPDATE storages SET created_at=IF(id=?,'2000-01-01','2000-01-02')")
        .bind(first)
        .execute(db)
        .await?;
    let mut buckets = Vec::new();
    for (name, storage) in [("first", first), ("second", second)] {
        buckets.push(
            BucketStore::create(
                &backend,
                &Principal::Local,
                CreateBucket {
                    name: name.into(),
                    project_id: None,
                    storage_id: Some(storage),
                },
            )
            .await?,
        );
    }
    let mut streams = Vec::new();
    let mut chunks = Vec::new();
    for (bucket, key, storage, seed) in [
        (buckets[0].id, "A", first, 1),
        (buckets[0].id, "m", first, 2),
        (buckets[1].id, "B", second, 3),
    ] {
        let stream = cache_tests::stream(&backend, bucket, key).await?;
        let chunk = cache_tests::chunk(&backend, storage, stream, seed).await?;
        backend.register_encoding(&chunk).await?;
        ChunkStore::complete(&backend, stream, 0, &chunk).await?;
        publish(&backend, stream).await?;
        streams.push(stream);
        chunks.push(chunk);
    }
    let copy = cache_tests::stream(&backend, buckets[0].id, "a ").await?;
    backend.reference(copy, 0, chunks[0].id, 32).await?;
    publish(&backend, copy).await?;
    let (a, b) = tokio::join!(
        backend.start(request(), None, None),
        backend.start(request(), None, None)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let (id, children) = a.or(b)?;
    assert_eq!(children, 0);
    assert!(backend.start(request(), None, None).await.is_err());
    assert!(
        backend
            .start(request(), None, Some(Uuid::new_v4()))
            .await
            .is_err()
    );
    assert!(backend.exists(id).await?);
    assert!(!backend.exists(Uuid::new_v4()).await?);
    let task = execute(&backend, id).await?;
    let mut d = detail(&backend, id).await?;
    assert_eq!(d.storage_id, Some(first));
    assert_eq!(d.storages.len(), 2);
    assert_eq!(d.storages[1].storage_id, Some(second));
    assert_eq!(d.upper_object, Some((buckets[0].id, "m".into())));
    let selected = backend.chunks(&d, 0, 10).await?;
    assert_eq!(
        selected.iter().map(|c| c.id).collect::<Vec<_>>(),
        vec![chunks[0].id, chunks[1].id]
    );
    assert_eq!(
        backend.chunks(&d, chunks[0].id, 1).await?[0].id,
        chunks[1].id
    );
    let (mapped, bucket, key) = backend.mapping(&d, &Cursor::default()).await?.unwrap();
    assert_eq!(
        (mapped.id, bucket, key.as_str()),
        (streams[0], buckets[0].id, "A")
    );
    assert!(IntegrityStore::current(&backend, bucket, &key, mapped.id).await?);
    assert!(!IntegrityStore::current(&backend, bucket, &key, Uuid::new_v4()).await?);
    assert_eq!(
        backend.chunk_stream(chunks[0].id, &d).await?,
        Some(streams[0])
    );
    let cursor = Cursor {
        bucket: Some(bucket),
        key: Some("A".into()),
        ..Default::default()
    };
    assert_eq!(backend.mapping(&d, &cursor).await?.unwrap().2, "a ");
    let cursor = Cursor {
        stream: Some(streams[1]),
        ..Default::default()
    };
    assert_eq!(
        backend.mapping(&d, &cursor).await?.unwrap().0.id,
        streams[1]
    );
    sqlx::query("UPDATE streams SET object_key=? WHERE id=?")
        .bind(b"mismatched".as_slice())
        .bind(streams[0])
        .execute(db)
        .await?;
    let (mapped, _, key) = backend.mapping(&d, &Cursor::default()).await?.unwrap();
    assert_eq!(
        (mapped.object_key.as_str(), key.as_str()),
        ("mismatched", "A")
    );
    sqlx::query("UPDATE streams SET object_key=? WHERE id=?")
        .bind(b"A".as_slice())
        .bind(streams[0])
        .execute(db)
        .await?;
    let fragment = Uuid::new_v4();
    sqlx::query("INSERT INTO fragments(id,owner_stream,sealed,size,hash) VALUES(?,?,TRUE,8,?)")
        .bind(fragment)
        .bind(streams[0])
        .bind([9u8; 32].as_slice())
        .execute(db)
        .await?;
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,fragment_id) VALUES(?,32,8,?)")
        .bind(streams[0])
        .bind(fragment)
        .execute(db)
        .await?;
    let ranges = backend.ranges(streams[0], -1).await?;
    assert_eq!(ranges.len(), 2);
    assert_eq!(ranges[0].state.as_deref(), Some("ready"));
    assert_eq!(ranges[0].raw_size, Some(32));
    assert_eq!(ranges[1].fragment_id, Some(fragment));
    assert!(ranges[1].chunk_id.is_none() && ranges[1].state.is_none());
    assert_eq!(backend.ranges(streams[0], 0).await?.len(), 1);
    assert!(backend.ranges(streams[0], 32).await?.is_empty());

    let late = cache_tests::stream(&backend, buckets[1].id, "zz").await?;
    let chunk = cache_tests::chunk(&backend, second, late, 4).await?;
    backend.register_encoding(&chunk).await?;
    ChunkStore::complete(&backend, late, 0, &chunk).await?;
    publish(&backend, late).await?;
    super::storage_tests::fixture_named(&backend, "late").await?;
    d.objects_checked = 2;
    d.chunks_checked = 1;
    let object_issue = Finding {
        subject: format!("object:{}:32", streams[0]),
        code: "SizeMismatch".into(),
        chunk_id: None,
        location_id: None,
        stream_id: Some(streams[0]),
        bucket_id: Some(buckets[0].id),
        key: Some("z\u{2603}".into()),
        detail: json!({"offset":32}),
    };
    backend
        .checkpoint(
            task,
            "first".into(),
            d,
            vec![
                finding(chunks[0].id, "Corrupt"),
                finding(chunks[0].id, "Corrupt"),
                object_issue,
            ],
        )
        .await?;
    d = detail(&backend, id).await?;
    assert_eq!(d.issues, 2);
    assert_eq!(backend.raw(id).await?.unwrap()["processed"], 3);
    backend
        .checkpoint(
            task,
            "duplicate".into(),
            d,
            vec![finding(chunks[0].id, "Corrupt")],
        )
        .await?;
    assert_eq!(detail(&backend, id).await?.issues, 2);
    let stale = Execution {
        id,
        token: Uuid::new_v4(),
    };
    assert!(
        backend
            .checkpoint(
                stale,
                "stale".into(),
                detail(&backend, id).await?,
                vec![finding(chunks[1].id, "Corrupt")]
            )
            .await
            .is_err()
    );
    assert_eq!(backend.findings(id, 0, 10, None).await?.len(), 2);
    assert!(
        backend
            .checkpoint(
                task,
                "invalid".into(),
                detail(&backend, id).await?,
                vec![
                    finding(chunks[1].id, "Rollback"),
                    finding(chunks[1].id, &"x".repeat(65))
                ]
            )
            .await
            .is_err()
    );
    assert_eq!(backend.findings(id, 0, 10, None).await?.len(), 2);
    assert_eq!(backend.raw(id).await?.unwrap()["cursor"], "duplicate");
    let saved_upper = backend.report_upper(id).await?;
    backend
        .checkpoint(
            task,
            "later".into(),
            detail(&backend, id).await?,
            vec![finding(i64::MAX, "Missing")],
        )
        .await?;
    assert_eq!(detail(&backend, id).await?.issues, 3);
    assert_eq!(backend.report_page(id, 0, saved_upper).await?.len(), 2);
    let reports = backend.findings(id, 0, 10, None).await?;
    assert_eq!(reports.len(), 3);
    assert_eq!(
        backend.groups(id).await?,
        vec![
            ("Corrupt".into(), "1".into()),
            ("Missing".into(), "1".into()),
            ("SizeMismatch".into(), "1".into())
        ]
    );
    let issue = &backend.findings(id, 0, 10, Some("Corrupt")).await?[0];
    let issue_id = issue["id"].as_str().unwrap().parse()?;
    assert_eq!(issue["chunk_id"], chunks[0].id.to_string());
    assert!(issue["created_at"].as_str().unwrap().ends_with('Z'));
    let objects = backend.objects(id, issue_id, None).await?;
    assert_eq!(
        objects
            .iter()
            .map(|o| o["key"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["A", "a "]
    );
    assert_eq!(
        backend
            .objects(id, issue_id, Some((buckets[0].id, "A".into())))
            .await?
            .len(),
        1
    );
    assert!(
        backend
            .objects(Uuid::new_v4(), issue_id, None)
            .await
            .is_err()
    );
    let issue = &backend.findings(id, 0, 10, Some("SizeMismatch")).await?[0];
    assert_eq!(issue["object_key"], "z\u{2603}");
    assert_eq!(
        backend
            .objects(id, issue["id"].as_str().unwrap().parse()?, None)
            .await?
            .len(),
        1
    );
    let issue = &backend.findings(id, 0, 10, Some("Missing")).await?[0];
    assert!(
        backend
            .objects(id, issue["id"].as_str().unwrap().parse()?, None)
            .await?
            .is_empty()
    );
    let report = backend.report_task(id).await?.unwrap();
    for field in [
        "parent_task_id",
        "execution_token",
        "lease_token",
        "lease_node_id",
    ] {
        assert!(report.get(field).is_none());
    }
    assert!(report["detail"].get("role").is_none());

    d = detail(&backend, id).await?;
    d.phase = "done".into();
    d.finished_at = Some(chrono::Utc::now());
    backend
        .checkpoint(task, "end-first".into(), d, vec![])
        .await?;
    d = detail(&backend, id).await?;
    assert_eq!(
        (d.storage_index, d.storage_id, d.phase.as_str()),
        (1, Some(second), "metadata")
    );
    assert!(d.finished_at.is_none());
    assert_eq!(d.storages.len(), 2);
    assert_eq!(
        backend.raw(id).await?.unwrap()["cursor"],
        serde_json::to_string(&Cursor::default())?
    );
    assert_eq!(
        backend
            .chunks(&d, 0, 10)
            .await?
            .iter()
            .map(|c| c.id)
            .collect::<Vec<_>>(),
        vec![chunks[2].id]
    );
    assert_eq!(
        backend.mapping(&d, &Cursor::default()).await?.unwrap().0.id,
        streams[2]
    );
    assert!(
        backend
            .mapping(
                &d,
                &Cursor {
                    bucket: Some(buckets[1].id),
                    key: Some("B".into()),
                    ..Default::default()
                }
            )
            .await?
            .is_none()
    );
    d.phase = "done".into();
    d.finished_at = Some(chrono::Utc::now());
    backend.checkpoint(task, "end".into(), d, vec![]).await?;
    assert_eq!(backend.raw(id).await?.unwrap()["state"], "completed");
    assert!(
        backend
            .checkpoint(task, "again".into(), detail(&backend, id).await?, vec![])
            .await
            .is_err()
    );
    let scoped = Request {
        mode: Mode::Metadata,
        bucket: Some(buckets[0].name.clone()),
        key: Some("a ".into()),
    };
    let (id, children) = backend
        .start(scoped, Some(buckets[0].clone()), None)
        .await?;
    assert_eq!(children, 0);
    let d = detail(&backend, id).await?;
    assert_eq!(d.storages.len(), 1);
    assert_eq!(
        backend.mapping(&d, &Cursor::default()).await?.unwrap().0.id,
        copy
    );
    assert_eq!(
        backend
            .chunks(&d, 0, 10)
            .await?
            .iter()
            .map(|c| c.id)
            .collect::<Vec<_>>(),
        vec![chunks[0].id]
    );
    assert!(backend.chunk_stream(chunks[1].id, &d).await?.is_none());
    Ok(())
}
