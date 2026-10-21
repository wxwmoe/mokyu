use super::{Backend, connection, queries, transactions};
use crate::authorization::{Action, DEFAULT_PROJECT, Permit, Principal};
use crate::config::Config;
use crate::model::Id;
use crate::model::quota::Limits;
use crate::model::{
    ChunkCreate, ChunkStore, PhysicalStore, PublishStore, QuotaStore, StreamCreate, StreamStore,
};
use anyhow::Result;
use serde_json::json;
use uuid::Uuid;

async fn stream(backend: &Backend, bucket: Uuid, key: &str) -> Result<Id> {
    backend
        .create(StreamCreate {
            authority: Permit::for_action(Principal::Local, bucket, Action::Write),
            bucket,
            key: key.into(),
            stream_type: "object".into(),
            metadata: json!({"content_type":"image/png"}),
            public: false,
            claim_object: true,
            completion: None,
            owner: None,
        })
        .await
}
fn limits(bytes: Option<i64>, inflight: Option<i64>) -> Limits {
    Limits {
        logical_bytes_limit: bytes.map(|n| n.to_string()),
        inflight_bytes_limit: inflight.map(|n| n.to_string()),
        bucket_count_limit: None,
    }
}
async fn usage(
    backend: &Backend,
    bucket: Uuid,
    used: i64,
    reserved: i64,
    inflight: i64,
    objects: i64,
) -> Result<()> {
    for (scope, id) in [("project", DEFAULT_PROJECT), ("bucket", bucket)] {
        let quota = backend.read(&Principal::Local, scope, id).await?;
        assert_eq!(
            (
                quota.used_bytes,
                quota.reserved_bytes,
                quota.inflight_bytes,
                quota.object_count
            ),
            (
                Some(used.to_string()),
                Some(reserved.to_string()),
                Some(inflight.to_string()),
                Some(objects.to_string())
            ),
            "{scope}"
        );
    }
    Ok(())
}
async fn abandon(backend: &Backend, bucket: Uuid, stream: Id) -> Result<()> {
    let mut tx = backend.write.begin_with(queries::BEGIN_WRITE).await?;
    queries::publish_stream_record(stream)
        .fetch_one(&mut *tx)
        .await?;
    let mut accounts = transactions::lock_quota(&mut tx, bucket).await?;
    transactions::forget_quota_write(&mut tx, &mut accounts, stream).await?;
    sqlx::query("UPDATE streams SET state='abandoned' WHERE id=?")
        .bind(stream)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_SQLITE_CONFIG pointing to a fresh isolated SQLite database"]
async fn sqlite_quota_and_publication_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.write;
    let storage = super::storage_tests::fixture(&backend).await?;
    let bucket = Uuid::new_v4();
    sqlx::query("INSERT INTO buckets(id,name,project_id,storage_id) VALUES(?,'quota',?,?)")
        .bind(bucket)
        .bind(DEFAULT_PROJECT)
        .bind(storage)
        .execute(db)
        .await?;
    queries::insert_quota_scope("bucket", bucket)
        .execute(db)
        .await?;
    sqlx::query("UPDATE quota_scopes SET bucket_count=1 WHERE scope='project' AND scope_id=?")
        .bind(DEFAULT_PROJECT)
        .execute(db)
        .await?;
    backend
        .save(
            &Principal::Local,
            "project",
            DEFAULT_PROJECT,
            limits(Some(80), Some(160)),
        )
        .await?;
    backend
        .save(&Principal::Local, "bucket", bucket, limits(Some(64), None))
        .await?;
    assert!(
        backend
            .save(&Principal::Local, "bucket", bucket, limits(None, Some(1)))
            .await
            .is_err()
    );
    assert!(
        backend
            .save(&Principal::Local, "unknown", bucket, limits(None, None))
            .await
            .is_err()
    );
    assert!(
        backend
            .save(&Principal::Local, "bucket", bucket, limits(Some(-1), None))
            .await
            .is_err()
    );
    let denied = Principal::Service {
        access_key: "missing".into(),
        revision: 0,
    };
    assert!(backend.read(&denied, "bucket", bucket).await.is_err());
    assert!(
        backend
            .save(&denied, "bucket", bucket, limits(None, None))
            .await
            .is_err()
    );
    assert!(
        crate::model::audit::scope(
            (-1).into(),
            backend.save(&Principal::Local, "bucket", bucket, limits(Some(999), None))
        )
        .await
        .is_err()
    );
    assert_eq!(
        backend
            .read(&Principal::Local, "bucket", bucket)
            .await?
            .logical_bytes_limit
            .as_deref(),
        Some("64")
    );

    let held = backend.write.begin_with(queries::BEGIN_WRITE).await?;
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        backend.read(&Principal::Local, "bucket", bucket),
    )
    .await??;
    held.rollback().await?;
    let first = stream(&backend, bucket, "object.png").await?;
    assert_eq!(backend.reserve(first, 32, true, None).await?, 64);
    usage(&backend, bucket, 0, 64, 64, 0).await?;
    let error = backend.reserve(first, 65, false, None).await.unwrap_err();
    assert_eq!(
        crate::model::error(&error),
        Some(crate::model::Error::QuotaExceeded)
    );
    usage(&backend, bucket, 0, 64, 64, 0).await?;
    assert_eq!(backend.reserve(first, 32, false, None).await?, 32);
    let (mut physical, _) = backend
        .allocate(ChunkCreate {
            storage,
            stream: first,
            hash: [1; 32],
            raw_size: 32,
            algorithm: "none".into(),
            key: None,
        })
        .await?;
    physical.stored_size = Some(32);
    physical.location_id = Uuid::new_v4();
    backend.mark_uploading(&physical).await?;
    backend.register_encoding(&physical).await?;
    ChunkStore::complete(&backend, first, 0, &physical).await?;
    backend.finish(first, 32, "one", json!({})).await?;
    assert!(
        backend
            .publish(first, None, Some(s3s::dto::ETagCondition::Any), None)
            .await?
    );
    usage(&backend, bucket, 32, 0, 0, 1).await?;
    assert_eq!(backend.current(bucket, "object.png").await?.id, first);
    let category: String = sqlx::query_scalar(
        "SELECT catalog_category FROM objects WHERE bucket_id=? AND object_key=?",
    )
    .bind(bucket)
    .bind(b"object.png".as_slice())
    .fetch_one(db)
    .await?;
    assert_eq!(category, "image");

    let failed = stream(&backend, bucket, "object.png").await?;
    backend.reserve(failed, 32, false, None).await?;
    usage(&backend, bucket, 32, 0, 32, 1).await?;
    let replacement = stream(&backend, bucket, "object.png").await?;
    backend.reserve(replacement, 64, false, None).await?;
    usage(&backend, bucket, 32, 32, 96, 1).await?;
    assert!(backend.reserve(failed, 32, false, None).await.is_err());
    assert!(backend.publish(failed, None, None, None).await.is_err());
    abandon(&backend, bucket, failed).await?;
    usage(&backend, bucket, 32, 32, 64, 1).await?;
    backend.reference(replacement, 0, physical.id, 32).await?;
    backend.reference(replacement, 32, physical.id, 32).await?;
    backend.finish(replacement, 64, "two", json!({})).await?;
    assert!(
        backend
            .publish(replacement, None, Some(s3s::dto::ETagCondition::Any), None)
            .await
            .is_err()
    );
    assert_eq!(backend.current(bucket, "object.png").await?.id, first);
    assert!(
        backend
            .publish(
                replacement,
                Some(s3s::dto::ETagCondition::ETag(s3s::dto::ETag::Strong(
                    "one".into()
                ))),
                None,
                None
            )
            .await?
    );
    usage(&backend, bucket, 64, 0, 0, 1).await?;

    let empty = stream(&backend, bucket, "empty").await?;
    backend.reserve(empty, 0, false, None).await?;
    backend.finish(empty, 0, "empty", json!({})).await?;
    assert!(backend.publish(empty, None, None, None).await?);
    usage(&backend, bucket, 64, 0, 0, 2).await?;
    backend
        .save(&Principal::Local, "bucket", bucket, limits(None, None))
        .await?;
    backend
        .save(
            &Principal::Local,
            "project",
            DEFAULT_PROJECT,
            limits(None, Some(64)),
        )
        .await?;
    let a = stream(&backend, bucket, "a").await?;
    let b = stream(&backend, bucket, "b").await?;
    let (ra, rb) = tokio::join!(
        backend.reserve(a, 48, false, None),
        backend.reserve(b, 48, false, None)
    );
    assert_ne!(ra.is_ok(), rb.is_ok());
    assert_eq!(
        crate::model::error(&ra.err().or_else(|| rb.err()).unwrap()),
        Some(crate::model::Error::QuotaExceeded)
    );
    usage(&backend, bucket, 64, 48, 48, 2).await?;
    abandon(&backend, bucket, a).await?;
    abandon(&backend, bucket, b).await?;
    usage(&backend, bucket, 64, 0, 0, 2).await?;
    backend
        .save(
            &Principal::Local,
            "project",
            DEFAULT_PROJECT,
            limits(None, None),
        )
        .await?;
    let malformed = stream(&backend, bucket, "gaps").await?;
    backend.reserve(malformed, 32, false, None).await?;
    backend.reference(malformed, 1, physical.id, 31).await?;
    backend.finish(malformed, 32, "gap", json!({})).await?;
    assert!(backend.publish(malformed, None, None, None).await.is_err());
    assert!(backend.current(bucket, "gaps").await.is_err());
    abandon(&backend, bucket, malformed).await?;

    // Accepted multipart bytes stay reserved; unaccepted writes retain inflight bytes.
    let upload = Uuid::new_v4();
    sqlx::query("INSERT INTO uploads(id,bucket_id,object_key,access_key) VALUES(?,?,?,'fixture')")
        .bind(upload)
        .bind(bucket)
        .bind(b"multipart".as_slice())
        .execute(db)
        .await?;
    let mut part_ids = Vec::new();
    for number in 1..=2 {
        let part = backend
            .create(StreamCreate {
                authority: Permit::for_action(Principal::Local, bucket, Action::Write),
                bucket,
                key: "multipart".into(),
                stream_type: "part".into(),
                metadata: json!({}),
                public: false,
                claim_object: false,
                completion: None,
                owner: None,
            })
            .await?;
        sqlx::query("INSERT INTO parts(upload_id,part_number,write_token) VALUES(?,?,?)")
            .bind(upload)
            .bind(number)
            .bind(part)
            .execute(db)
            .await?;
        backend
            .reserve(part, 32, false, Some((upload, number)))
            .await?;
        backend.reference(part, 0, physical.id, 32).await?;
        backend.finish(part, 32, "part", json!({})).await?;
        part_ids.push(part);
    }
    usage(&backend, bucket, 64, 64, 64, 2).await?;
    // Start with one accepted part and one unfinished write.
    let mut tx = backend.write.begin_with(queries::BEGIN_WRITE).await?;
    sqlx::query("UPDATE parts SET stream_id=?,quota_size=32 WHERE upload_id=? AND part_number=1")
        .bind(part_ids[0])
        .bind(upload)
        .execute(&mut *tx)
        .await?;
    queries::delete_quota_write(part_ids[0])
        .execute(&mut *tx)
        .await?;
    queries::publish_stream(part_ids[0])
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    let completion_token = Uuid::new_v4();
    sqlx::query("UPDATE uploads SET state='completing',manifest_hash='manifest',completion_token=?,completion_expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+120000000 WHERE id=?")
        .bind(completion_token)
        .bind(upload)
        .execute(db)
        .await?;
    let wrong_output = stream(&backend, bucket, "wrong-output").await?;
    assert!(
        QuotaStore::complete(&backend, upload, wrong_output, 32)
            .await
            .is_err()
    );
    assert!(
        backend
            .publish(
                wrong_output,
                None,
                None,
                Some((upload, "manifest".into(), json!({}), completion_token))
            )
            .await
            .is_err()
    );
    let output = stream(&backend, bucket, "multipart").await?;
    QuotaStore::complete(&backend, upload, output, 32).await?;
    backend.reference(output, 0, physical.id, 32).await?;
    backend.finish(output, 32, "multipart", json!({})).await?;
    assert!(
        !backend
            .publish(
                output,
                None,
                None,
                Some((upload, "wrong".into(), json!({}), completion_token))
            )
            .await?
    );
    usage(&backend, bucket, 64, 64, 64, 2).await?;
    assert!(
        crate::model::audit::scope(
            (-1).into(),
            backend.publish(
                output,
                None,
                None,
                Some((upload, "manifest".into(), json!({}), completion_token))
            )
        )
        .await
        .is_err()
    );
    assert!(backend.current(bucket, "multipart").await.is_err());
    assert!(
        backend
            .publish(
                output,
                None,
                None,
                Some((
                    upload,
                    "manifest".into(),
                    json!({"etag":"multipart"}),
                    completion_token
                ))
            )
            .await?
    );
    usage(&backend, bucket, 96, 0, 32, 3).await?;
    assert!(
        backend
            .reserve(part_ids[1], 32, false, Some((upload, 2)))
            .await
            .is_err()
    );
    abandon(&backend, bucket, part_ids[1]).await?;
    usage(&backend, bucket, 96, 0, 0, 3).await?;

    let overflow = stream(&backend, bucket, "large").await?;
    assert_eq!(
        backend.reserve(overflow, i64::MAX, true, None).await?,
        i64::MAX
    );
    assert!(
        backend
            .reserve(stream(&backend, bucket, "overflow").await?, 1, false, None)
            .await
            .is_err()
    );
    abandon(&backend, bucket, overflow).await?;
    usage(&backend, bucket, 96, 0, 0, 3).await?;
    backend.check_owner()?;
    backend.read.close().await;
    backend.write.close().await;
    Ok(())
}
