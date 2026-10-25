use super::*;
use crate::model::{node_read as read, node_tail as tail, node_write as write};

pub(super) async fn check(
    backend: &Backend,
    peer: &Backend,
    node: &crate::cluster::NodeState,
    key: &Key,
    session: &crate::model::session::Snapshot,
    targets: (Uuid, Uuid),
    revision: i64,
) -> Result<()> {
    let pool = &backend.pool;
    let (bucket, storage) = targets;
    let run = session.run;
    let intent = Intent {
        access: "writer".into(),
        credential_revision: revision,
        bucket: "multipart-control-a".into(),
        key: "parts/body".into(),
        operation: Operation::Create {
            metadata: serde_json::json!({"content_type":"text/plain"}),
            public: false,
            algorithm: Some("SHA256".into()),
            checksum_type: Some("COMPOSITE".into()),
        },
    };
    let Outcome::Created { upload } = backend
        .multipart_operation(
            node,
            key,
            Request {
                run,
                operation: Uuid::now_v7(),
                intent: intent.clone(),
            },
            None,
        )
        .await?
    else {
        panic!("upload missing")
    };
    let begin = |operation, number| write::Begin {
        run,
        operation,
        cached: vec![],
        intent: write::Intent {
            completion: None,
            access: intent.access.clone(),
            credential_revision: revision,
            bucket: intent.bucket.clone(),
            key: intent.key.clone(),
            metadata: serde_json::json!({}),
            public: false,
            size: None,
            copy_read: None,
            part: Some(write::Part {
                upload,
                number,
                algorithm: None,
            }),
        },
    };
    let id = Uuid::now_v7();
    let (first, replay) = tokio::join!(
        backend.begin_write(node, key, begin(id, 1)),
        peer.begin_write(node, key, begin(id, 1))
    );
    let first = first?;
    assert_eq!(first.stream, replay?.stream);
    assert_eq!(first.phase, write::Phase::Active);
    assert_eq!(first.checksum_algorithm.as_deref(), Some("SHA256"));
    assert_eq!(first.bucket, Some(bucket));
    let stream = first.stream.unwrap();
    let (ty, metadata): (String, serde_json::Value) =
        sqlx::query_as("SELECT stream_type,metadata FROM streams WHERE id=$1")
            .bind(stream)
            .fetch_one(pool)
            .await?;
    assert_eq!(ty, "part");
    assert_eq!(metadata["content_type"], "text/plain");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM objects WHERE bucket_id=$1 AND object_key=$2"
        )
        .bind(bucket)
        .bind(&intent.key)
        .fetch_one(pool)
        .await?,
        0
    );
    let mut wrong = begin(Uuid::now_v7(), 1);
    wrong.intent.key.push_str("/wrong");
    assert!(backend.begin_write(node, key, wrong).await.is_err());
    let mut wrong = begin(Uuid::now_v7(), 1);
    wrong.intent.part.as_mut().unwrap().algorithm = Some("SHA1".into());
    assert!(backend.begin_write(node, key, wrong).await.is_err());
    assert!(backend.begin_write(node, key, begin(id, 2)).await.is_err());
    assert!(
        backend
            .begin_write(node, key, begin(Uuid::now_v7(), 10001))
            .await
            .is_err()
    );

    sqlx::query("UPDATE runtime_settings SET value=jsonb_set(value,'{staging_storage_id}',to_jsonb($1::text)) WHERE section='uploads'")
        .bind(storage.to_string()).execute(pool).await?;
    sqlx::query("UPDATE storage_profiles SET upload_concurrency=1 WHERE storage_id=$1")
        .bind(storage)
        .execute(pool)
        .await?;
    let prepare = |operation, size| tail::Prepare {
        run,
        operation,
        offset: 0,
        size,
        hash: [17; 32],
        cached: vec![],
    };
    let tail = backend.prepare_tail(node, key, prepare(id, 3)).await?;
    assert!(!tail.committed);
    assert_eq!(tail.end, 3);
    assert_eq!(tail.materials.len(), 1);
    assert_eq!(tail.grants.len(), 1);
    let renew = first
        .grants
        .iter()
        .chain(&tail.grants)
        .map(|grant| read::GrantRef {
            r#type: grant.r#type,
            id: grant.id,
        })
        .collect();
    let ack = backend
        .pulse_session(
            node,
            key,
            run,
            session.epoch,
            1,
            false,
            read::KeepAlive {
                renew,
                release: vec![],
            },
        )
        .await?;
    assert_eq!(ack.grants.len(), 2);
    assert!(
        ack.grants
            .iter()
            .all(|grant| grant.accepted && grant.valid_for_ms >= 60_000)
    );
    assert_eq!(
        peer.prepare_tail(node, key, prepare(id, 3)).await?.fragment,
        tail.fragment
    );
    assert!(
        backend
            .prepare_tail(node, key, prepare(id, 4))
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM fragments WHERE owner_stream=$1")
            .bind(stream)
            .fetch_one(pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT bytes FROM staging_objects WHERE storage_id=$1 AND object_key=$2"
        )
        .bind(storage)
        .bind(&tail.object_key)
        .fetch_one(pool)
        .await?,
        3
    );
    let publication = |operation, size| write::Publish {
        run,
        operation,
        size,
        etag: "900150983cd24fb0d6963f7d28e17f72".into(),
        checksums: [(
            "sha256".into(),
            "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=".into(),
        )]
        .into(),
        if_match: None,
        if_none: None,
    };
    assert!(
        backend
            .publish_write(node, key, publication(id, 0))
            .await
            .is_err()
    );
    let lookup = |operation| write::Lookup { run, operation };
    assert!(backend.commit_tail(node, key, lookup(id)).await?.committed);
    assert!(peer.commit_tail(node, key, lookup(id)).await?.committed);
    let result = backend.publish_write(node, key, publication(id, 3)).await?;
    assert_eq!(result.phase, write::Phase::Published);
    assert_eq!(
        peer.publish_write(node, key, publication(id, 3))
            .await?
            .published
            .unwrap()
            .size,
        3
    );
    assert!(
        backend
            .prepare_tail(node, key, prepare(id, 3))
            .await?
            .committed
    );
    assert_eq!(
        queries::upload_part(upload, 1).fetch_one(pool).await?.id,
        stream.into()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM extents WHERE stream_id=$1")
            .bind(stream)
            .fetch_one(pool)
            .await?,
        1
    );

    let stale = Uuid::now_v7();
    let stale_write = backend.begin_write(node, key, begin(stale, 1)).await?;
    let replacement = Uuid::now_v7();
    let current = peer.begin_write(node, key, begin(replacement, 1)).await?;
    assert!(
        backend
            .prepare_tail(node, key, prepare(stale, 3))
            .await
            .is_err()
    );
    assert_eq!(
        queries::upload_part(upload, 1).fetch_one(pool).await?.id,
        stream.into()
    );
    let keep = read::KeepAlive {
        renew: stale_write
            .grants
            .iter()
            .map(|grant| read::GrantRef {
                r#type: grant.r#type,
                id: grant.id,
            })
            .collect(),
        release: vec![],
    };
    let ack = backend
        .pulse_session(node, key, run, session.epoch, 2, false, keep)
        .await?;
    assert!(ack.grants.iter().all(|grant| !grant.accepted));
    backend.write_status(node, key, lookup(stale), true).await?;
    assert_eq!(
        queries::node_write(node.node_id, replacement)
            .fetch_one(pool)
            .await?
            .stream_id,
        current.stream
    );
    backend
        .prepare_tail(node, key, prepare(replacement, 3))
        .await?;
    backend.commit_tail(node, key, lookup(replacement)).await?;
    backend
        .publish_write(node, key, publication(replacement, 3))
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM streams WHERE id=$1")
            .bind(stream)
            .fetch_one(pool)
            .await?,
        "retired"
    );
    assert_eq!(
        queries::upload_part(upload, 1).fetch_one(pool).await?.id,
        current.stream.unwrap().into()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT logical_bytes FROM quota_reservations WHERE id=$1")
            .bind(upload)
            .fetch_one(pool)
            .await?,
        3
    );

    sqlx::query("UPDATE runtime_settings SET value=jsonb_set(value,'{staging_bytes_limit}','\"1B\"') WHERE section='uploads'").execute(pool).await?;
    let bounded = Uuid::now_v7();
    let bounded_write = backend.begin_write(node, key, begin(bounded, 2)).await?;
    assert!(
        backend
            .prepare_tail(node, key, prepare(bounded, 3))
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM fragments WHERE owner_stream=$1")
            .bind(bounded_write.stream)
            .fetch_one(pool)
            .await?,
        0
    );
    sqlx::query(
        "UPDATE runtime_settings SET value=value-'staging_bytes_limit' WHERE section='uploads'",
    )
    .execute(pool)
    .await?;
    backend
        .write_status(node, key, lookup(bounded), true)
        .await?;

    sqlx::query("UPDATE buckets SET uploads_paused=true WHERE id=$1")
        .bind(bucket)
        .execute(pool)
        .await?;
    let empty = Uuid::now_v7();
    backend.begin_write(node, key, begin(empty, 2)).await?;
    let mut value = publication(empty, 0);
    value.etag = "d41d8cd98f00b204e9800998ecf8427e".into();
    backend.publish_write(node, key, value).await?;
    sqlx::query("UPDATE buckets SET uploads_paused=false WHERE id=$1")
        .bind(bucket)
        .execute(pool)
        .await?;
    let expires = Uuid::now_v7();
    let expires_write = backend.begin_write(node, key, begin(expires, 3)).await?;
    sqlx::query("UPDATE stream_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE stream_id=$1").bind(expires_write.stream).execute(pool).await?;
    assert!(
        backend
            .prepare_tail(node, key, prepare(expires, 3))
            .await
            .is_err()
    );
    backend
        .write_status(node, key, lookup(expires), true)
        .await?;

    let mut abort = intent;
    abort.operation = Operation::Abort {
        upload,
        initiated: None,
    };
    backend
        .multipart_operation(
            node,
            key,
            Request {
                run,
                operation: Uuid::now_v7(),
                intent: abort,
            },
            None,
        )
        .await?;
    assert_eq!(queries::active_upload_count().fetch_one(pool).await?, 0);
    Ok(())
}
