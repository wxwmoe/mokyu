use super::*;
use crate::model::{
    node_complete as complete, node_native as native, node_read as read, node_write as write,
};

pub(super) async fn check(
    backend: &Backend,
    peer: &Backend,
    node: &crate::cluster::NodeState,
    key: &Key,
    session: &model::session::Snapshot,
    storage: Uuid,
    revision: i64,
) -> Result<()> {
    let pool = &backend.pool;
    let run = session.run;
    sqlx::query("UPDATE storage_profiles SET cdc_enabled=false,compression_enabled=false,pack_enabled=false,pack_range_enabled=false,upload_cache=false,write_processing='inline',upload_concurrency=1,read_concurrency=1 WHERE storage_id=$1").bind(storage).execute(pool).await?;
    let intent = Intent {
        access: "writer".into(),
        credential_revision: revision,
        bucket: "multipart-control-a".into(),
        key: "native/body".into(),
        operation: Operation::Create {
            metadata: serde_json::json!({}),
            public: false,
            algorithm: None,
            checksum_type: None,
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
        panic!("missing native upload");
    };
    let id = Uuid::now_v7();
    let begin = |operation, completion: Option<complete::Manifest>| write::Begin {
        run,
        operation,
        intent: write::Intent {
            access: intent.access.clone(),
            credential_revision: revision,
            bucket: intent.bucket.clone(),
            key: intent.key.clone(),
            metadata: serde_json::json!({}),
            public: false,
            size: if completion.is_none() { Some(3) } else { None },
            copy_read: None,
            part: if completion.is_none() {
                Some(write::Part {
                    upload,
                    number: 1,
                    algorithm: None,
                })
            } else {
                None
            },
            completion,
        },
        cached: vec![],
    };
    let admitted = backend.begin_write(node, key, begin(id, None)).await?;
    let stream = admitted.stream.unwrap();
    let local = || native::LocalPart {
        run,
        operation: id,
        size: 3,
        hashes: vec![*blake3::hash(b"abc").as_bytes()],
    };
    backend.prepare_local_part(node, key, local()).await?;
    peer.prepare_local_part(node, key, local()).await?;
    let mut changed = local();
    changed.hashes[0][0] ^= 1;
    assert!(peer.prepare_local_part(node, key, changed).await.is_err());
    let etag = "900150983cd24fb0d6963f7d28e17f72";
    let publish = |operation, etag: &str| write::Publish {
        run,
        operation,
        size: 3,
        etag: etag.into(),
        checksums: Default::default(),
        if_match: None,
        if_none: None,
    };
    backend.publish_write(node, key, publish(id, etag)).await?;
    peer.publish_write(node, key, publish(id, etag)).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM extents WHERE stream_id=$1")
            .bind(stream)
            .fetch_one(pool)
            .await?,
        1
    );
    assert!(
        backend
            .check_local_files(
                node,
                key,
                native::Inventory {
                    run,
                    files: vec![stream],
                    recover: false,
                }
            )
            .await?
            .iter()
            .any(|file| file.id == stream
                && file.storage == Some(storage)
                && file.size == 3
                && file.state == crate::model::node_raw::FileState::Pending)
    );
    let claim = |operation| native::Claim {
        run,
        operation,
        stream: Some(stream),
        cached: vec![],
    };
    let initialize = Uuid::now_v7();
    let work = backend.claim_native(node, key, claim(initialize)).await?;
    assert!(matches!(
        work.work.as_ref().unwrap().action,
        native::Action::Initialize
    ));
    assert_eq!(
        peer.claim_native(node, key, claim(initialize))
            .await?
            .grants[0]
            .id,
        work.grants[0].id
    );
    let finish = |operation, value, retry| native::Finish {
        run,
        operation,
        result: native::ResultValue {
            value,
            retry,
            source_error: None,
        },
    };
    backend
        .finish_native(
            node,
            key,
            finish(initialize, Some("upstream-id".into()), false),
        )
        .await?;
    peer.finish_native(
        node,
        key,
        finish(initialize, Some("upstream-id".into()), false),
    )
    .await?;
    assert!(
        peer.finish_native(
            node,
            key,
            finish(initialize, Some("different-id".into()), false)
        )
        .await
        .is_err()
    );
    let first = Uuid::now_v7();
    let part = backend.claim_native(node, key, claim(first)).await?;
    assert!(
        matches!(part.work.as_ref().unwrap().action,native::Action::Part{file,..} if file==stream)
    );
    let sequence: i64 = sqlx::query_scalar("SELECT sequence FROM node_sessions WHERE node_id=$1")
        .bind(node.node_id)
        .fetch_one(pool)
        .await?;
    let renewed = peer
        .pulse_session(
            node,
            key,
            run,
            session.epoch,
            sequence + 1,
            false,
            read::KeepAlive {
                renew: part
                    .grants
                    .iter()
                    .map(|grant| read::GrantRef {
                        r#type: grant.r#type,
                        id: grant.id,
                    })
                    .collect(),
                release: vec![],
            },
        )
        .await?;
    assert!(renewed.grants.iter().all(|grant| grant.accepted));
    let mut fenced = finish(first, Some("part-etag".into()), false);
    fenced.run.fence = Uuid::new_v4();
    assert!(backend.finish_native(node, key, fenced).await.is_err());
    sqlx::query("UPDATE node_native_work SET expires_at=clock_timestamp()-interval '1 second' WHERE operation_id=$1").bind(first).execute(pool).await?;
    sqlx::query("UPDATE parts SET remote_expires_at=clock_timestamp()-interval '1 second' WHERE stream_id=$1").bind(stream).execute(pool).await?;
    sqlx::query("UPDATE storage_permits SET expires_at=clock_timestamp()-interval '1 second' WHERE token=$1").bind(part.grants[0].id).execute(pool).await?;
    let second = Uuid::now_v7();
    let retry = peer.claim_native(node, key, claim(second)).await?;
    assert_ne!(part.grants[0].id, retry.grants[0].id);
    assert!(
        backend
            .finish_native(node, key, finish(first, Some("old-etag".into()), false))
            .await
            .is_err()
    );
    let mut missing = finish(second, None, true);
    missing.result.source_error = Some(native::SourceError::Missing);
    peer.finish_native(node, key, missing).await?;
    assert!(
        backend
            .claim_native(node, key, claim(Uuid::now_v7()))
            .await
            .is_err()
    );
    assert!(sqlx::query_scalar::<_, bool>("SELECT remote_retry_at='infinity'::timestamptz AND remote_error='local_source_missing' FROM parts WHERE stream_id=$1")
        .bind(stream).fetch_one(pool).await?);
    peer.check_local_files(
        node,
        key,
        native::Inventory {
            run,
            files: vec![stream],
            recover: true,
        },
    )
    .await?;
    let restored = Uuid::now_v7();
    assert!(
        peer.claim_native(node, key, claim(restored))
            .await?
            .work
            .is_some()
    );
    peer.finish_native(node, key, finish(restored, Some("part-etag".into()), false))
        .await?;
    assert!(
        backend
            .claim_native(node, key, claim(Uuid::now_v7()))
            .await?
            .ready
    );
    let manifest = complete::Manifest {
        upload,
        parts: vec![complete::Part {
            number: 1,
            etag: etag.into(),
            checksums: Default::default(),
        }],
        if_match: None,
        if_none: None,
        checksum_type: None,
        size: None,
        checksums: Default::default(),
    };
    let completion = Uuid::now_v7();
    sqlx::query("UPDATE storage_profiles SET cdc_enabled=true WHERE storage_id=$1")
        .bind(storage)
        .execute(pool)
        .await?;
    assert!(
        backend
            .begin_write(node, key, begin(completion, Some(manifest.clone())))
            .await
            .is_err()
    );
    sqlx::query("UPDATE storage_profiles SET cdc_enabled=false WHERE storage_id=$1")
        .bind(storage)
        .execute(pool)
        .await?;
    let admitted = backend
        .begin_write(node, key, begin(completion, Some(manifest)))
        .await?;
    let summary = admitted.completion.unwrap();
    let prepare = || native::Complete {
        run,
        operation: completion,
        resend: false,
    };
    let plan = peer.prepare_native_complete(node, key, prepare()).await?;
    assert_eq!(
        plan.value.parts[0].remote_etag.as_deref(),
        Some("part-etag")
    );
    assert_eq!(plan.grants.len(), 2);
    let renewed = backend
        .pulse_session(
            node,
            key,
            run,
            session.epoch,
            sequence + 2,
            false,
            read::KeepAlive {
                renew: plan
                    .grants
                    .iter()
                    .map(|grant| read::GrantRef {
                        r#type: grant.r#type,
                        id: grant.id,
                    })
                    .collect(),
                release: vec![],
            },
        )
        .await?;
    assert!(renewed.grants.iter().all(|grant| grant.accepted));
    let adopt = || native::Adopt {
        run,
        operation: completion,
        digest: hex::decode(etag).unwrap().try_into().unwrap(),
        hash: *blake3::hash(b"abc").as_bytes(),
    };
    backend.adopt_native_complete(node, key, adopt()).await?;
    peer.adopt_native_complete(node, key, adopt()).await?;
    let mut changed = adopt();
    changed.digest[0] ^= 1;
    assert!(
        backend
            .adopt_native_complete(node, key, changed)
            .await
            .is_err()
    );
    let published = backend
        .publish_write(node, key, publish(completion, &summary.etag))
        .await?;
    assert_eq!(published.phase, write::Phase::Published);
    sqlx::query("UPDATE uploads SET completion_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1")
        .bind(upload).execute(pool).await?;
    let cleanup = Uuid::now_v7();
    let work = backend
        .claim_native(
            node,
            key,
            native::Claim {
                run,
                operation: cleanup,
                stream: None,
                cached: vec![],
            },
        )
        .await?;
    assert!(matches!(work.work.unwrap().action, native::Action::Abort));
    backend
        .finish_native(node, key, finish(cleanup, None, false))
        .await?;
    assert!(backend.remote_upload(upload).await?.is_none());
    sqlx::query("UPDATE storage_profiles SET cdc_enabled=true,upload_concurrency=NULL,read_concurrency=NULL WHERE storage_id=$1").bind(storage).execute(pool).await?;
    Ok(())
}
