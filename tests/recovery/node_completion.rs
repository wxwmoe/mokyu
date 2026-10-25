use super::*;
use crate::model::{
    node_chunk as chunk, node_complete as complete, node_read as read, node_tail as tail,
    node_write as write,
};

pub(super) async fn check(
    backend: &Backend,
    peer: &Backend,
    node: &crate::cluster::NodeState,
    key: &Key,
    session: &model::session::Snapshot,
    bucket: Uuid,
    revision: i64,
) -> Result<()> {
    let pool = &backend.pool;
    let run = session.run;
    let intent = Intent {
        access: "writer".into(),
        credential_revision: revision,
        bucket: "multipart-control-a".into(),
        key: "completion/body".into(),
        operation: Operation::Create {
            metadata: serde_json::json!({"content_type":"text/plain"}),
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
        panic!("upload missing")
    };
    let etag = "900150983cd24fb0d6963f7d28e17f72";
    let begin = |operation, completion, part| write::Begin {
        run,
        operation,
        cached: vec![],
        intent: write::Intent {
            access: intent.access.clone(),
            credential_revision: revision,
            bucket: intent.bucket.clone(),
            key: intent.key.clone(),
            metadata: serde_json::json!({}),
            public: false,
            size: None,
            copy_read: None,
            part,
            completion,
        },
    };
    let part = Uuid::now_v7();
    backend
        .begin_write(
            node,
            key,
            begin(
                part,
                None,
                Some(write::Part {
                    upload,
                    number: 1,
                    algorithm: None,
                }),
            ),
        )
        .await?;
    backend
        .prepare_tail(
            node,
            key,
            tail::Prepare {
                run,
                operation: part,
                offset: 0,
                size: 3,
                hash: *blake3::hash(b"abc").as_bytes(),
                cached: vec![],
            },
        )
        .await?;
    backend
        .commit_tail(
            node,
            key,
            write::Lookup {
                run,
                operation: part,
            },
        )
        .await?;
    backend
        .publish_write(
            node,
            key,
            write::Publish {
                run,
                operation: part,
                size: 3,
                etag: etag.into(),
                checksums: Default::default(),
                if_match: None,
                if_none: None,
            },
        )
        .await?;
    let manifest = complete::Manifest {
        upload,
        parts: vec![complete::Part {
            number: 1,
            etag: etag.into(),
            checksums: Default::default(),
        }],
        if_match: None,
        if_none: Some("*".into()),
        checksum_type: None,
        size: Some(3),
        checksums: Default::default(),
    };
    let mut wrong = manifest.clone();
    wrong.parts[0].etag = "00000000000000000000000000000000".into();
    denied(
        backend
            .begin_write(node, key, begin(Uuid::now_v7(), Some(wrong), None))
            .await
            .err()
            .unwrap(),
        s3s::S3ErrorCode::InvalidPart,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM node_completions WHERE upload_id=$1")
            .bind(upload)
            .fetch_one(pool)
            .await?,
        0
    );
    let id = Uuid::now_v7();
    let (a, b) = tokio::join!(
        backend.begin_write(node, key, begin(id, Some(manifest.clone()), None)),
        peer.begin_write(node, key, begin(id, Some(manifest.clone()), None))
    );
    let first = a?;
    assert_eq!(first.stream, b?.stream);
    assert_eq!(first.phase, write::Phase::Active);
    assert_eq!(first.completion.as_ref().unwrap().size, 3);
    let active = peer
        .begin_write(
            node,
            key,
            begin(Uuid::now_v7(), Some(manifest.clone()), None),
        )
        .await?;
    assert_eq!(active.phase, write::Phase::Busy);
    let mut changed = manifest.clone();
    changed.if_none = None;
    denied(
        backend
            .begin_write(
                node,
                key,
                begin(Uuid::now_v7(), Some(changed.clone()), None),
            )
            .await
            .err()
            .unwrap(),
        s3s::S3ErrorCode::InvalidRequest,
    );
    assert!(
        backend
            .begin_write(
                node,
                key,
                begin(
                    Uuid::now_v7(),
                    None,
                    Some(write::Part {
                        upload,
                        number: 1,
                        algorithm: None
                    })
                )
            )
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT sum(logical_bytes)::bigint FROM quota_reservations WHERE bucket_id=$1"
        )
        .bind(bucket)
        .fetch_one(pool)
        .await?,
        3
    );
    let read_id = Uuid::now_v7();
    sqlx::query("UPDATE storage_profiles SET read_concurrency=1 WHERE storage_id=(SELECT storage_id FROM buckets WHERE id=$1)").bind(bucket).execute(pool).await?;
    let sources = |operation, first| complete::Sources {
        run,
        operation,
        first,
        reads: vec![read_id],
        shared_permits: vec![],
        cached: vec![],
    };
    let source = backend.complete_sources(node, key, sources(id, 0)).await?;
    assert_eq!(source.first, 0);
    assert_eq!(source.next, None);
    assert_eq!(source.parts.len(), 1);
    assert_eq!(source.parts[0].length, 3);
    let replay = peer.complete_sources(node, key, sources(id, 0)).await?;
    assert_eq!(source.parts[0].grants[0].id, replay.parts[0].grants[0].id);
    let permit = source.parts[0]
        .grants
        .iter()
        .find(|grant| grant.r#type == read::GrantType::StorageRead)
        .unwrap()
        .id;
    let mut shared = sources(id, 0);
    shared.reads = vec![Uuid::now_v7()];
    shared.shared_permits = vec![permit];
    let second = peer.complete_sources(node, key, shared).await?;
    assert!(
        second.parts[0]
            .grants
            .iter()
            .any(|grant| grant.id == permit)
    );
    let mut forged = sources(id, 0);
    forged.shared_permits = vec![Uuid::new_v4()];
    assert!(backend.complete_sources(node, key, forged).await.is_err());
    backend
        .pulse_session(
            node,
            key,
            run,
            session.epoch,
            3,
            false,
            read::KeepAlive {
                renew: vec![],
                release: source.parts[0]
                    .grants
                    .iter()
                    .filter(|grant| grant.r#type != read::GrantType::StorageRead)
                    .map(|grant| read::GrantRef {
                        r#type: grant.r#type,
                        id: grant.id,
                    })
                    .collect(),
            },
        )
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM node_read_permits WHERE token=$1")
            .bind(permit)
            .fetch_one(pool)
            .await?,
        1
    );
    let shared = backend
        .pulse_session(
            node,
            key,
            run,
            session.epoch,
            4,
            false,
            read::KeepAlive {
                renew: vec![read::GrantRef {
                    r#type: read::GrantType::StorageRead,
                    id: permit,
                }],
                release: vec![],
            },
        )
        .await?;
    assert!(shared.grants[0].accepted);
    assert!(
        backend
            .complete_sources(node, key, sources(id, 1))
            .await
            .is_err()
    );
    assert!(
        backend
            .complete_sources(node, key, sources(Uuid::now_v7(), 0))
            .await
            .is_err()
    );
    let grants = || {
        first
            .grants
            .iter()
            .map(|g| read::GrantRef {
                r#type: g.r#type,
                id: g.id,
            })
            .collect()
    };
    sqlx::query("UPDATE uploads SET completion_expires_at=clock_timestamp()+interval '10 seconds' WHERE id=$1").bind(upload).execute(pool).await?;
    let pulse = backend
        .pulse_session(
            node,
            key,
            run,
            session.epoch,
            5,
            false,
            read::KeepAlive {
                renew: grants(),
                release: vec![],
            },
        )
        .await?;
    assert!(pulse.grants.iter().all(|g| g.accepted));
    assert!(sqlx::query_scalar::<_, bool>("SELECT completion_expires_at>clock_timestamp()+interval '100 seconds' FROM uploads WHERE id=$1").bind(upload).fetch_one(pool).await?);
    sqlx::query("UPDATE uploads SET completion_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1").bind(upload).execute(pool).await?;
    let status = || complete::Lookup {
        run,
        access: intent.access.clone(),
        revision,
        bucket: intent.bucket.clone(),
        key: intent.key.clone(),
        upload,
        hash: manifest.hash().unwrap(),
    };
    assert_eq!(
        backend.completion_status(node, key, status()).await?.0,
        write::Phase::Expired
    );
    denied(
        backend
            .begin_write(node, key, begin(Uuid::now_v7(), Some(changed), None))
            .await
            .err()
            .unwrap(),
        s3s::S3ErrorCode::InvalidRequest,
    );
    let takeover = Uuid::now_v7();
    let next = peer
        .begin_write(node, key, begin(takeover, Some(manifest.clone()), None))
        .await?;
    assert_ne!(first.stream, next.stream);
    assert!(
        backend
            .complete_sources(node, key, sources(id, 0))
            .await
            .is_err()
    );
    let pulse = backend
        .pulse_session(
            node,
            key,
            run,
            session.epoch,
            6,
            false,
            read::KeepAlive {
                renew: grants(),
                release: vec![],
            },
        )
        .await?;
    assert!(pulse.grants.iter().all(|g| !g.accepted));
    let hash = *blake3::hash(b"abc").as_bytes();
    let batch = chunk::Prepare {
        run,
        operation: takeover,
        batch: 1,
        offset: 0,
        items: vec![chunk::Input {
            hash,
            raw_size: 3,
            payload_hash: hash,
            payload_size: 3,
            compressed: false,
        }],
        parallelism: 1,
        cached: vec![],
    };
    backend.prepare_chunks(node, key, batch.clone()).await?;
    let prepared = backend.activate_chunks(node, key, batch, vec![]).await?;
    backend
        .commit_chunks(
            node,
            key,
            chunk::Commit {
                run,
                operation: takeover,
                batch: 1,
                encoded: prepared
                    .items
                    .iter()
                    .map(|item| chunk::Encoded {
                        chunk: item.chunk.uuid,
                        digest: [7; 32],
                    })
                    .collect(),
            },
        )
        .await?;
    let publish = |operation| write::Publish {
        run,
        operation,
        size: 3,
        etag: next.completion.as_ref().unwrap().etag.clone(),
        checksums: Default::default(),
        if_match: None,
        if_none: None,
    };
    assert!(backend.publish_write(node, key, publish(id)).await.is_err());
    let result = backend.publish_write(node, key, publish(takeover)).await?;
    assert_eq!(result.phase, write::Phase::Published);
    assert_eq!(
        result.completion.as_ref().unwrap().result,
        peer.publish_write(node, key, publish(takeover))
            .await?
            .completion
            .unwrap()
            .result
    );
    let repeated = peer
        .begin_write(
            node,
            key,
            begin(Uuid::now_v7(), Some(manifest.clone()), None),
        )
        .await?;
    assert_eq!(repeated.phase, write::Phase::Published);
    assert_eq!(
        repeated.completion.unwrap().result,
        result.completion.unwrap().result
    );
    assert_eq!(
        backend.completion_status(node, key, status()).await?.0,
        write::Phase::Published
    );
    assert_eq!(state(pool, upload).await?, "completed");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM parts WHERE upload_id=$1")
            .bind(upload)
            .fetch_one(pool)
            .await?,
        0
    );
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT coalesce(sum(logical_bytes),0)::bigint FROM quota_reservations WHERE bucket_id=$1").bind(bucket).fetch_one(pool).await?, 0);
    assert_eq!(queries::active_upload_count().fetch_one(pool).await?, 0);
    let sequence: i64 = sqlx::query_scalar("SELECT sequence FROM node_sessions WHERE node_id=$1")
        .bind(node.node_id)
        .fetch_one(pool)
        .await?;
    backend
        .pulse_session(
            node,
            key,
            run,
            session.epoch,
            sequence + 1,
            false,
            read::KeepAlive {
                renew: vec![],
                release: second.parts[0]
                    .grants
                    .iter()
                    .map(|grant| read::GrantRef {
                        r#type: grant.r#type,
                        id: grant.id,
                    })
                    .collect(),
            },
        )
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM storage_permits WHERE token=$1")
            .bind(permit)
            .fetch_one(pool)
            .await?,
        0
    );
    Ok(())
}
