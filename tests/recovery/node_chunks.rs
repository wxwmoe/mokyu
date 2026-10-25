use super::{Backend, migration, queries};
use crate::{
    authorization::Principal,
    content::codec::{self, Key},
    model::{
        self, RuntimeSettingsStore,
        cluster::ClusterStore,
        key::ManagedKeyStore,
        node_chunk as chunk, node_write as write,
        session::{Open, Run},
    },
};
use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[path = "node_copy.rs"]
mod copy;

fn begin(run: Run, name: &str) -> write::Begin {
    write::Begin {
        run,
        operation: Uuid::now_v7(),
        intent: write::Intent {
            access: "writer".into(),
            credential_revision: 1,
            copy_read: None,
            part: None,
            completion: None,
            bucket: "batches".into(),
            key: name.into(),
            metadata: serde_json::json!({}),
            public: false,
            size: None,
        },
        cached: vec![],
    }
}

fn item(value: &[u8]) -> chunk::Input {
    let hash = *blake3::hash(value).as_bytes();
    chunk::Input {
        hash,
        raw_size: value.len() as i32,
        payload_hash: hash,
        payload_size: value.len() as i32,
        compressed: false,
    }
}

fn completion(run: Run, operation: Uuid, batch: i64, values: &[chunk::Item]) -> chunk::Commit {
    let mut seen = std::collections::HashSet::new();
    chunk::Commit {
        run,
        operation,
        batch,
        encoded: values
            .iter()
            .filter(|value| !value.reused && seen.insert(value.chunk.uuid))
            .map(|value| chunk::Encoded {
                chunk: value.chunk.uuid,
                digest: [7; 32],
            })
            .collect(),
    }
}

#[tokio::test]
#[ignore = "requires a new isolated PostgreSQL database"]
async fn postgres_node_chunks_replay_claims_nonces_and_publication() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(url.trim())
        .await?;
    assert!(queries::metadata_table().fetch_one(&pool).await?.is_none());
    {
        let mut owner = pool.acquire().await?;
        migration::run(&mut owner).await?;
    }
    sqlx::query("UPDATE mokyu_meta SET deployment_mode='cluster'")
        .execute(&pool)
        .await?;
    let backend = Backend::new(pool.clone(), pool.acquire().await?.detach(), false);
    let key = Key::new("aes-256-gcm", [73; 32])?;
    let config =
        toml::from_str::<crate::config::FileConfig>("[database]\ntype='postgres'\n")?.into_config();
    RuntimeSettingsStore::ensure(&backend, &model::runtime_defaults(&config)).await?;
    let control = backend
        .register(
            "control",
            "control",
            crate::cluster::identity::credential()?,
            &key,
        )
        .await?
        .unwrap();
    let admin = Principal::Node {
        id: control.node_id,
        role: "control".into(),
    };
    let node = backend.provision_node(&admin, "data", "data", &key).await?;
    let peer = backend.provision_node(&admin, "peer", "data", &key).await?;
    let hello = || Open {
        boot: Uuid::new_v4(),
        connection: Uuid::new_v4(),
        previous_epoch: 0,
    };
    let session = backend.open_session(&node, &key, hello()).await?;
    let peer_session = backend.open_session(&peer, &key, hello()).await?;
    let storage = Uuid::now_v7();
    sqlx::query("INSERT INTO storages(id,name,type,endpoint_url,remote_bucket_name,region,access_key_encrypted,secret_key_encrypted,identity_marker) VALUES($1,'target','s3','http://backend:8333','batch-fixture','us-east-1',$2,$3,$4)")
        .bind(storage).bind(codec::protect(b"source-access", &key, format!("storage-{storage}-access").as_bytes())?)
        .bind(codec::protect(b"source-secret", &key, format!("storage-{storage}-secret").as_bytes())?)
        .bind(crate::storage::model::identity_marker(node.deployment_id, storage)?).execute(&pool).await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id,upload_concurrency) VALUES($1,4)")
        .bind(storage)
        .execute(&pool)
        .await?;
    let block_key = Uuid::now_v7();
    backend
        .create(
            &admin,
            model::key::ManagedKeyCreate {
                id: block_key,
                name: "batch-key".into(),
                purpose: "block".into(),
                algorithm: "aes-256-gcm".into(),
                protected: codec::protect(
                    &[53; 32],
                    &key,
                    format!("managed-key-{block_key}").as_bytes(),
                )?,
                fingerprint: *blake3::hash(&[53; 32]).as_bytes(),
            },
        )
        .await?;
    sqlx::query("UPDATE storages SET block_key_id=$2 WHERE id=$1")
        .bind(storage)
        .bind(block_key)
        .execute(&pool)
        .await?;
    let bucket: Uuid = sqlx::query_scalar(
        "INSERT INTO buckets(name,storage_id) VALUES('batches',$1) RETURNING id",
    )
    .bind(storage)
    .fetch_one(&pool)
    .await?;
    sqlx::query("INSERT INTO s3_credentials(access_key_id,secret_encrypted) VALUES('writer',$1)")
        .bind(codec::protect(b"writer-secret", &key, b"writer")?)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO s3_credential_grants(access_key,bucket_id,actions) VALUES('writer',$1,ARRAY['object.write'])").bind(bucket).execute(&pool).await?;
    let admitted = begin(session.run, "result");
    let operation = admitted.operation;
    let opened = backend.begin_write(&node, &key, admitted).await?;
    let stream = opened.stream.unwrap();
    let first = chunk::Prepare {
        run: session.run,
        operation,
        batch: 1,
        offset: 0,
        items: vec![item(b"abc"), item(b"abc")],
        parallelism: 2,
        cached: vec![],
    };
    let requests = (0..8).map(|_| backend.prepare_chunks(&node, &key, first.clone()));
    let mut replies = futures_util::future::try_join_all(requests).await?;
    let allocated = replies.pop().unwrap();
    assert!(!allocated.ready && !allocated.committed);
    assert_eq!(allocated.items[0].chunk.uuid, allocated.items[1].chunk.uuid);
    for reply in replies {
        assert_eq!(reply.items[0].chunk.uuid, allocated.items[0].chunk.uuid);
    }
    let contender = begin(peer_session.run, "contender");
    let other = contender.operation;
    backend.begin_write(&peer, &key, contender).await?;
    let blocked = chunk::Prepare {
        run: peer_session.run,
        operation: other,
        batch: 1,
        offset: 0,
        items: vec![item(b"unique"), item(b"abc")],
        parallelism: 2,
        cached: vec![],
    };
    assert_eq!(
        model::error(
            &backend
                .prepare_chunks(&peer, &key, blocked)
                .await
                .err()
                .unwrap()
        ),
        Some(model::Error::ChunkBusy)
    );
    let pending: (i64, i64) = sqlx::query_as(
        "SELECT next_batch,next_offset FROM node_writes WHERE node_id=$1 AND operation_id=$2",
    )
    .bind(peer.node_id)
    .bind(other)
    .fetch_one(&pool)
    .await?;
    assert_eq!(pending, (1, 0));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM chunks WHERE hash=$1")
        .bind(blake3::hash(b"unique").as_bytes().as_slice())
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 0, "a busy batch must roll back every new claim");
    backend
        .write_status(
            &peer,
            &key,
            write::Lookup {
                run: peer_session.run,
                operation: other,
            },
            true,
        )
        .await?;

    let (start, _) = backend
        .reserve_nonce(block_key, crate::content::nonce::CHUNK, 64)
        .await?;
    let nonce = |id| chunk::Nonce {
        chunk: allocated.items[0].chunk.uuid,
        value: codec::typed_nonce(
            allocated.items[0].chunk.created_at,
            id,
            crate::content::nonce::CHUNK,
        )
        .unwrap()
        .to_vec(),
    };
    let (left, right) = tokio::join!(
        backend.activate_chunks(&node, &key, first.clone(), vec![nonce(start)]),
        backend.activate_chunks(&node, &key, first.clone(), vec![nonce(start + 1)])
    );
    let left = left?;
    let right = right?;
    assert_eq!(left.items[0].chunk.nonce, right.items[0].chunk.nonce);
    assert_eq!(left.items[0].chunk.nonce, left.items[1].chunk.nonce);
    assert_eq!(
        left.grants
            .iter()
            .filter(|grant| grant.r#type == model::node_read::GrantType::StorageWrite)
            .count(),
        1
    );
    let permit = left
        .grants
        .iter()
        .find(|grant| grant.r#type == model::node_read::GrantType::StorageWrite)
        .unwrap()
        .id;
    let grant = || model::node_read::GrantRef {
        r#type: model::node_read::GrantType::StorageWrite,
        id: permit,
    };
    let pulse = backend
        .pulse_session(
            &peer,
            &key,
            peer_session.run,
            peer_session.epoch,
            1,
            false,
            model::node_read::KeepAlive {
                renew: vec![grant()],
                release: vec![],
            },
        )
        .await?;
    assert!(!pulse.grants[0].accepted);
    sqlx::query("UPDATE storage_permits SET expires_at=clock_timestamp()+interval '5 seconds' WHERE token=$1").bind(permit).execute(&pool).await?;
    let pulse = backend
        .pulse_session(
            &node,
            &key,
            session.run,
            session.epoch,
            1,
            false,
            model::node_read::KeepAlive {
                renew: vec![grant()],
                release: vec![],
            },
        )
        .await?;
    assert!(pulse.grants[0].accepted && pulse.grants[0].valid_for_ms > 50_000);
    sqlx::query("UPDATE storage_permits SET expires_at=clock_timestamp()-interval '1 second' WHERE token=$1").bind(permit).execute(&pool).await?;
    let pulse = backend
        .pulse_session(
            &node,
            &key,
            session.run,
            session.epoch,
            2,
            false,
            model::node_read::KeepAlive {
                renew: vec![grant()],
                release: vec![],
            },
        )
        .await?;
    assert!(!pulse.grants[0].accepted);
    let reacquired = backend
        .activate_chunks(&node, &key, first.clone(), vec![])
        .await?;
    assert!(reacquired.grants.iter().any(|grant| grant.r#type
        == model::node_read::GrantType::StorageWrite
        && grant.id != permit));
    assert_eq!(reacquired.items[0].chunk.nonce, left.items[0].chunk.nonce);
    let second = chunk::Prepare {
        run: session.run,
        operation,
        batch: 2,
        offset: 6,
        items: vec![item(b"def")],
        parallelism: 1,
        cached: vec![],
    };
    let assembly = backend.prepare_chunks(&node, &key, second.clone()).await?;
    let next = &assembly.items[0].chunk;
    let prepared = backend
        .activate_chunks(
            &node,
            &key,
            second.clone(),
            vec![chunk::Nonce {
                chunk: next.uuid,
                value: codec::typed_nonce(
                    next.created_at,
                    start + 2,
                    crate::content::nonce::CHUNK,
                )?
                .to_vec(),
            }],
        )
        .await?;
    assert_eq!(
        backend
            .commit_chunks(
                &node,
                &key,
                completion(session.run, operation, 2, &prepared.items)
            )
            .await?,
        9
    );
    let publication = || write::Publish {
        run: session.run,
        operation,
        size: 9,
        etag: "e80b5017098950fc58aad83c8c14978e".into(),
        checksums: Default::default(),
        if_match: None,
        if_none: None,
    };
    assert!(
        backend
            .publish_write(&node, &key, publication())
            .await
            .is_err()
    );
    assert_eq!(
        backend
            .commit_chunks(
                &node,
                &key,
                completion(session.run, operation, 1, &left.items)
            )
            .await?,
        6
    );
    assert_eq!(
        backend
            .commit_chunks(
                &node,
                &key,
                completion(session.run, operation, 1, &left.items)
            )
            .await?,
        6
    );
    let mut changed = completion(session.run, operation, 1, &left.items);
    changed.encoded[0].digest = [8; 32];
    assert_eq!(
        model::error(
            &backend
                .commit_chunks(&node, &key, changed)
                .await
                .err()
                .unwrap()
        ),
        Some(model::Error::NodeOperationConflict)
    );
    let counts: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT size FROM streams WHERE id=$1),count(*),count(DISTINCT chunk_id) FROM extents WHERE stream_id=$1").bind(stream).fetch_one(&pool).await?;
    assert_eq!(counts, (9, 3, 2));
    let receipts: (i64, i64) = sqlx::query_as("SELECT count(*),sum(jsonb_array_length(plan))::bigint FROM node_write_batches WHERE node_id=$1 AND operation_id=$2 AND commit_hash IS NOT NULL").bind(node.node_id).bind(operation).fetch_one(&pool).await?;
    assert_eq!(receipts, (2, 0));
    let reused = backend.prepare_chunks(&node, &key, first.clone()).await?;
    assert!(reused.committed && reused.items.is_empty());
    let result = backend.publish_write(&node, &key, publication()).await?;
    assert_eq!(result.phase, write::Phase::Published);
    let replay = backend.publish_write(&node, &key, publication()).await?;
    assert_eq!(
        result.published.unwrap().modified,
        replay.published.unwrap().modified
    );
    assert_eq!(
        backend
            .write_status(
                &node,
                &key,
                write::Lookup {
                    run: session.run,
                    operation
                },
                true
            )
            .await?
            .phase,
        write::Phase::Published
    );
    let retained: Option<Uuid> = sqlx::query_scalar(
        "SELECT stream_id FROM objects WHERE bucket_id=$1 AND object_key='result'",
    )
    .bind(bucket)
    .fetch_one(&pool)
    .await?;
    assert_eq!(retained, Some(stream));
    let permits: i64 = sqlx::query_scalar("SELECT count(*) FROM node_write_permits")
        .fetch_one(&pool)
        .await?;
    assert_eq!(permits, 0);

    for admitted in [Some(4), None] {
        sqlx::query("UPDATE storage_profiles SET upload_concurrency=$2,revision=revision+1 WHERE storage_id=$1")
            .bind(storage)
            .bind(admitted)
            .execute(&pool)
            .await?;
        let input = begin(session.run, &format!("current-limit-{admitted:?}"));
        let operation = input.operation;
        backend.begin_write(&node, &key, input).await?;
        sqlx::query("UPDATE storage_profiles SET upload_concurrency=1,revision=revision+1 WHERE storage_id=$1")
            .bind(storage)
            .execute(&pool)
            .await?;
        backend
            .prepare_chunks(
                &node,
                &key,
                chunk::Prepare {
                    run: session.run,
                    operation,
                    batch: 1,
                    offset: 0,
                    items: (0..2)
                        .map(|index| item(format!("current-limit-{admitted:?}-{index}").as_bytes()))
                        .collect(),
                    parallelism: 2,
                    cached: vec![],
                },
            )
            .await?;
        let count = || {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM node_write_permits WHERE node_id=$1 AND operation_id=$2",
            )
            .bind(node.node_id)
            .bind(operation)
        };
        assert_eq!(
            count().fetch_one(&pool).await?,
            1,
            "new batches exceeded the current storage limit"
        );
        backend
            .write_status(
                &node,
                &key,
                write::Lookup {
                    run: session.run,
                    operation,
                },
                true,
            )
            .await?;
        assert_eq!(count().fetch_one(&pool).await?, 0);
    }

    let empty = begin(session.run, "empty");
    let empty_id = empty.operation;
    backend.begin_write(&node, &key, empty).await?;
    let mut empty_publication = publication();
    empty_publication.operation = empty_id;
    empty_publication.size = 0;
    assert_eq!(
        backend
            .publish_write(&node, &key, empty_publication)
            .await?
            .phase,
        write::Phase::Published
    );
    copy::exercise(&backend, &pool, &node, &key, session.run).await?;
    Ok(())
}
