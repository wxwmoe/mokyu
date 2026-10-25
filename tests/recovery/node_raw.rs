use super::{Backend, migration, queries};
use crate::{
    authorization::Principal,
    content::codec::{self, Key},
    model::{
        self, RuntimeSettingsStore,
        cluster::ClusterStore,
        node_raw as raw, node_write as write,
        session::{Open, Run},
    },
};
use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

fn begin(run: Run, name: &str) -> write::Begin {
    write::Begin {
        run,
        operation: Uuid::now_v7(),
        intent: write::Intent {
            access: "writer".into(),
            credential_revision: 1,
            copy_read: None,
            part: None,
            bucket: "batches".into(),
            key: name.into(),
            metadata: serde_json::json!({}),
            public: false,
            size: None,
        },
        cached: vec![],
    }
}

#[tokio::test]
#[ignore = "requires a new isolated PostgreSQL database"]
async fn postgres_node_raw_replays_deduplicates_and_fences_uploads() -> Result<()> {
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
    sqlx::query("INSERT INTO storage_profiles(storage_id,upload_concurrency,cdc_enabled,compression_enabled,pack_enabled,deduplication_enabled,write_processing,upload_cache) VALUES($1,1,false,false,false,true,'inline',false)")
        .bind(storage)
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
    let first = raw::Prepare {
        run: session.run,
        operation,
        input: raw::Input {
            size: 3,
            digest: hex::decode("900150983cd24fb0d6963f7d28e17f72")?
                .try_into()
                .unwrap(),
            hash: Some(*blake3::hash(b"abc").as_bytes()),
        },
    };
    let replies = futures_util::future::try_join_all(
        (0..8).map(|_| backend.prepare_raw(&node, &key, first.clone())),
    )
    .await?;
    let source = replies[0].source;
    assert!(replies.iter().all(|reply| reply.source == source
        && reply.size == 3
        && !reply.committed
        && reply.grants.len() == 1));
    let mut changed = first.clone();
    changed.input.size = 4;
    assert_eq!(
        model::error(
            &backend
                .prepare_raw(&node, &key, changed)
                .await
                .err()
                .unwrap()
        ),
        Some(model::Error::NodeOperationConflict)
    );
    let mut foreign = first.clone();
    foreign.run = peer_session.run;
    assert!(backend.prepare_raw(&node, &key, foreign).await.is_err());
    let commit = || raw::Commit {
        run: session.run,
        operation,
        source,
    };
    let publication = || write::Publish {
        run: session.run,
        operation,
        size: 3,
        etag: hex::encode(first.input.digest),
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
    assert!(
        backend
            .commit_raw(
                &node,
                &key,
                raw::Commit {
                    source: Uuid::now_v7(),
                    ..commit()
                }
            )
            .await
            .is_err()
    );

    let permit = replies[0].grants[0].id;
    let reference = || model::node_read::GrantRef {
        r#type: model::node_read::GrantType::StorageWrite,
        id: permit,
    };
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
                renew: vec![reference()],
                release: vec![],
            },
        )
        .await?;
    assert!(pulse.grants[0].accepted && pulse.grants[0].valid_for_ms > 50_000);
    let pulse = backend
        .pulse_session(
            &peer,
            &key,
            peer_session.run,
            peer_session.epoch,
            1,
            false,
            model::node_read::KeepAlive {
                renew: vec![reference()],
                release: vec![],
            },
        )
        .await?;
    assert!(!pulse.grants[0].accepted);
    let second = begin(session.run, "contender");
    let other = second.operation;
    backend.begin_write(&node, &key, second).await?;
    let mut pending = first.clone();
    pending.operation = other;
    assert_eq!(
        model::error(
            &backend
                .prepare_raw(&node, &key, pending.clone())
                .await
                .err()
                .unwrap()
        ),
        Some(model::Error::NodeWriteBusy)
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM raw_sources")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 1, "capacity rejection left an uncommitted source");

    assert!(backend.commit_raw(&node, &key, commit()).await?.committed);
    assert!(backend.commit_raw(&node, &key, commit()).await?.committed);
    assert!(
        backend
            .prepare_raw(&node, &key, first.clone())
            .await?
            .committed
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM storage_permits")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 0);
    let duplicate = backend.prepare_raw(&node, &key, pending.clone()).await?;
    assert!(duplicate.committed && duplicate.grants.is_empty());
    assert_eq!(
        duplicate.source, source,
        "deduplication allocated an upload target"
    );
    let mut bad = publication();
    bad.etag = "00000000000000000000000000000000".into();
    assert!(backend.publish_write(&node, &key, bad).await.is_err());
    assert_eq!(
        backend
            .publish_write(&node, &key, publication())
            .await?
            .phase,
        write::Phase::Published
    );
    assert_eq!(
        backend
            .publish_write(&node, &key, publication())
            .await?
            .phase,
        write::Phase::Published
    );
    assert!(backend.commit_raw(&node, &key, commit()).await?.committed);
    let stored: (String, i64, Uuid) = sqlx::query_as("SELECT s.payload_type,s.size,r.source_id FROM streams s JOIN raw_ranges r ON r.stream_id=s.id WHERE s.id=$1").bind(stream).fetch_one(&pool).await?;
    assert_eq!(stored, ("raw".into(), 3, source));
    backend
        .write_status(
            &node,
            &key,
            write::Lookup {
                run: session.run,
                operation: other,
            },
            true,
        )
        .await?;

    // Empty raw objects retain a physical identity and can be published and reused.
    let empty = begin(session.run, "empty");
    let empty_id = empty.operation;
    backend.begin_write(&node, &key, empty).await?;
    let request = raw::Prepare {
        operation: empty_id,
        input: raw::Input {
            size: 0,
            digest: hex::decode("d41d8cd98f00b204e9800998ecf8427e")?
                .try_into()
                .unwrap(),
            hash: Some(*blake3::hash(b"").as_bytes()),
        },
        ..first.clone()
    };
    let plan = backend.prepare_raw(&node, &key, request.clone()).await?;
    backend
        .commit_raw(
            &node,
            &key,
            raw::Commit {
                run: session.run,
                operation: empty_id,
                source: plan.source,
            },
        )
        .await?;
    let mut publish = publication();
    publish.operation = empty_id;
    publish.size = 0;
    publish.etag = hex::encode(request.input.digest);
    backend.publish_write(&node, &key, publish).await?;

    // A request admitted without a limit must obey a subsequently tightened cap.
    sqlx::query("UPDATE storage_profiles SET upload_concurrency=NULL WHERE storage_id=$1")
        .bind(storage)
        .execute(&pool)
        .await?;
    let cancel = begin(session.run, "cancelled");
    let cancel_id = cancel.operation;
    let opened = backend.begin_write(&node, &key, cancel).await?;
    sqlx::query("UPDATE storage_profiles SET upload_concurrency=1 WHERE storage_id=$1")
        .bind(storage)
        .execute(&pool)
        .await?;
    let mut pending = first.clone();
    pending.operation = cancel_id;
    pending.input.digest = [9; 16];
    pending.input.hash = Some([9; 32]);
    let plan = backend.prepare_raw(&node, &key, pending.clone()).await?;
    assert_eq!(plan.grants.len(), 1);
    backend
        .write_status(
            &node,
            &key,
            write::Lookup {
                run: session.run,
                operation: cancel_id,
            },
            true,
        )
        .await?;
    assert!(
        backend
            .commit_raw(
                &node,
                &key,
                raw::Commit {
                    run: session.run,
                    operation: cancel_id,
                    source: plan.source
                }
            )
            .await
            .is_err()
    );
    assert!(backend.prepare_raw(&node, &key, pending).await.is_err());
    let retained: (Uuid, String) =
        sqlx::query_as("SELECT owner_stream,state FROM raw_sources WHERE id=$1")
            .bind(plan.source)
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        retained,
        (opened.stream.unwrap(), "writing".into()),
        "abort lost the tracked external upload"
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM storage_permits")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 0);
    let limited = begin(session.run, "limited");
    let limited_id = limited.operation;
    let opened = backend.begin_write(&node, &key, limited).await?;
    let mut limited = first.clone();
    limited.operation = limited_id;
    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM raw_sources")
        .fetch_one(&pool)
        .await?;
    sqlx::query("UPDATE quota_scopes SET logical_bytes_limit=used_bytes+2 WHERE scope='bucket' AND scope_id=$1").bind(bucket).execute(&pool).await?;
    assert_eq!(
        model::error(
            &backend
                .prepare_raw(&node, &key, limited.clone())
                .await
                .err()
                .unwrap()
        ),
        Some(model::Error::QuotaExceeded)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raw_sources")
            .fetch_one(&pool)
            .await?,
        before
    );
    sqlx::query(
        "UPDATE quota_scopes SET logical_bytes_limit=NULL WHERE scope='bucket' AND scope_id=$1",
    )
    .bind(bucket)
    .execute(&pool)
    .await?;
    sqlx::query("UPDATE s3_credentials SET enabled=false WHERE access_key_id='writer'")
        .execute(&pool)
        .await?;
    assert!(
        backend
            .prepare_raw(&node, &key, limited.clone())
            .await
            .is_err()
    );
    sqlx::query("UPDATE s3_credentials SET enabled=true WHERE access_key_id='writer'")
        .execute(&pool)
        .await?;
    // The original authorization is rechecked before even a deduplicated reference is attached.
    backend
        .write_status(
            &node,
            &key,
            write::Lookup {
                run: session.run,
                operation: limited_id,
            },
            true,
        )
        .await?;
    let state: (String, i64) = sqlx::query_as("SELECT state,size FROM streams WHERE id=$1")
        .bind(opened.stream.unwrap())
        .fetch_one(&pool)
        .await?;
    assert_eq!(state.1, 0);
    Ok(())
}
