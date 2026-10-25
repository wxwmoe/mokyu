use super::{Backend, migration, queries};
use crate::{
    authorization::Principal,
    content::codec::{self, Key},
    model::{
        self, RuntimeSettingsStore,
        cluster::ClusterStore,
        node_read as read, node_write as write,
        session::{Open, Run},
    },
};
use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

fn request(run: Run, operation: Uuid) -> write::Begin {
    write::Begin {
        run,
        operation,
        intent: write::Intent {
            access: "writer".into(),
            credential_revision: 1,
            copy_read: None,
            part: None,
            bucket: "writes".into(),
            key: "object".into(),
            metadata: serde_json::json!({"content_type":"text/plain"}),
            public: false,
            size: Some(10),
        },
        cached: Vec::new(),
    }
}

fn dated(milliseconds: i64) -> Uuid {
    let mut bytes = *Uuid::now_v7().as_bytes();
    bytes[..6].copy_from_slice(&(milliseconds as u64).to_be_bytes()[2..]);
    Uuid::from_bytes(bytes)
}

#[tokio::test]
#[ignore = "requires a new isolated PostgreSQL database"]
async fn postgres_node_writes_bind_intent_quota_and_run_ownership() -> Result<()> {
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
    let worker = backend
        .provision_node(&admin, "worker", "worker", &key)
        .await?;
    let hello = || Open {
        boot: Uuid::new_v4(),
        connection: Uuid::new_v4(),
        previous_epoch: 0,
    };
    let session = backend.open_session(&node, &key, hello()).await?;
    let peer_session = backend.open_session(&peer, &key, hello()).await?;
    let worker_session = backend.open_session(&worker, &key, hello()).await?;
    let storage = Uuid::now_v7();
    sqlx::query("INSERT INTO storages(id,name,type,endpoint_url,remote_bucket_name,region,access_key_encrypted,secret_key_encrypted,identity_marker) VALUES($1,'target','s3','http://backend:8333','write-fixture','us-east-1',$2,$3,$4)")
        .bind(storage).bind(codec::protect(b"source-access", &key, format!("storage-{storage}-access").as_bytes())?)
        .bind(codec::protect(b"source-secret", &key, format!("storage-{storage}-secret").as_bytes())?)
        .bind(crate::storage::model::identity_marker(node.deployment_id, storage)?).execute(&pool).await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id) VALUES($1)")
        .bind(storage)
        .execute(&pool)
        .await?;
    let block_key = Uuid::now_v7();
    sqlx::query("INSERT INTO managed_keys(id,name,purpose,key_algorithm,secret_encrypted,fingerprint) VALUES($1,'writer-key','block','aes-256-gcm',$2,$3)")
        .bind(block_key).bind(codec::protect(&[53; 32], &key, block_key.as_bytes())?).bind(vec![53u8; 32]).execute(&pool).await?;
    sqlx::query("UPDATE storages SET block_key_id=$2 WHERE id=$1")
        .bind(storage)
        .bind(block_key)
        .execute(&pool)
        .await?;
    let bucket: Uuid =
        sqlx::query_scalar("INSERT INTO buckets(name,storage_id) VALUES('writes',$1) RETURNING id")
            .bind(storage)
            .fetch_one(&pool)
            .await?;
    sqlx::query("INSERT INTO s3_credentials(access_key_id,secret_encrypted) VALUES('writer',$1)")
        .bind(codec::protect(b"writer-secret", &key, b"writer")?)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO s3_credential_grants(access_key,bucket_id,actions) VALUES('writer',$1,ARRAY['object.write'])").bind(bucket).execute(&pool).await?;
    sqlx::query("UPDATE quota_scopes SET logical_bytes_limit=100,inflight_bytes_limit=100 WHERE scope='bucket' AND scope_id=$1").bind(bucket).execute(&pool).await?;
    let prepare = |access: &str, cached| model::node_request::Prepare {
        run: session.run,
        access: Some(access.into()),
        route: Some(model::ReadRoute::Bucket {
            name: "writes".into(),
            key: String::new(),
            alias: None,
        }),
        include: vec![
            read::MaterialType::Credential,
            read::MaterialType::Bucket,
            read::MaterialType::Policy,
        ],
        cached,
        query: None,
    };
    let preflight = backend
        .prepare_request(&node, &key, prepare("writer", vec![]))
        .await?
        .context;
    assert!(preflight.outcome == read::Outcome::Ready && preflight.grants.is_empty());
    assert_eq!(preflight.materials.len(), 3);
    assert!(preflight.mapping.is_none() && preflight.object.is_none());
    let cached = preflight
        .materials
        .iter()
        .map(|value| value.version.clone())
        .collect();
    let repeated = backend
        .prepare_request(&node, &key, prepare("writer", cached))
        .await?
        .context;
    assert!(repeated.materials.iter().all(|value| value.json.is_none()));
    sqlx::query(
        "INSERT INTO s3_credentials(access_key_id,secret_encrypted) VALUES('rotating-writer',$1)",
    )
    .bind(codec::protect(b"first-secret", &key, b"rotating-writer")?)
    .execute(&pool)
    .await?;
    sqlx::query("INSERT INTO s3_credential_grants(access_key,bucket_id,actions) VALUES('rotating-writer',$1,ARRAY['object.write'])").bind(bucket).execute(&pool).await?;
    let original = backend
        .prepare_request(&node, &key, prepare("rotating-writer", vec![]))
        .await?
        .context;
    sqlx::query("UPDATE s3_credentials SET secret_encrypted=$1,authorization_revision=authorization_revision+1 WHERE access_key_id='rotating-writer'")
        .bind(codec::protect(b"second-secret", &key, b"rotating-writer")?).execute(&pool).await?;
    let refreshed = backend
        .prepare_request(
            &node,
            &key,
            prepare(
                "rotating-writer",
                original
                    .materials
                    .iter()
                    .map(|value| value.version.clone())
                    .collect(),
            ),
        )
        .await?
        .context;
    let credential: read::Credential = serde_json::from_slice(
        refreshed
            .materials
            .iter()
            .find(|value| value.version.r#type == read::MaterialType::Credential)
            .unwrap()
            .json
            .as_deref()
            .unwrap(),
    )?;
    assert_eq!(credential.revision, 2);
    assert_eq!(credential.secret, "second-secret");
    let mut stale = request(session.run, Uuid::now_v7());
    stale.intent.access = "rotating-writer".into();
    let error = backend.begin_write(&node, &key, stale).await.err().unwrap();
    assert_eq!(
        error.downcast_ref::<s3s::S3Error>().unwrap().code(),
        &s3s::S3ErrorCode::AccessDenied
    );
    let unchanged: (i64, i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM streams),(SELECT count(*) FROM objects),(SELECT count(*) FROM stream_leases),(SELECT count(*) FROM quota_reservations)").fetch_one(&pool).await?;
    assert_eq!(unchanged, (0, 0, 0, 0));
    let operation = Uuid::now_v7();
    let mut invalid = request(session.run, operation);
    invalid.intent.public = true;
    assert!(backend.begin_write(&node, &key, invalid).await.is_err());
    let mut too_large = request(session.run, operation);
    too_large.intent.size = Some(101);
    assert_eq!(
        model::error(
            &backend
                .begin_write(&node, &key, too_large)
                .await
                .err()
                .unwrap()
        ),
        Some(model::Error::QuotaExceeded)
    );
    assert!(
        backend
            .begin_write(&worker, &key, request(worker_session.run, operation))
            .await
            .is_err()
    );
    sqlx::query("UPDATE mokyu_meta SET maintenance=true")
        .execute(&pool)
        .await?;
    assert!(
        backend
            .begin_write(&node, &key, request(session.run, operation))
            .await
            .is_err()
    );
    sqlx::query("UPDATE mokyu_meta SET maintenance=false")
        .execute(&pool)
        .await?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM streams")
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        count, 0,
        "failed admission must not leave streams, claims or reservations"
    );

    let first = backend
        .begin_write(&node, &key, request(session.run, operation))
        .await?;
    assert_eq!(first.phase, write::Phase::Active);
    assert_eq!(first.materials.len(), 1);
    let stream = first.stream.unwrap();
    sqlx::query("UPDATE storages SET block_key_id=NULL WHERE id=$1")
        .bind(storage)
        .execute(&pool)
        .await?;
    assert!(
        queries::managed_key_in_use(block_key)
            .fetch_one(&pool)
            .await?
    );
    let lease = first.grants[0].id;
    let quota: (i64, i64) = sqlx::query_as("SELECT reserved_bytes,inflight_bytes FROM quota_scopes WHERE scope='bucket' AND scope_id=$1").bind(bucket).fetch_one(&pool).await?;
    assert_eq!(quota, (10, 10));
    let mut replay = request(session.run, operation);
    replay.cached = first.materials.iter().map(|m| m.version.clone()).collect();
    let repeated = backend.begin_write(&node, &key, replay).await?;
    assert_eq!(repeated.stream, Some(stream));
    assert_eq!(repeated.grants[0].id, lease);
    assert!(repeated.materials[0].json.is_none());
    let parallel = futures_util::future::join_all(
        (0..8).map(|_| backend.begin_write(&node, &key, request(session.run, operation))),
    )
    .await;
    assert!(
        parallel
            .into_iter()
            .all(|r| r.is_ok_and(|r| r.stream == Some(stream)))
    );
    let mut conflict = request(session.run, operation);
    conflict.intent.size = Some(11);
    assert_eq!(
        model::error(
            &backend
                .begin_write(&node, &key, conflict)
                .await
                .err()
                .unwrap()
        ),
        Some(model::Error::NodeOperationConflict)
    );
    sqlx::query(
        "UPDATE s3_credential_grants SET actions=ARRAY['object.read'] WHERE access_key='writer'",
    )
    .execute(&pool)
    .await?;
    assert!(
        backend
            .begin_write(&node, &key, request(session.run, operation))
            .await
            .is_err()
    );
    sqlx::query(
        "UPDATE s3_credential_grants SET actions=ARRAY['object.write'] WHERE access_key='writer'",
    )
    .execute(&pool)
    .await?;
    let lookup = || write::Lookup {
        run: session.run,
        operation,
    };
    let status = backend.write_status(&node, &key, lookup(), false).await?;
    assert_eq!(status.phase, write::Phase::Active);
    assert!(status.materials.is_empty() && status.grants.is_empty());
    assert!(
        backend
            .write_status(
                &peer,
                &key,
                write::Lookup {
                    run: peer_session.run,
                    operation
                },
                false
            )
            .await
            .is_err()
    );
    let reference = || read::GrantRef {
        r#type: read::GrantType::Write,
        id: lease,
    };
    let pulse = backend
        .pulse_session(
            &peer,
            &key,
            peer_session.run,
            peer_session.epoch,
            1,
            false,
            read::KeepAlive {
                renew: vec![reference()],
                release: vec![],
            },
        )
        .await?;
    assert!(!pulse.grants[0].accepted);
    sqlx::query(
        "UPDATE stream_leases SET expires_at=clock_timestamp()+interval '5 seconds' WHERE id=$1",
    )
    .bind(lease)
    .execute(&pool)
    .await?;
    let pulse = backend
        .pulse_session(
            &node,
            &key,
            session.run,
            session.epoch,
            1,
            false,
            read::KeepAlive {
                renew: vec![reference()],
                release: vec![],
            },
        )
        .await?;
    assert!(pulse.grants[0].accepted && pulse.grants[0].valid_for_ms > 80_000);
    let aborted = backend.write_status(&node, &key, lookup(), true).await?;
    assert_eq!(aborted.phase, write::Phase::Aborted);
    assert!(
        !queries::managed_key_in_use(block_key)
            .fetch_one(&pool)
            .await?
    );
    assert_eq!(
        backend
            .write_status(&node, &key, lookup(), true)
            .await?
            .phase,
        write::Phase::Aborted
    );
    assert_eq!(
        backend
            .begin_write(&node, &key, request(session.run, operation))
            .await?
            .phase,
        write::Phase::Aborted
    );
    let quota: (i64, i64) = sqlx::query_as("SELECT reserved_bytes,inflight_bytes FROM quota_scopes WHERE scope='bucket' AND scope_id=$1").bind(bucket).fetch_one(&pool).await?;
    assert_eq!(quota, (0, 0));
    let state: String = sqlx::query_scalar("SELECT state FROM streams WHERE id=$1")
        .bind(stream)
        .fetch_one(&pool)
        .await?;
    assert_eq!(state, "abandoned");
    sqlx::query("DELETE FROM objects WHERE write_token=$1")
        .bind(stream)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM streams WHERE id=$1")
        .bind(stream)
        .execute(&pool)
        .await?;
    assert!(
        backend
            .write_status(&node, &key, lookup(), false)
            .await?
            .stream
            .is_none()
    );
    assert_eq!(
        backend
            .begin_write(&node, &key, request(session.run, operation))
            .await?
            .phase,
        write::Phase::Aborted
    );

    let stale = dated(chrono::Utc::now().timestamp_millis() - 2 * 86_400_000);
    sqlx::query("UPDATE node_writes SET operation_id=$2,created_at=clock_timestamp()-interval '2 days',completed_at=clock_timestamp()-interval '2 days' WHERE operation_id=$1").bind(operation).bind(stale).execute(&pool).await?;
    let revision = queries::node_write_credential("writer")
        .fetch_one(&pool)
        .await?;
    let request = |run, operation| {
        let mut input = request(run, operation);
        input.intent.credential_revision = revision;
        input
    };
    let second_id = Uuid::now_v7();
    let second = backend
        .begin_write(&node, &key, request(session.run, second_id))
        .await?;
    for id in [stale, dated(chrono::Utc::now().timestamp_millis() + 60_000)] {
        assert_eq!(
            model::error(
                &backend
                    .begin_write(&node, &key, request(session.run, id))
                    .await
                    .err()
                    .unwrap()
            ),
            Some(model::Error::NodeOperationExpired)
        );
    }
    assert!(
        backend
            .write_status(
                &node,
                &key,
                write::Lookup {
                    run: session.run,
                    operation: stale
                },
                false
            )
            .await
            .is_err()
    );
    let release = read::GrantRef {
        r#type: read::GrantType::Write,
        id: second.grants[0].id,
    };
    backend
        .pulse_session(
            &node,
            &key,
            session.run,
            session.epoch,
            2,
            false,
            read::KeepAlive {
                renew: vec![],
                release: vec![release],
            },
        )
        .await?;
    assert_eq!(
        backend
            .write_status(
                &node,
                &key,
                write::Lookup {
                    run: session.run,
                    operation: second_id
                },
                false
            )
            .await?
            .phase,
        write::Phase::Aborted
    );

    let third_id = Uuid::now_v7();
    let third = backend
        .begin_write(&node, &key, request(session.run, third_id))
        .await?;
    sqlx::query(
        "UPDATE stream_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(third.grants[0].id)
    .execute(&pool)
    .await?;
    let pulse = backend
        .pulse_session(
            &node,
            &key,
            session.run,
            session.epoch,
            3,
            false,
            read::KeepAlive {
                renew: vec![read::GrantRef {
                    r#type: read::GrantType::Write,
                    id: third.grants[0].id,
                }],
                release: vec![],
            },
        )
        .await?;
    assert!(!pulse.grants[0].accepted);
    assert_eq!(
        backend
            .begin_write(&node, &key, request(session.run, third_id))
            .await?
            .phase,
        write::Phase::Expired
    );
    let mut original = request(session.run, Uuid::now_v7());
    original.intent.key = "superseded".into();
    let original_id = original.operation;
    let original = backend.begin_write(&node, &key, original).await?;
    let mut replacement = request(peer_session.run, Uuid::now_v7());
    replacement.intent.key = "superseded".into();
    let replacement_id = replacement.operation;
    let replacement = backend.begin_write(&peer, &key, replacement).await?;
    assert_eq!(
        backend
            .write_status(
                &node,
                &key,
                write::Lookup {
                    run: session.run,
                    operation: original_id
                },
                false
            )
            .await?
            .phase,
        write::Phase::Expired
    );
    let pulse = backend
        .pulse_session(
            &node,
            &key,
            session.run,
            session.epoch,
            4,
            false,
            read::KeepAlive {
                renew: vec![read::GrantRef {
                    r#type: read::GrantType::Write,
                    id: original.grants[0].id,
                }],
                release: vec![],
            },
        )
        .await?;
    assert!(!pulse.grants[0].accepted);
    backend
        .write_status(
            &node,
            &key,
            write::Lookup {
                run: session.run,
                operation: original_id,
            },
            true,
        )
        .await?;
    assert_eq!(
        backend
            .write_status(
                &peer,
                &key,
                write::Lookup {
                    run: peer_session.run,
                    operation: replacement_id
                },
                false
            )
            .await?
            .phase,
        write::Phase::Active
    );
    let token: Uuid = sqlx::query_scalar(
        "SELECT write_token FROM objects WHERE bucket_id=$1 AND object_key='superseded'",
    )
    .bind(bucket)
    .fetch_one(&pool)
    .await?;
    assert_eq!(Some(token), replacement.stream);
    backend
        .write_status(
            &peer,
            &key,
            write::Lookup {
                run: peer_session.run,
                operation: replacement_id,
            },
            true,
        )
        .await?;
    let mut left = request(session.run, Uuid::now_v7());
    left.intent.key = "quota-left".into();
    left.intent.size = Some(80);
    let left_id = left.operation;
    let mut right = request(peer_session.run, Uuid::now_v7());
    right.intent.key = "quota-right".into();
    right.intent.size = Some(80);
    let right_id = right.operation;
    let (left, right) = tokio::join!(
        backend.begin_write(&node, &key, left),
        backend.begin_write(&peer, &key, right)
    );
    assert_ne!(
        left.is_ok(),
        right.is_ok(),
        "concurrent admissions must share the quota limit"
    );
    for (result, owner, run, operation) in [
        (left, &node, session.run, left_id),
        (right, &peer, peer_session.run, right_id),
    ] {
        match result {
            Ok(_) => {
                backend
                    .write_status(owner, &key, write::Lookup { run, operation }, true)
                    .await?;
            }
            Err(error) => assert_eq!(model::error(&error), Some(model::Error::QuotaExceeded)),
        }
    }
    sqlx::query("UPDATE node_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE node_id=$1").bind(node.node_id).execute(&pool).await?;
    let next = backend.open_session(&node, &key, hello()).await?;
    assert!(
        backend
            .begin_write(&node, &key, request(next.run, third_id))
            .await
            .is_err()
    );
    assert!(
        backend
            .write_status(
                &node,
                &key,
                write::Lookup {
                    run: next.run,
                    operation: third_id
                },
                true
            )
            .await
            .is_err()
    );
    assert_eq!(
        backend
            .write_status(
                &node,
                &key,
                write::Lookup {
                    run: next.run,
                    operation: third_id
                },
                false
            )
            .await?
            .phase,
        write::Phase::Expired
    );
    Ok(())
}
