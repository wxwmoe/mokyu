use super::{Backend, migration, queries};
use crate::{
    authorization::Principal,
    content::codec::{self, Key},
    model::{
        self, Id, RuntimeSettingsStore,
        cluster::ClusterStore,
        node_read as read,
        session::{Open, Run},
    },
};
use anyhow::{Context, Result};
use axum::http::HeaderMap;
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

#[path = "node_access.rs"]
mod access;

fn request(run: Run) -> read::Query {
    read::Query {
        run,
        operation: Uuid::new_v4(),
        route: model::ReadRoute::Bucket {
            name: "reads".into(),
            key: "object".into(),
            alias: None,
        },
        access: Some("reader".into()),
        head: false,
        web: false,
        mapping: true,
        protection: false,
        policy: false,
        headers: HeaderMap::new(),
        overrides: HeaderMap::new(),
        cached: Vec::new(),
        max_items: read::MAX_ITEMS,
        max_bytes: read::TARGET_BYTES,
        body: None,
    }
}

fn material<T: serde::de::DeserializeOwned>(
    reply: &read::Reply,
    r#type: read::MaterialType,
) -> Result<T> {
    Ok(serde_json::from_slice(
        reply
            .materials
            .iter()
            .find(|item| item.version.r#type == r#type)
            .and_then(|item| item.json.as_deref())
            .context("material missing")?,
    )?)
}

fn reference(reply: &read::Reply, r#type: read::GrantType) -> read::GrantRef {
    let grant = reply
        .grants
        .iter()
        .find(|grant| grant.r#type == r#type)
        .unwrap();
    read::GrantRef {
        r#type,
        id: grant.id,
    }
}

async fn active(pool: &PgPool) -> Result<(i64, i64, i64)> {
    Ok(sqlx::query_as("SELECT (SELECT count(*) FROM node_reads),(SELECT count(*) FROM node_read_windows),(SELECT count(*) FROM storage_permits)").fetch_one(pool).await?)
}

async fn publication_waits(
    pool: &PgPool,
    node: &crate::cluster::NodeState,
    key: &Key,
    run: Run,
    bucket: Uuid,
) -> Result<()> {
    for rpc in [false, true] {
        for commit in [false, true] {
            let name = format!("publication-{rpc}-{commit}");
            let mut streams = Vec::new();
            for state in ["ready", "writing"] {
                let stream: Uuid = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,etag,public_read,metadata) VALUES($1,$2,'object',$3,0,'empty',true,'{}') RETURNING id")
                    .bind(bucket).bind(&name).bind(state).fetch_one(pool).await?;
                streams.push(stream);
            }
            sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,$2,$3,uuidv7())")
                .bind(bucket).bind(&name).bind(streams[0]).execute(pool).await?;
            let mut writer = pool.begin().await?;
            let writer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *writer)
                .await?;
            queries::lock_object_stream(bucket, &name)
                .fetch_one(&mut *writer)
                .await?;
            sqlx::query("UPDATE streams SET state=CASE WHEN id=$1 THEN 'retired' ELSE 'ready' END WHERE id=ANY($2)")
                .bind(streams[0]).bind(&streams).execute(&mut *writer).await?;
            sqlx::query("UPDATE objects SET stream_id=$3 WHERE bucket_id=$1 AND object_key=$2")
                .bind(bucket)
                .bind(&name)
                .bind(streams[1])
                .execute(&mut *writer)
                .await?;
            let reader_pool = pool.clone();
            let reader_node = node.clone();
            let reader_key = key.clone();
            let reader = tokio::spawn(async move {
                if rpc {
                    let mut input = request(run);
                    input.route = model::ReadRoute::Bucket {
                        name: "reads".into(),
                        key: name,
                        alias: None,
                    };
                    input.head = true;
                    input.mapping = false;
                    let reply = super::transactions::open_node_read(
                        &reader_pool,
                        &reader_node,
                        &reader_key,
                        input,
                    )
                    .await?;
                    assert!(matches!(reply.outcome, read::Outcome::Ready));
                    Ok::<Id, anyhow::Error>(
                        material::<read::Object>(&reply, read::MaterialType::Object)?
                            .id
                            .into(),
                    )
                } else {
                    let (stream, lease) =
                        super::transactions::current_stream(&reader_pool, bucket, &name, 90)
                            .await?;
                    queries::release_read_lease(&reader_pool, lease).await?;
                    Ok(stream.id)
                }
            });
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))")
                        .bind(writer_pid).fetch_one(pool).await?;
                    if waiting { return Ok::<(), anyhow::Error>(()); }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            }).await??;
            if commit {
                writer.commit().await?;
            } else {
                writer.rollback().await?;
            }
            assert_eq!(
                tokio::time::timeout(std::time::Duration::from_secs(5), reader).await???,
                Id::from(streams[usize::from(commit)]),
            );
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a new isolated PostgreSQL database"]
async fn postgres_node_reads_close_dependencies_bound_windows_and_protect_sources() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = PgPoolOptions::new()
        .max_connections(6)
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
    let node = backend
        .provision_node(&admin, "combined", "node", &key)
        .await?;
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
    let peer_run = backend.open_session(&peer, &key, hello()).await?;
    let worker_run = backend.open_session(&worker, &key, hello()).await?;
    let storage = Uuid::now_v7();
    sqlx::query("INSERT INTO storages(id,name,type,endpoint_url,remote_bucket_name,region,access_key_encrypted,secret_key_encrypted,identity_marker) VALUES($1,'source','s3','http://backend:8333','read-fixture','us-east-1',$2,$3,$4)")
        .bind(storage).bind(codec::protect(b"source-access", &key, format!("storage-{storage}-access").as_bytes())?)
        .bind(codec::protect(b"source-secret", &key, format!("storage-{storage}-secret").as_bytes())?)
        .bind(crate::storage::model::identity_marker(node.deployment_id, storage)?).execute(&pool).await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id,read_concurrency) VALUES($1,2)")
        .bind(storage)
        .execute(&pool)
        .await?;
    let bucket: Uuid =
        sqlx::query_scalar("INSERT INTO buckets(name,storage_id) VALUES('reads',$1) RETURNING id")
            .bind(storage)
            .fetch_one(&pool)
            .await?;
    let stream: Uuid = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,etag,public_read,metadata) VALUES($1,'object','object','ready',4198400,'fixture',true,'{\"content_type\":\"application/octet-stream\"}') RETURNING id").bind(bucket).fetch_one(&pool).await?;
    sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,'object',$2,uuidv7())")
        .bind(bucket)
        .bind(stream)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO s3_credentials(access_key_id,secret_encrypted) VALUES('reader',$1)")
        .bind(codec::protect(b"reader-secret", &key, b"reader")?)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO s3_credential_grants(access_key,bucket_id,actions) VALUES('reader',$1,ARRAY['object.read'])").bind(bucket).execute(&pool).await?;
    publication_waits(&pool, &node, &key, session.run, bucket).await?;
    let observed: Uuid = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,etag) VALUES($1,'observed','object','ready',4096,'observed') RETURNING id")
        .bind(bucket).fetch_one(&pool).await?;
    let observations = || model::node_observation::Batch {
        run: session.run,
        compression: vec![
            model::CompressionObservation {
                stream: observed.into(),
                version: 1,
                encoding: "gzip",
                size: 100,
            },
            model::CompressionObservation {
                stream: observed.into(),
                version: 1,
                encoding: "br",
                size: 90,
            },
        ],
        credentials: vec!["reader".into()],
        access: None,
    };
    let mut forbidden = observations();
    forbidden.run = worker_run.run;
    assert!(
        backend
            .report_observations(&worker, &key, forbidden)
            .await
            .is_err()
    );
    let mut expired = observations();
    expired.run.fence = Uuid::new_v4();
    assert!(
        backend
            .report_observations(&node, &key, expired)
            .await
            .is_err()
    );
    sqlx::query("UPDATE mokyu_meta SET maintenance=true")
        .execute(&pool)
        .await?;
    assert!(
        backend
            .report_observations(&node, &key, observations())
            .await
            .is_err()
    );
    sqlx::query("UPDATE mokyu_meta SET maintenance=false")
        .execute(&pool)
        .await?;
    let mut invalid = observations();
    invalid.compression[1].size = -1;
    assert!(
        backend
            .report_observations(&node, &key, invalid)
            .await
            .is_err()
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT last_used_at IS NULL FROM s3_credentials WHERE access_key_id='reader'"
        )
        .fetch_one(&pool)
        .await?
    );
    assert_eq!(
        backend
            .report_observations(&node, &key, observations())
            .await?,
        [true, true]
    );
    let learned: serde_json::Value =
        sqlx::query_scalar("SELECT http_compression FROM streams WHERE id=$1")
            .bind(observed)
            .fetch_one(&pool)
            .await?;
    assert_eq!(learned, serde_json::json!({"version":1,"gzip":100,"br":90}));
    let used: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT last_used_at FROM s3_credentials WHERE access_key_id='reader'")
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        backend
            .report_observations(&node, &key, observations())
            .await?,
        [false, false]
    );
    assert_eq!(
        sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
            "SELECT last_used_at FROM s3_credentials WHERE access_key_id='reader'"
        )
        .fetch_one(&pool)
        .await?,
        used
    );
    sqlx::query("UPDATE streams SET state='retired' WHERE id=$1")
        .bind(observed)
        .execute(&pool)
        .await?;
    let mut retired = observations();
    retired.compression[0].version = 2;
    assert_eq!(
        backend.report_observations(&node, &key, retired).await?,
        [false, false]
    );
    sqlx::query("INSERT INTO chunks(storage_id,hash,raw_size,standalone_size,encryption_algorithm,state) SELECT $1,decode(md5(n::text)||md5(n::text),'hex'),4096,4096,'none','ready' FROM generate_series(1,1025) n").bind(storage).execute(&pool).await?;
    sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,standalone_size,standalone_compressed,state,created_at,encryption_algorithm,storage_id,encoding_fingerprint) SELECT current_encoding_id,id,4096,false,'ready',clock_timestamp(),'none',storage_id,hash FROM chunks").execute(&pool).await?;
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) SELECT $1,(row_number() OVER(ORDER BY id)-1)*4096,4096,id FROM chunks").bind(stream).execute(&pool).await?;
    access::verify(
        &backend,
        &pool,
        &key,
        (&node, session.run),
        (&peer, peer_run.run),
        (&worker, worker_run.run),
    )
    .await?;

    assert!(
        backend
            .open_read(&worker, &key, request(worker_run.run))
            .await
            .is_err()
    );
    let mut head = request(session.run);
    head.head = true;
    head.mapping = false;
    head.policy = true;
    let header = backend.open_read(&node, &key, head).await?;
    assert!(
        header.outcome == read::Outcome::Ready
            && header.grants.is_empty()
            && header.mapping.is_none()
    );
    assert_eq!(
        material::<read::Policy>(&header, read::MaterialType::Policy)?.storage_id,
        storage
    );
    let mut cached = request(session.run);
    cached.head = true;
    cached.mapping = false;
    cached.policy = true;
    cached.cached = header
        .materials
        .iter()
        .map(|item| item.version.clone())
        .collect();
    let cached = backend.open_read(&node, &key, cached).await?;
    assert!(cached.materials.iter().all(|item| item.json.is_none()));
    assert!(!header.materials.iter().any(|item| matches!(
        item.version.r#type,
        read::MaterialType::Storage | read::MaterialType::Key
    )));
    assert_eq!(active(&pool).await?, (0, 0, 0));
    let mut conditional = request(session.run);
    conditional
        .headers
        .insert("if-none-match", "\"fixture\"".parse()?);
    let conditional = backend.open_read(&node, &key, conditional).await?;
    assert!(conditional.outcome == read::Outcome::NotModified && conditional.grants.is_empty());
    let mut invalid = request(session.run);
    invalid.headers.insert("range", "bytes=99999999-".parse()?);
    assert!(backend.open_read(&node, &key, invalid).await?.outcome == read::Outcome::InvalidRange);
    let mut missing = request(session.run);
    missing.route = model::ReadRoute::Bucket {
        name: "missing".into(),
        key: "object".into(),
        alias: None,
    };
    let missing = backend.open_read(&node, &key, missing).await?;
    assert!(missing.outcome == read::Outcome::NoSuchBucket);
    assert_eq!(
        material::<read::Credential>(&missing, read::MaterialType::Credential)?.secret,
        "reader-secret"
    );
    assert_eq!(active(&pool).await?, (0, 0, 0));

    let mut input = request(session.run);
    input.max_items = 2;
    let operation = input.operation;
    let first = backend.open_read(&node, &key, input).await?;
    assert!(first.outcome == read::Outcome::Ready);
    let mapping: read::Mapping = material(&first, read::MaterialType::Mapping)?;
    assert_eq!(
        (mapping.start, mapping.end, mapping.segments.len()),
        (0, 8192, 2)
    );
    assert_eq!(
        material::<read::Storage>(&first, read::MaterialType::Storage)?
            .connection
            .secret
            .as_deref(),
        Some("source-secret")
    );
    assert_eq!(active(&pool).await?, (1, 1, 1));
    let mut retry = request(session.run);
    retry.operation = operation;
    retry.max_items = 2;
    retry.cached = first
        .materials
        .iter()
        .map(|item| item.version.clone())
        .collect();
    let replay = backend.open_read(&node, &key, retry).await?;
    assert!(replay.materials.iter().all(|item| item.json.is_none()));
    assert_eq!(
        reference(&first, read::GrantType::Read).id,
        reference(&replay, read::GrantType::Read).id
    );
    assert_eq!(active(&pool).await?, (1, 1, 1));
    let mut changed = request(session.run);
    changed.operation = operation;
    changed.headers.insert("range", "bytes=1-5".parse()?);
    assert!(matches!(
        backend
            .open_read(&node, &key, changed)
            .await
            .err()
            .as_ref()
            .and_then(model::error),
        Some(model::Error::NodeConflict)
    ));

    let window = |run: Run, cursor: read::Cursor| read::Window {
        run,
        cursor,
        cached: Vec::new(),
        max_items: 2,
        max_bytes: read::TARGET_BYTES,
    };
    assert!(
        backend
            .read_window(
                &peer,
                &key,
                window(peer_run.run, first.next.clone().unwrap())
            )
            .await
            .is_err()
    );
    let second = backend
        .read_window(
            &node,
            &key,
            window(session.run, first.next.clone().unwrap()),
        )
        .await?;
    assert_eq!(active(&pool).await?, (1, 2, 1));
    assert!(matches!(
        backend
            .read_window(
                &node,
                &key,
                window(session.run, second.next.clone().unwrap())
            )
            .await
            .err()
            .as_ref()
            .and_then(model::error),
        Some(model::Error::NodeReadBusy)
    ));
    sqlx::raw_sql(
        "CREATE TABLE release_statements(name text NOT NULL);
         CREATE FUNCTION record_release_statement() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO release_statements VALUES(TG_TABLE_NAME); RETURN NULL; END $$;
         CREATE TRIGGER release_statement AFTER DELETE ON storage_permits
         FOR EACH STATEMENT EXECUTE FUNCTION record_release_statement();
         CREATE TRIGGER release_statement AFTER DELETE ON node_read_windows
         FOR EACH STATEMENT EXECUTE FUNCTION record_release_statement();
         CREATE TRIGGER release_statement AFTER DELETE ON stream_leases
         FOR EACH STATEMENT EXECUTE FUNCTION record_release_statement();
         CREATE TRIGGER release_statement AFTER UPDATE ON node_native_work
         FOR EACH STATEMENT EXECUTE FUNCTION record_release_statement();
         CREATE TRIGGER release_statement AFTER UPDATE ON node_work
         FOR EACH STATEMENT EXECUTE FUNCTION record_release_statement();",
    )
    .execute(&pool)
    .await?;
    let renewed = backend
        .pulse_session(
            &node,
            &key,
            session.run,
            session.epoch,
            1,
            false,
            read::KeepAlive {
                renew: first
                    .grants
                    .iter()
                    .map(|grant| read::GrantRef {
                        r#type: grant.r#type,
                        id: grant.id,
                    })
                    .collect(),
                release: Vec::new(),
            },
        )
        .await?;
    assert!(renewed.grants.iter().all(|grant| grant.accepted));
    let statements: Vec<String> = sqlx::query_scalar("SELECT name FROM release_statements")
        .fetch_all(&pool)
        .await?;
    assert!(
        statements.is_empty(),
        "renewal ran release statements: {statements:?}"
    );
    let pulse = backend
        .pulse_session(
            &node,
            &key,
            session.run,
            session.epoch,
            2,
            false,
            read::KeepAlive {
                renew: first
                    .grants
                    .iter()
                    .filter(|g| g.r#type != read::GrantType::Window)
                    .map(|g| read::GrantRef {
                        r#type: g.r#type,
                        id: g.id,
                    })
                    .collect(),
                release: vec![reference(&first, read::GrantType::Window)],
            },
        )
        .await?;
    assert!(pulse.grants.iter().all(|grant| grant.accepted));
    let statements: Vec<String> = sqlx::query_scalar("SELECT name FROM release_statements")
        .fetch_all(&pool)
        .await?;
    assert_eq!(statements, ["node_read_windows"]);
    sqlx::raw_sql(
        "DROP FUNCTION record_release_statement() CASCADE; DROP TABLE release_statements",
    )
    .execute(&pool)
    .await?;
    let third = backend
        .read_window(
            &node,
            &key,
            window(session.run, second.next.clone().unwrap()),
        )
        .await?;
    assert_eq!(active(&pool).await?, (1, 2, 1));

    let mut tail = request(session.run);
    tail.headers
        .insert("range", "bytes=4194304-4198399".parse()?);
    let tail = backend.open_read(&node, &key, tail).await?;
    let mapping: read::Mapping = material(&tail, read::MaterialType::Mapping)?;
    assert_eq!(
        (mapping.start, mapping.end, mapping.segments.len()),
        (4194304, 4198400, 1)
    );
    assert!(tail.next.is_none());
    assert!(matches!(
        backend
            .open_read(&node, &key, request(session.run))
            .await
            .err()
            .as_ref()
            .and_then(model::error),
        Some(model::Error::NodeReadBusy)
    ));
    assert_eq!(active(&pool).await?, (2, 3, 2));
    let owned = reference(&tail, read::GrantType::Read);
    let foreign = backend
        .pulse_session(
            &peer,
            &key,
            peer_run.run,
            peer_run.epoch,
            1,
            false,
            read::KeepAlive {
                renew: vec![owned],
                release: Vec::new(),
            },
        )
        .await?;
    assert!(!foreign.grants[0].accepted);
    backend
        .pulse_session(
            &peer,
            &key,
            peer_run.run,
            peer_run.epoch,
            2,
            false,
            read::KeepAlive {
                renew: Vec::new(),
                release: vec![owned],
            },
        )
        .await?;
    assert_eq!(active(&pool).await?, (2, 3, 2));

    let pinned = material::<read::Mapping>(&third, read::MaterialType::Mapping)?.segments[0]
        .source
        .pin()?
        .1;
    sqlx::query("DELETE FROM extents WHERE stream_id=$1")
        .bind(stream)
        .execute(&pool)
        .await?;
    sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=clock_timestamp()-interval '1 day' WHERE encoding_id=$1").bind(pinned).execute(&pool).await?;
    assert_eq!(
        queries::claim_physical_gc(false, Id::Uuid(pinned), 0.0, 0.0)
            .execute(&pool)
            .await?
            .rows_affected(),
        0
    );
    backend
        .pulse_session(
            &node,
            &key,
            session.run,
            session.epoch,
            3,
            false,
            read::KeepAlive {
                renew: Vec::new(),
                release: vec![reference(&third, read::GrantType::Window)],
            },
        )
        .await?;
    assert_eq!(
        queries::claim_physical_gc(false, Id::Uuid(pinned), 0.0, 0.0)
            .execute(&pool)
            .await?
            .rows_affected(),
        1
    );
    sqlx::query(
        "UPDATE stream_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(owned.id)
    .execute(&pool)
    .await?;
    let expired = backend
        .pulse_session(
            &node,
            &key,
            session.run,
            session.epoch,
            4,
            false,
            read::KeepAlive {
                renew: tail
                    .grants
                    .iter()
                    .map(|g| read::GrantRef {
                        r#type: g.r#type,
                        id: g.id,
                    })
                    .collect(),
                release: Vec::new(),
            },
        )
        .await?;
    assert!(
        expired
            .grants
            .iter()
            .all(|grant| !grant.accepted && grant.valid_for_ms == 0)
    );
    backend
        .pulse_session(
            &node,
            &key,
            session.run,
            session.epoch,
            5,
            false,
            read::KeepAlive {
                renew: Vec::new(),
                release: vec![owned, reference(&first, read::GrantType::Read)],
            },
        )
        .await?;
    assert_eq!(active(&pool).await?, (0, 0, 0));
    let raw: Uuid = sqlx::query_scalar("INSERT INTO raw_sources(storage_id,medium,state,size,digest,profile_revision,created_at,unreferenced_at) VALUES($1,'shared_s3','ready',8,$2,1,clock_timestamp()-interval '1 day',clock_timestamp()-interval '1 day') RETURNING id")
        .bind(storage).bind(vec![7u8; 16]).fetch_one(&pool).await?;
    sqlx::query("UPDATE streams SET payload_type='raw',payload_revision=payload_revision+1,size=8 WHERE id=$1").bind(stream).execute(&pool).await?;
    sqlx::query(
        "INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id) VALUES($1,0,8,$2)",
    )
    .bind(stream)
    .bind(raw)
    .execute(&pool)
    .await?;
    let raw_reply = backend.open_read(&node, &key, request(session.run)).await?;
    let mapping: read::Mapping = material(&raw_reply, read::MaterialType::Mapping)?;
    assert!(matches!(
        mapping.segments[0].source,
        read::Source::Raw { .. }
    ));
    sqlx::query("DELETE FROM raw_ranges WHERE stream_id=$1")
        .bind(stream)
        .execute(&pool)
        .await?;
    assert!(!model::raw::RawStore::claim(&backend, Id::Uuid(raw), 1, 0, 0).await?);
    backend
        .pulse_session(
            &node,
            &key,
            session.run,
            session.epoch,
            6,
            false,
            read::KeepAlive {
                renew: Vec::new(),
                release: vec![reference(&raw_reply, read::GrantType::Read)],
            },
        )
        .await?;
    assert!(model::raw::RawStore::claim(&backend, Id::Uuid(raw), 1, 0, 0).await?);
    sqlx::query(
        "UPDATE raw_sources SET state='ready',medium='local',pending_upload=true WHERE id=$1",
    )
    .bind(raw)
    .execute(&pool)
    .await?;
    sqlx::query("INSERT INTO raw_uploads(source_id,deduplicate,background,node_id) VALUES($1,false,true,$2)").bind(raw).bind(peer.node_id).execute(&pool).await?;
    sqlx::query(
        "INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id) VALUES($1,0,8,$2)",
    )
    .bind(stream)
    .bind(raw)
    .execute(&pool)
    .await?;
    let remote_pending = backend.open_read(&node, &key, request(session.run)).await?;
    assert!(remote_pending.outcome == read::Outcome::Pending && remote_pending.grants.is_empty());
    let local_pending = backend
        .open_read(&peer, &key, request(peer_run.run))
        .await?;
    assert!(local_pending.outcome == read::Outcome::Ready);
    assert!(
        !local_pending
            .grants
            .iter()
            .any(|grant| grant.r#type == read::GrantType::StorageRead)
    );
    backend
        .pulse_session(
            &peer,
            &key,
            peer_run.run,
            peer_run.epoch,
            3,
            false,
            read::KeepAlive {
                renew: Vec::new(),
                release: vec![reference(&local_pending, read::GrantType::Read)],
            },
        )
        .await?;
    sqlx::query("DELETE FROM raw_ranges WHERE stream_id=$1")
        .bind(stream)
        .execute(&pool)
        .await?;
    sqlx::query("UPDATE streams SET payload_type='chunks',payload_revision=payload_revision+1,size=4 WHERE id=$1")
        .bind(stream).execute(&pool).await?;
    let pack: Uuid = sqlx::query_scalar("INSERT INTO packs(storage_id,encryption_algorithm,raw_size,stored_size,digest,member_count,state,pack_fingerprint,created_at) VALUES($1,'none',8,124,$2,2,'ready',$3,clock_timestamp()-interval '1 day') RETURNING id")
        .bind(storage).bind(vec![91u8; 32]).bind(vec![92u8; 32]).fetch_one(&pool).await?;
    let member: Uuid = sqlx::query_scalar("INSERT INTO chunks(storage_id,hash,raw_size,encryption_algorithm,state,pack_id) VALUES($1,$2,4,'none','ready',$3) RETURNING id")
        .bind(storage).bind(vec![93u8; 32]).bind(pack).fetch_one(&pool).await?;
    sqlx::query("INSERT INTO chunks(storage_id,hash,raw_size,encryption_algorithm,state,pack_id) VALUES($1,$2,4,'none','ready',$3)")
        .bind(storage).bind(vec![94u8; 32]).bind(pack).execute(&pool).await?;
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES($1,0,4,$2)")
        .bind(stream)
        .bind(member)
        .execute(&pool)
        .await?;
    let packed = backend.open_read(&node, &key, request(session.run)).await?;
    let mapping: read::Mapping = material(&packed, read::MaterialType::Mapping)?;
    assert!(
        matches!(&mapping.segments[0].source, read::Source::Pack { pack: selected, chunk } if selected.id == Id::Uuid(pack) && selected.location_id == pack && chunk.id == Id::Uuid(member))
    );
    assert!(
        packed
            .grants
            .iter()
            .any(|grant| grant.r#type == read::GrantType::StorageRead)
    );
    sqlx::query("DELETE FROM extents WHERE stream_id=$1")
        .bind(stream)
        .execute(&pool)
        .await?;
    sqlx::query("UPDATE chunks SET pack_id=NULL WHERE pack_id=$1")
        .bind(pack)
        .execute(&pool)
        .await?;
    sqlx::query("UPDATE packs SET state='retired',unreferenced_at=clock_timestamp()-interval '1 day' WHERE id=$1")
        .bind(pack).execute(&pool).await?;
    assert_eq!(
        queries::claim_physical_gc(true, Id::Uuid(pack), 0.0, 0.0)
            .execute(&pool)
            .await?
            .rows_affected(),
        0
    );
    backend
        .pulse_session(
            &node,
            &key,
            session.run,
            session.epoch,
            7,
            false,
            read::KeepAlive {
                renew: Vec::new(),
                release: vec![reference(&packed, read::GrantType::Read)],
            },
        )
        .await?;
    assert_eq!(
        queries::claim_physical_gc(true, Id::Uuid(pack), 0.0, 0.0)
            .execute(&pool)
            .await?
            .rows_affected(),
        1
    );

    let fragment: Uuid = sqlx::query_scalar("INSERT INTO fragments(size,hash,sealed,staging_storage_id,staging_object_key) VALUES(4,$1,true,$2,'staged/fixture') RETURNING id")
        .bind(vec![95u8; 32]).bind(storage).fetch_one(&pool).await?;
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,fragment_id) VALUES($1,0,4,$2)")
        .bind(stream)
        .bind(fragment)
        .execute(&pool)
        .await?;
    sqlx::query("UPDATE streams SET payload_revision=payload_revision+1 WHERE id=$1")
        .bind(stream)
        .execute(&pool)
        .await?;
    let staged = backend.open_read(&node, &key, request(session.run)).await?;
    let mapping: read::Mapping = material(&staged, read::MaterialType::Mapping)?;
    assert!(
        matches!(&mapping.segments[0].source, read::Source::Staged { fragment: selected, storage: selected_storage, .. } if *selected == fragment && *selected_storage == storage)
    );
    assert!(
        staged
            .grants
            .iter()
            .any(|grant| grant.r#type == read::GrantType::StorageRead)
    );
    sqlx::query("DELETE FROM extents WHERE stream_id=$1")
        .bind(stream)
        .execute(&pool)
        .await?;
    assert!(
        queries::lock_cleanup_fragment(Id::Uuid(fragment))
            .fetch_optional(&pool)
            .await?
            .is_none()
    );
    backend
        .pulse_session(
            &node,
            &key,
            session.run,
            session.epoch,
            8,
            false,
            read::KeepAlive {
                renew: Vec::new(),
                release: vec![reference(&staged, read::GrantType::Read)],
            },
        )
        .await?;
    assert!(
        queries::lock_cleanup_fragment(Id::Uuid(fragment))
            .fetch_optional(&pool)
            .await?
            .is_some()
    );
    sqlx::query("UPDATE s3_credentials SET enabled=false WHERE access_key_id='reader'")
        .execute(&pool)
        .await?;
    let mut cached = request(session.run);
    cached.cached = first
        .materials
        .iter()
        .map(|material| material.version.clone())
        .collect();
    let denied = backend.open_read(&node, &key, cached).await?;
    assert!(
        denied.outcome == read::Outcome::AccessDenied
            && denied.credential.is_none()
            && denied.object.is_none()
    );
    assert_eq!(active(&pool).await?, (0, 0, 0));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a new isolated PostgreSQL database"]
async fn postgres_refresh_reads_batch_public_metadata_and_fence_callers() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = PgPoolOptions::new()
        .max_connections(6)
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
    let node = backend
        .provision_node(&admin, "combined", "node", &key)
        .await?;
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
    let peer_run = backend.open_session(&peer, &key, hello()).await?;
    let worker_run = backend.open_session(&worker, &key, hello()).await?;
    let storage = Uuid::now_v7();
    sqlx::query("INSERT INTO storages(id,name,type,endpoint_url,remote_bucket_name,region,access_key_encrypted,secret_key_encrypted,identity_marker) VALUES($1,'source','s3','http://backend:8333','read-fixture','us-east-1',$2,$3,$4)")
        .bind(storage).bind(codec::protect(b"source-access", &key, format!("storage-{storage}-access").as_bytes())?)
        .bind(codec::protect(b"source-secret", &key, format!("storage-{storage}-secret").as_bytes())?)
        .bind(crate::storage::model::identity_marker(node.deployment_id, storage)?).execute(&pool).await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id,read_concurrency) VALUES($1,2)")
        .bind(storage)
        .execute(&pool)
        .await?;
    let bucket: Uuid =
        sqlx::query_scalar("INSERT INTO buckets(name,storage_id) VALUES('reads',$1) RETURNING id")
            .bind(storage)
            .fetch_one(&pool)
            .await?;
    let stream: Uuid = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,etag,public_read,metadata) VALUES($1,'object','object','ready',4198400,'fixture',true,'{\"content_type\":\"application/octet-stream\"}') RETURNING id").bind(bucket).fetch_one(&pool).await?;
    sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,'object',$2,uuidv7())")
        .bind(bucket)
        .bind(stream)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO s3_credentials(access_key_id,secret_encrypted) VALUES('reader',$1)")
        .bind(codec::protect(b"reader-secret", &key, b"reader")?)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO s3_credential_grants(access_key,bucket_id,actions) VALUES('reader',$1,ARRAY['object.read'])").bind(bucket).execute(&pool).await?;

    sqlx::query("UPDATE streams SET payload_type='raw',size=8 WHERE id=$1")
        .bind(stream)
        .execute(&pool)
        .await?;
    let raw: Uuid = sqlx::query_scalar("INSERT INTO raw_sources(storage_id,medium,state,size,digest,profile_revision) VALUES($1,'shared_s3','ready',8,$2,1) RETURNING id").bind(storage).bind(vec![8u8;16]).fetch_one(&pool).await?;
    sqlx::query(
        "INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id) VALUES($1,0,8,$2)",
    )
    .bind(stream)
    .bind(raw)
    .execute(&pool)
    .await?;
    for n in 1..32 {
        let id:Uuid=sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,payload_type,state,size,etag,public_read,metadata) VALUES($1,$2,'object','raw','ready',8,'fixture',true,'{\"content_type\":\"application/octet-stream\"}') RETURNING id").bind(bucket).bind(format!("object-{n}")).fetch_one(&pool).await?;
        sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,$2,$3,uuidv7())").bind(bucket).bind(format!("object-{n}")).bind(id).execute(&pool).await?;
        sqlx::query(
            "INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id) VALUES($1,0,8,$2)",
        )
        .bind(id)
        .bind(raw)
        .execute(&pool)
        .await?;
    }
    sqlx::query("INSERT INTO bucket_domains(host,bucket_id) VALUES('reads.test',$1)")
        .bind(bucket)
        .execute(&pool)
        .await?;
    let mut input = request(session.run);
    input.access = None;
    input.policy = true;
    let first = backend.open_read(&node, &key, input).await?;
    let body = read::CachedBody {
        object: first.object.clone().unwrap(),
        mapping: first.mapping.clone().unwrap(),
        storages: first
            .materials
            .iter()
            .filter(|m| m.version.r#type == read::MaterialType::Storage)
            .map(|m| m.version.clone())
            .collect(),
        representation: "identity".into(),
        policy: crate::http::object::POLICY as u32,
        verified: true,
    };
    let baseline = active(&pool).await?;
    let batch = |run, count: usize| {
        let reads = (0..count)
            .map(|n| {
                let mut q = request(run);
                q.access = None;
                q.mapping = false;
                q.policy = true;
                q.route = model::ReadRoute::Bucket {
                    name: "reads".into(),
                    key: if n == 0 {
                        "object".into()
                    } else {
                        format!("object-{n}")
                    },
                    alias: None,
                };
                if n == 0 {
                    q.body = Some(body.clone());
                }
                q
            })
            .collect();
        model::node_refresh::Request {
            run,
            reads,
            cached: Vec::new(),
        }
    };
    let before = backend.refresh_status();
    let (a, b) = tokio::join!(
        backend.refresh_reads(&node, &key, batch(session.run, 32)),
        backend.refresh_reads(&peer, &key, batch(peer_run.run, 32))
    );
    let (a, b) = (a?, b?);
    assert_eq!(
        active(&pool).await?,
        baseline,
        "refresh must never acquire read protection"
    );
    assert_eq!(a.reads.len(), 32);
    assert_eq!(b.reads.len(), 32);
    assert!(a.reads.iter().all(|r| r.outcome == read::Outcome::Ready
        && r.grants.is_empty()
        && r.materials.is_empty()));
    assert!(a.reads[0].body_reused && b.reads[0].body_reused);
    assert_eq!(
        a.materials
            .iter()
            .filter(|m| m.version.r#type == read::MaterialType::Bucket)
            .count(),
        1
    );
    let after = backend.refresh_status();
    assert_eq!(
        after["requests"].as_u64().unwrap() - before["requests"].as_u64().unwrap(),
        2
    );
    let groups = after["groups"].as_u64().unwrap() - before["groups"].as_u64().unwrap();
    assert!((1..=2).contains(&groups));
    assert_eq!(
        after["routes"].as_u64().unwrap() - before["routes"].as_u64().unwrap(),
        32 * groups
    );
    // Submit already validated fixture identities without scheduling-dependent DB latency.
    let before = backend.refresh_status();
    let queued_a =
        super::refresh::enqueue(&pool, &backend.refresh, &node, &key, batch(session.run, 32))?;
    let queued_b = super::refresh::enqueue(
        &pool,
        &backend.refresh,
        &peer,
        &key,
        batch(peer_run.run, 32),
    )?;
    assert_eq!(queued_a.await??.reads.len(), 32);
    assert_eq!(queued_b.await??.reads.len(), 32);
    let after = backend.refresh_status();
    assert_eq!(
        after["groups"].as_u64().unwrap() - before["groups"].as_u64().unwrap(),
        1
    );
    assert_eq!(
        after["routes"].as_u64().unwrap() - before["routes"].as_u64().unwrap(),
        32
    );
    let mut cached = batch(session.run, 32);
    cached.cached = a.materials.iter().map(|m| m.version.clone()).collect();
    let cached = backend.refresh_reads(&node, &key, cached).await?;
    assert!(cached.materials.iter().all(|m| m.json.is_none()));
    assert!(backend.refresh_status()["material_hits"].as_u64().unwrap() > 0);
    let mut routes = batch(session.run, 3);
    routes.reads[0].route = model::ReadRoute::Host {
        host: "reads.test".into(),
        key: "object".into(),
    };
    routes.reads[1].route = model::ReadRoute::Bucket {
        name: "missing".into(),
        key: "wrong".into(),
        alias: Some(("reads".into(), "object".into())),
    };
    routes.reads[2].route = model::ReadRoute::Bucket {
        name: "reads".into(),
        key: "missing".into(),
        alias: None,
    };
    let routes = backend.refresh_reads(&node, &key, routes).await?;
    assert!(
        routes.reads[0].body_reused
            && routes.reads[1].outcome == read::Outcome::Ready
            && routes.reads[2].outcome == read::Outcome::NoSuchKey
    );
    assert!(
        backend
            .refresh_reads(&worker, &key, batch(worker_run.run, 1))
            .await
            .is_err()
    );
    assert!(
        backend
            .refresh_reads(&peer, &key, batch(session.run, 1))
            .await
            .is_err()
    );
    assert!(
        backend
            .refresh_reads(&node, &key, batch(session.run, 33))
            .await
            .is_err()
    );
    sqlx::query("UPDATE raw_sources SET quarantine_code='digest_mismatch' WHERE id=$1")
        .bind(raw)
        .execute(&pool)
        .await?;
    assert!(
        !backend
            .refresh_reads(&node, &key, batch(session.run, 1))
            .await?
            .reads[0]
            .body_reused
    );
    sqlx::query("UPDATE raw_sources SET quarantine_code=NULL WHERE id=$1")
        .bind(raw)
        .execute(&pool)
        .await?;
    sqlx::query("UPDATE streams SET public_read=false WHERE id=$1")
        .bind(stream)
        .execute(&pool)
        .await?;
    let private = backend
        .refresh_reads(&node, &key, batch(session.run, 2))
        .await?;
    assert!(
        private.reads[0].outcome == read::Outcome::AccessDenied
            && private.reads[1].outcome == read::Outcome::Ready
    );
    assert!(private.reads[0].object.is_none() && !private.reads[0].body_reused);
    assert_eq!(active(&pool).await?, baseline);
    Ok(())
}
