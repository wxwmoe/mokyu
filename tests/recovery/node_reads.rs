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
    sqlx::query("INSERT INTO chunks(storage_id,hash,raw_size,standalone_size,encryption_algorithm,state) SELECT $1,decode(md5(n::text)||md5(n::text),'hex'),4096,4096,'none','ready' FROM generate_series(1,1025) n").bind(storage).execute(&pool).await?;
    sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,standalone_size,standalone_compressed,state,created_at,encryption_algorithm,storage_id,encoding_fingerprint) SELECT current_encoding_id,id,4096,false,'ready',clock_timestamp(),'none',storage_id,hash FROM chunks").execute(&pool).await?;
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) SELECT $1,(row_number() OVER(ORDER BY id)-1)*4096,4096,id FROM chunks").bind(stream).execute(&pool).await?;

    assert!(
        backend
            .open_read(&worker, &key, request(worker_run.run))
            .await
            .is_err()
    );
    let mut head = request(session.run);
    head.head = true;
    head.mapping = false;
    let header = backend.open_read(&node, &key, head).await?;
    assert!(
        header.outcome == read::Outcome::Ready
            && header.grants.is_empty()
            && header.mapping.is_none()
    );
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
    let pulse = backend
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
            2,
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
            3,
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
            4,
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
            5,
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
            6,
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
            7,
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
