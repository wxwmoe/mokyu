use super::*;
use crate::model::{Id, node_chunk as chunk, node_write as write, raw::Conversion};

pub(super) async fn check(
    backend: &Backend,
    peer: &Backend,
    admin: &Principal,
    key: &Key,
) -> Result<()> {
    let pool = &backend.pool;
    let node = backend
        .provision_node(admin, "cdc-worker", "worker", key)
        .await?;
    let session = backend
        .open_session(
            &node,
            key,
            Open {
                boot: Uuid::new_v4(),
                connection: Uuid::new_v4(),
                previous_epoch: 0,
            },
        )
        .await?;
    let run = session.run;
    let storage = Uuid::now_v7();
    sqlx::query("INSERT INTO storages(id,name,type,endpoint_url,remote_bucket_name,region,access_key_encrypted,secret_key_encrypted,identity_marker) VALUES($1,'node-cdc','s3','http://backend:8333','node-cdc','us-east-1',$2,$3,$4)")
        .bind(storage).bind(codec::protect(b"access",key,format!("storage-{storage}-access").as_bytes())?)
        .bind(codec::protect(b"secret",key,format!("storage-{storage}-secret").as_bytes())?)
        .bind(crate::storage::model::identity_marker(node.deployment_id,storage)?).execute(pool).await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id,control_concurrency,maintenance_concurrency,upload_concurrency,pack_enabled,compression_enabled) VALUES($1,1,1,1,false,false)").bind(storage).execute(pool).await?;
    let bucket: Uuid = sqlx::query_scalar(
        "INSERT INTO buckets(name,storage_id) VALUES('node-cdc',$1) RETURNING id",
    )
    .bind(storage)
    .fetch_one(pool)
    .await?;
    let source = Id::from(Uuid::now_v7());
    let etag = "02".repeat(16);
    sqlx::query("INSERT INTO raw_sources(id,storage_id,medium,state,size,digest,profile_revision) VALUES($1,$2,'shared_s3','ready',16,$3,1)").bind(source).bind(storage).bind(vec![2u8;16]).execute(pool).await?;
    let stream:Id=sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,payload_type,etag) VALUES($1,'original','object','ready',16,'raw',$2) RETURNING id").bind(bucket).bind(&etag).fetch_one(pool).await?;
    sqlx::query("INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_offset,source_id) VALUES($1,0,16,0,$2)").bind(stream).bind(source).execute(pool).await?;
    sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,'original',$2,uuidv7())").bind(bucket).bind(stream).execute(pool).await?;
    let encoding = crate::model::cache::EncodingPolicy {
        compression: crate::content::compression::Config {
            enabled: false,
            ..Default::default()
        },
        pack_enabled: false,
        pack_max_size: "8MiB".into(),
        pack_flush_timeout: "1s".into(),
        algorithm: "none".into(),
        block_key_id: None,
    };
    let mut detail = serde_json::to_value(Conversion {
        stream,
        source,
        bucket,
        object_key: "original".into(),
        payload_revision: 1,
        encoding,
        target_key_id: None,
    })?;
    detail["operation"] = serde_json::json!("cdc");
    let task = Uuid::now_v7();
    queries::insert_task(task, "upload", Some(bucket), detail, None)
        .execute(pool)
        .await?;
    queries::conversion_link(source, task).execute(pool).await?;
    let exchange = |operation, completion| work::Exchange {
        run,
        operation,
        completion,
        class: 2,
        max_items: 64,
        cached: vec![],
    };
    let first = Uuid::now_v7();
    let work::Plan::ConvertRaw {
        write: operation,
        sealed,
        ..
    } = backend
        .exchange_work(&node, key, exchange(first, None))
        .await?
        .plan
        .unwrap()
    else {
        panic!("expected CDC plan")
    };
    assert!(!sealed);
    let lookup = || write::Lookup { run, operation };
    let admitted = peer.write_status(&node, key, lookup(), false).await?;
    assert_eq!(admitted.phase, write::Phase::Active);
    assert!(!admitted.materials.is_empty() && !admitted.grants.is_empty());
    let candidate = Id::from(admitted.stream.unwrap());
    assert_ne!(candidate, stream);
    assert_eq!(
        sqlx::query_scalar::<_, Id>(
            "SELECT stream_id FROM objects WHERE bucket_id=$1 AND object_key='original'"
        )
        .bind(bucket)
        .fetch_one(pool)
        .await?,
        stream
    );
    let input = chunk::Prepare {
        run,
        operation,
        batch: 1,
        offset: 0,
        parallelism: 1,
        cached: vec![],
        items: vec![chunk::Input {
            hash: [4; 32],
            raw_size: 16,
            payload_hash: [4; 32],
            payload_size: 16,
            compressed: false,
        }],
    };
    backend.prepare_chunks(&node, key, input.clone()).await?;
    let ready = backend.activate_chunks(&node, key, input, vec![]).await?;
    backend
        .commit_chunks(
            &node,
            key,
            chunk::Commit {
                run,
                operation,
                batch: 1,
                encoded: vec![chunk::Encoded {
                    chunk: ready.items[0].chunk.uuid,
                    digest: [5; 32],
                }],
            },
        )
        .await?;
    let publish = |etag: String| write::Publish {
        run,
        operation,
        size: 16,
        etag,
        checksums: Default::default(),
        if_match: None,
        if_none: None,
    };
    assert!(
        backend
            .publish_write(&node, key, publish("wrong".into()))
            .await
            .is_err()
    );
    assert_eq!(
        backend
            .publish_write(&node, key, publish(etag.clone()))
            .await?
            .phase,
        write::Phase::Published
    );
    assert_eq!(
        peer.publish_write(&node, key, publish(etag.clone()))
            .await?
            .phase,
        write::Phase::Published
    );
    let lease:Uuid=sqlx::query_scalar("INSERT INTO stream_leases(stream_id,lease_type,expires_at) VALUES($1,'read',clock_timestamp()+interval '1 minute') RETURNING id").bind(stream).fetch_one(pool).await?;
    let completed = |operation| {
        Some(work::Completion {
            operation,
            outcomes: vec![work::Outcome::Converted],
        })
    };
    assert!(
        backend
            .exchange_work(&node, key, exchange(Uuid::now_v7(), completed(first)))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT payload_type FROM streams WHERE id=$1")
            .bind(stream)
            .fetch_one(pool)
            .await?,
        "raw"
    );
    sqlx::query("DELETE FROM stream_leases WHERE id=$1")
        .bind(lease)
        .execute(pool)
        .await?;
    sqlx::query("UPDATE tasks SET updated_at=clock_timestamp()-interval '2 seconds' WHERE id=$1")
        .bind(task)
        .execute(pool)
        .await?;
    let resumed = Uuid::now_v7();
    let work::Plan::ConvertRaw { write, sealed, .. } = peer
        .exchange_work(&node, key, exchange(resumed, None))
        .await?
        .plan
        .unwrap()
    else {
        panic!("expected sealed candidate")
    };
    assert!(sealed);
    assert_eq!(write, operation);
    assert!(
        peer.exchange_work(&node, key, exchange(Uuid::now_v7(), completed(resumed)))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(
        sqlx::query_as::<_, (String, i64, String)>(
            "SELECT payload_type,payload_revision,etag FROM streams WHERE id=$1"
        )
        .bind(stream)
        .fetch_one(pool)
        .await?,
        ("chunks".into(), 2, etag)
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM streams WHERE id=$1")
            .bind(candidate)
            .fetch_one(pool)
            .await?,
        "abandoned"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(pool)
            .await?,
        "completed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM quota_reservations WHERE stream_id=$1")
            .bind(candidate)
            .fetch_one(pool)
            .await?,
        0
    );
    Ok(())
}
