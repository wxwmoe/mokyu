use super::{Backend, cache_tests, connection, storage_tests};
use crate::authorization::{Action, Permit, Principal};
use crate::config::Config;
use crate::model::bucket::CreateBucket;
use crate::model::metrics::MetricsStore;
use crate::model::{AccountStore, BucketStore, TaskControlStore};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::json;
use uuid::Uuid;

async fn chunk(
    backend: &Backend,
    storage: Uuid,
    n: u8,
    state: &str,
    size: Option<i32>,
    encoding: Option<&str>,
) -> Result<i64> {
    let db = &backend.pool;
    let id=sqlx::query("INSERT INTO chunks(storage_id,hash,raw_size,standalone_size,encryption_algorithm,state,current_encoding_id,unreferenced_at,deleted_at) VALUES(?,?,100,?,'none',?,1,IF(?='deleting',CURRENT_TIMESTAMP(6)-INTERVAL 2 DAY,NULL),IF(?='deleted',CURRENT_TIMESTAMP(6)-INTERVAL 30 DAY,NULL))").bind(storage).bind([n;32].as_slice()).bind(size).bind(state).bind(state).bind(state).execute(db).await?.last_insert_id() as i64;
    if let Some(state) = encoding {
        let encoding=sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,standalone_size,state,encryption_algorithm,encoding_fingerprint,unreferenced_at,deleted_at) VALUES(?,?,?,?,?,'none',?,IF(?='retired',CURRENT_TIMESTAMP(6)-INTERVAL 2 DAY,NULL),IF(?='deleted',CURRENT_TIMESTAMP(6)-INTERVAL 30 DAY,NULL))").bind(id).bind(Uuid::new_v4()).bind(storage).bind(size).bind(state).bind([0u8;32].as_slice()).bind(state).bind(state).execute(db).await?.last_insert_id() as i64;
        sqlx::query("UPDATE chunks SET current_encoding_id=? WHERE id=?")
            .bind(encoding)
            .bind(id)
            .execute(db)
            .await?;
    }
    Ok(id)
}
async fn object(backend: &Backend, bucket: Uuid, chunk: i64, key: &str) -> Result<()> {
    let stream = Uuid::new_v4();
    let db = &backend.pool;
    sqlx::query("INSERT INTO streams(id,bucket_id,object_key,stream_type,state,size,write_authorization) VALUES(?,?,?,'object','ready',100,?)").bind(stream).bind(bucket).bind(key.as_bytes()).bind(sqlx::types::Json(Permit::for_action(Principal::Local,bucket,Action::Write))).execute(db).await?;
    sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES(?,?,?,?)")
        .bind(bucket)
        .bind(key.as_bytes())
        .bind(stream)
        .bind(stream)
        .execute(db)
        .await?;
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) VALUES(?,0,100,?,0)").bind(stream).bind(chunk).execute(db).await?;
    sqlx::query("UPDATE quota_scopes SET used_bytes=used_bytes+100,object_count=object_count+1 WHERE scope='bucket' AND scope_id=?").bind(bucket).execute(db).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_inventory_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    backend.initialize_admin("admin", "password", false).await?;
    let empty = backend
        .inventory(&config.metrics, &config.maintenance)
        .await?;
    assert_eq!(empty["objects"], 0);
    assert_eq!(empty["logical_bytes"], 0);
    assert_eq!(empty["chunks"]["states"], json!({}));
    assert_eq!(empty["physical"], json!([]));
    assert_eq!(empty["live"]["stored_bytes"], 0);
    assert_eq!(empty["cleanup"].as_object().unwrap().len(), 7);
    assert!(
        empty["cleanup"]
            .as_object()
            .unwrap()
            .values()
            .all(|v| v["eligible"] == 0 && v["oldest_at"].is_null())
    );
    let a = storage_tests::fixture_named(&backend, "first").await?;
    let b = storage_tests::fixture_named(&backend, "second").await?;
    let mut buckets = Vec::new();
    for (name, storage) in [("inventory-a", a), ("inventory-b", b)] {
        buckets.push(
            BucketStore::create(
                &backend,
                &Principal::Local,
                CreateBucket {
                    name: name.into(),
                    project_id: None,
                    storage_id: Some(storage),
                },
            )
            .await?
            .id,
        );
    }
    let c1 = chunk(&backend, a, 1, "ready", Some(60), Some("ready")).await?;
    let c2 = chunk(&backend, a, 2, "ready", None, None).await?;
    let c3 = chunk(&backend, a, 3, "ready", None, None).await?;
    let c4 = chunk(&backend, a, 4, "deleting", Some(20), Some("retired")).await?;
    let c5 = chunk(&backend, a, 5, "deleted", Some(30), Some("deleted")).await?;
    let c6 = chunk(&backend, b, 6, "ready", Some(40), Some("ready")).await?;
    for (bucket, chunk, key) in [
        (buckets[0], c1, "one"),
        (buckets[0], c1, "duplicate"),
        (buckets[0], c2, "packed"),
        (buckets[0], c3, "pending"),
        (buckets[1], c6, "other"),
    ] {
        object(&backend, bucket, chunk, key).await?;
    }
    let task = TaskControlStore::enqueue(&backend, "pack", None, json!({})).await?;
    let mut packs = Vec::new();
    for (state, size) in [("ready", 50), ("preparing", 70), ("retired", 80)] {
        let pack = cache_tests::pack(&backend, a, task).await?;
        sqlx::query("UPDATE packs SET state=?,raw_size=200,stored_size=?,unreferenced_at=IF(?='retired',CURRENT_TIMESTAMP(6)-INTERVAL 2 DAY,NULL) WHERE id=?").bind(state).bind(size).bind(state).bind(pack).execute(db).await?;
        packs.push(pack);
    }
    sqlx::query("UPDATE chunks SET pack_id=? WHERE id=?")
        .bind(packs[0])
        .bind(c2)
        .execute(db)
        .await?;
    sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,standalone_size,state,encryption_algorithm,encoding_fingerprint) VALUES(?,?,?,30,'uploading','none',?)").bind(c3).bind(Uuid::new_v4()).bind(a).bind([0u8;32].as_slice()).execute(db).await?;
    sqlx::query("UPDATE tasks SET state='completed',updated_at=CURRENT_TIMESTAMP(6)-INTERVAL 30 DAY WHERE id=?").bind(task).execute(db).await?;
    sqlx::query("UPDATE storages SET enabled=FALSE WHERE id=?")
        .bind(b)
        .execute(db)
        .await?;
    let inventory = backend
        .inventory(&config.metrics, &config.maintenance)
        .await?;
    assert_eq!(inventory["objects"], 5);
    assert_eq!(inventory["logical_bytes"], 500);
    assert_eq!(inventory["buckets"].as_array().unwrap().len(), 2);
    assert_eq!(inventory["buckets_truncated"], false);
    assert_eq!(
        inventory["chunks"],
        json!({"states":{"ready":4,"deleting":1,"deleted":1},"stored_bytes":250,"unconfirmed_bytes":100})
    );
    assert_eq!(
        inventory["live"],
        json!({"raw_objects":0,"chunks":4,"reference_bytes":500,"raw_bytes":400,"pending_raw_bytes":100,"stored_bytes":150,"payload_bytes":150})
    );
    assert_eq!(
        inventory["unreferenced"],
        json!({"chunks":1,"eligible_chunks":1,"stored_bytes":100,"eligible_bytes":100})
    );
    sqlx::query("UPDATE storage_profiles SET unreferenced_grace='7d',deleted_chunk_retention='90d' WHERE storage_id=?").bind(a).execute(db).await?;
    let delayed = backend
        .inventory(&config.metrics, &config.maintenance)
        .await?;
    assert_eq!(delayed["unreferenced"]["eligible_chunks"], 0);
    assert_eq!(delayed["unreferenced"]["eligible_bytes"], 0);
    assert_eq!(delayed["cleanup"]["chunks"]["eligible"], 0);
    sqlx::query("UPDATE storage_profiles SET unreferenced_grace='48h',deleted_chunk_retention='7d' WHERE storage_id=?").bind(a).execute(db).await?;
    assert_eq!(inventory["tasks"], json!({"completed":1}));
    assert_eq!(inventory["uploads"], json!({}));
    for name in ["chunks", "chunk_encodings", "tasks"] {
        assert_eq!(inventory["cleanup"][name]["eligible"], 1);
        DateTime::parse_from_rfc3339(inventory["cleanup"][name]["oldest_at"].as_str().unwrap())?;
    }
    DateTime::parse_from_rfc3339(inventory["as_of"].as_str().unwrap())?;
    let tables = inventory["database"].as_array().unwrap();
    assert_eq!(tables.len(), 26);
    assert!(tables.iter().all(|v| v["total_bytes"].is_i64()
        && v["index_bytes"].is_i64()
        && v["live_rows_estimate"].is_i64()
        && v["dead_rows_estimate"].is_null()
        && v["last_autovacuum"].is_null()));
    let first = backend.storage_inventory(a).await?;
    assert_eq!(
        (
            first["buckets"].as_i64(),
            first["objects"].as_i64(),
            first["logical_bytes"].as_i64()
        ),
        (Some(1), Some(4), Some(400))
    );
    assert_eq!(first["chunks"], 4);
    assert_eq!(first["stored_bytes"], 80);
    assert_eq!(first["physical"], json!({"objects":4,"stored_bytes":210}));
    let second = backend.storage_inventory(b).await?;
    assert_eq!(second["objects"], 1);
    assert_eq!(second["physical"], json!({"objects":1,"stored_bytes":40}));
    assert_eq!(
        backend.storage_inventory(Uuid::new_v4()).await?["objects"],
        0
    );
    let usage = backend
        .usage(&Principal::Local, &Default::default(), 64)
        .await?;
    assert_eq!(usage.usage.unwrap().logical_bytes, "500");
    assert_eq!(usage.history.len(), 1);

    // A pending upload protects its chunk even after the grace period has elapsed.
    sqlx::query("INSERT INTO pending_uploads(chunk_id,offset_bytes,staged_size,next_retry_at) VALUES(?,0,20,CURRENT_TIMESTAMP(6))").bind(c4).execute(db).await?;
    assert_eq!(
        backend
            .inventory(&config.metrics, &config.maintenance)
            .await?["unreferenced"]["eligible_chunks"],
        0
    );
    sqlx::query("INSERT INTO pack_members(pack_id,chunk_id,ordinal,offset_bytes) VALUES(?,?,0,0)")
        .bind(packs[2])
        .bind(c5)
        .execute(db)
        .await?;
    assert_eq!(
        backend
            .inventory(&config.metrics, &config.maintenance)
            .await?["cleanup"]["chunks"]["eligible"],
        0
    );

    let mut lock = backend.owner.lock().await;
    sqlx::raw_sql("LOCK TABLES extents WRITE")
        .execute(&mut *lock)
        .await?;
    let mut bounded = config.metrics.clone();
    bounded.query_timeout = "1s".into();
    let error = backend
        .inventory(&bounded, &config.maintenance)
        .await
        .unwrap_err();
    assert_eq!(
        crate::model::error(&error),
        Some(crate::model::Error::QueryCancelled)
    );
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(200),
            backend.inventory(&config.metrics, &config.maintenance)
        )
        .await
        .is_err()
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        sqlx::query("SELECT 1").execute(db),
    )
    .await??;
    assert!(
        backend
            .usage(&Principal::Local, &Default::default(), 64)
            .await?
            .usage
            .is_some()
    );
    sqlx::raw_sql("UNLOCK TABLES").execute(&mut *lock).await?;
    drop(lock);

    let mut insert = sqlx::QueryBuilder::<sqlx::MySql>::new(
        "INSERT INTO buckets(id,name,project_id,storage_id) ",
    );
    insert.push_values(0..1001, |mut row, n| {
        row.push_bind(Uuid::new_v4())
            .push_bind(format!("extra-{n}"))
            .push_bind(crate::authorization::DEFAULT_PROJECT)
            .push_bind(a);
    });
    insert.build().execute(db).await?;
    let truncated = backend
        .inventory(&config.metrics, &config.maintenance)
        .await?;
    assert_eq!(truncated["buckets_truncated"], true);
    assert_eq!(truncated["buckets"].as_array().unwrap().len(), 1000);
    assert_eq!(truncated["objects"], 5);
    assert_eq!(truncated["logical_bytes"], 500);
    assert_eq!(backend.storage_inventory(a).await?["buckets"], 1002);
    let usage = backend
        .usage(&Principal::Local, &Default::default(), 64)
        .await?
        .usage
        .unwrap();
    assert_eq!(usage.buckets, "1003");
    assert_eq!(usage.logical_bytes, "500");
    assert!(
        Utc::now()
            >= DateTime::parse_from_rfc3339(truncated["as_of"].as_str().unwrap())?
                .with_timezone(&Utc)
    );
    Ok(())
}
