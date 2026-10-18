use super::connection;
use crate::authorization::{Action, DEFAULT_PROJECT, Permit, Principal};
use crate::config::Config;
use crate::model::{
    ChunkCreate, ChunkStore, PhysicalSource, PhysicalStore, StreamCreate, StreamStore, StreamTail,
};
use anyhow::Result;
use serde_json::json;
use uuid::Uuid;

fn stream(bucket: Uuid, key: &str) -> StreamCreate {
    StreamCreate {
        authority: Permit::for_action(Principal::Local, bucket, Action::Write),
        bucket,
        key: key.into(),
        stream_type: "object".into(),
        metadata: json!({"content_type":"application/octet-stream"}),
        public: false,
        claim_object: true,
        completion: None,
        owner: None,
    }
}

fn chunk(storage: Uuid, stream: Uuid, hash: u8) -> ChunkCreate {
    ChunkCreate {
        storage,
        stream,
        hash: [hash; 32],
        raw_size: 32,
        algorithm: "none".into(),
        key: None,
    }
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_SQLITE_CONFIG pointing to a fresh isolated SQLite database"]
async fn sqlite_content_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.write;
    let storage = super::storage_tests::fixture(&backend).await?;
    let bucket = Uuid::new_v4();
    sqlx::query("INSERT INTO buckets(id,name,project_id,storage_id) VALUES(?,'content',?,?)")
        .bind(bucket)
        .bind(DEFAULT_PROJECT)
        .bind(storage)
        .execute(db)
        .await?;
    super::queries::insert_quota_scope("bucket", bucket)
        .execute(db)
        .await?;
    sqlx::query("UPDATE quota_scopes SET bucket_count=1 WHERE scope='project' AND scope_id=?")
        .bind(DEFAULT_PROJECT)
        .execute(db)
        .await?;
    let key = format!("{}A ", "\u{732b}".repeat(340));
    let first = backend.create(stream(bucket, &key)).await?;
    use sqlx::{Execute, Row};
    let plan_input = chunk(storage, first, 42);
    let mut statement = super::queries::fail_chunk_allocations(&plan_input);
    let arguments = statement
        .take_arguments()
        .map_err(anyhow::Error::from_boxed)?
        .unwrap();
    let explain = format!("EXPLAIN QUERY PLAN {}", statement.sql().as_str());
    let plan = sqlx::query_with(sqlx::AssertSqlSafe(explain), arguments)
        .fetch_all(db)
        .await?;
    assert!(
        plan.iter()
            .any(|row| row.get::<String, _>("detail").contains("chunks_dedup"))
    );
    assert_eq!(backend.storage_for_stream(first).await?, Some(storage));
    assert!(
        backend
            .create(stream(bucket, &"x".repeat(1025)))
            .await
            .is_err()
    );
    assert!(backend.create(stream(bucket, "")).await.is_err());
    let mut denied = stream(bucket, "denied");
    denied.authority = Permit::for_action(
        Principal::Service {
            access_key: "missing".into(),
            revision: 0,
        },
        bucket,
        Action::Write,
    );
    assert!(backend.create(denied).await.is_err());
    let task = Uuid::new_v4();
    let token = Uuid::new_v4();
    sqlx::query("INSERT INTO tasks(id,task_type,state,execution_token) VALUES(?,'storage_migration','running',?)").bind(task).bind(token).execute(db).await?;
    let mut owned = stream(bucket, "owned");
    owned.owner = Some((task, Uuid::new_v4()));
    assert!(backend.create(owned).await.is_err());
    let mut owned = stream(bucket, "owned");
    owned.owner = Some((task, token));
    backend.create(owned).await?;
    sqlx::query("UPDATE buckets SET uploads_paused=TRUE WHERE id=?")
        .bind(bucket)
        .execute(db)
        .await?;
    assert!(backend.create(stream(bucket, "paused")).await.is_err());
    let mut part = stream(bucket, "part");
    part.stream_type = "part".into();
    part.claim_object = false;
    backend.create(part).await?;
    let upload = Uuid::new_v4();
    sqlx::query("INSERT INTO uploads(id,bucket_id,object_key,access_key,state) VALUES(?,?,?,'fixture','completing')").bind(upload).bind(bucket).bind(b"completion".as_slice()).execute(db).await?;
    let mut completion = stream(bucket, "completion");
    completion.completion = Some(upload);
    backend.create(completion).await?;
    sqlx::query("UPDATE buckets SET uploads_paused=FALSE WHERE id=?")
        .bind(bucket)
        .execute(db)
        .await?;

    assert!(!backend.has_chunks().await?);
    let key_id = Uuid::new_v4();
    sqlx::query("INSERT INTO managed_keys(id,name,purpose,key_algorithm,fingerprint) VALUES(?,'aes','block','aes-256-gcm',?)").bind(key_id).bind(vec![0u8;32]).execute(db).await?;
    let mut invalid = chunk(storage, first, 99);
    invalid.key = Some(key_id);
    invalid.algorithm = "chacha20-poly1305".into();
    assert!(backend.allocate(invalid).await.is_err());
    let (mut physical, reused) = backend.allocate(chunk(storage, first, 1)).await?;
    assert!(!reused && physical.id > 0 && physical.encoding_id > 0);
    let failed = physical.clone();
    (physical, _) = backend.allocate(chunk(storage, first, 1)).await?;
    assert!(physical.id > failed.id && physical.encoding_id > failed.encoding_id);
    let old_state: String =
        sqlx::query_scalar("SELECT state FROM chunk_encodings WHERE encoding_id=?")
            .bind(failed.encoding_id)
            .fetch_one(db)
            .await?;
    assert_eq!(old_state, "retired");
    assert!(backend.mark_uploading(&failed).await.is_err());
    physical.location_id = Uuid::new_v4();
    physical.stored_size = Some(32);
    backend.mark_uploading(&physical).await?;
    assert!(backend.ready_chunk(physical.id).await?.is_none());
    assert!(backend.complete(first, 0, &physical).await.is_err());
    backend.register_encoding(&physical).await?;
    assert!(backend.indexed(physical.location_id).await?);
    backend.complete(first, 0, &physical).await?;
    assert!(backend.complete(first, 0, &physical).await.is_err());
    let ready = backend.ready_chunk(physical.id).await?.unwrap();
    assert_eq!(
        (ready.encoding_id, ready.location_id, ready.stored_size),
        (physical.encoding_id, physical.location_id, Some(32))
    );
    let source = backend.source(physical.id, false).await?;
    assert!(matches!(source,PhysicalSource::Chunk(c) if c.location_id==physical.location_id));
    let (reused, existing) = backend.allocate(chunk(storage, first, 1)).await?;
    assert!(existing);
    assert_eq!(reused.id, physical.id);
    assert!(!backend.can_stage_reuse(physical.id).await?);
    backend.reference(first, 32, physical.id, 32).await?;
    let all = backend.extents_for_copy(first, 0).await?;
    assert_eq!(all.len(), 2);
    assert_eq!(backend.read_start(first, 63).await?, Some(32));
    let read = backend.read_extents(first, 0, 64).await?;
    assert_eq!(read.len(), 2);
    assert_eq!(read[1].chunk.id, physical.id);
    assert!(
        backend
            .finish(first, 64, "etag", json!({"sha256":"checksum"}))
            .await?
    );

    // Publication remains a separate transaction; this fixture exposes its stored stream.
    super::queries::publish_stream(first).execute(db).await?;
    sqlx::query("UPDATE objects SET stream_id=? WHERE bucket_id=? AND object_key=?")
        .bind(first)
        .bind(bucket)
        .bind(key.as_bytes())
        .execute(db)
        .await?;
    assert_eq!(backend.current(bucket, &key).await?.object_key, key);
    assert_eq!(
        backend.current(bucket, &key).await?.http_compression,
        json!({"estimated_size":64})
    );
    sqlx::query("UPDATE streams SET http_compression=json_object('version',1,'estimated_size',64,'future',5) WHERE id=?").bind(first).execute(db).await?;
    crate::model::stream::compression_tests::check(&backend, bucket, &key).await?;
    assert!(backend.current(bucket, key.trim_end()).await.is_err());
    assert!(
        backend
            .current(bucket, &key.replace('A', "a"))
            .await
            .is_err()
    );
    assert!(!backend.finish(first, 64, "etag", json!({})).await?);
    assert!(backend.reference(first, 64, physical.id, 32).await.is_err());
    let next = backend.create(stream(bucket, &key)).await?;
    assert_eq!(backend.current(bucket, &key).await?.id, first);
    backend
        .copy_references(next, first, 0, 64, &[physical.id])
        .await?;
    assert_eq!(backend.extents(next, 0, 64).await?.len(), 2);
    assert!(
        backend
            .copy_references(next, first, 0, 64, &[physical.id])
            .await
            .is_err()
    );
    assert_eq!(backend.extents(next, 0, 64).await?.len(), 2);

    let sliced = backend.create(stream(bucket, "sliced")).await?;
    let mut inserts = sqlx::QueryBuilder::new(
        "INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) ",
    );
    inserts.push_values(0..513, |mut row, index| {
        row.push_bind(sliced)
            .push_bind(i64::from(index) * 8)
            .push_bind(8)
            .push_bind(physical.id)
            .push_bind(7);
    });
    inserts.build().execute(db).await?;
    assert_eq!(backend.extents_for_copy(sliced, 0).await?.len(), 500);
    assert_eq!(backend.extents(sliced, 0, 513 * 8).await?.len(), 64);
    let page = backend.read_extents(sliced, 500 * 8, 513 * 8).await?;
    assert_eq!(page.len(), 13);
    assert_eq!(page[0].extent.source_offset, 7);
    sqlx::query("UPDATE chunks SET state='deleting' WHERE id=?")
        .bind(physical.id)
        .execute(db)
        .await?;
    assert!(backend.read_extents(sliced, 0, 8).await.is_err());
    sqlx::query("UPDATE chunks SET state='ready' WHERE id=?")
        .bind(physical.id)
        .execute(db)
        .await?;

    let fragment = Uuid::new_v4();
    backend
        .append_tail(StreamTail {
            fragment,
            stream: next,
            offset: 64,
            size: 5,
            hash: [7; 32],
            staging_storage: None,
            staging_key: None,
        })
        .await?;
    let tail = backend.fragment(fragment).await?;
    assert_eq!(tail.size, 5);
    assert!(tail.staging_storage_id.is_none() && tail.staging_object_key.is_none());
    let failed_tail = Uuid::new_v4();
    assert!(
        backend
            .append_tail(StreamTail {
                fragment: failed_tail,
                stream: next,
                offset: 64,
                size: 5,
                hash: [8; 32],
                staging_storage: None,
                staging_key: None
            })
            .await
            .is_err()
    );
    let sealed: bool = sqlx::query_scalar("SELECT sealed FROM fragments WHERE id=?")
        .bind(failed_tail)
        .fetch_one(db)
        .await?;
    assert!(!sealed);
    assert!(
        backend
            .append_tail(StreamTail {
                fragment: Uuid::new_v4(),
                stream: next,
                offset: 69,
                size: 5,
                hash: [9; 32],
                staging_storage: Some(storage),
                staging_key: Some("invalid".into())
            })
            .await
            .is_err()
    );

    sqlx::query("INSERT INTO pending_uploads(chunk_id,stream_id,offset_bytes,staged_size,next_retry_at) VALUES(?,?,0,32,CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(physical.id).bind(next).execute(db).await?;
    let pins = backend.upload_pins(0).await?;
    assert_eq!(pins.len(), 1);
    assert_eq!(pins[0], (physical.id, None, None, 32));
    assert!(backend.pending_source(physical.id).await?.is_none());
    assert!(!backend.pending_only(physical.id).await?);
    let pin_stream = backend.create(stream(bucket, "pin")).await?;
    backend.reference(pin_stream, 0, physical.id, 32).await?;
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM cache_pins WHERE chunk_id=? AND owner_id=?")
            .bind(physical.id)
            .bind(pin_stream)
            .fetch_one(db)
            .await?;
    assert_eq!(count, 1);
    sqlx::query("UPDATE chunk_encodings SET state='retired' WHERE encoding_id=?")
        .bind(physical.encoding_id)
        .execute(db)
        .await?;
    assert!(backend.pending_only(physical.id).await?);
    let pack_location = Uuid::new_v4();
    let pack=sqlx::query("INSERT INTO packs(location_id,storage_id,encryption_algorithm,raw_size,stored_size,member_count,state,pack_fingerprint) VALUES(?,?,'none',64,64,2,'ready',?)").bind(pack_location).bind(storage).bind("[]").execute(db).await?.last_insert_rowid();
    sqlx::query("INSERT INTO pack_members(pack_id,ordinal,chunk_id,offset_bytes) VALUES(?,0,?,0)")
        .bind(pack)
        .bind(physical.id)
        .execute(db)
        .await?;
    sqlx::query("UPDATE chunks SET pack_id=? WHERE id=?")
        .bind(pack)
        .bind(physical.id)
        .execute(db)
        .await?;
    assert!(backend.indexed(pack_location).await?);
    assert!(backend.pack_contains(pack, physical.id).await?);
    assert!(!backend.pending_only(physical.id).await?);
    assert!(
        matches!(backend.source(physical.id, false).await?,PhysicalSource::Pack(p) if p.id==pack)
    );
    assert_eq!(backend.inspection_members(pack).await?[0].1, 0);
    assert!(backend.inspection(task, pack).await?.is_none());
    backend.record_inspection(task, pack, None).await?;
    backend
        .record_inspection(task, pack, Some("ignored"))
        .await?;
    assert_eq!(backend.inspection(task, pack).await?, Some(None));
    assert_eq!(backend.ready_chunks(&[physical.id]).await?.len(), 1);
    assert!(backend.ready_chunks(&[]).await?.is_empty());
    sqlx::query("DELETE FROM pending_uploads WHERE chunk_id=?")
        .bind(physical.id)
        .execute(db)
        .await?;
    assert!(backend.can_stage_reuse(physical.id).await?);
    super::queries::publish_stream(first).execute(db).await?;
    assert_eq!(
        backend.current(bucket, &key).await?.http_compression,
        json!({"estimated_size":64})
    );
    sqlx::query("UPDATE streams SET size=4104 WHERE id=?")
        .bind(sliced)
        .execute(db)
        .await?;
    super::queries::publish_stream(sliced).execute(db).await?;
    let estimated: String = sqlx::query_scalar("SELECT http_compression FROM streams WHERE id=?")
        .bind(sliced)
        .fetch_one(db)
        .await?;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&estimated)?,
        json!({"estimated_size":4104})
    );
    backend.check_owner()?;
    backend.read.close().await;
    backend.write.close().await;
    Ok(())
}
