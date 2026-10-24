use super::*;
use crate::model::{multipart::RemoteUpload, raw::RawSource};
use md5::{Digest, Md5};
use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};

struct Barrier(PathBuf);
impl Barrier {
    fn new(name: &str) -> Result<Self> {
        let path = PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?).join(name);
        let _ = std::fs::remove_file(path.with_extension("hit"));
        std::fs::write(&path, b"")?;
        Ok(Self(path))
    }
    fn reached(&self) -> bool {
        self.0.with_extension("hit").exists()
    }
    fn release(&self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
impl Drop for Barrier {
    fn drop(&mut self) {
        self.release();
        let _ = std::fs::remove_file(self.0.with_extension("hit"));
    }
}

async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("raw permit condition timed out");
}

async fn join<T>(task: &mut tokio::task::JoinHandle<Result<T>>) -> Result<T> {
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("raw operation did not make progress")?
}

async fn read(storage: &Arc<Storage>, source: &RawSource, expected: &[u8]) -> Result<()> {
    let mut reader = storage.raw_reader(source, true).await?;
    let mut bytes = vec![0; expected.len() + 1];
    let mut offset = 0;
    loop {
        let count = reader.file.read(&mut bytes[offset..]).await?;
        if count == 0 {
            break;
        }
        offset += count;
    }
    assert_eq!(&bytes[..offset], expected);
    Ok(())
}

async fn write(storage: &Arc<Storage>, target: Uuid, data: &[u8]) -> Result<RawSource> {
    let source = RawSource {
        id: Id::default(),
        location_id: Uuid::now_v7(),
        storage_id: target,
        size: data.len() as i64,
        digest: Some(Md5::digest(data).to_vec()),
        state: "ready".into(),
        medium: if storage.store_backend(target)?.is_local {
            "local".into()
        } else {
            "shared_s3".into()
        },
        pending_upload: false,
        revision: 1,
        quarantine_code: None,
    };
    let mut writer = storage
        .raw_writer(
            target,
            source.id,
            source.location_id,
            Some(source.size),
            false,
        )
        .await?;
    writer.write(data).await?;
    writer
        .finish(
            Id::default(),
            data.len() as u64,
            source.digest.as_ref().unwrap(),
            None,
        )
        .await?;
    Ok(source)
}

async fn queue(pool: &SqlitePool, source: &mut RawSource, data: &[u8]) -> Result<i64> {
    let bucket = Uuid::new_v4();
    sqlx::query("INSERT INTO buckets(id,name,project_id,storage_id) VALUES(?,?,?,?)")
        .bind(bucket)
        .bind(bucket.to_string())
        .bind(crate::authorization::DEFAULT_PROJECT)
        .bind(source.storage_id)
        .execute(pool)
        .await?;
    let stream = sqlx::query("INSERT INTO streams(bucket_id,object_key,stream_type,state,payload_type,size,etag,write_authorization) VALUES(?,?,'object','ready','raw',?,?,'{}')")
        .bind(bucket).bind(b"object".as_slice()).bind(source.size).bind(hex::encode(Md5::digest(data))).execute(pool).await?.last_insert_rowid();
    source.id = sqlx::query("INSERT INTO raw_sources(location_id,storage_id,state,size,digest,profile_revision) VALUES(?,?,'ready',?,?,1)")
        .bind(source.location_id).bind(source.storage_id).bind(source.size).bind(&source.digest).execute(pool).await?.last_insert_rowid().into();
    sqlx::query("INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id) VALUES(?,0,?,?)")
        .bind(stream)
        .bind(source.size)
        .bind(source.id)
        .execute(pool)
        .await?;
    sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES(?,?,?,?)")
        .bind(bucket)
        .bind(b"object".as_slice())
        .bind(stream)
        .bind(stream)
        .execute(pool)
        .await?;
    sqlx::query("INSERT INTO raw_processing(stream_id,source_id,payload_revision) VALUES(?,?,1)")
        .bind(stream)
        .bind(source.id)
        .execute(pool)
        .await?;
    Ok(stream)
}

async fn processing(pool: &SqlitePool) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT count(*) FROM raw_processing")
        .fetch_one(pool)
        .await?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a fresh MOKYU_TEST_SQLITE_CONFIG, fault directory and disposable MOKYU_TEST_BACKEND"]
async fn raw_permit_order_cancellation_and_retry() -> Result<()> {
    ensure!(std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")? == "isolated-only");
    let file = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (mut config, secrets, _) = Config::load(std::path::Path::new(&file))?;
    config.instance.cpu_jobs = Some(1);
    config.instance.storage_read_slots = Some(1);
    config.instance.storage_upload_slots = Some(1);
    let budget = config.budget()?;
    let database = config
        .database
        .sqlite_path(&config.instance.data_path)
        .unwrap();
    ensure!(!database.exists(), "fixture database must be new");
    let backend_path = config.instance.data_path.join("backend");
    let (app, owner) = crate::app::App::new(config, secrets, budget, false, true, "single").await?;
    let store = crate::control::storage::create_record(
        &app,
        serde_json::from_value(serde_json::json!({
            "name":"raw-permits", "type":"local", "path":backend_path, "initialize":true,
            "read_concurrency":1,"upload_concurrency":1
        }))?,
    )
    .await?;
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(database)
                .foreign_keys(true),
        )
        .await?;
    let storage = app.storage.clone();
    let target = storage.store_backend(store.id)?;
    let data = vec![73; 128 * 1024];

    for cancel in [false, true] {
        let mut source = write(&storage, store.id, &data).await?;
        let stream = queue(&pool, &mut source, &data).await?;
        let barrier = Barrier::new("raw-verify-before-cpu")?;
        let copy = storage.clone();
        let row = source.clone();
        let input = data.clone();
        let mut reader = tokio::spawn(async move { read(&copy, &row, &input).await });
        until(|| barrier.reached()).await;
        let worker = tokio::spawn(crate::worker::raw::run(app.clone()));
        until(|| storage.reads.snapshot()["queued"]["maintenance"] == 1).await;
        if cancel {
            worker.abort();
            assert!(worker.await.unwrap_err().is_cancelled());
            assert_eq!(storage.cpu.available_permits(), 1);
            assert_eq!(processing(&pool).await?, 1);
        } else {
            barrier.release();
            join(&mut reader).await?;
            worker.abort();
            assert!(worker.await.unwrap_err().is_cancelled());
        }
        if cancel {
            barrier.release();
            join(&mut reader).await?;
        }
        let worker = tokio::spawn(crate::worker::raw::run(app.clone()));
        tokio::time::timeout(Duration::from_secs(10), async {
            while processing(&pool).await? != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        let ranges = app.raw.page(stream.into(), 0, source.size).await?;
        assert_eq!(ranges.len(), 1);
        read(&storage, &ranges[0].source, &data).await?;
        let hash: Vec<u8> = sqlx::query_scalar("SELECT content_hash FROM raw_sources WHERE id=?")
            .bind(ranges[0].source.id)
            .fetch_one(&pool)
            .await?;
        assert_eq!(hash, blake3::hash(&data).as_bytes());
        assert_eq!(storage.cpu.available_permits(), 1);
        assert_eq!(storage.reads.snapshot()["running"], 0);
        assert_eq!(target.reads.local.snapshot()["running"], 0);
        println!(
            "PASS raw verified read with background deduplication, cancel={cancel}, persisted retry and content intact"
        );
    }

    // Cancel a verified reader while it waits for CPU, then retry both valid and corrupt content.
    let source = write(&storage, store.id, &data).await?;
    let cpu = storage.cpu.acquire().await?;
    let barrier = Barrier::new("raw-verify-before-cpu")?;
    let mut reader = Box::pin(storage.raw_reader(&source, true));
    tokio::select! {
        _ = &mut reader => panic!("verified reader bypassed the CPU barrier"),
        _ = until(|| barrier.reached()) => {}
    }
    barrier.release();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut reader)
            .await
            .is_err()
    );
    drop(reader);
    drop(cpu);
    assert_eq!(storage.reads.snapshot()["running"], 0);
    assert_eq!(target.reads.local.snapshot()["running"], 0);
    let mut corrupt = source.clone();
    corrupt.digest.as_mut().unwrap()[0] ^= 1;
    assert!(storage.raw_reader(&corrupt, true).await.is_err());
    read(&storage, &source, &data).await?;
    assert_eq!(storage.cpu.available_permits(), 1);
    println!("PASS raw reader cancellation and integrity failure return all permits");

    let backend: serde_json::Value =
        serde_json::from_slice(&std::fs::read(std::env::var("MOKYU_TEST_BACKEND")?)?)?;
    let remote = crate::control::storage::create_record(
        &app,
        serde_json::from_value(serde_json::json!({
            "name":"raw-permits-s3", "type":"s3", "initialize":true,
            "endpoint":backend["endpoint"], "bucket":backend["bucket"], "region":backend["region"],
            "prefix":format!("raw-permits-{}", Uuid::new_v4()),
            "access_key":backend["access_key"], "secret_key":backend["secret_key"],
            "read_concurrency":1,"upload_concurrency":1
        }))?,
    )
    .await?;
    let mut profile = (*storage.profile(remote.id)?).clone();
    profile.cdc_enabled = false;
    profile.deduplication_enabled = false;
    profile.compression_enabled = false;
    profile.pack_enabled = false;
    profile.pack_range_enabled = false;
    profile.upload_cache = false;
    profile.cache_enabled = false;
    app.storage_profiles
        .update("raw-permits-s3".into(), profile)
        .await?;
    storage.refresh_store(remote.id).await?;
    let target = storage.store_backend(remote.id)?;
    let mut upload = RemoteUpload {
        upload: Uuid::new_v4(),
        state: "active".into(),
        source: Uuid::now_v7(),
        storage: remote.id,
        profile: target.profile_revision,
        upstream: None,
        manifest: None,
    };
    upload.upstream = Some(storage.create_remote(&upload).await?);
    let data = Bytes::from(data);
    for limit in [1, 3] {
        storage
            .writes
            .configure(limit, Duration::from_secs(30), false);
        let mut held = Vec::new();
        for _ in 0..limit {
            held.push(storage.writes.acquire().await?);
        }
        let mut parts = Vec::new();
        for number in 1..=limit {
            let copy = storage.clone();
            let current = upload.clone();
            let input = data.clone();
            parts.push(tokio::spawn(async move {
                copy.send_remote_part(
                    &current,
                    number as i32,
                    input.len() as i64,
                    &Md5::digest(&input).into(),
                    || Box::pin(futures_util::stream::iter([Ok(input.clone())])),
                )
                .await
            }));
        }
        until(|| storage.writes.snapshot()["queued"]["foreground"] == limit).await;
        let copy = storage.clone();
        let input = data.clone();
        let mut raw = tokio::spawn(async move { write(&copy, remote.id, &input).await });
        until(|| storage.writes.snapshot()["queued"]["foreground"] == limit + 1).await;
        drop(held);
        for part in &mut parts {
            join(part).await?;
        }
        let source = join(&mut raw).await?;
        read(&storage, &source, &data).await?;
        assert_eq!(storage.writes.snapshot()["running"], 0);
        assert_eq!(target.writes.local.snapshot()["running"], 0);
        println!("PASS raw PUT and native Multipart with {limit} saturated global slots");
    }
    storage.writes.configure(1, Duration::from_secs(30), false);
    for hold_backend in [false, true] {
        let global = if hold_backend {
            None
        } else {
            Some(storage.writes.acquire().await?)
        };
        let backend = if hold_backend {
            Some(target.writes.acquire().await?)
        } else {
            None
        };
        for native in [false, true] {
            let copy = storage.clone();
            let current = upload.clone();
            let input = data.clone();
            let task = tokio::spawn(async move {
                if native {
                    copy.send_remote_part(
                        &current,
                        1,
                        input.len() as i64,
                        &Md5::digest(&input).into(),
                        || Box::pin(futures_util::stream::iter([Ok(input.clone())])),
                    )
                    .await?;
                } else {
                    write(&copy, remote.id, &input).await?;
                }
                Ok::<_, anyhow::Error>(())
            });
            until(|| {
                if hold_backend {
                    target.writes.local.snapshot()["queued"]["foreground"] == 1
                } else {
                    storage.writes.snapshot()["queued"]["foreground"] == 1
                }
            })
            .await;
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert_eq!(
                storage.writes.snapshot()["running"],
                usize::from(!hold_backend)
            );
            assert_eq!(
                target.writes.local.snapshot()["running"],
                usize::from(hold_backend)
            );
        }
        drop((global, backend));
        let source = write(&storage, remote.id, &data).await?;
        read(&storage, &source, &data).await?;
        println!(
            "PASS canceled raw/native uploads release permits and allow retry, backend_wait={hold_backend}"
        );
    }
    let tag = storage
        .send_remote_part(
            &upload,
            1,
            data.len() as i64,
            &Md5::digest(&data).into(),
            || Box::pin(futures_util::stream::iter([Ok(data.clone())])),
        )
        .await?;
    assert!(
        storage
            .complete_remote(&upload, &[(1, tag)], data.len() as i64)
            .await?
    );
    let mut body = storage.remote_body(&upload);
    let mut received = Vec::new();
    while let Some(bytes) = body.next().await {
        received.extend_from_slice(&bytes?);
    }
    assert_eq!(received, data);
    drop(body);
    assert_eq!(storage.writes.snapshot()["running"], 0);
    assert_eq!(storage.reads.snapshot()["running"], 0);
    assert_eq!(target.writes.local.snapshot()["running"], 0);
    assert_eq!(target.reads.local.snapshot()["running"], 0);
    println!("PASS native Multipart completes after cancellation and returns the original payload");
    pool.close().await;
    drop(app);
    drop(owner);
    Ok(())
}
