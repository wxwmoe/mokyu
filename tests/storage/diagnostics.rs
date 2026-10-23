use super::*;
use sqlx::sqlite::SqlitePoolOptions;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a fresh MOKYU_TEST_SQLITE_CONFIG and dedicated fault directory"]
async fn storage_diagnostics_count_shared_failures_and_cache_fallback() -> Result<()> {
    ensure!(std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")? == "isolated-only");
    let file = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&file))?;
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
            "name":"diagnostics", "type":"local", "path":backend_path, "initialize":true
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
    let raw = Bytes::from(vec![73; 128 * 1024]);
    let chunk = Chunk {
        uuid: Uuid::now_v7(),
        id: 1.into(),
        encoding_id: 1.into(),
        location_id: Uuid::new_v4(),
        storage_id: Some(store.id),
        hash: blake3::hash(&raw).as_bytes().to_vec(),
        raw_size: raw.len() as i32,
        stored_size: Some(raw.len() as i32),
        algorithm: "none".into(),
        block_key_id: None,
        compressed: false,
        nonce: None,
        format: 2,
        state: "ready".into(),
        created_at: chrono::Utc::now(),
    };
    sqlx::query("INSERT INTO chunks(uuid,id,storage_id,hash,raw_size,encryption_algorithm,state,current_encoding_id,standalone_size) VALUES(?,1,?,?,?,'none','ready',1,?)").bind(chunk.uuid)
        .bind(store.id).bind(&chunk.hash).bind(chunk.raw_size).bind(chunk.stored_size).execute(&pool).await?;
    sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,location_id,storage_id,standalone_size,state,encryption_algorithm,encoding_fingerprint) VALUES(1,1,?,?,?,'ready','none','{}')")
        .bind(chunk.location_id).bind(store.id).bind(chunk.stored_size).execute(&pool).await?;
    let marker =
        PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?).join("storage-read-permitted");
    tokio::fs::write(&marker, b"").await?;
    let mut readers = Vec::new();
    for _ in 0..16 {
        let storage = storage.clone();
        let chunk = chunk.clone();
        readers.push(tokio::spawn(
            async move { storage.get_with(&chunk, None).await },
        ));
    }
    let joined = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if marker.with_extension("hit").exists()
                && storage
                    .misses
                    .lock()
                    .unwrap()
                    .get(&(chunk.id, chunk.location_id, false))
                    .is_some_and(|entry| entry.strong_count() == 16)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    tokio::fs::remove_file(&marker).await?;
    joined?;
    for reader in readers {
        ensure!(reader.await?.is_err(), "missing source succeeded");
    }
    let stats = storage.statistics();
    assert_eq!(stats["backend"]["get"]["failed"], 1);
    assert_eq!(stats["diagnostics"]["backend"]["missing"], 1);
    assert_eq!(stats["diagnostics"]["content"]["hash"], 0);

    let path = storage.path_for(store.id, chunk.location_id)?;
    let mut corrupt = raw.to_vec();
    corrupt[0] ^= 1;
    storage.put_path_for(store.id, &path, corrupt).await?;
    ensure!(storage.get_with(&chunk, None).await.is_err());
    let stats = storage.statistics();
    assert_eq!(stats["backend"]["get"]["failed"], 1);
    assert_eq!(stats["diagnostics"]["content"]["hash"], 1);
    storage
        .put_path_for(store.id, &path, vec![0; MAX + 17])
        .await?;
    ensure!(storage.get_with(&chunk, None).await.is_err());
    assert_eq!(storage.statistics()["diagnostics"]["content"]["length"], 1);
    assert_eq!(storage.statistics()["backend"]["get"]["failed"], 1);

    storage.put_path_for(store.id, &path, raw.to_vec()).await?;
    // Exercise cache I/O with the local fixture; public local profiles disable caching.
    Arc::get_mut(storage.targets.write().unwrap().get_mut(&store.id).unwrap())
        .unwrap()
        .cache_enabled = true;
    let _fills = storage
        .fills
        .clone()
        .acquire_many_owned(storage.fills.available_permits() as u32)
        .await?;
    // An invalid cache entry remains skippable even when its path is a directory.
    let id = cache::cache_id(&chunk);
    let cache_path = storage.cache_path(id, false);
    tokio::fs::create_dir_all(cache_path.parent().unwrap()).await?;
    tokio::fs::create_dir(&cache_path).await?;
    storage
        .fifo
        .lock()
        .await
        .insert(id, raw.len() as u64, false);
    storage.disk.account_existing(Area::Cache, raw.len() as u64);
    assert_eq!(storage.get_with(&chunk, None).await?, raw);
    assert_eq!(storage.statistics()["diagnostics"]["cache"]["invalid"], 1);
    assert_eq!(storage.statistics()["diagnostics"]["cache"]["fallback"], 1);
    assert!(!storage.fifo.lock().await.entries[&id].valid);
    // Missing indexed cache files have their own counter and never taint content.
    tokio::fs::remove_dir(&cache_path).await?;
    storage
        .fifo
        .lock()
        .await
        .entries
        .get_mut(&id)
        .unwrap()
        .valid = true;
    assert!(storage.cached_file(&chunk, id).await?.is_none());
    assert_eq!(storage.statistics()["diagnostics"]["cache"]["missing"], 1);
    assert_eq!(storage.statistics()["diagnostics"]["content"]["hash"], 1);
    storage.delete_path_for(store.id, &path).await?;
    storage.delete_path_for(store.id, &path).await?;
    assert_eq!(storage.statistics()["backend"]["delete"]["failed"], 0);
    assert!(storage.head_path_for(store.id, &path).await.is_err());
    pool.close().await;
    drop(app);
    drop(owner);
    Ok(())
}
