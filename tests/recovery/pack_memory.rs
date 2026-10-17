use super::*;
use crate::content::{compression::Strategy, pack::Pack};
use sqlx::sqlite::SqlitePoolOptions;
use std::path::Path as FilePath;

struct Barrier(PathBuf);
impl Barrier {
    fn new(name: &str, panic: bool) -> Result<Self> {
        let path = PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?).join(name);
        let _ = std::fs::remove_file(path.with_extension("hit"));
        std::fs::write(&path, if panic { b"panic".as_slice() } else { b"" })?;
        Ok(Self(path))
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
    .expect("pack resource condition timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a fresh MOKYU_TEST_SQLITE_CONFIG and dedicated fault directory"]
async fn pack_memory_follows_blocking_work_and_last_reader() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let file = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, budget) = Config::load(FilePath::new(&file))?;
    let database = config
        .database
        .sqlite_path(&config.instance.data_path)
        .unwrap();
    assert!(!database.exists(), "fixture database must be new");
    let backend_path = config.instance.data_path.join("backend");
    let (app, owner) = crate::app::App::new(config, secrets, budget, false, true, "single").await?;
    let store = crate::control::storage::create_record(
        &app,
        serde_json::from_value(serde_json::json!({
            "name":"pack-memory", "type":"local", "path":backend_path, "initialize":true
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
    let store_id = store.id;
    let members: Vec<_> = (1..=2)
        .map(|id| {
            let raw = Bytes::from(vec![id as u8; 128 * 1024]);
            (
                Chunk {
                    id,
                    encoding_id: id,
                    location_id: Uuid::new_v4(),
                    storage_id: Some(store_id),
                    hash: blake3::hash(&raw).as_bytes().to_vec(),
                    raw_size: raw.len() as i32,
                    stored_size: None,
                    algorithm: "none".into(),
                    block_key_id: None,
                    compressed: false,
                    nonce: None,
                    format: 1,
                    state: "ready".into(),
                    created_at: chrono::Utc::now(),
                },
                raw,
            )
        })
        .collect();
    let template = Pack {
        id: 1,
        location_id: Uuid::new_v4(),
        storage_id: Some(store_id),
        algorithm: "none".into(),
        block_key_id: None,
        raw_size: members.iter().map(|(c, _)| i64::from(c.raw_size)).sum(),
        stored_size: None,
        compressed: false,
        nonce: None,
        digest: None,
        member_count: 2,
        state: "ready".into(),
        created_at: chrono::Utc::now(),
    };
    let (pack, encoded) = crate::content::pack::encode(
        template.clone(),
        &members,
        &storage.secrets,
        &storage.compression,
        Strategy::Never,
        false,
    )?;
    sqlx::query("INSERT INTO packs(id,location_id,storage_id,encryption_algorithm,raw_size,stored_size,digest,member_count,state,pack_fingerprint) VALUES(?,?,?,'none',?,?,?,2,'ready','{}')")
        .bind(pack.id).bind(pack.location_id).bind(store_id).bind(pack.raw_size)
        .bind(pack.stored_size).bind(&pack.digest).execute(&pool).await?;
    let mut offset = 0i64;
    for (index, (chunk, _)) in members.iter().enumerate() {
        sqlx::query("INSERT INTO chunks(id,storage_id,hash,raw_size,encryption_algorithm,state,current_encoding_id,pack_id) VALUES(?,?,?,?,'none','ready',?,?)")
            .bind(chunk.id).bind(store_id).bind(&chunk.hash).bind(chunk.raw_size).bind(chunk.id).bind(pack.id).execute(&pool).await?;
        sqlx::query(
            "INSERT INTO pack_members(pack_id,ordinal,chunk_id,offset_bytes) VALUES(?,?,?,?)",
        )
        .bind(pack.id)
        .bind(index as i64)
        .bind(chunk.id)
        .bind(offset)
        .execute(&pool)
        .await?;
        offset += i64::from(chunk.raw_size);
    }
    let capacity = storage.pack_memory.available_permits();
    let work_capacity = storage.pack_work.available_permits();
    let cpu = storage.cpu.available_permits();
    for operation in ["decode", "inspect", "encode", "chunk-encode"] {
        for outcome in ["cancel", "timeout", "error", "panic", "success"] {
            let mut source = encoded.clone();
            if outcome == "error" {
                *source.last_mut().unwrap() ^= 1;
            }
            storage
                .put_path_for(
                    store_id,
                    &storage.pack_path_for(store_id, pack.location_id)?,
                    source,
                )
                .await?;
            let inspection = Uuid::new_v4();
            sqlx::query("INSERT INTO tasks(id,task_type,state) VALUES(?,'integrity','running')")
                .bind(inspection)
                .execute(&pool)
                .await?;
            let marker = Barrier::new(&format!("pack-{operation}"), outcome == "panic")?;
            let copy = storage.clone();
            let current = pack.clone();
            let template = template.clone();
            let inputs = members.clone();
            let task = tokio::spawn(async move {
                tokio::time::timeout(
                    Duration::from_secs(if outcome == "timeout" { 1 } else { 20 }),
                    async {
                        match operation {
                            "decode" => Ok(Some(copy.load_pack(current).await?)),
                            "inspect" => {
                                copy.inspect(
                                    inspection,
                                    &inputs[0].0,
                                    crate::worker::integrity::Mode::Full,
                                    &mut serde_json::Value::Null,
                                )
                                .await?;
                                Ok(None)
                            }
                            _ => {
                                let memory = copy.pack_work(current.payload_size()?).await?;
                                if operation == "encode" {
                                    let mut template = template;
                                    if outcome == "error" {
                                        template.raw_size += 1;
                                    }
                                    let (pack, bytes) = copy
                                        .encode_pack(
                                            template,
                                            inputs.clone(),
                                            Strategy::Never,
                                            false,
                                            &memory,
                                        )
                                        .await?;
                                    let decoded = crate::content::pack::decode(
                                        &pack,
                                        bytes,
                                        &copy.secrets,
                                        &copy.compression,
                                    )?;
                                    assert_eq!(decoded.chunk(&inputs[0].0)?, inputs[0].1);
                                } else {
                                    let (mut chunk, raw) = inputs[0].clone();
                                    if outcome == "error" {
                                        chunk.raw_size += 1;
                                    }
                                    let (chunk, bytes, _) = copy
                                        .encode(chunk, raw.to_vec(), false, Some(&memory))
                                        .await?;
                                    assert_eq!(copy.decode(&chunk, bytes).await?.0, raw);
                                }
                                Ok::<_, anyhow::Error>(None)
                            }
                        }
                    },
                )
                .await
            });
            until(|| marker.0.with_extension("hit").exists()).await;
            if matches!(outcome, "cancel" | "timeout") {
                if outcome == "cancel" {
                    task.abort();
                    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
                } else {
                    assert!(task.await?.is_err());
                }
                let charged = if matches!(operation, "encode" | "chunk-encode") {
                    storage.pack_work.available_permits() < work_capacity
                } else {
                    storage.pack_memory.available_permits() < capacity
                };
                assert!(
                    charged,
                    "{operation} released memory while blocking work still ran"
                );
                assert_eq!(storage.cpu.available_permits(), cpu - 1);
                marker.release();
            } else {
                if outcome != "panic" {
                    marker.release();
                }
                let result = task.await??;
                marker.release();
                if matches!(outcome, "error" | "panic") {
                    assert!(result.is_err(), "{operation} accepted {outcome}");
                } else if let Some(loaded) = result? {
                    assert!(storage.pack_memory.available_permits() < capacity);
                    let shared = loaded.clone();
                    drop(loaded);
                    assert!(storage.pack_memory.available_permits() < capacity);
                    let raw = shared.data.chunk(&members[0].0)?;
                    drop(shared);
                    assert_eq!(raw, members[0].1);
                }
            }
            until(|| {
                storage.pack_memory.available_permits() == capacity
                    && storage.pack_work.available_permits() == work_capacity
                    && storage.cpu.available_permits() == cpu
            })
            .await;
            println!("PASS {operation} {outcome}: memory and CPU ownership recovered");
        }
    }
    pool.close().await;
    drop(app);
    drop(owner);
    Ok(())
}
