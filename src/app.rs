use crate::{
    authorization::{Action, Permit, Principal},
    codec::{Chunk, MAX},
    config::{self, Budget, Config, Secrets},
    storage::Storage,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use s3s::{dto::StreamingBlob, s3_error};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

#[derive(Clone, sqlx::FromRow, Serialize)]
pub struct Bucket {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub state: String,
    pub cors: Value,
    pub website_enabled: bool,
    pub index_document: String,
    pub error_document: String,
    pub created_at: DateTime<Utc>,
}
#[derive(Clone, sqlx::FromRow, Serialize)]
pub struct StoredStream {
    pub id: Uuid,
    pub bucket_id: Uuid,
    pub object_key: String,
    pub kind: String,
    pub state: String,
    pub size: i64,
    pub etag: String,
    pub metadata: Value,
    pub public_read: bool,
    pub checksums: Value,
    pub created_at: DateTime<Utc>,
    pub touched_at: DateTime<Utc>,
}
#[derive(Clone, sqlx::FromRow, Serialize)]
pub struct Extent {
    pub stream_id: Uuid,
    pub offset_bytes: i64,
    pub length: i32,
    pub chunk_id: Option<i64>,
    pub fragment_id: Option<Uuid>,
    pub source_offset: i32,
}
impl Extent {
    fn slice(&self, data: Bytes) -> Result<Bytes> {
        let start = self.source_offset as usize;
        let end = start
            .checked_add(self.length as usize)
            .context("extent overflow")?;
        ensure!(end <= data.len(), "extent exceeds source");
        Ok(data.slice(start..end))
    }
}
#[derive(sqlx::FromRow)]
struct ReadExtent {
    #[sqlx(flatten)]
    extent: Extent,
    #[sqlx(flatten)]
    chunk: Chunk,
}
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Metadata {
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub expires: Option<String>,
    pub user: Option<s3s::dto::Metadata>,
}
pub struct Active {
    id: Uuid,
    registry: Arc<Mutex<HashMap<Uuid, usize>>>,
    wake: Arc<tokio::sync::Notify>,
}
impl Drop for Active {
    fn drop(&mut self) {
        let mut registry = self.registry.lock().unwrap();
        if let Some(n) = registry.get_mut(&self.id) {
            *n -= 1;
            if *n == 0 {
                registry.remove(&self.id);
                self.wake.notify_one();
            }
        }
    }
}
pub struct App {
    pub config: Config,
    pub budget: Budget,
    pub secrets: Arc<Secrets>,
    pub db: PgPool,
    pub storage: Arc<Storage>,
    pub statistics: crate::stats::Statistics,
    pub cleanup_state: crate::lifecycle::Cleanup,
    pub integrity_pacing: std::sync::Mutex<std::time::Instant>,
    pub local_cleanup: tokio::sync::Mutex<()>,
    pub coord: tokio::sync::Mutex<()>,
    pub active: Arc<Mutex<HashMap<Uuid, usize>>>,
    pub uploads: Arc<Semaphore>,
    pub reads: Arc<Semaphore>,
    pub slots: Arc<Semaphore>,
    pub maintenance: Arc<std::sync::atomic::AtomicBool>,
    pub gc_running: std::sync::atomic::AtomicBool,
    pub wake_gc: Arc<tokio::sync::Notify>,
    pub wake_tasks: tokio::sync::Notify,
    hashes: Mutex<HashMap<[u8; 32], Weak<tokio::sync::Mutex<()>>>>,
    pub upload_locks: Mutex<HashMap<Uuid, Weak<tokio::sync::Mutex<()>>>>,
    _file_lock: std::fs::File,
}
impl App {
    pub async fn new(
        config: Config,
        secrets: Secrets,
        budget: Budget,
        start_maintenance: bool,
    ) -> Result<(Arc<Self>, sqlx::PgConnection)> {
        tokio::fs::create_dir_all(&config.storage.data).await?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(config.storage.data.join("gateway.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)
            .context("another gateway owns this data directory")?;
        let (db, owner) = crate::db::connect(&config, &secrets, &budget).await?;
        if start_maintenance {
            sqlx::query("UPDATE mokyu_meta SET maintenance=true")
                .execute(&db)
                .await?;
        }
        let maintenance: bool = sqlx::query_scalar("SELECT maintenance FROM mokyu_meta")
            .fetch_one(&db)
            .await?;
        let maintenance = Arc::new(std::sync::atomic::AtomicBool::new(maintenance));
        let secrets = Arc::new(secrets);
        let storage = Arc::new(
            Storage::new(
                &config,
                secrets.clone(),
                &budget,
                db.clone(),
                maintenance.clone(),
            )
            .await?,
        );
        if let Err(error) = storage
            .check_identity(&db, maintenance.load(std::sync::atomic::Ordering::Acquire))
            .await
        {
            let pending:bool=sqlx::query_scalar("SELECT backend_initialized AND EXISTS(SELECT 1 FROM pending_uploads) FROM mokyu_meta").fetch_one(&db).await?;
            if !pending || error.downcast_ref::<object_store::Error>().is_none() {
                return Err(error);
            }
            tracing::warn!(error=%error,"backend identity unavailable; serving durable local uploads while remote operations wait for verification");
        }
        let app = Arc::new(Self {
            uploads: Arc::new(Semaphore::new(budget.upload_concurrency)),
            reads: Arc::new(Semaphore::new(budget.read_concurrency)),
            slots: Arc::new(Semaphore::new(budget.data_slots)),
            config,
            budget,
            secrets,
            db,
            storage,
            statistics: crate::stats::Statistics::default(),
            cleanup_state: crate::lifecycle::Cleanup::default(),
            integrity_pacing: std::sync::Mutex::new(std::time::Instant::now()),
            local_cleanup: tokio::sync::Mutex::new(()),
            coord: tokio::sync::Mutex::new(()),
            active: Arc::new(Mutex::new(HashMap::new())),
            maintenance,
            gc_running: false.into(),
            wake_gc: Arc::new(tokio::sync::Notify::new()),
            wake_tasks: tokio::sync::Notify::new(),
            hashes: Mutex::new(HashMap::new()),
            upload_locks: Mutex::new(HashMap::new()),
            _file_lock: lock,
        });
        Ok((app, owner))
    }
    pub fn pin(&self, id: Uuid) -> Active {
        *self.active.lock().unwrap().entry(id).or_default() += 1;
        Active {
            id,
            registry: self.active.clone(),
            wake: self.wake_gc.clone(),
        }
    }
    pub fn is_active(&self, id: Uuid) -> bool {
        self.active.lock().unwrap().contains_key(&id)
    }
    pub fn writable(&self) -> Result<()> {
        if self.maintenance.load(std::sync::atomic::Ordering::Acquire) {
            return Err(s3_error!(ServiceUnavailable, "gateway is in maintenance mode").into());
        }
        Ok(())
    }
    pub async fn set_cors(
        &self,
        principal: &Principal,
        bucket: Uuid,
        rules: Value,
    ) -> Result<Value> {
        self.writable()?;
        crate::http::validate_cors(&rules).map_err(|e| s3_error!(InvalidArgument, "{e}"))?;
        let mut tx = self.db.begin().await?;
        Permit::for_action(principal.clone(), bucket, Action::Settings)
            .lock(&mut tx)
            .await?;
        let result = sqlx::query_scalar(
            "UPDATE buckets SET cors=$2 WHERE id=$1 AND state='active' RETURNING cors",
        )
        .bind(bucket)
        .bind(rules)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3_error!(NoSuchBucket))?;
        tx.commit().await?;
        Ok(result)
    }
    pub async fn admit(
        &self,
        upload: bool,
    ) -> Result<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        let semaphore = if upload { &self.uploads } else { &self.reads };
        let permit = semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| s3_error!(SlowDown))?;
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| s3_error!(SlowDown))?;
        Ok((permit, slot))
    }
    pub async fn bucket(&self, name: &str, write: bool) -> Result<Bucket> {
        if write {
            self.writable()?;
        }
        let bucket: Bucket = sqlx::query_as("SELECT * FROM buckets WHERE name=$1")
            .bind(name)
            .fetch_optional(&self.db)
            .await?
            .ok_or_else(|| s3_error!(NoSuchBucket))?;
        if write && bucket.state != "active" {
            return Err(s3_error!(OperationAborted, "bucket is being purged").into());
        }
        Ok(bucket)
    }
    pub async fn authorize(
        &self,
        key: Option<&str>,
        bucket: Uuid,
        action: Action,
    ) -> Result<Permit> {
        let key = key.ok_or_else(|| s3_error!(AccessDenied))?;
        Principal::service(&self.db, key)
            .await?
            .require(&self.db, bucket, action)
            .await
    }
    pub async fn current(&self, bucket: Uuid, key: &str) -> Result<(StoredStream, Active)> {
        let _guard = self.coord.lock().await;
        let stream:StoredStream=sqlx::query_as("SELECT s.* FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=$1 AND o.key=$2 AND s.state='ready'").bind(bucket).bind(key).fetch_optional(&self.db).await?.ok_or_else(||s3_error!(NoSuchKey))?;
        let pin = self.pin(stream.id);
        Ok((stream, pin))
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn new_stream(
        &self,
        authority: &Permit,
        bucket: Uuid,
        key: &str,
        kind: &str,
        metadata: Value,
        public: bool,
        claim_object: bool,
    ) -> Result<(Uuid, Active)> {
        self.writable()?;
        if key.is_empty() || key.len() > 1024 {
            return Err(s3_error!(InvalidArgument, "invalid object key length").into());
        }
        let _guard = self.coord.lock().await;
        let id = Uuid::new_v4();
        self.writable()?;
        let mut tx = self.db.begin().await?;
        let mut authority = authority.clone();
        authority.add(bucket, Action::Write);
        if public {
            authority.add(bucket, Action::Acl);
        }
        authority.lock(&mut tx).await?;
        let state: String = sqlx::query_scalar("SELECT state FROM buckets WHERE id=$1 FOR SHARE")
            .bind(bucket)
            .fetch_one(&mut *tx)
            .await?;
        if state != "active" {
            return Err(s3_error!(OperationAborted).into());
        }
        sqlx::query("INSERT INTO streams(id,bucket_id,object_key,kind,state,metadata,public_read,write_authorization) VALUES($1,$2,$3,$4,'writing',$5,$6,$7)").bind(id).bind(bucket).bind(key).bind(kind).bind(metadata).bind(public).bind(sqlx::types::Json(authority)).execute(&mut *tx).await?;
        if claim_object {
            sqlx::query("INSERT INTO objects(bucket_id,key,write_epoch) VALUES($1,$2,$3) ON CONFLICT(bucket_id,key) DO UPDATE SET write_epoch=excluded.write_epoch").bind(bucket).bind(key).bind(id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok((id, self.pin(id)))
    }
    pub(crate) async fn reference(&self, stream: Uuid, offset: i64, c: &Chunk) -> Result<()> {
        let mut tx = self.db.begin().await?;
        let state: String = sqlx::query_scalar("SELECT state FROM chunks WHERE id=$1 FOR UPDATE")
            .bind(c.id)
            .fetch_one(&mut *tx)
            .await?;
        if state != "ready" {
            return Err(s3_error!(OperationAborted, "chunk is being reclaimed").into());
        }
        sqlx::query(
            "INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES($1,$2,$3,$4)",
        )
        .bind(stream)
        .bind(offset)
        .bind(c.raw_size)
        .bind(c.id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE chunks SET unreferenced_at=NULL WHERE id=$1")
            .bind(c.id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO cache_pins(chunk_id,pin_type,owner_id) SELECT chunk_id,CASE WHEN source_pack IS NULL THEN 'upload' ELSE 'pack' END,$2 FROM pending_uploads WHERE chunk_id=$1 ON CONFLICT DO NOTHING")
            .bind(c.id).bind(stream).execute(&mut *tx).await?;
        sqlx::query("UPDATE pending_uploads SET stream_id=$2,offset_bytes=$3 WHERE chunk_id=$1 AND owner_task IS NULL AND source_pack IS NULL AND EXISTS(SELECT 1 FROM streams WHERE id=$2 AND kind='object' AND state='writing')")
            .bind(c.id).bind(stream).bind(offset).execute(&mut *tx).await?;
        sqlx::query("UPDATE streams SET size=GREATEST(size,$2),touched_at=now() WHERE id=$1 AND state='writing'").bind(stream).bind(offset+c.raw_size as i64).execute(&mut *tx).await?;
        tx.commit().await?;
        self.seal_uploads(stream, false).await?;
        Ok(())
    }
    pub async fn put_chunk(
        &self,
        stream: Uuid,
        offset: i64,
        raw: Vec<u8>,
        should_compress: bool,
    ) -> Result<Chunk> {
        tokio::time::timeout(
            std::time::Duration::from_secs(config::seconds(
                &self.config.processing.upload_idle_timeout,
            )?),
            self.put_chunk_inner(stream, offset, raw, should_compress),
        )
        .await
        .map_err(|_| s3_error!(RequestTimeout, "upload processing made no progress"))?
    }
    async fn put_chunk_inner(
        &self,
        stream: Uuid,
        offset: i64,
        raw: Vec<u8>,
        should_compress: bool,
    ) -> Result<Chunk> {
        ensure!(!raw.is_empty() && raw.len() <= MAX, "invalid chunk length");
        let hash = *blake3::hash(&raw).as_bytes();
        let mutex = {
            let mut m = self.hashes.lock().unwrap();
            m.retain(|_, v| v.strong_count() > 0);
            if let Some(l) = m.get(&hash).and_then(Weak::upgrade) {
                l
            } else {
                let l = Arc::new(tokio::sync::Mutex::new(()));
                m.insert(hash, Arc::downgrade(&l));
                l
            }
        };
        let _guard = mutex.lock().await;
        let algorithm = &self.config.encryption.algorithm;
        let key_id = if algorithm == "none" {
            ""
        } else {
            &self.secrets.active_key
        };
        let old:Option<Chunk>=sqlx::query_as("SELECT * FROM chunks WHERE hash=$1 AND raw_size=$2 AND algorithm=$3 AND key_id=$4 AND state='ready'")
            .bind(hash.as_slice()).bind(raw.len() as i32).bind(algorithm).bind(key_id).fetch_optional(&self.db).await?;
        if let Some(c) = old {
            if self.config.cache.upload_cache {
                let packed:bool=sqlx::query_scalar("SELECT pack_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=$1) FROM chunks WHERE id=$1")
                    .bind(c.id).fetch_one(&self.db).await?;
                if packed {
                    let (data, compressed) = self
                        .storage
                        .cache_payload(&c, Bytes::copy_from_slice(&raw))
                        .await?;
                    if self
                        .stage_chunk(stream, offset, &c, data, compressed, false)
                        .await?
                    {
                        return Ok(c);
                    }
                }
            }
            match self.reference(stream, offset, &c).await {
                Ok(()) => return Ok(c),
                Err(e) if e.downcast_ref::<s3s::S3Error>().is_some() => {}
                Err(e) => return Err(e),
            }
        }
        // A lost request never re-encrypts its previous ID or physical object key.
        sqlx::query("UPDATE chunks SET state='failed',unreferenced_at=COALESCE(unreferenced_at,now()) WHERE hash=$1 AND raw_size=$2 AND algorithm=$3 AND key_id=$4 AND state IN ('preparing','uploading')")
            .bind(hash.as_slice()).bind(raw.len() as i32).bind(algorithm).bind(key_id).execute(&self.db).await?;
        let mut tx = self.db.begin().await?;
        let c:Chunk=sqlx::query_as("INSERT INTO chunks(storage_id,hash,raw_size,algorithm,key_id,owner_stream,state,unreferenced_at) VALUES($1,$2,$3,$4,$5,$6,'preparing',now()) RETURNING *")
            .bind(Uuid::new_v4()).bind(hash.as_slice()).bind(raw.len() as i32).bind(algorithm).bind(key_id).bind(stream).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("chunk-allocated").await;
        let (c, encoded, cache) = self.storage.encode(c, raw, should_compress).await?;
        sqlx::query("UPDATE chunks SET stored_size=$2,compressed=$3,nonce=$4,state='uploading' WHERE id=$1 AND state='preparing'")
            .bind(c.id).bind(c.stored_size).bind(c.compressed).bind(&c.nonce).execute(&self.db).await?;
        if self
            .stage_chunk(
                stream,
                offset,
                &c,
                cache.clone(),
                crate::codec::cache_compressed(
                    &c,
                    self.config.cache.min_compression_savings_percent,
                ),
                true,
            )
            .await?
        {
            return Ok(c);
        }
        self.storage.register_location(&c).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("chunk-uploading").await;
        self.storage.put(&c, encoded, cache).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("chunk-stored").await;
        let mut tx = self.db.begin().await?;
        let changed=sqlx::query("UPDATE chunks SET state='ready',unreferenced_at=NULL,owner_stream=NULL WHERE id=$1 AND state='uploading'").bind(c.id).execute(&mut *tx).await?.rows_affected();
        sqlx::query("UPDATE chunk_locations SET state='ready',unreferenced_at=NULL,stored_at=clock_timestamp() WHERE id=$1 AND state='uploading'").bind(c.encoding_id).execute(&mut *tx).await?;
        if changed != 1 {
            return Err(s3_error!(OperationAborted).into());
        }
        sqlx::query(
            "INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES($1,$2,$3,$4)",
        )
        .bind(stream)
        .bind(offset)
        .bind(c.raw_size)
        .bind(c.id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE streams SET size=$2,touched_at=now() WHERE id=$1 AND state='writing'")
            .bind(stream)
            .bind(offset + c.raw_size as i64)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(c)
    }
    pub async fn tail(&self, stream: Uuid, offset: i64, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO fragments(id,size,hash,owner_stream) VALUES($1,$2,$3,$4)")
            .bind(id)
            .bind(data.len() as i32)
            .bind(blake3::hash(data).as_bytes().as_slice())
            .bind(stream)
            .execute(&self.db)
            .await?;
        let _lease = self
            .storage
            .disk
            .write_fragment(id, data)
            .await
            .map_err(|e| {
                tracing::warn!(error=%e,"multipart fragment could not be persisted");
                s3_error!(SlowDown, "local multipart storage unavailable")
            })?;
        let mut tx = self.db.begin().await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("fragment-persisted").await;
        sqlx::query("UPDATE fragments SET sealed=true WHERE id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO extents(stream_id,offset_bytes,length,fragment_id) VALUES($1,$2,$3,$4)",
        )
        .bind(stream)
        .bind(offset)
        .bind(data.len() as i32)
        .bind(id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE streams SET size=$2,touched_at=now() WHERE id=$1")
            .bind(stream)
            .bind(offset + data.len() as i64)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn extent_bytes(&self, e: &Extent) -> Result<Bytes> {
        self.extent_bytes_with(e, &crate::storage::ReadContext::default())
            .await
    }
    pub async fn extent_bytes_with(
        &self,
        e: &Extent,
        context: &crate::storage::ReadContext,
    ) -> Result<Bytes> {
        tokio::time::timeout(
            std::time::Duration::from_secs(config::seconds(
                &self.config.processing.upload_idle_timeout,
            )?),
            self.extent_bytes_inner(e, context),
        )
        .await
        .context("multipart reconstruction made no progress before timeout")?
    }
    async fn extent_bytes_inner(
        &self,
        e: &Extent,
        context: &crate::storage::ReadContext,
    ) -> Result<Bytes> {
        let data = if let Some(id) = e.chunk_id {
            let c: Chunk = sqlx::query_as("SELECT * FROM chunks WHERE id=$1 AND state='ready'")
                .bind(id)
                .fetch_optional(&self.db)
                .await?
                .context("referenced chunk not ready")?;
            self.storage.get_with(&c, Some(context)).await?
        } else {
            let id = e.fragment_id.context("extent has no source")?;
            let (size, hash): (i32, Vec<u8>) =
                sqlx::query_as("SELECT size,hash FROM fragments WHERE id=$1")
                    .bind(id)
                    .fetch_one(&self.db)
                    .await?;
            let data = crate::storage::read_bounded(self.storage.disk.fragment_path(id), MAX)
                .await
                .context("multipart fragment missing")?;
            ensure!(
                data.len() == size as usize
                    && data.len() <= MAX
                    && blake3::hash(&data).as_bytes() == hash.as_slice(),
                "local fragment integrity mismatch"
            );
            Bytes::from(data)
        };
        e.slice(data)
    }
    pub async fn extents(&self, stream: Uuid, from: i64, until: i64) -> Result<Vec<Extent>> {
        Ok(sqlx::query_as("SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=$2 AND offset_bytes<$3 ORDER BY offset_bytes LIMIT 64").bind(stream).bind(from).bind(until).fetch_all(&self.db).await?)
    }
    async fn read_extents(&self, stream: Uuid, from: i64, until: i64) -> Result<Vec<ReadExtent>> {
        // Bound the mapping page before joining; a missing ready chunk must fail decoding the row.
        Ok(sqlx::query_as("SELECT e.*,c.* FROM (SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=$2 AND offset_bytes<$3 ORDER BY offset_bytes LIMIT 64) e LEFT JOIN chunks c ON c.id=e.chunk_id AND c.state='ready' ORDER BY e.offset_bytes")
            .bind(stream).bind(from).bind(until).fetch_all(&self.db).await?)
    }
    pub fn body(
        self: &Arc<Self>,
        stream: StoredStream,
        pin: Active,
        start: i64,
        end: i64,
        permits: (OwnedSemaphorePermit, OwnedSemaphorePermit),
        range: bool,
    ) -> StreamingBlob {
        let app = self.clone();
        let body = async_stream::try_stream! {
            let _pin=pin;let _permits=permits;let read_context=Arc::new(crate::storage::ReadContext::user(range));
            let mut cursor:Option<i64>=sqlx::query_scalar("SELECT offset_bytes FROM extents WHERE stream_id=$1 AND offset_bytes<=$2 ORDER BY offset_bytes DESC LIMIT 1").bind(stream.id).bind(start).fetch_optional(&app.db).await?;
            let mut position=start;
            while position<end {
                let rows=app.read_extents(stream.id,cursor.context("object mapping is incomplete")?,end).await?;
                (!rows.is_empty()).then_some(()).context("object mapping is incomplete")?;
                let mut loaded=futures_util::stream::iter(rows.into_iter().map(|row| {let app=app.clone();let context=read_context.clone();async move{let data=app.storage.get_with(&row.chunk,Some(&context)).await?;let data=row.extent.slice(data)?;Ok::<_,anyhow::Error>((row.extent,data))}})).buffered(2);
                while let Some(result)=loaded.next().await {
                    let (row,data)=result?;
                    (row.offset_bytes<=position && row.offset_bytes+row.length as i64>position).then_some(()).context("object mapping has a gap")?;
                    let skip=(position-row.offset_bytes) as usize;let count=((end-position) as usize).min(data.len()-skip);
                    position+=count as i64;cursor=Some(row.offset_bytes+row.length as i64);
                    if let Some(id)=row.chunk_id {app.storage.access.chunk(id,range,count,false);}
                    yield data.slice(skip..skip+count);
                    if position==end {break;}
                }
            }
        };
        let mut body = Box::pin(body.map(|r: Result<Bytes>| r.map_err(std::io::Error::other)));
        let idle = std::time::Duration::from_secs(
            config::seconds(&self.config.processing.read_idle_timeout)
                .expect("validated read timeout"),
        );
        let limited = async_stream::stream! {
            loop {match tokio::time::timeout(idle,body.next()).await {
                Ok(Some(result))=>{let failed=result.is_err();yield result;if failed {break;}},
                Ok(None)=>break,
                Err(_)=>{yield Err(std::io::Error::new(std::io::ErrorKind::TimedOut,"read made no progress"));break;}
            }}
        };
        let body = Mutex::new(Box::pin(limited));
        StreamingBlob::wrap(futures_util::stream::poll_fn(move |cx| {
            futures_util::Stream::poll_next(body.lock().unwrap().as_mut(), cx)
        }))
    }
    pub async fn finish_stream(
        &self,
        id: Uuid,
        size: i64,
        etag: &str,
        checksums: Value,
    ) -> Result<()> {
        let changed=sqlx::query("UPDATE streams SET size=$2,etag=$3,checksums=$4,touched_at=now() WHERE id=$1 AND state='writing'").bind(id).bind(size).bind(etag).bind(checksums).execute(&self.db).await?.rows_affected();
        if changed != 1 {
            return Err(s3_error!(OperationAborted).into());
        }
        Ok(())
    }
    pub async fn publish(
        &self,
        id: Uuid,
        if_match: Option<&s3s::dto::ETagCondition>,
        if_none: Option<&s3s::dto::ETagCondition>,
    ) -> Result<()> {
        self.publish_complete(id, if_match, if_none, None).await
    }
    pub async fn publish_complete(
        &self,
        id: Uuid,
        if_match: Option<&s3s::dto::ETagCondition>,
        if_none: Option<&s3s::dto::ETagCondition>,
        completion: Option<(Uuid, String, Value)>,
    ) -> Result<()> {
        self.writable()?;
        let _coord = self.coord.lock().await;
        self.writable()?;
        let mut tx = self.db.begin().await?;
        let authority: sqlx::types::Json<Permit> =
            sqlx::query_scalar("SELECT write_authorization FROM streams WHERE id=$1")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
        authority.lock(&mut tx).await?;
        let s: StoredStream = sqlx::query_as("SELECT * FROM streams WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
        let bucket: String = sqlx::query_scalar("SELECT state FROM buckets WHERE id=$1 FOR SHARE")
            .bind(s.bucket_id)
            .fetch_one(&mut *tx)
            .await?;
        if bucket != "active" {
            return Err(s3_error!(OperationAborted).into());
        }
        let (old, epoch): (Option<Uuid>, Uuid) = sqlx::query_as(
            "SELECT stream_id,write_epoch FROM objects WHERE bucket_id=$1 AND key=$2 FOR UPDATE",
        )
        .bind(s.bucket_id)
        .bind(&s.object_key)
        .fetch_one(&mut *tx)
        .await?;
        if epoch != id || s.state != "writing" {
            return Err(s3_error!(OperationAborted, "write was superseded").into());
        }
        let previous: Option<String> = if let Some(old) = old {
            Some(
                sqlx::query_scalar("SELECT etag FROM streams WHERE id=$1")
                    .bind(old)
                    .fetch_one(&mut *tx)
                    .await?,
            )
        } else {
            None
        };
        check_write_conditions(previous.as_deref(), if_match, if_none)?;
        let invalid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=$1 AND (e.fragment_id IS NOT NULL OR c.state<>'ready'))").bind(id).fetch_one(&mut *tx).await?;
        ensure!(!invalid, "cannot publish an incomplete generation");
        let (end,contiguous):(i64,bool)=sqlx::query_as("SELECT COALESCE(max(offset_bytes+length),0),COALESCE(bool_and(offset_bytes=previous),true) FROM (SELECT offset_bytes,length,lag(offset_bytes+length,1,0::bigint) OVER (ORDER BY offset_bytes) AS previous FROM extents WHERE stream_id=$1) e").bind(id).fetch_one(&mut *tx).await?;
        ensure!(
            end == s.size && contiguous,
            "cannot publish a mapping with gaps or overlaps"
        );
        sqlx::query("UPDATE objects SET stream_id=$3 WHERE bucket_id=$1 AND key=$2")
            .bind(s.bucket_id)
            .bind(&s.object_key)
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE streams SET state='ready',touched_at=now() WHERE id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        if let Some(old) = old {
            sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1")
                .bind(old)
                .execute(&mut *tx)
                .await?;
        }
        if let Some((upload, hash, result)) = completion {
            let changed=sqlx::query("UPDATE uploads SET state='completed',result=$3,output_stream=$4,touched_at=now() WHERE id=$1 AND state='completing' AND manifest_hash=$2").bind(upload).bind(hash).bind(result).bind(id).execute(&mut *tx).await?.rows_affected();
            if changed != 1 {
                return Err(s3_error!(OperationAborted).into());
            }
            sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id IN (SELECT stream_id FROM parts WHERE upload_id=$1)").bind(upload).execute(&mut *tx).await?;
            sqlx::query("DELETE FROM parts WHERE upload_id=$1")
                .bind(upload)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        self.wake_gc.notify_one();
        self.seal_uploads(id, true).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("object-published").await;
        Ok(())
    }
    pub async fn delete_object(
        &self,
        principal: &Principal,
        bucket: Uuid,
        key: &str,
    ) -> Result<()> {
        self.change_object(principal, bucket, key, None, None).await
    }
    pub async fn change_object(
        &self,
        principal: &Principal,
        bucket: Uuid,
        key: &str,
        expected: Option<Uuid>,
        public: Option<bool>,
    ) -> Result<()> {
        self.writable()?;
        let _coord = self.coord.lock().await;
        self.writable()?;
        let mut tx = self.db.begin().await?;
        Permit::for_action(
            principal.clone(),
            bucket,
            if public.is_some() {
                Action::Acl
            } else {
                Action::Delete
            },
        )
        .lock(&mut tx)
        .await?;
        let state: String = sqlx::query_scalar("SELECT state FROM buckets WHERE id=$1 FOR SHARE")
            .bind(bucket)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3_error!(NoSuchBucket))?;
        if state != "active" {
            return Err(s3_error!(OperationAborted, "bucket is being purged").into());
        }
        let old: Option<Uuid> = sqlx::query_scalar(
            "SELECT stream_id FROM objects WHERE bucket_id=$1 AND key=$2 FOR UPDATE",
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        if expected.is_some() && expected != old {
            return Err(s3_error!(
                PreconditionFailed,
                "object changed; refresh before retrying"
            )
            .into());
        }
        if let Some(public) = public {
            let id = old.ok_or_else(|| s3_error!(NoSuchKey))?;
            sqlx::query("UPDATE streams SET public_read=$2 WHERE id=$1")
                .bind(id)
                .bind(public)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Ok(());
        }
        sqlx::query("INSERT INTO objects(bucket_id,key,write_epoch) VALUES($1,$2,$3) ON CONFLICT(bucket_id,key) DO UPDATE SET stream_id=NULL,write_epoch=excluded.write_epoch").bind(bucket).bind(key).bind(Uuid::new_v4()).execute(&mut *tx).await?;
        if let Some(old) = old {
            sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1")
                .bind(old)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        self.wake_gc.notify_one();
        Ok(())
    }
    pub async fn status(&self) -> Result<Value> {
        let (paused, maintenance): (bool, bool) =
            sqlx::query_as("SELECT gc_paused,maintenance FROM mokyu_meta")
                .fetch_one(&self.db)
                .await?;
        let mut status = json!({"version":env!("CARGO_PKG_VERSION"),"resources":self.budget,"local_bytes":self.storage.disk.used(),"gc_paused":paused,"maintenance":maintenance,"backend_gets":self.storage.backend_gets.load(std::sync::atomic::Ordering::Relaxed),"cache_hits":self.storage.cache_hits.load(std::sync::atomic::Ordering::Relaxed),"backend_puts":self.storage.backend_puts.load(std::sync::atomic::Ordering::Relaxed),"backend_deletes":self.storage.backend_deletes.load(std::sync::atomic::Ordering::Relaxed),"backend_read_bytes":self.storage.backend_read_bytes.load(std::sync::atomic::Ordering::Relaxed),"backend_write_bytes":self.storage.backend_write_bytes.load(std::sync::atomic::Ordering::Relaxed),"cache_hit_bytes":self.storage.cache_hit_bytes.load(std::sync::atomic::Ordering::Relaxed),"db_pool_size":self.db.size(),"db_pool_idle":self.db.num_idle(),"data_slots_available":self.slots.available_permits(),"active_streams":self.active.lock().unwrap().len()});
        status["runtime"] = self.statistics.runtime();
        status["cleanup"] = self.cleanup_status();
        status["storage"] = self.statistics.inventory(crate::config::seconds(
            &self.config.statistics.refresh_interval,
        )?);
        status["io"] = self.storage.statistics();
        status["upload_cache"] = self.upload_cache_status().await?;
        status["process_memory"] = crate::stats::process_memory().await;
        status["gc_running"] = json!(self.gc_running.load(std::sync::atomic::Ordering::Relaxed));
        status["upload_slots_available"] = json!(self.uploads.available_permits());
        status["read_slots_available"] = json!(self.reads.available_permits());
        Ok(status)
    }
}
pub fn check_write_conditions(
    current: Option<&str>,
    if_match: Option<&s3s::dto::ETagCondition>,
    if_none: Option<&s3s::dto::ETagCondition>,
) -> Result<()> {
    use s3s::dto::{ETag, ETagCondition};
    let matches = |c: &ETagCondition| match c {
        ETagCondition::Any => current.is_some(),
        ETagCondition::ETag(e) => current.is_some_and(|v| ETag::Strong(v.into()).strong_cmp(e)),
    };
    if if_match.is_some_and(|c| !matches(c)) || if_none.is_some_and(matches) {
        return Err(s3_error!(PreconditionFailed).into());
    }
    Ok(())
}
pub fn internal(error: anyhow::Error) -> s3s::S3Error {
    match error.downcast::<s3s::S3Error>() {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(error=%e,"request failed");
            s3_error!(InternalError, "request could not be completed")
        }
    }
}
