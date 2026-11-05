use super::{
    Format, InputLimits, Limits, Output, Pipeline, Source,
    cache::{Cache, Entry, Metadata},
    probe, render,
};
use crate::{
    app::{Active, App, StoredStream},
    storage::{Disk, local::Reservation},
};
use anyhow::{Result, ensure};
use axum::body::{Body, Bytes};
use futures_util::StreamExt;
use std::{
    future::Future,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{OwnedSemaphorePermit, Semaphore},
};
use uuid::Uuid;

pub(crate) async fn recover(disk: &Arc<Disk>, deferred: &super::cache::Deferred) -> Result<()> {
    let root = disk.root.join("cache/images");
    let mut builder = tokio::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    builder.mode(0o700);
    builder.create(&root).await?;
    let mut files = tokio::fs::read_dir(&root).await?;
    while let Some(file) = files.next_entry().await? {
        if file.file_type().await?.is_dir()
            && file
                .file_name()
                .to_str()
                .and_then(|n| n.strip_prefix(".work-"))
                .and_then(|n| Uuid::parse_str(n).ok())
                .is_some()
        {
            if let Err(error) = tokio::fs::remove_dir_all(file.path()).await {
                let bytes = super::cache::remaining(&file.path()).await;
                disk.account_existing(crate::storage::Area::Images, bytes);
                super::cache::deferred(&mut deferred.lock().unwrap(), file.path(), bytes);
                tracing::warn!(%error, "image workspace cleanup deferred");
            }
        }
    }
    Ok(())
}
pub(super) struct Temporary {
    pub path: PathBuf,
    pub charge: Option<Reservation>,
    pub retained: bool,
    pub deferred: super::cache::Deferred,
}
impl Drop for Temporary {
    fn drop(&mut self) {
        if self.retained {
            return;
        }
        if let Err(error) = std::fs::remove_dir_all(&self.path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                let bytes = self.charge.as_ref().map_or(0, |charge| charge.bytes);
                if let Some(charge) = self.charge.take() {
                    charge.commit();
                }
                super::cache::deferred(
                    &mut self.deferred.lock().unwrap(),
                    self.path.clone(),
                    bytes,
                );
                tracing::warn!(%error, "image workspace cleanup failed");
            }
        }
    }
}
pub(crate) fn capacity(units: u32) -> serde_json::Value {
    let available = units.min(768);
    serde_json::json!({"memory_mib": available, "minimum_mib": super::engine::MIN_MEMORY_MIB, "enabled_by_budget": available >= super::engine::MIN_MEMORY_MIB})
}
pub(crate) struct Resources {
    pub disk: Arc<Disk>,
    pub cpu: Arc<Semaphore>,
    pub work: Arc<Semaphore>,
    pub slots: Arc<Semaphore>,
    pub memory: u64,
    pub access: Arc<crate::status::access::Access>,
}
pub(crate) struct Service {
    pub cache: Cache,
    resources: Resources,
    flights: crate::runtime::locks::Locks<String>,
    waiting: Arc<Semaphore>,
    job: Arc<Semaphore>,
}
pub(crate) struct Product {
    pub source: Source,
    pub image: Source,
    pub output: Output,
    pub elapsed_ms: u64,
    pub cache_hit: bool,
    pub run: Option<Arc<crate::node::RunLease>>,
    pub source_bytes: i64,
    pub etag: String,
    entry: Arc<Entry>,
    file: tokio::fs::File,
    slot: Arc<OwnedSemaphorePermit>,
    access: Arc<crate::status::access::Access>,
    pub read: Arc<Mutex<crate::status::access::DerivedRead>>,
    spans: Option<Arc<Vec<crate::app::responses::DemandSpan>>>,
}
impl Product {
    pub fn path(&self) -> PathBuf {
        self.entry.path.join("output")
    }
    pub async fn body(self) -> Result<Body> {
        let length = self.output.bytes;
        self.range(0..length).await
    }
    pub async fn range(mut self, range: std::ops::Range<u64>) -> Result<Body> {
        ensure!(
            range.start <= range.end && range.end <= self.output.bytes,
            "invalid image range"
        );
        self.file
            .seek(std::io::SeekFrom::Start(range.start))
            .await?;
        let stream = futures_util::stream::try_unfold(
            (self, range.end - range.start, false),
            |(mut product, remaining, served)| async move {
                if remaining == 0 {
                    return Ok::<_, std::io::Error>(None);
                }
                if let Some(run) = &product.run {
                    run.check().map_err(std::io::Error::other)?;
                }
                let mut bytes = vec![0u8; remaining.min(65536) as usize];
                let _io = product
                    .entry
                    .disk
                    .io
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(std::io::Error::other)?;
                let n = product.file.read(&mut bytes).await?;
                if n == 0 {
                    return Err(std::io::ErrorKind::UnexpectedEof.into());
                }
                bytes.truncate(n);
                if !served {
                    if product.cache_hit
                        && let Some(spans) = &product.spans
                    {
                        crate::app::responses::demand_spans(
                            spans,
                            &product.access,
                            0..product.source_bytes,
                        );
                    } else {
                        product
                            .read
                            .lock()
                            .unwrap()
                            .served(&product.access, product.source_bytes);
                    }
                }
                let _retained = &product.slot;
                Ok(Some((
                    Bytes::from(bytes),
                    (product, remaining - n as u64, true),
                )))
            },
        );
        Ok(Body::from_stream(stream))
    }
}
pub(super) const SOURCE_PREFIX: &str = "verified-v1:";
pub(crate) fn identity(stream: &StoredStream) -> String {
    format!(
        "{SOURCE_PREFIX}{}:{}:{}",
        stream.bucket_id, stream.id, stream.etag
    )
}
fn key(source: &str, rule: &Pipeline) -> Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(&(
        source,
        rule,
        super::engine_version()?,
    ))?)
    .to_hex()
    .to_string())
}
fn resolve(rule: &Pipeline, source: &Source, accept: Option<&str>) -> Result<Pipeline> {
    let mut fixed = rule.clone();
    if fixed.format() == Format::Auto {
        fixed.stages.last_mut().unwrap().format = rule.select(
            source,
            accept,
            &[
                Format::Jxl,
                Format::Avif,
                Format::Webp,
                Format::Jpeg,
                Format::Png,
            ],
        )?;
    }
    if source.frames == 1 {
        for stage in &mut fixed.stages {
            stage
                .operations
                .retain(|op| !matches!(op, super::Operation::Frame { index: 0 }));
        }
    }
    Ok(fixed)
}
impl Service {
    pub fn new(resources: Resources, capacity: u64) -> Arc<Self> {
        Arc::new(Self {
            cache: Cache::new(resources.disk.clone(), capacity),
            resources,
            flights: Default::default(),
            waiting: Arc::new(Semaphore::new(32)),
            job: Arc::new(Semaphore::new(1)),
        })
    }
    pub fn source(&self, identity: &str) -> Option<Source> {
        self.cache.source(identity)
    }
    pub fn needs_demand(&self, stream: &StoredStream, rule: &Pipeline) -> bool {
        if !self.resources.access.collecting() || rule.format() == Format::Auto {
            return false;
        }
        let identity = identity(stream);
        let rule = self
            .source(&identity)
            .map_or_else(|| Ok(rule.clone()), |s| resolve(rule, &s, None));
        !rule
            .and_then(|rule| key(&identity, &rule))
            .is_ok_and(|key| self.cache.footprint(&key, stream.payload_revision))
    }
    pub fn cached(&self, stream: &StoredStream, rule: &Pipeline) -> bool {
        let identity = identity(stream);
        let source = self.source(&identity);
        if rule.format() == Format::Auto {
            return source.is_some();
        }
        let fixed = source.map_or_else(|| Ok(rule.clone()), |source| resolve(rule, &source, None));
        fixed
            .and_then(|rule| key(&identity, &rule))
            .is_ok_and(|key| self.cache.contains(&key))
            && !self.needs_demand(stream, rule)
    }
    fn slot(
        &self,
        retained: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Result<Arc<OwnedSemaphorePermit>> {
        Ok(match retained {
            Some(slot) => slot,
            None => Arc::new(
                self.resources
                    .slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| s3s::s3_error!(SlowDown))?,
            ),
        })
    }
    fn product(
        &self,
        entry: Arc<Entry>,
        file: tokio::fs::File,
        slot: Arc<OwnedSemaphorePermit>,
        read: Arc<Mutex<crate::status::access::DerivedRead>>,
        bytes: i64,
        elapsed: u64,
        cache_hit: bool,
    ) -> Product {
        Product {
            source: entry.metadata.source.clone(),
            image: entry.metadata.image.clone(),
            output: entry.metadata.output.clone(),
            etag: format!("\"{}\"", entry.metadata.digest),
            spans: None,
            cache_hit,
            run: None,
            elapsed_ms: elapsed,
            source_bytes: bytes,
            file,
            entry,
            slot,
            read,
            access: self.resources.access.clone(),
        }
    }
    async fn hit(
        &self,
        identity: &str,
        rule: &Pipeline,
        accept: Option<&str>,
        bytes: i64,
        input: InputLimits,
        output: u64,
        retained: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Result<Option<Product>> {
        let fixed = if let Some(source) = self.source(identity) {
            input.check(&source)?;
            resolve(rule, &source, accept)?
        } else if rule.format() == Format::Auto {
            return Ok(None);
        } else {
            rule.clone()
        };
        let slot = self.slot(retained)?;
        let _io = self.resources.disk.io.clone().acquire_owned().await?;
        if let Some((entry, file)) = self
            .cache
            .get(&key(identity, &fixed)?)
            .await
            .filter(|(e, _)| e.metadata.output.bytes <= output)
        {
            input.check(&entry.metadata.source)?;
            return Ok(Some(self.product(
                entry,
                file,
                slot,
                Default::default(),
                bytes,
                0,
                true,
            )));
        }
        Ok(None)
    }
    async fn capture<F>(
        &self,
        bytes: i64,
        output: u64,
        body: F,
    ) -> Result<(
        Temporary,
        PathBuf,
        Arc<Mutex<crate::status::access::DerivedRead>>,
    )>
    where
        F: Future<Output = Result<Body>> + Send + 'static,
    {
        let root = self
            .resources
            .disk
            .root
            .join("cache/images")
            .join(format!(".work-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&root).await?;
        let mut temporary = Temporary {
            path: root,
            charge: None,
            retained: false,
            deferred: self.cache.deferred.clone(),
        };
        temporary.charge = Some(
            self.resources
                .disk
                .image_workspace(bytes as u64 + output * 2 + 65536, &temporary.path)?,
        );
        let input = temporary.path.join("input");
        let read = Arc::new(Mutex::new(crate::status::access::DerivedRead::default()));
        let capture = async {
            let mut body = body.await?.into_data_stream();
            let mut file = tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&input)
                .await?;
            let mut received = 0u64;
            while let Some(chunk) = body.next().await {
                let chunk = chunk?;
                received = received
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| s3s::s3_error!(EntityTooLarge))?;
                ensure!(received <= bytes as u64, s3s::s3_error!(InvalidRequest));
                file.write_all(&chunk).await?;
            }
            ensure!(received == bytes as u64, s3s::s3_error!(IncompleteBody));
            file.flush().await?;
            Ok::<_, anyhow::Error>(())
        };
        tokio::time::timeout(
            Duration::from_secs(30),
            crate::status::access::DERIVING.scope(read.clone(), capture),
        )
        .await??;
        Ok((temporary, input, read))
    }
    pub async fn inspect<F>(
        self: &Arc<Self>,
        identity: String,
        bytes: i64,
        limit: InputLimits,
        body: F,
    ) -> Result<Source>
    where
        F: Future<Output = Result<Body>> + Send + 'static,
    {
        ensure!(
            bytes > 0 && bytes as u64 <= limit.bytes,
            s3s::s3_error!(EntityTooLarge)
        );
        self.cache.ready().await?;
        self.cache.reap();
        if let Some(source) = self.source(&identity) {
            limit.check(&source)?;
            return Ok(source);
        }
        let queued = self
            .waiting
            .clone()
            .try_acquire_owned()
            .map_err(|_| s3s::s3_error!(SlowDown))?;
        let flight = self.flights.get(format!("inspect:{identity}"));
        let flight = tokio::time::timeout(Duration::from_secs(35), flight.lock_owned()).await?;
        if let Some(source) = self.source(&identity) {
            limit.check(&source)?;
            return Ok(source);
        }
        let job = self
            .job
            .clone()
            .try_acquire_owned()
            .map_err(|_| s3s::s3_error!(SlowDown))?;
        let this = self.clone();
        tokio::spawn(async move {
            let (_queued, _flight, _job) = (queued, flight, job);
            let limits = Limits::new(limit, this.resources.memory.min(768 * 1024 * 1024))
                .map_err(|_| s3s::s3_error!(SlowDown))?;
            let (_temporary, path, _read) = this.capture(bytes, 0, body).await?;
            let _cpu = tokio::time::timeout(
                Duration::from_secs(5),
                this.resources.cpu.clone().acquire_owned(),
            )
            .await??;
            let _memory = this
                .resources
                .work
                .clone()
                .try_acquire_many_owned(limits.memory.div_ceil(1024 * 1024) as u32)
                .map_err(|_| s3s::s3_error!(SlowDown))?;
            let source = probe(&path, limits).await?;
            limit.check(&source)?;
            this.cache.remember(identity, source.clone());
            Ok::<_, anyhow::Error>(source)
        })
        .await?
    }
    pub async fn execute<F>(
        self: &Arc<Self>,
        source: StoredStream,
        rule: Pipeline,
        accept: Option<String>,
        input: InputLimits,
        output: u64,
        managed: bool,
        retained: Option<Arc<OwnedSemaphorePermit>>,
        spans: Vec<crate::app::responses::DemandSpan>,
        body: F,
    ) -> Result<Product>
    where
        F: Future<Output = Result<Body>> + Send + 'static,
    {
        let identity = identity(&source);
        let bytes = source.size;
        let revision = source.payload_revision;
        let spans = if spans.len() <= 256 {
            spans
        } else {
            Vec::new()
        };
        rule.validate()
            .map_err(|e| s3s::s3_error!(InvalidArgument, "{e}"))?;
        ensure!(
            bytes > 0 && bytes as u64 <= input.bytes,
            s3s::s3_error!(EntityTooLarge)
        );
        self.cache.ready().await?;
        self.cache.reap();
        if let Some(mut hit) = self
            .hit(
                &identity,
                &rule,
                accept.as_deref(),
                bytes,
                input,
                output,
                retained.clone(),
            )
            .await?
        {
            hit.spans = self.cache.demand(&hit.entry, revision, &spans);
            return Ok(hit);
        }
        let queued = self
            .waiting
            .clone()
            .try_acquire_owned()
            .map_err(|_| s3s::s3_error!(SlowDown))?;
        let flight = self.flights.get(format!(
            "{identity}:{}:{}",
            rule.text(),
            if rule.format() == Format::Auto {
                accept.as_deref().unwrap_or("")
            } else {
                ""
            }
        ));
        let flight = tokio::time::timeout(Duration::from_secs(65), flight.lock_owned()).await?;
        if let Some(mut hit) = self
            .hit(
                &identity,
                &rule,
                accept.as_deref(),
                bytes,
                input,
                output,
                retained.clone(),
            )
            .await?
        {
            hit.spans = self.cache.demand(&hit.entry, revision, &spans);
            return Ok(hit);
        }
        let job = if managed {
            tokio::time::timeout(Duration::from_secs(35), self.job.clone().acquire_owned())
                .await??
        } else {
            self.job
                .clone()
                .try_acquire_owned()
                .map_err(|_| s3s::s3_error!(SlowDown))?
        };
        if let Some(mut hit) = self
            .hit(
                &identity,
                &rule,
                accept.as_deref(),
                bytes,
                input,
                output,
                retained.clone(),
            )
            .await?
        {
            hit.spans = self.cache.demand(&hit.entry, revision, &spans);
            return Ok(hit);
        }
        let this = self.clone();
        tokio::spawn(async move {
            let (_queued, _flight, _job) = (queued, flight, job);
            let mut limits = Limits::new(input, this.resources.memory.min(768 * 1024 * 1024))
                .map_err(|_| s3s::s3_error!(SlowDown, "insufficient image memory budget"))?;
            limits.output = limits.output.min(output);
            #[cfg(feature = "fault-injection")]
            crate::faults::point("image-before-capture").await;
            let (mut temporary, input, read) = this.capture(bytes, limits.output, body).await?;
            let destination = temporary.path.join("output");
            let _cpu = tokio::time::timeout(
                Duration::from_secs(5),
                this.resources.cpu.clone().acquire_owned(),
            )
            .await??;
            let _memory = this
                .resources
                .work
                .clone()
                .try_acquire_many_owned(limits.memory.div_ceil(1024 * 1024) as u32)
                .map_err(|_| s3s::s3_error!(SlowDown))?;
            let started = Instant::now();
            let rule = if rule.format() == Format::Auto {
                let source = match this.source(&identity) {
                    Some(source) => source,
                    None => probe(&input, limits).await?,
                };
                resolve(&rule, &source, accept.as_deref())?
            } else {
                rule
            };
            let super::engine::Rendered {
                source,
                image,
                output,
            } = render(&input, &destination, &rule, limits).await?;
            limits.input.check(&source)?;
            let fixed = resolve(&rule, &source, None)?;
            this.cache.remember(identity.clone(), source.clone());
            tokio::fs::remove_file(&input).await?;
            let mut file = tokio::fs::File::open(&destination).await?;
            let mut hash = blake3::Hasher::new();
            let mut buffer = vec![0u8; 65536];
            loop {
                let n = file.read(&mut buffer).await?;
                if n == 0 {
                    break;
                }
                hash.update(&buffer[..n]);
            }
            file.rewind().await?;
            let slot = this.slot(retained)?;
            let elapsed = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
            let metadata = Metadata {
                key: key(&identity, &fixed)?,
                source_id: identity.clone(),
                source_revision: revision,
                spans,
                source,
                image,
                output,
                digest: hash.finalize().to_hex().to_string(),
            };
            let entry = this.cache.publish(&mut temporary, metadata).await?;
            Ok::<_, anyhow::Error>(this.product(entry, file, slot, read, bytes, elapsed, false))
        })
        .await?
    }
}
pub(crate) async fn execute(
    app: Arc<App>,
    stream: StoredStream,
    pin: Active,
    rule: Pipeline,
    accept: Option<String>,
    input_limit: InputLimits,
    output_limit: u64,
) -> Result<Product> {
    let size = stream.size;
    let service = app.images.clone();
    let mapping = if service.needs_demand(&stream, &rule) {
        app.response_mapping(&stream)
            .await
            .ok()
            .flatten()
            .map(|m| m.2)
    } else {
        None
    };
    let product = service
        .execute(
            stream.clone(),
            rule,
            accept,
            input_limit,
            output_limit,
            true,
            None,
            mapping.unwrap_or_default(),
            async move {
                let permits = app.admit(false).await?;
                Ok(Body::new(s3s::Body::from(
                    app.body(stream, pin, 0..size, permits, false, None).await?,
                )))
            },
        )
        .await?;
    Ok(product)
}
