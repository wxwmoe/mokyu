use super::{Format, InputLimits, Limits, Output, Pipeline, Source, probe, render};
use crate::{
    app::{Active, App, StoredStream},
    storage::{Disk, local::Reservation},
};
use anyhow::{Result, ensure};
use axum::body::{Body, Bytes};
use futures_util::StreamExt;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::OwnedSemaphorePermit,
};
use uuid::Uuid;

pub(crate) async fn recover(disk: &Arc<Disk>) -> Result<()> {
    let root = disk.root.join("cache/images");
    tokio::fs::create_dir_all(&root).await?;
    let mut files = tokio::fs::read_dir(&root).await?;
    while let Some(file) = files.next_entry().await? {
        let name = file.file_name();
        if file.file_type().await?.is_dir()
            && name
                .to_str()
                .and_then(|n| n.strip_prefix(".work-"))
                .and_then(|n| Uuid::parse_str(n).ok())
                .is_some()
        {
            tokio::fs::remove_dir_all(file.path()).await?;
        }
    }
    Ok(())
}
struct Temporary {
    path: PathBuf,
    charge: Option<Reservation>,
}
impl Drop for Temporary {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                if let Some(charge) = self.charge.take() {
                    charge.commit()
                }
                tracing::warn!(%error,"image workspace cleanup failed");
            }
        }
    }
}
pub(crate) struct Product {
    temporary: Temporary,
    pub source: Source,
    pub image: Source,
    pub output: Output,
    pub elapsed_ms: u64,
    pub source_bytes: i64,
    slot: OwnedSemaphorePermit,
    app: Arc<App>,
    read: Arc<Mutex<crate::status::access::DerivedRead>>,
}
impl Product {
    pub fn path(&self) -> PathBuf {
        self.temporary.path.join("output")
    }
    pub async fn body(self) -> Result<Body> {
        let file = tokio::fs::File::open(self.path()).await?;
        let stream = futures_util::stream::try_unfold(
            (file, self, false),
            |(mut file, product, served)| async move {
                let mut bytes = vec![0u8; 64 * 1024];
                let count = file.read(&mut bytes).await?;
                if count == 0 {
                    return Ok::<_, std::io::Error>(None);
                }
                bytes.truncate(count);
                if !served {
                    product
                        .read
                        .lock()
                        .unwrap()
                        .served(&product.app.storage.access, product.source_bytes);
                }
                let _retained = &product.slot;
                Ok(Some((Bytes::from(bytes), (file, product, true))))
            },
        );
        Ok(Body::from_stream(stream))
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
    rule.validate()
        .map_err(|e| s3s::s3_error!(InvalidArgument, "{e}"))?;
    ensure!(
        stream.size > 0 && stream.size as u64 <= input_limit.bytes,
        s3s::s3_error!(EntityTooLarge)
    );
    let job = app
        .thumbnails
        .job
        .clone()
        .try_acquire_owned()
        .map_err(|_| s3s::s3_error!(SlowDown))?;
    let memory = (u64::from(app.budget.pack_memory_units) * 1024 * 1024).min(768 * 1024 * 1024);
    let mut limits = Limits::new(input_limit, memory)
        .map_err(|_| s3s::s3_error!(SlowDown, "insufficient image memory budget"))?;
    limits.output = limits.output.min(output_limit);
    let permits = app.admit(false).await?;
    let task = tokio::spawn(async move {
        let _job = job;
        let root = app
            .storage
            .disk
            .root
            .join("cache/images")
            .join(format!(".work-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&root).await?;
        let mut temporary = Temporary {
            path: root,
            charge: None,
        };
        temporary.charge = Some(app.storage.disk.image_workspace(
            stream.size as u64 + limits.output * 2 + 65536,
            &temporary.path,
        )?);
        let input = temporary.path.join("input");
        let destination = temporary.path.join("output");
        let source_bytes = stream.size;
        let body = Body::new(s3s::Body::from(
            app.body(stream, pin, 0..source_bytes, permits, false, None)
                .await?,
        ));
        let read = Arc::new(Mutex::new(crate::status::access::DerivedRead::default()));
        let capture = async {
            let mut body = body.into_data_stream();
            let mut file = tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&input)
                .await?;
            let mut bytes = 0u64;
            while let Some(chunk) = body.next().await {
                let chunk = chunk?;
                bytes = bytes
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| s3s::s3_error!(EntityTooLarge))?;
                ensure!(bytes <= source_bytes as u64, s3s::s3_error!(InvalidRequest));
                file.write_all(&chunk).await?;
            }
            ensure!(bytes == source_bytes as u64, s3s::s3_error!(IncompleteBody));
            file.flush().await?;
            Ok::<_, anyhow::Error>(())
        };
        tokio::time::timeout(
            Duration::from_secs(30),
            crate::status::access::DERIVING.scope(read.clone(), capture),
        )
        .await??;
        let _cpu = tokio::time::timeout(
            Duration::from_secs(5),
            app.storage.cpu.clone().acquire_owned(),
        )
        .await??;
        let _memory = app
            .storage
            .image_work(limits.memory)
            .ok_or_else(|| s3s::s3_error!(SlowDown))?;
        let started = Instant::now();
        let source = probe(&input, limits).await?;
        input_limit.check(&source)?;
        let selected = rule.select(
            &source,
            accept.as_deref(),
            &[
                Format::Jxl,
                Format::Avif,
                Format::Webp,
                Format::Jpeg,
                Format::Png,
            ],
        )?;
        let mut rule = rule;
        if rule.format() == Format::Auto {
            rule.stages.last_mut().unwrap().format = selected;
        }
        let output = render(&input, &destination, &rule, limits).await?;
        let image = probe(
            &destination,
            Limits {
                input: InputLimits::new(limits.output),
                ..limits
            },
        )
        .await?;
        let elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let slot = app
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| s3s::s3_error!(SlowDown))?;
        Ok::<_, anyhow::Error>(Product {
            temporary,
            source,
            image,
            output,
            elapsed_ms,
            source_bytes,
            slot,
            app,
            read,
        })
    });
    task.await?
}
