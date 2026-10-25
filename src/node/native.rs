use super::*;
use crate::model::node_native::{Action, Completion, SourceError, Work};
use futures_util::stream::BoxStream;
use md5::{Digest, Md5};
use tokio::io::AsyncReadExt;

enum Progress {
    Idle,
    Transferred,
    Ready,
}

fn replay(path: std::path::PathBuf, size: i64, etag: String) -> BoxStream<'static, Result<Bytes>> {
    Box::pin(async_stream::try_stream! {
        let metadata=tokio::fs::symlink_metadata(&path).await.map_err(|error| {
            if error.kind()==std::io::ErrorKind::NotFound {anyhow::Error::from(SourceError::Missing)} else {error.into()}
        })?;
        if !metadata.is_file() || metadata.len()!=size as u64 {Err(SourceError::Corrupt)?;}
        let mut file=tokio::fs::File::open(path).await?;
        let mut hash=Md5::new();
        let mut received=0u64;
        loop {
            let mut bytes=vec![0;256*1024];
            let count=file.read(&mut bytes).await?;
            if count==0 {break;}
            bytes.truncate(count);
            hash.update(&bytes);
            received+=count as u64;
            if received>size as u64 {Err(SourceError::Corrupt)?;}
            yield Bytes::from(bytes);
        }
        if received!=size as u64 || hex::encode(hash.finalize())!=etag {Err(SourceError::Corrupt)?;}
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn replay_classifies_local_source_failures() -> Result<()> {
        let path = std::env::temp_dir().join(format!("mokyu-native-{}", Uuid::new_v4()));
        let etag = hex::encode(Md5::digest(b"abc"));
        let error = replay(path.clone(), 3, etag.clone())
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<SourceError>(),
            Some(SourceError::Missing)
        ));
        tokio::fs::write(&path, b"abc").await?;
        assert_eq!(
            replay(path.clone(), 3, etag.clone())
                .try_collect::<Vec<_>>()
                .await?,
            [Bytes::from_static(b"abc")]
        );
        let error = replay(path.clone(), 4, etag.clone())
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<SourceError>(),
            Some(SourceError::Corrupt)
        ));
        tokio::fs::write(&path, b"bad").await?;
        let error = replay(path.clone(), 3, etag)
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<SourceError>(),
            Some(SourceError::Corrupt)
        ));
        tokio::fs::remove_file(path).await?;
        Ok(())
    }
}

impl Writes {
    pub(crate) fn start_native(self: &Arc<Self>) {
        let writes = Arc::downgrade(self);
        let node = self.reads.node.clone();
        tokio::spawn(async move {
            let mut state = node.subscribe();
            let mut recover = true;
            let mut inventory = tokio::time::interval_at(
                Instant::now() + Duration::from_secs(60),
                Duration::from_secs(60),
            );
            inventory.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                if recover {
                    let Some(current) = writes.upgrade() else {
                        return;
                    };
                    match current.reads.reader.files.reclaim_local(&node, true).await {
                        Ok(()) => {
                            recover = false;
                            node.native_work.notify_one();
                        }
                        Err(error) => tracing::debug!(%error,"local source recovery postponed"),
                    }
                }
                tokio::select! {
                    _=node.native_work.notified()=>{},
                    _=inventory.tick()=>{
                        let Some(current)=writes.upgrade() else {return;};
                        if let Err(error)=current.reads.reader.files.reclaim_local(&node, false).await {tracing::debug!(%error,"local upload inventory postponed");}
                        continue;
                    },
                    result=state.changed()=>{
                        if result.is_err() || state.borrow().phase==super::super::Phase::Stopped {return;}
                        continue;
                    }
                }
                loop {
                    let Some(current) = writes.upgrade() else {
                        return;
                    };
                    let result = current.native_step(None).await;
                    drop(current);
                    match result {
                        Ok(Progress::Transferred) => continue,
                        Err(error) => {
                            tracing::debug!(%error,"native upload work postponed");
                            tokio::time::sleep(Duration::from_secs(3)).await;
                        }
                        _ => {}
                    }
                    break;
                }
            }
        });
    }

    pub(super) async fn wait_native(&self, stream: &str) -> Result<()> {
        loop {
            match self.native_step(Some(stream.to_owned())).await? {
                Progress::Ready => return Ok(()),
                Progress::Transferred => {}
                Progress::Idle => tokio::time::sleep(Duration::from_millis(250)).await,
            }
        }
    }

    async fn native_step(&self, stream: Option<String>) -> Result<Progress> {
        let node = &self.reads.node;
        let snapshot = node.background_snapshot()?;
        let io = Arc::new(self.io.clone().acquire_owned().await?);
        let held = self.reads.cache.snapshot();
        let target = stream.is_some();
        let id = Uuid::now_v7().to_string();
        let started = Instant::now();
        let plan = node
            .client
            .claim_native(wire::NativeClaimRequest {
                run: Some(snapshot.lease.run.clone()),
                operation_id: id.clone(),
                stream_id: stream,
                cached: reads::cached(&held),
            })
            .await?;
        ensure!(plan.operation_id == id, "native work identity differs");
        if plan.ready {
            ensure!(plan.work_json.is_none(), "unexpected ready work");
            return Ok(Progress::Ready);
        }
        let Some(json) = plan.work_json else {
            return Ok(Progress::Idle);
        };
        let work: Work = serde_json::from_slice(&json)?;
        let grants = node
            .resources
            .adopt(&snapshot.lease, &plan.grants, started)?;
        ensure!(
            grants.len() == 1
                && grants[0].reference.r#type == wire::GrantType::NativeWork as i32
                && grants[0].storage == Some(work.upload.storage),
            "native work protection missing"
        );
        let materials =
            self.reads
                .cache
                .apply(node.client.identity.deployment_id, held, plan.materials)?;
        let backend = reads::backend(&materials, work.upload.storage)?;
        let path = crate::storage::raw::remote_path(work.upload.source);
        let future = async {
            backend.store.ensure_verified().await?;
            match &work.action {
                Action::Initialize => {
                    #[cfg(feature = "fault-injection")]
                    crate::faults::point("node-native-before-initialize").await;
                    let (pending, _) = backend
                        .store
                        .streaming
                        .as_ref()
                        .context("S3 streaming missing")?
                        .pending_multipart(&path)
                        .await?;
                    let upstream = if let Some(upstream) = pending.into_iter().next() {
                        upstream
                    } else {
                        backend
                            .store
                            .multipart
                            .as_ref()
                            .context("S3 multipart missing")?
                            .create_multipart(&path)
                            .await?
                    };
                    Ok(Some(upstream))
                }
                Action::Part {
                    file,
                    number,
                    size,
                    etag,
                    ..
                } => {
                    ensure!(
                        (1..=10000).contains(number) && *size >= 0 && etag.len() == 32,
                        "invalid native part plan"
                    );
                    let digest: [u8; 16] = hex::decode(etag)?
                        .try_into()
                        .map_err(|_| anyhow::anyhow!("invalid part digest"))?;
                    let disk = self.reads.reader.files.multipart().await?;
                    let local = disk.fragment_path(*file);
                    let _lock = self.reads.reader.files.local_lock(*file).await;
                    let value = backend
                        .store
                        .streaming
                        .as_ref()
                        .context("S3 streaming missing")?
                        .part(
                            &path,
                            work.upload
                                .upstream
                                .as_deref()
                                .context("native upload missing")?,
                            (*number - 1) as usize,
                            *size as u64,
                            &digest,
                            || {
                                replay(local.clone(), *size, etag.clone())
                                    .map_ok(|bytes| {
                                        Bytes::from_owner(Upload {
                                            bytes,
                                            _memory: io.clone(),
                                            _io: io.clone(),
                                            _grants: grants.clone(),
                                        })
                                    })
                                    .boxed()
                            },
                        )
                        .await?;
                    #[cfg(feature = "fault-injection")]
                    crate::faults::point("node-native-part-uploaded").await;
                    Ok(Some(value.content_id))
                }
                Action::Abort => {
                    let (pending, more) = backend
                        .store
                        .streaming
                        .as_ref()
                        .context("S3 streaming missing")?
                        .pending_multipart(&path)
                        .await?;
                    for upstream in pending {
                        match backend
                            .store
                            .multipart
                            .as_ref()
                            .context("S3 multipart missing")?
                            .abort_multipart(&path, &upstream)
                            .await
                        {
                            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                            Err(error) => return Err(error.into()),
                        }
                    }
                    ensure!(!more, "native upload cleanup has another batch");
                    Ok(None)
                }
            }
        };
        tokio::pin!(future);
        let mut events = node.subscribe();
        let result = loop {
            snapshot.lease.check()?;
            for grant in &grants {
                grant.check()?;
            }
            let deadline = grants
                .iter()
                .map(|grant| grant.deadline())
                .fold(snapshot.lease.deadline(), std::cmp::min);
            tokio::select! {
                biased;
                _=events.changed()=>{},
                _=tokio::time::sleep_until(deadline)=>{},
                result=&mut future=>break result,
            }
        };
        let report = wire::NativeFinishRequest {
            run: Some(snapshot.lease.run.clone()),
            operation_id: id,
            value: result.as_ref().ok().cloned().flatten(),
            retry: result.is_err(),
            source_error: result
                .as_ref()
                .err()
                .and_then(|error| error.downcast_ref::<SourceError>())
                .map(|error| match error {
                    SourceError::Missing => wire::LocalSourceError::LocalSourceMissing as i32,
                    SourceError::Corrupt => wire::LocalSourceError::LocalSourceCorrupt as i32,
                }),
        };
        node.client.finish_native(report).await?;
        result?;
        Ok(if target && matches!(work.action, Action::Part { .. }) {
            Progress::Ready
        } else {
            Progress::Transferred
        })
    }

    pub(super) async fn local_part(
        &self,
        operation: &Operation,
        mut body: s3s::dto::StreamingBlob,
        mut check: Integrity,
        trailers: Option<s3s::TrailingHeaders>,
        length: Option<u64>,
    ) -> Result<(u64, String, serde_json::Value)> {
        let node = &self.reads.node;
        let files = &self.reads.reader.files;
        let disk = files.multipart().await?;
        let id = Uuid::parse_str(&operation.stream)?;
        let mut guard = files.local_lock(id).await;
        let path = disk.fragment_path(id);
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;
        file.sync_all().await?;
        tokio::fs::File::open(path.parent().unwrap())
            .await?
            .sync_all()
            .await?;
        drop(file);
        let mut hashes = Vec::new();
        let mut buffer = Vec::with_capacity(codec::MAX);
        let mut size = 0u64;
        let idle = Duration::from_secs(crate::config::seconds(
            &node.snapshot()?.runtime().uploads.client_idle_timeout,
        )?);
        loop {
            let data = tokio::time::timeout(idle, body.next())
                .await
                .map_err(|_| s3s::s3_error!(RequestTimeout))?;
            let eof = data.is_none();
            let data = data
                .transpose()
                .map_err(|_| s3s::s3_error!(IncompleteBody))?
                .unwrap_or_default();
            size = size
                .checked_add(data.len() as u64)
                .context("part length overflow")?;
            ensure!(
                size <= operation.limit && length.is_none_or(|length| size <= length),
                s3s::s3_error!(EntityTooLarge)
            );
            check.update(&data);
            let mut rest = data.as_ref();
            loop {
                let count = rest.len().min(codec::MAX - buffer.len());
                buffer.extend_from_slice(&rest[..count]);
                rest = &rest[count..];
                if buffer.len() == codec::MAX || (eof && !buffer.is_empty()) {
                    hashes.push(blake3::hash(&buffer).as_bytes().to_vec());
                    let (_, _, held) = disk.append_fragment(id, &buffer, guard).await?;
                    guard = held;
                    buffer.clear();
                }
                if rest.is_empty() {
                    break;
                }
            }
            if eof {
                break;
            }
        }
        ensure!(
            length.is_none_or(|length| length == size),
            s3s::s3_error!(IncompleteBody)
        );
        let (etag, sums) = check.finish(trailers)?;
        operation.check()?;
        let reply = node
            .client
            .prepare_local_part(wire::PrepareLocalPartRequest {
                run: Some(operation.request.run.run.clone()),
                operation_id: operation.id.clone(),
                size,
                hashes,
            })
            .await?;
        ensure!(
            reply.operation_id == operation.id
                && reply.phase == wire::WritePhase::WriteActive as i32,
            "local part preparation differs"
        );
        drop(guard);
        Ok((size, etag, sums))
    }

    pub(super) async fn native_complete(
        &self,
        operation: &Operation,
        value: &crate::model::node_complete::Summary,
    ) -> Result<wire::WriteReply> {
        let node = &self.reads.node;
        let mut resend = false;
        loop {
            operation.check()?;
            let io = self.io.clone().acquire_owned().await?;
            let started = Instant::now();
            let reply = node
                .client
                .prepare_native_complete(wire::NativeCompleteRequest {
                    run: Some(operation.request.run.run.clone()),
                    operation_id: operation.id.clone(),
                    resend,
                })
                .await?;
            resend = false;
            let plan: Completion = serde_json::from_slice(&reply.completion_json)?;
            ensure!(
                plan.upload.upload == value.upload && plan.parts.len() == value.parts,
                "native completion manifest differs"
            );
            if plan.upload.upstream.is_none()
                || plan.parts.iter().any(|part| part.remote_etag.is_none())
            {
                node.native_work.notify_one();
                drop(io);
                operation
                    .guard(node, &[], async {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        Ok(())
                    })
                    .await?;
                continue;
            }
            let grants = node
                .resources
                .adopt(&operation.request.run, &reply.grants, started)?;
            let path = crate::storage::raw::remote_path(plan.upload.source);
            let backend = &operation.backend;
            let completed = operation
                .guard(node, &grants, async {
                    backend.store.ensure_verified().await?;
                    match backend.store.backend.head(&path).await {
                        Ok(meta) => {
                            ensure!(
                                meta.size == value.size as u64,
                                "completed native object length differs"
                            );
                            return Ok(true);
                        }
                        Err(object_store::Error::NotFound { .. }) => {}
                        Err(error) => return Err(error.into()),
                    }
                    let parts: Vec<_> = plan
                        .parts
                        .iter()
                        .map(|part| (part.number, part.remote_etag.clone().unwrap()))
                        .collect();
                    let result = backend
                        .store
                        .streaming
                        .as_ref()
                        .context("S3 streaming missing")?
                        .complete(&path, plan.upload.upstream.as_deref().unwrap(), &parts)
                        .await;
                    if result.is_err()
                        && let Ok(meta) = backend.store.backend.head(&path).await
                    {
                        ensure!(
                            meta.size == value.size as u64,
                            "completed native object length differs"
                        );
                        return Ok(true);
                    }
                    result
                })
                .await?;
            if !completed {
                resend = true;
                drop(grants);
                drop(io);
                continue;
            }
            #[cfg(feature = "fault-injection")]
            crate::faults::point("node-native-completed").await;
            let (digest, hash, sums) = operation
                .guard(node, &grants, async {
                    let _read = self.reads.reader.io.clone().acquire_owned().await?;
                    let mut body = backend.store.backend.get(&path).await?.into_stream();
                    let mut full =
                        Integrity::new(&hyper::HeaderMap::new(), value.algorithm.as_deref())?;
                    let mut hash = blake3::Hasher::new();
                    let mut rest = Bytes::new();
                    for part in &plan.parts {
                        let mut size = 0i64;
                        let mut md5 = Md5::new();
                        while size < part.size {
                            if rest.is_empty() {
                                rest = body
                                    .next()
                                    .await
                                    .transpose()?
                                    .context("completed native object truncated")?;
                            }
                            let count = rest.len().min((part.size - size) as usize);
                            md5.update(&rest[..count]);
                            full.update(&rest[..count]);
                            hash.update(&rest[..count]);
                            size += count as i64;
                            rest = rest.slice(count..);
                        }
                        ensure!(
                            hex::encode(md5.finalize()) == part.etag,
                            "completed native part checksum differs"
                        );
                    }
                    ensure!(
                        rest.is_empty() && body.next().await.transpose()?.is_none(),
                        "completed native object exceeds manifest"
                    );
                    let (digest, sums) = full.finish(None)?;
                    Ok((
                        hex::decode(digest)?,
                        hash.finalize().as_bytes().to_vec(),
                        sums,
                    ))
                })
                .await?;
            node.client
                .adopt_native_complete(wire::AdoptNativeRequest {
                    run: Some(operation.request.run.run.clone()),
                    operation_id: operation.id.clone(),
                    digest,
                    content_hash: hash,
                })
                .await?;
            return self
                .publish(operation, value.size as u64, &value.etag, &sums, None, None)
                .await;
        }
    }
}
