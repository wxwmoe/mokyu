use super::*;
use crate::{
    model::node_complete::{Manifest, SOURCES, Summary},
    upload::multipart,
};
use md5::{Digest, Md5};
use s3s::dto::CompleteMultipartUploadOutput;
use tracing::Instrument;

fn summary(reply: &wire::WriteReply) -> Result<Summary> {
    Ok(serde_json::from_slice(
        reply
            .completion_json
            .as_deref()
            .context("completion summary missing")?,
    )?)
}

fn output(bucket: &str, key: &str, summary: &Summary) -> Result<CompleteMultipartUploadOutput> {
    Ok(multipart::completion_output(
        bucket,
        key,
        summary
            .result
            .as_ref()
            .context("completion result missing")?,
    ))
}

impl Writes {
    pub(crate) async fn complete(
        self: &Arc<Self>,
        mut request: Request,
        bucket: String,
        key: String,
        manifest: Manifest,
    ) -> Result<CompleteMultipartUploadOutput> {
        let credential = request.authorize(&request.access, &bucket)?;
        let permit = Arc::new(
            self.completions
                .clone()
                .try_acquire_owned()
                .map_err(|_| multipart::completion_busy())?,
        );
        request.memory = permit.clone();
        let id = Uuid::now_v7().to_string();
        let held = self.reads.cache.snapshot();
        let status = wire::CompleteStatusRequest {
            run: Some(request.run.run.clone()),
            access_key: request.access.clone(),
            credential_revision: Some(credential.revision as u64),
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: manifest.upload.to_string(),
            manifest_hash: manifest.hash()?,
        };
        let started = Instant::now();
        let reply = self
            .reads
            .node
            .client
            .begin_complete(wire::BeginCompleteRequest {
                run: status.run.clone(),
                operation_id: id.clone(),
                access_key: status.access_key.clone(),
                credential_revision: status.credential_revision,
                bucket: bucket.clone(),
                key: key.clone(),
                manifest_json: serde_json::to_vec(&manifest)?,
                cached: reads::cached(&held),
            })
            .await?;
        ensure!(reply.operation_id == id, "invalid completion identity");
        let value = summary(&reply)?;
        ensure!(
            value.upload == manifest.upload
                && value.parts == manifest.parts.len()
                && value.size >= 0,
            "invalid completion summary"
        );
        let work = match wire::WritePhase::try_from(reply.phase)? {
            wire::WritePhase::WritePublished => return output(&bucket, &key, &value),
            wire::WritePhase::WriteBusy => {
                drop(request);
                drop(permit);
                None
            }
            wire::WritePhase::WriteActive => {
                let mut operation =
                    self.accept(request, reply, held, started, false, value.size as u64)?;
                let writes = self.clone();
                let name = key.clone();
                Some(tokio::spawn(
                    async move {
                        let _permit = permit;
                        let result = async {
                            let mut value = value;
                            tokio::time::timeout(
                                multipart::completion_timeout(),
                                operation.guard(&writes.reads.node, &[], async {
                                    while value.pending_sources {
                                        tokio::time::sleep(Duration::from_secs(3)).await;
                                        let reply = writes
                                            .reads
                                            .node
                                            .client
                                            .write_status(operation.lookup())
                                            .await?;
                                        ensure!(
                                            reply.phase == wire::WritePhase::WriteActive as i32,
                                            "completion write is no longer active"
                                        );
                                        value = summary(&reply)?;
                                    }
                                    Ok(())
                                }),
                            )
                            .await
                            .map_err(|_| multipart::completion_retry())??;
                            let slot = operation
                                .guard(&writes.reads.node, &[], async {
                                    Ok(writes.slots.clone().acquire_owned().await?)
                                })
                                .await?;
                            operation.request.memory = Arc::new(slot);
                            let _cache =
                                writes.reads.public.changing_object(operation.bucket, &name);
                            #[cfg(feature = "fault-injection")]
                            operation
                                .guard(&writes.reads.node, &[], async {
                                    crate::faults::point("node-completion-assembling").await;
                                    Ok(())
                                })
                                .await?;
                            if !operation.backend.profile.cdc_enabled {
                                return summary(&writes.native_complete(&operation, &value).await?);
                            }
                            let body = writes.completion_body(&operation, &value);
                            let source_error = Arc::new(std::sync::Mutex::new(None));
                            let failed = source_error.clone();
                            let body = crate::http::object::blob(
                                body.map_err(move |error| {
                                    *failed.lock().unwrap() = Some(error);
                                    std::io::Error::other("completion source failed")
                                })
                                .boxed(),
                            );
                            let check = Integrity::new(
                                &hyper::HeaderMap::new(),
                                value.algorithm.as_deref(),
                            )?;
                            let received = writes
                                .write_body(&operation, body, check, None, Some(value.size as u64))
                                .await;
                            if let Some(error) = source_error.lock().unwrap().take() {
                                return Err(error);
                            }
                            let (size, _, sums) = received?;
                            let reply = writes
                                .publish(&operation, size, &value.etag, &sums, None, None)
                                .await?;
                            summary(&reply)
                        }
                        .await;
                        if result.is_err() {
                            let _ = writes
                                .reads
                                .node
                                .client
                                .abort_write(operation.lookup())
                                .await;
                        }
                        result
                    }
                    .in_current_span(),
                ))
            }
            _ => return Err(multipart::completion_retry().into()),
        };
        let writes = self.clone();
        let future = Box::pin(async move {
            let value = tokio::time::timeout(multipart::completion_timeout(), async {
                if let Some(work) = work {
                    return work.await.map_err(anyhow::Error::from)?;
                }
                loop {
                    let reply = writes
                        .reads
                        .node
                        .client
                        .complete_status(status.clone())
                        .await?;
                    match wire::WritePhase::try_from(reply.phase)? {
                        wire::WritePhase::WritePublished => {
                            return Ok(serde_json::from_slice(&reply.summary_json)?);
                        }
                        wire::WritePhase::WriteBusy => {}
                        _ => return Err(multipart::completion_retry().into()),
                    }
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            })
            .await
            .map_err(|_| multipart::completion_retry())
            .and_then(|value| value.map_err(crate::node::delivery::error))?;
            output(&bucket, &key, &value).map_err(crate::node::delivery::error)
        });
        Ok(CompleteMultipartUploadOutput {
            future: Some(future),
            ..Default::default()
        })
    }

    fn completion_body(
        self: &Arc<Self>,
        operation: &Operation,
        summary: &Summary,
    ) -> futures_util::stream::BoxStream<'static, Result<Bytes>> {
        let writes = self.clone();
        let id = operation.id.clone();
        let run = operation.request.run.clone();
        let memory = operation.request.memory.clone();
        let count = summary.parts;
        let expected = summary.size as u64;
        Box::pin(async_stream::try_stream! {
            let deadline = Instant::now() + multipart::completion_timeout();
            let buffers = Arc::new(Semaphore::new(super::super::body::BUFFER_UNITS));
            let mut first = 0u32;
            let mut total = 0u64;
            // Keep one live reader per shared permit across source batches.
            let mut retained: Vec<Arc<crate::node::resources::Grant>> = Vec::new();
            loop {
                run.check()?;
                let held = writes.reads.cache.snapshot();
                let request = wire::ReadCompleteRequest {
                    run: Some(run.run.clone()), operation_id: id.clone(), first,
                    read_operations: (0..SOURCES.min(count - first as usize)).map(|_| Uuid::now_v7().to_string()).collect(),
                    shared_permits: retained.iter().filter(|grant| grant.reference.r#type == wire::GrantType::StorageRead as i32).map(|grant| grant.reference.id.clone()).collect(),
                    cached: reads::cached(&held),
                };
                let (reply, started) = tokio::time::timeout_at(deadline, async {
                    loop {
                        run.check()?;
                        let started = Instant::now();
                        match writes.reads.node.client.read_complete(request.clone()).await {
                            Ok(reply) => break Ok::<_, anyhow::Error>((reply, started)),
                            Err(error) if error.code() == tonic::Code::FailedPrecondition
                                && error.metadata().get("mokyu-retryable").is_some_and(|value| value == "source-pending") => {
                                tokio::time::sleep(Duration::from_secs(3)).await;
                            }
                            Err(error) => break Err(error.into()),
                        }
                    }
                }).await.map_err(|_| multipart::completion_retry())??;
                let next = first as usize + reply.parts.len();
                if reply.first != first || reply.parts.is_empty() || reply.parts.len() > SOURCES || next > count || reply.next != (next < count).then_some(next as u32) {
                    Err(anyhow::anyhow!("invalid completion source window"))?;
                }
                let sources = reply.parts.into_iter().map(|reply| writes.reads.internal(run.clone(), memory.clone(), reply, held.clone(), started, buffers.clone())).collect::<Result<Vec<_>>>()?;
                for source in &sources {
                    for grant in source.grants.iter().filter(|grant| grant.reference.r#type == wire::GrantType::StorageRead as i32) {
                        if retained.len() < 32 && !retained.iter().any(|held| held.reference.id == grant.reference.id) {
                            let owner = source.grants.iter().find(|grant| grant.reference.r#type == wire::GrantType::ReadProtection as i32).context("completion read protection missing")?;
                            retained.extend([grant.clone(), owner.clone()]);
                        }
                    }
                }
                for source in sources {
                    if source.reply.outcome != wire::ReadOutcome::Ready as i32 || source.reply.offset != 0 {
                        Err(anyhow::anyhow!("invalid completion source"))?;
                    }
                    let object: read::Object = source.material(source.reply.object.as_ref().context("completion source object missing")?)?;
                    if object.size < 0 || source.reply.length != object.size as u64 {
                        Err(anyhow::anyhow!("invalid completion source length"))?;
                    }
                    let mut size = 0u64;
                    let mut digest = Md5::new();
                    let mut body = writes.reads.body(source);
                    while let Some(bytes) = body.next().await {
                        let bytes = bytes?;
                        size += bytes.len() as u64;
                        if size > object.size as u64 {
                            Err(anyhow::anyhow!("completion source exceeds its length"))?;
                        }
                        digest.update(&bytes);
                        yield bytes;
                    }
                    if size != object.size as u64 || hex::encode(digest.finalize()) != object.etag {
                        Err(anyhow::anyhow!("completion source checksum differs"))?;
                    }
                    total = total.checked_add(size).context("completion size overflow")?;
                    if total > expected {
                        Err(anyhow::anyhow!("completion sources exceed manifest size"))?;
                    }
                }
                if reply.next.is_none() { break; }
                first = next as u32;
            }
            if total != expected {
                Err(anyhow::anyhow!("completion sources are incomplete"))?;
            }
        })
    }
}
