use super::*;
use crate::{
    model::{
        mirroring::{Assignment, Begin, Source},
        node_work::Outcome,
    },
    storage::origin,
};

impl Writes {
    pub(crate) async fn mirror_range(
        &self,
        prepared: super::super::reads::Prepared,
        request: &hyper::HeaderMap,
    ) -> Result<crate::http::object::Read> {
        prepared.check()?;
        let plan: crate::model::mirroring::Ranged = serde_json::from_slice(
            prepared
                .reply
                .origin_range_json
                .as_deref()
                .context("origin range missing")?,
        )?;
        let network = self.reads.reader.io.clone().acquire_owned().await?;
        let buffers = self.reads.reader.work_memory(1024 * 1024).await?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let node = self.reads.node.clone();
        let run = prepared.run.clone();
        tokio::spawn(async move {
            if let Ok((bytes, conflict)) = receiver.await {
                let _ = node
                    .client
                    .report_origin(wire::OriginReport {
                        run: Some(run.run.clone()),
                        task_id: plan.task.to_string(),
                        token: plan.token.to_string(),
                        bytes,
                        conflict,
                    })
                    .await;
            }
        });
        let result = origin::ranged(&plan, request, sender).await;
        if result.as_ref().is_err_and(origin::changed) {
            let report = wire::OriginReport {
                run: Some(prepared.run.run.clone()),
                task_id: plan.task.to_string(),
                token: plan.token.to_string(),
                bytes: 0,
                conflict: true,
            };
            loop {
                prepared.check()?;
                match self.reads.node.client.report_origin(report.clone()).await {
                    Ok(_) => break,
                    Err(error) if crate::rpc::client::retryable(&error) => {
                        tokio::time::sleep(Duration::from_millis(200)).await
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        let mut read = result?;
        if let Some(mut body) = read.body.take() {
            read.body = Some(s3s::dto::StreamingBlob::wrap(async_stream::stream! {
                let _resources = (prepared, network, buffers);
                while let Some(bytes) = body.next().await { yield bytes.map_err(std::io::Error::other); }
            }));
        }
        Ok(read)
    }
    pub(crate) async fn mirror_foreground(
        self: &Arc<Self>,
        prepared: super::super::reads::Prepared,
    ) -> Result<crate::http::object::Read> {
        prepared.check()?;
        let lease: crate::model::mirroring::Lease = serde_json::from_slice(
            prepared
                .reply
                .origin_work_json
                .as_deref()
                .context("origin work missing")?,
        )?;
        let grants = self.reads.node.resources.adopt(
            &prepared.run,
            &[wire::Grant {
                r#type: wire::GrantType::TaskWork as i32,
                id: lease.token.to_string(),
                valid_for_ms: lease.valid_for_ms,
                storage_id: String::new(),
            }],
            prepared.started.into(),
        )?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let writes = self.clone();
        let wait = Duration::from_secs(lease.assignment.config.header_timeout_seconds.into());
        let run = prepared.run.clone();
        tokio::spawn(async move {
            let mut sender = Some(sender);
            let mut events = writes.reads.node.subscribe();
            let result = {
                let body = writes.mirror_body(lease.operation, &lease.assignment, &mut sender);
                tokio::pin!(body);
                loop {
                    let valid = run
                        .check()
                        .and_then(|_| grants.iter().try_for_each(|grant| grant.check()))
                        .map_err(anyhow::Error::from);
                    if let Err(error) = valid {
                        break Err(error);
                    }
                    let until = grants
                        .iter()
                        .map(|grant| grant.deadline())
                        .fold(run.deadline(), std::cmp::min);
                    tokio::select! {
                        _ = events.changed() => {},
                        _ = tokio::time::sleep_until(until) => {},
                        result = &mut body => break result,
                    }
                }
            };
            let missing = result.as_ref().is_err_and(crate::app::mirroring::missing);
            if let Some(sender) = sender {
                let _ = sender.send(Err(if missing {
                    s3s::s3_error!(NoSuchKey)
                } else {
                    s3s::s3_error!(ServiceUnavailable, "origin fill failed")
                }
                .into()));
            }
            if let Err(error) = &result {
                tracing::warn!(error=%error, "origin fill failed");
            }
            let outcome = Outcome::Mirrored {
                size: result.ok(),
                missing,
            };
            let completion = crate::model::node_work::Completion {
                operation: lease.operation,
                outcomes: vec![outcome],
            };
            let request = wire::WorkRequest {
                run: Some(run.run.clone()),
                operation_id: Uuid::now_v7().to_string(),
                completion_json: Some(
                    serde_json::to_vec(&completion).expect("origin outcome serializes"),
                ),
                class: 2,
                max_items: 0,
                cached: vec![],
            };
            loop {
                if run.check().is_err() || grants.iter().any(|grant| grant.check().is_err()) {
                    break;
                }
                match writes
                    .reads
                    .node
                    .client
                    .exchange_work(request.clone())
                    .await
                {
                    Ok(_) => break,
                    Err(error) if crate::rpc::client::retryable(&error) => {
                        tokio::time::sleep(Duration::from_secs(1)).await
                    }
                    Err(error) => {
                        tracing::warn!(code=?error.code(), "origin receipt rejected");
                        break;
                    }
                }
            }
        });
        let mut read = tokio::time::timeout(wait, receiver)
            .await
            .map_err(|_| s3s::s3_error!(SlowDown, "origin fill is still in progress"))?
            .map_err(|_| s3s::s3_error!(ServiceUnavailable))??;
        if let Some(mut body) = read.body.take() {
            read.body = Some(s3s::dto::StreamingBlob::wrap(async_stream::stream! {
                let _prepared = prepared;
                while let Some(bytes) = body.next().await { yield bytes.map_err(std::io::Error::other); }
            }));
        }
        Ok(read)
    }

    pub(crate) async fn mirror(&self, work: Uuid, plan: &Assignment) -> Result<Outcome> {
        let result = self.mirror_body(work, plan, &mut None).await;
        match result {
            Ok(size) => Ok(Outcome::Mirrored {
                size: Some(size),
                missing: false,
            }),
            Err(error) => {
                tracing::warn!(work=%work,error=%error,"origin fill failed");
                Ok(Outcome::Mirrored {
                    size: None,
                    missing: crate::app::mirroring::missing(&error),
                })
            }
        }
    }
    async fn mirror_body(
        &self,
        work: Uuid,
        plan: &Assignment,
        reader: &mut Option<tokio::sync::oneshot::Sender<Result<crate::http::object::Read>>>,
    ) -> Result<u64> {
        let memory = Arc::new(self.reads.reader.acquire(&self.slots, 1).await?);
        let buffers = self
            .reads
            .reader
            .work_memory(codec::MAX + 1024 * 1024)
            .await?;
        let network = self.reads.reader.io.clone().acquire_owned().await?;
        let mapped = plan.rule.key(&plan.key)?;
        let connection = |source: &Source| match source {
            Source::S3 { external_id, .. } => plan
                .connections
                .iter()
                .find(|s| s.external_id == *external_id),
            _ => None,
        };
        let mut selected = &plan.rule.source;
        let mut opened = origin::open(
            selected,
            &plan.config,
            &mapped,
            connection(selected),
            hyper::Method::GET,
            hyper::HeaderMap::new(),
        )
        .await;
        if opened.as_ref().is_err_and(origin::retry_backup)
            && let Some(backup) = &plan.rule.backup
        {
            selected = backup;
            opened = origin::open(
                selected,
                &plan.config,
                &mapped,
                connection(selected),
                hyper::Method::GET,
                hyper::HeaderMap::new(),
            )
            .await;
        }
        let mut opened = opened?;
        ensure!(
            opened.status == hyper::StatusCode::OK,
            s3s::s3_error!(ServiceUnavailable)
        );
        let length = opened.length;
        let check = Integrity::new(&origin::integrity(&opened.headers), None)?;
        let node = &self.reads.node;
        let snapshot = node.background_snapshot()?;
        let held = self.reads.cache.snapshot();
        let started = Instant::now();
        let write = Uuid::now_v7();
        let reply = node
            .client
            .begin_mirror(wire::BeginMirrorRequest {
                run: Some(snapshot.lease.run.clone()),
                work_id: work.to_string(),
                operation_id: write.to_string(),
                cached: reads::cached(&held),
                input_json: serde_json::to_vec(&Begin {
                    source: connection(selected).map(|s| (s.external_id, s.revision)),
                    metadata: serde_json::to_value(origin::metadata(&opened.headers)?)?,
                    size: length.map(i64::try_from).transpose()?,
                    observed: origin::observed(
                        &opened,
                        selected,
                        !std::ptr::eq(selected, &plan.rule.source),
                    ),
                })?,
            })
            .await?;
        let request = Request {
            reply: wire::RequestReply::default(),
            access: String::new(),
            bucket: String::new(),
            query: None,
            run: snapshot.lease.clone(),
            materials: held.clone(),
            memory,
        };
        let mut operation = self.accept(
            request,
            reply,
            held,
            started,
            false,
            plan.config.max_object_size.unwrap_or(5 * 1024_u64.pow(4)),
        )?;
        operation.packing = operation.backend.profile.pack_enabled;
        let mut proxy = None;
        if let Some(reader) = reader.take() {
            let (send, mut receive) = tokio::sync::mpsc::channel::<std::io::Result<Bytes>>(8);
            let metadata = origin::metadata(&opened.headers)?;
            let mut headers = crate::http::object::metadata_headers(&metadata)?;
            if let Some(length) = length {
                headers.insert(hyper::header::CONTENT_LENGTH, length.to_string().parse()?);
            }
            let read = crate::http::object::Read {
                status: hyper::StatusCode::OK,
                headers,
                metadata,
                body: Some(s3s::dto::StreamingBlob::wrap(
                    async_stream::stream! { while let Some(bytes) = receive.recv().await { yield bytes; } },
                )),
                checksums: serde_json::json!({}),
                partial: false,
            };
            if reader.send(Ok(read)).is_ok() {
                proxy = Some(send);
            }
        }
        let error_sender = proxy.clone();
        let source_bytes = opened.bytes.clone();
        let progress_node = self.reads.node.clone();
        let progress_run = snapshot.lease.run.clone();
        let body=crate::http::object::blob(async_stream::try_stream! {
            let network=network;
            let mut reported = Instant::now();
            while let Some(bytes)=opened.body.next().await {
                let bytes=bytes.map_err(std::io::Error::other)?;
                for slice in bytes.chunks(64 * 1024) {
                    let bytes = crate::storage::external::buffered(slice.to_vec(), &buffers);
                    if let Some(sender) = &proxy {
                        if !matches!(tokio::time::timeout(Duration::from_secs(30), sender.send(Ok(bytes.clone()))).await, Ok(Ok(()))) { proxy=None; }
                    }
                    yield bytes;
                }
                if reported.elapsed() >= Duration::from_secs(1) {
                    progress_node.client.progress_mirror(wire::MirrorProgress { run:Some(progress_run.clone()),
                        operation_id:work.to_string(), bytes:opened.bytes.load(std::sync::atomic::Ordering::Relaxed), publishing:false }).await
                        .map_err(std::io::Error::other)?;
                    reported = Instant::now();
                }
            }
            drop(network);
        }.boxed());
        let result = operation
            .guard(node, &[], async {
                let (size, etag, sums) = self
                    .write_body(&operation, body, check, None, length, None)
                    .await?;
                node.client
                    .progress_mirror(wire::MirrorProgress {
                        run: Some(snapshot.lease.run.clone()),
                        operation_id: work.to_string(),
                        bytes: source_bytes.load(std::sync::atomic::Ordering::Relaxed),
                        publishing: true,
                    })
                    .await?;
                #[cfg(feature = "fault-injection")]
                crate::faults::point("mirror-before-publish").await;
                self.publish(&operation, size, &etag, &sums, Default::default())
                    .await?;
                Ok(size)
            })
            .await;
        if result.is_err() {
            if let Some(sender) = error_sender {
                let _ = sender.try_send(Err(std::io::Error::other("origin fill interrupted")));
            }
            let _ = node.client.abort_write(operation.lookup()).await;
        }
        result
    }
}
