use super::{App, Bucket};
use crate::{
    authorization::{Action, Permit},
    content::codec,
    http::object::Read,
    model::{
        StreamPurpose,
        external::Snapshot,
        mirroring::{Config, Source, Start, View},
        task::Execution,
    },
    storage::origin,
};
use anyhow::{Context, Result, ensure};
use axum::http::{HeaderMap, Method, StatusCode, header};
use futures_util::StreamExt;
use s3s::dto::StreamingBlob;
use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;
use uuid::Uuid;

pub(crate) type Readers = Mutex<HashMap<Uuid, oneshot::Sender<Result<Read>>>>;

pub(crate) fn missing(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<s3s::S3Error>()
        .is_some_and(|error| error.code() == &s3s::S3ErrorCode::NoSuchKey)
}

struct Waiting<'a>(&'a Readers, Uuid);
impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.lock().unwrap().remove(&self.1);
    }
}

impl App {
    pub(crate) async fn mirroring_config(&self, bucket: Uuid) -> Result<View> {
        let record = self.mirroring.get(bucket).await?;
        let config = match record.encrypted {
            Some(bytes) => serde_json::from_str(&codec::unprotect(
                &bytes,
                &self.secrets.credential_key,
                format!("bucket-mirroring-{bucket}-{}", record.revision).as_bytes(),
            )?)?,
            None => Config::default(),
        };
        Ok(View {
            revision: record.revision,
            config,
        })
    }

    // This is called only after a true missing-object result and read authorization.
    pub(crate) async fn mirror_read(
        &self,
        bucket: &Bucket,
        key: &str,
        request: &HeaderMap,
        public: bool,
        wait: bool,
    ) -> Result<Option<Read>> {
        self.writable()?;
        let _admission = if public {
            Some(self.admit(false).await?)
        } else {
            None
        };
        ensure!(
            bucket.state == "active" && !request.contains_key("x-mokyu-origin-hop"),
            s3s::s3_error!(NoSuchKey)
        );
        let view = self.mirroring_config(bucket.id).await?;
        let rule = view
            .config
            .select(key)
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        ensure!(!public || rule.public_read, s3s::s3_error!(AccessDenied));
        let proposed = Uuid::now_v7();
        let (sender, mut receiver) = oneshot::channel();
        let stream = !wait && self.role == "single";
        if stream {
            self.mirror_readers.lock().unwrap().insert(proposed, sender);
        }
        let _waiting = Waiting(&self.mirror_readers, proposed);
        let task = self
            .mirroring
            .request(Start {
                foreground: false,
                bucket: bucket.id,
                key: key.into(),
                revision: view.revision,
                rule: rule.id,
                task: proposed,
                storage: bucket
                    .storage_id
                    .ok_or_else(|| s3s::s3_error!(ServiceUnavailable))?,
            })
            .await?;
        self.work_queued();
        let first = stream && task == proposed;
        let wait = async {
            loop {
                if first {
                    match receiver.try_recv() {
                        Ok(read) => return read.map(Some),
                        Err(oneshot::error::TryRecvError::Closed) => {}
                        Err(oneshot::error::TryRecvError::Empty) => {}
                    }
                }
                let state = self.task_execution.load(task).await?;
                if state.state == "running"
                    && origin::requested(request)
                    && let Some(plan) = self
                        .mirroring
                        .range(bucket.id, key, &self.secrets.credential_key)
                        .await?
                {
                    let network = self.storage.reads.acquire().await?;
                    let memory = self.storage.pack_work(1024 * 1024).await?;
                    let (sender, receiver) = oneshot::channel();
                    let store = self.mirroring.clone();
                    let owner = Execution {
                        id: plan.task,
                        token: plan.token,
                    };
                    tokio::spawn(async move {
                        if let Ok((bytes, conflict)) = receiver.await {
                            if let Err(error) = store.range_result(owner, bytes, conflict).await {
                                tracing::warn!(error=%error,"origin range report failed");
                            }
                        }
                    });
                    let result = origin::ranged(&plan, request, sender).await;
                    if result.as_ref().is_err_and(origin::changed) {
                        self.mirroring.range_result(owner, 0, true).await?;
                    }
                    let mut read = result?;
                    if let Some(mut body) = read.body.take() {
                        read.body = Some(StreamingBlob::wrap(async_stream::stream! {
                            let _resources = (network,memory);
                            while let Some(bytes) = body.next().await { yield bytes.map_err(io::Error::other); }
                        }));
                    }
                    return Ok(Some(read));
                }
                match state.state.as_str() {
                    "completed" => return Ok(None),
                    "failed" | "paused" | "cancelled" => {
                        if state.detail["origin_missing"].as_bool() == Some(true) {
                            return Err(s3s::s3_error!(NoSuchKey).into());
                        }
                        return Err(s3s::s3_error!(
                            ServiceUnavailable,
                            "origin fill did not complete"
                        )
                        .into());
                    }
                    _ => {}
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };
        let mut read = tokio::time::timeout(
            Duration::from_secs(view.config.header_timeout_seconds.into()),
            wait,
        )
        .await
        .map_err(|_| s3s::s3_error!(SlowDown, "origin fill is still in progress"))??;
        if let Some(read) = &mut read
            && let Some(mut body) = read.body.take()
        {
            read.body = Some(StreamingBlob::wrap(async_stream::stream! {
                let _admission = _admission;
                while let Some(bytes) = body.next().await { yield bytes.map_err(io::Error::other); }
            }));
        }
        Ok(read)
    }

    async fn origin_snapshot(&self, source: &Source, project: Uuid) -> Result<Option<Snapshot>> {
        let Source::S3 { external_id, .. } = source else {
            return Ok(None);
        };
        let mut permit = Permit::system();
        permit.add(*external_id, Action::Read);
        let record = self
            .external_buckets
            .authorized(&permit)
            .await?
            .pop()
            .context("origin connection missing")?;
        ensure!(
            record.bucket.project_id == project,
            s3s::s3_error!(AccessDenied)
        );
        Ok(Some(Snapshot::from_record(
            record,
            &self.secrets.credential_key,
        )?))
    }

    async fn open_origin(
        &self,
        task: Execution,
        source: &Source,
        config: &Config,
        key: &str,
        project: Uuid,
    ) -> Result<origin::Open> {
        let snapshot = self.origin_snapshot(source, project).await?;
        self.mirroring
            .bind_source(task, snapshot.as_ref().map(|s| (s.external_id, s.revision)))
            .await?;
        let opened = origin::open(
            source,
            config,
            key,
            snapshot.as_ref(),
            Method::GET,
            HeaderMap::new(),
        )
        .await?;
        ensure!(
            opened.status == StatusCode::OK,
            s3s::s3_error!(
                ServiceUnavailable,
                "origin returned an unsolicited partial response"
            )
        );
        Ok(opened)
    }

    pub(crate) async fn mirror_batch(
        self: &Arc<Self>,
        task: Execution,
        bucket: Uuid,
        mut detail: serde_json::Value,
    ) -> Result<bool> {
        let mut reader = self.mirror_readers.lock().unwrap().remove(&task.id);
        let result = self
            .fill_origin(task, bucket, &mut detail, &mut reader)
            .await;
        if let Err(error) = &result {
            let absent = missing(error);
            if !absent {
                let _ = self.mirroring.failed(task, None).await;
            }
            detail["phase"] = "failed".into();
            detail["origin_missing"] = absent.into();
            self.task_control.progress(task, "", 0, detail).await?;
            if let Some(reader) = reader.take() {
                let _ = reader.send(Err(if absent {
                    s3s::s3_error!(NoSuchKey)
                } else {
                    s3s::s3_error!(ServiceUnavailable, "origin fill failed")
                }
                .into()));
            }
        }
        result
    }

    async fn fill_origin(
        self: &Arc<Self>,
        task: Execution,
        bucket: Uuid,
        detail: &mut serde_json::Value,
        reader: &mut Option<oneshot::Sender<Result<Read>>>,
    ) -> Result<bool> {
        let view = self.mirroring_config(bucket).await?;
        ensure!(
            detail["revision"].as_i64() == Some(view.revision),
            s3s::s3_error!(OperationAborted)
        );
        let key = detail["key"]
            .as_str()
            .context("origin key missing")?
            .to_owned();
        let rule = view
            .config
            .select(&key)
            .ok_or_else(|| s3s::s3_error!(OperationAborted))?;
        ensure!(
            detail["rule_id"].as_str() == Some(rule.id.to_string().as_str()),
            s3s::s3_error!(OperationAborted)
        );
        let (target, _, _) = self.buckets.settings(bucket).await?;
        let storage = target
            .storage_id
            .ok_or_else(|| s3s::s3_error!(ServiceUnavailable))?;
        let mapped = rule.key(&key)?;
        let _target = self.storage.profile_permit(storage, "migration").await?;
        let work_memory = self.storage.pack_work(codec::MAX + 1024 * 1024).await?;
        let _network = self.storage.reads.acquire().await?;
        let mut using_backup = false;
        let mut opened = self
            .open_origin(task, &rule.source, &view.config, &mapped, target.project_id)
            .await;
        if opened.as_ref().is_err_and(|error| {
            missing(error)
                || error.downcast_ref::<s3s::S3Error>().is_some_and(|e| {
                    matches!(
                        e.code(),
                        s3s::S3ErrorCode::ServiceUnavailable
                            | s3s::S3ErrorCode::SlowDown
                            | s3s::S3ErrorCode::RequestTimeout
                    )
                })
        }) && let Some(backup) = &rule.backup
        {
            using_backup = true;
            opened = self
                .open_origin(task, backup, &view.config, &mapped, target.project_id)
                .await;
        }
        if opened.as_ref().is_err_and(missing) {
            self.mirroring
                .failed(task, Some(view.config.negative_ttl_seconds))
                .await?;
        }
        let mut opened = opened?;
        detail["source_size"] = serde_json::to_value(opened.length)?;
        // The selected source and validators are bound before another request can read a range.
        detail["origin"] = serde_json::to_value(origin::observed(
            &opened,
            if using_backup {
                rule.backup.as_ref().unwrap()
            } else {
                &rule.source
            },
            using_backup,
        ))?;
        let metadata = origin::metadata(&opened.headers)?;
        let integrity = origin::integrity(&opened.headers);
        let (stream, _pin) = self
            .new_stream(
                &Permit::system(),
                bucket,
                &key,
                StreamPurpose::Mirror,
                serde_json::to_value(&metadata)?,
                rule.public_read,
                None,
                Some((task.id, task.token)),
            )
            .await?;
        let mut proxy = None;
        let mut memory = None;
        if let Some(reader) = reader.take() {
            let hold = work_memory.clone();
            let (send, mut receive) = tokio::sync::mpsc::channel::<io::Result<bytes::Bytes>>(8);
            let body = async_stream::stream! { while let Some(bytes) = receive.recv().await { yield bytes; } };
            let mut headers = crate::http::object::metadata_headers(&metadata)?;
            if let Some(length) = opened.length {
                headers.insert(header::CONTENT_LENGTH, length.to_string().parse()?);
            }
            // Origin validators are deliberately omitted; the stored object gets Mokyu's ETag.
            let read = Read {
                status: StatusCode::OK,
                headers,
                body: Some(StreamingBlob::wrap(body)),
                metadata,
                checksums: serde_json::json!({}),
                partial: false,
            };
            if reader.send(Ok(read)).is_ok() {
                proxy = Some(send);
                memory = Some(hold);
            }
        }
        detail["phase"] = "reading".into();
        self.task_control
            .progress(task, "", 0, detail.clone())
            .await?;
        let error_sender = proxy.clone();
        let app = self.clone();
        let mut progress = detail.clone();
        let length = opened.length.map(i64::try_from).transpose()?;
        let body = async_stream::try_stream! {
            let network = _network;
            let mut bytes_read = 0_i64;
            let mut reported = tokio::time::Instant::now();
            while let Some(bytes) = opened.body.next().await {
                let bytes = bytes?;
                for slice in bytes.chunks(64 * 1024) {
                    let bytes = if let Some(memory) = &memory { crate::storage::external::buffered(slice.to_vec(), memory) } else { bytes::Bytes::copy_from_slice(slice) };
                    bytes_read += bytes.len() as i64;
                    if let Some(send) = &proxy {
                        if !matches!(tokio::time::timeout(Duration::from_secs(30), send.send(Ok(bytes.clone()))).await, Ok(Ok(()))) { proxy = None; }
                    }
                    yield bytes;
                }
                if reported.elapsed() >= Duration::from_secs(1) {
                    progress["source_bytes"] = bytes_read.into();
                    app.task_control.progress(task, "", 0, progress.clone()).await?;
                    if !app.task_control.running(task).await? { Err(s3s::s3_error!(OperationAborted))?; }
                    reported = tokio::time::Instant::now();
                }
            }
            drop(network);
        };
        let body = crate::http::object::blob(Box::pin(
            body.map(|value: Result<bytes::Bytes>| value.map_err(io::Error::other)),
        ));
        let received = self
            .receive_origin(stream, storage, body, &integrity, length)
            .await;
        let (size, _, _, _) = match received {
            Ok(value) => value,
            Err(error) => {
                if let Some(sender) = error_sender {
                    let _ = sender.try_send(Err(io::Error::other("origin fill interrupted")));
                }
                return Err(error);
            }
        };
        drop(error_sender);
        detail["phase"] = "publishing".into();
        detail["source_bytes"] = size.into();
        self.task_control
            .progress(task, "", 0, detail.clone())
            .await?;
        self.publish(stream, (bucket, &key), None, None).await?;
        detail["phase"] = "saved".into();
        detail["published"] = true.into();
        self.task_control
            .progress(task, "", 1, detail.clone())
            .await?;
        self.task_control.complete(task).await?;
        Ok(false)
    }
}
