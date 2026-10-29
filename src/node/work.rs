use super::{NodeContext, reads, resources::Grant};
use crate::{model::node_work as work, rpc::wire};
use anyhow::{Context, Result, ensure};
use object_store::{ObjectStoreExt, path::Path};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;
use uuid::Uuid;

#[path = "sweep.rs"]
mod sweep;

pub(crate) struct Worker {
    node: Arc<NodeContext>,
    cache: reads::Cache,
    reader: super::body::Reader,
    writes: super::writes::Writes,
}

impl Worker {
    pub(crate) fn new(node: Arc<NodeContext>) -> Result<Self> {
        let mut reads = reads::Reads::new(node.clone())?;
        reads.reader.background = true;
        let mut reader = super::body::Reader::new(&node)?;
        reader.background = true;
        Ok(Self {
            writes: super::writes::Writes::new(Arc::new(reads))?,
            reader,
            cache: reads::Cache::for_node(&node),
            node,
        })
    }

    #[cfg(test)]
    pub(crate) async fn block_reads(&self) -> Result<tokio::sync::OwnedSemaphorePermit> {
        let count = self
            .node
            .budget
            .get()
            .context("node budget missing")?
            .storage_read_slots;
        Ok(self
            .reader
            .io
            .clone()
            .acquire_many_owned(count as u32)
            .await?)
    }

    async fn drain(&self, class: u32) -> Result<()> {
        let snapshot = self.node.background_snapshot()?;
        let mut completion = None;
        let mut fills = Vec::new();
        let mut previous = Vec::<Arc<Grant>>::new();
        let mut upload_resources = None;
        loop {
            let mut maximum = work::MAX_ITEMS;
            if class == 2 && upload_resources.is_none() {
                if previous.is_empty() {
                    let memory = Arc::new(self.writes.slots.clone().acquire_owned().await?);
                    let io = Arc::new(self.writes.io.clone().acquire_owned().await?);
                    upload_resources = Some((memory, io));
                } else if let Ok(memory) = self.writes.slots.clone().try_acquire_owned()
                    && let Ok(io) = self.writes.io.clone().try_acquire_owned()
                {
                    upload_resources = Some((Arc::new(memory), Arc::new(io)));
                } else {
                    // Release completed backend work before waiting for new local capacity.
                    maximum = 0;
                }
            }
            let held = self.cache.snapshot();
            let operation = Uuid::now_v7();
            let input = wire::WorkRequest {
                run: Some(snapshot.lease.run.clone()),
                operation_id: operation.to_string(),
                completion_json: completion
                    .take()
                    .map(|value: work::Completion| serde_json::to_vec(&value))
                    .transpose()?,
                class,
                max_items: maximum as u32,
                cached: reads::cached(&held),
            };
            let (reply, started) = loop {
                snapshot.lease.check()?;
                for grant in &previous {
                    grant.check()?;
                }
                let started = Instant::now();
                match self.node.client.exchange_work(input.clone()).await {
                    Ok(reply) => break (reply, started),
                    Err(error) if crate::rpc::client::retryable(&error) => {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            previous.clear();
            ensure!(
                reply.operation_id == operation.to_string(),
                "work identity differs"
            );
            snapshot.lease.check()?;
            let materials = self.cache.apply(
                self.node.client.identity.deployment_id,
                held,
                reply.materials,
            )?;
            crate::storage::cache::publish_fills(std::mem::take(&mut fills));
            let Some(json) = reply.plan_json else {
                if maximum == 0 {
                    continue;
                }
                return Ok(());
            };
            let plan: work::Plan = serde_json::from_slice(&json)?;
            if !matches!(plan, work::Plan::UploadRaw { .. }) {
                upload_resources.take();
            }
            let grants = self
                .node
                .resources
                .adopt(&snapshot.lease, &reply.grants, started)?;
            ensure!(
                grants.len() == 1
                    && grants[0].reference.r#type == wire::GrantType::TaskWork as i32
                    && grants[0].storage == plan.storage(),
                "work protection is missing"
            );
            let future = async {
                if let work::Plan::ExternalTest(test) = &plan {
                    let connection = serde_json::from_slice(
                        reply
                            .external_connection_json
                            .as_deref()
                            .context("external connection missing")?,
                    )?;
                    let _io = self.reader.io.clone().acquire_owned().await?;
                    let report =
                        crate::storage::external_test::execute(test.clone(), &connection).await?;
                    return Ok(work::Completion {
                        operation,
                        outcomes: vec![work::Outcome::ExternalTest(report)],
                    });
                }
                let backend =
                    reads::backend(&materials, plan.storage().context("work storage missing")?)?;
                let outcomes = match plan {
                    work::Plan::ExternalTest(_) => unreachable!(),
                    work::Plan::UploadRaw { storage, items } => {
                        ensure!(
                            !items.is_empty() && items.len() <= work::MAX_ITEMS,
                            "invalid cache upload batch"
                        );
                        let (memory, io) = upload_resources
                            .as_ref()
                            .context("cache upload resources missing")?
                            .clone();
                        let mut outcomes = Vec::with_capacity(items.len());
                        for item in items {
                            ensure!(
                                item.source.storage_id == storage,
                                "cache upload storage differs"
                            );
                            snapshot.lease.check()?;
                            for grant in &grants {
                                grant.check()?;
                            }
                            let result = self
                                .writes
                                .flush_raw(
                                    &backend,
                                    &item.source,
                                    item.hashing,
                                    memory.clone(),
                                    io.clone(),
                                    grants.clone(),
                                )
                                .await;
                            outcomes.push(result.or_else(retry)?);
                        }
                        outcomes
                    }
                    work::Plan::ConvertRaw {
                        source,
                        write,
                        etag,
                        checksums,
                        sealed,
                        part,
                        ..
                    } => {
                        let result = if sealed {
                            Ok(())
                        } else {
                            Box::pin(self.writes.convert(write, source, etag, checksums, part))
                                .await
                        };
                        if let Err(error) = &result {
                            tracing::debug!(%error, "raw conversion postponed");
                        }
                        vec![result.map(|_| work::Outcome::Converted).or_else(retry)?]
                    }
                    work::Plan::Migrate {
                        write,
                        size,
                        sealed,
                        ..
                    } => {
                        let result = if sealed {
                            Ok(())
                        } else {
                            Box::pin(self.writes.migrate(write, size)).await
                        };
                        if let Err(error) = &result {
                            tracing::debug!(%error, "storage migration postponed");
                        }
                        vec![result.map(|_| work::Outcome::Migrated).or_else(retry)?]
                    }
                    work::Plan::HashRaw { storage, items } => {
                        ensure!(items.len() <= work::MAX_ITEMS, "invalid raw hash batch");
                        let count = items.len();
                        let hashing = async {
                            let mut outcomes = Vec::with_capacity(count);
                            for item in items {
                                ensure!(item.source.storage_id == storage, "raw storage differs");
                                snapshot.lease.check()?;
                                for grant in &grants {
                                    grant.check()?;
                                }
                                let (outcome, candidates) =
                                    self.reader.hash_raw(&backend, item).await.or_else(
                                        |error| retry(error).map(|outcome| (outcome, Vec::new())),
                                    )?;
                                fills.extend(candidates);
                                outcomes.push(outcome);
                            }
                            #[cfg(feature = "fault-injection")]
                            crate::faults::point("node-raw-hashed").await;
                            Ok::<_, anyhow::Error>(outcomes)
                        };
                        hashing
                            .await
                            .or_else(|error| retry(error).map(|value| vec![value; count]))?
                    }
                    work::Plan::Collect {
                        items,
                        delete_before,
                        ..
                    } => {
                        ensure!(items.len() <= work::MAX_ITEMS, "invalid work size");
                        let _io = self.reader.acquire(&self.reader.io, 1).await?;
                        if !items.is_empty() {
                            backend.store.ensure_verified().await?;
                        }
                        let mut outcomes = Vec::with_capacity(items.len());
                        for item in items {
                            snapshot.lease.check()?;
                            for grant in &grants {
                                grant.check()?;
                            }
                            let path = match &item.source {
                                work::Physical::Chunk => {
                                    Path::from(crate::storage::chunk_name(item.location))
                                }
                                work::Physical::Pack => Path::from(format!(
                                    "packs/{}",
                                    crate::storage::chunk_name(item.location)
                                )),
                                work::Physical::Raw { .. } => {
                                    crate::storage::raw::remote_path(item.location)
                                }
                                work::Physical::Staging { key } => Path::parse(key)?,
                            };
                            let result = async {
                                if let work::Physical::Raw {
                                    upload: Some(upload),
                                } = &item.source
                                {
                                    let multipart = backend
                                        .store
                                        .multipart
                                        .as_ref()
                                        .context("S3 multipart backend is missing")?;
                                    match multipart.abort_multipart(&path, upload).await {
                                        Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                                        Err(error) => return Err(anyhow::Error::from(error)),
                                    }
                                }
                                let stored_at = if item.inspect_age {
                                    match backend.store.backend.head(&path).await {
                                        Ok(meta) if meta.last_modified > delete_before => {
                                            return Ok(work::Outcome::Retained {
                                                stored_at: meta.last_modified,
                                            });
                                        }
                                        Ok(meta) => Some(meta.last_modified),
                                        Err(object_store::Error::NotFound { .. }) => {
                                            return Ok(work::Outcome::Deleted { stored_at: None });
                                        }
                                        Err(error) => return Err(error.into()),
                                    }
                                } else {
                                    None
                                };
                                match backend.store.backend.delete(&path).await {
                                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                                        Ok(work::Outcome::Deleted { stored_at })
                                    }
                                    Err(error) => Err(error.into()),
                                }
                            }
                            .await;
                            outcomes.push(result.unwrap_or(work::Outcome::Retry));
                        }
                        outcomes
                    }
                    work::Plan::Inspect {
                        snapshot,
                        members,
                        inspect,
                        location,
                    } => {
                        ensure!(
                            location == snapshot.location(),
                            "inspection identity differs"
                        );
                        vec![if inspect {
                            let (result, candidates) = self
                                .reader
                                .inspect(
                                    backend,
                                    Arc::new(reads::keys(&materials)?),
                                    *snapshot,
                                    members,
                                    crate::worker::integrity::Mode::Full,
                                )
                                .await?;
                            fills.extend(candidates);
                            match &result {
                                work::Outcome::Inspected { code: Some(code) }
                                    if !crate::model::source::quarantine_code(code) =>
                                {
                                    work::Outcome::Retry
                                }
                                _ => result,
                            }
                        } else {
                            work::Outcome::Inspected { code: None }
                        }]
                    }
                    work::Plan::Audit { mode, items, .. } => {
                        ensure!(
                            items.len() <= work::MAX_ITEMS
                                && mode != crate::worker::integrity::Mode::Metadata,
                            "invalid inspection plan"
                        );
                        let keys = Arc::new(reads::keys(&materials)?);
                        let count = items.len();
                        let timeout = Duration::from_secs(crate::config::seconds(
                            &backend.profile.integrity_batch_timeout,
                        )?);
                        let checking = async {
                            let mut outcomes = Vec::with_capacity(items.len());
                            for item in items {
                                snapshot.lease.check()?;
                                for grant in &grants {
                                    grant.check()?;
                                }
                                let outcome = if let Some(previous) = item.previous {
                                    previous
                                } else {
                                    let source =
                                        item.snapshot.context("inspection source is missing")?;
                                    ensure!(
                                        source.location() == item.location,
                                        "inspection identity differs"
                                    );
                                    if matches!(&source.physical, crate::model::PhysicalSource::Raw(r) if r.quarantine_code.is_some())
                                    {
                                        work::Outcome::Inspected {
                                            code: Some("source_unavailable".into()),
                                        }
                                    } else {
                                        let (outcome, candidates) = self
                                            .reader
                                            .inspect(
                                                backend.clone(),
                                                keys.clone(),
                                                *source,
                                                item.members,
                                                mode,
                                            )
                                            .await?;
                                        fills.extend(candidates);
                                        outcome
                                    }
                                };
                                if matches!(outcome, work::Outcome::Retry) {
                                    return Ok::<_, anyhow::Error>(vec![
                                        work::Outcome::Retry;
                                        count
                                    ]);
                                }
                                outcomes.push(outcome);
                            }
                            Ok(outcomes)
                        };
                        match tokio::time::timeout(timeout, checking).await {
                            Ok(result) => result?,
                            Err(_) => vec![work::Outcome::Retry; count],
                        }
                    }
                    work::Plan::Pack { batch, .. } => {
                        let keys = Arc::new(reads::keys(&materials)?);
                        let encoding = async {
                            let (result, candidates) =
                                self.reader.pack(backend, keys, *batch).await?;
                            fills.extend(candidates);
                            #[cfg(feature = "fault-injection")]
                            crate::faults::point("node-pack-stored").await;
                            Ok::<_, anyhow::Error>(work::Outcome::Packed(result))
                        };
                        vec![encoding.await.or_else(retry)?]
                    }
                    work::Plan::Rewrite { items, .. } => {
                        ensure!(items.len() <= work::MAX_ITEMS, "invalid rewrite size");
                        let keys = Arc::new(reads::keys(&materials)?);
                        let count = items.len();
                        let encoding = async {
                            let mut outcomes = Vec::with_capacity(count);
                            for item in items {
                                snapshot.lease.check()?;
                                for grant in &grants {
                                    grant.check()?;
                                }
                                let (outcome, candidates) = self
                                    .reader
                                    .rewrite(backend.clone(), keys.clone(), item)
                                    .await?;
                                fills.extend(candidates);
                                outcomes.push(outcome);
                            }
                            #[cfg(feature = "fault-injection")]
                            crate::faults::point("node-rewrite-stored").await;
                            Ok::<_, anyhow::Error>(outcomes)
                        };
                        encoding
                            .await
                            .or_else(|error| retry(error).map(|value| vec![value; count]))?
                    }
                    work::Plan::SweepScan {
                        after,
                        limit,
                        bytes,
                        ..
                    } => {
                        vec![
                            self.scan(backend, after, limit, bytes)
                                .await
                                .or_else(retry)?,
                        ]
                    }
                    work::Plan::SweepDelete { before, items, .. } => {
                        let count = items.len();
                        self.prune(backend, before, items)
                            .await
                            .or_else(|error| retry(error).map(|value| vec![value; count]))?
                    }
                };
                if outcomes
                    .iter()
                    .any(|outcome| matches!(outcome, work::Outcome::Retry))
                {
                    fills.clear();
                }
                Ok::<_, anyhow::Error>(work::Completion {
                    operation,
                    outcomes,
                })
            };
            tokio::pin!(future);
            let mut events = self.node.subscribe();
            completion = Some(loop {
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
                    value=&mut future=>break value?,
                }
            });
            previous = grants.clone();
        }
    }

    pub(crate) async fn run(self) -> Result<()> {
        tokio::try_join!(self.run_class(0), self.run_class(1), self.run_class(2))?;
        Ok(())
    }

    async fn run_class(&self, class: u32) -> Result<()> {
        let mut events = self.node.subscribe();
        loop {
            let state = events.borrow_and_update().clone();
            if state.phase == super::Phase::Stopped {
                return Ok(());
            }
            if state.work_pending
                && let Err(error) = crate::storage::resource_gate::PRIORITY
                    .scope(
                        if class == 2 {
                            crate::storage::resource_gate::UPLOAD
                        } else {
                            crate::storage::resource_gate::MAINTENANCE
                        },
                        self.drain(class),
                    )
                    .await
            {
                tracing::debug!(%error,class,"background work postponed");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            if events.changed().await.is_err() {
                return Ok(());
            }
        }
    }
}

pub(super) fn busy(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.is::<tokio::sync::TryAcquireError>())
}

fn retry(error: anyhow::Error) -> Result<work::Outcome> {
    if busy(&error) {
        Err(error)
    } else {
        tracing::debug!(%error, "background work postponed");
        Ok(work::Outcome::Retry)
    }
}
