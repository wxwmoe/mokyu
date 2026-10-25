use super::{NodeContext, reads, resources::Grant};
use crate::{model::node_work as work, rpc::wire};
use anyhow::{Context, Result, ensure};
use object_store::{ObjectStoreExt, path::Path};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;
use uuid::Uuid;

pub(crate) struct Worker {
    node: Arc<NodeContext>,
    cache: reads::Cache,
}

impl Worker {
    pub(crate) fn new(node: Arc<NodeContext>) -> Self {
        Self {
            node,
            cache: reads::Cache::default(),
        }
    }

    pub(crate) async fn drain(&self) -> Result<()> {
        let snapshot = self.node.background_snapshot()?;
        let mut completion = None;
        let mut previous = Vec::<Arc<Grant>>::new();
        loop {
            let held = self.cache.snapshot();
            let operation = Uuid::now_v7();
            let input = wire::WorkRequest {
                run: Some(snapshot.lease.run.clone()),
                operation_id: operation.to_string(),
                completion_json: completion
                    .take()
                    .map(|value: work::Completion| serde_json::to_vec(&value))
                    .transpose()?,
                class: 0,
                max_items: work::MAX_ITEMS as u32,
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
            let Some(json) = reply.plan_json else {
                return Ok(());
            };
            let plan: work::Plan = serde_json::from_slice(&json)?;
            let grants = self
                .node
                .resources
                .adopt(&snapshot.lease, &reply.grants, started)?;
            ensure!(
                grants.len() == 1
                    && grants[0].reference.r#type == wire::GrantType::TaskWork as i32
                    && grants[0].storage == Some(plan.storage()),
                "work protection is missing"
            );
            let materials = self.cache.apply(
                self.node.client.identity.deployment_id,
                held,
                reply.materials,
            )?;
            let backend = reads::backend(&materials, plan.storage())?;
            let future = async {
                let outcomes = match plan {
                    work::Plan::Collect {
                        items,
                        delete_before,
                        ..
                    } => {
                        ensure!(items.len() <= work::MAX_ITEMS, "invalid work size");
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
                };
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
        let mut events = self.node.subscribe();
        loop {
            tokio::select! {
                _=self.node.work.notified()=>{},
                result=events.changed()=>{
                    if result.is_err() || events.borrow().phase==super::Phase::Stopped {return Ok(());}
                    continue;
                }
            }
            if let Err(error) = self.drain().await {
                tracing::debug!(%error,"background work postponed");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}
