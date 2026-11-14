use crate::{
    config::Secrets,
    content::fragment::{Area, Header},
    model::{
        storage_cluster::{Cluster, StorageClusters, Target},
        storage_native::{Condition, Object, ReadLease, Write},
        storage_payload::{BATCH_LIMIT, Intent, Mapping, StoragePayloads},
        storage_transfer::{Action, Completion, Destination, Grant, SignedGrant},
    },
};
use anyhow::{Context, Result, ensure};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::Instant};
use uuid::Uuid;

mod maintenance;

struct Snapshot {
    cluster: Cluster,
    targets: Vec<Target>,
    expires: Instant,
}

pub(crate) struct Control {
    clusters: Arc<dyn StorageClusters>,
    payloads: Arc<dyn StoragePayloads>,
    secrets: Arc<Secrets>,
    cached: Mutex<HashMap<Uuid, Arc<Snapshot>>>,
    refresh: crate::runtime::locks::Locks<Uuid>,
}

pub(crate) struct WriteRequest<'a> {
    pub namespace: Uuid,
    pub cluster: Uuid,
    pub path: &'a str,
    pub owner: Uuid,
    pub condition: Condition,
    pub area: Area,
    pub total: u64,
    pub region: Option<&'a str>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct Piece {
    pub mapping: Mapping,
    pub destination: Option<Destination>,
    pub region: Option<String>,
}

fn until() -> i64 {
    chrono::Utc::now().timestamp_micros() + 90_000_000
}

impl Control {
    async fn maintain_member(
        self: &Arc<Self>,
        app: &crate::app::App,
        pool: &super::transport::Pool,
    ) -> Result<bool> {
        use crate::model::storage_cluster::MoveStep;
        let Some(step) = self.clusters.advance_move().await? else {
            return Ok(false);
        };
        match step {
            MoveStep::Advanced => (),
            MoveStep::Evidence {
                job,
                media,
                namespace,
                after,
                through,
            } => {
                for (proof, manifest) in app
                    .journal
                    .native_history(namespace, after, through)
                    .await?
                {
                    let target = self.publication_target(&proof, &manifest, media).await?;
                    pool.publish(&target).await?;
                    self.clusters.move_evidence(job, namespace, &proof).await?;
                }
            }
            MoveStep::Backup {
                job,
                slot,
                receipt,
                remove,
            } => {
                let snap = self.snapshot(job.cluster).await?;
                let find = |media| {
                    snap.targets
                        .iter()
                        .find(|t| t.device.media_id == media)
                        .context("backup replacement member is unavailable")
                };
                let h = &receipt.header;
                if remove {
                    pool.remove(&self.sign(
                        &snap,
                        find(job.source)?,
                        Action::BackupRemove {
                            receipt: receipt.clone(),
                        },
                        None,
                    )?)
                    .await?;
                    self.clusters
                        .moved_backup(&self.secrets.credential_key, job.id, slot, &receipt, None)
                        .await?;
                } else {
                    let source = self.sign(
                        &snap,
                        find(job.source)?,
                        Action::BackupRead {
                            receipt: receipt.clone(),
                            offset: 0,
                            length: h.shard_bytes,
                        },
                        None,
                    )?;
                    let target = self.sign(
                        &snap,
                        find(job.target)?,
                        Action::Backup { header: h.clone() },
                        None,
                    )?;
                    let renewal = Arc::new(MoveRenewal {
                        control: self.clone(),
                        job: job.clone(),
                        receipt: receipt.clone(),
                    });
                    let body = pool.read(source, renewal.clone()).await?;
                    pool.write(
                        target,
                        super::replication::paced(body, job.bytes_per_second as u64),
                        renewal.as_ref(),
                    )
                    .await?;
                    let evidence = if h.area == Area::Recovery {
                        Some(self.payloads.backup_link(slot).await?)
                    } else {
                        None
                    };
                    let check = self.sign(
                        &snap,
                        find(job.target)?,
                        Action::BackupCheck {
                            receipt: receipt.clone(),
                            evidence,
                        },
                        None,
                    )?;
                    let proof = pool.check_backup(&check).await?;
                    self.clusters
                        .moved_backup(
                            &self.secrets.credential_key,
                            job.id,
                            slot,
                            &receipt,
                            Some(&proof),
                        )
                        .await?;
                }
            }
            MoveStep::Collect {
                namespace,
                operation,
                area,
            } => {
                self.reclaim(namespace, operation, area, pool).await?;
            }
        }
        Ok(true)
    }

    pub async fn maintain_members(self: &Arc<Self>, app: &crate::app::App) -> Result<()> {
        let pool = Arc::new(super::transport::Pool::new(Duration::from_secs(30)));
        let members = async {
            loop {
                let mut delay = Duration::from_secs(2);
                if app.writable().is_ok() {
                    match self.maintain_member(app, &pool).await {
                        Ok(true) => {
                            app.wake_tasks.notify_waiters();
                            delay = Duration::from_millis(150);
                        }
                        Ok(false) => (),
                        Err(error) => tracing::warn!(%error,"member maintenance will retry"),
                    }
                }
                tokio::time::sleep(delay).await;
            }
        };
        tokio::select! { _=members=>Ok(()), result=self.maintain_runs(app,pool.clone())=>result }
    }

    pub async fn maintain_backups(&self, app: &crate::app::App) -> Result<()> {
        loop {
            let delay = if app.writable().is_ok() {
                match self.payloads.backfill_backups().await {
                    Ok(true) => {
                        app.wake_tasks.notify_waiters();
                        Duration::from_millis(100)
                    }
                    Ok(false) => Duration::from_secs(5),
                    Err(error) => {
                        tracing::warn!(%error,"backup reconciliation will retry");
                        Duration::from_secs(5)
                    }
                }
            } else {
                Duration::from_secs(5)
            };
            tokio::time::sleep(delay).await;
        }
    }
    pub async fn recovery_target(
        &self,
        anchor: &crate::model::storage_cluster::Manifest,
        media: Uuid,
        action: Action,
        previous: Option<&SignedGrant>,
    ) -> Result<Destination> {
        ensure!(
            matches!(
                action,
                Action::Scan {
                    area: Area::Recovery,
                    ..
                } | Action::History { .. }
                    | Action::Layouts
                    | Action::Namespaces
            ) || matches!(&action, Action::Read { receipt, .. } if receipt.header.area == Area::Recovery)
                || matches!(&action,Action::BackupRead {..} if anchor.members.iter().any(|m|m.media_id==media && m.topology.purpose=="backup")),
            "recovery access is read-only"
        );
        let snapshot = self.snapshot(anchor.cluster_id).await?;
        ensure!(
            snapshot.cluster.manifest == *anchor,
            "recovery membership changed"
        );
        let target = snapshot
            .targets
            .iter()
            .find(|t| t.device.media_id == media)
            .context("recovery member is unavailable")?;
        if let Some(previous) = previous {
            previous.verify(anchor)?;
            ensure!(
                previous.grant.expires > chrono::Utc::now().timestamp(),
                "recovery authority expired"
            );
        }
        self.sign(&snapshot, target, action, previous.map(|p| &p.grant))
    }

    pub async fn publication_target(
        &self,
        proof: &crate::model::storage_recovery::Proof,
        history: &crate::model::storage_cluster::Manifest,
        media: Uuid,
    ) -> Result<Destination> {
        proof.verify(history)?;
        let snapshot = self.snapshot(proof.publication.cluster).await?;
        ensure!(
            history.revision <= snapshot.cluster.revision,
            "future recovery publication"
        );
        let target = snapshot
            .targets
            .iter()
            .find(|target| target.device.media_id == media)
            .context("recovery evidence target is unavailable")?;
        self.sign(
            &snapshot,
            target,
            Action::Publish {
                proof: proof.clone(),
                manifest: history.clone(),
            },
            None,
        )
    }

    pub async fn next_fill(
        &self,
        operation: Uuid,
    ) -> Result<Option<crate::model::storage_native::Fill>> {
        self.payloads.next_fill(operation).await
    }
    pub async fn node_replicate(
        &self,
        node: &crate::cluster::NodeState,
        run: crate::model::session::Run,
        request: super::replication::Request,
    ) -> Result<super::replication::Reply> {
        let (plan, expires) = self
            .payloads
            .authorize_fill(node, &self.secrets.credential_key, run, request.work)
            .await?;
        self.replicate(&plan, request.action, expires).await
    }

    pub async fn replicate(
        &self,
        plan: &crate::model::storage_native::Fill,
        action: super::replication::Operation,
        expires: i64,
    ) -> Result<super::replication::Reply> {
        use super::replication::{Operation, Reply};
        let payload = self.payloads.get(plan.operation).await?;
        ensure!(
            (payload.state == "published" || plan.backup.is_some() && payload.state == "retired")
                && payload.intent.namespace == plan.storage,
            crate::model::Error::OperationAborted
        );
        if let Some(id) = plan.backup {
            return self.replicate_backup(plan, id, action, expires).await;
        }
        if let Some(id) = plan.repair {
            return self.replicate_repair(plan, id, action, expires).await;
        }
        let matches = |header: &Header| {
            header.operation == plan.operation
                && header.namespace == plan.storage
                && header.sequence == plan.sequence
        };
        match action {
            Operation::Checked { .. } => anyhow::bail!("replication is not a repair check"),
            Operation::Window => {
                let rows = self
                    .write_window_until(plan.operation, plan.sequence, 1, expires)
                    .await?;
                let mut targets = Vec::new();
                for piece in rows.iter().filter(|p| p.mapping.receipt.is_none()) {
                    targets.push(
                        piece
                            .destination
                            .clone()
                            .context("replica target is unavailable")?,
                    );
                }
                let mappings = self.read_window(plan.operation, plan.sequence, 1).await?;
                let mut sources = Vec::new();
                for mapping in mappings
                    .iter()
                    .filter(|m| m.receipt.is_some() && !m.deleted)
                {
                    let snapshot = self.snapshot(mapping.header.cluster).await?;
                    if let Some(target) = snapshot
                        .targets
                        .iter()
                        .find(|t| t.device.media_id == mapping.header.media)
                        && let Ok(destination) = self.sign_until(
                            &snapshot,
                            target,
                            Action::Read {
                                receipt: mapping.receipt.clone().unwrap(),
                                offset: 0,
                                length: mapping.header.shard_bytes,
                            },
                            None,
                            expires,
                        )
                    {
                        sources.push(destination);
                    }
                }
                ensure!(
                    targets.is_empty() || !sources.is_empty(),
                    "no durable replica source is online"
                );
                Ok(Reply::Window {
                    sources,
                    targets,
                    rate: 0,
                })
            }
            Operation::Accept { receipts } => {
                ensure!(
                    !receipts.is_empty()
                        && receipts.len() <= 2
                        && receipts.iter().all(|r| matches(&r.receipt.receipt.header)),
                    "replica receipts differ from the task"
                );
                self.accept(plan.operation, &receipts).await?;
                Ok(Reply::Accepted)
            }
            Operation::Renew { grant } => {
                let header = match &grant.grant.action {
                    Action::Read { receipt, .. } => &receipt.header,
                    Action::Write { header } => header,
                    _ => anyhow::bail!("replica task cannot renew this action"),
                };
                ensure!(matches(header), "replica renewal differs from the task");
                Ok(Reply::Renewed {
                    grant: self.renew_until(&grant, expires).await?,
                })
            }
        }
    }

    async fn replicate_backup(
        &self,
        plan: &crate::model::storage_native::Fill,
        id: Uuid,
        action: super::replication::Operation,
        expires: i64,
    ) -> Result<super::replication::Reply> {
        use super::replication::{Operation, Reply};
        let file = self
            .payloads
            .backup_file(id, plan.sequence, plan.shard)
            .await?;
        let header = &file.receipt.header;
        ensure!(
            header.operation == plan.operation
                && header.namespace == plan.storage
                && header.sequence == plan.sequence
                && header.shard == plan.shard,
            "backup task differs from its carrier"
        );
        let snapshot = self.snapshot(header.cluster).await?;
        let destination = snapshot
            .targets
            .iter()
            .find(|v| v.device.media_id == file.media)
            .context("backup media is offline")?;
        ensure!(
            snapshot
                .cluster
                .manifest
                .members
                .iter()
                .any(|m| m.media_id == file.media
                    && m.state == "available"
                    && m.topology.purpose == "backup"),
            "backup media is unavailable"
        );
        match action {
            Operation::Accept { receipts } => {
                ensure!(receipts.len() == 1, "backup requires one full receipt");
                if header.area == Area::Recovery {
                    let pool = super::transport::Pool::new(Duration::from_secs(30));
                    for _ in 0..32 {
                        let Some((proof, history)) =
                            self.payloads.backup_evidence(id, file.media).await?
                        else {
                            break;
                        };
                        let link = proof.link()?;
                        let target = self
                            .publication_target(&proof, &history, file.media)
                            .await?;
                        pool.publish(&target).await?;
                        self.payloads
                            .backup_published(
                                id,
                                file.media,
                                plan.sequence,
                                plan.shard,
                                proof.publication.revision,
                                &link,
                            )
                            .await?;
                    }
                }
                self.payloads
                    .backup_accept(&self.secrets.credential_key, id, &receipts[0])
                    .await?;
                Ok(Reply::Accepted)
            }
            Operation::Renew { grant } => {
                grant.verify(&snapshot.cluster.manifest)?;
                match &grant.grant.action {
                    Action::Backup { header: planned }
                        if !file.verified
                            && planned == header
                            && grant.grant.media == file.media =>
                    {
                        Ok(Reply::Renewed {
                            grant: self
                                .sign_until(
                                    &snapshot,
                                    destination,
                                    grant.grant.action.clone(),
                                    Some(&grant.grant),
                                    expires,
                                )?
                                .authorization,
                        })
                    }
                    Action::Read {
                        receipt,
                        offset: 0,
                        length,
                    } if receipt.header.operation == plan.operation
                        && receipt.header.sequence == plan.sequence
                        && receipt.header.shard == plan.shard
                        && receipt.root == file.receipt.root
                        && receipt.header.digest == header.digest
                        && *length == header.shard_bytes =>
                    {
                        Ok(Reply::Renewed {
                            grant: self.renew_until(&grant, expires).await?,
                        })
                    }
                    _ => anyhow::bail!("backup renewal differs from its task"),
                }
            }
            Operation::Window if file.verified => Ok(Reply::Window {
                sources: vec![],
                targets: vec![],
                rate: 0,
            }),
            Operation::Window => {
                if file.recheck {
                    let evidence = if header.area == Area::Recovery {
                        Some(self.payloads.backup_link(id).await?)
                    } else {
                        None
                    };
                    let grant = self.sign_until(
                        &snapshot,
                        destination,
                        Action::BackupCheck {
                            receipt: file.receipt.clone(),
                            evidence,
                        },
                        None,
                        expires,
                    )?;
                    let pool = super::transport::Pool::new(Duration::from_secs(30));
                    match pool.check_backup(&grant).await {
                        Ok(proof) => {
                            self.payloads
                                .backup_accept(&self.secrets.credential_key, id, &proof)
                                .await?;
                            return Ok(Reply::Window {
                                sources: vec![],
                                targets: vec![],
                                rate: 0,
                            });
                        }
                        Err(e)
                            if e.downcast_ref::<tonic::Status>().is_some_and(|s| {
                                s.code() == tonic::Code::FailedPrecondition
                                    && s.message() == "backup evidence is missing"
                            }) =>
                        {
                            self.payloads
                                .reset_evidence(id, file.media, plan.sequence, plan.shard)
                                .await?;
                        }
                        Err(e)
                            if e.downcast_ref::<tonic::Status>().is_some_and(|s| {
                                matches!(s.code(), tonic::Code::DataLoss | tonic::Code::NotFound)
                            }) => {}
                        Err(e) => return Err(e),
                    }
                }
                let mut sources = Vec::new();
                for mapping in self
                    .payloads
                    .resume_read(plan.operation, plan.sequence, 1, until())
                    .await?
                {
                    if mapping.deleted {
                        continue;
                    }
                    if let Some(receipt) = mapping.receipt
                        && receipt.header.shard == plan.shard
                        && receipt.root == file.receipt.root
                        && receipt.header.digest == header.digest
                        && let Some(target) = snapshot
                            .targets
                            .iter()
                            .find(|t| t.device.media_id == receipt.header.media)
                        && let Ok(source) = self.sign_until(
                            &snapshot,
                            target,
                            Action::Read {
                                receipt,
                                offset: 0,
                                length: header.shard_bytes,
                            },
                            None,
                            expires,
                        )
                    {
                        sources.push(source);
                    }
                }
                ensure!(!sources.is_empty(), "no verified primary source for backup");
                let target = self.sign_until(
                    &snapshot,
                    destination,
                    Action::Backup {
                        header: header.clone(),
                    },
                    None,
                    expires,
                )?;
                Ok(Reply::Window {
                    sources,
                    targets: vec![target],
                    rate: snapshot.cluster.policy.backup_bytes_per_second,
                })
            }
            Operation::Checked { .. } => anyhow::bail!("backup is not a repair check"),
        }
    }

    async fn replicate_repair(
        &self,
        plan: &crate::model::storage_native::Fill,
        id: Uuid,
        action: super::replication::Operation,
        expires: i64,
    ) -> Result<super::replication::Reply> {
        use super::replication::{Operation, Reply};
        let repair = self.payloads.repair(id).await?;
        let original = repair.original.header();
        ensure!(
            original.operation == plan.operation
                && original.namespace == plan.storage
                && original.sequence == plan.sequence,
            "repair task differs from its physical source"
        );
        match action {
            Operation::Accept { receipts } => {
                ensure!(receipts.len() == 1, "repair needs one complete receipt");
                self.payloads
                    .accept_repair(&self.secrets.credential_key, id, &receipts[0])
                    .await?;
                return Ok(Reply::Accepted);
            }
            Operation::Renew { grant } => {
                let snapshot = self.snapshot(original.cluster).await?;
                grant.verify(&snapshot.cluster.manifest)?;
                if let Action::Repair { header } = &grant.grant.action {
                    ensure!(
                        repair.state == "writing" && repair.candidate.as_ref() == Some(header),
                        "repair grant was replaced"
                    );
                    self.payloads.prepare_repair(id, true).await?;
                    let target = snapshot
                        .targets
                        .iter()
                        .find(|target| target.device.media_id == header.media)
                        .context("repair target is offline")?;
                    return Ok(Reply::Renewed {
                        grant: self
                            .sign_until(
                                &snapshot,
                                target,
                                grant.grant.action.clone(),
                                Some(&grant.grant),
                                expires,
                            )?
                            .authorization,
                    });
                }
                ensure!(
                    matches!(&grant.grant.action, Action::Read { receipt, .. } if receipt.header.operation == plan.operation && receipt.header.sequence == plan.sequence),
                    "repair source renewal differs from its task"
                );
                return Ok(Reply::Renewed {
                    grant: self.renew_until(&grant, expires).await?,
                });
            }
            Operation::Checked { damaged } => {
                self.payloads.prepare_repair(id, damaged).await?;
            }
            Operation::Window => (),
        }
        let repair = self.payloads.repair(id).await?;
        let snapshot = self.snapshot(original.cluster).await?;
        let movement = self
            .clusters
            .move_rate(original.cluster, original.media)
            .await?;
        if repair.state == "queued" && movement.is_none() {
            let target = snapshot
                .targets
                .iter()
                .find(|t| t.device.media_id == original.media)
                .context("repair target is offline")?;
            return Ok(Reply::Check {
                target: self.sign_until(
                    &snapshot,
                    target,
                    Action::Read {
                        receipt: repair
                            .original
                            .receipt()
                            .context("unwritten replica requires a replacement")?
                            .clone(),
                        offset: 0,
                        length: original.shard_bytes,
                    },
                    None,
                    expires,
                )?,
            });
        }
        if matches!(repair.state.as_str(), "installed" | "healthy") {
            return Ok(Reply::Window {
                rate: 0,
                sources: vec![],
                targets: vec![],
            });
        }
        let repair = self.payloads.prepare_repair(id, true).await?;
        let header = repair.candidate.context("repair candidate is missing")?;
        let target = snapshot
            .targets
            .iter()
            .find(|t| t.device.media_id == header.media)
            .context("repair target is offline")?;
        let mut sources = Vec::new();
        for mapping in self.read_window(plan.operation, plan.sequence, 1).await? {
            let Some(receipt) = mapping.receipt else {
                continue;
            };
            if mapping.deleted
                || (receipt.header.fragment == id && header.media == original.media)
                || receipt.header.shard != header.shard
            {
                continue;
            }
            if let Some(target) = snapshot
                .targets
                .iter()
                .find(|target| target.device.media_id == receipt.header.media)
                && let Ok(source) = self.sign_until(
                    &snapshot,
                    target,
                    Action::Read {
                        offset: 0,
                        length: receipt.header.shard_bytes,
                        receipt,
                    },
                    None,
                    expires,
                )
            {
                sources.push(source);
            }
        }
        ensure!(
            !sources.is_empty(),
            "no verified primary copy is available for repair"
        );
        Ok(Reply::Window {
            rate: movement.unwrap_or(0),
            sources,
            targets: vec![self.sign_until(
                &snapshot,
                target,
                Action::Repair { header },
                None,
                expires,
            )?],
        })
    }

    pub async fn request_repair(
        &self,
        principal: &crate::authorization::Principal,
        cluster: Uuid,
        fragment: Uuid,
    ) -> Result<Uuid> {
        self.payloads
            .request_repair(principal, cluster, fragment)
            .await
    }

    pub fn new(
        clusters: Arc<dyn StorageClusters>,
        payloads: Arc<dyn StoragePayloads>,
        secrets: Arc<Secrets>,
    ) -> Self {
        Self {
            clusters,
            payloads,
            secrets,
            cached: Mutex::new(HashMap::new()),
            refresh: Default::default(),
        }
    }

    async fn snapshot(&self, cluster: Uuid) -> Result<Arc<Snapshot>> {
        if let Some(value) = self
            .cached
            .lock()
            .await
            .get(&cluster)
            .filter(|value| value.expires > Instant::now())
        {
            return Ok(value.clone());
        }
        let _refresh = self.refresh.get(cluster).lock_owned().await;
        if let Some(value) = self
            .cached
            .lock()
            .await
            .get(&cluster)
            .filter(|value| value.expires > Instant::now())
        {
            return Ok(value.clone());
        }
        let (cluster, targets) = self.clusters.placement_snapshot(cluster).await?;
        let value = Arc::new(Snapshot {
            cluster,
            targets,
            expires: Instant::now() + Duration::from_secs(1),
        });
        let mut cached = self.cached.lock().await;
        if cached.len() >= 256 {
            cached.retain(|_, value| value.expires > Instant::now());
        }
        ensure!(
            cached.len() < 256 || cached.contains_key(&value.cluster.id),
            "too many active storage clusters"
        );
        cached.insert(value.cluster.id, value.clone());
        Ok(value)
    }

    pub async fn reserve(&self, request: WriteRequest<'_>) -> Result<Object> {
        self.reserve_with(request, None).await
    }

    pub async fn reserve_recovery(
        &self,
        request: WriteRequest<'_>,
        identity: crate::model::storage_native::RecoveryIdentity,
    ) -> Result<Object> {
        ensure!(
            request.area == Area::Recovery && !identity.payload.is_nil() && identity.revision > 0,
            "invalid native recovery write"
        );
        self.reserve_with(request, Some(identity)).await
    }

    async fn reserve_with(
        &self,
        request: WriteRequest<'_>,
        recovery: Option<crate::model::storage_native::RecoveryIdentity>,
    ) -> Result<Object> {
        if let Some((object, condition)) = self
            .payloads
            .find_write(request.namespace, request.path, request.owner)
            .await?
        {
            ensure!(
                condition == request.condition
                    && object.payload.intent.cluster == request.cluster
                    && object.payload.intent.total == request.total
                    && object.payload.intent.area == request.area,
                crate::model::Error::OperationAborted
            );
            ensure!(
                recovery.is_none_or(|identity| object.payload.intent.payload == identity.payload
                    && object.payload.intent.revision == identity.revision),
                "recovery write identity changed"
            );
            return Ok(object);
        }
        let snapshot = self.snapshot(request.cluster).await?;
        let cluster = &snapshot.cluster;
        let group = cluster.manifest.groups.first().ok_or_else(|| {
            s3s::s3_error!(
                ServiceUnavailable,
                "storage cluster has no enabled layout group"
            )
        })?;
        let payload = recovery.map_or_else(Uuid::now_v7, |identity| identity.payload);
        self.payloads
            .reserve_object(
                &Write {
                    recovery,
                    region: request.region.map(str::to_owned),
                    path: request.path.to_owned(),
                    owner: request.owner,
                    condition: request.condition,
                    intent: Intent {
                        operation: Uuid::now_v7(),
                        namespace: request.namespace,
                        payload,
                        revision: recovery.map_or(1, |identity| identity.revision),
                        cluster: cluster.id,
                        layout_revision: cluster.manifest.revision as u64,
                        takeover_epoch: cluster.manifest.takeover_epoch,
                        group: group.id,
                        layout: cluster.policy.layout.clone(),
                        members: group.members.clone(),
                        area: request.area,
                        total: request.total,
                        acknowledgement: cluster.policy.acknowledgement,
                    },
                },
                until(),
            )
            .await
    }

    pub async fn open(&self, namespace: Uuid, path: &str, owner: Option<Uuid>) -> Result<Object> {
        self.payloads
            .read_object(namespace, path, owner, until())
            .await
    }

    pub async fn publish(&self, namespace: Uuid, payload: Uuid, owner: Uuid) -> Result<()> {
        self.payloads
            .publish_object(namespace, payload, owner)
            .await
    }

    pub async fn remove(&self, namespace: Uuid, path: &str) -> Result<()> {
        self.payloads.remove_object(namespace, path, None).await
    }

    pub async fn list(
        &self,
        namespace: Uuid,
        after: Option<&str>,
    ) -> Result<Vec<crate::model::storage_native::Metadata>> {
        self.payloads.list_objects(namespace, after).await
    }

    pub async fn node_read(
        &self,
        node: &crate::cluster::NodeState,
        run: crate::model::session::Run,
        request: &crate::model::storage_native::access::ReadRequest,
    ) -> Result<super::reader::Plan> {
        request.validate()?;
        let scope = &request.access;
        let expires = self
            .payloads
            .authorize_read(node, &self.secrets.credential_key, run, scope)
            .await?;
        self.plan_read(
            scope.storage,
            &scope.path,
            request.version,
            request.lease.as_ref(),
            scope.range.clone(),
            request.region.as_deref(),
            expires,
        )
        .await
    }

    pub async fn node_maintenance(
        &self,
        node: &crate::cluster::NodeState,
        run: crate::model::session::Run,
        input: &crate::model::storage_native::access::Maintenance,
    ) -> Result<Vec<crate::model::storage_native::Metadata>> {
        self.payloads
            .maintain(node, &self.secrets.credential_key, run, input)
            .await
    }

    pub async fn node_upload(
        &self,
        node: &crate::cluster::NodeState,
        run: crate::model::session::Run,
        input: &crate::model::storage_native::access::Upload,
        region: Option<&str>,
    ) -> Result<Intent> {
        if let Some(region) = region {
            crate::config::storage::label(region, "region")?;
        }
        let access = self
            .payloads
            .authorize_upload(node, &self.secrets.credential_key, run, input)
            .await?;
        Ok(self
            .reserve(WriteRequest {
                namespace: input.storage,
                cluster: access.cluster,
                path: &input.path,
                owner: access.owner,
                condition: Condition::Create,
                area: Area::Blocks,
                total: input.size,
                region,
            })
            .await?
            .payload
            .intent)
    }

    async fn upload_access(
        &self,
        node: &crate::cluster::NodeState,
        run: crate::model::session::Run,
        input: &crate::model::storage_native::access::Upload,
        operation: Uuid,
    ) -> Result<i64> {
        let access = self
            .payloads
            .authorize_upload(node, &self.secrets.credential_key, run, input)
            .await?;
        let object = self
            .open(input.storage, &input.path, Some(access.owner))
            .await?;
        ensure!(
            object.payload.intent.operation == operation
                && object.payload.intent.total == input.size
                && object.payload.intent.cluster == access.cluster
                && object.payload.intent.area == Area::Blocks,
            s3s::s3_error!(AccessDenied)
        );
        Ok(access.expires)
    }

    pub async fn node_write_window(
        &self,
        node: &crate::cluster::NodeState,
        run: crate::model::session::Run,
        input: &crate::model::storage_native::access::Upload,
        operation: Uuid,
        sequence: u32,
    ) -> Result<Vec<Piece>> {
        let expires = self.upload_access(node, run, input, operation).await?;
        self.write_window_until(operation, sequence, 1, expires)
            .await
    }

    pub async fn node_accept(
        &self,
        node: &crate::cluster::NodeState,
        run: crate::model::session::Run,
        input: &crate::model::storage_native::access::Upload,
        operation: Uuid,
        completed: &[Completion],
    ) -> Result<()> {
        ensure!(
            !completed.is_empty() && completed.len() <= 2,
            "invalid native completion batch"
        );
        self.upload_access(node, run, input, operation).await?;
        self.accept(operation, completed).await
    }

    pub async fn renew_write(
        &self,
        node: &crate::cluster::NodeState,
        run: crate::model::session::Run,
        input: &crate::model::storage_native::access::Upload,
        signed: &SignedGrant,
    ) -> Result<SignedGrant> {
        let Action::Write { header } = &signed.grant.action else {
            anyhow::bail!("write authority required");
        };
        let expires = self
            .upload_access(node, run, input, header.operation)
            .await?;
        self.renew_until(signed, expires).await
    }

    pub async fn renew_read(
        &self,
        node: &crate::cluster::NodeState,
        run: crate::model::session::Run,
        access: &crate::model::storage_native::access::Read,
        signed: &SignedGrant,
    ) -> Result<SignedGrant> {
        let expires = self
            .payloads
            .authorize_read(node, &self.secrets.credential_key, run, access)
            .await?;
        let Action::Read {
            receipt,
            offset,
            length,
        } = &signed.grant.action
        else {
            anyhow::bail!("read authority required");
        };
        let range = access.range.as_ref().context("read range is missing")?;
        let start = receipt
            .header
            .offset
            .checked_add(*offset)
            .context("native read offset overflow")?;
        let end = start
            .checked_add(*length)
            .context("native read end overflow")?;
        ensure!(
            receipt.header.namespace == access.storage && start >= range.start && end <= range.end,
            s3s::s3_error!(AccessDenied)
        );
        let object = self.open(access.storage, &access.path, None).await?;
        ensure!(
            object.payload.intent.operation == receipt.header.operation,
            crate::model::Error::StorageObjectChanged
        );
        self.renew_until(signed, expires).await
    }

    pub async fn plan_read(
        &self,
        namespace: Uuid,
        path: &str,
        expected: Option<Uuid>,
        lease: Option<&ReadLease>,
        range: Option<std::ops::Range<u64>>,
        region: Option<&str>,
        expires: i64,
    ) -> Result<super::reader::Plan> {
        use super::reader::{Metadata, Part, Plan};
        let resuming = lease.is_some();
        let object = if let Some(lease) = lease {
            lease.verify(&self.secrets.credential_key, namespace, path)?;
            ensure!(
                expected == Some(lease.operation),
                "native read lease differs from its version"
            );
            let mut object = self
                .payloads
                .resume_object(namespace, path, lease.operation, until())
                .await?;
            object.modified = lease.modified;
            object
        } else {
            self.open(namespace, path, None).await?
        };
        let intent = &object.payload.intent;
        ensure!(
            expected.is_none_or(|id| id == intent.operation),
            crate::model::Error::StorageObjectChanged
        );
        ensure!(
            intent.layout.data_shards == 1
                && intent.layout.parity_shards == 0
                && intent.layout.extra_copies == 1,
            "native reads require a double-copy layout"
        );
        let lease = ReadLease::issue(&self.secrets.credential_key, namespace, path, &object)?;
        let mut plan = Plan {
            lease,
            object: Metadata {
                path: path.into(),
                version: intent.operation,
                size: intent.total,
                modified: object.modified,
            },
            range,
            parts: Vec::new(),
            parallelism: 1,
        };
        let Some(range) = &plan.range else {
            return Ok(plan);
        };
        ensure!(
            range.start <= range.end
                && range.end <= intent.total
                && range.end - range.start <= super::reader::WINDOW,
            "invalid native read range"
        );
        if range.is_empty() {
            return Ok(plan);
        }
        let first = range.start / intent.span();
        let count = (range.end - 1) / intent.span() - first + 1;
        ensure!(count <= 16, "native read exceeds its window");
        let snapshot = self.snapshot(intent.cluster).await?;
        plan.parallelism = snapshot.cluster.policy.read_parallelism.min(2);
        let mappings = if resuming {
            self.payloads
                .resume_read(intent.operation, first as u32, count as u32, until())
                .await?
        } else {
            self.read_window(intent.operation, first as u32, count as u32)
                .await?
        };
        for sequence in first..first + count {
            let start = range.start.max(sequence * intent.span());
            let end = range.end.min((sequence + 1) * intent.span());
            let mut copies: Vec<_> = mappings
                .iter()
                .filter(|mapping| {
                    u64::from(mapping.header.sequence) == sequence
                        && mapping.receipt.is_some()
                        && !mapping.deleted
                })
                .filter_map(|mapping| {
                    snapshot
                        .targets
                        .iter()
                        .find(|target| target.device.media_id == mapping.header.media)
                        .map(|target| (mapping, target))
                })
                .filter(|(_, target)| {
                    target.run.is_some() && target.expires > chrono::Utc::now().timestamp_micros()
                })
                .collect();
            copies.sort_by_key(|(mapping, target)| {
                (
                    region.is_some()
                        && target.device.announcement.topology.region.as_deref() != region,
                    mapping.header.copy,
                )
            });
            ensure!(
                !copies.is_empty(),
                s3s::s3_error!(ServiceUnavailable, "no durable storage copy is online")
            );
            let mut offset = start;
            while offset < end {
                let length = super::reader::PART.min(end - offset);
                let preferred = if plan.parallelism > 1 {
                    plan.parts.len() % copies.len()
                } else {
                    0
                };
                let mut destinations = Vec::new();
                for index in 0..copies.len() {
                    let (mapping, target) = copies[(preferred + index) % copies.len()];
                    if let Ok(destination) = self.sign_until(
                        &snapshot,
                        target,
                        Action::Read {
                            receipt: mapping.receipt.clone().unwrap(),
                            offset: offset - mapping.header.offset,
                            length,
                        },
                        None,
                        expires,
                    ) {
                        destinations.push(destination);
                    }
                }
                ensure!(
                    !destinations.is_empty(),
                    "storage read authority is expiring"
                );
                plan.parts.push(Part {
                    offset,
                    length,
                    copies: destinations,
                });
                offset += length;
            }
        }
        plan.validate()?;
        Ok(plan)
    }

    pub async fn write_window(
        &self,
        operation: Uuid,
        first: u32,
        count: u32,
    ) -> Result<Vec<Piece>> {
        self.write_window_until(operation, first, count, i64::MAX)
            .await
    }

    async fn write_window_until(
        &self,
        operation: Uuid,
        first: u32,
        count: u32,
        expires: i64,
    ) -> Result<Vec<Piece>> {
        let value = self.payloads.get(operation).await?;
        let snapshot = self.snapshot(value.intent.cluster).await?;
        let reserved = self.payloads.reserve_targets(operation).await?;
        let mappings = self
            .payloads
            .write_window(operation, first, count, until())
            .await?;
        let mut pieces = Vec::with_capacity(mappings.len());
        for mapping in mappings {
            let target = snapshot
                .targets
                .iter()
                .find(|target| target.device.media_id == mapping.header.media);
            let available = snapshot.cluster.manifest.members.iter().any(|member| {
                member.media_id == mapping.header.media && member.state == "available"
            });
            let destination = if available && reserved.contains(&mapping.header.media) {
                target.and_then(|target| {
                    self.sign_until(
                        &snapshot,
                        target,
                        Action::Write {
                            header: mapping.header.clone(),
                        },
                        None,
                        expires,
                    )
                    .ok()
                })
            } else {
                None
            };
            ensure!(
                destination.is_some()
                    || mapping.receipt.is_some()
                    || !value
                        .intent
                        .required(mapping.header.copy, mapping.header.shard),
                "required storage write target is unavailable"
            );
            pieces.push(Piece {
                mapping,
                destination,
                region: target
                    .and_then(|target| target.device.announcement.topology.region.clone()),
            });
        }
        Ok(pieces)
    }

    pub async fn read_window(
        &self,
        operation: Uuid,
        first: u32,
        count: u32,
    ) -> Result<Vec<Mapping>> {
        self.payloads.read(operation, first, count, until()).await
    }

    pub async fn read(&self, mapping: &Mapping, offset: u64, length: u64) -> Result<Destination> {
        ensure!(!mapping.deleted, "storage fragment was retired");
        let receipt = mapping
            .receipt
            .as_ref()
            .context("storage fragment is not durable")?;
        let snapshot = self.snapshot(mapping.header.cluster).await?;
        let target = snapshot
            .targets
            .iter()
            .find(|target| target.device.media_id == mapping.header.media)
            .context("storage read target is unavailable")?;
        self.sign(
            &snapshot,
            target,
            Action::Read {
                receipt: receipt.clone(),
                offset,
                length,
            },
            None,
        )
    }

    pub async fn accept(&self, operation: Uuid, completed: &[Completion]) -> Result<()> {
        self.payloads
            .accept_receipts(&self.secrets.credential_key, operation, completed)
            .await
    }

    pub async fn collect_candidates(&self, namespace: Uuid, limit: usize) -> Result<Vec<Uuid>> {
        self.payloads.collect_candidates(namespace, limit).await
    }

    pub async fn reclaim(
        &self,
        namespace: Uuid,
        operation: Uuid,
        area: Area,
        pool: &super::transport::Pool,
    ) -> Result<bool> {
        let value = self.payloads.get(operation).await?;
        ensure!(
            value.intent.namespace == namespace && value.intent.area == area,
            "cleanup belongs to another physical payload"
        );
        let rows = match self.payloads.reclaim(operation, None).await {
            Ok(rows) => rows,
            Err(error)
                if matches!(
                    error.downcast_ref::<crate::model::Error>(),
                    Some(crate::model::Error::OperationAborted)
                ) =>
            {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        for row in &rows {
            let snapshot = self.snapshot(row.header.cluster).await?;
            let target = snapshot
                .targets
                .iter()
                .find(|target| target.device.media_id == row.header.media)
                .context("recovery cleanup target is unavailable")?;
            let destination = self.sign(
                &snapshot,
                target,
                match &row.receipt {
                    Some(receipt) => Action::Remove {
                        receipt: receipt.clone(),
                    },
                    None => Action::Discard {
                        header: row.header.clone(),
                    },
                },
                None,
            )?;
            pool.remove(&destination).await?;
            self.payloads
                .removed(operation, &[row.header.fragment])
                .await?;
        }
        Ok(self.payloads.reclaim(operation, None).await?.is_empty())
    }

    fn sign(
        &self,
        snapshot: &Snapshot,
        target: &Target,
        action: Action,
        previous: Option<&Grant>,
    ) -> Result<Destination> {
        self.sign_until(snapshot, target, action, previous, i64::MAX)
    }

    fn sign_until(
        &self,
        snapshot: &Snapshot,
        target: &Target,
        action: Action,
        previous: Option<&Grant>,
        limit: i64,
    ) -> Result<Destination> {
        let run = target.run.context("storage node is offline")?;
        let now = chrono::Utc::now().timestamp();
        let expires = (now + 60)
            .min(target.expires / 1_000_000)
            .min(limit / 1_000_000);
        ensure!(
            expires > now + 5 && target.credential.is_some(),
            "storage node lease is expiring"
        );
        let manifest = &snapshot.cluster.manifest;
        let mut revision = manifest.revision as u64;
        if matches!(action, Action::Read { .. }) && previous.is_none() {
            let member = manifest
                .members
                .iter()
                .find(|m| m.media_id == target.device.media_id)
                .context("storage member is missing")?;
            ensure!(member.state != "paused", "storage member is paused");
            // An unrelated topology edit must not interrupt reads from the last acknowledged layout.
            if let Some(announced) = &target.device.announcement.manifest {
                ensure!(
                    announced.takeover_epoch == manifest.takeover_epoch
                        && announced.deployment_id == manifest.deployment_id,
                    "storage read membership is stale"
                );
                revision = revision.min(announced.revision as u64);
            }
        }
        let grant = Grant {
            id: previous.map_or_else(Uuid::now_v7, |grant| grant.id),
            deployment: manifest.deployment_id,
            node: target.device.node_id,
            media: target.device.media_id,
            boot: run.boot,
            fence: run.fence,
            cluster: manifest.cluster_id,
            takeover_epoch: manifest.takeover_epoch,
            manifest_revision: previous.map_or(revision, |grant| grant.manifest_revision),
            issued: now,
            expires,
            action,
        };
        if let Some(previous) = previous {
            previous.renewal(&grant)?;
        }
        Ok(Destination {
            url: target.device.announcement.topology.url.clone(),
            authorization: SignedGrant::sign(grant, &self.secrets.credential_key, manifest)?,
        })
    }

    pub async fn renew(&self, signed: &SignedGrant) -> Result<SignedGrant> {
        self.renew_until(signed, i64::MAX).await
    }

    async fn renew_until(&self, signed: &SignedGrant, expires: i64) -> Result<SignedGrant> {
        let grant = &signed.grant;
        ensure!(
            matches!(grant.action, Action::Read { .. })
                || grant.expires > chrono::Utc::now().timestamp(),
            "storage authority expired"
        );
        let snapshot = self.snapshot(grant.cluster).await?;
        signed.verify(&snapshot.cluster.manifest)?;
        let (header, writing) = match &grant.action {
            Action::Write { header } => (header, true),
            Action::Read { receipt, .. } => (&receipt.header, false),
            _ => anyhow::bail!("this storage action cannot be renewed"),
        };
        let archived = if writing {
            None
        } else {
            self.payloads
                .resume_repair(header.fragment, until())
                .await?
        };
        let rows = if let Some(archived) = archived {
            vec![archived]
        } else if writing {
            self.payloads
                .write_window(header.operation, header.sequence, 1, until())
                .await?
        } else {
            self.payloads
                .resume_read(header.operation, header.sequence, 1, until())
                .await?
        };
        ensure!(
            rows.len() <= BATCH_LIMIT,
            "storage mapping exceeds its window"
        );
        let mapping = rows
            .iter()
            .find(|mapping| mapping.header.fragment == header.fragment)
            .context("storage fragment is no longer protected")?;
        let mut actual: Header = mapping.header.clone();
        if writing && header.digest == [0; 32] {
            actual.digest = [0; 32];
        }
        ensure!(actual == *header, "storage renewal changed its fragment");
        if let Action::Read { receipt, .. } = &grant.action {
            ensure!(
                mapping.receipt.as_ref() == Some(receipt),
                "storage receipt was replaced"
            );
        }
        let target = snapshot
            .targets
            .iter()
            .find(|target| target.device.media_id == grant.media)
            .context("storage target is unavailable")?;
        Ok(self
            .sign_until(
                &snapshot,
                target,
                grant.action.clone(),
                Some(grant),
                expires,
            )?
            .authorization)
    }
}

struct MoveRenewal {
    control: Arc<Control>,
    job: crate::model::storage_cluster::MemberMove,
    receipt: crate::model::storage_payload::Receipt,
}
#[async_trait::async_trait]
impl super::transport::Renewal for MoveRenewal {
    async fn renew(&self, signed: &SignedGrant) -> Result<SignedGrant> {
        let g = &signed.grant;
        let snap = self.control.snapshot(self.job.cluster).await?;
        signed.verify(&snap.cluster.manifest)?;
        ensure!(
            self.control
                .clusters
                .move_live(self.job.id, self.job.source, self.job.target)
                .await?
                && g.expires > chrono::Utc::now().timestamp()
                && (matches!(&g.action,Action::BackupRead{receipt,offset:0,length} if g.media==self.job.source && receipt==&self.receipt && *length==receipt.header.shard_bytes)
                    || matches!(&g.action,Action::Backup{header} if g.media==self.job.target && *header==self.receipt.header)),
            "backup move lease is no longer active"
        );
        let target = snap
            .targets
            .iter()
            .find(|t| t.device.media_id == g.media)
            .context("backup move member is offline")?;
        Ok(self
            .control
            .sign(&snap, target, g.action.clone(), Some(g))?
            .authorization)
    }
}

#[async_trait::async_trait]
impl super::transport::Renewal for Control {
    async fn renew(&self, grant: &SignedGrant) -> Result<SignedGrant> {
        Control::renew(self, grant).await
    }
}
