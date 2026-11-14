use super::{Control, WriteRequest, until};
use crate::{
    content::fragment::Area,
    model::{storage_cluster::RunObject, storage_native::Condition, storage_payload::Mapping},
    storage::native::{
        reader,
        replication::paced,
        transport::{Pool, Renewal},
        writer,
    },
};
use anyhow::{Context, Result, ensure};
use futures_util::{StreamExt, TryStreamExt};
use std::{sync::Arc, time::Duration};

impl Control {
    pub(super) async fn maintain_runs(
        self: &Arc<Self>,
        app: &crate::app::App,
        pool: Arc<Pool>,
    ) -> Result<()> {
        loop {
            let delay = if app.writable().is_ok() {
                match self.clusters.next_run().await {
                    Ok(Some(item)) => {
                        let result=async {
                            let work=self.run_object(&pool,&item);
                            tokio::pin!(work);
                            loop {
                                tokio::select! {
                                    value=&mut work=>break value,
                                    _=tokio::time::sleep(Duration::from_secs(20))=>self.clusters.renew_run(item.job.id,item.token).await?,
                                }
                            }
                        }.await;
                        if let Err(error) = result {
                            tracing::warn!(job=%item.job.id,%error,"storage maintenance will retry");
                            if let Err(cause) = self
                                .clusters
                                .run_error(item.job.id, item.token, &error.to_string())
                                .await
                            {
                                tracing::warn!(%cause,"could not checkpoint storage maintenance");
                            }
                        }
                        Duration::from_millis(150)
                    }
                    Ok(None) => Duration::from_secs(2),
                    Err(error) => {
                        tracing::warn!(%error,"could not claim storage maintenance");
                        Duration::from_secs(5)
                    }
                }
            } else {
                Duration::from_secs(3)
            };
            tokio::time::sleep(delay).await;
        }
    }

    async fn check_fragment(
        self: &Arc<Self>,
        pool: &Pool,
        m: &Mapping,
        h: &mut blake3::Hasher,
        rate: u64,
    ) -> Result<()> {
        let size = m.header.shard_bytes;
        let mut pos = 0;
        let mut hash = blake3::Hasher::new();
        loop {
            let len = (size - pos).min(reader::PART);
            let dest = self.read(m, pos, len).await?;
            let mut body = paced(
                pool.read(dest, self.clone() as Arc<dyn Renewal>).await?,
                rate,
            );
            let mut got = 0;
            while let Some(bytes) = body.try_next().await? {
                got += bytes.len() as u64;
                ensure!(got <= len, "storage readback exceeds its fragment");
                hash.update(&bytes);
                h.update(&bytes);
            }
            ensure!(len == got, "storage readback is truncated");
            pos += got;
            if pos == size {
                break;
            }
        }
        ensure!(
            hash.finalize().as_bytes() == &m.header.digest,
            "storage fragment digest differs from its receipt"
        );
        Ok(())
    }

    async fn run_object(self: &Arc<Self>, pool: &Arc<Pool>, item: &RunObject) -> Result<()> {
        let intent = &item.intent;
        let head = match self.open(intent.namespace, &item.path, None).await {
            Ok(head) => Some(head),
            Err(error)
                if crate::model::error(&error)
                    == Some(crate::model::Error::StorageObjectMissing) =>
            {
                None
            }
            Err(error) => return Err(error),
        };
        if head
            .as_ref()
            .is_none_or(|h| h.payload.intent.operation != intent.operation)
        {
            return self.clusters.finish_run_object(item, None).await;
        }
        let rate = item.job.bytes_per_second as u64;
        if item.job.kind != "rebuild" {
            for seq in 0..intent.sequences() as u32 {
                let mappings = self.read_window(intent.operation, seq, 1).await?;
                ensure!(
                    mappings.len() == intent.layout.width(),
                    "storage scrub is waiting for outstanding repairs"
                );
                for m in mappings
                    .iter()
                    .filter(|m| item.job.media_id.is_none_or(|id| id == m.header.media))
                {
                    if let Err(error) = self
                        .check_fragment(pool, m, &mut blake3::Hasher::new(), rate)
                        .await
                    {
                        self.clusters.scrub_damage(item, m).await?;
                        return Err(
                            error.context("storage scrub is waiting for a verified fragment")
                        );
                    }
                }
            }
            return self.clusters.finish_run_object(item, None).await;
        }
        ensure!(
            intent.area == Area::Blocks,
            "recovery files require a new published snapshot"
        );
        let pending = self
            .reserve(WriteRequest {
                namespace: intent.namespace,
                cluster: intent.cluster,
                path: &item.path,
                owner: item.job.id,
                condition: Condition::Rebuild(intent.operation),
                area: Area::Blocks,
                total: intent.total,
                region: None,
            })
            .await?;
        let target = &pending.payload.intent;
        ensure!(
            target.layout.data_shards == 1 && target.layout.parity_shards == 0,
            "unsupported rebuild verification layout"
        );
        let source_plan = if item.job.backup_group.is_none() {
            Some(
                self.plan_read(
                    intent.namespace,
                    &item.path,
                    Some(intent.operation),
                    None,
                    None,
                    None,
                    until(),
                )
                .await?,
            )
        } else {
            None
        };
        let backup = Arc::new(RebuildReader {
            control: self.clone(),
            job: item.job.id,
            token: item.token,
            source: intent.clone(),
        });
        if item.job.backup_group.is_some() && intent.total == 0 {
            let dest = backup
                .destination(intent.operation, 0, 0, 0, 0, None)
                .await?;
            let mut stream = pool.read(dest, backup.clone()).await?;
            while let Some(bytes) = stream.try_next().await? {
                ensure!(bytes.is_empty(), "empty backup returned data");
            }
        }
        let digest = std::sync::Mutex::new(blake3::Hasher::new());
        let hash = &digest;
        let body=async_stream::try_stream! {
            let mut start=0;
            while start<intent.total {
                let (end,body) = if let Some(plan) = &source_plan {
                    let end=intent.total.min(start+reader::WINDOW);
                    let plan=self.plan_read(intent.namespace,&item.path,Some(intent.operation),Some(&plan.lease),Some(start..end),None,until()).await?;
                    (end,reader::read(pool.clone(),self.clone(),plan,2)?)
                } else {
                    let stripe=u64::from(intent.layout.stripe_bytes);
                    let seq=(start/intent.span()) as u32;
                    let rel=start%intent.span();
                    let shard=(rel/stripe%u64::from(intent.layout.data_shards)) as u16;
                    let offset=rel/(stripe*u64::from(intent.layout.data_shards))*stripe+rel%stripe;
                    let len=(intent.total-start).min(stripe-rel%stripe).min(reader::PART);
                    let dest=backup.destination(intent.operation,seq,shard,offset,len,None).await?;
                    (start+len,pool.read(dest,backup.clone()).await?)
                };
                let mut body=paced(body,rate);
                while let Some(bytes)=body.try_next().await? {
                    hash.lock().unwrap().update(&bytes);yield bytes;
                }
                start=end;
            }
        }.boxed();
        writer::write(self.as_ref(), pool, target, body, None).await?;
        let expected = digest.into_inner().unwrap().finalize();
        let mut copies = vec![blake3::Hasher::new(); target.layout.width()];
        for seq in 0..target.sequences() as u32 {
            let mappings = self
                .payloads
                .write_window(target.operation, seq, 1, until())
                .await?;
            ensure!(
                mappings.len() == copies.len(),
                "incomplete rebuild readback"
            );
            for (i, m) in mappings.iter().enumerate() {
                ensure!(
                    m.receipt.is_some() && m.header.copy as usize == i && m.header.shard == 0,
                    "rebuild fragment has no verified receipt"
                );
                self.check_fragment(pool, m, &mut copies[i], 0).await?;
            }
        }
        ensure!(
            copies.iter().all(|h| h.finalize() == expected),
            "rebuild readback differs from the source"
        );
        #[cfg(feature = "fault-injection")]
        crate::faults::point("storage-rebuild-verified").await;
        self.clusters
            .finish_run_object(item, Some(target.operation))
            .await
            .context("storage rebuild publication changed")
    }
}

struct RebuildReader {
    control: Arc<Control>,
    job: uuid::Uuid,
    token: uuid::Uuid,
    source: crate::model::storage_payload::Intent,
}

impl RebuildReader {
    async fn destination(
        &self,
        op: uuid::Uuid,
        seq: u32,
        shard: u16,
        offset: u64,
        length: u64,
        previous: Option<&crate::model::storage_transfer::SignedGrant>,
    ) -> Result<crate::model::storage_transfer::Destination> {
        use crate::model::storage_transfer::Action;
        let (media, receipt) = self
            .control
            .clusters
            .rebuild_source(self.job, self.token, op, seq, shard)
            .await?;
        self.source.accepts(&receipt.header)?;
        let snapshot = self.control.snapshot(receipt.header.cluster).await?;
        let target = snapshot
            .targets
            .iter()
            .find(|t| t.device.media_id == media)
            .context("rebuild backup member is offline")?;
        ensure!(
            snapshot
                .cluster
                .manifest
                .members
                .iter()
                .any(|m| m.media_id == media
                    && m.topology.purpose == "backup"
                    && m.state == "available"),
            "rebuild backup member is unavailable"
        );
        if let Some(old) = previous {
            old.verify(&snapshot.cluster.manifest)?;
        }
        self.control.sign(
            &snapshot,
            target,
            Action::BackupRead {
                receipt,
                offset,
                length,
            },
            previous.map(|s| &s.grant),
        )
    }
}

#[async_trait::async_trait]
impl Renewal for RebuildReader {
    async fn renew(
        &self,
        signed: &crate::model::storage_transfer::SignedGrant,
    ) -> Result<crate::model::storage_transfer::SignedGrant> {
        let crate::model::storage_transfer::Action::BackupRead {
            receipt,
            offset,
            length,
        } = &signed.grant.action
        else {
            anyhow::bail!("backup rebuild cannot renew another action");
        };
        let h = &receipt.header;
        Ok(self
            .destination(
                h.operation,
                h.sequence,
                h.shard,
                *offset,
                *length,
                Some(signed),
            )
            .await?
            .authorization)
    }
}
