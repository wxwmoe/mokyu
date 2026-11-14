use super::transport::{Pool, Renewal};
use crate::{
    model::{
        storage_native::Fill,
        storage_transfer::{Action, Completion, Destination, SignedGrant},
    },
    rpc::{client::Client, wire},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub work: Uuid,
    pub action: Operation,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Operation {
    Window,
    Checked { damaged: bool },
    Accept { receipts: Vec<Completion> },
    Renew { grant: SignedGrant },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Reply {
    Check {
        target: Destination,
    },
    Window {
        sources: Vec<Destination>,
        targets: Vec<Destination>,
        #[serde(default)]
        rate: u64,
    },
    Accepted,
    Renewed {
        grant: SignedGrant,
    },
}

pub(crate) enum Worker {
    Remote {
        client: Arc<Client>,
        run: wire::Run,
        work: Uuid,
    },
    Local {
        control: Arc<super::control::Control>,
        plan: Fill,
    },
}

impl Worker {
    async fn request(&self, action: Operation) -> Result<Reply> {
        match self {
            Self::Local { control, plan } => control.replicate(plan, action, i64::MAX).await,
            Self::Remote { client, run, work } => {
                let reply = client
                    .storage_replicate(wire::NativeMaintenanceRequest {
                        run: Some(run.clone()),
                        input_json: serde_json::to_vec(&Request {
                            work: *work,
                            action,
                        })?,
                    })
                    .await?;
                Ok(serde_json::from_slice(&reply.result_json)?)
            }
        }
    }

    pub async fn execute(self: Arc<Self>, pool: &Pool, plan: &Fill) -> Result<()> {
        let reply = self.request(Operation::Window).await?;
        let reply = if let Reply::Check { target } = reply {
            let Action::Read {
                receipt,
                offset: 0,
                length,
            } = &target.authorization.grant.action
            else {
                anyhow::bail!("repair check is not a full read");
            };
            ensure!(
                *length == receipt.header.shard_bytes
                    && receipt.header.operation == plan.operation
                    && receipt.header.namespace == plan.storage
                    && receipt.header.sequence == plan.sequence
                    && Some(receipt.header.fragment) == plan.repair,
                "repair check differs from its task"
            );
            let expected = receipt.header.digest;
            let checked: Result<()> = async {
                use futures_util::TryStreamExt;
                let mut body = pool.read(target, self.clone()).await?;
                let mut hash = blake3::Hasher::new();
                while let Some(bytes) = body.try_next().await? {
                    hash.update(&bytes);
                }
                ensure!(
                    hash.finalize().as_bytes() == &expected,
                    tonic::Status::data_loss("fragment checksum mismatch")
                );
                Ok(())
            }
            .await;
            let damaged = match checked {
                Ok(()) => false,
                Err(error)
                    if error.downcast_ref::<tonic::Status>().is_some_and(|status| {
                        matches!(status.code(), tonic::Code::NotFound | tonic::Code::DataLoss)
                    }) =>
                {
                    true
                }
                Err(error) => return Err(error),
            };
            self.request(Operation::Checked { damaged }).await?
        } else {
            reply
        };
        let Reply::Window {
            sources,
            targets,
            rate,
        } = reply
        else {
            anyhow::bail!("native replication window missing");
        };
        ensure!(
            sources.len() <= crate::model::storage_cluster::MEMBER_LIMIT
                && targets.len() <= crate::model::storage_cluster::MEMBER_LIMIT,
            "native replication window exceeds its limit"
        );
        for target in targets {
            let Some(header) = target.authorization.grant.action.write_header() else {
                anyhow::bail!("native replication destination is not writable");
            };
            ensure!(
                header.operation == plan.operation
                    && header.namespace == plan.storage
                    && header.sequence == plan.sequence,
                "native replication target differs from its plan"
            );
            let mut completed = None;
            let mut failure = anyhow::anyhow!("native replication has no readable source");
            for source in &sources {
                let Action::Read {
                    receipt,
                    offset,
                    length,
                } = &source.authorization.grant.action
                else {
                    anyhow::bail!("native replication source is not readable");
                };
                ensure!(
                    receipt.header.operation == plan.operation
                        && receipt.header.namespace == plan.storage
                        && receipt.header.sequence == plan.sequence
                        && *offset == 0
                        && receipt.header.layout == header.layout
                        && *length == receipt.header.shard_bytes,
                    "native replication source differs from its target"
                );
                if receipt.header.shard != header.shard {
                    continue;
                }
                let result = async {
                    let body = pool.read(source.clone(), self.clone()).await?;
                    pool.write(target.clone(), paced(body, rate), self.as_ref())
                        .await
                }
                .await;
                match result {
                    Ok(value) => {
                        completed = Some(value);
                        break;
                    }
                    Err(error) => failure = error,
                }
            }
            if completed.is_none() && header.layout.parity_shards > 0 && plan.backup.is_none() {
                let mut distinct = std::collections::HashSet::new();
                let mut streams = Vec::new();
                for source in &sources {
                    let (receipt, _, _) = source
                        .authorization
                        .grant
                        .action
                        .read_range()
                        .context("read grant required")?;
                    let shard = receipt.header.shard;
                    if shard == header.shard || distinct.contains(&shard) {
                        continue;
                    }
                    if let Ok(body) = pool.read(source.clone(), self.clone()).await {
                        streams.push((usize::from(shard), body, bytes::Bytes::new()));
                        distinct.insert(shard);
                    }
                }
                ensure!(
                    streams.len() >= usize::from(header.layout.data_shards),
                    "too few independent repair sources"
                );
                let size = header.shard_bytes;
                let code = crate::content::erasure::codec(&header.layout)?;
                let count = usize::from(header.layout.data_shards + header.layout.parity_shards);
                let wanted = usize::from(header.shard);
                let data = usize::from(header.layout.data_shards);
                let body = async_stream::try_stream! {
                    use futures_util::TryStreamExt;
                    let mut left=size;
                    while left>0 {
                        let len=left.min(crate::content::fragment::UNIT as u64) as usize;
                        let mut shards=vec![None;count];
                        // Keep all open source streams at the same physical offset; a failed one stays excluded.
                        let next=futures_util::future::join_all(streams.iter_mut().map(|(index,body,pending)| async move {
                            let mut out=Vec::with_capacity(len);
                            while out.len()<len {
                                if pending.is_empty() { *pending=body.try_next().await?.context("truncated repair source")?; }
                                out.extend_from_slice(&pending.split_to((len-out.len()).min(pending.len())));
                            }
                            Ok::<_,anyhow::Error>((*index,out))
                        })).await;
                        let mut live=next.iter().map(Result::is_ok);
                        streams.retain(|_|live.next().unwrap());
                        for (index,buf) in next.into_iter().flatten() { shards[index]=Some(buf); }
                        (streams.len()>=data).then_some(()).context("repair lost too many fragments")?;
                        code.reconstruct(&mut shards)?;
                        yield bytes::Bytes::from(shards[wanted].take().context("missing repaired shard")?);
                        left-=len as u64;
                    }
                    for (_,mut body,pending) in streams {
                        (pending.is_empty() && body.try_next().await?.is_none()).then_some(()).context("invalid repair source length")?;
                    }
                };
                completed = Some(
                    pool.write(target.clone(), paced(Box::pin(body), rate), self.as_ref())
                        .await?,
                );
            }
            let receipt = completed.ok_or(failure)?;
            #[cfg(feature = "fault-injection")]
            if plan.repair.is_some() {
                crate::faults::point("storage-repair-written").await;
            }
            ensure!(
                matches!(
                    self.request(Operation::Accept {
                        receipts: vec![receipt]
                    })
                    .await?,
                    Reply::Accepted
                ),
                "native replication receipt was not accepted"
            );
            #[cfg(feature = "fault-injection")]
            if plan.repair.is_some() {
                crate::faults::point("storage-repair-installed").await;
            }
        }
        Ok(())
    }
}

pub(super) fn paced(
    body: futures_util::stream::BoxStream<'static, Result<bytes::Bytes>>,
    rate: u64,
) -> futures_util::stream::BoxStream<'static, Result<bytes::Bytes>> {
    use futures_util::{StreamExt, TryStreamExt};
    if rate == 0 {
        return body;
    }
    let started = tokio::time::Instant::now();
    async_stream::try_stream!{
        let mut source=body;
        let mut sent=0u64;
        while let Some(bytes)=source.try_next().await? {
            sent+=bytes.len() as u64;
            tokio::time::sleep_until(started+std::time::Duration::from_secs_f64(sent as f64/rate as f64)).await;
            yield bytes;
        }
    }.boxed()
}

#[async_trait::async_trait]
impl Renewal for Worker {
    async fn renew(&self, grant: &SignedGrant) -> Result<SignedGrant> {
        let Reply::Renewed { grant: next } = self
            .request(Operation::Renew {
                grant: grant.clone(),
            })
            .await?
        else {
            anyhow::bail!("native replication renewal missing");
        };
        grant.grant.renewal(&next.grant)?;
        Ok(next)
    }
}
