use super::transport::{Pool, Renewal};
use crate::{
    model::{
        storage_native::Fill,
        storage_transfer::{Action, Completion, Destination, SignedGrant},
    },
    rpc::{client::Client, wire},
};
use anyhow::{Result, ensure};
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
            sources.len() <= 2 && targets.len() <= 2,
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
                        && *length == header.shard_bytes
                        && receipt.header.shard == header.shard,
                    "native replication source differs from its target"
                );
                let result = async {
                    let body = pool.read(source.clone(), self.clone()).await?;
                    if rate==0 {
                        pool.write(target.clone(),body,self.as_ref()).await
                    } else {
                        use futures_util::{StreamExt,TryStreamExt};
                        let started=tokio::time::Instant::now();
                        let paced=async_stream::try_stream! {
                            let mut source=body;
                            let mut sent=0u64;
                            while let Some(bytes)=source.try_next().await? {
                                sent+=bytes.len() as u64;
                                tokio::time::sleep_until(started+std::time::Duration::from_secs_f64(sent as f64/rate as f64)).await;
                                yield bytes;
                            }
                        };
                        pool.write(target.clone(),paced.boxed(),self.as_ref()).await
                    }
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
