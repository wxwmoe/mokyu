use crate::{
    content::fragment::{self, UNIT},
    model::{storage_cluster::Manifest, storage_payload::Receipt},
    node::storage::{
        frozen::{self, Identity, Medium, Page, Scope},
        readonly,
    },
    rpc::wire::{self, storage_recovery_client::StorageRecoveryClient},
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream::BoxStream};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tonic::{
    Request,
    metadata::MetadataValue,
    transport::{Channel, ClientTlsConfig, Endpoint},
};
use uuid::Uuid;

pub(crate) enum Source {
    Directory(Arc<Medium>),
    Remote {
        client: StorageRecoveryClient<Channel>,
        token: MetadataValue<tonic::metadata::Ascii>,
    },
}

impl Source {
    pub async fn directory(path: PathBuf) -> Result<(Identity, Self)> {
        let m = tokio::task::spawn_blocking(move || Medium::open(path)).await??;
        Ok((m.identity.clone(), Self::Directory(m)))
    }

    pub async fn remote(url: &str, credential: &Path) -> Result<(Identity, Self)> {
        let mut token: MetadataValue<_> =
            format!("Bearer {}", readonly::credential(credential).await?).parse()?;
        token.set_sensitive(true);
        let endpoint = Endpoint::from_shared(url.to_owned())?
            .connect_timeout(Duration::from_secs(8))
            .timeout(Duration::from_secs(60))
            .initial_stream_window_size(1024 * 1024)
            .initial_connection_window_size(4 * 1024 * 1024)
            .tls_config(ClientTlsConfig::new().with_native_roots())?;
        let mut client = StorageRecoveryClient::new(endpoint.connect().await?)
            .max_decoding_message_size(8 * 1024 * 1024);
        let answer = client
            .identify(request(wire::RecoveryIdentify {}, &token))
            .await?
            .into_inner();
        let identity: Identity = serde_json::from_slice(&answer.identity_json)?;
        identity.media.validate()?;
        identity.manifest.validate()?;
        ensure!(
            identity
                .manifest
                .members
                .iter()
                .any(|m| m.media_id == identity.media.media_id),
            "remote medium is outside its manifest"
        );
        Ok((identity, Self::Remote { client, token }))
    }

    pub async fn pages(
        &self,
        medium: Uuid,
        namespace: Uuid,
        scope: &str,
        prefix: u16,
    ) -> Result<BoxStream<'static, Result<Page>>> {
        match self {
            Self::Directory(m) => Ok(m.pages(
                namespace,
                match scope {
                    "layouts" => Scope::Layouts,
                    "history" => Scope::History,
                    "recovery" => Scope::Recovery,
                    _ => anyhow::bail!("unknown inventory scope"),
                },
                prefix,
            )),
            Self::Remote { client, token } => {
                let mut stream = client
                    .clone()
                    .inventory(request(
                        wire::RecoveryScan {
                            medium: medium.to_string(),
                            namespace: namespace.to_string(),
                            scope: scope.to_owned(),
                            prefix: prefix.into(),
                        },
                        token,
                    ))
                    .await?
                    .into_inner();
                Ok(async_stream::try_stream! {
                    while let Some(page)=stream.message().await? {yield serde_json::from_slice(&page.result_json)?;}
                }.boxed())
            }
        }
    }

    async fn bytes(
        &self,
        medium: Uuid,
        receipt: &Receipt,
        offset: u64,
        length: u64,
    ) -> Result<Bytes> {
        let reply = match self {
            Self::Directory(m) => {
                let m = m.clone();
                let r = receipt.clone();
                tokio::task::spawn_blocking(move || m.range(&r, offset, length)).await??
            }
            Self::Remote { client, token } => client
                .clone()
                .read(request(
                    wire::RecoveryRead {
                        medium: medium.to_string(),
                        receipt_json: serde_json::to_vec(receipt)?,
                        offset,
                        length,
                    },
                    token,
                ))
                .await?
                .into_inner(),
        };
        let end = offset.checked_add(length).context("range overflow")?;
        let first = offset / UNIT as u64;
        let last = (end - 1) / UNIT as u64;
        ensure!(
            reply.units.len() as u64 == last - first + 1,
            "incomplete recovery range"
        );
        let mut body = Vec::with_capacity(length as usize);
        for (i, unit) in (first..=last).zip(reply.units) {
            let siblings = unit
                .siblings
                .iter()
                .map(|h| h.as_slice().try_into())
                .collect::<Result<Vec<fragment::Digest>, _>>()?;
            fragment::verify_unit(
                &receipt.header,
                i as usize,
                &unit.data,
                &siblings,
                &receipt.root,
            )?;
            let base = i * UNIT as u64;
            body.extend_from_slice(
                &unit.data[offset.saturating_sub(base) as usize
                    ..((end - base) as usize).min(unit.data.len())],
            );
        }
        ensure!(body.len() as u64 == length, "recovery range length differs");
        Ok(body.into())
    }
}

fn request<T>(message: T, token: &MetadataValue<tonic::metadata::Ascii>) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", token.clone());
    request
}

#[derive(Default)]
pub(crate) struct Media {
    pub sources: BTreeMap<Uuid, Vec<(Manifest, Source)>>,
}

impl Media {
    pub(super) async fn read(
        &self,
        candidates: &[super::part::Candidate],
        sequence: u32,
        shard: u16,
        offset: u64,
        length: u64,
    ) -> Result<Bytes> {
        ensure!(
            length > 0 && length <= frozen::MAX_RANGE,
            "invalid offline read window"
        );
        let mut failures = Vec::new();
        for (receipt, id, _) in candidates
            .iter()
            .filter(|(r, _, _)| r.header.sequence == sequence && r.header.shard == shard)
        {
            if let Some(sources) = self.sources.get(id) {
                for (_, source) in sources {
                    match source.bytes(*id, receipt, offset, length).await {
                        Ok(bytes) => return Ok(bytes),
                        Err(error) => failures.push(format!("{id}: {error}")),
                    }
                }
            }
        }
        anyhow::bail!(
            "stripe {sequence} needs shard {shard}; available media could not supply it: {}",
            failures.join(", ")
        )
    }
}
