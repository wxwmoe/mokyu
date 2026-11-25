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
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
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
        view: Uuid,
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
        let endpoint = Endpoint::from_shared(url.to_owned())?
            .connect_timeout(Duration::from_secs(8))
            .timeout(Duration::from_secs(60))
            .initial_stream_window_size(1024 * 1024)
            .initial_connection_window_size(4 * 1024 * 1024)
            .tls_config(ClientTlsConfig::new().with_native_roots())?;
        Self::connect(endpoint, credential).await
    }

    pub(super) async fn connect(endpoint: Endpoint, credential: &Path) -> Result<(Identity, Self)> {
        let mut token: MetadataValue<_> =
            format!("Bearer {}", readonly::credential(credential).await?).parse()?;
        token.set_sensitive(true);
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
        let view = identity.view;
        Ok((
            identity,
            Self::Remote {
                view,
                client,
                token,
            },
        ))
    }

    pub fn view(&self) -> Uuid {
        match self {
            Self::Directory(m) => m.identity.view,
            Self::Remote { view, .. } => *view,
        }
    }

    pub async fn layout_history(
        &self,
        media: Uuid,
        namespace: Uuid,
    ) -> Result<BTreeMap<String, Manifest>> {
        let mut history = BTreeMap::new();
        let mut pages = self.pages(media, namespace, "layouts", 0).await?;
        while let Some(page) = pages.try_next().await? {
            let Page::Evidence(page) = page else {
                anyhow::bail!("expected layout history");
            };
            for entry in page.entries {
                if let Some(m) = entry.manifest
                    && !entry.pending
                    && entry.error.is_none()
                {
                    m.validate()?;
                    let digest = m.digest()?;
                    history.insert(digest, m);
                }
            }
        }
        Ok(history)
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
            Self::Remote {
                view,
                client,
                token,
            } => {
                let mut stream = client
                    .clone()
                    .inventory(request(
                        wire::RecoveryScan {
                            medium: medium.to_string(),
                            namespace: namespace.to_string(),
                            scope: scope.to_owned(),
                            prefix: prefix.into(),
                            view: view.to_string(),
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
            Self::Remote { client, token, .. } => client
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
    directory_blocks: AtomicU64,
    remote_blocks: AtomicU64,
    directory_recovery: AtomicU64,
    remote_recovery: AtomicU64,
    failed_reads: AtomicU64,
    reconstructed_windows: AtomicU64,
}

impl Drop for Media {
    fn drop(&mut self) {
        tracing::debug!(target: "mokyu::dump::reads",
            directory_blocks = self.directory_blocks.load(Ordering::Relaxed),
            remote_blocks = self.remote_blocks.load(Ordering::Relaxed),
            directory_recovery = self.directory_recovery.load(Ordering::Relaxed),
            remote_recovery = self.remote_recovery.load(Ordering::Relaxed),
            failed_reads = self.failed_reads.load(Ordering::Relaxed),
            reconstructed_windows = self.reconstructed_windows.load(Ordering::Relaxed), "Verified recovery reads");
    }
}

#[cfg(test)]
#[path = "../../../../tests/storage/offline_stripes.rs"]
mod tests;

impl Media {
    pub(super) async fn window(
        &self,
        intent: &crate::model::storage_payload::Intent,
        candidates: &[super::part::Candidate],
        start: u64,
        end: u64,
    ) -> Result<(u64, BoxStream<'static, Result<Bytes>>)> {
        use crate::content::erasure;
        use futures_util::stream;
        let layout = &intent.layout;
        let sequence = u32::try_from(start / intent.span())?;
        let relative = start % intent.span();
        let (wanted, offset, contiguous) = erasure::locate(layout, relative);
        let stripe = u64::from(layout.stripe_bytes);
        let k = usize::from(layout.data_shards);
        let width = k + usize::from(layout.parity_shards);
        let total = stripe * k as u64;
        let available = (end - start).min(intent.span() - relative);
        let full = k > 1
            && relative % total == 0
            && available >= total
            && stripe <= frozen::MAX_RANGE
            && total <= 4 * 1024 * 1024;
        let length = if full {
            stripe
        } else {
            contiguous.min(available).min(frozen::MAX_RANGE)
        };
        for (receipt, _, _) in candidates {
            intent.accepts(&receipt.header)?;
        }
        let requested = if full {
            0..k
        } else {
            usize::from(wanted)..usize::from(wanted) + 1
        };
        let mut slots: Vec<Option<Vec<u8>>> = vec![None; width];
        let mut failures = Vec::new();
        let fetch = |slot: usize| async move {
            (
                slot,
                self.read(candidates, sequence, slot as u16, offset, length)
                    .await,
            )
        };
        let mut reads = stream::iter(requested.clone())
            .map(fetch)
            .buffer_unordered(k);
        while let Some((slot, result)) = reads.next().await {
            match result {
                Ok(bytes) => slots[slot] = Some(bytes.to_vec()),
                Err(e) => failures.push(e.to_string()),
            }
        }
        if requested.clone().any(|s| slots[s].is_none()) {
            let mut have = slots.iter().flatten().count();
            let others = (0..width).filter(|s| !requested.contains(s));
            let mut reads = stream::iter(others)
                .map(fetch)
                .buffer_unordered((k - have).max(1));
            while have < k
                && let Some((slot, result)) = reads.next().await
            {
                if let Ok(bytes) = result {
                    slots[slot] = Some(bytes.to_vec());
                    have += 1;
                }
            }
            ensure!(
                have >= k && layout.parity_shards > 0,
                "stripe {sequence} has {have} usable independent shards, needs {k}: {}",
                failures.join(", ")
            );
            erasure::codec(layout)?.reconstruct_data(&mut slots)?;
            self.reconstructed_windows.fetch_add(1, Ordering::Relaxed);
        }
        let body = requested
            .map(|slot| Bytes::from(slots[slot].take().expect("verified data slot")))
            .collect::<Vec<_>>();
        Ok((
            length * body.len() as u64,
            stream::iter(body.into_iter().map(Ok)).boxed(),
        ))
    }

    async fn read(
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
                        Ok(bytes) => {
                            use fragment::Area;
                            let count = match (source, receipt.header.area) {
                                (Source::Directory(_), Area::Blocks) => &self.directory_blocks,
                                (Source::Directory(_), Area::Recovery) => &self.directory_recovery,
                                (Source::Remote { .. }, Area::Blocks) => &self.remote_blocks,
                                (Source::Remote { .. }, Area::Recovery) => &self.remote_recovery,
                            };
                            let unit = UNIT as u64;
                            let verified = (offset + length)
                                .div_ceil(unit)
                                .saturating_mul(unit)
                                .min(receipt.header.shard_bytes)
                                - offset / unit * unit;
                            count.fetch_add(verified, Ordering::Relaxed);
                            return Ok(bytes);
                        }
                        Err(error) => {
                            self.failed_reads.fetch_add(1, Ordering::Relaxed);
                            failures.push(format!("{id}: {error}"));
                        }
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
