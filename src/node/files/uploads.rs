use super::*;
use crate::{model::node_raw::FileState, rpc::wire, storage::cache::UploadCharge};

impl Cache {
    pub(crate) async fn recover_uploads(self: &Arc<Self>, node: &NodeContext) -> Result<()> {
        self.uploads_ready
            .get_or_try_init(|| async {
                self.disk.initialize().await?;
                self.raw_inventory(node, true).await?;
                self.part_inventory(node, true).await?;
                self.recover_requests(node).await
            })
            .await?;
        self.initialize().await?;
        Ok(())
    }

    async fn recover_requests(&self, node: &NodeContext) -> Result<()> {
        // Receivers wait for uploads_ready; only interrupted, unpublished requests are present.
        let mut directories = tokio::fs::read_dir(self.disk.root.join("pending/raw")).await?;
        while let Some(directory) = directories.next_entry().await? {
            if !directory.file_type().await?.is_dir() {
                continue;
            }
            let mut entries = tokio::fs::read_dir(directory.path()).await?;
            let mut batch = Vec::with_capacity(128);
            loop {
                let entry = entries.next_entry().await?;
                if let Some(entry) = &entry {
                    let name = format!(
                        "{}/{}",
                        directory.file_name().to_string_lossy(),
                        entry.file_name().to_string_lossy()
                    );
                    if entry.file_type().await?.is_file()
                        && let Some(id) = name
                            .strip_suffix(".partial")
                            .and_then(crate::storage::parse_chunk_name)
                    {
                        batch.push(id);
                    }
                }
                if !batch.is_empty() && (batch.len() == 128 || entry.is_none()) {
                    let snapshot = node.background_snapshot()?;
                    let reply = node
                        .client
                        .check_raw_files(wire::LocalFilesRequest {
                            run: Some(snapshot.lease.run.clone()),
                            files: batch.iter().map(ToString::to_string).collect(),
                            recover: true,
                        })
                        .await?;
                    let states: Vec<crate::model::node_raw::File> =
                        serde_json::from_slice(&reply.files_json)?;
                    ensure!(
                        states.len() == batch.len()
                            && states
                                .iter()
                                .map(|file| file.id)
                                .collect::<HashSet<_>>()
                                .len()
                                == batch.len()
                            && states.iter().all(|file| batch.contains(&file.id)
                                && matches!(file.state, FileState::Removable | FileState::Cached)),
                        "interrupted request still has protected metadata"
                    );
                    snapshot.lease.check()?;
                    for id in batch.drain(..) {
                        tokio::fs::remove_file(
                            CacheFile::PendingRaw
                                .path(&self.disk.root, id)
                                .with_extension("partial"),
                        )
                        .await?;
                    }
                }
                if entry.is_none() {
                    break;
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn raw_inventory(
        self: &Arc<Self>,
        node: &NodeContext,
        recover: bool,
    ) -> Result<()> {
        let root = self.disk.root.join("pending/raw");
        tokio::fs::create_dir_all(&root).await?;
        ensure!(
            tokio::fs::symlink_metadata(&root).await?.is_dir(),
            "raw directory was replaced"
        );
        let mut directories = tokio::fs::read_dir(root).await?;
        let mut batch = Vec::with_capacity(128);
        while let Some(directory) = directories.next_entry().await? {
            if !directory.file_type().await?.is_dir() {
                continue;
            }
            let mut entries = tokio::fs::read_dir(directory.path()).await?;
            while let Some(entry) = entries.next_entry().await? {
                if !entry.file_type().await?.is_file() {
                    continue;
                }
                let name = format!(
                    "{}/{}",
                    directory.file_name().to_string_lossy(),
                    entry.file_name().to_string_lossy()
                );
                if let Some(id) = crate::storage::parse_chunk_name(&name) {
                    batch.push((id, entry.metadata().await?.len()));
                }
                if batch.len() == 128 {
                    self.reconcile_uploads(node, &batch, recover).await?;
                    batch.clear();
                }
            }
        }
        if !batch.is_empty() {
            self.reconcile_uploads(node, &batch, recover).await?;
        }
        Ok(())
    }

    async fn reconcile_uploads(
        self: &Arc<Self>,
        node: &NodeContext,
        batch: &[(Uuid, u64)],
        recover: bool,
    ) -> Result<()> {
        let snapshot = node.background_snapshot()?;
        let reply = node
            .client
            .check_raw_files(wire::LocalFilesRequest {
                run: Some(snapshot.lease.run.clone()),
                files: batch.iter().map(|(id, _)| id.to_string()).collect(),
                recover,
            })
            .await?;
        let states: Vec<crate::model::node_raw::File> = serde_json::from_slice(&reply.files_json)?;
        ensure!(
            states.len() == batch.len()
                && states
                    .iter()
                    .map(|file| file.id)
                    .collect::<HashSet<_>>()
                    .len()
                    == batch.len(),
            "invalid raw inventory"
        );
        for state in states {
            let size = batch
                .iter()
                .find_map(|(id, size)| (*id == state.id).then_some(*size))
                .context("unexpected raw inventory identity")?;
            let files = self.clone();
            let lease = snapshot.lease.clone();
            tokio::spawn(async move { files.reconcile_upload(state, size, &lease).await })
                .await??;
        }
        Ok(())
    }

    async fn reconcile_upload(
        &self,
        state: crate::model::node_raw::File,
        size: u64,
        lease: &super::super::RunLease,
    ) -> Result<()> {
        let _lock = self.local_lock(state.id).await;
        lease.check()?;
        let path = CacheFile::PendingRaw.path(&self.disk.root, state.id);
        if !tokio::fs::try_exists(&path).await? {
            return Ok(());
        }
        if !self.uploads.lock().unwrap().contains_key(&state.id) {
            let _write = self.writes.lock().await;
            self.disk.account_existing(Area::Cache, size);
            self.fifo
                .lock()
                .await
                .insert(state.id, size, CacheFile::PendingRaw);
            self.uploads.lock().unwrap().insert(
                state.id,
                self.disk
                    .restore_upload(state.storage.unwrap_or_default(), size),
            );
        }
        if matches!(state.state, FileState::Pending | FileState::Protected) {
            return Ok(());
        }
        let policy = crate::storage::target::CachePolicy {
            enabled: matches!(state.state, FileState::Cached) && state.size == size,
            cold: false,
            capacity: state
                .cache_limit
                .as_deref()
                .map(crate::config::cache_bytes)
                .transpose()?
                .or(self.disk.limits[1]),
            max_entries: state
                .cache_entries
                .map(|value| value as usize)
                .unwrap_or(self.entries)
                .min(self.entries),
            upload_cache: false,
            upload_capacity: None,
        };
        if self
            .files()
            .promote(state.id, Some(state.id), None, policy)
            .await?
        {
            self.uploads.lock().unwrap().remove(&state.id);
        }
        Ok(())
    }

    pub(crate) async fn claim_upload(
        self: &Arc<Self>,
        node: &NodeContext,
        profile: &StorageProfile,
        size: u64,
    ) -> Result<Option<UploadCharge>> {
        self.recover_uploads(node).await?;
        if !profile.upload_cache {
            return Ok(None);
        }
        let limit = profile
            .upload_cache_size
            .as_deref()
            .map(crate::config::cache_bytes)
            .transpose()?;
        let charge = if self.uploads.lock().unwrap().len() < self.entries {
            self.disk
                .claim_upload_with_limit(size, profile.storage_id, limit)
        } else {
            Err(anyhow::anyhow!("write cache entry limit reached"))
        };
        match charge {
            Ok(charge) => Ok(Some(charge)),
            Err(_) if profile.upload_cache_overflow == "fallback" => Ok(None),
            Err(error) => {
                let mut retry = s3s::s3_error!(SlowDown, "write cache is full");
                retry.set_headers(
                    [
                        (
                            hyper::header::RETRY_AFTER,
                            hyper::header::HeaderValue::from_static("60"),
                        ),
                        (
                            hyper::header::CACHE_CONTROL,
                            hyper::header::HeaderValue::from_static("no-store"),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                );
                Err(anyhow::Error::new(retry).context(error))
            }
        }
    }

    pub(crate) fn hold_part(&self, id: Uuid, charge: UploadCharge) {
        self.uploads.lock().unwrap().insert(id, charge);
    }

    pub(crate) fn release_part(&self, id: Uuid) {
        self.uploads.lock().unwrap().remove(&id);
    }

    pub(crate) async fn publish_upload(
        self: &Arc<Self>,
        mut file: crate::storage::raw_request::File,
        id: Uuid,
        charge: UploadCharge,
        resources: impl Send + 'static,
    ) -> Result<()> {
        let files = self.clone();
        // Complete durable publication and accounting even if the request disappears.
        tokio::spawn(async move {
            let _resources = resources;
            let _lock = files.local_lock(id).await;
            ensure!(
                !files.uploads.lock().unwrap().contains_key(&id),
                "raw upload already exists"
            );
            file.publish_in(&files.disk.root, id, false).await?;
            files
                .fifo
                .lock()
                .await
                .insert(id, charge.size, CacheFile::PendingRaw);
            files.uploads.lock().unwrap().insert(id, charge);
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }
}
