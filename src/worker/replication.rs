use crate::{
    app::App,
    model::{replication::Step, task::Execution},
};
use anyhow::Result;

struct Export<'a> {
    app: &'a App,
    task: Execution,
    plan: &'a crate::model::replication::external::Plan,
    memory: std::sync::Arc<tokio::sync::OwnedSemaphorePermit>,
}

#[async_trait::async_trait]
impl crate::storage::external::objects::Source for Export<'_> {
    async fn read(
        &self,
        key: &str,
        object: &crate::storage::external::objects::Object,
        start: u64,
        size: usize,
    ) -> Result<bytes::Bytes> {
        use futures_util::StreamExt;
        anyhow::ensure!(
            self.app.role == "single"
                && self.plan.key.as_deref() == Some(key)
                && self
                    .plan
                    .native
                    .as_ref()
                    .is_some_and(|native| &native.object == object)
                && size <= crate::storage::external::objects::WINDOW
                && start <= object.size
                && size as u64 <= object.size - start,
            "invalid native export range"
        );
        let mut bytes = Vec::with_capacity(size);
        while bytes.len() < size {
            let position = start + bytes.len() as u64;
            let (source, raw) = self
                .app
                .replications
                .native_source(self.task, self.plan, position, (size - bytes.len()) as u32)
                .await?;
            let _pin = self.app.pin(source.id);
            if let Some(raw) = raw {
                let mut reader = self
                    .app
                    .storage
                    .raw_reader_uncached(&raw.source, false)
                    .await?;
                let resources = crate::content::transfer::Resources {
                    cpu: self.app.storage.cpu.clone(),
                    memory: self.memory.clone(),
                };
                let part = crate::content::transfer::read_range(&raw, &mut reader.file, &resources)
                    .await?;
                reader
                    .file
                    .account_read(&self.app.storage, u64::from(raw.proof.size));
                anyhow::ensure!(
                    part.len() <= size - bytes.len(),
                    "native export range exceeds size"
                );
                bytes.extend_from_slice(&part);
            } else {
                let mut stream = self
                    .app
                    .checked_range(&source, position..start + size as u64);
                while let Some(part) = stream.next().await {
                    let part = part?;
                    anyhow::ensure!(
                        part.len() <= size - bytes.len(),
                        "native export range exceeds size"
                    );
                    bytes.extend_from_slice(&part);
                }
                break;
            }
        }
        anyhow::ensure!(bytes.len() == size, "native export range is incomplete");
        Ok(crate::storage::external::buffered(bytes, &self.memory))
    }
}

impl App {
    async fn transfer_encoded(
        &self,
        input: &crate::model::transfer::Input,
        memory: &std::sync::Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<Vec<(crate::model::Id, bytes::Bytes)>> {
        use crate::model::transfer::Input;
        let source = input.storage()?;
        let (path, limit, key) = match input {
            Input::Chunk { chunk } => (
                self.storage.path_for(source, chunk.location_id)?,
                crate::content::codec::MAX + 16,
                chunk.block_key_id,
            ),
            Input::Pack { pack, .. } => (
                self.storage.pack_path_for(source, pack.location_id)?,
                pack.payload_size()? + 16,
                pack.block_key_id,
            ),
            Input::Raw { .. } => unreachable!(),
        };
        self.storage.ensure_key_loaded(key).await?;
        let encoded = {
            let _io = self.storage.reads.acquire().await?;
            self.storage.read_path_for(source, &path, limit).await?
        };
        let input = input.clone();
        let secrets = self.secrets.clone();
        let pool = self.storage.compression.clone();
        let cpu = self.storage.cpu.clone().acquire_owned().await?;
        let memory = memory.clone();
        let (data, _memory) = tokio::task::spawn_blocking(move || {
            let _cpu = cpu;
            Ok::<_, anyhow::Error>((
                crate::content::transfer::decode(&input, encoded, &secrets, &pool)?,
                memory,
            ))
        })
        .await??;
        Ok(data)
    }

    pub(crate) async fn copy_transfer(
        &self,
        task: Execution,
        plan: &crate::model::transfer::Copy,
    ) -> Result<(
        Vec<crate::model::packing::PackOutput>,
        crate::storage::cache::Fills,
    )> {
        use crate::model::transfer::Input;
        use anyhow::{Context, ensure};
        plan.validate()?;
        if plan.prepared.targets.iter().all(|target| target.reused) {
            return Ok((Vec::new(), Vec::new()));
        }
        let source = plan.input.storage()?;
        let mut permits = Vec::new();
        for storage in std::collections::BTreeSet::from([source, plan.storage]) {
            permits.push(self.storage.profile_permit(storage, "migration").await?);
        }
        let bytes = match &plan.input {
            Input::Pack { pack, .. } => pack.payload_size()?,
            _ => usize::try_from(plan.prepared.end - plan.prepared.start)?,
        };
        let memory = self.storage.pack_work(bytes).await?;
        let resources = crate::content::transfer::Resources {
            cpu: self.storage.cpu.clone(),
            memory: memory.clone(),
        };
        let data = if let Input::Raw { source } = &plan.input {
            let mut input = self.storage.raw_reader_uncached(source, false).await?;
            crate::content::transfer::read_proofs(plan, &mut input.file, &resources).await?
        } else {
            self.transfer_encoded(&plan.input, &memory).await?
        };
        let data: std::collections::BTreeMap<_, _> = data.into_iter().collect();
        let mut rows = Vec::new();
        let mut selected = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for target in &plan.prepared.targets {
            if target.reused || !seen.insert(target.chunk.id) {
                continue;
            }
            let data = data
                .get(&target.source)
                .context("transfer source data missing")?;
            ensure!(
                data.len() == target.chunk.raw_size as usize,
                "transfer target length differs"
            );
            rows.push(crate::model::Mapping {
                chunk: target.chunk.clone(),
                offset_bytes: i64::try_from(target.offset)?,
                pack_id: None,
            });
            selected.push((target.chunk.clone(), data.clone()));
        }
        self.storage
            .ensure_key_loaded(plan.prepared.policy.block_key_id)
            .await?;
        let mut fills = Vec::new();
        let outputs = self
            .pack_data(
                task,
                &rows,
                &selected,
                &memory,
                None,
                "",
                plan.prepared.policy.pack_enabled,
                Some(&plan.prepared.policy),
                &mut fills,
            )
            .await?;
        Ok((outputs, fills))
    }

    async fn copy_raw_transfer(
        &self,
        task: Execution,
        plan: &crate::model::transfer::RawCopy,
    ) -> Result<()> {
        use crate::model::transfer::Input;
        use crate::storage::{raw::RawFile, transfer::Target};
        use anyhow::{Context, ensure};
        use md5::Digest;
        plan.validate()?;
        let mut permits = Vec::new();
        for storage in
            std::collections::BTreeSet::from([plan.input.storage()?, plan.target.storage_id])
        {
            permits.push(self.storage.profile_permit(storage, "migration").await?);
        }
        let target = self.storage.store_backend(plan.target.storage_id)?;
        self.storage.verify_raw_target(&target).await?;
        let payload = match &plan.input {
            Input::Pack { pack, .. } => pack.payload_size()?,
            Input::Chunk { chunk } => chunk.raw_size as usize,
            Input::Raw { .. } => crate::model::transfer::PROOF_BYTES,
        };
        let memory = self.storage.pack_work(payload).await?;
        let mut file;
        let mut reader;
        let (input, digest) = if let Input::Raw { source } = &plan.input {
            reader = self.storage.raw_reader_uncached(source, false).await?;
            (
                &mut reader.file,
                source
                    .digest
                    .as_deref()
                    .context("raw digest missing")?
                    .try_into()?,
            )
        } else {
            let data = self.transfer_encoded(&plan.input, &memory).await?;
            let hold = memory.clone();
            let cpu = self.storage.cpu.clone().acquire_owned().await?;
            let size = plan.target.size as usize;
            let (bytes, digest, _memory) = tokio::task::spawn_blocking(move || {
                let _cpu = cpu;
                let mut bytes = Vec::with_capacity(size);
                for (_, data) in data {
                    bytes.extend_from_slice(&data);
                }
                ensure!(bytes.len() == size, "raw transfer source length differs");
                let digest: [u8; 16] = md5::Md5::digest(&bytes).into();
                Ok::<_, anyhow::Error>((bytes::Bytes::from(bytes), digest, hold))
            })
            .await??;
            file = RawFile::Memory {
                data: bytes,
                offset: 0,
            };
            (&mut file, digest)
        };
        let _space = self
            .storage
            .transfer_space(&target, plan.target.size as u64, plan.target.location_id)
            .await?;
        let _io = self.storage.writes.acquire().await?;
        let _write = target.writes.acquire().await?;
        let mut fill = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            self.storage.begin_raw_output(
                plan.target.storage_id,
                plan.target.location_id,
                plan.target.size as u64,
            ),
        )
        .await
        .ok()
        .and_then(Result::ok)
        .flatten();
        let destination = if let Some(root) = &target.raw_root {
            Target::Local(root.join(crate::storage::chunk_name(plan.target.location_id)))
        } else {
            Target::Remote {
                backend: target.backend.clone(),
                streaming: target
                    .streaming
                    .as_ref()
                    .context("S3 streaming backend missing")?,
                multipart: target
                    .multipart
                    .as_deref()
                    .context("S3 multipart backend missing")?,
            }
        };
        let copied = crate::storage::transfer::copy(
            destination,
            plan,
            digest,
            input,
            &mut fill,
            |progress| async move {
                let _coord = self.coord.lock().await;
                self.writable()?;
                self.replications.raw_progress(task, plan, &progress).await
            },
        )
        .await?;
        self.storage
            .backend_puts
            .fetch_add(copied.requests, std::sync::atomic::Ordering::Relaxed);
        self.storage
            .backend_write_bytes
            .fetch_add(copied.bytes, std::sync::atomic::Ordering::Relaxed);
        if let Some(fill) = fill {
            let ready = self.storage.prepare_raw_fill(
                fill,
                plan.target.location_id,
                plan.target.size as u64,
                target,
            );
            let _ = ready.send(());
        }
        Ok(())
    }

    pub(crate) async fn replication_batch(&self, task: Execution) -> Result<bool> {
        let result = {
            let _coord = self.coord.lock().await;
            self.writable()?;
            let _cache = self.public_reads.changing();
            self.replications.step(task).await?
        };
        if let Step::External { plan, connections } = &result {
            anyhow::ensure!(self.role == "single", "external transfers require a worker");
            let snapshots = connections
                .iter()
                .map(|connection| connection.open(task.id, &self.secrets.credential_key))
                .collect::<Result<Vec<_>>>()?;
            let outcome = {
                let memory = self
                    .storage
                    .pack_work(crate::storage::external::objects::WINDOW)
                    .await?;
                let _io = if plan.options.source_external {
                    Some(self.storage.reads.acquire().await?)
                } else {
                    None
                };
                let reader = Export {
                    app: self,
                    task,
                    plan,
                    memory: memory.clone(),
                };
                crate::storage::external::transfer::execute(
                    plan,
                    &snapshots,
                    &memory,
                    Some(&reader),
                )
                .await?
            };
            self.replications
                .external_complete(task, plan, &outcome)
                .await?;
        }
        if let Step::Scan(plan) = &result {
            anyhow::ensure!(self.role == "single", "transfer scanning requires a worker");
            let _permit = self
                .storage
                .profile_permit(plan.source.storage_id, "migration")
                .await?;
            let resources = crate::content::transfer::Resources {
                cpu: self.storage.cpu.clone(),
                memory: self
                    .storage
                    .pack_work(crate::model::transfer::PROOF_BYTES)
                    .await?,
            };
            let mut input = self
                .storage
                .raw_reader_uncached(&plan.source, false)
                .await?;
            input
                .file
                .seek(std::io::SeekFrom::Start(plan.checksum.object.offset))
                .await?;
            let scanned = crate::content::transfer::scan(plan, &mut input.file, &resources).await?;
            input
                .file
                .account_read(&self.storage, scanned.end - plan.checksum.object.offset);
            #[cfg(feature = "fault-injection")]
            crate::faults::point("transfer-scanned").await;
            self.replications.scanned(task, &scanned).await?;
        }
        if let Step::Raw(plan) = &result {
            anyhow::ensure!(self.role == "single", "raw transfer requires a worker");
            self.copy_raw_transfer(task, plan).await?;
        }
        if let Step::Copy(plan) = &result {
            anyhow::ensure!(self.role == "single", "transfer copying requires a worker");
            let (outputs, fills) = self.copy_transfer(task, plan).await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("transfer-written").await;
            self.replications.copied(task, plan, &outputs).await?;
            crate::storage::cache::publish_fills(fills);
        }
        if let Step::Verify { storage, sources } = result {
            anyhow::ensure!(self.role == "single", "raw verification requires a worker");
            let _permit = self.storage.profile_permit(storage, "migration").await?;
            for source in &sources {
                drop(self.storage.raw_reader_uncached(source, true).await?);
            }
            #[cfg(feature = "fault-injection")]
            crate::faults::point("replication-verified").await;
            self.replications.verified(task, &sources).await?;
        }
        #[cfg(feature = "fault-injection")]
        crate::faults::point("replication-batch").await;
        self.wake_gc.notify_one();
        Ok(false)
    }
}
