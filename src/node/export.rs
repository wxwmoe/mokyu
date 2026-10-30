use super::*;
use crate::model::replication::external::Plan;
use crate::storage::external::objects::{Object, Source, WINDOW};

struct Export<'a> {
    writes: &'a Writes,
    operation: Uuid,
    plan: &'a Plan,
    memory: Arc<OwnedSemaphorePermit>,
}

#[async_trait::async_trait]
impl Source for Export<'_> {
    async fn read(&self, key: &str, object: &Object, start: u64, size: usize) -> Result<Bytes> {
        ensure!(
            self.plan.key.as_deref() == Some(key)
                && self
                    .plan
                    .native
                    .as_ref()
                    .is_some_and(|native| &native.object == object)
                && size > 0
                && size <= WINDOW
                && start <= object.size
                && size as u64 <= object.size - start,
            "invalid native export range"
        );
        let reads = &self.writes.reads;
        let node = &reads.node;
        let mut bytes = Vec::with_capacity(size);
        while bytes.len() < size {
            let position = start + bytes.len() as u64;
            let snapshot = node.background_snapshot()?;
            let held = reads.cache.snapshot();
            let started = Instant::now();
            let result = node
                .client
                .read_transfer(wire::ReadTransferRequest {
                    run: Some(snapshot.lease.run.clone()),
                    operation_id: self.operation.to_string(),
                    read_id: Uuid::now_v7().to_string(),
                    offset: position,
                    length: (size - bytes.len()) as u32,
                    cached: reads::cached(&held),
                })
                .await?;
            ensure!(
                result.raw_json.len() <= 16 * 1024,
                "transfer proof exceeds limit"
            );
            let raw: Option<crate::model::replication::external::RawRead> =
                if result.raw_json.is_empty() {
                    None
                } else {
                    Some(serde_json::from_slice(&result.raw_json)?)
                };
            let reply = result.read.context("native export reply missing")?;
            ensure!(
                reply.outcome == wire::ReadOutcome::Ready as i32
                    && reply.offset == position
                    && reply.length > 0
                    && reply.length <= (size - bytes.len()) as u64
                    && raw
                        .as_ref()
                        .map_or(
                            reply.length == (size - bytes.len()) as u64,
                            |raw| u64::from(raw.length) == reply.length
                        ),
                "native export window differs"
            );
            let source = reads.internal(
                snapshot.lease.clone(),
                self.memory.clone(),
                reply,
                held,
                started,
                Arc::new(Semaphore::new(super::super::body::BUFFER_UNITS)),
            )?;
            let selected: read::Object = source.material(
                source
                    .reply
                    .object
                    .as_ref()
                    .context("native source object missing")?,
            )?;
            ensure!(
                self.plan
                    .native
                    .as_ref()
                    .is_some_and(|native| native.stream == selected.id.into()),
                "native source identity differs"
            );
            if let Some(raw) = raw {
                let part = reads.reader.export_range(&source, &raw).await?;
                ensure!(
                    part.len() <= size - bytes.len(),
                    "native export range exceeds size"
                );
                bytes.extend_from_slice(&part);
            } else {
                let before = bytes.len();
                let mut stream = reads.body(source);
                while let Some(part) = stream.try_next().await? {
                    ensure!(
                        part.len() <= size - bytes.len(),
                        "native export range exceeds size"
                    );
                    bytes.extend_from_slice(&part);
                }
                ensure!(bytes.len() > before, "native export range made no progress");
            }
        }
        ensure!(bytes.len() == size, "native export range is incomplete");
        Ok(crate::storage::external::buffered(bytes, &self.memory))
    }
}

impl Writes {
    pub(crate) async fn external_transfer(
        &self,
        operation: Uuid,
        plan: &Plan,
        connections: &[crate::model::external::Snapshot],
    ) -> Result<crate::model::replication::external::Outcome> {
        let memory = self.reads.reader.transfer_memory().await?;
        let _io = if plan.options.source_external {
            Some(self.reads.reader.acquire(&self.reads.reader.io, 1).await?)
        } else {
            None
        };
        let reader = Export {
            writes: self,
            operation,
            plan,
            memory: memory.clone(),
        };
        crate::storage::external::transfer::execute(plan, connections, &memory, Some(&reader)).await
    }
}
