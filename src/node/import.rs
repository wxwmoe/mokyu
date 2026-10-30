use super::*;
use crate::model::replication::external::{Outcome, Plan};

impl Writes {
    pub(crate) async fn import_part(
        &self,
        plan: &Plan,
        connections: &[crate::model::external::Snapshot],
        buffers: &Arc<OwnedSemaphorePermit>,
    ) -> Result<Outcome> {
        let incoming = plan.incoming.as_ref().context("import write missing")?;
        incoming.validate(plan)?;
        let write = incoming.operation.context("import operation missing")?;
        let memory = Arc::new(self.reads.reader.acquire(&self.slots, 1).await?);
        let node = &self.reads.node;
        let snapshot = node.background_snapshot()?;
        let held = self.reads.cache.snapshot();
        let started = Instant::now();
        let reply = node
            .client
            .write_status(wire::WriteRequest {
                run: Some(snapshot.lease.run.clone()),
                operation_id: write.to_string(),
            })
            .await?;
        ensure!(
            reply.operation_id == write.to_string(),
            "import write identity differs"
        );
        if reply.phase == wire::WritePhase::WritePublished as i32 {
            return Ok(Outcome::Imported);
        }
        let request = Request {
            reply: wire::RequestReply::default(),
            access: String::new(),
            bucket: String::new(),
            query: None,
            run: snapshot.lease.clone(),
            materials: held.clone(),
            memory,
        };
        let mut operation = self.accept(request, reply, held, started, false, incoming.size)?;
        operation.packing = operation.backend.profile.pack_enabled;
        let result = operation
            .guard(node, &[], async {
                let source = crate::storage::external::transfer::import_source(
                    plan,
                    connections,
                    buffers,
                    &crate::storage::external::Reads::Node(self.reads.reader.io.clone()),
                )
                .await?;
                let body = crate::http::object::blob(
                    crate::storage::external::transfer::import_body(
                        source,
                        plan.key.clone().context("import key missing")?,
                        plan.state.source.clone().context("import source missing")?,
                        incoming.offset..incoming.offset + incoming.size,
                    )
                    .map_err(std::io::Error::other)
                    .boxed(),
                );
                let check = Integrity::new(&hyper::HeaderMap::new(), Some("SHA256"))?;
                let (size, etag, sums) = self
                    .write_body(&operation, body, check, None, Some(incoming.size))
                    .await?;
                ensure!(size == incoming.size, s3s::s3_error!(IncompleteBody));
                #[cfg(feature = "fault-injection")]
                crate::faults::point("import-part-written").await;
                self.publish(&operation, size, &etag, &sums, None, None)
                    .await?;
                #[cfg(feature = "fault-injection")]
                crate::faults::point("import-part-sealed").await;
                Ok::<_, anyhow::Error>(Outcome::Imported)
            })
            .await;
        if result.is_err() {
            let _ = node.client.abort_write(operation.lookup()).await;
        }
        match result {
            Err(error)
                if error
                    .downcast_ref::<s3s::S3Error>()
                    .is_some_and(|error| error.code() == &s3s::S3ErrorCode::PreconditionFailed) =>
            {
                Ok(Outcome::Conflict)
            }
            result => result,
        }
    }
}
