use super::*;

impl Writes {
    pub(crate) async fn migrate(&self, write: Uuid, size: u64) -> Result<()> {
        let memory = Arc::new(self.slots.clone().acquire_owned().await?);
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
            "migration write identity differs"
        );
        if reply.phase == wire::WritePhase::WritePublished as i32 {
            return Ok(());
        }
        let request = Request {
            reply: wire::RequestReply::default(),
            access: String::new(),
            bucket: String::new(),
            query: None,
            run: snapshot.lease.clone(),
            materials: held.clone(),
            memory: memory.clone(),
        };
        let mut operation = self.accept(request, reply, held, started, false, size)?;
        operation.packing = operation.backend.profile.pack_enabled;
        let result = operation
            .guard(node, &[], async {
                let held = self.reads.cache.snapshot();
                let started = Instant::now();
                let reply = node
                    .client
                    .read_migration(wire::ReadMigrationRequest {
                        run: Some(snapshot.lease.run.clone()),
                        operation_id: write.to_string(),
                        cached: reads::cached(&held),
                    })
                    .await?;
                let source = self.reads.internal(
                    snapshot.lease.clone(),
                    memory,
                    reply,
                    held,
                    started,
                    Arc::new(Semaphore::new(super::super::body::BUFFER_UNITS)),
                )?;
                ensure!(
                    source.reply.outcome == wire::ReadOutcome::Ready as i32
                        && source.reply.offset == 0
                        && source.reply.length == size,
                    "invalid migration source"
                );
                let object: read::Object = source.material(
                    source
                        .reply
                        .object
                        .as_ref()
                        .context("migration object missing")?,
                )?;
                ensure!(
                    object.size >= 0 && object.size as u64 == size,
                    "migration source length differs"
                );
                let protection = source
                    .grants
                    .iter()
                    .find(|grant| grant.reference.r#type == wire::GrantType::ReadProtection as i32)
                    .context("migration read protection missing")?
                    .clone();
                let body = crate::http::object::blob(
                    self.reads
                        .body(source)
                        .map_err(std::io::Error::other)
                        .boxed(),
                );
                let check = Integrity::new(&hyper::HeaderMap::new(), None)?;
                let (received, _, _) = self
                    .write_body(&operation, body, check, None, Some(size))
                    .await?;
                ensure!(received == size, s3s::s3_error!(IncompleteBody));
                protection.check()?;
                #[cfg(feature = "fault-injection")]
                crate::faults::point("node-migration-before-seal").await;
                self.publish(
                    &operation,
                    size,
                    &object.etag,
                    &object.checksums,
                    None,
                    None,
                )
                .await?;
                #[cfg(feature = "fault-injection")]
                crate::faults::point("node-migration-sealed").await;
                Ok::<_, anyhow::Error>(())
            })
            .await;
        if result.is_err() {
            let _ = node.client.abort_write(operation.lookup()).await;
        }
        result
    }
}
