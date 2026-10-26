use super::*;
use crate::model::raw::RawSource;

impl Writes {
    pub(crate) async fn convert(
        &self,
        write: Uuid,
        source: RawSource,
        etag: String,
        checksums: serde_json::Value,
    ) -> Result<()> {
        source.validate()?;
        ensure!(
            (source.medium == "shared_s3" && !source.pending_upload)
                || (source.medium == "local" && source.pending_upload),
            codec::IntegrityError::RawMetadata
        );
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
            "conversion write identity differs"
        );
        if reply.phase == wire::WritePhase::WritePublished as i32 {
            ensure!(
                reply
                    .published
                    .as_ref()
                    .is_some_and(|value| value.size == source.size as u64 && value.etag == etag),
                "conversion receipt differs"
            );
            return Ok(());
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
        let mut operation =
            self.accept(request, reply, held, started, false, source.size as u64)?;
        operation.packing = operation.backend.profile.pack_enabled;
        ensure!(
            operation.backend.profile.storage_id == source.storage_id,
            "conversion storage differs"
        );
        let result = operation
            .guard(node, &[], async {
                let io = self.reads.reader.io.clone().acquire_owned().await?;
                #[cfg(feature = "fault-injection")]
                crate::faults::point("node-conversion-before-read").await;
                let body = if source.pending_upload {
                    let path = crate::storage::cache::CacheFile::Raw
                        .path(&node.config.instance.data_path, source.location_id);
                    crate::http::object::blob(
                        super::native::replay(
                            path,
                            source.size,
                            hex::encode(source.digest.as_ref().context("raw digest missing")?),
                        )
                        .map_err(std::io::Error::other)
                        .boxed(),
                    )
                } else {
                    operation.backend.store.verify().await?;
                    let backend = operation.backend.store.backend.clone();
                    let meta = backend
                        .head(&Path::from(format!(
                            "raw/{}",
                            crate::storage::chunk_name(source.location_id)
                        )))
                        .await?;
                    ensure!(
                        meta.size == source.size as u64,
                        codec::IntegrityError::Length
                    );
                    let value = backend
                        .get_opts(
                            &meta.location,
                            object_store::GetOptions {
                                if_match: meta.e_tag,
                                ..Default::default()
                            },
                        )
                        .await?;
                    ensure!(
                        value.meta.size == source.size as u64,
                        codec::IntegrityError::Length
                    );
                    crate::http::object::blob(
                        value.into_stream().map_err(std::io::Error::other).boxed(),
                    )
                };
                let mut headers = hyper::HeaderMap::new();
                use base64::Engine;
                headers.insert(
                    "content-md5",
                    base64::engine::general_purpose::STANDARD
                        .encode(source.digest.as_ref().context("raw digest missing")?)
                        .parse()?,
                );
                let check = Integrity::new(&headers, None)?;
                let (size, _, _) = self
                    .write_body(&operation, body, check, None, Some(source.size as u64))
                    .await?;
                drop(io);
                #[cfg(feature = "fault-injection")]
                crate::faults::point("node-conversion-before-seal").await;
                self.publish(&operation, size, &etag, &checksums, None, None)
                    .await?;
                #[cfg(feature = "fault-injection")]
                crate::faults::point("node-conversion-sealed").await;
                Ok::<_, anyhow::Error>(())
            })
            .await;
        if result.is_err() {
            let _ = node.client.abort_write(operation.lookup()).await;
        }
        result
    }
}
