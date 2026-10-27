use super::reads::{Prepared, Reads};
use crate::{
    content::checks::{RawCheck, SKIPPED},
    model::node_read::Source,
    rpc::wire,
};
use anyhow::{Context, Result};
use bytes::Bytes;
use std::sync::Arc;

impl Reads {
    pub(super) fn check_chunk(
        self: &Arc<Self>,
        prepared: Arc<Prepared>,
        index: usize,
        source: &Source,
        bytes: Bytes,
    ) -> Result<()> {
        let chunk = match source {
            Source::Chunk { chunk } | Source::Pack { chunk, .. } => chunk,
            _ => return Ok(()),
        };
        let backend = prepared.backend(source.storage()?.context("source storage missing")?)?;
        let cache_id = super::files::id(source).context("cache identity missing")?;
        match backend.profile.read_integrity.as_str() {
            "background" => {
                let reads = self.clone();
                let request = prepared.clone();
                let evidence = self.checks.chunk(
                    chunk.hash.clone(),
                    bytes,
                    chunk.raw_size as usize + 16,
                    move |valid, bytes| async move {
                        if !valid {
                            reads.reader.files.reject(cache_id).await;
                            reads.report(request, index).await;
                        } else {
                            reads.reader.files.fill(cache_id, bytes, &backend.profile);
                        }
                    },
                );
                prepared.record_check(evidence);
            }
            "off" => {
                prepared.record_check(self.checks.unchecked());
            }
            _ => self.reader.files.fill(cache_id, bytes, &backend.profile),
        }
        Ok(())
    }

    pub(super) fn check_raw(
        self: &Arc<Self>,
        prepared: Arc<Prepared>,
        index: usize,
        source: &crate::model::raw::RawSource,
        complete: bool,
        remote: bool,
    ) -> Result<Option<RawCheck>> {
        if source.pending_upload {
            return Ok(None);
        }
        let backend = prepared.backend(source.storage_id)?;
        match backend.profile.read_integrity.as_str() {
            "background" if complete => {
                let reads = self.clone();
                let request = prepared.clone();
                let cache_id = source.location_id;
                let files = self.reader.files.clone();
                let source = source.clone();
                let candidate = source.clone();
                let audit = self.checks.raw(
                    source.size as u64,
                    source.digest.clone(),
                    async move {
                        if remote { files.begin_raw(&candidate, &backend.profile).await.ok().flatten() } else { None }
                    },
                    move |valid, cache| async move {
                        if !valid {
                            reads.reader.files.reject(cache_id).await;
                            reads.report(request, index).await;
                        } else if let Some(cache) = cache
                            && let Err(error) = reads.reader.files.finish_raw(cache, &source, request).await {
                            tracing::debug!(%error, "node background raw cache publication skipped");
                        }
                    },
                );
                prepared.record_check(audit.evidence.clone());
                Ok(Some(audit))
            }
            "background" => {
                let (evidence, job) = self.checks.begin();
                self.checks.finish(&evidence, SKIPPED);
                prepared.record_check(evidence);
                if let Some(job) = job {
                    let reads = self.clone();
                    tokio::spawn(async move {
                        let _job = job;
                        // A partial raw response has no full-source digest; verification belongs to a worker.
                        reads.report(prepared, index).await;
                    });
                }
                Ok(None)
            }
            "off" => {
                prepared.record_check(self.checks.unchecked());
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    pub(super) async fn report_error(
        &self,
        prepared: Arc<Prepared>,
        index: usize,
        source: &Source,
        error: &anyhow::Error,
    ) {
        let shared = matches!(source, Source::Chunk { .. } | Source::Pack { .. })
            || matches!(source, Source::Raw { source } if !source.pending_upload && source.medium == "shared_s3");
        if shared
            && (error
                .downcast_ref::<crate::content::codec::IntegrityError>()
                .is_some()
                || matches!(
                    error.downcast_ref::<object_store::Error>(),
                    Some(object_store::Error::NotFound { .. })
                ))
        {
            self.report(prepared, index).await;
        }
    }

    async fn report(&self, prepared: Arc<Prepared>, index: usize) {
        let result = prepared
            .guard(&self.node, async {
                let grant = |r#type: wire::GrantType| -> Result<String> {
                    Ok(prepared
                        .grants
                        .iter()
                        .find(|grant| grant.reference.r#type == r#type as i32)
                        .context("read report grant missing")?
                        .reference
                        .id
                        .clone())
                };
                self.node
                    .client
                    .report_read(wire::ReadReportRequest {
                        run: Some(prepared.run.run.clone()),
                        read_id: grant(wire::GrantType::ReadProtection)?,
                        window_id: grant(wire::GrantType::ReadWindow)?,
                        segments: vec![index.try_into()?],
                    })
                    .await?;
                Ok(())
            })
            .await;
        if result.is_err() {
            tracing::debug!("read source verification report was not acknowledged");
        }
    }
}
