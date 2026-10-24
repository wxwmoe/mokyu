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
        match backend.profile.read_integrity.as_str() {
            "background" => {
                let reads = self.clone();
                self.checks.chunk(
                    chunk.hash.clone(),
                    bytes,
                    chunk.raw_size as usize + 16,
                    move |valid, _| async move {
                        if !valid {
                            reads.report(prepared, index).await;
                        }
                    },
                );
            }
            "off" => {
                self.checks.unchecked();
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) fn check_raw(
        self: &Arc<Self>,
        prepared: Arc<Prepared>,
        index: usize,
        source: &crate::model::raw::RawSource,
        complete: bool,
    ) -> Result<Option<RawCheck>> {
        if source.pending_upload {
            return Ok(None);
        }
        let backend = prepared.backend(source.storage_id)?;
        match backend.profile.read_integrity.as_str() {
            "background" if complete => {
                let reads = self.clone();
                Ok(Some(self.checks.raw(
                    source.size as u64,
                    source.digest.clone(),
                    move |valid| async move {
                        if !valid {
                            reads.report(prepared, index).await;
                        }
                    },
                )))
            }
            "background" => {
                let (evidence, job) = self.checks.begin();
                self.checks.finish(&evidence, SKIPPED);
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
                self.checks.unchecked();
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
