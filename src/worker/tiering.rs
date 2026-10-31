use crate::{
    app::App,
    model::{task::Execution, tiering::Work},
};
use anyhow::Result;

impl App {
    pub(crate) async fn tier_batch(&self, task: Execution) -> Result<bool> {
        let stale =
            i64::try_from(crate::config::seconds(&self.config.metrics.access_flush_interval)? * 2)?;
        let gate = self.storage.source_gate.write().await;
        let step = self.tiering.step(task, stale, None).await?;
        if step.switched {
            self.storage.invalidate_reads();
            #[cfg(feature = "fault-injection")]
            crate::faults::point("tier-head-published").await;
        }
        let Some(work) = step.work else {
            return Ok(false);
        };
        if matches!(&work,Work::Remove{source} if self.storage.physical_active(source.address())) {
            self.tiering
                .step(task, stale, Some(work.source().address()))
                .await?;
            return Ok(false);
        }
        drop(gate);
        let outcome = self.storage.tier_work(&work).await.unwrap_or_else(|error| {
            tracing::debug!(task=%task.id,%error,"tier operation postponed");
            crate::model::tiering::Outcome::Retry
        });
        #[cfg(feature = "fault-injection")]
        {
            outcome.boundary().await;
            crate::faults::point("tier-before-receipt").await;
        }
        self.tiering.finish(task, &outcome).await?;
        Ok(false)
    }
}
