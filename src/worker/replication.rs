use crate::{
    app::App,
    model::{replication::Step, task::Execution},
};
use anyhow::Result;

impl App {
    pub(crate) async fn replication_batch(&self, task: Execution) -> Result<bool> {
        let result = {
            let _coord = self.coord.lock().await;
            self.writable()?;
            let _cache = self.public_reads.changing();
            self.replications.step(task).await?
        };
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
