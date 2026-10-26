use super::NodeContext;
use crate::{model::node_observation::Access, rpc::wire};
use anyhow::{Context, Result};
use std::{sync::Arc, time::Duration};

impl NodeContext {
    pub(crate) async fn report_access(self: &Arc<Self>) -> Result<()> {
        if self.client.identity.role != "data" {
            return std::future::pending().await;
        }
        let mut sequence = 0i64;
        loop {
            sequence = sequence
                .checked_add(1)
                .context("access sequence exhausted")?;
            let pending = Access::new(sequence, self.access.take());
            let result = tokio::time::timeout(Duration::from_secs(2), async {
                let snapshot = self.background_snapshot()?;
                self.client
                    .report_observations(wire::ObservationRequest {
                        run: Some(snapshot.lease.run.clone()),
                        compression: Vec::new(),
                        credentials: Vec::new(),
                        access_json: Some(serde_json::to_vec(&pending)?),
                    })
                    .await?;
                Ok::<_, anyhow::Error>(())
            })
            .await;
            if !matches!(result, Ok(Ok(()))) {
                self.access.lost();
            }
            let seconds = self
                .background_snapshot()
                .ok()
                .and_then(|snapshot| {
                    crate::config::seconds(&snapshot.runtime().metrics.access_flush_interval).ok()
                })
                .unwrap_or(60)
                .max(1);
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(seconds)) => {},
                _ = self.access.wake.notified() => {},
            }
        }
    }
}
