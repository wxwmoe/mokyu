use crate::app::App;
use anyhow::Result;
use std::{sync::Arc, time::Duration};

pub async fn run(app: Arc<App>) -> Result<()> {
    loop {
        let wake = app.wake_maintenance.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        if app.writable().is_ok() {
            match app.journal.seed_page(128).await {
                Ok(true) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Ok(false) => {}
                Err(err) => tracing::warn!(error = %err, "metadata journal scan failed"),
            }
        }
        tokio::select! {
            _ = wake => (),
            _ = tokio::time::sleep(Duration::from_secs(5)) => (),
        }
    }
}
