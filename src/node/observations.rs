use super::{NodeContext, Phase};
use crate::{
    app::{compression::Observations, credential_activity::Activity},
    model::CompressionObservation,
    rpc::wire,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;

struct Receivers {
    compression: mpsc::Receiver<CompressionObservation>,
    credentials: mpsc::Receiver<String>,
}

pub(crate) struct Reports {
    pub(crate) compression: Arc<Observations>,
    pub(crate) activity: Arc<Activity>,
    receivers: Mutex<Option<Receivers>>,
}

impl Reports {
    pub(super) fn new() -> Self {
        let (compression, compression_receiver) = Observations::new();
        let (activity, credentials) = Activity::new();
        Self {
            compression,
            activity,
            receivers: Mutex::new(Some(Receivers {
                compression: compression_receiver,
                credentials,
            })),
        }
    }

    pub(super) fn start(&self, node: &Arc<NodeContext>) {
        let Some(mut receiver) = self.receivers.lock().unwrap().take() else {
            return;
        };
        let mut state = node.subscribe();
        let node = Arc::downgrade(node);
        tokio::spawn(async move {
            loop {
                let mut compression = Vec::new();
                let mut credentials = Vec::new();
                tokio::select! {
                    Some(value) = receiver.compression.recv() => compression.push(value),
                    Some(value) = receiver.credentials.recv() => credentials.push(value),
                    change = state.changed() => {
                        if change.is_err() || matches!(state.borrow().phase, Phase::Stopped | Phase::Fenced) { break }
                        continue;
                    }
                }
                // Coalesce observations without delaying the HTTP response or retaining object buffers.
                #[cfg(feature = "fault-injection")]
                crate::faults::point("node-observations-before-batch").await;
                tokio::time::sleep(Duration::from_millis(200)).await;
                while compression.len() < 32 {
                    let Ok(value) = receiver.compression.try_recv() else {
                        break;
                    };
                    compression.push(value);
                }
                while credentials.len() < 32 {
                    let Ok(value) = receiver.credentials.try_recv() else {
                        break;
                    };
                    credentials.push(value);
                }
                let Some(node) = node.upgrade() else { break };
                let result = tokio::time::timeout(Duration::from_secs(2), async {
                    let snapshot = node.snapshot()?;
                    #[cfg(feature = "fault-injection")]
                    crate::faults::point("node-observations-before-report").await;
                    let reply = node
                        .client
                        .report_observations(wire::ObservationRequest {
                            run: Some(snapshot.lease.run.clone()),
                            compression: compression
                                .iter()
                                .map(|value| wire::CompressionObservation {
                                    stream_id: value.stream.to_string(),
                                    version: value.version as u32,
                                    encoding: value.encoding.into(),
                                    size: value.size as u64,
                                })
                                .collect(),
                            credentials: credentials.clone(),
                        })
                        .await?;
                    if reply.compression_written.len() != compression.len() {
                        return Err(tonic::Status::internal("invalid observation reply"));
                    }
                    Ok(reply)
                })
                .await;
                for (index, observation) in compression.into_iter().enumerate() {
                    node.observations.compression.complete(
                        observation,
                        match &result {
                            Ok(Ok(reply)) => Ok(reply.compression_written[index]),
                            _ => Err(()),
                        },
                    );
                }
                for access in credentials {
                    node.observations
                        .activity
                        .complete(&access, matches!(&result, Ok(Ok(_))));
                }
            }
        });
    }
}
