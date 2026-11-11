use super::*;
use crate::{
    model::{
        mirroring::{Assignment, Begin, Source},
        node_work::Outcome,
    },
    storage::origin,
};

impl Writes {
    pub(crate) async fn mirror(&self, work: Uuid, plan: &Assignment) -> Result<Outcome> {
        let result = self.mirror_body(work, plan).await;
        match result {
            Ok(size) => Ok(Outcome::Mirrored {
                size: Some(size),
                missing: false,
            }),
            Err(error) => {
                tracing::warn!(work=%work,error=%error,"origin fill failed");
                Ok(Outcome::Mirrored {
                    size: None,
                    missing: crate::app::mirroring::missing(&error),
                })
            }
        }
    }
    async fn mirror_body(&self, work: Uuid, plan: &Assignment) -> Result<u64> {
        let memory = Arc::new(self.reads.reader.acquire(&self.slots, 1).await?);
        let _buffers = self
            .reads
            .reader
            .work_memory(codec::MAX + 1024 * 1024)
            .await?;
        let network = self.reads.reader.io.clone().acquire_owned().await?;
        let mapped = plan.rule.key(&plan.key)?;
        let connection = |source: &Source| match source {
            Source::S3 { external_id, .. } => plan
                .connections
                .iter()
                .find(|s| s.external_id == *external_id),
            _ => None,
        };
        let mut selected = &plan.rule.source;
        let mut opened = origin::open(
            selected,
            &plan.config,
            &mapped,
            connection(selected),
            hyper::Method::GET,
            hyper::HeaderMap::new(),
        )
        .await;
        if opened.as_ref().is_err_and(origin::retry_backup)
            && let Some(backup) = &plan.rule.backup
        {
            selected = backup;
            opened = origin::open(
                selected,
                &plan.config,
                &mapped,
                connection(selected),
                hyper::Method::GET,
                hyper::HeaderMap::new(),
            )
            .await;
        }
        let mut opened = opened?;
        ensure!(
            opened.status == hyper::StatusCode::OK,
            s3s::s3_error!(ServiceUnavailable)
        );
        let length = opened.length;
        let check = Integrity::new(&origin::integrity(&opened.headers), None)?;
        let node = &self.reads.node;
        let snapshot = node.background_snapshot()?;
        let held = self.reads.cache.snapshot();
        let started = Instant::now();
        let write = Uuid::now_v7();
        let reply = node
            .client
            .begin_mirror(wire::BeginMirrorRequest {
                run: Some(snapshot.lease.run.clone()),
                work_id: work.to_string(),
                operation_id: write.to_string(),
                cached: reads::cached(&held),
                input_json: serde_json::to_vec(&Begin {
                    source: connection(selected).map(|s| (s.external_id, s.revision)),
                    metadata: serde_json::to_value(origin::metadata(&opened.headers)?)?,
                    size: length.map(i64::try_from).transpose()?,
                })?,
            })
            .await?;
        let request = Request {
            reply: wire::RequestReply::default(),
            access: String::new(),
            bucket: String::new(),
            query: None,
            run: snapshot.lease.clone(),
            materials: held.clone(),
            memory,
        };
        let mut operation = self.accept(request, reply, held, started, false, MAX_OBJECT as u64)?;
        operation.packing = operation.backend.profile.pack_enabled;
        let body=crate::http::object::blob(async_stream::try_stream! {
            let network=network;
            while let Some(bytes)=opened.body.next().await {yield bytes.map_err(std::io::Error::other)?;}
            drop(network);
        }.boxed());
        let result = operation
            .guard(node, &[], async {
                let (size, etag, sums) = self
                    .write_body(&operation, body, check, None, length, None)
                    .await?;
                #[cfg(feature = "fault-injection")]
                crate::faults::point("mirror-before-publish").await;
                self.publish(&operation, size, &etag, &sums, Default::default())
                    .await?;
                Ok(size)
            })
            .await;
        if result.is_err() {
            let _ = node.client.abort_write(operation.lookup()).await;
        }
        result
    }
}
