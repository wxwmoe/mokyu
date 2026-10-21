use crate::{
    app::{App, StoredStream},
    authorization::Permit,
    model::{
        Id,
        multipart::{RemoteOutput, RemoteUpload},
    },
    upload::{
        Integrity,
        multipart::{Upload, completion_checksums},
    },
    worker::raw::processor,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::StreamExt;
use md5::{Digest, Md5};
use s3s::dto::{CompleteMultipartUploadInput, CompleteMultipartUploadOutput};
use std::time::Duration;
use uuid::Uuid;

impl App {
    pub(crate) async fn remote_multipart(
        &self,
        upload: Uuid,
        storage: Uuid,
    ) -> Result<Option<RemoteUpload>> {
        if self.role == "single" {
            return Ok(None);
        }
        let cluster = self.cluster()?;
        let mut remote = if let Some(remote) = cluster.remote_upload(upload).await? {
            remote
        } else if let Some(revision) = self.storage.raw_policy(storage)? {
            cluster
                .begin_remote(upload, storage, revision, processor(self))
                .await?
        } else {
            return Ok(None);
        };
        ensure!(remote.state == "active", s3s::s3_error!(NoSuchUpload));
        ensure!(
            remote.manifest.is_none(),
            s3s::s3_error!(InvalidRequest, "multipart finalization has already started")
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(125);
        while remote.upstream.is_none() {
            let token = Uuid::new_v4();
            if cluster.claim_remote(upload, token, processor(self)).await? {
                let created = self.storage.create_remote(&remote).await;
                match created {
                    Ok(id) => {
                        if cluster
                            .initialized_remote(upload, token, Some(&id), processor(self))
                            .await?
                        {
                            remote.upstream = Some(id)
                        } else {
                            self.storage.abort_remote(&remote, &id).await?;
                            return Err(crate::model::Error::OperationAborted.into());
                        }
                    }
                    Err(error) => {
                        let _ = cluster
                            .initialized_remote(upload, token, None, processor(self))
                            .await;
                        return Err(error);
                    }
                }
            } else {
                ensure!(
                    tokio::time::Instant::now() < deadline,
                    crate::app::readiness::pending()
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
                remote = cluster
                    .remote_upload(upload)
                    .await?
                    .ok_or_else(|| s3s::s3_error!(NoSuchUpload))?;
                ensure!(remote.state == "active", s3s::s3_error!(NoSuchUpload));
            }
        }
        Ok(Some(remote))
    }
    pub(crate) async fn transfer_remote_part(&self, stream: Option<Id>) -> Result<bool> {
        if self.role == "single" {
            return Ok(false);
        }
        let Some(work) = self
            .cluster()?
            .claim_remote_part(Uuid::new_v4(), processor(self), stream)
            .await?
        else {
            return Ok(false);
        };
        let _upload = self.pin_upload(work.upload.upload);
        let pin = self.pin_reader(work.stream.id, work.read_lease, 120);
        let sending = async {
            let _slot = self.slots.clone().acquire_owned().await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("remote-part-before-send").await;
            let digest: [u8; 16] = hex::decode(&work.stream.etag)?
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid part checksum"))?;
            let etag = self
                .storage
                .send_remote_part(
                    &work.upload,
                    work.part_number,
                    work.stream.size,
                    &digest,
                    || Box::pin(self.payload_bytes(&work.stream)),
                )
                .await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("remote-part-before-ack").await;
            let _coord = self.coord.lock().await;
            ensure!(
                self.cluster()?
                    .finish_remote_part(&work, &etag, processor(self))
                    .await?,
                crate::model::Error::OperationAborted
            );
            self.storage.release_remote_charge(work.stream.id);
            Ok::<_, anyhow::Error>(())
        };
        let renewing = async {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                self.ensure_mode()?;
                ensure!(
                    self.cluster()?
                        .renew_remote_part(&work, processor(self))
                        .await?,
                    crate::model::Error::OperationAborted
                );
            }
        };
        let result = pin
            .with_read_lease(async {
                tokio::select! {result=sending=>result,result=renewing=>result}
            })
            .await;
        if let Err(error) = result {
            self.cluster()?
                .retry_remote_part(&work, &error.to_string())
                .await?;
            return Err(error);
        }
        Ok(true)
    }
    pub(crate) async fn wait_remote_part(&self, upload: Uuid, stream: Id) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
        loop {
            let rows = self.cluster()?.remote_parts(upload, &[stream]).await?;
            ensure!(rows.len() == 1, crate::model::Error::OperationAborted);
            if rows[0].2.is_some() {
                return Ok(());
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                crate::app::readiness::pending()
            );
            if !self.transfer_remote_part(Some(stream)).await? {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    pub(crate) async fn remote_housekeeping(&self) -> Result<()> {
        if self.role == "single" {
            return Ok(());
        }
        {
            let _coord = self.coord.lock().await;
            self.storage.reconcile_remote_charges().await?;
        }
        self.reclaim_multipart_files(
            None,
            64,
            std::time::Instant::now() + Duration::from_millis(100),
        )
        .await?;
        let token = Uuid::new_v4();
        if let Some(upload) = self
            .cluster()?
            .claim_remote_cleanup(token, processor(self))
            .await?
        {
            if let Some(upstream) = &upload.upstream {
                self.storage.abort_remote(&upload, upstream).await?;
            }
            self.cluster()?.clean_remote(upload.upload, token).await?;
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn assemble_remote(
        &self,
        authority: &Permit,
        u: &Upload,
        parts: &[StoredStream],
        input: &CompleteMultipartUploadInput,
        headers: &hyper::HeaderMap,
        total: i64,
        etag: &str,
        hash: &str,
        token: Uuid,
    ) -> Result<CompleteMultipartUploadOutput> {
        let cluster = self.cluster()?;
        let remote = cluster.freeze_remote(u.id, token, hash).await?;
        let selected: Vec<_> = parts.iter().map(|part| part.id).collect();
        loop {
            let rows = cluster.remote_parts(u.id, &selected).await?;
            ensure!(
                rows.len() == parts.len(),
                crate::model::Error::OperationAborted
            );
            if rows.iter().all(|row| row.2.is_some()) {
                let manifest = rows
                    .into_iter()
                    .map(|(_, number, etag)| (number, etag.unwrap()))
                    .collect::<Vec<_>>();
                #[cfg(feature = "fault-injection")]
                crate::faults::point("remote-before-complete").await;
                if self
                    .storage
                    .complete_remote(&remote, &manifest, total)
                    .await?
                {
                    break;
                }
                // A late UploadPart can overwrite the same upstream number after losing its lease.
                cluster.resend_remote(u.id, token, &selected).await?;
            }
            self.ensure_mode()?;
            self.wake_raw_uploads.notify_one();
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        #[cfg(feature = "fault-injection")]
        crate::faults::point("remote-completed").await;
        let slot = self.slots.clone().acquire_owned().await?;
        let (id, _pin) = self
            .new_stream(
                authority,
                u.bucket_id,
                &u.object_key,
                "object",
                u.metadata.clone(),
                u.public_read,
                true,
                Some(u.id),
                None,
            )
            .await?;
        self.complete_quota(u.id, id, total).await?;
        let mut full = Integrity::new(&hyper::HeaderMap::new(), u.checksum_algorithm.as_deref())?;
        let mut content = blake3::Hasher::new();
        let mut body = self.storage.remote_body(&remote);
        let mut rest = Bytes::new();
        let mut renew = tokio::time::Instant::now();
        for part in parts {
            let mut received = 0i64;
            let mut md5 = Md5::new();
            while received < part.size {
                if rest.is_empty() {
                    rest = body
                        .next()
                        .await
                        .transpose()?
                        .context("completed S3 object is truncated")?;
                }
                let count = rest.len().min((part.size - received) as usize);
                md5.update(&rest[..count]);
                full.update(&rest[..count]);
                content.update(&rest[..count]);
                received += count as i64;
                rest = rest.slice(count..);
                if renew.elapsed() >= Duration::from_secs(3) {
                    self.renew_write_lease(id).await?;
                    renew = tokio::time::Instant::now();
                }
            }
            ensure!(
                hex::encode(md5.finalize()) == part.etag,
                "completed S3 part checksum mismatch"
            );
        }
        ensure!(
            rest.is_empty() && body.next().await.transpose()?.is_none(),
            "completed S3 object exceeds its selected parts"
        );
        drop(body);
        let (digest, sums) = completion_checksums(full, u, parts, headers)?;
        let digest = hex::decode(digest)?;
        let content = *content.finalize().as_bytes();
        let profile = self.storage.profile(remote.storage)?;
        let background = profile.deduplication_enabled && profile.write_processing == "background";
        let source = cluster
            .adopt_remote(RemoteOutput {
                upload: u.id,
                token,
                stream: id,
                size: total,
                digest: &digest,
                hash: &content,
                background,
            })
            .await?;
        if profile.deduplication_enabled && !background {
            self.deduplicate_raw(id, remote.storage, source.into(), Some(content))
                .await?;
        }
        drop(slot);
        self.publish_multipart(id, u, input, total, etag, hash, token, sums)
            .await
    }
}
