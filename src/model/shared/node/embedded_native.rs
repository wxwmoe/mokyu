use super::{
    Backend,
    queries::{self, native as q, node_registry as registry},
    transactions,
};
use crate::model::{
    Error, Id,
    cluster::RemoteParts,
    multipart::{RemoteCommit, RemoteOutput, RemotePart, RemoteUpload},
    raw::Processor,
};
use anyhow::{Result, ensure};
use uuid::Uuid;
async fn primary(tx: &mut registry::Tx<'_>, who: Processor<'_>) -> Result<Uuid> {
    let (state, role, key) = who.ok_or(Error::NodeAuthentication)?;
    ensure!(
        role == "control" && state.role == role,
        Error::NodeAuthentication
    );
    super::node_registry::authenticated(tx, state, key).await?;
    Ok(state.node_id)
}
#[async_trait::async_trait]
impl RemoteParts for Backend {
    async fn remote_upload(&self, upload: Uuid) -> Result<Option<RemoteUpload>> {
        let mut tx = registry::begin(self).await?;
        Ok(q::upload(upload).fetch_optional(&mut *tx).await?)
    }
    async fn remote_source_id(&self, upload: Uuid, stream: Id, token: Uuid) -> Result<Id> {
        let mut tx = registry::begin(self).await?;
        ensure!(
            q::completion_owner(upload, token)
                .fetch_one(&mut *tx)
                .await?,
            Error::OperationAborted
        );
        let info = q::upload(upload).fetch_one(&mut *tx).await?;
        let id = super::native_parts::candidate(&mut tx, stream, &info).await?;
        tx.commit().await?;
        Ok(id)
    }
    async fn begin_remote(
        &self,
        upload: Uuid,
        storage: Uuid,
        revision: i64,
        who: Processor<'_>,
    ) -> Result<RemoteUpload> {
        let mut tx = registry::begin(self).await?;
        primary(&mut tx, who).await?;
        let parent = queries::lock_upload(upload).fetch_one(&mut *tx).await?;
        ensure!(parent.state == "active", Error::OperationAborted);
        let profile = queries::storage_profile(storage)
            .fetch_one(&mut *tx)
            .await?;
        let target = queries::work::storage(storage)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::SourceUnavailable)?;
        ensure!(
            !profile.cdc_enabled
                && !profile.compression_enabled
                && !profile.pack_enabled
                && target.block_key_id.is_none()
                && profile.revision == revision,
            Error::OperationAborted
        );
        q::open(upload, storage, revision, Uuid::now_v7())
            .execute(&mut *tx)
            .await?;
        let info = q::upload(upload).fetch_one(&mut *tx).await?;
        ensure!(info.storage == storage, Error::OperationAborted);
        tx.commit().await?;
        Ok(info)
    }
    async fn claim_remote(&self, upload: Uuid, token: Uuid, who: Processor<'_>) -> Result<bool> {
        let mut tx = registry::begin(self).await?;
        primary(&mut tx, who).await?;
        let changed = q::claim_init(upload, token)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1;
        tx.commit().await?;
        Ok(changed)
    }
    async fn initialized_remote(
        &self,
        upload: Uuid,
        token: Uuid,
        upstream: Option<&str>,
        who: Processor<'_>,
    ) -> Result<bool> {
        ensure!(
            upstream.is_none_or(|id| !id.is_empty() && id.len() <= 4096),
            Error::NodeOperationConflict
        );
        let mut tx = registry::begin(self).await?;
        primary(&mut tx, who).await?;
        let changed = q::set_upstream(upload, token, upstream)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1;
        tx.commit().await?;
        Ok(changed)
    }
    async fn commit_remote_part(&self, input: RemoteCommit<'_>) -> Result<()> {
        let mut tx = registry::begin(self).await?;
        let owner = primary(&mut tx, input.node).await?;
        let s = queries::quota_stream(input.stream)
            .fetch_one(&mut *tx)
            .await?;
        transactions::commit_upload_part_in(
            &mut tx,
            input.authority,
            input.bucket,
            input.upload,
            input.number,
            input.stream,
        )
        .await?;
        // Embedded parts may use several scratch fragments. The local transfer path replays the protected stream.
        q::queue(
            input.upload,
            input.number,
            input.stream,
            None,
            owner,
            s.size,
            input.cached,
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
    async fn claim_remote_part(
        &self,
        token: Uuid,
        who: Processor<'_>,
        stream: Option<Id>,
    ) -> Result<Option<RemotePart>> {
        let mut tx = registry::begin(self).await?;
        let owner = primary(&mut tx, who).await?;
        let Some((upload, number, stream, _, _, _)) = q::due(&mut tx, owner, stream).await? else {
            return Ok(None);
        };
        let parent = q::upload(upload).fetch_one(&mut *tx).await?;
        ensure!(parent.upstream.is_some(), Error::OperationAborted);
        let version = queries::quota_stream(stream).fetch_one(&mut *tx).await?;
        ensure!(
            q::claim_part(upload, number, stream, token)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        q::pin(stream, token).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some(RemotePart {
            upload: parent,
            stream: version,
            part_number: number,
            remote_token: token,
            read_lease: None,
        }))
    }
    async fn renew_remote_part(&self, part: &RemotePart, who: Processor<'_>) -> Result<bool> {
        let mut tx = registry::begin(self).await?;
        primary(&mut tx, who).await?;
        let changed = q::charge_part(part.remote_token, true)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1;
        ensure!(
            !changed
                || q::touch_pin(part.remote_token, true)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
            Error::OperationAborted
        );
        tx.commit().await?;
        Ok(changed)
    }
    async fn finish_remote_part(
        &self,
        part: &RemotePart,
        etag: &str,
        who: Processor<'_>,
    ) -> Result<bool> {
        ensure!(
            !etag.is_empty() && etag.len() <= 1024,
            Error::NodeOperationConflict
        );
        let mut tx = registry::begin(self).await?;
        primary(&mut tx, who).await?;
        let changed = q::set_part(
            part.upload.upload,
            part.part_number,
            part.stream.id,
            part.remote_token,
            Some(etag),
            None,
        )
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;
        q::unpin(part.remote_token).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(changed)
    }
    async fn retry_remote_part(&self, part: &RemotePart, error: &str) -> Result<()> {
        let mut tx = registry::begin(self).await?;
        q::set_part(
            part.upload.upload,
            part.part_number,
            part.stream.id,
            part.remote_token,
            None,
            Some(error),
        )
        .execute(&mut *tx)
        .await?;
        q::unpin(part.remote_token).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn remote_parts(
        &self,
        upload: Uuid,
        streams: &[Id],
    ) -> Result<Vec<(Id, i32, Option<String>)>> {
        let mut tx = registry::begin(self).await?;
        Ok(q::parts(upload, streams).fetch_all(&mut *tx).await?)
    }
    async fn freeze_remote(&self, upload: Uuid, token: Uuid, hash: &str) -> Result<RemoteUpload> {
        let mut tx = registry::begin(self).await?;
        ensure!(
            q::completion_owner(upload, token)
                .fetch_one(&mut *tx)
                .await?,
            Error::OperationAborted
        );
        let mut item = q::upload(upload).fetch_one(&mut *tx).await?;
        ensure!(
            item.manifest.as_deref().is_none_or(|s| s == hash),
            Error::NodeOperationConflict
        );
        q::manifest(upload, hash).execute(&mut *tx).await?;
        item.manifest = Some(hash.to_owned());
        tx.commit().await?;
        Ok(item)
    }
    async fn resend_remote(&self, upload: Uuid, token: Uuid, streams: &[Id]) -> Result<()> {
        let mut tx = registry::begin(self).await?;
        ensure!(
            q::completion_owner(upload, token)
                .fetch_one(&mut *tx)
                .await?,
            Error::OperationAborted
        );
        q::resend(upload, streams).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn adopt_remote(&self, input: RemoteOutput<'_>) -> Result<Id> {
        let mut tx = registry::begin(self).await?;
        ensure!(
            q::completion_owner(input.upload, input.token)
                .fetch_one(&mut *tx)
                .await?,
            Error::OperationAborted
        );
        let source = q::raw(input.upload)
            .fetch_one(&mut *tx)
            .await?
            .ok_or(Error::SourceUnavailable)?;
        ensure!(
            queries::raw_prepare(input.stream)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        super::native_parts::attach(&mut tx, input.stream, source, input.size, input.digest)
            .await?;
        if input.background {
            queries::raw_enqueue_processing(input.stream, source)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(source)
    }
    async fn remote_charges(&self, node: Uuid, after: Id) -> Result<Vec<(Id, Uuid, i64)>> {
        let mut tx = registry::begin(self).await?;
        Ok(q::charges(node, after).fetch_all(&mut *tx).await?)
    }
    async fn claim_remote_cleanup(
        &self,
        token: Uuid,
        who: Processor<'_>,
    ) -> Result<Option<RemoteUpload>> {
        let mut tx = registry::begin(self).await?;
        primary(&mut tx, who).await?;
        let Some(id) = q::abort_due().fetch_optional(&mut *tx).await? else {
            return Ok(None);
        };
        if q::claim_abort(id, token)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            != 1
        {
            return Ok(None);
        }
        let item = q::upload(id).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some(item))
    }
    async fn clean_remote(&self, upload: Uuid, token: Uuid) -> Result<()> {
        let mut tx = registry::begin(self).await?;
        q::set_upstream(upload, token, None)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}
