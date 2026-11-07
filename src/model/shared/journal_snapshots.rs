use super::{Backend, queries::journal_snapshots as q};
use crate::{
    authorization::Principal,
    model::journal::snapshots::{
        Action, Garbage, Item, Job, Member, Part, Scope, SnapshotStore, Status,
    },
    storage::journal::{
        segment_path,
        snapshot::{Page, manifest_path, page_path},
    },
};
use anyhow::{Context, Result, ensure};
use std::collections::BTreeSet;
use uuid::Uuid;

#[async_trait::async_trait]
impl SnapshotStore for Backend {
    async fn snapshot_status(&self, storage: Uuid) -> Result<Status> {
        let (current, rev, log, deletes, _invalid, gc, pages, entries, overlays) =
            q::status(storage)
                .fetch_optional(q::read(self))
                .await?
                .unwrap_or_default();
        let next = q::first(storage).fetch_optional(q::read(self)).await?;
        let (task, operation, task_state, error) = next
            .map_or((None, None, None, None), |(id, k, s, e)| {
                (Some(id), Some(k), Some(s), e)
            });
        let invalid = q::waste(storage).fetch_one(q::read(self)).await?;
        let error = error.or(q::gc_error(storage)
            .fetch_optional(q::read(self))
            .await?
            .flatten());
        Ok(Status {
            current,
            revision: rev.to_string(),
            pending_log_bytes: log.to_string(),
            delete_log_bytes: deletes.to_string(),
            invalid_bytes: invalid.to_string(),
            gc_bytes: gc.to_string(),
            pages: pages.to_string(),
            source_entries: entries.to_string(),
            snapshot_deletions: overlays.to_string(),
            task,
            operation,
            task_state,
            error,
        })
    }

    async fn snapshot_request(&self, p: &Principal, storage: Uuid, action: Action) -> Result<Uuid> {
        let mut tx = q::admin(self, p).await?;
        q::installed(storage)
            .fetch_optional(&mut *tx)
            .await?
            .context("no confirmed metadata journal on this storage")?;
        let mut id = Uuid::new_v4();
        if let Some(previous) = q::next(storage).fetch_optional(&mut *tx).await?
            && previous.kind == action.name()
        {
            id = previous.id;
            if previous.state == "failed" {
                q::retry(id).execute(&mut *tx).await?;
            }
        } else {
            q::request(id, storage, action.name())
                .execute(&mut *tx)
                .await?;
        }
        q::sync(storage).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(id)
    }
    async fn snapshot_sources(&self) -> Result<Vec<Uuid>> {
        Ok(q::sources().fetch_all(q::read(self)).await?)
    }
    async fn snapshot_claim(&self, storage: Uuid, token: Uuid) -> Result<bool> {
        Ok(q::claim(token, storage)
            .execute(q::write(self))
            .await?
            .rows_affected()
            == 1)
    }
    async fn snapshot_renew(&self, storage: Uuid, token: Uuid) -> Result<bool> {
        Ok(q::renew(storage, token)
            .execute(q::write(self))
            .await?
            .rows_affected()
            == 1)
    }
    async fn snapshot_release(&self, storage: Uuid, token: Uuid) -> Result<()> {
        q::release(storage, token).execute(q::write(self)).await?;
        Ok(())
    }
    async fn snapshot_begin(&self, storage: Uuid, token: Uuid) -> Result<Option<Job>> {
        let mut tx = q::begin(self).await?;
        q::fence(storage, token).fetch_one(&mut *tx).await?;
        let next = q::next(storage).fetch_optional(&mut *tx).await?;
        let job = if let Some(job) = next {
            if job.state == "prepared" {
                return Ok(Some(job));
            }
            if job.state == "failed" {
                if !q::retry_due(job.id).fetch_one(&mut *tx).await? {
                    return Ok(None);
                }
                q::retry(job.id).execute(&mut *tx).await?;
                let refreshed = q::job(job.id).fetch_one(&mut *tx).await?;
                if refreshed.state == "prepared" {
                    tx.commit().await?;
                    return Ok(Some(refreshed));
                }
                refreshed
            } else {
                job
            }
        } else if q::auto(storage).fetch_one(&mut *tx).await? {
            let id = Uuid::new_v4();
            q::request(id, storage, "append").execute(&mut *tx).await?;
            q::job(id).fetch_one(&mut *tx).await?
        } else {
            return Ok(None);
        };
        if !q::ready(storage).fetch_one(&mut *tx).await? {
            q::sync(storage).execute(&mut *tx).await?;
            tx.commit().await?;
            return Ok(None);
        }
        let (parent, revision, sequence, tail, covered, policy, journal) =
            q::point(storage).fetch_one(&mut *tx).await?;
        let id = job.id;
        // No remote files exist for a building job. Its staged plan can be regenerated at a later cut.
        if job.state == "building" {
            q::clear_refs(id).execute(&mut *tx).await?;
            q::clear_members(id).execute(&mut *tx).await?;
            q::clear_pages(id).execute(&mut *tx).await?;
            q::clear_work(id).execute(&mut *tx).await?;
            q::clear_scopes(id).execute(&mut *tx).await?;
        }
        let digest = tail.as_deref().unwrap_or(&[0; 32]);
        ensure!(
            digest.len() == 32 && revision < i64::MAX,
            "invalid snapshot cut"
        );
        q::freeze(
            parent,
            revision + 1,
            sequence,
            digest,
            journal.writer_epoch,
            i64::from(policy.snapshot_page_bytes),
            covered,
            id,
        )
        .execute(&mut *tx)
        .await?;
        q::reset_dictionary(storage, journal.writer_epoch)
            .execute(&mut *tx)
            .await?;
        q::scope(id, storage).execute(&mut *tx).await?;
        q::deltas(id, id, parent).execute(&mut *tx).await?;
        if job.kind == "new" {
            q::current(id, id, storage, id).execute(&mut *tx).await?;
        } else if let Some(parent) = parent {
            let mut hot = BTreeSet::new();
            if job.kind == "compact" {
                for (owner, family, total, dead) in q::scores(id, id, storage, parent)
                    .fetch_all(&mut *tx)
                    .await?
                {
                    if dead > 0 && dead >= total - dead {
                        hot.insert((owner, family));
                        q::hot_items(id, id, owner, family, storage, id)
                            .execute(&mut *tx)
                            .await?;
                    }
                }
            }
            for part in q::pages(parent)
                .fetch_all(&mut *tx)
                .await?
                .into_iter()
                .filter(|p| !hot.contains(&(p.owner, p.family)))
                .enumerate()
            {
                q::reference(id, i64::try_from(part.0)?, part.1.owner, part.1.page_no)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        q::omit_deletes(id, id).execute(&mut *tx).await?;
        let job = q::job(id).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some(job))
    }
    async fn snapshot_scopes(&self, id: Uuid) -> Result<Vec<Scope>> {
        Ok(q::scopes(id).fetch_all(q::read(self)).await?)
    }
    async fn snapshot_items(
        &self,
        id: Uuid,
        bucket: Uuid,
        after: Option<&str>,
        limit: i64,
    ) -> Result<Vec<Item>> {
        Ok(q::items(id, bucket, after, after, limit)
            .fetch_all(q::read(self))
            .await?)
    }
    async fn snapshot_input(
        &self,
        bucket: Uuid,
        seq: i64,
        after: i32,
    ) -> Result<Vec<(i32, Vec<u8>)>> {
        Ok(q::inputs(bucket, seq, after, bucket, seq, after)
            .fetch_all(q::read(self))
            .await?)
    }
    async fn snapshot_pages(&self, id: Uuid) -> Result<Vec<Part>> {
        Ok(q::pages(id).fetch_all(q::read(self)).await?)
    }
    async fn snapshot_page(
        &self,
        job: &Job,
        token: Uuid,
        pos: i64,
        family: i64,
        bucket: Uuid,
        parts: &[(Page, Vec<u8>)],
        members: &[Member],
    ) -> Result<()> {
        ensure!(!parts.is_empty(), "empty snapshot page");
        let mut tx = q::begin(self).await?;
        q::fence(job.storage_id, token).fetch_one(&mut *tx).await?;
        for (i, (page, data)) in parts.iter().enumerate() {
            q::page(
                job.id,
                i64::from(page.number),
                family,
                bucket,
                page,
                data,
                i64::try_from(data.len())?,
            )
            .execute(&mut *tx)
            .await?;
            q::reference(
                job.id,
                pos + i64::try_from(i)?,
                job.id,
                i64::from(page.number),
            )
            .execute(&mut *tx)
            .await?;
        }
        for member in members {
            let item = &member.item;
            q::member(
                job.id,
                family,
                bucket,
                item.generation,
                item.seq,
                &item.object_key,
                item.deleted,
                member.bytes,
            )
            .execute(&mut *tx)
            .await?;
            if !item.deleted {
                q::archive(bucket, item.seq).execute(&mut *tx).await?;
                q::hold(bucket, item.seq).execute(&mut *tx).await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }
    async fn snapshot_seal(
        &self,
        job: &Job,
        token: Uuid,
        manifest: &[u8],
        index: &[u8],
    ) -> Result<()> {
        let mut tx = q::begin(self).await?;
        q::fence(job.storage_id, token).fetch_one(&mut *tx).await?;
        ensure!(
            !q::changed(job.id).fetch_one(&mut *tx).await?,
            "bucket settings changed during snapshot"
        );
        ensure!(
            q::seal(manifest, index, job.id)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            "snapshot state changed"
        );
        tx.commit().await?;
        Ok(())
    }
    async fn snapshot_blobs(&self, job: Uuid) -> Result<(Vec<u8>, Vec<u8>)> {
        Ok(q::blobs(job).fetch_one(q::read(self)).await?)
    }
    async fn snapshot_bytes(&self, owner: Uuid, number: i64) -> Result<Vec<u8>> {
        Ok(q::bytes(owner, number).fetch_one(q::read(self)).await?)
    }
    async fn snapshot_receipt(
        &self,
        job: &Job,
        token: Uuid,
        number: Option<i64>,
        etag: &str,
        version: Option<&str>,
    ) -> Result<()> {
        let mut tx = q::begin(self).await?;
        q::fence(job.storage_id, token).fetch_one(&mut *tx).await?;
        if let Some(n) = number {
            q::receipt(etag, version, job.id, n)
                .execute(&mut *tx)
                .await?;
        } else {
            q::manifest(etag, version, job.id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    async fn snapshot_scope_changed(&self, id: Uuid) -> Result<bool> {
        Ok(q::changed(id).fetch_one(q::read(self)).await?)
    }
    async fn snapshot_abandon(&self, job: &Job, token: Uuid) -> Result<()> {
        let id = job.id;
        let s = job.storage_id;
        let mut tx = q::begin(self).await?;
        q::fence(s, token).fetch_one(&mut *tx).await?;
        if let Some((etag, version, size)) =
            q::abandoned_manifest(id).fetch_optional(&mut *tx).await?
        {
            q::queue_gc(
                s,
                id,
                0,
                "manifest",
                &manifest_path(id),
                &etag,
                version.as_deref(),
                size,
                0,
            )
            .execute(&mut *tx)
            .await?;
        }
        q::clear_refs(id).execute(&mut *tx).await?;
        q::clear_work(id).execute(&mut *tx).await?;
        q::clear_scopes(id).execute(&mut *tx).await?;
        ensure!(
            q::abandoned(id).execute(&mut *tx).await?.rows_affected() == 1,
            "snapshot already switched"
        );
        q::request(Uuid::new_v4(), s, &job.kind)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    async fn snapshot_parent_etag(&self, id: Uuid) -> Result<String> {
        Ok(q::parent_etag(id).fetch_one(q::read(self)).await?)
    }
    async fn snapshot_commit(
        &self,
        job: &Job,
        token: Uuid,
        etag: &str,
        checkpoint: &str,
        version: Option<&str>,
        grace: i64,
    ) -> Result<()> {
        let s = job.storage_id;
        let id = job.id;
        let mut tx = q::begin(self).await?;
        q::fence(s, token).fetch_one(&mut *tx).await?;
        let (parent, revision, _, _, bytes, policy, _) = q::point(s).fetch_one(&mut *tx).await?;
        ensure!(
            parent == job.parent && revision + 1 == job.revision && bytes >= job.covered_bytes,
            "snapshot registration conflicts"
        );
        if let Some(parent) = parent {
            q::retire(grace, parent).execute(&mut *tx).await?;
        }
        ensure!(
            q::activate(etag, checkpoint, version, id)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            "snapshot is not prepared"
        );
        q::remove_values(s, id).execute(&mut *tx).await?;
        q::remove_changes(s, id).execute(&mut *tx).await?;
        q::values(s, id).execute(&mut *tx).await?;
        q::dirty(id).execute(&mut *tx).await?;
        for reopened in q::reopened(id).fetch_all(&mut *tx).await? {
            super::queries::journal::set_mode(reopened, 2)
                .execute(&mut *tx)
                .await?;
            super::journal::configure(&mut tx, reopened, true).await?;
        }
        q::clear_usage(s).execute(&mut *tx).await?;
        q::usage(s, s, id).execute(&mut *tx).await?;
        let (deleted, invalid) = q::stats(s, id).fetch_one(&mut *tx).await?;
        q::settle(
            id,
            job.revision,
            deleted,
            invalid,
            i64::try_from(policy.snapshot_trigger_bytes)?,
            job.covered_bytes,
            job.covered_bytes,
            s,
        )
        .execute(&mut *tx)
        .await?;
        let epoch = job.epoch.context("missing snapshot epoch")?;
        for (file, first, tag, version, bytes) in q::segments(s, epoch, job.sequence_no)
            .fetch_all(&mut *tx)
            .await?
        {
            q::queue_gc(
                s,
                file,
                0,
                "segment",
                &segment_path(epoch, u64::try_from(first)?),
                &tag,
                version.as_deref(),
                bytes,
                grace,
            )
            .execute(&mut *tx)
            .await?;
            q::bucket_log(file, file, -1, 0).execute(&mut *tx).await?;
            q::log_gc(file).execute(&mut *tx).await?;
        }
        q::clear_work(id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn snapshot_fail(
        &self,
        job: &Job,
        token: Uuid,
        error: &str,
        retryable: bool,
    ) -> Result<()> {
        let mut tx = q::begin(self).await?;
        q::fence(job.storage_id, token).fetch_one(&mut *tx).await?;
        q::failed(retryable, error, job.id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    async fn snapshot_gc_failure(&self, storage: Uuid, token: Uuid, reason: &str) -> Result<()> {
        q::gc_failure(reason, storage, token)
            .execute(q::write(self))
            .await?;
        Ok(())
    }
    async fn snapshot_gc(&self, s: Uuid, token: Uuid, _grace: i64) -> Result<Vec<Garbage>> {
        let mut tx = q::begin(self).await?;
        q::fence(s, token).fetch_one(&mut *tx).await?;
        if q::gc_error(s).fetch_one(&mut *tx).await?.is_some()
            || q::pinned(s).fetch_one(&mut *tx).await?
            || q::next(s).fetch_optional(&mut *tx).await?.is_some()
        {
            return Ok(Vec::new());
        }
        for (id, tag, version, manifest_size, epoch, revision, ctag, cversion, index_size) in
            q::retained(s).fetch_all(&mut *tx).await?
        {
            q::clear_refs(id).execute(&mut *tx).await?;
            q::clear_scopes(id).execute(&mut *tx).await?;
            q::gone(id).execute(&mut *tx).await?;
            q::queue_gc(
                s,
                id,
                0,
                "manifest",
                &manifest_path(id),
                &tag,
                version.as_deref(),
                manifest_size,
                0,
            )
            .execute(&mut *tx)
            .await?;
            q::queue_gc(
                s,
                id,
                0,
                "checkpoint",
                &crate::storage::journal::snapshot::checkpoint_path(
                    epoch,
                    u64::try_from(revision)?,
                ),
                &ctag,
                cversion.as_deref(),
                index_size,
                0,
            )
            .execute(&mut *tx)
            .await?;
        }
        for (owner, page, tag, version, bytes) in q::unreferenced(s).fetch_all(&mut *tx).await? {
            q::queue_gc(
                s,
                owner,
                page,
                "page",
                &page_path(owner, u32::try_from(page)?),
                &tag,
                version.as_deref(),
                bytes,
                0,
            )
            .execute(&mut *tx)
            .await?;
            q::page_pending(owner, page).execute(&mut *tx).await?;
        }
        let batch = q::gc(s).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok(batch)
    }
    async fn snapshot_reclaimed(&self, token: Uuid, g: &Garbage) -> Result<()> {
        let s = g.storage_id;
        let id = g.object_id;
        let mut tx = q::begin(self).await?;
        q::fence(s, token).fetch_one(&mut *tx).await?;
        match g.kind.as_str() {
            "segment" => {
                q::log_gone(id).execute(&mut *tx).await?;
                q::remove_parts(id).execute(&mut *tx).await?;
                q::remove_file_entries(id).execute(&mut *tx).await?;
                q::drop_drafts(s).execute(&mut *tx).await?;
                q::drop_holds(s).execute(&mut *tx).await?;
                q::drop_entries(s).execute(&mut *tx).await?;
                q::prune_log(id).execute(&mut *tx).await?;
            }
            "page" => {
                q::page_gone(id, g.page_no).execute(&mut *tx).await?;
                q::drop_members(id).execute(&mut *tx).await?;
                q::drop_inputs(s).execute(&mut *tx).await?;
                q::drop_snap_holds(s).execute(&mut *tx).await?;
                q::prune_pages(id).execute(&mut *tx).await?;
            }
            "manifest" | "checkpoint" => {}
            _ => anyhow::bail!("unknown journal garbage kind"),
        }
        q::dequeue(s, id, &g.kind, g.page_no)
            .execute(&mut *tx)
            .await?;
        if g.kind != "segment" {
            q::prune_job(id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
