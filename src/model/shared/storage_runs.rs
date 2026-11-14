use super::{
    Backend,
    queries::{
        journal_restore, journal_snapshots as snaps, storage_cluster as clusters,
        storage_native as native, storage_payload as physical, storage_repair as repairs,
        storage_runs as q,
    },
};
use crate::{
    authorization::Principal,
    content::fragment::Area,
    model::{
        Error,
        storage_cluster::{ClusterRun, ClusterRuns, RunInput, RunObject},
        storage_payload::Mapping,
        storage_repair::Origin,
    },
};
use anyhow::{Result, ensure};
use uuid::Uuid;

fn now() -> i64 {
    chrono::Utc::now().timestamp_micros()
}

async fn checked(tx: &mut clusters::Tx<'_>, id: Uuid, revision: i64) -> Result<()> {
    let row = clusters::cluster(id).fetch_one(&mut **tx).await?;
    ensure!(revision > 0 && row.2 == revision, Error::OperationAborted);
    Ok(())
}
async fn lease(tx: &mut clusters::Tx<'_>, id: Uuid, token: Uuid) -> Result<()> {
    ensure!(
        q::lease(id, token, now()).fetch_one(&mut **tx).await?,
        Error::OperationAborted
    );
    Ok(())
}

#[async_trait::async_trait]
impl ClusterRuns for Backend {
    async fn start_run(
        &self,
        principal: &Principal,
        cluster: Uuid,
        input: RunInput,
    ) -> Result<ClusterRun> {
        let mut tx = clusters::begin(self).await?;
        clusters::admin(&mut tx, principal).await?;
        checked(&mut tx, cluster, input.revision).await?;
        ensure!(
            matches!(input.kind.as_str(), "rebuild" | "scrub" | "sync")
                && (input.kind == "sync") == input.media_id.is_some(),
            s3s::s3_error!(InvalidArgument, "unknown storage maintenance operation")
        );
        let active = q::active(cluster).fetch_one(&mut *tx).await?;
        ensure!(!active, Error::OperationAborted);
        let row = clusters::cluster(cluster).fetch_one(&mut *tx).await?;
        ensure!(
            row.4
                .groups
                .iter()
                .any(|g| g.layout == row.3.layout && g.replacing.is_none()),
            s3s::s3_error!(InvalidRequest, "no complete layout group is enabled")
        );
        if let Some(group) = &input.backup_group {
            ensure!(
                input.kind == "rebuild"
                    && row
                        .4
                        .members
                        .iter()
                        .any(|m| m.topology.purpose == "backup"
                            && &m.topology.storage_group == group),
                s3s::s3_error!(
                    InvalidArgument,
                    "select a backup group belonging to this cluster"
                )
            );
        }
        if let Some(media) = input.media_id {
            ensure!(
                row.4.members.iter().any(|m| m.media_id == media
                    && m.state == "available"
                    && m.topology.purpose == "primary"),
                s3s::s3_error!(
                    InvalidArgument,
                    "select an available primary cluster member"
                )
            );
        }
        let rate = input.bytes_per_second.unwrap_or(16 * 1024 * 1024);
        ensure!(
            (1..=1_099_511_627_776).contains(&rate),
            s3s::s3_error!(InvalidArgument, "invalid transfer rate")
        );
        let job = ClusterRun {
            id: Uuid::now_v7(),
            cluster,
            revision: input.revision,
            media_id: input.media_id,
            backup_group: input.backup_group,
            kind: input.kind,
            state: "objects".into(),
            checked: 0,
            byte_count: 0,
            bytes_per_second: rate,
            last_error: None,
        };
        let cut = q::cut(cluster)
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or_default();
        q::insert(&job, &row.3.layout, cut)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(job)
    }
    async fn cancel_run(
        &self,
        principal: &Principal,
        cluster: Uuid,
        id: Uuid,
        revision: i64,
    ) -> Result<ClusterRun> {
        let mut tx = clusters::begin(self).await?;
        clusters::admin(&mut tx, principal).await?;
        checked(&mut tx, cluster, revision).await?;
        let mut job = q::job(id).fetch_one(&mut *tx).await?;
        ensure!(
            job.cluster == cluster
                && matches!(job.state.as_str(), "objects" | "snapshots" | "cancelled"),
            Error::OperationAborted
        );
        for (ns, payload, op) in q::staged(id).fetch_all(&mut *tx).await? {
            let head = native::identified(ns, payload).fetch_one(&mut *tx).await?;
            if head.3 == Some(op) {
                physical::retire(op).execute(&mut *tx).await?;
                q::abandon(op).execute(&mut *tx).await?;
            }
        }
        job.state = "cancelled".into();
        q::state(id, &job.state).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(job)
    }
    async fn next_run(&self) -> Result<Option<RunObject>> {
        let mut tx = clusters::begin(self).await?;
        let Some(id) = q::due(now()).fetch_optional(&mut *tx).await? else {
            return Ok(None);
        };
        let job = q::job(id).fetch_one(&mut *tx).await?;
        let (cursor, ceiling, layout) = q::position(id).fetch_one(&mut *tx).await?;
        let token = Uuid::now_v7();
        q::claim(id, token, now() + 90_000_000)
            .execute(&mut *tx)
            .await?;
        let row = clusters::cluster(job.cluster).fetch_one(&mut *tx).await?;
        let mut work = None;
        if job.kind == "rebuild" && row.3.layout != *layout {
            q::release(
                id,
                token,
                now() + 30_000_000,
                "The selected layout changed. Cancel this rebuild or restore its layout.",
            )
            .execute(&mut *tx)
            .await?;
        } else if job.state == "snapshots" {
            if let Some(ns) = q::journals(job.cluster, cursor)
                .fetch_optional(&mut *tx)
                .await?
            {
                let saved = q::snapshot(id, ns).fetch_optional(&mut *tx).await?;
                if saved == Some(true) {
                    q::advance(id, token, ns, 0).execute(&mut *tx).await?;
                } else if saved.is_none()
                    && journal_restore::native_pending(ns)
                        .fetch_optional(&mut *tx)
                        .await?
                        .is_none()
                {
                    // Create the fresh recovery point and its checkpoint in the same transaction.
                    let snap = Uuid::now_v7();
                    snaps::request(snap, ns, "new").execute(&mut *tx).await?;
                    snaps::sync(ns).execute(&mut *tx).await?;
                    q::save_snapshot(id, ns, snap).execute(&mut *tx).await?;
                    q::claim(id, token, now() + 2_000_000)
                        .execute(&mut *tx)
                        .await?;
                } else {
                    q::claim(id, token, now() + 2_000_000)
                        .execute(&mut *tx)
                        .await?;
                }
            } else {
                q::state(id, "completed").execute(&mut *tx).await?;
            }
        } else if let Some((op, path, intent)) = match job.media_id {
            Some(media) => {
                q::next_member(job.cluster, cursor, ceiling, media)
                    .fetch_optional(&mut *tx)
                    .await?
            }
            None => {
                q::next(job.cluster, cursor, ceiling)
                    .fetch_optional(&mut *tx)
                    .await?
            }
        } {
            if job.kind == "rebuild" && intent.area == Area::Recovery {
                q::advance(id, token, op, 0).execute(&mut *tx).await?;
            } else if journal_restore::native_pending(intent.namespace)
                .fetch_optional(&mut *tx)
                .await?
                .is_some()
            {
                q::release(
                    id,
                    token,
                    now() + 5_000_000,
                    "Waiting for storage takeover.",
                )
                .execute(&mut *tx)
                .await?;
            } else {
                intent.validate()?;
                work = Some(RunObject {
                    job,
                    token,
                    intent: intent.0,
                    path,
                });
            }
        } else {
            q::state(
                id,
                if job.kind != "rebuild" {
                    "completed"
                } else {
                    "snapshots"
                },
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(work)
    }
    async fn rebuild_source(
        &self,
        run: Uuid,
        token: Uuid,
        op: Uuid,
        sequence: u32,
        shard: u16,
    ) -> Result<(Uuid, crate::model::storage_payload::Receipt)> {
        let mut tx = physical::begin(self).await?;
        let (media, receipt) = q::source(run, token, now(), op, sequence, shard)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| {
                s3s::s3_error!(
                    ServiceUnavailable,
                    "verified bytes are missing from the selected backup group"
                )
            })?;
        receipt.validate()?;
        ensure!(
            receipt.header.operation == op
                && receipt.header.sequence == sequence
                && receipt.header.shard == shard,
            Error::OperationAborted
        );
        tx.commit().await?;
        Ok((media, receipt.0))
    }
    async fn renew_run(&self, id: Uuid, token: Uuid) -> Result<()> {
        let mut tx = physical::begin(self).await?;
        ensure!(
            q::renew(id, token, now() + 90_000_000)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        tx.commit().await?;
        Ok(())
    }
    async fn run_error(&self, id: Uuid, token: Uuid, error: &str) -> Result<()> {
        let mut tx = physical::begin(self).await?;
        q::release(
            id,
            token,
            now() + 5_000_000,
            &error.chars().take(512).collect::<String>(),
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
    async fn finish_run_object(&self, item: &RunObject, replacement: Option<Uuid>) -> Result<()> {
        let mut tx = clusters::begin(self).await?;
        lease(&mut tx, item.job.id, item.token).await?;
        let intent = &item.intent;
        let head = native::identified(intent.namespace, intent.payload)
            .fetch_one(&mut *tx)
            .await?;
        let mut count = 0;
        if head.2 == Some(intent.operation) {
            ensure!(
                journal_restore::native_pending(intent.namespace)
                    .fetch_optional(&mut *tx)
                    .await?
                    .is_none(),
                Error::OperationAborted
            );
            if item.job.kind == "rebuild" {
                let op = replacement.ok_or(Error::OperationAborted)?;
                ensure!(head.3 == Some(op), Error::OperationAborted);
                let target = super::storage_payload::current(&mut tx, op).await?;
                let (_, _, layout) = q::position(item.job.id).fetch_one(&mut *tx).await?;
                let cluster = clusters::cluster(item.job.cluster)
                    .fetch_one(&mut *tx)
                    .await?;
                ensure!(
                    cluster.3.layout == *layout
                        && target.intent.layout == *layout
                        && target.intent.total == intent.total
                        && target.intent.acknowledgement
                            == crate::model::storage_cluster::Acknowledgement::All,
                    Error::OperationAborted
                );
                if let Some(path) = super::queries::storage_move::root_path(intent.operation)
                    .fetch_optional(&mut *tx)
                    .await?
                {
                    super::queries::storage_journal::track(
                        intent.operation,
                        intent.namespace,
                        &path,
                    )
                    .execute(&mut *tx)
                    .await?;
                }
                super::storage_native::publish_with(
                    &mut tx,
                    intent.namespace,
                    intent.payload,
                    item.job.id,
                    Some(item.token),
                )
                .await?;
            }
            count = intent.total as i64;
        }
        ensure!(
            q::advance(item.job.id, item.token, intent.operation, count)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        tx.commit().await?;
        Ok(())
    }
    async fn scrub_damage(&self, item: &RunObject, mapping: &Mapping) -> Result<()> {
        let mut tx = clusters::begin(self).await?;
        lease(&mut tx, item.job.id, item.token).await?;
        let intent = &item.intent;
        let h = &mapping.header;
        ensure!(
            matches!(item.job.kind.as_str(), "scrub" | "sync")
                && item.job.media_id.is_none_or(|id| id == h.media)
                && h.operation == intent.operation
                && !mapping.deleted,
            Error::OperationAborted
        );
        ensure!(
            journal_restore::native_pending(intent.namespace)
                .fetch_optional(&mut *tx)
                .await?
                .is_none(),
            Error::OperationAborted
        );
        if let Some(row) = physical::fragment(h.fragment, h.operation)
            .fetch_optional(&mut *tx)
            .await?
        {
            let actual = super::storage_payload::mapping(row)?;
            if actual.header == *h && !actual.deleted {
                let id = if let Some(receipt) = actual.receipt {
                    repairs::create(&Origin::Durable(receipt))
                        .execute(&mut *tx)
                        .await?;
                    repairs::restart(h.fragment).execute(&mut *tx).await?;
                    h.fragment
                } else {
                    h.operation
                };
                physical::enqueue_fill(id, h.namespace)
                    .execute(&mut *tx)
                    .await?;
                repairs::restart_task(id).execute(&mut *tx).await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }
}
