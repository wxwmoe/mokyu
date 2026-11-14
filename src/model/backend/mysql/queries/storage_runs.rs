use super::{Rows, Scalar, Statement};
use crate::model::storage_cluster::{ClusterRun, Layout};
use sqlx::types::Json;
use uuid::Uuid;

pub(in super::super) fn list() -> Rows<'static, ClusterRun> {
    sqlx::query_as(
        "SELECT id,cluster_id AS cluster,media_id,backup_group,revision,kind,state,checked,byte_count,bytes_per_second,last_error FROM storage_cluster_runs ORDER BY CASE WHEN state IN ('objects','snapshots') THEN 0 ELSE 1 END,id DESC LIMIT 256",
    )
}

pub(in super::super) fn job(id: Uuid) -> Rows<'static, ClusterRun> {
    sqlx::query_as("SELECT id,cluster_id AS cluster,media_id,backup_group,revision,kind,state,checked,byte_count,bytes_per_second,last_error FROM storage_cluster_runs WHERE id=?").bind(id)
}

pub(in super::super) fn active(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storage_cluster_runs WHERE cluster_id=? AND state IN ('objects','snapshots'))").bind(id)
}

pub(in super::super) fn cut(cluster: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT operation FROM storage_payloads WHERE cluster_id=? AND state='published' ORDER BY operation DESC LIMIT 1").bind(cluster)
}

pub(in super::super) fn insert<'a>(
    j: &'a ClusterRun,
    layout: &'a Layout,
    cut: Uuid,
) -> Statement<'a> {
    sqlx::query("INSERT INTO storage_cluster_runs(id,cluster_id,revision,layout,kind,state,cursor_id,ceiling,bytes_per_second,media_id,backup_group) VALUES(?,?,?,?,?,'objects',?,?,?,?,?)").bind(j.id).bind(j.cluster).bind(j.revision).bind(Json(layout)).bind(&j.kind).bind(Uuid::nil()).bind(cut).bind(j.bytes_per_second).bind(j.media_id).bind(&j.backup_group)
}

pub(in super::super) fn due(now: i64) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM storage_cluster_runs WHERE state IN ('objects','snapshots') AND lease_until<=? ORDER BY lease_until,id LIMIT 1").bind(now)
}

pub(in super::super) fn position(id: Uuid) -> Rows<'static, (Uuid, Uuid, Json<Layout>)> {
    sqlx::query_as("SELECT cursor_id,ceiling,layout FROM storage_cluster_runs WHERE id=?").bind(id)
}

pub(in super::super) fn claim(id: Uuid, token: Uuid, until: i64) -> Statement<'static> {
    sqlx::query("UPDATE storage_cluster_runs SET check_token=?,lease_until=? WHERE id=?")
        .bind(token)
        .bind(until)
        .bind(id)
}

pub(in super::super) fn renew(id: Uuid, token: Uuid, until: i64) -> Statement<'static> {
    sqlx::query("UPDATE storage_cluster_runs SET lease_until=? WHERE id=? AND check_token=? AND state IN ('objects','snapshots')").bind(until).bind(id).bind(token)
}

pub(in super::super) fn lease(id: Uuid, token: Uuid, now: i64) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storage_cluster_runs WHERE id=? AND check_token=? AND lease_until>? AND state='objects')").bind(id).bind(token).bind(now)
}

pub(in super::super) fn release(id: Uuid, token: Uuid, delay: i64, error: &str) -> Statement<'_> {
    sqlx::query("UPDATE storage_cluster_runs SET check_token=NULL,lease_until=?,last_error=? WHERE id=? AND check_token=?").bind(delay).bind(error).bind(id).bind(token)
}

pub(in super::super) fn next(
    cluster: Uuid,
    after: Uuid,
    cut: Uuid,
) -> Rows<'static, (Uuid, String, Json<crate::model::storage_payload::Intent>)> {
    sqlx::query_as("SELECT p.operation,o.path,p.intent FROM storage_payloads p JOIN storage_objects o ON o.namespace_id=p.namespace_id AND o.payload_id=p.payload_id AND o.head=p.operation WHERE p.cluster_id=? AND p.state='published' AND p.operation>? AND p.operation<=? ORDER BY p.operation LIMIT 1").bind(cluster).bind(after).bind(cut)
}

pub(in super::super) fn next_member(
    cluster: Uuid,
    after: Uuid,
    cut: Uuid,
    media: Uuid,
) -> Rows<'static, (Uuid, String, Json<crate::model::storage_payload::Intent>)> {
    sqlx::query_as("SELECT p.operation,o.path,p.intent FROM storage_payloads p JOIN storage_objects o ON o.namespace_id=p.namespace_id AND o.payload_id=p.payload_id AND o.head=p.operation WHERE p.cluster_id=? AND p.state='published' AND p.operation>? AND p.operation<=? AND EXISTS(SELECT 1 FROM storage_fragments f WHERE f.operation=p.operation AND f.media_id=? AND NOT f.deleted) ORDER BY p.operation LIMIT 1").bind(cluster).bind(after).bind(cut).bind(media)
}

pub(in super::super) fn advance(
    id: Uuid,
    token: Uuid,
    cursor: Uuid,
    bytes: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE storage_cluster_runs SET cursor_id=?,checked=checked+1,byte_count=byte_count+?,last_error=NULL,check_token=NULL,lease_until=0 WHERE id=? AND check_token=? AND state IN ('objects','snapshots')").bind(cursor).bind(bytes).bind(id).bind(token)
}

pub(in super::super) fn state(id: Uuid, state: &str) -> Statement<'_> {
    sqlx::query("UPDATE storage_cluster_runs SET cursor_id=?,state=?,last_error=NULL,check_token=NULL,lease_until=0 WHERE id=?").bind(Uuid::nil()).bind(state).bind(id)
}

pub(in super::super) fn journals(cluster: Uuid, after: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT s.id FROM storages s JOIN journal_stores j ON j.storage_id=s.id WHERE s.cluster_id=? AND j.marker_installed AND s.id>? ORDER BY s.id LIMIT 1").bind(cluster).bind(after)
}

pub(in super::super) fn snapshot(job: Uuid, storage: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT j.confirmed_at IS NOT NULL FROM storage_run_snapshots r JOIN journal_snapshots j ON j.id=r.snapshot_id AND j.storage_id=r.storage_id WHERE (j.confirmed_at IS NOT NULL OR j.state<>'gone') AND r.run_id=? AND r.storage_id=?",
    )
    .bind(job)
    .bind(storage)
}

pub(in super::super) fn save_snapshot(
    job: Uuid,
    storage: Uuid,
    snapshot: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_run_snapshots(run_id,storage_id,snapshot_id) VALUES(?,?,?) ON DUPLICATE KEY UPDATE snapshot_id=VALUES(snapshot_id)")
        .bind(job)
        .bind(storage)
        .bind(snapshot)
}

pub(in super::super) fn staged(job: Uuid) -> Rows<'static, (Uuid, Uuid, Uuid)> {
    sqlx::query_as("SELECT o.namespace_id,o.payload_id,o.pending FROM storage_objects o JOIN storage_object_writes w ON w.operation=o.pending WHERE w.owner=? ORDER BY o.namespace_id,o.payload_id LIMIT 2").bind(job)
}

pub(in super::super) fn abandon(op: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_objects SET pending=NULL WHERE pending=?").bind(op)
}

pub(in super::super) fn source(
    run: Uuid,
    token: Uuid,
    now: i64,
    op: Uuid,
    seq: u32,
    shard: u16,
) -> Rows<'static, (Uuid, Json<crate::model::storage_payload::Receipt>)> {
    sqlx::query_as("SELECT f.media_id,f.receipt FROM storage_backup_files f JOIN storage_backups b ON b.id=f.backup_id JOIN storage_payloads p ON p.operation=b.operation JOIN storage_cluster_runs r ON r.cluster_id=p.cluster_id AND r.backup_group=b.storage_group JOIN storage_objects o ON o.namespace_id=p.namespace_id AND o.payload_id=p.payload_id AND o.head=p.operation WHERE r.id=? AND r.check_token=? AND r.lease_until>? AND r.kind='rebuild' AND r.state='objects' AND p.state='published' AND p.operation=? AND p.operation>r.cursor_id AND p.operation<=r.ceiling AND b.verified_at IS NOT NULL AND f.verified_at IS NOT NULL AND f.sequence=? AND f.shard=?").bind(run).bind(token).bind(now).bind(op).bind(i64::from(seq)).bind(i32::from(shard))
}
