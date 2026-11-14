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
    sqlx::query_as("SELECT id,cluster_id AS cluster,media_id,backup_group,revision,kind,state,checked,byte_count,bytes_per_second,last_error FROM storage_cluster_runs WHERE id=$1").bind(id)
}

pub(in super::super) fn active(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storage_cluster_runs WHERE cluster_id=$1 AND state IN ('objects','snapshots'))").bind(id)
}

pub(in super::super) fn cut(cluster: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT operation FROM storage_payloads WHERE cluster_id=$1 AND state='published' ORDER BY operation DESC LIMIT 1").bind(cluster)
}

pub(in super::super) fn insert<'a>(
    j: &'a ClusterRun,
    layout: &'a Layout,
    cut: Uuid,
) -> Statement<'a> {
    sqlx::query("INSERT INTO storage_cluster_runs(id,cluster_id,revision,layout,kind,state,cursor_id,ceiling,bytes_per_second,media_id,backup_group) VALUES($1,$2,$3,$4,$5,'objects',$6,$7,$8,$9,$10)").bind(j.id).bind(j.cluster).bind(j.revision).bind(Json(layout)).bind(&j.kind).bind(Uuid::nil()).bind(cut).bind(j.bytes_per_second).bind(j.media_id).bind(&j.backup_group)
}

pub(in super::super) fn due(now: i64) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM storage_cluster_runs WHERE state IN ('objects','snapshots') AND lease_until<=$1 ORDER BY lease_until,id LIMIT 1").bind(now)
}

pub(in super::super) fn position(id: Uuid) -> Rows<'static, (Uuid, Uuid, Json<Layout>)> {
    sqlx::query_as("SELECT cursor_id,ceiling,layout FROM storage_cluster_runs WHERE id=$1").bind(id)
}

pub(in super::super) fn claim(id: Uuid, token: Uuid, until: i64) -> Statement<'static> {
    sqlx::query("UPDATE storage_cluster_runs SET check_token=$1,lease_until=$2 WHERE id=$3")
        .bind(token)
        .bind(until)
        .bind(id)
}

pub(in super::super) fn renew(id: Uuid, token: Uuid, until: i64) -> Statement<'static> {
    sqlx::query("UPDATE storage_cluster_runs SET lease_until=$1 WHERE id=$2 AND check_token=$3 AND state IN ('objects','snapshots')").bind(until).bind(id).bind(token)
}

pub(in super::super) fn lease(id: Uuid, token: Uuid, now: i64) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storage_cluster_runs WHERE id=$1 AND check_token=$2 AND lease_until>$3 AND state='objects')").bind(id).bind(token).bind(now)
}

pub(in super::super) fn release(id: Uuid, token: Uuid, delay: i64, error: &str) -> Statement<'_> {
    sqlx::query("UPDATE storage_cluster_runs SET check_token=NULL,lease_until=$1,last_error=$2 WHERE id=$3 AND check_token=$4").bind(delay).bind(error).bind(id).bind(token)
}

pub(in super::super) fn next(
    cluster: Uuid,
    after: Uuid,
    cut: Uuid,
) -> Rows<'static, (Uuid, String, Json<crate::model::storage_payload::Intent>)> {
    sqlx::query_as("SELECT p.operation,o.path,p.intent FROM storage_payloads p JOIN storage_objects o ON o.namespace_id=p.namespace_id AND o.payload_id=p.payload_id AND o.head=p.operation WHERE p.cluster_id=$1 AND p.state='published' AND p.operation>$2 AND p.operation<=$3 ORDER BY p.operation LIMIT 1").bind(cluster).bind(after).bind(cut)
}

pub(in super::super) fn next_member(
    cluster: Uuid,
    after: Uuid,
    cut: Uuid,
    media: Uuid,
) -> Rows<'static, (Uuid, String, Json<crate::model::storage_payload::Intent>)> {
    sqlx::query_as("SELECT p.operation,o.path,p.intent FROM storage_payloads p JOIN storage_objects o ON o.namespace_id=p.namespace_id AND o.payload_id=p.payload_id AND o.head=p.operation WHERE p.cluster_id=$1 AND p.state='published' AND p.operation>$2 AND p.operation<=$3 AND EXISTS(SELECT 1 FROM storage_fragments f WHERE f.operation=p.operation AND f.media_id=$4 AND NOT f.deleted) ORDER BY p.operation LIMIT 1").bind(cluster).bind(after).bind(cut).bind(media)
}

pub(in super::super) fn advance(
    id: Uuid,
    token: Uuid,
    cursor: Uuid,
    bytes: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE storage_cluster_runs SET cursor_id=$1,checked=checked+1,byte_count=byte_count+$2,last_error=NULL,check_token=NULL,lease_until=0 WHERE id=$3 AND check_token=$4 AND state IN ('objects','snapshots')").bind(cursor).bind(bytes).bind(id).bind(token)
}

pub(in super::super) fn state(id: Uuid, state: &str) -> Statement<'_> {
    sqlx::query("UPDATE storage_cluster_runs SET cursor_id=$1,state=$2,last_error=NULL,check_token=NULL,lease_until=0 WHERE id=$3").bind(Uuid::nil()).bind(state).bind(id)
}

pub(in super::super) fn journals(cluster: Uuid, after: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT s.id FROM storages s JOIN journal_stores j ON j.storage_id=s.id WHERE s.cluster_id=$1 AND j.marker_installed AND s.id>$2 ORDER BY s.id LIMIT 1").bind(cluster).bind(after)
}

pub(in super::super) fn snapshot(job: Uuid, storage: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT j.confirmed_at IS NOT NULL FROM storage_run_snapshots r JOIN journal_snapshots j ON j.id=r.snapshot_id AND j.storage_id=r.storage_id WHERE (j.confirmed_at IS NOT NULL OR j.state<>'gone') AND r.run_id=$1 AND r.storage_id=$2",
    )
    .bind(job)
    .bind(storage)
}

pub(in super::super) fn save_snapshot(
    job: Uuid,
    storage: Uuid,
    snapshot: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_run_snapshots(run_id,storage_id,snapshot_id) VALUES($1,$2,$3) ON CONFLICT(run_id,storage_id) DO UPDATE SET snapshot_id=excluded.snapshot_id")
        .bind(job)
        .bind(storage)
        .bind(snapshot)
}

pub(in super::super) fn staged(job: Uuid) -> Rows<'static, (Uuid, Uuid, Uuid)> {
    sqlx::query_as("SELECT o.namespace_id,o.payload_id,o.pending FROM storage_objects o JOIN storage_object_writes w ON w.operation=o.pending WHERE w.owner=$1 ORDER BY o.namespace_id,o.payload_id LIMIT 2").bind(job)
}

pub(in super::super) fn abandon(op: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_objects SET pending=NULL WHERE pending=$1").bind(op)
}

pub(in super::super) fn source(
    run: Uuid,
    token: Uuid,
    now: i64,
    op: Uuid,
    seq: u32,
    shard: u16,
) -> Rows<'static, (Uuid, Json<crate::model::storage_payload::Receipt>)> {
    sqlx::query_as("SELECT f.media_id,f.receipt FROM storage_backup_files f JOIN storage_backups b ON b.id=f.backup_id JOIN storage_payloads p ON p.operation=b.operation JOIN storage_cluster_runs r ON r.cluster_id=p.cluster_id AND r.backup_group=b.storage_group JOIN storage_objects o ON o.namespace_id=p.namespace_id AND o.payload_id=p.payload_id AND o.head=p.operation WHERE r.id=$1 AND r.check_token=$2 AND r.lease_until>$3 AND r.kind='rebuild' AND r.state='objects' AND p.state='published' AND p.operation=$4 AND p.operation>r.cursor_id AND p.operation<=r.ceiling AND b.verified_at IS NOT NULL AND f.verified_at IS NOT NULL AND f.sequence=$5 AND f.shard=$6").bind(run).bind(token).bind(now).bind(op).bind(i64::from(seq)).bind(i32::from(shard))
}
