use super::{Rows, Scalar, Statement};
use uuid::Uuid;

pub(in super::super) type Row = (Uuid, Uuid, Uuid, Uuid, Uuid, String, i64);

pub(in super::super) fn list() -> Rows<'static, Row> {
    sqlx::query_as(
        "SELECT id,cluster_id,group_id,source_media,target_media,state,bytes_per_second FROM storage_member_moves ORDER BY CASE WHEN state IN ('cancelled','completed') THEN 1 ELSE 0 END,created_at DESC,id DESC LIMIT 256",
    )
}
pub(in super::super) fn job(id: Uuid) -> Rows<'static, Row> {
    sqlx::query_as("SELECT id,cluster_id,group_id,source_media,target_media,state,bytes_per_second FROM storage_member_moves WHERE id=? FOR UPDATE").bind(id)
}
pub(in super::super) fn assigned(cluster: Uuid, media: Uuid) -> Rows<'static, Row> {
    sqlx::query_as("SELECT id,cluster_id,group_id,CASE WHEN state='reverting' THEN target_media ELSE source_media END,CASE WHEN state='reverting' THEN source_media ELSE target_media END,state,bytes_per_second FROM storage_member_moves WHERE cluster_id=? AND ((source_media=? AND state IN ('moving','paused')) OR (target_media=? AND state='reverting')) ORDER BY created_at DESC LIMIT 1").bind(cluster).bind(media).bind(media)
}
pub(in super::super) fn add(
    job: &crate::model::storage_cluster::MemberMove,
    now: i64,
) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_member_moves(id,cluster_id,group_id,source_media,target_media,state,bytes_per_second,created_at) VALUES(?,?,?,?,?,?,?,?)").bind(job.id).bind(job.cluster).bind(job.group).bind(job.source).bind(job.target).bind(&job.state).bind(job.bytes_per_second).bind(now)
}
pub(in super::super) fn state(id: Uuid, state: &str) -> Statement<'_> {
    sqlx::query("UPDATE storage_member_moves SET state=?,checked_at=0 WHERE id=?")
        .bind(state)
        .bind(id)
}
pub(in super::super) fn due(now: i64) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM storage_member_moves WHERE state IN ('moving','reverting','cleaning','undo_cleanup') AND checked_at<=? ORDER BY checked_at,id LIMIT 1").bind(now)
}
pub(in super::super) fn checked(id: Uuid, next: i64) -> Statement<'static> {
    sqlx::query("UPDATE storage_member_moves SET checked_at=? WHERE id=?")
        .bind(next)
        .bind(id)
}
pub(in super::super) fn root_path(op: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT o.path FROM storage_objects o WHERE o.head=? AND (o.path='mokyu.json' OR o.path LIKE '%/mokyu.json') AND EXISTS(SELECT 1 FROM storage_journal_publications j WHERE j.namespace_id=o.namespace_id AND j.settled)").bind(op)
}
pub(in super::super) fn waiting(media: Uuid) -> Rows<'static, super::storage_payload::FragmentRow> {
    sqlx::query_as("SELECT f.header,f.root,f.commitment,f.deleted FROM storage_fragments f JOIN storage_payloads p ON p.operation=f.operation WHERE f.media_id=? AND p.state='published' AND NOT f.deleted AND NOT EXISTS(SELECT 1 FROM storage_repairs r WHERE r.id=f.id AND r.state IN ('writing','queued')) ORDER BY f.operation,f.id LIMIT 1").bind(media)
}
pub(in super::super) fn live(media: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storage_fragments f JOIN storage_payloads p ON p.operation=f.operation WHERE f.media_id=? AND NOT f.deleted AND p.state IN ('prepared','published'))").bind(media)
}
pub(in super::super) fn prepared(cluster: Uuid, media: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storage_payloads p WHERE p.cluster_id=? AND p.state='prepared' AND JSON_CONTAINS(JSON_EXTRACT(p.intent,'$.members'),JSON_QUOTE(?)))").bind(cluster).bind(media.to_string())
}
pub(in super::super) fn remains(media: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storage_repairs r WHERE LOWER(HEX(?))=REPLACE(JSON_UNQUOTE(JSON_EXTRACT(r.original_receipt,'$.header.media')),'-','') AND NOT r.original_removed AND r.state='installed' UNION ALL SELECT 1 FROM storage_fragments f WHERE f.media_id=? AND NOT f.deleted UNION ALL SELECT 1 FROM storage_reservations WHERE media_id=?)").bind(media).bind(media).bind(media)
}
pub(in super::super) fn retired(
    media: Uuid,
    now: i64,
) -> Scalar<'static, sqlx::types::Json<crate::model::storage_payload::Intent>> {
    sqlx::query_scalar("SELECT p.intent FROM storage_payloads p WHERE p.state='retired' AND p.collect_after<=? AND EXISTS(SELECT 1 FROM storage_fragments f WHERE f.operation=p.operation AND f.media_id=? AND NOT f.deleted) ORDER BY p.collect_after,p.operation LIMIT 1").bind(now).bind(media)
}
pub(in super::super) fn cleanup(
    media: Uuid,
    now: i64,
) -> Scalar<'static, sqlx::types::Json<crate::model::storage_payload::Intent>> {
    sqlx::query_scalar("SELECT p.intent FROM storage_repairs r JOIN storage_payloads p ON p.operation=r.operation WHERE LOWER(HEX(?))=REPLACE(JSON_UNQUOTE(JSON_EXTRACT(r.original_receipt,'$.header.media')),'-','') AND r.state='installed' AND NOT r.original_removed AND r.protected_until<=? AND p.collect_after<=? ORDER BY p.collect_after,r.id LIMIT 1").bind(media).bind(now).bind(now)
}
pub(in super::super) fn started(media: Uuid, target: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storage_repairs r WHERE LOWER(HEX(?))=REPLACE(JSON_UNQUOTE(JSON_EXTRACT(r.original_receipt,'$.header.media')),'-','') AND LOWER(HEX(?))=REPLACE(JSON_UNQUOTE(JSON_EXTRACT(r.candidate,'$.media')),'-','') AND r.state IN ('writing','installed'))").bind(media).bind(target)
}
pub(in super::super) fn writing(media: Uuid, target: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storage_repairs r WHERE LOWER(HEX(?))=REPLACE(JSON_UNQUOTE(JSON_EXTRACT(r.original_receipt,'$.header.media')),'-','') AND LOWER(HEX(?))=REPLACE(JSON_UNQUOTE(JSON_EXTRACT(r.candidate,'$.media')),'-','') AND r.state='writing')").bind(media).bind(target)
}
pub(in super::super) fn lagging(source: Uuid, target: Uuid) -> Rows<'static, (Uuid, i64, i64)> {
    sqlx::query_as("SELECT s.namespace_id,s.revision,COALESCE(t.revision,0) FROM storage_journal_receipts s LEFT JOIN storage_journal_receipts t ON t.namespace_id=s.namespace_id AND t.media_id=? WHERE s.media_id=? AND s.revision>COALESCE(t.revision,0) ORDER BY s.namespace_id LIMIT 1").bind(target).bind(source)
}

pub(in super::super) fn draining() -> Rows<'static, (Uuid, Uuid, Uuid)> {
    sqlx::query_as(
        "SELECT h.node_id,d.cluster_id,d.media_id FROM node_handoffs h JOIN storage_devices d ON d.node_id=h.node_id WHERE h.state='preparing' AND d.cluster_id IS NOT NULL ORDER BY h.node_id LIMIT 256",
    )
}

pub(in super::super) fn occupied(source: Uuid, target: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storage_member_moves WHERE state NOT IN ('completed','cancelled') AND (source_media IN (?,?) OR target_media IN (?,?)))").bind(source).bind(target).bind(source).bind(target)
}

pub(in super::super) fn status(media: Uuid) -> Rows<'static, (i64, i64, i64, i64)> {
    sqlx::query_as("SELECT (SELECT COUNT(*) FROM storage_fragments WHERE media_id=? AND NOT deleted)+(SELECT COUNT(*) FROM storage_backup_files WHERE media_id=?),(SELECT COUNT(*) FROM storage_member_moves WHERE state NOT IN ('completed','cancelled') AND (source_media=? OR target_media=?)),(SELECT COUNT(*) FROM storage_reservations WHERE media_id=?),(SELECT COUNT(*) FROM storage_journal_receipts WHERE media_id=? AND revision>0)").bind(media).bind(media).bind(media).bind(media).bind(media).bind(media)
}

pub(in super::super) fn clear_evidence(media: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM storage_journal_receipts WHERE media_id=?").bind(media)
}

pub(in super::super) fn backup_ref(
    media: Uuid,
) -> Rows<
    'static,
    (
        Uuid,
        sqlx::types::Json<crate::model::storage_payload::Receipt>,
    ),
> {
    sqlx::query_as("SELECT backup_id,receipt FROM storage_backup_files WHERE media_id=? ORDER BY backup_id,sequence,shard LIMIT 1").bind(media)
}

pub(in super::super) fn backup_list(
    job: Uuid,
    cleanup: bool,
) -> Rows<
    'static,
    (
        Uuid,
        sqlx::types::Json<crate::model::storage_payload::Receipt>,
    ),
> {
    sqlx::query_as("SELECT backup_id,receipt FROM storage_backup_moves WHERE job_id=? AND copied=? AND NOT removed ORDER BY backup_id,sequence,shard LIMIT 1").bind(job).bind(cleanup)
}

pub(in super::super) fn backup_stage(
    job: Uuid,
    id: Uuid,
    r: &crate::model::storage_payload::Receipt,
) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_backup_moves(job_id,backup_id,sequence,shard,receipt) VALUES(?,?,?,?,?)").bind(job).bind(id).bind(i64::from(r.header.sequence)).bind(i32::from(r.header.shard)).bind(sqlx::types::Json(r.clone()))
}

pub(in super::super) fn backup_confirm(
    job: Uuid,
    id: Uuid,
    sequence: u32,
    shard: u16,
) -> Statement<'static> {
    sqlx::query("UPDATE storage_backup_moves SET copied=TRUE WHERE job_id=? AND backup_id=? AND sequence=? AND shard=? AND NOT copied").bind(job).bind(id).bind(i64::from(sequence)).bind(i32::from(shard))
}

pub(in super::super) fn backup_removed(
    job: Uuid,
    id: Uuid,
    sequence: u32,
    shard: u16,
) -> Statement<'static> {
    sqlx::query("UPDATE storage_backup_moves SET removed=TRUE WHERE job_id=? AND backup_id=? AND sequence=? AND shard=? AND copied").bind(job).bind(id).bind(i64::from(sequence)).bind(i32::from(shard))
}

pub(in super::super) fn backup_switch(
    id: Uuid,
    r: &crate::model::storage_payload::Receipt,
    source: Uuid,
    target: Uuid,
    now: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE storage_backup_files SET media_id=?,verified_at=? WHERE backup_id=? AND sequence=? AND shard=? AND media_id=?").bind(target).bind(now).bind(id).bind(i64::from(r.header.sequence)).bind(i32::from(r.header.shard)).bind(source)
}

pub(in super::super) fn backup_release(op: Uuid, media: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM storage_reservations WHERE operation=? AND media_id=? AND NOT EXISTS(SELECT 1 FROM storage_backup_files f JOIN storage_backups b ON b.id=f.backup_id WHERE f.media_id=? AND b.operation=?)").bind(op).bind(media).bind(media).bind(op)
}

pub(in super::super) fn excluded(cluster: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT d.media_id FROM storage_devices d JOIN node_handoffs h ON h.node_id=d.node_id WHERE d.cluster_id=? AND h.state<>'serving' UNION SELECT source_media FROM storage_member_moves WHERE cluster_id=? AND state NOT IN ('completed','cancelled')").bind(cluster).bind(cluster)
}

pub(in super::super) fn uncapture(media: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM storage_repairs WHERE state='queued' AND candidate IS NULL AND id IN (SELECT f.id FROM storage_fragments f WHERE f.media_id=? AND f.root IS NULL AND NOT f.deleted)").bind(media)
}

pub(in super::super) fn backup_remaining(
    job: Uuid,
) -> Rows<
    'static,
    (
        Uuid,
        sqlx::types::Json<crate::model::storage_payload::Receipt>,
    ),
> {
    sqlx::query_as("SELECT backup_id,receipt FROM storage_backup_moves WHERE job_id=? AND NOT removed ORDER BY backup_id,sequence,shard LIMIT 1").bind(job)
}

pub(in super::super) fn backup_restored(
    job: Uuid,
    id: Uuid,
    seq: u32,
    shard: u16,
) -> Statement<'static> {
    sqlx::query("UPDATE storage_backup_moves SET copied=FALSE WHERE job_id=? AND backup_id=? AND sequence=? AND shard=? AND NOT removed").bind(job).bind(id).bind(i64::from(seq)).bind(i32::from(shard))
}

pub(in super::super) fn backup_discarded(
    job: Uuid,
    id: Uuid,
    seq: u32,
    shard: u16,
) -> Statement<'static> {
    sqlx::query("UPDATE storage_backup_moves SET removed=TRUE WHERE job_id=? AND backup_id=? AND sequence=? AND shard=? AND NOT copied").bind(job).bind(id).bind(i64::from(seq)).bind(i32::from(shard))
}

pub(in super::super) fn transferring(media: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storage_repairs r JOIN storage_fragments f ON f.id=r.id JOIN storage_payloads p ON p.operation=r.operation WHERE f.media_id=? AND NOT f.deleted AND p.state='published' AND r.state IN ('queued','writing'))").bind(media)
}
