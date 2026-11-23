use super::{Rows, Scalar, Statement};
use crate::model::storage_payload::Receipt;
use sqlx::types::Json;
use uuid::Uuid;

pub(in super::super) fn schedule(cluster: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_backup_scans(cluster_id,after_id) VALUES(?,?) ON DUPLICATE KEY UPDATE after_id=VALUES(after_id)").bind(cluster).bind(Uuid::nil())
}
pub(in super::super) fn verify(cluster: Uuid, stamp: i64) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_backup_scans(cluster_id,after_id,verify_since) VALUES(?,?,?) ON DUPLICATE KEY UPDATE after_id=VALUES(after_id),verify_since=GREATEST(verify_since,VALUES(verify_since))").bind(cluster).bind(Uuid::nil()).bind(stamp)
}
pub(in super::super) fn queued_checks(operation: Uuid, stamp: i64) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM storage_backups WHERE operation=? AND verify_since<? ORDER BY ordinal",
    )
    .bind(operation)
    .bind(stamp)
}
pub(in super::super) fn reset(id: Uuid, stamp: i64) -> Statement<'static> {
    sqlx::query("UPDATE storage_backups SET verify_since=?,verified_at=NULL WHERE id=?")
        .bind(stamp)
        .bind(id)
}
pub(in super::super) fn stale(id: Uuid) -> Rows<'static, (i64, i64)> {
    sqlx::query_as("SELECT CAST(f.sequence AS SIGNED),CAST(f.shard AS SIGNED) FROM storage_backup_files f JOIN storage_backups b ON b.id=f.backup_id WHERE f.backup_id=? AND f.verified_at<b.verify_since ORDER BY f.verified_at,f.sequence,f.shard LIMIT 1").bind(id)
}
pub(in super::super) fn cutoff(id: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT verify_since FROM storage_backups WHERE id=?").bind(id)
}
pub(in super::super) fn due() -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT cluster_id FROM storage_backup_scans ORDER BY cluster_id LIMIT 1")
}
pub(in super::super) fn cursor(cluster: Uuid) -> Rows<'static, (Uuid, i64)> {
    sqlx::query_as(
        "SELECT after_id,verify_since FROM storage_backup_scans WHERE cluster_id=? FOR UPDATE",
    )
    .bind(cluster)
}
pub(in super::super) fn scan(cluster: Uuid, after: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT operation FROM storage_payloads WHERE cluster_id=? AND state='published' AND operation>? ORDER BY operation LIMIT 16").bind(cluster).bind(after)
}
pub(in super::super) fn advance(cluster: Uuid, after: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_backup_scans SET after_id=? WHERE cluster_id=?")
        .bind(after)
        .bind(cluster)
}
pub(in super::super) fn end(cluster: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM storage_backup_scans WHERE cluster_id=?").bind(cluster)
}
pub(in super::super) fn namespaces(cluster: Uuid, after: Uuid) -> Rows<'static, (Uuid, String)> {
    sqlx::query_as("SELECT id,name FROM storages WHERE cluster_id=? AND id>? ORDER BY id LIMIT 9")
        .bind(cluster)
        .bind(after)
}
pub(in super::super) fn gaps(storage: Uuid, group: &str) -> Rows<'_, (i64, i64)> {
    sqlx::query_as("SELECT COUNT(CASE WHEN CAST(JSON_UNQUOTE(JSON_EXTRACT(p.intent,'$.area')) AS BINARY)='blocks' THEN 1 END),COUNT(CASE WHEN CAST(JSON_UNQUOTE(JSON_EXTRACT(p.intent,'$.area')) AS BINARY)='recovery' THEN 1 END) FROM storage_payloads p WHERE p.namespace_id=? AND (p.state='published' OR EXISTS(SELECT 1 FROM storage_journal_sources s WHERE s.operation=p.operation)) AND NOT EXISTS(SELECT 1 FROM storage_backups b WHERE b.operation=p.operation AND b.storage_group=? AND b.verified_at IS NOT NULL)").bind(storage).bind(group)
}
pub(in super::super) fn slots(operation: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT COUNT(*) FROM storage_backups WHERE operation=?").bind(operation)
}

pub(in super::super) fn copies(cluster: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT CAST(JSON_UNQUOTE(JSON_EXTRACT(policy,'$.backup_copies')) AS SIGNED) FROM storage_clusters WHERE id=?").bind(cluster)
}

pub(in super::super) fn add(
    id: Uuid,
    operation: Uuid,
    ordinal: i32,
    now: i64,
) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_backups(id,operation,ordinal,created_at) VALUES(?,?,?,?)")
        .bind(id)
        .bind(operation)
        .bind(ordinal)
        .bind(now)
}

pub(in super::super) fn row(id: Uuid) -> Rows<'static, (Uuid, Option<String>, Option<i64>)> {
    sqlx::query_as(
        "SELECT operation,storage_group,verified_at FROM storage_backups WHERE id=? FOR UPDATE",
    )
    .bind(id)
}

pub(in super::super) fn stats() -> Rows<
    'static,
    (
        Uuid,
        Option<String>,
        i64,
        i64,
        i64,
        Option<i64>,
        Option<i64>,
    ),
> {
    sqlx::query_as(
        "SELECT p.cluster_id,b.storage_group,COUNT(*)-COUNT(b.verified_at),COUNT(b.verified_at),CAST(COALESCE(SUM(CASE WHEN b.verified_at IS NULL THEN CAST(JSON_UNQUOTE(JSON_EXTRACT(p.intent,'$.total')) AS SIGNED) ELSE 0 END),0) AS SIGNED),MIN(CASE WHEN b.verified_at IS NULL THEN b.created_at END),MAX(b.verified_at) FROM storage_backups b JOIN storage_payloads p ON p.operation=b.operation GROUP BY p.cluster_id,b.storage_group ORDER BY p.cluster_id,b.storage_group",
    )
}

pub(in super::super) fn groups(operation: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar(
        "SELECT storage_group FROM storage_backups WHERE operation=? AND storage_group IS NOT NULL",
    )
    .bind(operation)
}

pub(in super::super) fn assign(id: Uuid, group: &str) -> Statement<'_> {
    sqlx::query("UPDATE storage_backups SET storage_group=? WHERE id=? AND storage_group IS NULL")
        .bind(group)
        .bind(id)
}

pub(in super::super) fn file(
    id: Uuid,
    sequence: u32,
    shard: u16,
) -> Rows<'static, (Uuid, Json<Receipt>, Option<i64>)> {
    sqlx::query_as("SELECT media_id,receipt,verified_at FROM storage_backup_files WHERE backup_id=? AND sequence=? AND shard=? FOR UPDATE").bind(id).bind(i64::from(sequence)).bind(i32::from(shard))
}

pub(in super::super) fn insert(id: Uuid, media: Uuid, receipt: &Receipt) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_backup_files(backup_id,sequence,shard,media_id,receipt) VALUES(?,?,?,?,?)").bind(id).bind(i64::from(receipt.header.sequence)).bind(i32::from(receipt.header.shard)).bind(media).bind(Json(receipt))
}

pub(in super::super) fn count(id: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM storage_backup_files WHERE backup_id=? AND verified_at IS NOT NULL",
    )
    .bind(id)
}

pub(in super::super) fn checked(
    id: Uuid,
    sequence: u32,
    shard: u16,
    now: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE storage_backup_files SET verified_at=? WHERE backup_id=? AND sequence=? AND shard=?").bind(now).bind(id).bind(i64::from(sequence)).bind(i32::from(shard))
}

pub(in super::super) fn finish(id: Uuid, now: i64) -> Statement<'static> {
    sqlx::query("UPDATE storage_backups SET verified_at=? WHERE id=? AND verified_at IS NULL")
        .bind(now)
        .bind(id)
}

pub(in super::super) fn publication(namespace: Uuid, operation: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT p.revision FROM storage_journal_publications p WHERE p.namespace_id=? AND LOWER(HEX(?))=REPLACE(CAST(JSON_UNQUOTE(JSON_EXTRACT(p.proof,'$.publication.parts[0].intent.operation')) AS BINARY),'-','') AND p.settled ORDER BY p.revision DESC LIMIT 1").bind(namespace).bind(operation)
}

pub(in super::super) fn outstanding(media: Uuid, operation: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storage_backup_files f JOIN storage_backups b ON b.id=f.backup_id WHERE f.media_id=? AND b.operation=? AND f.verified_at IS NULL)").bind(media).bind(operation)
}

pub(in super::super) fn image(
    id: Uuid,
    hash: Vec<u8>,
) -> Rows<'static, (String, bool, Json<crate::model::storage_payload::Intent>)> {
    sqlx::query_as("SELECT path,published,intent FROM storage_recovery_objects WHERE catalog=? AND path_hash=?").bind(id).bind(hash)
}
pub(in super::super) fn put_image<'a>(
    id: Uuid,
    hash: Vec<u8>,
    path: &'a str,
    intent: &'a crate::model::storage_payload::Intent,
    published: bool,
) -> Statement<'a> {
    sqlx::query("INSERT INTO storage_recovery_objects(catalog,path_hash,path,revision,published,intent) VALUES(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE revision=VALUES(revision),published=VALUES(published),intent=VALUES(intent)").bind(id).bind(hash).bind(path).bind(intent.revision as i64).bind(published).bind(Json(intent))
}
pub(in super::super) fn shard(id: Uuid, r: &Receipt) -> Scalar<'static, Json<Receipt>> {
    let h = &r.header;
    sqlx::query_scalar("SELECT receipt FROM storage_recovery_shards WHERE catalog=? AND operation=? AND sequence=? AND shard_index=? AND copy_index=? FOR UPDATE").bind(id).bind(h.operation).bind(i64::from(h.sequence)).bind(i32::from(h.shard)).bind(i32::from(h.copy))
}
pub(in super::super) fn put_shard(id: Uuid, r: &Receipt) -> Statement<'_> {
    let h = &r.header;
    sqlx::query("INSERT INTO storage_recovery_shards(catalog,operation,sequence,shard_index,copy_index,receipt) VALUES(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE receipt=VALUES(receipt)").bind(id).bind(h.operation).bind(i64::from(h.sequence)).bind(i32::from(h.shard)).bind(i32::from(h.copy)).bind(Json(r))
}
pub(in super::super) fn shards(id: Uuid, op: Uuid, seq: u32) -> Scalar<'static, Json<Receipt>> {
    sqlx::query_scalar("SELECT receipt FROM storage_recovery_shards WHERE catalog=? AND operation=? AND sequence=? ORDER BY shard_index,copy_index LIMIT 257").bind(id).bind(op).bind(i64::from(seq))
}
pub(in super::super) fn reset_evidence(namespace: Uuid, media: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE storage_journal_receipts SET revision=0 WHERE namespace_id=? AND media_id=?",
    )
    .bind(namespace)
    .bind(media)
}
pub(in super::super) fn pending(operation: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM storage_backups WHERE operation=? AND verified_at IS NULL)",
    )
    .bind(operation)
}
