use super::{Rows, Scalar, Statement};
use crate::content::fragment::Header;
use crate::model::storage_payload::Receipt;
use sqlx::types::Json;
use uuid::Uuid;

pub(in super::super) type Row = (
    Json<crate::model::storage_repair::Origin>,
    Option<Json<Header>>,
    String,
    bool,
    bool,
    i64,
    bool,
);

pub(in super::super) fn row(id: Uuid) -> Rows<'static, Row> {
    sqlx::query_as("SELECT original_receipt,candidate,state,original_removed,candidate_removed,protected_until,collecting FROM storage_repairs WHERE id=? FOR UPDATE")
        .bind(id)
}

pub(in super::super) fn locate(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT operation FROM storage_fragments WHERE id=? AND root IS NOT NULL AND NOT deleted UNION SELECT operation FROM storage_repairs WHERE id=? LIMIT 1").bind(id).bind(id)
}

pub(in super::super) fn create(original: &crate::model::storage_repair::Origin) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_repairs(id,operation,sequence,original_receipt) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE id=id")
        .bind(original.header().fragment).bind(original.header().operation).bind(i64::from(original.header().sequence)).bind(Json(original))
}

pub(in super::super) fn prepare(id: Uuid, candidate: &Header) -> Statement<'_> {
    sqlx::query("UPDATE storage_repairs SET candidate=?,candidate_id=?,state='writing' WHERE id=? AND state='queued'")
        .bind(Json(candidate)).bind(candidate.fragment).bind(id)
}

pub(in super::super) fn healthy(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_repairs SET state='healthy' WHERE id=? AND state='queued'").bind(id)
}

pub(in super::super) fn installed(id: Uuid, candidate: &Header, until: i64) -> Statement<'_> {
    sqlx::query(
        "UPDATE storage_repairs SET candidate=?,state='installed',protected_until=? WHERE id=? AND state='writing'",
    )
    .bind(Json(candidate))
    .bind(until).bind(id)
}

pub(in super::super) fn replace(
    original: Uuid,
    receipt: &Receipt,
    encoded: Vec<u8>,
) -> Statement<'static> {
    sqlx::query("UPDATE storage_fragments SET id=?,media_id=?,header=?,root=?,commitment=? WHERE id=? AND operation=? AND NOT deleted")
        .bind(receipt.header.fragment).bind(receipt.header.media).bind(encoded)
        .bind(receipt.root.to_vec()).bind(receipt.commitment.to_vec()).bind(original).bind(receipt.header.operation)
}

pub(in super::super) fn pending(operation: Uuid, first: u32, end: u64) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM storage_repairs WHERE operation=? AND sequence>=? AND sequence<? AND state='writing' AND JSON_EXTRACT(candidate,'$.media')=JSON_EXTRACT(original_receipt,'$.header.media') ORDER BY id LIMIT 256")
        .bind(operation).bind(i64::from(first)).bind(end as i64)
}

pub(in super::super) fn incomplete(operation: Uuid, media: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storage_repairs r JOIN storage_fragments f ON f.id=r.id WHERE r.operation=? AND f.media_id=? AND r.state='writing')")
        .bind(operation).bind(media)
}

pub(in super::super) fn reserve(
    operation: Uuid,
    media: Uuid,
    cluster: Uuid,
    bytes: i64,
) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_reservations(operation,media_id,cluster_id,bytes) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE bytes=bytes+VALUES(bytes),complete=FALSE")
        .bind(operation).bind(media).bind(cluster).bind(bytes)
}

pub(in super::super) fn restart(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_repairs SET state='queued' WHERE id=? AND state='healthy'").bind(id)
}

pub(in super::super) fn restart_task(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='queued',execution_token=NULL WHERE id=? AND state IN ('completed','cancelled','failed')").bind(id)
}

pub(in super::super) fn operation(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT operation FROM storage_repairs WHERE id=?").bind(id)
}

pub(in super::super) fn garbage(operation: Uuid, after: Uuid) -> Rows<'static, Row> {
    sqlx::query_as("SELECT original_receipt,candidate,state,original_removed,candidate_removed,protected_until,collecting FROM storage_repairs WHERE operation=? AND ((state='installed' AND NOT original_removed AND id>?) OR (state='writing' AND NOT candidate_removed AND candidate_id>?)) ORDER BY id LIMIT 64")
        .bind(operation).bind(after).bind(after)
}

pub(in super::super) fn removed_original(operation: Uuid, id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_repairs SET original_removed=TRUE WHERE operation=? AND id=?")
        .bind(operation)
        .bind(id)
}

pub(in super::super) fn removed_candidate(operation: Uuid, id: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE storage_repairs SET candidate_removed=TRUE WHERE operation=? AND candidate_id=?",
    )
    .bind(operation)
    .bind(id)
}

pub(in super::super) fn protect(id: Uuid, until: i64) -> Statement<'static> {
    sqlx::query("UPDATE storage_repairs SET protected_until=CASE WHEN protected_until>? THEN protected_until ELSE ? END WHERE id=? AND NOT collecting")
        .bind(until).bind(until).bind(id)
}

pub(in super::super) fn live_garbage(operation: Uuid, after: Uuid, now: i64) -> Rows<'static, Row> {
    sqlx::query_as("SELECT original_receipt,candidate,state,original_removed,candidate_removed,protected_until,collecting FROM storage_repairs WHERE operation=? AND id>? AND state='installed' AND NOT original_removed AND protected_until<=? ORDER BY id LIMIT 64")
        .bind(operation).bind(after).bind(now)
}

pub(in super::super) fn collecting(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_repairs SET collecting=TRUE WHERE id=? AND state='installed' AND NOT original_removed").bind(id)
}

pub(in super::super) fn handoff(operation: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM storage_journal_sources WHERE operation=? AND published_revision<revision)").bind(operation)
}

#[cfg(test)]
pub(in super::super) fn test_expire(operation: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_repairs SET protected_until=0 WHERE operation=?").bind(operation)
}
