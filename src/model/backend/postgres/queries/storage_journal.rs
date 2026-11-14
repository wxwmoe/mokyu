use super::{Rows, Scalar, Statement};
use crate::model::journal::native::Capture;
use uuid::Uuid;

#[cfg(test)]
pub(in super::super) fn test_expire(operation: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_payloads SET protected_until=0,write_until=0 WHERE operation=$1")
        .bind(operation)
}

pub(in super::super) fn source(namespace: Uuid, hash: Vec<u8>, path: &str) -> Rows<'_, (Uuid,)> {
    sqlx::query_as("SELECT o.head FROM storage_objects o JOIN storage_payloads p ON p.operation=o.head WHERE o.namespace_id=$1 AND o.path_hash=$2 AND o.path=$3 AND p.state='published'")
        .bind(namespace).bind(hash).bind(path)
}

pub(in super::super) fn track(operation: Uuid, namespace: Uuid, path: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_journal_sources(operation,namespace_id,path,cursor_id) VALUES($1,$2,$3,$4) ON CONFLICT(operation) DO NOTHING")
        .bind(operation).bind(namespace).bind(path).bind(Uuid::nil())
}

pub(in super::super) fn dirty(operation: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE storage_journal_sources SET revision=revision+1,cursor_id=$1 WHERE operation=$2",
    )
    .bind(Uuid::nil())
    .bind(operation)
}

pub(in super::super) fn pending(storage: Uuid) -> Rows<'static, (Uuid, i64, Uuid, String)> {
    sqlx::query_as("SELECT operation,revision,cursor_id,path FROM storage_journal_sources WHERE namespace_id=$1 AND allocated_revision<revision ORDER BY operation LIMIT 1")
        .bind(storage)
}

pub(in super::super) fn fragments(
    operation: Uuid,
    after: Uuid,
) -> Rows<'static, super::storage_payload::FragmentRow> {
    sqlx::query_as("SELECT header,root,commitment,deleted FROM storage_fragments WHERE operation=$1 AND id>$2 AND root IS NOT NULL AND NOT deleted ORDER BY id LIMIT 254")
        .bind(operation).bind(after)
}

pub(in super::super) fn captured(
    storage: Uuid,
    capture: &Capture,
    file: Uuid,
) -> Statement<'static> {
    sqlx::query("UPDATE storage_journal_sources SET cursor_id=$1,allocated_revision=CASE WHEN $2 THEN revision ELSE allocated_revision END,file_id=$3 WHERE namespace_id=$4 AND operation=$5 AND revision=$6 AND cursor_id=$7 AND allocated_revision<revision")
        .bind(capture.next).bind(capture.complete).bind(file).bind(storage).bind(capture.operation).bind(capture.revision).bind(capture.after)
}

pub(in super::super) fn held(operation: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storage_journal_sources WHERE operation=$1)")
        .bind(operation)
}

pub(in super::super) fn identity(
    storage: Uuid,
) -> Rows<'static, (Uuid, sqlx::types::Json<serde_json::Value>, String)> {
    sqlx::query_as(
        "SELECT cluster_id,identity_marker,prefix FROM storages WHERE id=$1 AND type='mokyu'",
    )
    .bind(storage)
}
pub(in super::super) fn publication(
    id: Uuid,
) -> Rows<
    'static,
    (
        sqlx::types::Json<crate::model::storage_recovery::Proof>,
        bool,
    ),
> {
    sqlx::query_as("SELECT proof,settled FROM storage_journal_publications WHERE id=$1").bind(id)
}
pub(in super::super) fn latest(
    storage: Uuid,
) -> Rows<
    'static,
    (
        sqlx::types::Json<crate::model::storage_recovery::Proof>,
        bool,
    ),
> {
    sqlx::query_as("SELECT proof,settled FROM storage_journal_publications WHERE namespace_id=$1 ORDER BY revision DESC LIMIT 1").bind(storage)
}
pub(in super::super) fn prepare(
    proof: &crate::model::storage_recovery::Proof,
    manifest_revision: i64,
) -> Statement<'_> {
    let p = &proof.publication;
    sqlx::query("INSERT INTO storage_journal_publications(id,namespace_id,cluster_id,revision,manifest_revision,proof) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(p.id).bind(p.namespace).bind(p.cluster).bind(p.revision as i64).bind(manifest_revision).bind(sqlx::types::Json(proof))
}
pub(in super::super) fn history(
    storage: Uuid,
    after: u64,
    through: u64,
) -> Rows<
    'static,
    (
        sqlx::types::Json<crate::model::storage_recovery::Proof>,
        sqlx::types::Json<crate::model::storage_cluster::Manifest>,
    ),
> {
    sqlx::query_as("SELECT p.proof,h.manifest FROM storage_journal_publications p JOIN storage_cluster_history h ON h.cluster_id=p.cluster_id AND h.revision=p.manifest_revision WHERE p.namespace_id=$1 AND p.revision>$2 AND p.revision<=$3 ORDER BY p.revision LIMIT 32")
        .bind(storage).bind(after as i64).bind(through as i64)
}
pub(in super::super) fn evidence(storage: Uuid, media: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar(
        "SELECT revision FROM storage_journal_receipts WHERE namespace_id=$1 AND media_id=$2",
    )
    .bind(storage)
    .bind(media)
}
pub(in super::super) fn ensure_evidence(storage: Uuid, media: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_journal_receipts(namespace_id,media_id,revision) VALUES($1,$2,0) ON CONFLICT DO NOTHING").bind(storage).bind(media)
}
pub(in super::super) fn receipt(storage: Uuid, media: Uuid, revision: u64) -> Statement<'static> {
    sqlx::query("UPDATE storage_journal_receipts SET revision=$1 WHERE namespace_id=$2 AND media_id=$3 AND revision=$4")
        .bind(revision as i64).bind(storage).bind(media).bind(revision as i64 - 1)
}
pub(in super::super) fn settled(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_journal_publications SET settled=TRUE WHERE id=$1").bind(id)
}

pub(in super::super) fn fence(storage: Uuid, token: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT storage_id FROM journal_stores WHERE storage_id=$1 AND ((lease_token=$2 AND lease_until>clock_timestamp()) OR (snapshot_token=$2 AND snapshot_lease_until>clock_timestamp())) FOR UPDATE").bind(storage).bind(token)
}

pub(in super::super) fn source_revision(operation: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT revision FROM storage_journal_sources WHERE operation=$1")
        .bind(operation)
}

pub(in super::super) fn garbage(
    storage: Uuid,
    operation: Uuid,
) -> Rows<'static, crate::model::journal::snapshots::Garbage> {
    sqlx::query_as("SELECT storage_id,object_id,page_no,kind,file_path,etag,remote_version,byte_count FROM journal_snapshot_gc WHERE storage_id=$1 AND etag=$2 AND reclaim_at<=clock_timestamp()")
        .bind(storage).bind(format!("\"{operation}\""))
}

pub(in super::super) fn checkpoint(storage: Uuid) -> Rows<'static, (Uuid, String, String)> {
    sqlx::query_as("SELECT j.id,j.checkpoint_etag,j.index_etag FROM journal_stores s JOIN journal_snapshots j ON j.id=s.snapshot_id WHERE s.storage_id=$1 AND j.state='current'").bind(storage)
}
pub(in super::super) fn restore_receipt(
    storage: Uuid,
    media: Uuid,
    revision: u64,
) -> Statement<'static> {
    sqlx::query("UPDATE storage_journal_receipts SET revision=$1 WHERE namespace_id=$2 AND media_id=$3 AND revision<$4")
        .bind(revision as i64).bind(storage).bind(media).bind(revision as i64)
}

pub(in super::super) fn restore_identity(
    storage: Uuid,
    identity: &serde_json::Value,
) -> Statement<'_> {
    sqlx::query("UPDATE storages SET identity_marker=$1 WHERE id=$2 AND type='mokyu'")
        .bind(sqlx::types::Json(identity))
        .bind(storage)
}
pub(in super::super) fn protected_roots(
    storage: Uuid,
) -> Scalar<'static, sqlx::types::Json<crate::model::storage_recovery::Proof>> {
    sqlx::query_scalar("SELECT proof FROM storage_journal_publications WHERE namespace_id=$1 AND (NOT settled OR revision=(SELECT MAX(revision) FROM storage_journal_publications WHERE namespace_id=$2 AND settled)) ORDER BY revision DESC").bind(storage).bind(storage)
}

pub(in super::super) fn roots(
    storage: Uuid,
) -> Scalar<'static, sqlx::types::Json<crate::model::storage_recovery::Proof>> {
    sqlx::query_scalar("SELECT proof FROM storage_journal_publications WHERE namespace_id=$1 ORDER BY revision DESC LIMIT 2").bind(storage)
}

pub(in super::super) fn confirm_sources(file: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_journal_sources SET published_revision=allocated_revision WHERE file_id=$1 AND published_revision<allocated_revision").bind(file)
}
