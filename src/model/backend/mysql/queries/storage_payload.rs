use super::super::Backend;
use super::{Rows, Scalar, Statement};
use crate::content::fragment::Header;
use crate::model::{
    storage_cluster::Manifest,
    storage_payload::{Intent, Receipt},
};
use anyhow::Result;
use sqlx::types::Json;
use uuid::Uuid;

pub(in super::super) fn collect_candidates(namespace: Uuid, now: i64) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT p.operation FROM storage_payloads p WHERE p.namespace_id=? AND p.collect_after<=? AND JSON_UNQUOTE(JSON_EXTRACT(p.intent,'$.area'))='blocks' AND ((p.state='retired' AND p.protected_until<=? AND p.write_until<=? AND NOT EXISTS(SELECT 1 FROM storage_journal_sources j WHERE j.operation=p.operation) AND (EXISTS(SELECT 1 FROM storage_fragments f WHERE f.operation=p.operation AND NOT f.deleted) OR EXISTS(SELECT 1 FROM storage_repairs r WHERE r.operation=p.operation AND ((r.state='installed' AND NOT r.original_removed) OR (r.state='writing' AND NOT r.candidate_removed))))) OR (p.state='published' AND EXISTS(SELECT 1 FROM storage_repairs r WHERE r.operation=p.operation AND r.state='installed' AND NOT r.original_removed AND r.protected_until<=?))) ORDER BY p.collect_after,p.operation LIMIT 64")
        .bind(namespace).bind(now).bind(now).bind(now).bind(now)
}

pub(in super::super) fn claim_collection(
    operation: Uuid,
    now: i64,
    until: i64,
) -> Statement<'static> {
    sqlx::query(
        "UPDATE storage_payloads SET collect_after=? WHERE operation=? AND collect_after<=?",
    )
    .bind(until)
    .bind(operation)
    .bind(now)
}

pub(in super::super) fn missing_sequence(operation: Uuid) -> Scalar<'static, Option<i64>> {
    sqlx::query_scalar("SELECT MIN(sequence) FROM storage_fragments WHERE operation=? AND root IS NULL AND NOT deleted").bind(operation)
}

pub(in super::super) fn enqueue_fill(operation: Uuid, namespace: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO tasks(id,task_type,state,detail,created_by_label,source) VALUES(?,'storage_migration','queued',?,'Scheduler','system') ON DUPLICATE KEY UPDATE id=id")
        .bind(operation)
        .bind(Json(serde_json::json!({"operation":"native","storage_id":namespace})))
}

pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, sqlx::MySql>;
pub(in super::super) type PayloadRow = (Json<Intent>, String, i64, i64);
pub(in super::super) type FragmentRow = (Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>, bool);

pub(in super::super) async fn begin(backend: &Backend) -> Result<Tx<'_>> {
    super::node_registry::begin(backend).await
}

pub(in super::super) fn insert(value: &Intent) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_payloads(operation,namespace_id,payload_id,revision,cluster_id,intent) VALUES(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE operation=operation")
        .bind(value.operation).bind(value.namespace).bind(value.payload).bind(value.revision as i64).bind(value.cluster).bind(Json(value))
}

pub(in super::super) fn payload(operation: Uuid) -> Rows<'static, PayloadRow> {
    sqlx::query_as("SELECT intent,state,protected_until,write_until FROM storage_payloads WHERE operation=? FOR UPDATE").bind(operation)
}

pub(in super::super) fn manifest(cluster: Uuid, revision: u64) -> Scalar<'static, Json<Manifest>> {
    sqlx::query_scalar(
        "SELECT manifest FROM storage_cluster_history WHERE cluster_id=? AND revision=?",
    )
    .bind(cluster)
    .bind(revision as i64)
}

pub(in super::super) fn current_manifest(cluster: Uuid) -> Scalar<'static, Json<Manifest>> {
    sqlx::query_scalar("SELECT manifest FROM storage_clusters WHERE id=? LOCK IN SHARE MODE")
        .bind(cluster)
}

pub(in super::super) fn stage(
    header: &Header,
    bytes: Vec<u8>,
    required: bool,
) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_fragments(id,operation,media_id,sequence,copy_index,shard_index,header,file_bytes,required) VALUES(?,?,?,?,?,?,?,?,?)")
        .bind(header.fragment).bind(header.operation).bind(header.media).bind(i64::from(header.sequence))
        .bind(i32::from(header.copy)).bind(i32::from(header.shard)).bind(bytes).bind(header.file_bytes() as i64).bind(required)
}

pub(in super::super) fn fragment(id: Uuid, operation: Uuid) -> Rows<'static, FragmentRow> {
    sqlx::query_as(
        "SELECT header,root,commitment,deleted FROM storage_fragments WHERE id=? AND operation=?",
    )
    .bind(id)
    .bind(operation)
}

pub(in super::super) fn receipt(value: &Receipt, bytes: Vec<u8>) -> Statement<'static> {
    sqlx::query("UPDATE storage_fragments SET header=?,root=?,commitment=? WHERE id=? AND operation=? AND root IS NULL AND NOT deleted")
        .bind(bytes).bind(value.root.to_vec()).bind(value.commitment.to_vec()).bind(value.header.fragment).bind(value.header.operation)
}

pub(in super::super) fn slot(header: &Header) -> Rows<'static, FragmentRow> {
    sqlx::query_as("SELECT header,root,commitment,deleted FROM storage_fragments WHERE operation=? AND sequence=? AND copy_index=? AND shard_index=? FOR UPDATE")
        .bind(header.operation).bind(i64::from(header.sequence)).bind(i32::from(header.copy)).bind(i32::from(header.shard))
}

pub(in super::super) fn receipt_peer(header: &Header) -> Rows<'static, FragmentRow> {
    sqlx::query_as("SELECT header,root,commitment,deleted FROM storage_fragments WHERE operation=? AND sequence=? AND shard_index=? AND root IS NOT NULL AND NOT deleted ORDER BY copy_index LIMIT 1 LOCK IN SHARE MODE")
        .bind(header.operation).bind(i64::from(header.sequence)).bind(i32::from(header.shard))
}

pub(in super::super) fn complete(operation: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM storage_fragments WHERE operation=? AND required AND root IS NOT NULL AND NOT deleted").bind(operation)
}

pub(in super::super) fn publish(operation: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE storage_payloads SET state='published' WHERE operation=? AND state='prepared'",
    )
    .bind(operation)
}

pub(in super::super) fn protect(operation: Uuid, until: i64) -> Statement<'static> {
    sqlx::query(
        "UPDATE storage_payloads SET protected_until=? WHERE operation=? AND protected_until<?",
    )
    .bind(until)
    .bind(operation)
    .bind(until)
}

pub(in super::super) fn range(operation: Uuid, first: u32, end: u64) -> Rows<'static, FragmentRow> {
    sqlx::query_as("SELECT header,root,commitment,deleted FROM storage_fragments WHERE operation=? AND sequence>=? AND sequence<? AND NOT deleted ORDER BY sequence,copy_index,shard_index LIMIT 257")
        .bind(operation).bind(i64::from(first)).bind(end as i64)
}

pub(in super::super) fn retire(operation: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_payloads SET state='retired' WHERE operation=?").bind(operation)
}

pub(in super::super) fn garbage(operation: Uuid, after: Uuid) -> Rows<'static, FragmentRow> {
    sqlx::query_as("SELECT header,root,commitment,deleted FROM storage_fragments WHERE operation=? AND id>? AND NOT deleted ORDER BY id LIMIT 128").bind(operation).bind(after)
}

pub(in super::super) fn removed(operation: Uuid, id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_fragments SET deleted=TRUE WHERE operation=? AND id=?")
        .bind(operation)
        .bind(id)
}

pub(in super::super) fn renew(operation: Uuid, until: i64) -> Statement<'static> {
    sqlx::query("UPDATE storage_payloads SET write_until=? WHERE operation=? AND write_until<?")
        .bind(until)
        .bind(operation)
        .bind(until)
}

#[cfg(test)]
pub(in super::super) async fn test_backend() -> Result<Backend> {
    let path = std::env::var("MOKYU_TEST_PAYLOAD_CONFIG")?;
    let (config, secrets, budget) = crate::config::Config::load(std::path::Path::new(&path))?;
    let mut backend = super::super::connection::open(&config, &secrets, &budget, "single").await?;
    {
        let mut owner = backend.owner.lock().await;
        sqlx::query("UPDATE mokyu_meta SET deployment_mode='single_control'")
            .execute(&mut *owner)
            .await?;
        super::claim_control_epoch().execute(&mut *owner).await?;
        backend.control_epoch = Some(
            super::current_control_epoch()
                .fetch_one(&mut *owner)
                .await?,
        );
    }
    Ok(backend)
}
