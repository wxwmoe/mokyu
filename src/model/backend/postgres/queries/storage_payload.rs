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

pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, sqlx::Postgres>;
pub(in super::super) type PayloadRow = (Json<Intent>, String, i64, i64);
pub(in super::super) type FragmentRow = (Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>, bool);

pub(in super::super) async fn begin(backend: &Backend) -> Result<Tx<'_>> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    Ok(tx)
}

pub(in super::super) fn insert(value: &Intent) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_payloads(operation,namespace_id,payload_id,revision,cluster_id,intent) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING")
        .bind(value.operation).bind(value.namespace).bind(value.payload).bind(value.revision as i64).bind(value.cluster).bind(Json(value))
}

pub(in super::super) fn payload(operation: Uuid) -> Rows<'static, PayloadRow> {
    sqlx::query_as("SELECT intent,state,protected_until,write_until FROM storage_payloads WHERE operation=$1 FOR UPDATE").bind(operation)
}

pub(in super::super) fn manifest(cluster: Uuid, revision: u64) -> Scalar<'static, Json<Manifest>> {
    sqlx::query_scalar(
        "SELECT manifest FROM storage_cluster_history WHERE cluster_id=$1 AND revision=$2",
    )
    .bind(cluster)
    .bind(revision as i64)
}

pub(in super::super) fn current_manifest(cluster: Uuid) -> Scalar<'static, Json<Manifest>> {
    sqlx::query_scalar("SELECT manifest FROM storage_clusters WHERE id=$1 FOR SHARE").bind(cluster)
}

pub(in super::super) fn stage(
    header: &Header,
    bytes: Vec<u8>,
    required: bool,
) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_fragments(id,operation,media_id,sequence,copy_index,shard_index,header,file_bytes,required) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
        .bind(header.fragment).bind(header.operation).bind(header.media).bind(i64::from(header.sequence))
        .bind(i32::from(header.copy)).bind(i32::from(header.shard)).bind(bytes).bind(header.file_bytes() as i64).bind(required)
}

pub(in super::super) fn fragment(id: Uuid, operation: Uuid) -> Rows<'static, FragmentRow> {
    sqlx::query_as(
        "SELECT header,root,commitment,deleted FROM storage_fragments WHERE id=$1 AND operation=$2",
    )
    .bind(id)
    .bind(operation)
}

pub(in super::super) fn receipt(value: &Receipt, bytes: Vec<u8>) -> Statement<'static> {
    sqlx::query("UPDATE storage_fragments SET header=$1,root=$2,commitment=$3 WHERE id=$4 AND operation=$5 AND root IS NULL AND NOT deleted")
        .bind(bytes).bind(value.root.to_vec()).bind(value.commitment.to_vec()).bind(value.header.fragment).bind(value.header.operation)
}

pub(in super::super) fn receipt_peer(header: &Header) -> Rows<'static, FragmentRow> {
    sqlx::query_as("SELECT header,root,commitment,deleted FROM storage_fragments WHERE operation=$1 AND sequence=$2 AND shard_index=$3 AND root IS NOT NULL AND NOT deleted ORDER BY copy_index LIMIT 1 FOR SHARE")
        .bind(header.operation).bind(i64::from(header.sequence)).bind(i32::from(header.shard))
}

pub(in super::super) fn complete(operation: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM storage_fragments WHERE operation=$1 AND required AND root IS NOT NULL AND NOT deleted").bind(operation)
}

pub(in super::super) fn publish(operation: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE storage_payloads SET state='published' WHERE operation=$1 AND state='prepared'",
    )
    .bind(operation)
}

pub(in super::super) fn protect(operation: Uuid, until: i64) -> Statement<'static> {
    sqlx::query(
        "UPDATE storage_payloads SET protected_until=$1 WHERE operation=$2 AND protected_until<$3",
    )
    .bind(until)
    .bind(operation)
    .bind(until)
}

pub(in super::super) fn range(operation: Uuid, first: u32, end: u64) -> Rows<'static, FragmentRow> {
    sqlx::query_as("SELECT header,root,commitment,deleted FROM storage_fragments WHERE operation=$1 AND sequence>=$2 AND sequence<$3 AND NOT deleted ORDER BY sequence,copy_index,shard_index LIMIT 257")
        .bind(operation).bind(i64::from(first)).bind(end as i64)
}

pub(in super::super) fn retire(operation: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_payloads SET state='retired' WHERE operation=$1").bind(operation)
}

pub(in super::super) fn garbage(operation: Uuid, after: Uuid) -> Rows<'static, FragmentRow> {
    sqlx::query_as("SELECT header,root,commitment,deleted FROM storage_fragments WHERE operation=$1 AND id>$2 AND NOT deleted ORDER BY id LIMIT 256").bind(operation).bind(after)
}

pub(in super::super) fn removed(operation: Uuid, id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_fragments SET deleted=TRUE WHERE operation=$1 AND id=$2")
        .bind(operation)
        .bind(id)
}

pub(in super::super) fn renew(operation: Uuid, until: i64) -> Statement<'static> {
    sqlx::query("UPDATE storage_payloads SET write_until=$1 WHERE operation=$2 AND write_until<$3")
        .bind(until)
        .bind(operation)
        .bind(until)
}

#[cfg(test)]
pub(in super::super) async fn test_backend() -> Result<Backend> {
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(url.trim())
        .await?;
    let mut owner = pool.acquire().await?.detach();
    anyhow::ensure!(
        super::claim_runtime(true).fetch_one(&mut owner).await?,
        "isolated database is already owned"
    );
    super::super::migration::run(&mut owner).await?;
    sqlx::query("UPDATE mokyu_meta SET deployment_mode='single_control'")
        .execute(&mut owner)
        .await?;
    let epoch = super::claim_control_epoch()
        .fetch_optional(&mut owner)
        .await?;
    let mut backend = Backend::new(pool, owner, true);
    backend.control_epoch = epoch;
    Ok(backend)
}
