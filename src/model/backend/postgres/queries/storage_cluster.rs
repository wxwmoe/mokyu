use super::super::Backend;
use super::{Rows, Scalar, Statement};
use crate::{
    authorization::Principal,
    cluster::NodeState,
    content::codec::Key,
    model::{Error, session::Run, storage_cluster::*},
};
use anyhow::{Result, ensure};
use sqlx::types::Json;
use uuid::Uuid;

pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, sqlx::Postgres>;
type ClusterRow = (Uuid, String, i64, Json<Policy>, Json<Manifest>);
type DeviceRow = (Uuid, Uuid, Option<Uuid>, i64, Json<Announcement>);

pub(in super::super) async fn begin_sample(backend: &Backend) -> Result<Tx<'_>> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    check_mode(&mut tx).await?;
    Ok(tx)
}

pub(in super::super) async fn begin_view(backend: &Backend) -> Result<Tx<'_>> {
    let mut tx = backend.pool.begin().await?;
    super::inventory_isolation().execute(&mut *tx).await?;
    backend.fence_control(&mut tx).await?;
    check_mode(&mut tx).await?;
    Ok(tx)
}

async fn check_mode(tx: &mut Tx<'_>) -> Result<()> {
    let mode: String = sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton")
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        matches!(mode.as_str(), "single_control" | "cluster"),
        Error::NodeInactive
    );
    Ok(())
}

pub(in super::super) async fn begin(backend: &Backend) -> Result<Tx<'_>> {
    let mut tx = begin_sample(backend).await?;
    // Topology changes share one lock; capacity samples lock their device row.
    sqlx::query("UPDATE storage_cluster_lock SET id=id WHERE id=1")
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}

pub(in super::super) async fn admin(tx: &mut Tx<'_>, principal: &Principal) -> Result<()> {
    principal.lock_admin(tx).await
}

pub(in super::super) async fn authenticate(
    tx: &mut Tx<'_>,
    node: &NodeState,
    key: &Key,
    run: Run,
) -> Result<()> {
    super::super::transactions::authenticate_rpc_node(&mut **tx, node, key, false).await?;
    super::super::transactions::require_node_run(&mut **tx, node, run, false).await?;
    Ok(())
}

pub(in super::super) async fn node_active(tx: &mut Tx<'_>, id: Uuid) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes n JOIN node_sessions s ON s.node_id=n.id WHERE n.id=$1 AND n.enabled AND s.expires_at>clock_timestamp() AND s.credential_generation=n.credential_generation)").bind(id).fetch_one(&mut **tx).await?)
}

pub(in super::super) fn deployment() -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT deployment_id FROM mokyu_meta WHERE singleton")
}

pub(in super::super) async fn targets(
    _backend: &Backend,
    tx: &mut Tx<'_>,
    cluster: Option<Uuid>,
    media: Option<Uuid>,
    locked: bool,
) -> Result<Vec<Target>> {
    type Row = (
        Uuid,
        Uuid,
        i64,
        Json<Announcement>,
        bool,
        i64,
        Option<Vec<u8>>,
        Option<Uuid>,
        Option<Uuid>,
        i64,
    );

    let rows: Vec<Row> = sqlx::query_as(if locked { "SELECT d.media_id,d.node_id,d.observed_at,d.announcement,n.enabled,n.credential_generation,n.credential_encrypted,s.boot_id,s.fence,COALESCE(EXTRACT(EPOCH FROM s.expires_at)*1000000,0)::bigint FROM storage_devices d JOIN nodes n ON n.id=d.node_id AND n.role='storage' JOIN node_sessions s ON s.node_id=n.id AND s.credential_generation=n.credential_generation WHERE d.cluster_id IS NOT DISTINCT FROM $1 AND ($2::uuid IS NULL OR d.media_id=$2) ORDER BY d.media_id LIMIT 257 FOR SHARE OF n,s,d" } else { "SELECT d.media_id,d.node_id,d.observed_at,d.announcement,n.enabled,n.credential_generation,n.credential_encrypted,s.boot_id,s.fence,COALESCE(EXTRACT(EPOCH FROM s.expires_at)*1000000,0)::bigint FROM storage_devices d JOIN nodes n ON n.id=d.node_id AND n.role='storage' LEFT JOIN node_sessions s ON s.node_id=n.id AND s.credential_generation=n.credential_generation WHERE d.cluster_id IS NOT DISTINCT FROM $1 AND ($2::uuid IS NULL OR d.media_id=$2) ORDER BY d.media_id LIMIT 257" }).bind(cluster).bind(media).fetch_all(&mut **tx).await?;
    ensure!(
        rows.len() <= MEMBER_LIMIT,
        "storage cluster has too many devices"
    );
    Ok(rows
        .into_iter()
        .map(|row| {
            let run = row
                .7
                .zip(row.8)
                .filter(|_| row.4 && row.9 > chrono::Utc::now().timestamp_micros())
                .map(|(boot, fence)| (Run { boot, fence }, row.9));
            Target {
                device: Device {
                    media_id: row.0,
                    node_id: row.1,
                    cluster_id: cluster,
                    observed_at: row.2,
                    announcement: row.3.0,
                },
                run: run.map(|(run, _)| run),
                expires: run.map_or(0, |(_, expires)| expires),
                generation: row.5,
                credential: row.6,
            }
        })
        .collect())
}

pub(in super::super) fn clusters() -> Rows<'static, ClusterRow> {
    sqlx::query_as(
        "SELECT id,name,revision,policy,manifest FROM storage_clusters ORDER BY name,id LIMIT 257",
    )
}

pub(in super::super) fn cluster(id: Uuid) -> Rows<'static, ClusterRow> {
    sqlx::query_as("SELECT id,name,revision,policy,manifest FROM storage_clusters WHERE id=$1")
        .bind(id)
}

pub(in super::super) fn devices() -> Rows<'static, DeviceRow> {
    sqlx::query_as(
        "SELECT media_id,node_id,cluster_id,observed_at,announcement FROM storage_devices ORDER BY media_id LIMIT 1025",
    )
}

pub(in super::super) fn device(id: Uuid) -> Rows<'static, DeviceRow> {
    sqlx::query_as("SELECT media_id,node_id,cluster_id,observed_at,announcement FROM storage_devices WHERE media_id=$1").bind(id)
}

pub(in super::super) fn device_for_update(id: Uuid) -> Rows<'static, DeviceRow> {
    sqlx::query_as("SELECT media_id,node_id,cluster_id,observed_at,announcement FROM storage_devices WHERE media_id=$1 FOR UPDATE").bind(id)
}

pub(in super::super) fn node_media(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT media_id FROM storage_devices WHERE node_id=$1").bind(id)
}

pub(in super::super) fn insert(value: &Cluster) -> Statement<'_> {
    sqlx::query(
        "INSERT INTO storage_clusters(id,name,revision,policy,manifest) VALUES($1,$2,$3,$4,$5)",
    )
    .bind(value.id)
    .bind(&value.name)
    .bind(value.revision)
    .bind(Json(&value.policy))
    .bind(Json(&value.manifest))
}

pub(in super::super) fn save(value: &Cluster) -> Statement<'_> {
    sqlx::query("UPDATE storage_clusters SET name=$1,revision=$2,policy=$3,manifest=$4 WHERE id=$5 AND revision=$6")
        .bind(&value.name).bind(value.revision).bind(Json(&value.policy)).bind(Json(&value.manifest)).bind(value.id).bind(value.revision-1)
}

pub(in super::super) fn history<'a>(value: &'a Cluster, hash: &'a str) -> Statement<'a> {
    sqlx::query("INSERT INTO storage_cluster_history(cluster_id,revision,manifest,digest) VALUES($1,$2,$3,$4)")
        .bind(value.id).bind(value.revision).bind(Json(&value.manifest)).bind(hash)
}

pub(in super::super) fn historical_hash(id: Uuid, revision: i64) -> Scalar<'static, String> {
    sqlx::query_scalar(
        "SELECT digest FROM storage_cluster_history WHERE cluster_id=$1 AND revision=$2",
    )
    .bind(id)
    .bind(revision)
}

pub(in super::super) fn histories(id: Uuid, after: i64) -> Scalar<'static, Json<Manifest>> {
    sqlx::query_scalar("SELECT manifest FROM storage_cluster_history WHERE cluster_id=$1 AND revision>$2 ORDER BY revision LIMIT 16").bind(id).bind(after)
}

pub(in super::super) fn assign(media: Uuid, cluster: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_devices SET cluster_id=$1 WHERE media_id=$2 AND cluster_id IS NULL")
        .bind(cluster)
        .bind(media)
}

pub(in super::super) fn revision(id: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT revision FROM storage_clusters WHERE id=$1").bind(id)
}

pub(in super::super) fn announce(node: Uuid, value: &Announcement, observed: i64) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_devices(media_id,node_id,observed_at,announcement) VALUES($1,$2,$3,$4) ON CONFLICT(media_id) DO UPDATE SET node_id=excluded.node_id,observed_at=excluded.observed_at,announcement=excluded.announcement")
        .bind(value.media.media_id).bind(node).bind(observed).bind(Json(value))
}

pub(in super::super) fn member_node(node: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM storage_devices WHERE node_id=$1 AND cluster_id IS NOT NULL)",
    )
    .bind(node)
}

pub(in super::super) fn recovered_history(
    catalog: Uuid,
    after: i64,
) -> Rows<'static, (Vec<u8>, Json<crate::model::storage_recovery::CatalogEntry>)> {
    sqlx::query_as("SELECT content_hash,content FROM storage_recovery_entries WHERE catalog=$1 AND category=0 AND position>$2 ORDER BY position,id LIMIT 16")
        .bind(catalog).bind(after)
}

pub(in super::super) fn recovered_count(catalog: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM storage_recovery_entries WHERE catalog=$1 AND category=0",
    )
    .bind(catalog)
}

pub(in super::super) fn protection(
    id: Uuid,
    online: Vec<Uuid>,
) -> Rows<'static, (i64, i64, i64, i64, Option<i64>)> {
    sqlx::query_as("WITH payload AS (
 SELECT p.operation,(p.intent#>>'{layout,data_shards}')::bigint AS k,(p.intent#>>'{layout,parity_shards}')::bigint AS m,(p.intent#>>'{layout,extra_copies}')::bigint+1 AS copies,(p.intent#>>'{total}')::bigint AS bytes,
 CASE WHEN (p.intent#>>'{total}')::bigint=0 THEN 1 ELSE (((p.intent#>>'{total}')::bigint-1) / (1073741824*(p.intent#>>'{layout,data_shards}')::bigint))+1 END AS sequences
 FROM storage_payloads p JOIN storage_objects o ON o.head=p.operation AND o.namespace_id=p.namespace_id AND o.payload_id=p.payload_id
 WHERE p.cluster_id=$1 AND p.state='published'
), good AS (
 SELECT f.operation,f.sequence,f.copy_index,f.shard_index FROM payload p JOIN storage_fragments f ON f.operation=p.operation
 LEFT JOIN storage_repairs r ON r.id=f.id
 WHERE f.root IS NOT NULL AND NOT f.deleted AND (r.state IS NULL OR r.state NOT IN ('queued','writing')) AND f.media_id IN (SELECT value::uuid FROM jsonb_array_elements_text($2::jsonb))
), stripe AS (
 SELECT operation,sequence,COUNT(DISTINCT shard_index) AS pieces FROM good GROUP BY operation,sequence
), readable AS (
 SELECT p.operation,COUNT(*) AS stripes FROM payload p JOIN stripe s ON s.operation=p.operation WHERE s.pieces>=p.k GROUP BY p.operation
), sets AS (
 SELECT operation,copy_index,COUNT(*) AS pieces FROM good GROUP BY operation,copy_index
), copies_ready AS (
 SELECT s.operation,COUNT(*) AS complete FROM sets s JOIN payload p ON p.operation=s.operation
 WHERE s.pieces=p.sequences*(p.k+p.m) GROUP BY s.operation
)
SELECT COUNT(*),CAST(COALESCE(SUM(CASE WHEN r.stripes=p.sequences THEN 1 ELSE 0 END),0) AS bigint),
CAST(COALESCE(SUM(CASE WHEN c.complete=p.copies THEN 1 ELSE 0 END),0) AS bigint),
CAST(COALESCE(SUM(CASE WHEN COALESCE(c.complete,0)<p.copies THEN p.bytes ELSE 0 END),0) AS bigint),
CAST(MIN(COALESCE(c.complete,0)) AS bigint) FROM payload p LEFT JOIN readable r ON r.operation=p.operation LEFT JOIN copies_ready c ON c.operation=p.operation").bind(id).bind(Json(online))
}
