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

pub(in super::super) async fn begin(backend: &Backend) -> Result<Tx<'_>> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    let mode: String = sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton")
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        matches!(mode.as_str(), "single_control" | "cluster"),
        Error::NodeInactive
    );
    // Topology changes share one lock; payload writes use their own records.
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

pub(in super::super) fn announce(node: Uuid, value: &Announcement) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_devices(media_id,node_id,observed_at,announcement) VALUES($1,$2,$3,$4) ON CONFLICT(media_id) DO UPDATE SET node_id=excluded.node_id,observed_at=excluded.observed_at,announcement=excluded.announcement")
        .bind(value.media.media_id).bind(node).bind(chrono::Utc::now().timestamp_micros()).bind(Json(value))
}

pub(in super::super) fn member_node(node: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM storage_devices WHERE node_id=$1 AND cluster_id IS NOT NULL)",
    )
    .bind(node)
}
