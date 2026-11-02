use super::super::{
    Backend,
    node_registry::{Lease, Node},
};
use super::{Rows, Scalar, Statement};
use crate::model::Error;
use anyhow::Result;
use serde_json::Value;
use sqlx::types::Json;
use uuid::Uuid;

pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, sqlx::Sqlite>;

pub(in super::super) fn epoch(backend: &Backend) -> Result<i64> {
    backend
        .control_epoch
        .ok_or_else(|| Error::NodeInactive.into())
}

pub(in super::super) async fn begin(backend: &Backend) -> Result<Tx<'_>> {
    epoch(backend)?;
    let mut tx = backend.write.begin_with(super::BEGIN_WRITE).await?;
    backend.fence_control(&mut tx).await?;
    Ok(tx)
}

pub(in super::super) fn node(id: Uuid) -> Rows<'static, Node> {
    sqlx::query_as("SELECT n.id,n.role,n.enabled,n.credential_generation,n.credential_encrypted,n.run_state,(SELECT deployment_id FROM mokyu_meta WHERE id=1) AS deployment_id FROM nodes n WHERE n.id=?").bind(id)
}

pub(in super::super) fn named(name: &str) -> Rows<'_, Node> {
    sqlx::query_as("SELECT n.id,n.role,n.enabled,n.credential_generation,n.credential_encrypted,n.run_state,(SELECT deployment_id FROM mokyu_meta WHERE id=1) AS deployment_id FROM nodes n WHERE n.name=?").bind(name)
}

pub(in super::super) fn insert<'a>(
    id: Uuid,
    name: &'a str,
    role: &'a str,
    encrypted: &'a [u8],
) -> Statement<'a> {
    sqlx::query("INSERT INTO nodes(id,name,role,enabled,credential_generation,credential_encrypted) VALUES(?,?,?,TRUE,1,?)").bind(id).bind(name).bind(role).bind(encrypted)
}

pub(in super::super) fn rotate(id: Uuid, generation: i64, encrypted: &[u8]) -> Statement<'_> {
    sqlx::query("UPDATE nodes SET credential_generation=?,credential_encrypted=?,enabled=TRUE,run_state=NULL,report=NULL,last_seen_at=NULL WHERE id=?").bind(generation).bind(encrypted).bind(id)
}

pub(in super::super) fn release(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE nodes SET enabled=FALSE,credential_encrypted=NULL,run_state=NULL,report=NULL,last_seen_at=NULL WHERE id=?").bind(id)
}

pub(in super::super) fn seen(id: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE nodes SET last_seen_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?",
    )
    .bind(id)
}

pub(in super::super) fn known(node: Uuid, boot: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM node_boots WHERE node_id=? AND boot_id=?)")
        .bind(node)
        .bind(boot)
}

pub(in super::super) fn boot_count(node: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_boots WHERE node_id=?").bind(node)
}

pub(in super::super) fn remember(node: Uuid, boot: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO node_boots(node_id,boot_id) VALUES(?,?)")
        .bind(node)
        .bind(boot)
}

pub(in super::super) fn forget_boots(node: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM node_boots WHERE node_id=?").bind(node)
}

pub(in super::super) fn clear_report(node: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE nodes SET report=NULL WHERE id=?").bind(node)
}

pub(in super::super) fn save(node: Uuid, lease: &Lease) -> Statement<'_> {
    sqlx::query("UPDATE nodes SET run_state=?,last_seen_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?").bind(Json(lease)).bind(node)
}

pub(in super::super) fn report(node: Uuid) -> Scalar<'static, Option<Value>> {
    sqlx::query_scalar("SELECT report FROM nodes WHERE id=?").bind(node)
}

pub(in super::super) fn save_report<'a>(
    node: Uuid,
    lease: &'a Lease,
    report: &'a Value,
) -> Statement<'a> {
    sqlx::query("UPDATE nodes SET report=?,run_state=?,last_seen_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?")
        .bind(Json(report)).bind(Json(lease)).bind(node)
}

pub(in super::super) fn deployment() -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT deployment_id FROM mokyu_meta WHERE id=1")
}

pub(in super::super) fn embedded() -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM nodes WHERE role='control' AND enabled LIMIT 1")
}

pub(in super::super) fn mode() -> Rows<'static, (bool, String, i64)> {
    sqlx::query_as(
        "SELECT maintenance,deployment_mode,deployment_mode_revision FROM mokyu_meta WHERE id=1",
    )
}

pub(in super::super) fn settings() -> Rows<'static, (String, Value, i64)> {
    sqlx::query_as(
        "SELECT section,CASE WHEN section='io' THEN json_remove(value,'$.sqlite_synchronous') ELSE value END,revision FROM runtime_settings WHERE section IN ('requests','uploads','metrics','maintenance','io') ORDER BY section",
    )
}

pub(in super::super) fn has_registered_peers() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE role<>'control' AND enabled)")
}
