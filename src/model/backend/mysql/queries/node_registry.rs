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

pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, sqlx::MySql>;

pub(in super::super) fn epoch(backend: &Backend) -> Result<i64> {
    backend
        .control_epoch
        .map(|(epoch, _)| epoch)
        .ok_or_else(|| Error::NodeInactive.into())
}

pub(in super::super) async fn begin(backend: &Backend) -> Result<Tx<'_>> {
    membership(backend, None, false).await
}

pub(in super::super) async fn membership(
    backend: &Backend,
    handoff: Option<i64>,
    release: bool,
) -> Result<Tx<'_>> {
    epoch(backend)?;
    let mut tx = backend.pool.begin().await?;
    if release {
        super::guard_stage_mode().fetch_one(&mut *tx).await?;
    } else if let Some(revision) = handoff {
        hand_off(revision).execute(&mut *tx).await?;
    }
    backend.fence_control(&mut tx).await?;
    Ok(tx)
}

pub(in super::super) fn node(id: Uuid) -> Rows<'static, Node> {
    sqlx::query_as("SELECT n.id,n.role,n.enabled,n.credential_generation,n.credential_encrypted,n.run_state,(SELECT deployment_id FROM mokyu_meta WHERE id=1) AS deployment_id FROM nodes n WHERE n.id=? FOR UPDATE").bind(id)
}

pub(in super::super) fn named(name: &str) -> Rows<'_, Node> {
    sqlx::query_as("SELECT n.id,n.role,n.enabled,n.credential_generation,n.credential_encrypted,n.run_state,(SELECT deployment_id FROM mokyu_meta WHERE id=1) AS deployment_id FROM nodes n WHERE n.name=? FOR UPDATE").bind(name)
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
    sqlx::query("UPDATE nodes SET last_seen_at=UTC_TIMESTAMP(6) WHERE id=?").bind(id)
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
    sqlx::query("UPDATE nodes SET run_state=?,last_seen_at=UTC_TIMESTAMP(6) WHERE id=?")
        .bind(Json(lease))
        .bind(node)
}

pub(in super::super) fn report(node: Uuid) -> Scalar<'static, Option<Value>> {
    sqlx::query_scalar("SELECT report FROM nodes WHERE id=?").bind(node)
}

pub(in super::super) fn save_report<'a>(
    node: Uuid,
    lease: &'a Lease,
    report: &'a Value,
) -> Statement<'a> {
    sqlx::query("UPDATE nodes SET report=?,run_state=?,last_seen_at=UTC_TIMESTAMP(6) WHERE id=?")
        .bind(Json(report))
        .bind(Json(lease))
        .bind(node)
}

pub(in super::super) fn deployment() -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT deployment_id FROM mokyu_meta WHERE id=1")
}

pub(in super::super) fn embedded() -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM nodes WHERE role='control' AND enabled LIMIT 1")
}

pub(in super::super) fn mode() -> Rows<'static, (bool, String, i64, bool)> {
    sqlx::query_as(
        "SELECT maintenance,deployment_mode,deployment_mode_revision,(remote_work AND NOT maintenance AND (EXISTS(SELECT 1 FROM raw_processing w JOIN raw_sources r ON r.id=w.source_id WHERE r.medium='shared_s3' AND NOT r.pending_upload AND w.next_attempt_at<=UTC_TIMESTAMP(6) AND NOT EXISTS(SELECT 1 FROM raw_uploads u WHERE u.source_id=r.id) AND (w.token IS NULL OR w.lease_expires_at IS NULL OR w.lease_expires_at<UTC_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM tasks j WHERE j.id=w.token AND j.state IN ('queued','running','paused','failed'))) OR EXISTS(SELECT 1 FROM tasks t WHERE (t.state='queued' OR t.state='paused' AND EXISTS(SELECT 1 FROM redundancy_links l WHERE l.id=t.id AND l.state='idle' AND (l.next_reconcile_at<=UTC_TIMESTAMP(6) OR EXISTS(SELECT 1 FROM transfer_changes c WHERE c.task_id=l.id))) OR t.state='failed' AND t.updated_at<TIMESTAMPADD(SECOND,-30,UTC_TIMESTAMP(6)) AND EXISTS(SELECT 1 FROM redundancy_links l WHERE l.id=t.id AND l.state<>'paused')) AND (t.task_type IN ('pack','unpack') OR (t.task_type='integrity' AND (JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation'))='external_test' OR JSON_EXTRACT(t.detail,'$.source_operation') IS NOT NULL OR (JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.mode')) IN ('head','full') AND JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.phase')) IN ('raw','chunks')))) OR ((t.task_type='storage_migration' AND ((JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation')) IN ('tiering','replication') AND (COALESCE(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation')),'')<>'replication' OR COALESCE(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.external'))='true',FALSE) OR COALESCE(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.phase')),'')='verify')) OR JSON_EXTRACT(t.detail,'$.job') IS NOT NULL) OR t.task_type='upload' AND JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation')) IN ('deduplicate','cdc'))))))) OR (remote_work AND EXISTS(SELECT 1 FROM tasks WHERE task_type='sweep' AND state='queued')) FROM mokyu_meta WHERE id=1",
    )
}

pub(in super::super) fn settings() -> Rows<'static, (String, Value, i64)> {
    sqlx::query_as(
        "SELECT section,value,revision FROM runtime_settings WHERE section IN ('requests','uploads','metrics','maintenance','io') ORDER BY section",
    )
}

pub(in super::super) fn has_registered_peers() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE role<>'control' AND enabled)")
}

pub(in super::super) fn hand_off(revision: i64) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET remote_work=TRUE WHERE id=1 AND deployment_mode='single_control' AND deployment_mode_revision=? AND NOT maintenance AND NOT remote_work").bind(revision)
}

pub(in super::super) fn release_handoff() -> Statement<'static> {
    sqlx::query(
        "UPDATE mokyu_meta SET remote_work=FALSE WHERE id=1 AND deployment_mode='single_control' AND remote_work AND NOT EXISTS(SELECT 1 FROM nodes WHERE enabled AND role IN ('worker','node'))",
    )
}

pub(in super::super) fn pending_files(node: Uuid) -> Rows<'static, (i64, i64, i64, i64, i64)> {
    sqlx::query_as("SELECT (SELECT count(*) FROM pending_uploads WHERE node_id=?),(SELECT count(*) FROM raw_uploads WHERE node_id=?),(SELECT count(*) FROM native_parts p JOIN uploads u ON u.id=p.upload_id WHERE p.node_id=? AND u.state IN ('active','completing')),(SELECT count(*) FROM node_writes WHERE node_id=? AND (phase='preparing' OR phase='active' AND expires_at>UTC_TIMESTAMP(6))),(SELECT count(*) FROM node_reads WHERE node_id=? AND expires_at>UTC_TIMESTAMP(6))").bind(node).bind(node).bind(node).bind(node).bind(node)
}

pub(in super::super) fn active_grants() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM node_reads WHERE expires_at>UTC_TIMESTAMP(6)) OR EXISTS(SELECT 1 FROM node_writes WHERE phase='preparing' OR phase='active' AND expires_at>UTC_TIMESTAMP(6)) OR EXISTS(SELECT 1 FROM storage_permits WHERE expires_at>UTC_TIMESTAMP(6))",
    )
}
