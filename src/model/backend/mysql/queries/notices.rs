use super::super::Backend;
use super::{Rows, Scalar, Statement};
use crate::model::notification::Consumer;
use anyhow::Result;
use uuid::Uuid;

pub(in super::super) fn reader(b: &Backend) -> &sqlx::Pool<sqlx::MySql> {
    &b.pool
}

pub(in super::super) async fn begin(b: &Backend) -> Result<sqlx::Transaction<'_, sqlx::MySql>> {
    super::node_registry::begin(b).await
}

pub(in super::super) async fn membership(tx: &mut sqlx::MySqlConnection) -> Result<()> {
    sqlx::query("SELECT id FROM notice_membership WHERE id=1 FOR UPDATE")
        .fetch_one(tx)
        .await?;
    Ok(())
}

pub(in super::super) fn activate(c: Consumer, role: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO notification_consumers(node_id,generation,role,expires_at) VALUES(?,?,?,UTC_TIMESTAMP(6)+INTERVAL 90 SECOND) ON DUPLICATE KEY UPDATE generation=VALUES(generation),role=VALUES(role),expires_at=VALUES(expires_at)").bind(c.node).bind(c.generation).bind(role)
}
pub(in super::super) fn retire_previous(c: Consumer) -> Statement<'static> {
    sqlx::query("DELETE FROM node_notices WHERE node_id=? AND generation<>?")
        .bind(c.node)
        .bind(c.generation)
}
pub(in super::super) fn renew(c: Consumer) -> Statement<'static> {
    sqlx::query("UPDATE notification_consumers SET expires_at=UTC_TIMESTAMP(6)+INTERVAL 90 SECOND WHERE node_id=? AND generation=?").bind(c.node).bind(c.generation)
}
pub(in super::super) fn remove(node: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM notification_consumers WHERE node_id=?").bind(node)
}
pub(in super::super) fn pending(nodes: &str) -> Rows<'_, (Uuid, Uuid, Uuid, String, Uuid, String)> {
    sqlx::query_as("SELECT t.node_id,t.generation,t.event_id,t.scope,t.resource_id,CAST(t.object_key AS CHAR CHARACTER SET utf8mb4) FROM node_notices t JOIN notification_consumers c ON c.node_id=t.node_id AND c.generation=t.generation AND c.expires_at>UTC_TIMESTAMP(6) WHERE t.node_id IN (SELECT UNHEX(j.id) FROM JSON_TABLE(?, '$[*]' COLUMNS(id CHAR(32) PATH '$')) j) AND (t.retry_at IS NULL OR t.retry_at<=UTC_TIMESTAMP(6)) ORDER BY (t.retry_at IS NOT NULL),t.retry_at,t.slot,t.node_id LIMIT 128").bind(nodes)
}
pub(in super::super) fn claimed(ids: &str) -> Statement<'_> {
    sqlx::query("UPDATE node_notices SET retry_at=UTC_TIMESTAMP(6)+INTERVAL 2 SECOND WHERE event_id IN (SELECT UNHEX(j.id) FROM JSON_TABLE(?, '$[*]' COLUMNS(id CHAR(32) PATH '$')) j)").bind(ids)
}
pub(in super::super) fn acknowledge(c: Consumer, ids: &str) -> Statement<'_> {
    sqlx::query("DELETE FROM node_notices WHERE node_id=? AND generation=? AND retry_at IS NOT NULL AND event_id IN (SELECT UNHEX(j.id) FROM JSON_TABLE(?, '$[*]' COLUMNS(id CHAR(32) PATH '$')) j) AND EXISTS(SELECT 1 FROM notification_consumers c JOIN nodes n ON n.id=c.node_id WHERE n.enabled AND c.node_id=node_notices.node_id AND c.generation=node_notices.generation AND c.expires_at>UTC_TIMESTAMP(6))").bind(c.node).bind(c.generation).bind(ids)
}
pub(in super::super) fn has_expired() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM notification_consumers WHERE expires_at<=UTC_TIMESTAMP(6) AND role<>'control')",
    )
}
pub(in super::super) fn prune() -> Statement<'static> {
    sqlx::query(
        "DELETE FROM notification_consumers WHERE expires_at<=UTC_TIMESTAMP(6) AND role<>'control' LIMIT 32",
    )
}
