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
pub(in super::super) async fn acknowledge(
    db: &mut sqlx::MySqlConnection,
    c: Consumer,
    ids: &str,
) -> Result<u64> {
    // Producers lock their node before writing notices, so receipts must take the same order.
    if sqlx::query_scalar::<_, Uuid>("SELECT id FROM nodes WHERE id=? AND enabled LOCK IN SHARE MODE")
        .bind(c.node).fetch_optional(&mut *db).await?.is_none()
        || sqlx::query_scalar::<_, Uuid>("SELECT node_id FROM notification_consumers WHERE node_id=? AND generation=? AND expires_at>UTC_TIMESTAMP(6) LOCK IN SHARE MODE")
            .bind(c.node).bind(c.generation).fetch_optional(&mut *db).await?.is_none()
    {
        return Ok(0);
    }
    // A producer can update several slots in any order. Busy slots are delivered again.
    let slots: Vec<i16> = sqlx::query_scalar(
        "SELECT seen.slot FROM JSON_TABLE(?, '$[*]' COLUMNS(id CHAR(32) PATH '$')) j
         STRAIGHT_JOIN node_notices seen FORCE INDEX (event_id) ON seen.event_id=UNHEX(j.id)
         WHERE seen.node_id=? AND seen.event_id=(
             SELECT n.event_id FROM node_notices n FORCE INDEX (PRIMARY)
             WHERE n.node_id=seen.node_id AND n.slot=seen.slot AND n.generation=?
               AND n.retry_at IS NOT NULL FOR UPDATE SKIP LOCKED)",
    )
    .bind(ids)
    .bind(c.node)
    .bind(c.generation)
    .fetch_all(&mut *db)
    .await?;
    if slots.is_empty() {
        return Ok(0);
    }
    Ok(sqlx::query(
        "DELETE n FROM JSON_TABLE(?, '$[*]' COLUMNS(slot SMALLINT PATH '$')) j
         STRAIGHT_JOIN node_notices n FORCE INDEX (PRIMARY) ON n.node_id=? AND n.slot=j.slot
         WHERE n.generation=? AND EXISTS(SELECT 1 FROM notification_consumers c
             WHERE c.node_id=n.node_id AND c.generation=n.generation AND c.expires_at>UTC_TIMESTAMP(6))",
    )
    .bind(sqlx::types::Json(slots))
    .bind(c.node)
    .bind(c.generation)
    .execute(db)
    .await?
    .rows_affected())
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
