use super::super::Backend;
use super::{Rows, Scalar, Statement};
use crate::model::notification::Consumer;
use anyhow::Result;
use uuid::Uuid;

pub(in super::super) fn reader(b: &Backend) -> &sqlx::Pool<sqlx::Postgres> {
    &b.pool
}

pub(in super::super) async fn begin(b: &Backend) -> Result<sqlx::Transaction<'_, sqlx::Postgres>> {
    let mut tx = b.pool.begin().await?;
    b.fence_control(&mut tx).await?;
    Ok(tx)
}

pub(in super::super) async fn membership(tx: &mut sqlx::PgConnection) -> Result<()> {
    super::notifications::membership().execute(tx).await?;
    Ok(())
}

pub(in super::super) fn activate(c: Consumer, role: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO notification_consumers(node_id,generation,role,expires_at) VALUES($1,$2,$3,clock_timestamp()+interval '90 seconds') ON CONFLICT (node_id) DO UPDATE SET generation=excluded.generation,role=excluded.role,expires_at=excluded.expires_at").bind(c.node).bind(c.generation).bind(role)
}
pub(in super::super) fn retire_previous(c: Consumer) -> Statement<'static> {
    sqlx::query("DELETE FROM node_notices WHERE node_id=$1 AND generation<>$2")
        .bind(c.node)
        .bind(c.generation)
}
pub(in super::super) fn renew(c: Consumer) -> Statement<'static> {
    sqlx::query("UPDATE notification_consumers SET expires_at=clock_timestamp()+interval '90 seconds' WHERE node_id=$1 AND generation=$2").bind(c.node).bind(c.generation)
}
pub(in super::super) fn pending(nodes: &str) -> Rows<'_, (Uuid, Uuid, Uuid, String, Uuid, String)> {
    sqlx::query_as("SELECT t.node_id,t.generation,t.event_id,t.scope,t.resource_id,t.object_key FROM node_notices t JOIN notification_consumers c ON c.node_id=t.node_id AND c.generation=t.generation AND c.expires_at>clock_timestamp() WHERE t.node_id IN (SELECT value::uuid FROM jsonb_array_elements_text(($1::text)::jsonb)) AND (t.retry_at IS NULL OR t.retry_at<=clock_timestamp()) ORDER BY (t.retry_at IS NOT NULL),t.retry_at,t.slot,t.node_id LIMIT 128").bind(nodes)
}
pub(in super::super) fn claimed(ids: &str) -> Statement<'_> {
    sqlx::query("UPDATE node_notices SET retry_at=clock_timestamp()+interval '2 seconds' WHERE event_id IN (SELECT value::uuid FROM jsonb_array_elements_text(($1::text)::jsonb))").bind(ids)
}
pub(in super::super) async fn acknowledge(
    db: &mut sqlx::PgConnection,
    c: Consumer,
    ids: &str,
) -> Result<u64> {
    Ok(sqlx::query("DELETE FROM node_notices WHERE node_id=$1 AND generation=$2 AND retry_at IS NOT NULL AND event_id IN (SELECT value::uuid FROM jsonb_array_elements_text(($3::text)::jsonb)) AND EXISTS(SELECT 1 FROM notification_consumers c JOIN nodes n ON n.id=c.node_id WHERE n.enabled AND c.node_id=node_notices.node_id AND c.generation=node_notices.generation AND c.expires_at>clock_timestamp())").bind(c.node).bind(c.generation).bind(ids).execute(db).await?.rows_affected())
}
pub(in super::super) fn has_expired() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM notification_consumers WHERE expires_at<=clock_timestamp() AND role<>'control')",
    )
}
pub(in super::super) fn prune() -> Statement<'static> {
    sqlx::query(
        "DELETE FROM notification_consumers WHERE expires_at<=clock_timestamp() AND role<>'control' AND node_id IN (SELECT node_id FROM notification_consumers WHERE expires_at<=clock_timestamp() AND role<>'control' LIMIT 32)",
    )
}
