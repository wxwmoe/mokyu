use super::{Rows, Scalar, Statement};
use crate::model::notification::{Consumer, LIMIT};
use uuid::Uuid;

pub(crate) fn membership() -> Statement<'static> {
    sqlx::query("SELECT pg_advisory_xact_lock(734922709851088)")
}

pub(crate) fn activate(consumer: Consumer, role: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO notification_consumers(node_id,generation,role,expires_at) VALUES($1,$2,$3,clock_timestamp()+interval '90 seconds') ON CONFLICT(node_id) DO UPDATE SET generation=EXCLUDED.generation,role=EXCLUDED.role,expires_at=EXCLUDED.expires_at")
        .bind(consumer.node).bind(consumer.generation).bind(role)
}

pub(crate) fn renew(node: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE notification_consumers c SET expires_at=CASE WHEN c.role='control' THEN clock_timestamp()+interval '90 seconds' ELSE (SELECT expires_at FROM node_sessions s WHERE s.node_id=c.node_id AND s.connection_id=c.generation) END WHERE c.node_id=$1 AND (c.role='control' OR EXISTS(SELECT 1 FROM node_sessions s WHERE s.node_id=c.node_id AND s.connection_id=c.generation))").bind(node)
}

pub(crate) fn claim<'q>(
    nodes: &'q [Uuid],
    generations: &'q [Uuid],
    token: Uuid,
) -> Rows<'q, (Uuid, Uuid, Uuid, String, Uuid, String)> {
    sqlx::query_as("WITH selected AS MATERIALIZED (SELECT t.notification_id,t.node_id,t.generation FROM notification_targets t JOIN notification_consumers c USING(node_id,generation) JOIN nodes n ON n.id=t.node_id JOIN unnest($1::uuid[],$2::uuid[]) AS active(node_id,generation) USING(node_id,generation) WHERE n.enabled AND c.expires_at>clock_timestamp() AND t.applied_at IS NULL AND t.retired_at IS NULL AND (t.claim_until IS NULL OR t.claim_until<=clock_timestamp()) ORDER BY t.attempts,t.notification_id,t.node_id LIMIT $4 FOR UPDATE OF t SKIP LOCKED), claimed AS (UPDATE notification_targets t SET claim_token=$3,claim_until=clock_timestamp()+interval '2 seconds',attempts=LEAST(t.attempts,2147483646)+1 FROM selected s WHERE t.notification_id=s.notification_id AND t.node_id=s.node_id AND t.generation=s.generation RETURNING t.notification_id,t.node_id,t.generation) SELECT c.node_id,c.generation,n.id,n.scope,n.resource_id,n.object_key FROM claimed c JOIN notifications n ON n.id=c.notification_id")
        .bind(nodes).bind(generations).bind(token).bind(LIMIT as i64)
}

pub(crate) fn acknowledge<'q>(consumer: Consumer, ids: &'q [Uuid]) -> Statement<'q> {
    sqlx::query("UPDATE notification_targets t SET applied_at=clock_timestamp(),claim_token=NULL,claim_until=NULL FROM notification_consumers c,nodes n WHERE t.node_id=$1 AND t.generation=$2 AND t.notification_id=ANY($3) AND t.node_id=c.node_id AND t.generation=c.generation AND n.id=c.node_id AND n.enabled AND c.expires_at>clock_timestamp() AND (c.role='control' OR EXISTS(SELECT 1 FROM node_sessions s WHERE s.node_id=c.node_id AND s.connection_id=c.generation AND s.credential_generation=n.credential_generation AND s.expires_at>clock_timestamp())) AND t.applied_at IS NULL AND t.retired_at IS NULL AND t.claim_token IS NOT NULL")
        .bind(consumer.node).bind(consumer.generation).bind(ids)
}

pub(crate) fn expired_nodes() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT n.id FROM nodes n JOIN notification_consumers c ON c.node_id=n.id WHERE c.expires_at<=clock_timestamp() AND EXISTS(SELECT 1 FROM notification_targets t WHERE t.node_id=n.id AND t.applied_at IS NULL AND t.retired_at IS NULL) ORDER BY n.id LIMIT 32 FOR UPDATE OF n SKIP LOCKED",
    )
}

pub(crate) fn retire_expired(nodes: &[Uuid]) -> Statement<'_> {
    sqlx::query("UPDATE notification_targets t SET retired_at=clock_timestamp(),claim_token=NULL,claim_until=NULL FROM notification_consumers c,nodes n WHERE t.node_id=c.node_id AND n.id=c.node_id AND t.node_id=ANY($1) AND c.expires_at<=clock_timestamp() AND t.applied_at IS NULL AND t.retired_at IS NULL").bind(nodes)
}

pub(crate) fn retire_previous(consumer: Consumer) -> Statement<'static> {
    sqlx::query("UPDATE notification_targets SET retired_at=clock_timestamp(),claim_token=NULL,claim_until=NULL WHERE node_id=$1 AND generation<>$2 AND applied_at IS NULL AND retired_at IS NULL").bind(consumer.node).bind(consumer.generation)
}

pub(crate) fn collect() -> Statement<'static> {
    sqlx::query(
        "DELETE FROM notifications WHERE id IN (SELECT n.id FROM notifications n WHERE n.created_at<clock_timestamp()-interval '1 minute' AND NOT EXISTS(SELECT 1 FROM notification_targets t WHERE t.notification_id=n.id AND t.applied_at IS NULL AND t.retired_at IS NULL) ORDER BY n.created_at LIMIT 512 FOR UPDATE SKIP LOCKED)",
    )
}
