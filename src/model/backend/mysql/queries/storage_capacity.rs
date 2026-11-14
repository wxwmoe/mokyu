use super::{Rows, Scalar, Statement};
use crate::model::storage_cluster::{Accounting, Manifest, Policy};
use sqlx::types::Json;
use uuid::Uuid;

pub(in super::super) fn allocation(
    cluster: Uuid,
) -> Rows<'static, (Json<Policy>, Json<Manifest>, Option<Uuid>)> {
    sqlx::query_as(
        "SELECT policy,manifest,allocation_group FROM storage_clusters WHERE id=? FOR UPDATE",
    )
    .bind(cluster)
}

pub(in super::super) fn cursor(cluster: Uuid, group: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_clusters SET allocation_group=? WHERE id=?")
        .bind(group)
        .bind(cluster)
}

pub(in super::super) fn totals(cluster: Uuid) -> Rows<'static, (Uuid, i64)> {
    sqlx::query_as("SELECT media_id,CAST(SUM(bytes) AS SIGNED) FROM storage_reservations WHERE cluster_id=? GROUP BY media_id")
        .bind(cluster)
}

pub(in super::super) fn settle(media: Uuid, stamp: Accounting) -> Statement<'static> {
    sqlx::query("DELETE FROM storage_reservations WHERE media_id=? AND complete AND accounting_boot IS NOT NULL AND (accounting_boot<>? OR accounting_revision<=?)")
        .bind(media).bind(stamp.boot).bind(stamp.revision as i64)
}

pub(in super::super) fn abandoned(cluster: Uuid, now: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM storage_reservations WHERE cluster_id=? AND operation IN (SELECT p.operation FROM storage_payloads p WHERE p.cluster_id=? AND p.state='retired' AND p.write_until<=? AND NOT EXISTS(SELECT 1 FROM storage_fragments f WHERE f.operation=p.operation AND NOT f.deleted) AND NOT EXISTS(SELECT 1 FROM storage_repairs r WHERE r.operation=p.operation AND r.state='writing' AND NOT r.candidate_removed))")
        .bind(cluster).bind(cluster).bind(now)
}

pub(in super::super) fn reserved(operation: Uuid, media: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT bytes FROM storage_reservations WHERE operation=? AND media_id=?")
        .bind(operation)
        .bind(media)
}

pub(in super::super) fn received(operation: Uuid, media: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM storage_fragments WHERE operation=? AND media_id=? AND root IS NOT NULL AND NOT deleted")
        .bind(operation).bind(media)
}

pub(in super::super) fn insert(
    operation: Uuid,
    media: Uuid,
    cluster: Uuid,
    bytes: i64,
) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO storage_reservations(operation,media_id,cluster_id,bytes) VALUES(?,?,?,?)",
    )
    .bind(operation)
    .bind(media)
    .bind(cluster)
    .bind(bytes)
}

pub(in super::super) fn stamp(
    operation: Uuid,
    media: Uuid,
    stamp: Accounting,
    complete: bool,
) -> Statement<'static> {
    sqlx::query("UPDATE storage_reservations SET accounting_revision=CASE WHEN accounting_boot=? AND accounting_revision>? THEN accounting_revision ELSE ? END,accounting_boot=?,complete=? WHERE operation=? AND media_id=?")
        .bind(stamp.boot).bind(stamp.revision as i64).bind(stamp.revision as i64).bind(stamp.boot).bind(complete).bind(operation).bind(media)
}
