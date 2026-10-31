use crate::model::redundancy::{Placement, Reservation, Row, Settings};
use sqlx::{Sqlite, types::Json};
use uuid::Uuid;

type Statement<'q> = sqlx::query::Query<'q, Sqlite, <Sqlite as sqlx::Database>::Arguments>;
type Rows<'q, T> = sqlx::query::QueryAs<'q, Sqlite, T, <Sqlite as sqlx::Database>::Arguments>;
type Scalar<'q, T> = sqlx::query::QueryScalar<'q, Sqlite, T, <Sqlite as sqlx::Database>::Arguments>;

pub(super) fn lock_catalog() -> Statement<'static> {
    sqlx::query("SELECT singleton FROM redundancy_catalog")
}

pub(super) fn candidates<'a>(
    source: Uuid,
    settings: &Settings,
    placement: &'a Placement,
) -> Rows<'a, Reservation> {
    sqlx::query_as("SELECT source_bucket,target_bucket,target_prefix,source_space,target_space,target_domain FROM redundancy_links WHERE source_bucket=? OR target_bucket=? OR source_bucket=? OR target_bucket=? OR source_domain=? OR target_domain IN (?,?)")
        .bind(source)
        .bind(source)
        .bind((!settings.target_external).then_some(settings.target_bucket))
        .bind((!settings.target_external).then_some(settings.target_bucket))
        .bind(&placement.target.domain)
        .bind(&placement.source.domain)
        .bind(&placement.target.domain)
}

pub(super) fn insert<'a>(
    id: Uuid,
    source: Uuid,
    settings: &'a Settings,
    placement: &'a Placement,
) -> Statement<'a> {
    sqlx::query("INSERT INTO redundancy_links(id,source_bucket,target_bucket,external_bucket,target_prefix,source_domain,source_space,target_domain,target_space,settings,reconcile_prefix) VALUES(?,?,?,?,?,?,?,?,?,?,'')")
        .bind(id)
        .bind(source)
        .bind((!settings.target_external).then_some(settings.target_bucket))
        .bind(settings.target_external.then_some(settings.target_bucket))
        .bind(&settings.target_prefix)
        .bind(&placement.source.domain)
        .bind(&placement.source.path)
        .bind(&placement.target.domain)
        .bind(&placement.target.path)
        .bind(Json(settings))
}

pub(super) fn backlog(id: Uuid, capacity: i32) -> Statement<'static> {
    sqlx::query("INSERT INTO redundancy_backlogs(id,max_pending_keys) VALUES(?,?)")
        .bind(id)
        .bind(capacity)
}

pub(super) fn mark(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE transfer_runs SET redundancy_id=task_id WHERE task_id=?").bind(id)
}

pub(super) fn link(id: Uuid) -> Rows<'static, Row> {
    sqlx::query_as("SELECT l.id,l.source_bucket,l.settings,l.revision,l.state,l.reconciled_at,r.progress,t.state AS task_state,t.error,b.queued_keys,b.queued_keys+(SELECT COUNT(*) FROM transfer_items i WHERE i.task_id=l.id AND i.state IN ('planning','planned','copying')) AS pending,(SELECT COUNT(*) FROM transfer_items i WHERE i.task_id=l.id AND i.source_version IS NOT NULL AND i.state IN ('completed','skipped') AND i.error IS NULL AND NOT EXISTS(SELECT 1 FROM transfer_changes c WHERE c.task_id=i.task_id AND c.object_key=i.source_key)) AS confirmed FROM redundancy_links l JOIN redundancy_backlogs b ON b.id=l.id JOIN transfer_runs r ON r.task_id=l.id JOIN tasks t ON t.id=l.id WHERE l.id=?")
        .bind(id)
}

pub(super) fn page(bucket: Uuid, after: Option<Uuid>, limit: i64) -> Rows<'static, Row> {
    sqlx::query_as("SELECT l.id,l.source_bucket,l.settings,l.revision,l.state,l.reconciled_at,r.progress,t.state AS task_state,t.error,b.queued_keys,b.queued_keys+(SELECT COUNT(*) FROM transfer_items i WHERE i.task_id=l.id AND i.state IN ('planning','planned','copying')) AS pending,(SELECT COUNT(*) FROM transfer_items i WHERE i.task_id=l.id AND i.source_version IS NOT NULL AND i.state IN ('completed','skipped') AND i.error IS NULL AND NOT EXISTS(SELECT 1 FROM transfer_changes c WHERE c.task_id=i.task_id AND c.object_key=i.source_key)) AS confirmed FROM redundancy_links l JOIN redundancy_backlogs b ON b.id=l.id JOIN transfer_runs r ON r.task_id=l.id JOIN tasks t ON t.id=l.id WHERE (l.source_bucket=? OR l.target_bucket=?) AND (? IS NULL OR l.id>?) ORDER BY l.id LIMIT ?")
        .bind(bucket)
        .bind(bucket)
        .bind(after)
        .bind(after)
        .bind(limit)
}

pub(super) fn state(id: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT state FROM redundancy_links WHERE id=?").bind(id)
}

pub(super) fn transition<'a>(id: Uuid, state: &'a str) -> Statement<'a> {
    sqlx::query("UPDATE redundancy_links SET state=? WHERE id=?")
        .bind(state)
        .bind(id)
}

pub(super) fn command<'a>(
    id: Uuid,
    state: &'a str,
    prefix: &'a str,
    revision: i64,
) -> Statement<'a> {
    sqlx::query("UPDATE redundancy_links SET state=?,reconcile_prefix=?,revision=revision+1 WHERE id=? AND revision=?")
        .bind(state)
        .bind(prefix)
        .bind(id)
        .bind(revision)
}

pub(super) fn forget(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM redundancy_links WHERE id=? AND state='removing'").bind(id)
}

pub(super) fn scope(id: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT reconcile_prefix FROM redundancy_links WHERE id=?").bind(id)
}

pub(super) fn pending(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM transfer_changes WHERE task_id=?)").bind(id)
}

pub(super) fn due(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM redundancy_links WHERE id=? AND next_reconcile_at<=strftime('%Y-%m-%dT%H:%M:%fZ','now'))")
        .bind(id)
}

pub(super) fn settled(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE redundancy_links SET reconciled_at=CASE WHEN state IN ('active','reconciling') AND reconcile_prefix='' THEN strftime('%Y-%m-%dT%H:%M:%fZ','now') ELSE reconciled_at END,next_reconcile_at=CASE WHEN state IN ('active','reconciling') AND reconcile_prefix='' THEN strftime('%Y-%m-%dT%H:%M:%fZ','now','+1 hour') ELSE next_reconcile_at END,state='idle',reconcile_prefix='' WHERE id=?")
        .bind(id)
}

pub(super) fn admission(bucket: Uuid) -> Rows<'static, (String, bool, i32)> {
    sqlx::query_as("SELECT b.state,b.uploads_paused,g.managed FROM buckets b JOIN bucket_change_gates g ON g.bucket_id=b.id WHERE b.id=?").bind(bucket)
}

pub(super) fn gate(bucket: Uuid) -> Scalar<'static, i32> {
    sqlx::query_scalar("SELECT managed FROM bucket_change_gates WHERE bucket_id=?").bind(bucket)
}

pub(super) fn count(bucket: Uuid, delta: i32) -> Statement<'static> {
    sqlx::query("UPDATE bucket_change_gates SET managed=managed+? WHERE bucket_id=?")
        .bind(delta)
        .bind(bucket)
}

pub(super) fn manager<'a>(bucket: Uuid, key: &'a str) -> Rows<'a, (Uuid, String)> {
    sqlx::query_as("SELECT id,state FROM redundancy_links WHERE target_bucket=? AND substr(?,1,length(target_prefix))=target_prefix LIMIT 1")
        .bind(bucket)
        .bind(key)
}

pub(super) fn relocation(record: &crate::storage::model::Record) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storages s WHERE s.id=? AND (s.type IS NOT ? OR s.path IS NOT ? OR s.endpoint_url IS NOT ? OR s.remote_bucket_name IS NOT ? OR s.region IS NOT ? OR s.prefix IS NOT ?) AND EXISTS(SELECT 1 FROM buckets b JOIN redundancy_links l ON l.source_bucket=b.id OR l.target_bucket=b.id WHERE b.storage_id=s.id))").bind(record.id).bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket_name).bind(&record.region).bind(&record.prefix)
}

pub(super) fn profile(revision: i64, id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE transfer_runs SET profile_revision=? WHERE task_id=?")
        .bind(revision)
        .bind(id)
}

pub(super) fn outgoing(bucket: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM redundancy_links WHERE source_bucket=?)")
        .bind(bucket)
}

pub(super) fn referenced(bucket: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM redundancy_links WHERE source_bucket=? OR target_bucket=?)",
    )
    .bind(bucket)
    .bind(bucket)
}
use crate::model::{
    Id,
    replication::{Snapshot, changes::Change},
};

pub(super) fn work(id: Uuid) -> Rows<'static, (String, String, i32, i32, i32)> {
    sqlx::query_as("SELECT l.state,l.reconcile_prefix,l.scan_credit,b.queued_keys,b.max_pending_keys FROM redundancy_links l JOIN redundancy_backlogs b ON b.id=l.id WHERE l.id=?")
        .bind(id)
}

pub(super) fn credit(id: Uuid, value: i32) -> Statement<'static> {
    sqlx::query("UPDATE redundancy_links SET scan_credit=? WHERE id=?")
        .bind(value)
        .bind(id)
}

pub(super) fn upper<'a>(id: Uuid, prefix: &'a str, limit: Option<&'a str>) -> Scalar<'a, String> {
    sqlx::query_scalar("SELECT source_key FROM transfer_items WHERE task_id=? AND source_key>=? AND (? IS NULL OR source_key<?) ORDER BY source_key DESC LIMIT 1")
        .bind(id)
        .bind(prefix)
        .bind(limit)
        .bind(limit)
}

pub(super) fn history<'a>(
    id: Uuid,
    bucket: Uuid,
    prefix: &'a str,
    after: Option<&'a str>,
    end: &'a str,
) -> Rows<'a, (String, bool)> {
    sqlx::query_as("SELECT i.source_key,EXISTS(SELECT 1 FROM objects o WHERE o.bucket_id=? AND o.object_key=i.source_key AND o.stream_id IS NOT NULL) FROM transfer_items i WHERE task_id=? AND source_key>=? AND (? IS NULL OR source_key>?) AND source_key<=? ORDER BY source_key LIMIT 64")
        .bind(bucket)
        .bind(id)
        .bind(prefix)
        .bind(after)
        .bind(after)
        .bind(end)
}

pub(super) fn enqueue<'a>(
    id: Uuid,
    key: &'a str,
    version: Option<Id>,
    snapshot: Option<&'a Snapshot>,
) -> Statement<'a> {
    sqlx::query("INSERT INTO transfer_changes(task_id,object_key,source_version,snapshot,reconcile) VALUES(?,?,?,?,TRUE) ON CONFLICT(task_id,object_key) DO UPDATE SET reconcile=TRUE")
        .bind(id)
        .bind(key)
        .bind(version)
        .bind(snapshot.map(Json))
}

pub(super) fn peek(id: Uuid) -> Rows<'static, Change> {
    sqlx::query_as("SELECT object_key,source_version,snapshot,revision,reconcile FROM transfer_changes WHERE task_id=? ORDER BY object_key LIMIT 1")
        .bind(id)
}

pub(super) fn exact<'a>(id: Uuid, key: &'a str) -> Rows<'a, Change> {
    sqlx::query_as("SELECT object_key,source_version,snapshot,revision,reconcile FROM transfer_changes WHERE task_id=? AND object_key=?")
        .bind(id)
        .bind(key)
}

pub(super) fn target_space(id: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT target_space FROM redundancy_links WHERE id=?").bind(id)
}

pub(super) fn checking(id: Uuid, key: &str, revision: i64) -> Statement<'_> {
    sqlx::query("UPDATE transfer_changes SET reconcile=FALSE WHERE task_id=? AND object_key=? AND revision=?").bind(id).bind(key).bind(revision)
}

pub(super) fn missing(item: Id) -> Statement<'static> {
    sqlx::query("UPDATE transfer_items SET source_version=NULL,source_pin=NULL WHERE id=?")
        .bind(item)
}
