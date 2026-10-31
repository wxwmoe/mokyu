use crate::model::redundancy::{Placement, Reservation, Row, Settings};
use sqlx::{MySql, types::Json};
use uuid::Uuid;

type Statement<'q> = sqlx::query::Query<'q, MySql, <MySql as sqlx::Database>::Arguments>;
type Rows<'q, T> = sqlx::query::QueryAs<'q, MySql, T, <MySql as sqlx::Database>::Arguments>;
type Scalar<'q, T> = sqlx::query::QueryScalar<'q, MySql, T, <MySql as sqlx::Database>::Arguments>;

pub(super) fn lock_catalog() -> Statement<'static> {
    sqlx::query("SELECT singleton FROM redundancy_catalog FOR UPDATE")
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
    sqlx::query("INSERT INTO redundancy_links(id,source_bucket,target_bucket,external_bucket,target_prefix,source_domain,source_space,target_domain,target_space,settings,reconcile_prefix,synchronous) VALUES(?,?,?,?,?,?,?,?,?,?,'',?)")
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
        .bind(settings.mode == crate::model::redundancy::Mode::Synchronous)
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
    sqlx::query_as("SELECT l.id,l.source_bucket,l.settings,l.revision,l.state,l.reconciled_at,r.progress,t.state AS task_state,COALESCE(NULLIF(t.error,'replica_pending'),(SELECT w.error FROM replica_targets v JOIN tasks w ON w.id=v.task_id WHERE v.link_id=l.id AND v.phase='preparing' AND w.state='failed' ORDER BY v.publication_id LIMIT 1)) AS error,b.queued_keys,b.queued_keys+(SELECT COUNT(*) FROM replica_targets v WHERE v.link_id=l.id AND v.phase IN ('admitted','preparing','prepared'))+(SELECT COUNT(*) FROM transfer_items i WHERE i.task_id=l.id AND i.state IN ('planning','planned','copying')) AS pending,(SELECT COUNT(*) FROM transfer_items i WHERE i.task_id=l.id AND i.source_version IS NOT NULL AND i.state IN ('completed','skipped') AND i.error IS NULL AND NOT EXISTS(SELECT 1 FROM transfer_changes c WHERE c.task_id=i.task_id AND c.key_hash=i.source_key_hash AND BINARY c.object_key=BINARY i.source_key)) AS confirmed FROM redundancy_links l JOIN redundancy_backlogs b ON b.id=l.id JOIN transfer_runs r ON r.task_id=l.id JOIN tasks t ON t.id=l.id WHERE l.id=?")
        .bind(id)
}

pub(super) fn page(bucket: Uuid, after: Option<Uuid>, limit: i64) -> Rows<'static, Row> {
    sqlx::query_as("SELECT l.id,l.source_bucket,l.settings,l.revision,l.state,l.reconciled_at,r.progress,t.state AS task_state,COALESCE(NULLIF(t.error,'replica_pending'),(SELECT w.error FROM replica_targets v JOIN tasks w ON w.id=v.task_id WHERE v.link_id=l.id AND v.phase='preparing' AND w.state='failed' ORDER BY v.publication_id LIMIT 1)) AS error,b.queued_keys,b.queued_keys+(SELECT COUNT(*) FROM replica_targets v WHERE v.link_id=l.id AND v.phase IN ('admitted','preparing','prepared'))+(SELECT COUNT(*) FROM transfer_items i WHERE i.task_id=l.id AND i.state IN ('planning','planned','copying')) AS pending,(SELECT COUNT(*) FROM transfer_items i WHERE i.task_id=l.id AND i.source_version IS NOT NULL AND i.state IN ('completed','skipped') AND i.error IS NULL AND NOT EXISTS(SELECT 1 FROM transfer_changes c WHERE c.task_id=i.task_id AND c.key_hash=i.source_key_hash AND BINARY c.object_key=BINARY i.source_key)) AS confirmed FROM redundancy_links l JOIN redundancy_backlogs b ON b.id=l.id JOIN transfer_runs r ON r.task_id=l.id JOIN tasks t ON t.id=l.id WHERE (l.source_bucket=? OR l.target_bucket=?) AND (? IS NULL OR l.id>?) ORDER BY l.id LIMIT ?")
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
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM redundancy_links WHERE id=? AND next_reconcile_at<=UTC_TIMESTAMP(6))")
        .bind(id)
}

pub(super) fn settled(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE redundancy_links SET reconciled_at=CASE WHEN state IN ('active','reconciling') AND reconcile_prefix='' THEN UTC_TIMESTAMP(6) ELSE reconciled_at END,next_reconcile_at=CASE WHEN state IN ('active','reconciling') AND reconcile_prefix='' THEN UTC_TIMESTAMP(6)+INTERVAL 1 HOUR ELSE next_reconcile_at END,state='idle',reconcile_prefix='' WHERE id=?")
        .bind(id)
}

pub(super) fn admission(bucket: Uuid) -> Rows<'static, (String, bool, i32, i32, Option<String>)> {
    sqlx::query_as("SELECT b.state,b.uploads_paused,g.managed,g.required_targets,b.image_policy FROM buckets b JOIN bucket_change_gates g ON g.bucket_id=b.id WHERE b.id=? LOCK IN SHARE MODE").bind(bucket)
}

pub(super) fn publication(id: Uuid, bucket: Uuid, key: &str, stream: Id) -> Statement<'_> {
    sqlx::query(
        "INSERT INTO replica_publications(id,source_bucket,object_key,source_pin) VALUES(?,?,?,?)",
    )
    .bind(id)
    .bind(bucket)
    .bind(key.as_bytes())
    .bind(stream)
}

pub(super) fn freeze_targets(id: Uuid, bucket: Uuid, key: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO replica_targets(publication_id,link_id,link_revision,target_bucket,target_key,storage_id,profile_revision,authority,permissions,expected_version) SELECT ?,l.id,l.revision,l.target_bucket,CONCAT(l.target_prefix,?),r.storage_id,r.profile_revision,r.authority,JSON_UNQUOTE(JSON_EXTRACT(r.options,'$.permissions')),o.stream_id FROM redundancy_links l JOIN transfer_runs r ON r.task_id=l.id LEFT JOIN objects o ON o.bucket_id=l.target_bucket AND o.object_key=CONCAT(l.target_prefix,?) WHERE l.source_bucket=? AND l.synchronous AND l.state NOT IN ('pausing','paused','removing') AND OCTET_LENGTH(CONCAT(l.target_prefix,?)) BETWEEN 1 AND 1024 ORDER BY l.target_bucket,l.id")
        .bind(id)
        .bind(key.as_bytes())
        .bind(key.as_bytes())
        .bind(bucket)
        .bind(key.as_bytes())
}

pub(super) fn attach_publication(id: Uuid, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET publication_id=? WHERE id=? AND publication_id IS NULL")
        .bind(id)
        .bind(stream)
}

pub(super) fn gate(bucket: Uuid) -> Rows<'static, (i32, i32, Option<String>)> {
    sqlx::query_as(
        "SELECT g.managed,g.required_targets,b.image_policy FROM bucket_change_gates g JOIN buckets b ON b.id=g.bucket_id WHERE g.bucket_id=? LOCK IN SHARE MODE",
    )
    .bind(bucket)
}

pub(super) fn count(bucket: Uuid, delta: i32) -> Statement<'static> {
    sqlx::query("UPDATE bucket_change_gates SET managed=managed+? WHERE bucket_id=?")
        .bind(delta)
        .bind(bucket)
}

pub(super) fn manager<'a>(bucket: Uuid, key: &'a str) -> Rows<'a, (Uuid, String)> {
    sqlx::query_as("SELECT id,state FROM redundancy_links WHERE target_bucket=? AND LEFT(CAST(? AS BINARY),OCTET_LENGTH(target_prefix))=CAST(target_prefix AS BINARY) LIMIT 1 LOCK IN SHARE MODE")
        .bind(bucket)
        .bind(key)
}

pub(super) fn relocation(record: &crate::storage::model::Record) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storages s WHERE s.id=? AND (NOT (BINARY s.type <=> BINARY ?) OR NOT (BINARY s.path <=> BINARY ?) OR NOT (BINARY s.endpoint_url <=> BINARY ?) OR NOT (BINARY s.remote_bucket_name <=> BINARY ?) OR NOT (BINARY s.region <=> BINARY ?) OR NOT (BINARY s.prefix <=> BINARY ?) OR NOT ?) AND (EXISTS(SELECT 1 FROM buckets b JOIN redundancy_links l ON l.source_bucket=b.id OR l.target_bucket=b.id WHERE b.storage_id=s.id) OR EXISTS(SELECT 1 FROM bucket_tiering t WHERE t.hot_storage=s.id OR t.cold_storage=s.id) OR EXISTS(SELECT 1 FROM tier_locations h WHERE h.home_storage=s.id OR h.storage_id=s.id) OR EXISTS(SELECT 1 FROM tier_moves m WHERE m.source_storage=s.id OR m.target_storage=s.id)))").bind(record.id).bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket_name).bind(&record.region).bind(&record.prefix).bind(record.enabled)
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
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM redundancy_links WHERE source_bucket=? OR target_bucket=? LOCK IN SHARE MODE)")
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
    sqlx::query_scalar("SELECT source_key FROM transfer_items WHERE task_id=? AND source_key_bytes>=? AND (? IS NULL OR source_key_bytes<?) ORDER BY source_key_bytes DESC LIMIT 1")
        .bind(id)
        .bind(prefix.as_bytes())
        .bind(limit.map(str::as_bytes))
        .bind(limit.map(str::as_bytes))
}

pub(super) fn history<'a>(
    id: Uuid,
    bucket: Uuid,
    prefix: &'a str,
    after: Option<&'a str>,
    end: &'a str,
) -> Rows<'a, (String, bool)> {
    sqlx::query_as("SELECT i.source_key,EXISTS(SELECT 1 FROM objects o WHERE o.bucket_id=? AND o.object_key=i.source_key_bytes AND o.stream_id IS NOT NULL) FROM transfer_items i WHERE task_id=? AND source_key_bytes>=? AND (? IS NULL OR source_key_bytes>?) AND source_key_bytes<=? ORDER BY source_key_bytes LIMIT 64")
        .bind(bucket)
        .bind(id)
        .bind(prefix.as_bytes())
        .bind(after.map(str::as_bytes))
        .bind(after.map(str::as_bytes))
        .bind(end.as_bytes())
}

pub(super) fn enqueue<'a>(
    id: Uuid,
    key: &'a str,
    version: Option<Id>,
    snapshot: Option<&'a Snapshot>,
) -> Statement<'a> {
    sqlx::query("INSERT INTO transfer_changes(task_id,object_key,source_version,snapshot,reconcile) VALUES(?,?,?,?,TRUE) ON DUPLICATE KEY UPDATE object_key=IF(BINARY transfer_changes.object_key=BINARY VALUES(object_key),transfer_changes.object_key,NULL),reconcile=TRUE")
        .bind(id)
        .bind(key)
        .bind(version)
        .bind(snapshot.map(Json))
}

pub(super) fn peek(id: Uuid) -> Rows<'static, Change> {
    sqlx::query_as("SELECT object_key,source_version,snapshot,revision,reconcile FROM transfer_changes WHERE task_id=? AND NOT EXISTS(SELECT 1 FROM streams s JOIN replica_targets v ON v.publication_id=s.publication_id WHERE s.id=transfer_changes.source_version AND v.link_id=transfer_changes.task_id AND v.phase IN ('preparing','prepared')) ORDER BY object_key_bytes LIMIT 1")
        .bind(id)
}

pub(super) fn exact<'a>(id: Uuid, key: &'a str) -> Rows<'a, Change> {
    sqlx::query_as("SELECT object_key,source_version,snapshot,revision,reconcile FROM transfer_changes WHERE task_id=? AND object_key_bytes=? FOR UPDATE")
        .bind(id)
        .bind(key.as_bytes())
}

pub(super) fn target_space(id: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT target_space FROM redundancy_links WHERE id=?").bind(id)
}

pub(super) fn checking(id: Uuid, key: &str, revision: i64) -> Statement<'_> {
    sqlx::query("UPDATE transfer_changes SET reconcile=FALSE WHERE task_id=? AND object_key_bytes=? AND revision=?").bind(id).bind(key.as_bytes()).bind(revision)
}

pub(super) fn missing(item: Id) -> Statement<'static> {
    sqlx::query("UPDATE transfer_items SET source_version=NULL,source_pin=NULL WHERE id=?")
        .bind(item)
}
