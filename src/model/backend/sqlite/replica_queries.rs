use crate::model::Id;
use sqlx::Sqlite;
use uuid::Uuid;

pub(super) type Tx<'a> = sqlx::Transaction<'a, Sqlite>;
type Statement<'q> = sqlx::query::Query<'q, Sqlite, <Sqlite as sqlx::Database>::Arguments>;
type Rows<'q, T> = sqlx::query::QueryAs<'q, Sqlite, T, <Sqlite as sqlx::Database>::Arguments>;

pub(super) fn staged_source(
    task: Uuid,
    stream: Id,
) -> Rows<'static, super::replication_queries::Source> {
    sqlx::query_as("SELECT s.id,CAST(s.object_key AS TEXT) AS object_key,s.metadata,s.checksums,s.public_read,s.size,s.etag,s.payload_type FROM replica_targets t JOIN replica_publications p ON p.id=t.publication_id JOIN streams s ON s.id=p.source_pin WHERE t.task_id=? AND s.id=? AND p.phase='preparing' AND t.phase='preparing' AND s.state IN ('writing','ready','retired')")
        .bind(task).bind(stream)
}

pub(super) fn prepared(task: Uuid, candidate: Id) -> Statement<'static> {
    sqlx::query("UPDATE replica_targets SET phase='prepared' WHERE task_id=? AND candidate=? AND phase='preparing' AND EXISTS(SELECT 1 FROM replica_publications p WHERE p.id=replica_targets.publication_id AND p.phase='preparing')")
        .bind(task).bind(candidate)
}

pub(super) fn original(
    publication: Uuid,
    stream: Id,
) -> Rows<'static, super::replication_queries::Source> {
    sqlx::query_as("SELECT s.id,CAST(s.object_key AS TEXT) AS object_key,s.metadata,s.checksums,s.public_read,s.size,s.etag,s.payload_type FROM replica_publications p JOIN streams s ON s.id=p.source_pin WHERE p.id=? AND s.id=? AND s.state IN ('writing','ready','retired')")
        .bind(publication)
        .bind(stream)
}

pub(super) fn target(
    publication: Uuid,
    phase: &str,
) -> Rows<'_, super::replica_transactions::Target> {
    sqlx::query_as("SELECT link_id,link_revision,target_bucket,CAST(target_key AS TEXT) AS target_key,storage_id,profile_revision,authority,permissions,expected_version,candidate,task_id FROM replica_targets WHERE publication_id=? AND phase=? ORDER BY target_bucket,target_key,link_id LIMIT 1")
        .bind(publication)
        .bind(phase)
}

pub(super) fn begin<'a>(
    id: Uuid,
    intent: &'a serde_json::Value,
    hash: &'a [u8],
    early: bool,
) -> Statement<'a> {
    sqlx::query("UPDATE replica_publications SET phase='preparing',intent=?,intent_hash=?,early_ack=? WHERE id=? AND phase='admitted'")
        .bind(sqlx::types::Json(intent))
        .bind(hash)
        .bind(early)
        .bind(id)
}

pub(super) fn start_child(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE transfer_runs SET phase='plan',scanned=TRUE,sealed=TRUE WHERE task_id=? AND phase='scan'")
        .bind(task)
}

pub(super) fn reserve_item(task: Uuid, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE transfer_items SET candidate=? WHERE task_id=? AND state='planning' AND candidate IS NULL")
        .bind(stream)
        .bind(task)
}

pub(super) fn delegate(
    publication: Uuid,
    link: Uuid,
    task: Uuid,
    stream: Id,
) -> Statement<'static> {
    sqlx::query("UPDATE replica_targets SET candidate=?,task_id=?,phase='preparing' WHERE publication_id=? AND link_id=? AND phase='admitted'")
        .bind(stream)
        .bind(task)
        .bind(publication)
        .bind(link)
}

pub(super) fn identity(publication: Uuid) -> Rows<'static, (String, Option<Vec<u8>>, bool)> {
    sqlx::query_as("SELECT phase,intent_hash,early_ack FROM replica_publications WHERE id=?")
        .bind(publication)
}

pub(super) fn link(link: Uuid, source: Uuid, target: Uuid) -> Rows<'static, (i64, bool)> {
    sqlx::query_as("SELECT revision,synchronous FROM redundancy_links WHERE id=? AND source_bucket=? AND target_bucket=?")
        .bind(link)
        .bind(source)
        .bind(target)
}

pub(super) fn locally_pending(stream: Id) -> Rows<'static, (bool,)> {
    sqlx::query_as("SELECT EXISTS(SELECT 1 FROM raw_ranges x JOIN raw_sources r ON r.id=x.source_id WHERE x.stream_id=? AND r.state='ready' AND r.quarantine_code IS NULL AND (r.pending_upload OR r.durability IN ('buffered','volatile'))) OR EXISTS(SELECT 1 FROM extents e JOIN pending_uploads p ON p.chunk_id=e.chunk_id WHERE e.stream_id=?) OR EXISTS(SELECT 1 FROM extents e JOIN chunks c ON c.id=e.chunk_id JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id WHERE e.stream_id=? AND l.sync_pending AND l.quarantined_at IS NULL)").bind(stream).bind(stream).bind(stream)
}

pub(super) fn readiness(id: Uuid) -> Rows<'static, (bool, bool)> {
    sqlx::query_as("SELECT NOT EXISTS(SELECT 1 FROM replica_targets WHERE publication_id=? AND phase NOT IN ('prepared','published')),EXISTS(SELECT 1 FROM replica_targets r JOIN tasks t ON t.id=r.task_id WHERE r.publication_id=? AND (r.phase='aborted' OR t.state='failed'))").bind(id).bind(id)
}

pub(super) fn root(id: Uuid) -> Rows<'static, (Uuid, String, Option<Id>, bool)> {
    sqlx::query_as("SELECT source_bucket,CAST(object_key AS TEXT) AS object_key,source_pin,early_ack FROM replica_publications WHERE id=? AND phase='preparing'").bind(id)
}

pub(super) fn candidate(stream: Id) -> Rows<'static, crate::model::PublishingStream> {
    sqlx::query_as("SELECT * FROM streams WHERE id=?").bind(stream)
}

pub(super) fn installed(publication: Uuid, link: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE replica_targets SET phase='published',candidate=NULL WHERE publication_id=? AND link_id=? AND phase='prepared'").bind(publication).bind(link)
}

pub(super) fn finish(id: Uuid, result: &serde_json::Value) -> Statement<'_> {
    sqlx::query("UPDATE replica_publications SET phase='published',source_pin=NULL,result=?,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=? AND phase='preparing'").bind(sqlx::types::Json(result)).bind(id)
}

pub(super) fn publication_for_task(task: Uuid) -> Rows<'static, (Uuid,)> {
    sqlx::query_as("SELECT publication_id FROM replica_targets WHERE task_id=?").bind(task)
}

pub(super) fn intent(id: Uuid) -> Rows<'static, (sqlx::types::Json<serde_json::Value>,)> {
    sqlx::query_as("SELECT intent FROM replica_publications WHERE id=?").bind(id)
}
pub(super) fn state(
    id: Uuid,
) -> Rows<'static, (String, Option<sqlx::types::Json<serde_json::Value>>)> {
    sqlx::query_as("SELECT phase,result FROM replica_publications WHERE id=?").bind(id)
}

pub(super) fn abort_target(publication: Uuid, link: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE replica_targets SET phase='aborted',candidate=NULL WHERE publication_id=? AND link_id=? AND phase IN ('admitted','preparing','prepared')").bind(publication).bind(link)
}

pub(super) fn cancel_child(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE transfer_runs SET phase='cancel' WHERE task_id=?").bind(task)
}

pub(super) fn abort(id: Uuid, result: &serde_json::Value) -> Statement<'_> {
    sqlx::query("UPDATE replica_publications SET phase='aborted',source_pin=NULL,result=?,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=? AND phase='preparing'").bind(sqlx::types::Json(result)).bind(id)
}

pub(super) fn unclaim(bucket: Uuid, key: &str, stream: Id) -> Statement<'_> {
    sqlx::query(
        "UPDATE objects SET write_token=? WHERE bucket_id=? AND object_key=? AND write_token=?",
    )
    .bind(Id::default())
    .bind(bucket)
    .bind(key.as_bytes())
    .bind(stream)
}

pub(super) fn park(task: crate::model::task::Execution) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='paused',error='replica_pending',detail=json_set(detail,'$.replica_parked',1),execution_token=NULL WHERE id=? AND execution_token=? AND state='running'").bind(task.id).bind(task.token)
}

pub(super) fn wake(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='queued',error=NULL WHERE id=? AND state='paused' AND error='replica_pending'").bind(task)
}

pub(super) fn parked_stream(task: Uuid) -> Rows<'static, (Id, String)> {
    sqlx::query_as("SELECT s.id,p.phase FROM streams s JOIN replica_publications p ON p.id=s.publication_id WHERE s.owner_task_id=? AND s.state='writing' AND p.phase='preparing' ORDER BY s.id LIMIT 1").bind(task)
}

pub(super) fn failed(task: Uuid) -> Rows<'static, (Uuid, bool)> {
    sqlx::query_as("SELECT p.id,p.early_ack FROM replica_publications p WHERE p.phase='preparing' AND (EXISTS(SELECT 1 FROM replica_targets t WHERE t.publication_id=p.id AND t.task_id=?) OR EXISTS(SELECT 1 FROM streams s WHERE s.id=p.source_pin AND s.owner_task_id=?)) LIMIT 1").bind(task).bind(task)
}

pub(super) fn fail_parent(task: Uuid, reason: &str) -> Statement<'_> {
    sqlx::query("UPDATE tasks SET state='failed',error=? WHERE id=? AND state='paused' AND error='replica_pending'").bind(reason).bind(task)
}

pub(super) fn draining(link: Uuid) -> Rows<'static, (bool,)> {
    sqlx::query_as("SELECT EXISTS(SELECT 1 FROM replica_targets WHERE link_id=? AND phase IN ('admitted','preparing','prepared'))").bind(link)
}

pub(super) fn drained(link: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='queued',error=NULL WHERE id=? AND state='paused' AND (error='replica_pending' OR error='replica_drain' AND NOT EXISTS(SELECT 1 FROM replica_targets t WHERE t.link_id=tasks.id AND t.phase IN ('admitted','preparing','prepared')))").bind(link)
}

pub(super) fn abandoned_admissions(batch: i64) -> Rows<'static, (Id,)> {
    sqlx::query_as("SELECT s.id FROM replica_publications p JOIN streams s ON s.id=p.source_pin WHERE p.phase='admitted' AND s.state='abandoned' ORDER BY p.id LIMIT ?").bind(batch)
}

pub(super) fn admission(stream: Id, rejected: bool) -> Rows<'static, (Uuid, Uuid)> {
    sqlx::query_as("SELECT p.id,p.source_bucket FROM replica_publications p JOIN streams s ON s.id=p.source_pin WHERE p.source_pin=? AND p.phase='admitted' AND (? OR s.state='abandoned')").bind(stream).bind(rejected)
}

pub(super) fn forget_admission(id: Uuid, result: &serde_json::Value) -> Statement<'_> {
    sqlx::query("UPDATE replica_publications SET phase='aborted',source_pin=NULL,result=? WHERE id=? AND phase='admitted'").bind(sqlx::types::Json(result)).bind(id)
}

pub(super) fn expired(age: f64, batch: i64) -> Rows<'static, (Uuid,)> {
    sqlx::query_as("SELECT id FROM replica_publications WHERE phase IN ('published','aborted') AND updated_at<strftime('%Y-%m-%dT%H:%M:%fZ','now',printf('-%f seconds',?)) ORDER BY updated_at,id LIMIT ?").bind(age.max(600.0)).bind(batch)
}

pub(super) fn terminal(bucket: Uuid) -> Rows<'static, (Uuid,)> {
    sqlx::query_as("SELECT p.id FROM replica_publications p WHERE (p.source_bucket=? OR EXISTS(SELECT 1 FROM replica_targets t WHERE t.publication_id=p.id AND t.target_bucket=?)) AND p.phase IN ('published','aborted') ORDER BY p.id LIMIT 128").bind(bucket).bind(bucket)
}

pub(super) fn detach(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE streams SET publication_id=NULL WHERE publication_id=?").bind(id)
}

pub(super) fn prune(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM replica_publications WHERE id=? AND phase IN ('published','aborted')")
        .bind(id)
}

pub(super) fn retry(link: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='queued',error=NULL WHERE state='failed' AND EXISTS(SELECT 1 FROM replica_targets t JOIN replica_publications p ON p.id=t.publication_id WHERE t.link_id=? AND t.task_id=tasks.id AND t.phase='preparing' AND p.phase='preparing' AND p.early_ack)").bind(link)
}

pub(super) fn hold_completion(stream: Id, upload: Uuid, hash: &str, token: Uuid) -> Statement<'_> {
    sqlx::query("UPDATE uploads SET output_stream=? WHERE id=? AND manifest_hash=? AND completion_token=? AND state='completing' AND output_stream IS NULL AND completion_expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(stream).bind(upload).bind(hash).bind(token)
}

pub(super) fn release_completion(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET state='active',output_stream=NULL,completion_token=NULL,completion_expires_at=NULL WHERE output_stream=? AND state='completing'").bind(stream)
}
