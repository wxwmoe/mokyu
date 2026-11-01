use crate::model::Id;
use sqlx::Postgres;
use uuid::Uuid;

pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, Postgres>;
type Statement<'q> = sqlx::query::Query<'q, Postgres, <Postgres as sqlx::Database>::Arguments>;
type Rows<'q, T> = sqlx::query::QueryAs<'q, Postgres, T, <Postgres as sqlx::Database>::Arguments>;

pub(in super::super) fn staged_source(
    task: Uuid,
    stream: Id,
) -> Rows<'static, super::super::queries::replication::Source> {
    sqlx::query_as("SELECT s.id,s.object_key,s.metadata,s.checksums,s.public_read,s.size,s.etag,s.payload_type FROM replica_targets t JOIN replica_publications p ON p.id=t.publication_id JOIN streams s ON s.id=p.source_pin WHERE t.task_id=$1 AND s.id=$2 AND p.phase='preparing' AND t.phase='preparing' AND s.state IN ('writing','ready','retired') FOR SHARE OF s,p,t")
        .bind(task).bind(stream)
}

pub(in super::super) fn prepared(task: Uuid, candidate: Id) -> Statement<'static> {
    sqlx::query("UPDATE replica_targets SET phase='prepared' WHERE task_id=$1 AND candidate=$2 AND phase='preparing' AND EXISTS(SELECT 1 FROM replica_publications p WHERE p.id=replica_targets.publication_id AND p.phase='preparing')")
        .bind(task).bind(candidate)
}

pub(in super::super) fn original(
    publication: Uuid,
    stream: Id,
) -> Rows<'static, super::super::queries::replication::Source> {
    sqlx::query_as("SELECT s.id,s.object_key,s.metadata,s.checksums,s.public_read,s.size,s.etag,s.payload_type FROM replica_publications p JOIN streams s ON s.id=p.source_pin WHERE p.id=$1 AND s.id=$2 AND s.state IN ('writing','ready','retired') FOR UPDATE")
        .bind(publication)
        .bind(stream)
}

pub(in super::super) fn target(
    publication: Uuid,
    phase: &str,
) -> Rows<'_, super::super::replica_transactions::Target> {
    sqlx::query_as("SELECT link_id,link_revision,target_bucket,target_key,storage_id,profile_revision,authority,permissions,expected_version,candidate,task_id FROM replica_targets WHERE publication_id=$1 AND phase=$2 ORDER BY target_bucket,target_key,link_id LIMIT 1 FOR UPDATE")
        .bind(publication)
        .bind(phase)
}

pub(in super::super) fn begin<'a>(
    id: Uuid,
    intent: &'a serde_json::Value,
    hash: &'a [u8],
    early: bool,
) -> Statement<'a> {
    sqlx::query("UPDATE replica_publications SET phase='preparing',intent=$1,intent_hash=$2,early_ack=$3 WHERE id=$4 AND phase='admitted'")
        .bind(sqlx::types::Json(intent))
        .bind(hash)
        .bind(early)
        .bind(id)
}

pub(in super::super) fn start_child(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE transfer_runs SET phase='plan',scanned=TRUE,sealed=TRUE WHERE task_id=$1 AND phase='scan'")
        .bind(task)
}

pub(in super::super) fn reserve_item(task: Uuid, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE transfer_items SET candidate=$1 WHERE task_id=$2 AND state='planning' AND candidate IS NULL")
        .bind(stream)
        .bind(task)
}

pub(in super::super) fn delegate(
    publication: Uuid,
    link: Uuid,
    task: Uuid,
    stream: Id,
) -> Statement<'static> {
    sqlx::query("UPDATE replica_targets SET candidate=$1,task_id=$2,phase='preparing' WHERE publication_id=$3 AND link_id=$4 AND phase='admitted'")
        .bind(stream)
        .bind(task)
        .bind(publication)
        .bind(link)
}

pub(in super::super) fn identity(
    publication: Uuid,
) -> Rows<'static, (String, Option<Vec<u8>>, bool)> {
    sqlx::query_as(
        "SELECT phase,intent_hash,early_ack FROM replica_publications WHERE id=$1 FOR UPDATE",
    )
    .bind(publication)
}

pub(in super::super) fn link(link: Uuid, source: Uuid, target: Uuid) -> Rows<'static, (i64, bool)> {
    sqlx::query_as("SELECT revision,synchronous FROM redundancy_links WHERE id=$1 AND source_bucket=$2 AND target_bucket=$3 FOR SHARE")
        .bind(link)
        .bind(source)
        .bind(target)
}

pub(in super::super) fn locally_pending(stream: Id) -> Rows<'static, (bool,)> {
    sqlx::query_as("SELECT EXISTS(SELECT 1 FROM raw_ranges x JOIN raw_sources r ON r.id=x.source_id WHERE x.stream_id=$1 AND r.state='ready' AND r.quarantine_code IS NULL AND (r.pending_upload OR r.durability IN ('buffered','volatile'))) OR EXISTS(SELECT 1 FROM extents e JOIN pending_uploads p ON p.chunk_id=e.chunk_id WHERE e.stream_id=$1) OR EXISTS(SELECT 1 FROM extents e JOIN chunks c ON c.id=e.chunk_id JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id WHERE e.stream_id=$1 AND l.sync_pending AND l.quarantined_at IS NULL)").bind(stream)
}

pub(in super::super) fn readiness(id: Uuid) -> Rows<'static, (bool, bool)> {
    sqlx::query_as("SELECT NOT EXISTS(SELECT 1 FROM replica_targets WHERE publication_id=$1 AND phase NOT IN ('prepared','published')),EXISTS(SELECT 1 FROM replica_targets r JOIN tasks t ON t.id=r.task_id WHERE r.publication_id=$1 AND (r.phase='aborted' OR t.state='failed'))").bind(id)
}

pub(in super::super) fn root(id: Uuid) -> Rows<'static, (Uuid, String, Option<Id>, bool)> {
    sqlx::query_as("SELECT source_bucket,object_key,source_pin,early_ack FROM replica_publications WHERE id=$1 AND phase='preparing' FOR UPDATE").bind(id)
}

pub(in super::super) fn candidate(stream: Id) -> Rows<'static, crate::model::PublishingStream> {
    sqlx::query_as("SELECT * FROM streams WHERE id=$1 FOR UPDATE").bind(stream)
}

pub(in super::super) fn installed(publication: Uuid, link: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE replica_targets SET phase='published',candidate=NULL WHERE publication_id=$1 AND link_id=$2 AND phase='prepared'").bind(publication).bind(link)
}

pub(in super::super) fn finish(id: Uuid, result: &serde_json::Value) -> Statement<'_> {
    sqlx::query("UPDATE replica_publications SET phase='published',source_pin=NULL,result=$1,updated_at=now() WHERE id=$2 AND phase='preparing'").bind(sqlx::types::Json(result)).bind(id)
}

pub(in super::super) fn publication_for_task(task: Uuid) -> Rows<'static, (Uuid,)> {
    sqlx::query_as("SELECT publication_id FROM replica_targets WHERE task_id=$1").bind(task)
}

pub(in super::super) fn intent(id: Uuid) -> Rows<'static, (sqlx::types::Json<serde_json::Value>,)> {
    sqlx::query_as("SELECT intent FROM replica_publications WHERE id=$1").bind(id)
}
pub(in super::super) fn state(
    id: Uuid,
) -> Rows<'static, (String, Option<sqlx::types::Json<serde_json::Value>>)> {
    sqlx::query_as("SELECT phase,result FROM replica_publications WHERE id=$1").bind(id)
}

pub(in super::super) fn node(publication: Uuid, node: Uuid, operation: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE replica_publications SET intent=jsonb_set(intent,'{node}',jsonb_build_array($2::uuid,$3::uuid)) WHERE id=$1 AND phase='preparing'").bind(publication).bind(node).bind(operation)
}

pub(in super::super) fn abort_target(publication: Uuid, link: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE replica_targets SET phase='aborted',candidate=NULL WHERE publication_id=$1 AND link_id=$2 AND phase IN ('admitted','preparing','prepared')").bind(publication).bind(link)
}

pub(in super::super) fn cancel_child(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE transfer_runs SET phase='cancel' WHERE task_id=$1").bind(task)
}

pub(in super::super) fn abort(id: Uuid, result: &serde_json::Value) -> Statement<'_> {
    sqlx::query("UPDATE replica_publications SET phase='aborted',source_pin=NULL,result=$1,updated_at=now() WHERE id=$2 AND phase='preparing'").bind(sqlx::types::Json(result)).bind(id)
}

pub(in super::super) fn unclaim(bucket: Uuid, key: &str, stream: Id) -> Statement<'_> {
    sqlx::query(
        "UPDATE objects SET write_token=$1 WHERE bucket_id=$2 AND object_key=$3 AND write_token=$4",
    )
    .bind(Id::default())
    .bind(bucket)
    .bind(key)
    .bind(stream)
}

pub(in super::super) fn park(task: crate::model::task::Execution) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='paused',error='replica_pending',detail=jsonb_set(detail,'{replica_parked}','true'),lease_token=NULL,lease_node_id=NULL,lease_expires_at=NULL WHERE id=$1 AND lease_token=$2 AND state='running'").bind(task.id).bind(task.token)
}

pub(in super::super) fn wake(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='queued',error=NULL WHERE id=$1 AND state='paused' AND error='replica_pending'").bind(task)
}

pub(in super::super) fn parked_stream(task: Uuid) -> Rows<'static, (Id, String)> {
    sqlx::query_as("SELECT s.id,p.phase FROM streams s JOIN replica_publications p ON p.id=s.publication_id WHERE s.owner_task_id=$1 AND s.state='writing' AND p.phase='preparing' ORDER BY s.id LIMIT 1").bind(task)
}

pub(in super::super) fn failed(task: Uuid) -> Rows<'static, (Uuid, bool)> {
    sqlx::query_as("SELECT p.id,p.early_ack FROM replica_publications p WHERE p.phase='preparing' AND (EXISTS(SELECT 1 FROM replica_targets t WHERE t.publication_id=p.id AND t.task_id=$1) OR EXISTS(SELECT 1 FROM streams s WHERE s.id=p.source_pin AND s.owner_task_id=$1)) LIMIT 1").bind(task)
}

pub(in super::super) fn fail_parent(task: Uuid, reason: &str) -> Statement<'_> {
    sqlx::query("UPDATE tasks SET state='failed',error=$1 WHERE id=$2 AND state='paused' AND error='replica_pending'").bind(reason).bind(task)
}

pub(in super::super) fn draining(link: Uuid) -> Rows<'static, (bool,)> {
    sqlx::query_as("SELECT EXISTS(SELECT 1 FROM replica_targets WHERE link_id=$1 AND phase IN ('admitted','preparing','prepared'))").bind(link)
}

pub(in super::super) fn drained(link: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='queued',error=NULL WHERE id=$1 AND state='paused' AND (error='replica_pending' OR error='replica_drain' AND NOT EXISTS(SELECT 1 FROM replica_targets t WHERE t.link_id=tasks.id AND t.phase IN ('admitted','preparing','prepared')))").bind(link)
}

pub(in super::super) fn abandoned_admissions(batch: i64) -> Rows<'static, (Id,)> {
    sqlx::query_as("SELECT s.id FROM replica_publications p JOIN streams s ON s.id=p.source_pin WHERE p.phase='admitted' AND s.state='abandoned' ORDER BY p.id LIMIT $1").bind(batch)
}

pub(in super::super) fn admission(stream: Id, rejected: bool) -> Rows<'static, (Uuid, Uuid)> {
    sqlx::query_as("SELECT p.id,p.source_bucket FROM replica_publications p JOIN streams s ON s.id=p.source_pin WHERE p.source_pin=$1 AND p.phase='admitted' AND ($2 OR s.state='abandoned') FOR UPDATE").bind(stream).bind(rejected)
}

pub(in super::super) fn forget_admission(id: Uuid, result: &serde_json::Value) -> Statement<'_> {
    sqlx::query("UPDATE replica_publications SET phase='aborted',source_pin=NULL,result=$1 WHERE id=$2 AND phase='admitted'").bind(sqlx::types::Json(result)).bind(id)
}

pub(in super::super) fn expired(age: f64, batch: i64) -> Rows<'static, (Uuid,)> {
    sqlx::query_as("SELECT id FROM replica_publications WHERE phase IN ('published','aborted') AND updated_at<now()-$1::double precision*interval '1 second' ORDER BY updated_at,id LIMIT $2").bind(age.max(600.0)).bind(batch)
}

pub(in super::super) fn terminal(bucket: Uuid) -> Rows<'static, (Uuid,)> {
    sqlx::query_as("SELECT p.id FROM replica_publications p WHERE (p.source_bucket=$1 OR EXISTS(SELECT 1 FROM replica_targets t WHERE t.publication_id=p.id AND t.target_bucket=$1)) AND p.phase IN ('published','aborted') ORDER BY p.id LIMIT 128").bind(bucket)
}

pub(in super::super) fn detach(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE streams SET publication_id=NULL WHERE publication_id=$1").bind(id)
}

pub(in super::super) fn prune(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM replica_publications WHERE id=$1 AND phase IN ('published','aborted')")
        .bind(id)
}

pub(in super::super) fn retry(link: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='queued',error=NULL WHERE state='failed' AND EXISTS(SELECT 1 FROM replica_targets t JOIN replica_publications p ON p.id=t.publication_id WHERE t.link_id=$1 AND t.task_id=tasks.id AND t.phase='preparing' AND p.phase='preparing' AND p.early_ack)").bind(link)
}

pub(in super::super) fn node_failure(
    node: Uuid,
    operation: Uuid,
) -> Rows<'static, (Option<String>,)> {
    sqlx::query_as("SELECT p.result->>'error' FROM node_writes w JOIN streams s ON s.id=w.stream_id JOIN replica_publications p ON p.id=s.publication_id WHERE w.node_id=$1 AND w.operation_id=$2 AND p.phase='aborted'").bind(node).bind(operation)
}

pub(in super::super) fn hold_completion(
    stream: Id,
    upload: Uuid,
    hash: &str,
    token: Uuid,
) -> Statement<'_> {
    sqlx::query("UPDATE uploads SET output_stream=$1 WHERE id=$2 AND manifest_hash=$3 AND completion_token=$4 AND state='completing' AND output_stream IS NULL AND completion_expires_at>clock_timestamp()").bind(stream).bind(upload).bind(hash).bind(token)
}

pub(in super::super) fn release_completion(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET state='active',output_stream=NULL,completion_token=NULL,completion_expires_at=NULL WHERE output_stream=$1 AND state='completing'").bind(stream)
}
