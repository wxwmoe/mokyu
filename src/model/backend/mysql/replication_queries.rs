use crate::{
    authorization::Principal,
    model::{
        Id,
        raw::RawSource,
        replication::{Item, Options, Progress, Replication, Snapshot},
        task::Execution,
    },
};
use serde_json::Value;
use sqlx::{MySql, types::Json};
use uuid::Uuid;

type Statement<'q> = sqlx::query::Query<'q, MySql, <MySql as sqlx::Database>::Arguments>;
type Rows<'q, T> = sqlx::query::QueryAs<'q, MySql, T, <MySql as sqlx::Database>::Arguments>;
type Scalar<'q, T> = sqlx::query::QueryScalar<'q, MySql, T, <MySql as sqlx::Database>::Arguments>;

#[derive(sqlx::FromRow)]
pub(super) struct Run {
    pub task_id: Uuid,
    pub created_by: Option<Uuid>,
    pub options: Json<Options>,
    pub authority: Json<Principal>,
    pub storage_id: Uuid,
    pub profile_revision: i64,
    pub phase: String,
    pub cursor_key: Option<String>,
    pub progress: Json<Progress>,
    pub generation: i64,
    pub state: String,
    pub error: Option<String>,
    pub mode: String,
}
impl Run {
    pub(super) fn view(self) -> Replication {
        Replication {
            id: self.task_id,
            state: self.state,
            phase: self.phase,
            options: self.options.0,
            progress: self.progress.0,
            error: self.error,
        }
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct Source {
    pub id: Id,
    pub object_key: String,
    pub metadata: Json<Value>,
    pub checksums: Json<Value>,
    pub public_read: bool,
    pub size: i64,
    pub etag: String,
    pub payload_type: String,
}
impl Source {
    pub(super) fn snapshot(&self) -> Snapshot {
        Snapshot {
            metadata: self.metadata.0.clone(),
            checksums: self.checksums.0.clone(),
            public: self.public_read,
            size: self.size,
            etag: self.etag.clone(),
            payload: self.payload_type.clone(),
        }
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct Entry {
    pub id: Id,
    pub source_key: String,
    pub target_key: String,
    pub source_version: Option<Id>,
    pub source_pin: Option<Id>,
    pub target_version: Option<Id>,
    pub candidate: Option<Id>,
    pub output_version: Option<Id>,
    pub snapshot: Json<Snapshot>,
    pub state: String,
    pub position_bytes: i64,
    pub error: Option<String>,
}
impl Entry {
    pub(super) fn view(self) -> Item {
        Item {
            id: self.id,
            source_key: self.source_key,
            target_key: self.target_key,
            source_version: self.source_version,
            output_version: self.output_version,
            state: self.state,
            error: self.error,
        }
    }
}

pub(super) fn run<'q>(id: Uuid) -> Rows<'q, Run> {
    sqlx::query_as("SELECT r.task_id,r.created_by,r.options,r.authority,r.storage_id,r.profile_revision,r.phase,r.cursor_key,r.progress,r.generation,t.state,t.error,JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.execution_mode')) AS mode FROM transfer_runs r JOIN tasks t ON t.id=r.task_id WHERE r.task_id=?")
        .bind(id)
}

pub(super) fn owned<'q>(task: Execution) -> Scalar<'q, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM tasks WHERE id=? AND execution_token=? AND state='running' FOR UPDATE",
    )
    .bind(task.id)
    .bind(task.token)
}

pub(super) fn task_lock<'q>(id: Uuid) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE id=? FOR UPDATE").bind(id)
}

pub(super) fn mode<'q>() -> Scalar<'q, String> {
    sqlx::query_scalar("SELECT 'single'")
}

pub(super) fn list<'q>(user: Option<Uuid>, after: Option<Uuid>, limit: i64) -> Rows<'q, Run> {
    sqlx::query_as("SELECT r.task_id,r.created_by,r.options,r.authority,r.storage_id,r.profile_revision,r.phase,r.cursor_key,r.progress,r.generation,t.state,t.error,JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.execution_mode')) AS mode FROM transfer_runs r JOIN tasks t ON t.id=r.task_id WHERE (r.created_by=? OR (r.created_by IS NULL AND ? IS NULL)) AND (? IS NULL OR r.task_id>?) ORDER BY r.task_id LIMIT ?")
        .bind(user)
        .bind(user)
        .bind(after)
        .bind(after)
        .bind(limit)
}

pub(super) fn create<'q>(
    id: Uuid,
    actor: &'q Principal,
    options: &'q Options,
    storage: Uuid,
    revision: i64,
) -> Statement<'q> {
    sqlx::query("INSERT INTO transfer_runs(task_id,created_by,options,authority,storage_id,profile_revision,progress) VALUES(?,?,?,?,?,?,?)")
        .bind(id)
        .bind(actor.user_id())
        .bind(Json(options))
        .bind(Json(actor))
        .bind(storage)
        .bind(revision)
        .bind(Json(Progress::default()))
}

pub(super) fn save<'q>(
    id: Uuid,
    phase: &'q str,
    cursor: Option<&'q str>,
    progress: &'q Progress,
) -> Statement<'q> {
    sqlx::query("UPDATE transfer_runs SET phase=?,cursor_key=?,progress=? WHERE task_id=?")
        .bind(phase)
        .bind(cursor)
        .bind(Json(progress))
        .bind(id)
}

pub(super) fn task_progress<'q>(id: Uuid, detail: Value, completed: i64) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET detail=?,processed=?,updated_at=UTC_TIMESTAMP(6) WHERE id=?")
        .bind(Json(detail))
        .bind(completed)
        .bind(id)
}

pub(super) fn task_state<'q>(id: Uuid, state: &'q str, error: Option<&'q str>) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET state=?,error=?,execution_token=NULL,updated_at=UTC_TIMESTAMP(6) WHERE id=?")
        .bind(state)
        .bind(error)
        .bind(id)
}

pub(super) fn actor<'q>(id: Uuid, actor: &'q Principal) -> Statement<'q> {
    sqlx::query("UPDATE transfer_runs SET authority=? WHERE task_id=?")
        .bind(Json(actor))
        .bind(id)
}

pub(super) fn resume<'q>(id: Uuid) -> Statement<'q> {
    sqlx::query("UPDATE transfer_runs SET generation=generation+1,phase=CASE WHEN phase='publish' THEN 'verify' ELSE phase END WHERE task_id=?")
        .bind(id)
}

pub(super) fn profile<'q>(bucket: Uuid) -> Rows<'q, (Uuid, i64)> {
    sqlx::query_as("SELECT b.storage_id,p.revision FROM buckets b JOIN storages s ON s.id=b.storage_id JOIN storage_profiles p ON p.storage_id=b.storage_id WHERE b.id=? AND b.state='active' AND s.enabled FOR UPDATE")
        .bind(bucket)
}

pub(super) fn source<'q>(bucket: Uuid, key: &'q str) -> Rows<'q, Source> {
    sqlx::query_as("SELECT s.id,CONVERT(o.object_key USING utf8mb4) AS object_key,s.metadata,s.checksums,s.public_read,s.size,s.etag,s.payload_type FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=? AND o.object_key=? AND s.state='ready' FOR UPDATE")
        .bind(bucket)
        .bind(key.as_bytes())
}

pub(super) fn scan<'q>(bucket: Uuid, after: Option<&'q str>, prefix: &'q str) -> Rows<'q, Source> {
    sqlx::query_as("SELECT s.id,CONVERT(o.object_key USING utf8mb4) AS object_key,s.metadata,s.checksums,s.public_read,s.size,s.etag,s.payload_type FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=? AND (? IS NULL OR o.object_key>?) AND o.object_key>=? AND left(o.object_key,length(?))=? AND s.state='ready' ORDER BY o.object_key LIMIT 64 FOR UPDATE")
        .bind(bucket)
        .bind(after.map(str::as_bytes))
        .bind(after.map(str::as_bytes))
        .bind(prefix.as_bytes())
        .bind(prefix.as_bytes())
        .bind(prefix.as_bytes())
}

pub(super) fn insert_item<'q>(
    task: Uuid,
    key: &'q str,
    target: &'q str,
    source: Option<Id>,
    expected: Option<Id>,
    snapshot: &'q Snapshot,
    state: &'q str,
    error: Option<&'q str>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO transfer_items(task_id,source_key,target_key,source_version,source_pin,target_version,snapshot,state,error) VALUES(?,?,?,?,CASE WHEN ?='planning' THEN ? ELSE NULL END,?,?,?,?)")
        .bind(task)
        .bind(key)
        .bind(target)
        .bind(source)
        .bind(state)
        .bind(source)
        .bind(expected)
        .bind(Json(snapshot))
        .bind(state)
        .bind(error)
}

pub(super) fn items<'q>(id: Uuid, after: Option<Id>, limit: i64) -> Rows<'q, Entry> {
    sqlx::query_as("SELECT id,source_key,target_key,source_version,source_pin,target_version,candidate,output_version,snapshot,state,position_bytes,error FROM transfer_items WHERE task_id=? AND (? IS NULL OR id>?) ORDER BY id LIMIT ?")
        .bind(id)
        .bind(after)
        .bind(after)
        .bind(limit)
}

pub(super) fn pending<'q>(id: Uuid, state: &'q str) -> Rows<'q, Entry> {
    sqlx::query_as("SELECT id,source_key,target_key,source_version,source_pin,target_version,candidate,output_version,snapshot,state,position_bytes,error FROM transfer_items WHERE task_id=? AND state=? ORDER BY id LIMIT 1 FOR UPDATE")
        .bind(id)
        .bind(state)
}

pub(super) fn live<'q>(id: Uuid) -> Rows<'q, Entry> {
    sqlx::query_as("SELECT id,source_key,target_key,source_version,source_pin,target_version,candidate,output_version,snapshot,state,position_bytes,error FROM transfer_items WHERE task_id=? AND (source_pin IS NOT NULL OR candidate IS NOT NULL) ORDER BY id LIMIT 32 FOR UPDATE")
        .bind(id)
}

pub(super) fn advance<'q>(id: Id, state: &'q str, position: i64) -> Statement<'q> {
    sqlx::query("UPDATE transfer_items SET state=?,position_bytes=? WHERE id=?")
        .bind(state)
        .bind(position)
        .bind(id)
}

pub(super) fn candidate<'q>(id: Id, candidate: Id) -> Statement<'q> {
    sqlx::query("UPDATE transfer_items SET candidate=?,state='copying',position_bytes=0 WHERE id=?")
        .bind(candidate)
        .bind(id)
}

pub(super) fn replan<'q>(id: Id) -> Statement<'q> {
    sqlx::query(
        "UPDATE transfer_items SET candidate=NULL,state='planning',position_bytes=0 WHERE id=?",
    )
    .bind(id)
}

pub(super) fn finish_item<'q>(
    id: Id,
    state: &'q str,
    output: Option<Id>,
    error: Option<&'q str>,
) -> Statement<'q> {
    sqlx::query("UPDATE transfer_items SET state=?,output_version=?,error=?,source_pin=NULL,candidate=NULL WHERE id=?")
        .bind(state)
        .bind(output)
        .bind(error)
        .bind(id)
}

pub(super) fn adopt<'q>(candidate: Id, task: Execution) -> Statement<'q> {
    sqlx::query("UPDATE streams SET owner_execution_token=?,touched_at=UTC_TIMESTAMP(6) WHERE id=? AND owner_task_id=? AND state='writing'")
        .bind(task.token)
        .bind(candidate)
        .bind(task.id)
}

pub(super) fn finish_stream<'q>(
    id: Id,
    size: i64,
    etag: &'q str,
    checksums: Value,
) -> Statement<'q> {
    sqlx::query("UPDATE streams SET size=?,etag=?,checksums=?,touched_at=UTC_TIMESTAMP(6) WHERE id=? AND state='writing'")
        .bind(size)
        .bind(etag)
        .bind(Json(checksums))
        .bind(id)
}

pub(super) fn abandon<'q>(id: Id) -> Statement<'q> {
    sqlx::query("UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_execution_token=NULL,touched_at=UTC_TIMESTAMP(6) WHERE id=? AND state='writing'")
        .bind(id)
}

pub(super) fn unverified<'q>(id: Uuid, generation: i64) -> Scalar<'q, Id> {
    sqlx::query_scalar("SELECT source_id FROM transfer_sources WHERE task_id=? AND source_type='raw' AND generation<? ORDER BY source_id LIMIT 1")
        .bind(id)
        .bind(generation)
}

pub(super) fn proof<'q>(
    task: Uuid,
    source: Id,
    generation: i64,
) -> Scalar<'q, Option<Json<RawSource>>> {
    sqlx::query_scalar("SELECT snapshot FROM transfer_sources WHERE task_id=? AND source_type='raw' AND source_id=? AND generation=?")
        .bind(task)
        .bind(source)
        .bind(generation)
}

pub(super) fn verified<'q>(task: Uuid, source: &'q RawSource, generation: i64) -> Statement<'q> {
    sqlx::query("UPDATE transfer_sources SET snapshot=?,generation=? WHERE task_id=? AND source_type='raw' AND source_id=? AND generation<?")
        .bind(Json(source))
        .bind(generation)
        .bind(task)
        .bind(source.id)
        .bind(generation)
}

pub(super) fn invalidate<'q>(task: Uuid, source: Id) -> Statement<'q> {
    sqlx::query("UPDATE transfer_sources SET generation=0,snapshot=NULL WHERE task_id=? AND source_type='raw' AND source_id=?")
        .bind(task)
        .bind(source)
}

pub(super) fn same_data<'q>(source: Id, target: Id, payload: &'q str) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT CASE WHEN ?='raw' THEN NOT EXISTS(SELECT 1 FROM raw_ranges s LEFT JOIN raw_ranges t ON t.stream_id=? AND t.offset_bytes=s.offset_bytes AND t.length=s.length AND t.source_id=s.source_id AND t.source_offset=s.source_offset WHERE s.stream_id=? AND t.source_id IS NULL) ELSE NOT EXISTS(SELECT 1 FROM extents s LEFT JOIN extents t ON t.stream_id=? AND t.offset_bytes=s.offset_bytes AND t.length=s.length AND t.chunk_id=s.chunk_id WHERE s.stream_id=? AND (t.chunk_id IS NULL OR s.fragment_id IS NOT NULL)) END")
        .bind(payload)
        .bind(target)
        .bind(source)
        .bind(target)
        .bind(source)
}

pub(super) fn extents<'q>(
    stream: Id,
    offset: i64,
) -> Rows<'q, (i64, i32, Option<Id>, Option<Id>, Option<Id>)> {
    sqlx::query_as("SELECT e.offset_bytes,e.length,e.chunk_id,e.fragment_id,c.pack_id FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=? AND e.offset_bytes>=? ORDER BY e.offset_bytes LIMIT 128")
        .bind(stream)
        .bind(offset)
}

pub(super) fn raw<'q>(id: Id) -> Rows<'q, RawSource> {
    sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id=? FOR UPDATE")
        .bind(id)
}

pub(super) fn protect_raw<'q>(id: Id) -> Statement<'q> {
    sqlx::query("UPDATE raw_sources SET revision=revision+1 WHERE id=? AND state='ready'").bind(id)
}

pub(super) fn ranges<'q>(stream: Id, offset: i64) -> Rows<'q, crate::model::raw::RawRange> {
    sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,e.offset_bytes,e.length,e.source_offset FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id WHERE e.stream_id=? AND e.offset_bytes>=? ORDER BY e.offset_bytes LIMIT 128")
        .bind(stream)
        .bind(offset)
}

pub(super) fn units(task: Uuid, units: &[(&str, Id)]) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "INSERT IGNORE INTO transfer_sources(task_id,source_type,source_id) ",
    );
    query.push_values(units, |mut row, (source_type, id)| {
        row.push_bind(task).push_bind(*source_type).push_bind(*id);
    });
    query.push("");
    query
}

pub(super) fn selected(bucket: Uuid, keys: &[String]) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT s.id,CONVERT(o.object_key USING utf8mb4) AS object_key,s.metadata,s.checksums,s.public_read,s.size,s.etag,s.payload_type FROM objects o JOIN streams s ON s.id=o.stream_id WHERE s.state='ready' AND o.bucket_id=",
    );
    query.push_bind(bucket).push(" AND o.object_key IN (");
    let mut values = query.separated(",");
    for key in keys {
        values.push_bind(key.as_bytes().to_vec());
    }
    values.push_unseparated(") ORDER BY o.object_key FOR UPDATE");
    query
}
