use crate::{
    authorization::Principal,
    model::{
        Id,
        raw::RawSource,
        replication::{Item, Options, Progress, Snapshot, changes::Confirmation},
        task::Execution,
    },
};
use serde_json::Value;
use sqlx::{Sqlite, types::Json};
use uuid::Uuid;

type Statement<'q> = sqlx::query::Query<'q, Sqlite, <Sqlite as sqlx::Database>::Arguments>;
type Rows<'q, T> = sqlx::query::QueryAs<'q, Sqlite, T, <Sqlite as sqlx::Database>::Arguments>;
type Scalar<'q, T> = sqlx::query::QueryScalar<'q, Sqlite, T, <Sqlite as sqlx::Database>::Arguments>;

pub(super) use crate::model::replication::Run;

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
    pub reuse_target: bool,
    pub error: Option<String>,
    pub confirmation: Option<Json<Confirmation>>,
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
    sqlx::query_as("SELECT r.task_id,r.created_by,r.options,r.authority,r.storage_id,r.source_storage_id,r.profile_revision,r.phase,r.cursor_key,r.progress,r.generation,r.scanned,r.captured,r.sealed,r.scan_end,t.state,t.error,json_extract(t.detail,'$.execution_mode') AS mode FROM transfer_runs r JOIN tasks t ON t.id=r.task_id WHERE r.task_id=$1")
        .bind(id)
}

pub(super) fn owned<'q>(task: Execution) -> Scalar<'q, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM tasks WHERE id=$1 AND execution_token=$2 AND state='running'",
    )
    .bind(task.id)
    .bind(task.token)
}

pub(super) fn task_lock<'q>(id: Uuid) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE id=$1").bind(id)
}

pub(super) fn mode<'q>() -> Scalar<'q, String> {
    sqlx::query_scalar("SELECT 'single'")
}

pub(super) fn list<'q>(user: Option<Uuid>, after: Option<Uuid>, limit: i64) -> Rows<'q, Run> {
    sqlx::query_as("SELECT r.task_id,r.created_by,r.options,r.authority,r.storage_id,r.source_storage_id,r.profile_revision,r.phase,r.cursor_key,r.progress,r.generation,r.scanned,r.captured,r.sealed,r.scan_end,t.state,t.error,json_extract(t.detail,'$.execution_mode') AS mode FROM transfer_runs r JOIN tasks t ON t.id=r.task_id WHERE (r.created_by=$1 OR (r.created_by IS NULL AND $1 IS NULL)) AND ($2 IS NULL OR r.task_id>$2) ORDER BY r.task_id LIMIT $3")
        .bind(user)
        .bind(after)
        .bind(limit)
}

pub(super) fn create<'q>(
    id: Uuid,
    actor: &'q Principal,
    options: &'q Options,
    storage: Option<Uuid>,
    revision: Option<i64>,
    source: Option<Uuid>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO transfer_runs(task_id,created_by,options,authority,storage_id,profile_revision,progress,source_storage_id,captured) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
        .bind(id)
        .bind(actor.user_id())
        .bind(Json(options))
        .bind(Json(actor))
        .bind(storage)
        .bind(revision)
        .bind(Json(Progress::default()))
        .bind(source)
        .bind(!options.source_external)
}

pub(super) fn save<'q>(
    id: Uuid,
    phase: &'q str,
    cursor: Option<&'q str>,
    progress: &'q Progress,
    scanned: bool,
    sealed: bool,
    scan_end: Option<&'q str>,
) -> Statement<'q> {
    sqlx::query("UPDATE transfer_runs SET phase=$2,cursor_key=$3,progress=$4,scanned=$5,sealed=$6,scan_end=$7 WHERE task_id=$1")
        .bind(id)
        .bind(phase)
        .bind(cursor)
        .bind(Json(progress))
        .bind(scanned)
        .bind(sealed)
        .bind(scan_end)
}

pub(super) fn task_progress<'q>(id: Uuid, detail: Value, completed: i64) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET detail=$2,processed=$3,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=$1")
        .bind(id)
        .bind(Json(detail))
        .bind(completed)
}

pub(super) fn task_state<'q>(id: Uuid, state: &'q str, error: Option<&'q str>) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET state=$2,error=$3,execution_token=NULL,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=$1")
        .bind(id)
        .bind(state)
        .bind(error)
}

pub(super) fn actor<'q>(id: Uuid, actor: &'q Principal) -> Statement<'q> {
    sqlx::query("UPDATE transfer_runs SET authority=$2 WHERE task_id=$1")
        .bind(id)
        .bind(Json(actor))
}

pub(super) fn resume<'q>(id: Uuid) -> Statement<'q> {
    sqlx::query("UPDATE transfer_runs SET generation=generation+1,phase=CASE WHEN phase='publish' THEN 'verify' ELSE phase END WHERE task_id=$1")
        .bind(id)
}

pub(super) fn profile<'q>(bucket: Uuid) -> Rows<'q, (Uuid, i64)> {
    sqlx::query_as("SELECT b.storage_id,p.revision FROM buckets b JOIN storages s ON s.id=b.storage_id JOIN storage_profiles p ON p.storage_id=b.storage_id WHERE b.id=$1 AND b.state='active' AND s.enabled")
        .bind(bucket)
}

pub(super) fn source<'q>(bucket: Uuid, key: &'q str) -> Rows<'q, Source> {
    sqlx::query_as("SELECT s.id,CAST(o.object_key AS TEXT) AS object_key,s.metadata,s.checksums,s.public_read,s.size,s.etag,s.payload_type FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=$1 AND o.object_key=$2 AND s.state='ready'")
        .bind(bucket)
        .bind(key.as_bytes())
}

pub(super) fn scan<'q>(
    bucket: Uuid,
    after: Option<&'q str>,
    prefix: &'q str,
    end: &'q str,
) -> Rows<'q, Source> {
    sqlx::query_as("SELECT s.id,CAST(o.object_key AS TEXT) AS object_key,s.metadata,s.checksums,s.public_read,s.size,s.etag,s.payload_type FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=$1 AND ($2 IS NULL OR o.object_key>$2) AND o.object_key>=$3 AND o.object_key<=$4 AND substr(o.object_key,1,length($3))=$3 AND s.state='ready' ORDER BY o.object_key LIMIT 64")
        .bind(bucket)
        .bind(after.map(str::as_bytes))
        .bind(prefix.as_bytes())
        .bind(end.as_bytes())
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
    sqlx::query("INSERT INTO transfer_items(task_id,source_key,target_key,source_version,source_pin,target_version,snapshot,state,error) VALUES($1,$2,$3,$4,CASE WHEN $7='planning' THEN $4 ELSE NULL END,$5,$6,$7,$8)")
        .bind(task)
        .bind(key)
        .bind(target)
        .bind(source)
        .bind(expected)
        .bind(Json(snapshot))
        .bind(state)
        .bind(error)
}

pub(super) fn items<'q>(id: Uuid, after: Option<Id>, limit: i64) -> Rows<'q, Entry> {
    sqlx::query_as("SELECT id,source_key,target_key,source_version,source_pin,target_version,candidate,output_version,snapshot,state,position_bytes,reuse_target,error,confirmation FROM transfer_items WHERE task_id=$1 AND ($2 IS NULL OR id>$2) ORDER BY id LIMIT $3")
        .bind(id)
        .bind(after)
        .bind(limit)
}

pub(super) fn pending<'q>(id: Uuid, state: &'q str) -> Rows<'q, Entry> {
    sqlx::query_as("SELECT id,source_key,target_key,source_version,source_pin,target_version,candidate,output_version,snapshot,state,position_bytes,reuse_target,error,confirmation FROM transfer_items WHERE task_id=$1 AND state=$2 ORDER BY id LIMIT 1")
        .bind(id)
        .bind(state)
}

pub(super) fn live<'q>(id: Uuid) -> Rows<'q, Entry> {
    sqlx::query_as("SELECT id,source_key,target_key,source_version,source_pin,target_version,candidate,output_version,snapshot,state,position_bytes,reuse_target,error,confirmation FROM transfer_items WHERE task_id=$1 AND (source_pin IS NOT NULL OR candidate IS NOT NULL) ORDER BY id LIMIT 32")
        .bind(id)
}

pub(super) fn advance<'q>(id: Id, state: &'q str, position: i64) -> Statement<'q> {
    sqlx::query("UPDATE transfer_items SET state=$2,position_bytes=$3 WHERE id=$1")
        .bind(id)
        .bind(state)
        .bind(position)
}

pub(super) fn candidate<'q>(id: Id, candidate: Id) -> Statement<'q> {
    sqlx::query(
        "UPDATE transfer_items SET candidate=$2,state='copying',position_bytes=0 WHERE id=$1",
    )
    .bind(id)
    .bind(candidate)
}

pub(super) fn replan<'q>(id: Id) -> Statement<'q> {
    sqlx::query(
        "UPDATE transfer_items SET candidate=NULL,state='planning',position_bytes=0,reuse_target=FALSE WHERE id=$1",
    )
    .bind(id)
}

pub(super) fn finish_item<'q>(
    id: Id,
    state: &'q str,
    output: Option<Id>,
    error: Option<&'q str>,
) -> Statement<'q> {
    sqlx::query("UPDATE transfer_items SET state=$2,output_version=$3,error=$4,source_pin=NULL,candidate=NULL WHERE id=$1")
        .bind(id)
        .bind(state)
        .bind(output)
        .bind(error)
}

pub(super) fn adopt<'q>(candidate: Id, task: Execution) -> Statement<'q> {
    sqlx::query("UPDATE streams SET owner_execution_token=$2,touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=$1 AND owner_task_id=$3 AND state='writing'")
        .bind(candidate)
        .bind(task.token)
        .bind(task.id)
}

pub(super) fn finish_stream<'q>(
    id: Id,
    size: i64,
    etag: &'q str,
    checksums: Value,
) -> Statement<'q> {
    sqlx::query("UPDATE streams SET size=$2,etag=$3,checksums=$4,touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=$1 AND state='writing'")
        .bind(id)
        .bind(size)
        .bind(etag)
        .bind(Json(checksums))
}

pub(super) fn abandon<'q>(id: Id) -> Statement<'q> {
    sqlx::query("UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_execution_token=NULL,touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=$1 AND state='writing'")
        .bind(id)
}

pub(super) fn unverified<'q>(id: Uuid, generation: i64) -> Scalar<'q, Id> {
    sqlx::query_scalar("SELECT source_id FROM transfer_sources WHERE task_id=$1 AND source_type='raw' AND generation<$2 ORDER BY source_id LIMIT 1")
        .bind(id)
        .bind(generation)
}

pub(super) fn proof<'q>(
    task: Uuid,
    source: Id,
    generation: i64,
) -> Scalar<'q, Option<Json<RawSource>>> {
    sqlx::query_scalar("SELECT snapshot FROM transfer_sources WHERE task_id=$1 AND source_type='raw' AND source_id=$2 AND generation=$3")
        .bind(task)
        .bind(source)
        .bind(generation)
}

pub(super) fn verified<'q>(task: Uuid, source: &'q RawSource, generation: i64) -> Statement<'q> {
    sqlx::query("UPDATE transfer_sources SET snapshot=$3,generation=$4 WHERE task_id=$1 AND source_type='raw' AND source_id=$2 AND generation<$4")
        .bind(task)
        .bind(source.id)
        .bind(Json(source))
        .bind(generation)
}

pub(super) fn invalidate<'q>(task: Uuid, source: Id) -> Statement<'q> {
    sqlx::query("UPDATE transfer_sources SET generation=0,snapshot=NULL WHERE task_id=$1 AND source_type='raw' AND source_id=$2")
        .bind(task)
        .bind(source)
}

pub(super) fn same_data<'q>(source: Id, target: Id, payload: &'q str) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM transfer_items WHERE source_version=$1 AND output_version=$2 AND error IS NULL AND state IN ('completed','skipped')) OR (EXISTS(SELECT 1 FROM streams s JOIN streams t ON t.id=$2 WHERE s.id=$1 AND s.payload_type=t.payload_type) AND CASE WHEN $3='raw' THEN NOT EXISTS(SELECT 1 FROM raw_ranges s LEFT JOIN raw_sources sr ON sr.id=s.source_id WHERE s.stream_id=$1 AND NOT EXISTS(SELECT 1 FROM raw_ranges t JOIN raw_sources tr ON tr.id=t.source_id WHERE t.stream_id=$2 AND t.offset_bytes=s.offset_bytes AND t.length=s.length AND t.source_offset=s.source_offset AND (t.source_id=s.source_id OR (tr.size=sr.size AND length(sr.content_hash)=32 AND tr.content_hash=sr.content_hash)))) ELSE NOT EXISTS(SELECT 1 FROM extents s LEFT JOIN chunks sc ON sc.id=s.chunk_id WHERE s.stream_id=$1 AND (s.fragment_id IS NOT NULL OR NOT EXISTS(SELECT 1 FROM extents t JOIN chunks tc ON tc.id=t.chunk_id WHERE t.stream_id=$2 AND t.offset_bytes=s.offset_bytes AND t.length=s.length AND t.source_offset=s.source_offset AND t.fragment_id IS NULL AND (t.chunk_id=s.chunk_id OR (tc.hash=sc.hash AND tc.raw_size=sc.raw_size))))) END)")
        .bind(source)
        .bind(target)
        .bind(payload)
}

pub(super) fn ready<'q>(stream: Id, payload: &'q str) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT CASE WHEN $2='raw' THEN NOT EXISTS(SELECT 1 FROM raw_ranges x LEFT JOIN raw_sources r ON r.id=x.source_id WHERE x.stream_id=$1 AND (r.id IS NULL OR r.state<>'ready' OR r.pending_upload OR r.quarantine_code IS NOT NULL OR r.medium NOT IN ('local','shared_s3'))) ELSE NOT EXISTS(SELECT 1 FROM extents x LEFT JOIN chunks c ON c.id=x.chunk_id WHERE x.stream_id=$1 AND (c.id IS NULL OR x.fragment_id IS NOT NULL OR c.state<>'ready' OR (NOT EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.encoding_id=c.current_encoding_id AND l.state='ready' AND l.quarantined_at IS NULL) AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.state='ready' AND p.quarantined_at IS NULL)))) END")
        .bind(stream)
        .bind(payload)
}

pub(super) fn lock_raw(stream: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT r.state='ready' AND NOT r.pending_upload AND r.quarantine_code IS NULL AND r.medium IN ('local','shared_s3') FROM raw_sources r WHERE r.id IN (SELECT source_id FROM raw_ranges WHERE stream_id=$1) ORDER BY r.id").bind(stream)
}

pub(super) fn reuse_target(id: Id) -> Statement<'static> {
    sqlx::query(
        "UPDATE transfer_items SET state='planned',position_bytes=0,reuse_target=TRUE WHERE id=$1",
    )
    .bind(id)
}

pub(super) fn extents<'q>(
    stream: Id,
    offset: i64,
) -> Rows<'q, (i64, i32, Option<Id>, Option<Id>, Option<Id>)> {
    sqlx::query_as("SELECT e.offset_bytes,e.length,e.chunk_id,e.fragment_id,c.pack_id FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=$1 AND e.offset_bytes>=$2 ORDER BY e.offset_bytes LIMIT 128")
        .bind(stream)
        .bind(offset)
}

pub(super) fn raw<'q>(id: Id) -> Rows<'q, RawSource> {
    sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id=$1")
        .bind(id)
}

pub(super) fn protect_raw<'q>(id: Id) -> Statement<'q> {
    sqlx::query("UPDATE raw_sources SET revision=revision+1 WHERE id=$1 AND state='ready'").bind(id)
}

pub(super) fn ranges<'q>(stream: Id, offset: i64) -> Rows<'q, crate::model::raw::RawRange> {
    sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,e.offset_bytes,e.length,e.source_offset FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id WHERE e.stream_id=$1 AND e.offset_bytes>=$2 ORDER BY e.offset_bytes LIMIT 128")
        .bind(stream)
        .bind(offset)
}

pub(super) fn units(task: Uuid, units: &[(&str, Id)]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "INSERT OR IGNORE INTO transfer_sources(task_id,source_type,source_id) ",
    );
    query.push_values(units, |mut row, (source_type, id)| {
        row.push_bind(task).push_bind(*source_type).push_bind(*id);
    });
    query.push("");
    query
}

pub(super) fn selected(bucket: Uuid, keys: &[String]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT s.id,CAST(o.object_key AS TEXT) AS object_key,s.metadata,s.checksums,s.public_read,s.size,s.etag,s.payload_type FROM objects o JOIN streams s ON s.id=o.stream_id WHERE s.state='ready' AND o.bucket_id=",
    );
    query.push_bind(bucket).push(" AND o.object_key IN (");
    let mut values = query.separated(",");
    for key in keys {
        values.push_bind(key.as_bytes().to_vec());
    }
    values.push_unseparated(") ORDER BY o.object_key");
    query
}

pub(super) fn connections(
    task: Uuid,
) -> Rows<'static, crate::model::replication::external::Connection> {
    sqlx::query_as("SELECT external_id,revision,encrypted FROM transfer_connections WHERE task_id=$1 ORDER BY external_id")
        .bind(task)
}

pub(super) fn connection<'q>(
    task: Uuid,
    value: &'q crate::model::replication::external::Connection,
) -> Statement<'q> {
    sqlx::query("INSERT INTO transfer_connections(task_id,external_id,revision,encrypted) VALUES($1,$2,$3,$4)")
        .bind(task).bind(value.external_id).bind(value.revision).bind(&value.encrypted)
}
