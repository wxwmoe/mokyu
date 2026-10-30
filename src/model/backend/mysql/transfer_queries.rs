use crate::{
    content::{codec::Chunk, pack::Pack},
    model::{
        Id,
        transfer::{Checkpoints, Input, Output, Part, Prepared, Proof, Scanned},
    },
};
use sqlx::{MySql, types::Json};
use uuid::Uuid;

type Statement<'q> = sqlx::query::Query<'q, MySql, <MySql as sqlx::Database>::Arguments>;

pub(super) fn source_piece(stream: Id, position: i64, raw: bool) -> Rows<'static, (Id, i64, i64)> {
    if raw {
        sqlx::query_as("SELECT source_id,source_offset+?-offset_bytes,offset_bytes+length-? FROM raw_ranges WHERE stream_id=? AND offset_bytes<=? AND offset_bytes+length>? ORDER BY offset_bytes DESC LIMIT 1").bind(position).bind(position).bind(stream).bind(position).bind(position)
    } else {
        sqlx::query_as("SELECT chunk_id,source_offset+?-offset_bytes,offset_bytes+length-? FROM extents WHERE stream_id=? AND offset_bytes<=? AND offset_bytes+length>? AND chunk_id IS NOT NULL AND fragment_id IS NULL ORDER BY offset_bytes DESC LIMIT 1").bind(position).bind(position).bind(stream).bind(position).bind(position)
    }
}

pub(super) fn target_piece(task: Uuid, source: Id, raw: bool) -> Rows<'static, (Id, i64)> {
    if raw {
        sqlx::query_as("SELECT candidate,CAST(0 AS SIGNED) FROM transfer_sources WHERE task_id=? AND source_type='raw' AND source_id=? AND state='ready' AND candidate IS NOT NULL").bind(task).bind(source)
    } else {
        sqlx::query_as("SELECT u.candidate,CAST(COALESCE((SELECT SUM(t.length) FROM transfer_members t WHERE t.unit_id=m.unit_id AND t.source_offset<m.source_offset),0) AS SIGNED) FROM transfer_members m JOIN transfer_sources u ON u.id=m.unit_id WHERE m.task_id=? AND m.source_chunk=? AND u.state='ready' AND u.candidate IS NOT NULL").bind(task).bind(source)
    }
}

pub(super) fn projected(
    candidate: Id,
    start: i64,
    end: i64,
    limit: i64,
) -> Rows<'static, (i64, i32, Id, i32)> {
    sqlx::query_as("SELECT offset_bytes,length,chunk_id,source_offset FROM extents WHERE stream_id=? AND offset_bytes+length>? AND offset_bytes<? AND chunk_id IS NOT NULL AND fragment_id IS NULL ORDER BY offset_bytes LIMIT ?").bind(candidate).bind(start).bind(end).bind(limit)
}

pub(super) fn project(candidate: Id, rows: &[(i64, i32, Id, i32)]) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) ",
    );
    query.push_values(rows, |mut row, &(offset, length, chunk, source)| {
        row.push_bind(candidate)
            .push_bind(offset)
            .push_bind(length)
            .push_bind(chunk)
            .push_bind(source);
    });
    query
}

pub(super) fn copied(unit: Id, start: i64, end: i64, done: bool) -> Statement<'static> {
    sqlx::query("UPDATE transfer_sources SET output=NULL,written_bytes=?,state=IF(?,'ready','copying') WHERE id=? AND state='copying' AND written_bytes=? AND output IS NOT NULL").bind(end).bind(done).bind(unit).bind(start)
}

pub(super) fn append(candidate: Id, value: &Prepared) -> sqlx::QueryBuilder<MySql> {
    let mut query =
        sqlx::QueryBuilder::new("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) ");
    query.push_values(&value.targets, |mut row, target| {
        row.push_bind(candidate)
            .push_bind(target.offset as i64)
            .push_bind(target.chunk.raw_size)
            .push_bind(target.chunk.id);
    });
    query
}

pub(super) fn encoding(storage: Uuid) -> Rows<'static, (String, Option<Uuid>)> {
    sqlx::query_as("SELECT COALESCE(k.key_algorithm,'none'),s.block_key_id FROM storages s LEFT JOIN managed_keys k ON k.id=s.block_key_id WHERE s.id=? AND s.enabled AND (s.block_key_id IS NULL OR (k.purpose='block' AND k.revoked_at IS NULL)) LOCK IN SHARE MODE").bind(storage)
}

pub(super) fn candidate(unit: Id, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE transfer_sources SET candidate=?,state='copying' WHERE id=? AND candidate IS NULL AND state IN ('selected','copying')").bind(stream).bind(unit)
}

pub(super) fn prepare(unit: Id, value: &Prepared) -> Statement<'_> {
    sqlx::query("UPDATE transfer_sources SET output=? WHERE id=? AND output IS NULL AND state='copying' AND written_bytes=?").bind(Json(Output::Chunks(value.clone()))).bind(unit).bind(value.start as i64)
}

pub(super) fn span_rows(unit: Id, offset: i64) -> Rows<'static, (Id, i64, i32, Vec<u8>)> {
    sqlx::query_as("SELECT id,source_offset,length,hash FROM transfer_spans WHERE unit_id=? AND source_offset>=? ORDER BY source_offset LIMIT 64").bind(unit).bind(offset)
}

pub(super) fn cleanup(task: Uuid) -> Rows<'static, (Id, Option<Id>)> {
    sqlx::query_as("SELECT id,candidate FROM transfer_sources WHERE task_id=? AND (input IS NOT NULL OR candidate IS NOT NULL) ORDER BY id LIMIT 32 FOR UPDATE").bind(task)
}

pub(super) fn cleanup_spans(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_spans WHERE unit_id IN (SELECT id FROM transfer_sources WHERE task_id=?) ORDER BY id LIMIT 1024").bind(task)
}

pub(super) fn cleanup_parts(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_parts WHERE unit_id IN (SELECT id FROM transfer_sources WHERE task_id=?) ORDER BY unit_id,number LIMIT 1024").bind(task)
}

pub(super) fn cleanup_members(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_members WHERE task_id=? ORDER BY source_chunk LIMIT 1024")
        .bind(task)
}

pub(super) fn release(unit: Id) -> Statement<'static> {
    sqlx::query("UPDATE transfer_sources SET input=NULL,output=NULL,candidate=NULL,checksum=NULL,source_encoding=NULL,source_pack=NULL,source_raw=NULL,upload_id=NULL WHERE id=?").bind(unit)
}

pub(super) fn layouts(ids: &[Id]) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT c.id,CASE WHEN p.state='ready' AND p.quarantined_at IS NULL THEN p.id ELSE NULL END,COALESCE((c.state='ready' AND ((p.state='ready' AND p.quarantined_at IS NULL) OR (e.state='ready' AND e.quarantined_at IS NULL))),FALSE),EXISTS(SELECT 1 FROM pending_uploads u WHERE u.chunk_id=c.id) FROM chunks c LEFT JOIN packs p ON p.id=c.pack_id LEFT JOIN chunk_encodings e ON e.encoding_id=c.current_encoding_id WHERE c.id IN (",
    );
    let mut values = query.separated(",");
    for id in ids {
        values.push_bind(*id);
    }
    values.push_unseparated(") ORDER BY c.id LOCK IN SHARE MODE");
    query
}
type Rows<'q, T> = sqlx::query::QueryAs<'q, MySql, T, <MySql as sqlx::Database>::Arguments>;
type Scalar<'q, T> = sqlx::query::QueryScalar<'q, MySql, T, <MySql as sqlx::Database>::Arguments>;

#[derive(sqlx::FromRow)]
pub(super) struct Unit {
    pub id: Id,
    pub source_type: String,
    pub source_id: Id,
    pub input: Option<Json<Input>>,
    pub output: Option<Json<Output>>,
    pub candidate: Option<Id>,
    pub state: String,
    pub scanned_bytes: i64,
    pub written_bytes: i64,
    pub checksum: Option<Json<Checkpoints>>,
    pub upload_id: Option<String>,
}

pub(super) fn selected(task: Uuid, source_type: &str, source: Id) -> Rows<'_, Unit> {
    sqlx::query_as("SELECT id,source_type,source_id,input,output,candidate,state,scanned_bytes,written_bytes,checksum,upload_id FROM transfer_sources WHERE task_id=? AND source_type=? AND source_id=? FOR UPDATE")
        .bind(task).bind(source_type).bind(source)
}

pub(super) fn unit(task: Uuid, id: Id) -> Rows<'static, Unit> {
    sqlx::query_as("SELECT id,source_type,source_id,input,output,candidate,state,scanned_bytes,written_bytes,checksum,upload_id FROM transfer_sources WHERE task_id=? AND id=? FOR UPDATE")
        .bind(task).bind(id)
}

pub(super) fn pending(task: Uuid) -> Rows<'static, Unit> {
    sqlx::query_as("SELECT id,source_type,source_id,input,output,candidate,state,scanned_bytes,written_bytes,checksum,upload_id FROM transfer_sources WHERE task_id=? AND state<>'ready' ORDER BY id LIMIT 1 FOR UPDATE")
        .bind(task)
}

pub(super) fn freeze(id: Id, input: &Input) -> Statement<'_> {
    let (encoding, pack, raw) = match input {
        Input::Chunk { chunk } => (Some(chunk.encoding_id), None, None),
        Input::Pack { pack, .. } => (None, Some(pack.id), None),
        Input::Raw { source } => (None, None, Some(source.id)),
    };
    sqlx::query("UPDATE transfer_sources SET input=?,source_encoding=?,source_pack=?,source_raw=? WHERE id=? AND input IS NULL AND state='selected'")
        .bind(Json(input)).bind(encoding).bind(pack).bind(raw).bind(id)
}

pub(super) fn chunk(id: Id) -> sqlx::QueryBuilder<MySql> {
    let mut query = super::queries::chunk_query(true);
    query
        .push(" WHERE c.id=")
        .push_bind(id)
        .push(" AND c.state='ready' LOCK IN SHARE MODE");
    query
}

pub(super) fn pack(id: Id) -> Rows<'static, Pack> {
    sqlx::query_as("SELECT * FROM packs WHERE id=? AND state='ready' AND quarantined_at IS NULL LOCK IN SHARE MODE")
        .bind(id)
}

pub(super) fn member(task: Uuid, chunk: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT unit_id FROM transfer_members WHERE task_id=? AND source_chunk=?")
        .bind(task)
        .bind(chunk)
}

pub(super) fn capture_member(
    task: Uuid,
    unit: Id,
    chunk: Id,
    pack: Option<Id>,
) -> Statement<'static> {
    sqlx::query("INSERT INTO transfer_members(unit_id,task_id,source_chunk,source_offset,length) SELECT ?,?,c.id,COALESCE(m.offset_bytes,0),c.raw_size FROM chunks c LEFT JOIN pack_members m ON m.pack_id=? AND m.chunk_id=c.id WHERE c.id=? AND c.state='ready' AND (? IS NULL OR (c.pack_id=? AND m.chunk_id IS NOT NULL)) ON DUPLICATE KEY UPDATE source_chunk=transfer_members.source_chunk")
        .bind(unit).bind(task).bind(pack).bind(chunk).bind(pack).bind(pack)
}

pub(super) fn members(unit: Id) -> Rows<'static, (Id, Uuid, i64, i32, Vec<u8>)> {
    sqlx::query_as("SELECT m.source_chunk,c.uuid,m.source_offset,m.length,c.hash FROM transfer_members m JOIN chunks c ON c.id=m.source_chunk WHERE m.unit_id=? ORDER BY m.source_offset LIMIT 4097").bind(unit)
}

pub(super) fn begin_scan(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE transfer_sources SET state='scanning' WHERE id=? AND state='selected' AND input IS NOT NULL")
        .bind(id)
}

pub(super) fn scanned(value: &Scanned) -> Statement<'_> {
    sqlx::query("UPDATE transfer_sources SET scanned_bytes=?,checksum=?,state=CASE WHEN ? THEN 'copying' ELSE 'scanning' END WHERE id=? AND scanned_bytes=? AND state='scanning'")
        .bind(value.end as i64).bind(value.checksum.as_ref().map(Json)).bind(value.digest.is_some())
        .bind(value.unit).bind(value.start as i64)
}

pub(super) fn spans(unit: Id, values: &[Proof]) -> sqlx::QueryBuilder<MySql> {
    let mut query =
        sqlx::QueryBuilder::new("INSERT INTO transfer_spans(unit_id,source_offset,length,hash) ");
    query.push_values(values, |mut row, proof| {
        row.push_bind(unit)
            .push_bind(proof.offset as i64)
            .push_bind(proof.size as i32)
            .push_bind(proof.hash.clone());
    });
    query
}

pub(super) fn part(unit: Id, value: &Part) -> Statement<'_> {
    sqlx::query(
        "INSERT INTO transfer_parts(unit_id,number,source_offset,length,digest) VALUES(?,?,?,?,?)",
    )
    .bind(unit)
    .bind(i32::from(value.number))
    .bind(value.offset as i64)
    .bind(value.size as i64)
    .bind(&value.digest)
}

pub(super) fn policy(storage: Uuid, revision: i64) -> Rows<'static, (bool, bool)> {
    sqlx::query_as("SELECT (NOT p.cdc_enabled AND t.block_key_id IS NULL AND NOT p.compression_enabled AND NOT p.pack_enabled),p.cdc_enabled FROM storages t JOIN storage_profiles p ON p.storage_id=t.id WHERE t.id=? AND p.revision=? AND t.enabled LOCK IN SHARE MODE")
        .bind(storage).bind(revision)
}

pub(super) fn input(unit: Id, input: &Input) -> Statement<'_> {
    sqlx::query(
        "UPDATE transfer_sources SET input=? WHERE id=? AND state='selected' AND scanned_bytes=0",
    )
    .bind(Json(input))
    .bind(unit)
}

pub(super) fn discard_unselected(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE u FROM transfer_sources u WHERE task_id=? AND source_type<>'raw' AND input IS NULL AND NOT EXISTS(SELECT 1 FROM transfer_members m WHERE m.unit_id=u.id)")
        .bind(task)
}

pub(super) fn raw_output(unit: Id, source: &crate::model::raw::RawSource) -> Statement<'static> {
    sqlx::query(
        "UPDATE transfer_sources SET output=? WHERE id=? AND output IS NULL AND state='copying'",
    )
    .bind(Json(Output::Raw(source.clone())))
    .bind(unit)
}
pub(super) fn raw_upload(unit: Id, upload: Option<&str>) -> Statement<'_> {
    sqlx::query("UPDATE transfer_sources SET upload_id=? WHERE id=? AND state='copying'")
        .bind(upload)
        .bind(unit)
}
pub(super) fn target_upload(source: Id, candidate: Id, upload: Option<&str>) -> Statement<'_> {
    sqlx::query(
        "UPDATE raw_sources SET upload_id=? WHERE id=? AND owner_stream=? AND state='writing'",
    )
    .bind(upload)
    .bind(source)
    .bind(candidate)
}
pub(super) fn raw_parts(unit: Id) -> Rows<'static, (i32, i64, i64, Vec<u8>, Option<String>)> {
    sqlx::query_as("SELECT number,source_offset,length,digest,etag FROM transfer_parts WHERE unit_id=? ORDER BY number LIMIT 10000").bind(unit)
}
pub(super) fn raw_part(unit: Id, number: u16) -> Rows<'static, (i64, i64, Option<String>)> {
    sqlx::query_as(
        "SELECT source_offset,length,etag FROM transfer_parts WHERE unit_id=? AND number=?",
    )
    .bind(unit)
    .bind(i32::from(number))
}
pub(super) fn part_receipt(unit: Id, number: u16, etag: &str) -> Statement<'_> {
    sqlx::query("UPDATE transfer_parts SET etag=? WHERE unit_id=? AND number=? AND etag IS NULL")
        .bind(etag)
        .bind(unit)
        .bind(i32::from(number))
}
pub(super) fn reset_parts(unit: Id) -> Statement<'static> {
    sqlx::query("UPDATE transfer_parts SET etag=NULL WHERE unit_id=?").bind(unit)
}
pub(super) fn raw_written(unit: Id, size: i64, done: bool) -> Statement<'static> {
    sqlx::query("UPDATE transfer_sources SET written_bytes=?,state=IF(?,'ready','copying'),upload_id=IF(?,NULL,upload_id) WHERE id=? AND state='copying'").bind(size).bind(done).bind(done).bind(unit)
}
pub(super) fn empty(unit: Id) -> Statement<'static> {
    sqlx::query("UPDATE transfer_sources SET state='ready' WHERE id=? AND state='copying' AND written_bytes=0 AND output IS NULL").bind(unit)
}
pub(super) fn eligible(unit: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT CASE source_type WHEN 'chunk' THEN EXISTS(SELECT 1 FROM chunk_encodings e WHERE e.encoding_id=u.source_encoding AND e.state IN ('ready','retired') AND e.quarantined_at IS NULL) WHEN 'pack' THEN EXISTS(SELECT 1 FROM packs p WHERE p.id=u.source_pack AND p.state IN ('ready','retired') AND p.quarantined_at IS NULL) ELSE EXISTS(SELECT 1 FROM raw_sources r WHERE r.id=u.source_raw AND r.state='ready' AND r.quarantine_code IS NULL AND NOT r.pending_upload) END FROM transfer_sources u WHERE u.id=?").bind(unit)
}

pub(super) fn project_raw(
    candidate: Id,
    rows: &[(i64, i64, Id, i64)],
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id,source_offset) ",
    );
    query.push_values(rows, |mut row, &(offset, length, source, within)| {
        row.push_bind(candidate)
            .push_bind(offset)
            .push_bind(length)
            .push_bind(source)
            .push_bind(within);
    });
    query
}
