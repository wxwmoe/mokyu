use crate::{
    content::{codec::Chunk, pack::Pack},
    model::{
        Id,
        transfer::{Checkpoints, Input, Output, Part, Prepared, Proof, Scanned},
    },
};
use sqlx::{Postgres, types::Json};
use uuid::Uuid;

type Statement<'q> = sqlx::query::Query<'q, Postgres, <Postgres as sqlx::Database>::Arguments>;

pub(super) fn source_piece(stream: Id, position: i64, raw: bool) -> Rows<'static, (Id, i64, i64)> {
    if raw {
        sqlx::query_as("SELECT source_id,source_offset+$2-offset_bytes,offset_bytes+length-$2 FROM raw_ranges WHERE stream_id=$1 AND offset_bytes<=$2 AND offset_bytes+length>$2 ORDER BY offset_bytes DESC LIMIT 1").bind(stream).bind(position)
    } else {
        sqlx::query_as("SELECT chunk_id,source_offset+$2-offset_bytes,offset_bytes+length-$2 FROM extents WHERE stream_id=$1 AND offset_bytes<=$2 AND offset_bytes+length>$2 AND chunk_id IS NOT NULL AND fragment_id IS NULL ORDER BY offset_bytes DESC LIMIT 1").bind(stream).bind(position)
    }
}

pub(super) fn target_piece(task: Uuid, source: Id, raw: bool) -> Rows<'static, (Id, i64)> {
    if raw {
        sqlx::query_as("SELECT candidate,CAST(0 AS BIGINT) FROM transfer_sources WHERE task_id=$1 AND source_type='raw' AND source_id=$2 AND state='ready' AND candidate IS NOT NULL").bind(task).bind(source)
    } else {
        sqlx::query_as("SELECT u.candidate,COALESCE((SELECT SUM(t.length) FROM transfer_members t WHERE t.unit_id=m.unit_id AND t.source_offset<m.source_offset),0) FROM transfer_members m JOIN transfer_sources u ON u.id=m.unit_id WHERE m.task_id=$1 AND m.source_chunk=$2 AND u.state='ready' AND u.candidate IS NOT NULL").bind(task).bind(source)
    }
}

pub(super) fn projected(
    candidate: Id,
    start: i64,
    end: i64,
    limit: i64,
) -> Rows<'static, (i64, i32, Id, i32)> {
    sqlx::query_as("SELECT offset_bytes,length,chunk_id,source_offset FROM extents WHERE stream_id=$1 AND offset_bytes+length>$2 AND offset_bytes<$3 AND chunk_id IS NOT NULL AND fragment_id IS NULL ORDER BY offset_bytes LIMIT $4").bind(candidate).bind(start).bind(end).bind(limit)
}

pub(super) fn project(candidate: Id, rows: &[(i64, i32, Id, i32)]) -> sqlx::QueryBuilder<Postgres> {
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
    sqlx::query("UPDATE transfer_sources SET output=NULL,written_bytes=$3,state=CASE WHEN $4 THEN 'ready' ELSE 'copying' END WHERE id=$1 AND state='copying' AND written_bytes=$2 AND output IS NOT NULL").bind(unit).bind(start).bind(end).bind(done)
}

pub(super) fn append(candidate: Id, value: &Prepared) -> sqlx::QueryBuilder<Postgres> {
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
    sqlx::query_as("SELECT COALESCE(k.key_algorithm,'none'),s.block_key_id FROM storages s LEFT JOIN managed_keys k ON k.id=s.block_key_id WHERE s.id=$1 AND s.enabled AND (s.block_key_id IS NULL OR (k.purpose='block' AND k.revoked_at IS NULL)) FOR SHARE OF s").bind(storage)
}

pub(super) fn candidate(unit: Id, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE transfer_sources SET candidate=$2,state='copying' WHERE id=$1 AND candidate IS NULL AND state IN ('selected','copying')").bind(unit).bind(stream)
}

pub(super) fn prepare(unit: Id, value: &Prepared) -> Statement<'_> {
    sqlx::query("UPDATE transfer_sources SET output=$2 WHERE id=$1 AND output IS NULL AND state='copying' AND written_bytes=$3").bind(unit).bind(Json(Output::Chunks(value.clone()))).bind(value.start as i64)
}

pub(super) fn span_rows(unit: Id, offset: i64) -> Rows<'static, (Id, i64, i32, Vec<u8>)> {
    sqlx::query_as("SELECT id,source_offset,length,hash FROM transfer_spans WHERE unit_id=$1 AND source_offset>=$2 ORDER BY source_offset LIMIT 64").bind(unit).bind(offset)
}

pub(super) fn cleanup(task: Uuid) -> Rows<'static, (Id, Option<Id>)> {
    sqlx::query_as("SELECT id,candidate FROM transfer_sources WHERE task_id=$1 AND (input IS NOT NULL OR candidate IS NOT NULL) ORDER BY id LIMIT 32 FOR UPDATE").bind(task)
}

pub(super) fn cleanup_spans(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_spans WHERE id IN (SELECT s.id FROM transfer_spans s JOIN transfer_sources u ON u.id=s.unit_id WHERE u.task_id=$1 ORDER BY s.id LIMIT 1024)").bind(task)
}

pub(super) fn cleanup_parts(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_parts WHERE (unit_id,number) IN (SELECT p.unit_id,p.number FROM transfer_parts p JOIN transfer_sources u ON u.id=p.unit_id WHERE u.task_id=$1 ORDER BY p.unit_id,p.number LIMIT 1024)").bind(task)
}

pub(super) fn cleanup_members(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_members WHERE (task_id,source_chunk) IN (SELECT task_id,source_chunk FROM transfer_members WHERE task_id=$1 ORDER BY source_chunk LIMIT 1024)").bind(task)
}

pub(super) fn release(unit: Id) -> Statement<'static> {
    sqlx::query("UPDATE transfer_sources SET input=NULL,output=NULL,candidate=NULL,checksum=NULL,source_encoding=NULL,source_pack=NULL,source_raw=NULL,upload_id=NULL WHERE id=$1").bind(unit)
}

pub(super) fn layouts(ids: &[Id]) -> sqlx::QueryBuilder<Postgres> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT c.id,CASE WHEN p.state='ready' AND p.quarantined_at IS NULL THEN p.id ELSE NULL END,COALESCE((c.state='ready' AND ((p.state='ready' AND p.quarantined_at IS NULL) OR (e.state='ready' AND e.quarantined_at IS NULL))),FALSE),EXISTS(SELECT 1 FROM pending_uploads u WHERE u.chunk_id=c.id) FROM chunks c LEFT JOIN packs p ON p.id=c.pack_id LEFT JOIN chunk_encodings e ON e.encoding_id=c.current_encoding_id WHERE c.id=ANY(",
    );
    query
        .push_bind(ids.to_vec())
        .push(") ORDER BY c.id FOR SHARE OF c");
    query
}
type Rows<'q, T> = sqlx::query::QueryAs<'q, Postgres, T, <Postgres as sqlx::Database>::Arguments>;
type Scalar<'q, T> =
    sqlx::query::QueryScalar<'q, Postgres, T, <Postgres as sqlx::Database>::Arguments>;

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
    sqlx::query_as("SELECT id,source_type,source_id,input,output,candidate,state,scanned_bytes,written_bytes,checksum,upload_id FROM transfer_sources WHERE task_id=$1 AND source_type=$2 AND source_id=$3 FOR UPDATE")
        .bind(task).bind(source_type).bind(source)
}

pub(super) fn unit(task: Uuid, id: Id) -> Rows<'static, Unit> {
    sqlx::query_as("SELECT id,source_type,source_id,input,output,candidate,state,scanned_bytes,written_bytes,checksum,upload_id FROM transfer_sources WHERE task_id=$1 AND id=$2 FOR UPDATE")
        .bind(task).bind(id)
}

pub(super) fn pending(task: Uuid) -> Rows<'static, Unit> {
    sqlx::query_as("SELECT id,source_type,source_id,input,output,candidate,state,scanned_bytes,written_bytes,checksum,upload_id FROM transfer_sources WHERE task_id=$1 AND state<>'ready' ORDER BY id LIMIT 1 FOR UPDATE")
        .bind(task)
}

pub(super) fn freeze(id: Id, input: &Input) -> Statement<'_> {
    let (encoding, pack, raw) = match input {
        Input::Chunk { chunk } => (Some(chunk.encoding_id), None, None),
        Input::Pack { pack, .. } => (None, Some(pack.id), None),
        Input::Raw { source } => (None, None, Some(source.id)),
    };
    sqlx::query("UPDATE transfer_sources SET input=$2,source_encoding=$3,source_pack=$4,source_raw=$5 WHERE id=$1 AND input IS NULL AND state='selected'")
        .bind(id).bind(Json(input)).bind(encoding).bind(pack).bind(raw)
}

pub(super) fn chunk(id: Id) -> Rows<'static, Chunk> {
    sqlx::query_as("SELECT c.id,c.current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at FROM chunks c LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' AND l.quarantined_at IS NULL WHERE c.id=$1 AND c.state='ready' FOR SHARE OF c")
        .bind(id)
}

pub(super) fn pack(id: Id) -> Rows<'static, Pack> {
    sqlx::query_as(
        "SELECT * FROM packs WHERE id=$1 AND state='ready' AND quarantined_at IS NULL FOR SHARE",
    )
    .bind(id)
}

pub(super) fn member(task: Uuid, chunk: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT unit_id FROM transfer_members WHERE task_id=$1 AND source_chunk=$2")
        .bind(task)
        .bind(chunk)
}

pub(super) fn capture_member(
    task: Uuid,
    unit: Id,
    chunk: Id,
    pack: Option<Id>,
) -> Statement<'static> {
    sqlx::query("INSERT INTO transfer_members(unit_id,task_id,source_chunk,source_offset,length) SELECT $2,$1,c.id,COALESCE(m.offset_bytes,0),c.raw_size FROM chunks c LEFT JOIN pack_members m ON m.pack_id=$4 AND m.chunk_id=c.id WHERE c.id=$3 AND c.state='ready' AND ($4::uuid IS NULL OR (c.pack_id=$4 AND m.chunk_id IS NOT NULL)) ON CONFLICT(task_id,source_chunk) DO NOTHING")
        .bind(task).bind(unit).bind(chunk).bind(pack)
}

pub(super) fn members(unit: Id) -> Rows<'static, (Id, Uuid, i64, i32, Vec<u8>)> {
    sqlx::query_as("SELECT m.source_chunk,c.id,m.source_offset,m.length,c.hash FROM transfer_members m JOIN chunks c ON c.id=m.source_chunk WHERE m.unit_id=$1 ORDER BY m.source_offset LIMIT 4097").bind(unit)
}

pub(super) fn begin_scan(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE transfer_sources SET state='scanning' WHERE id=$1 AND state='selected' AND input IS NOT NULL")
        .bind(id)
}

pub(super) fn scanned(value: &Scanned) -> Statement<'_> {
    sqlx::query("UPDATE transfer_sources SET scanned_bytes=$3,checksum=$4,state=CASE WHEN $5 THEN 'copying' ELSE 'scanning' END WHERE id=$1 AND scanned_bytes=$2 AND state='scanning'")
        .bind(value.unit).bind(value.start as i64).bind(value.end as i64)
        .bind(value.checksum.as_ref().map(Json)).bind(value.digest.is_some())
}

pub(super) fn spans(unit: Id, values: &[Proof]) -> sqlx::QueryBuilder<Postgres> {
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
    sqlx::query("INSERT INTO transfer_parts(unit_id,number,source_offset,length,digest) VALUES($1,$2,$3,$4,$5)")
        .bind(unit).bind(i32::from(value.number)).bind(value.offset as i64).bind(value.size as i64).bind(&value.digest)
}

pub(super) fn policy(storage: Uuid, revision: i64) -> Rows<'static, (bool, bool)> {
    sqlx::query_as("SELECT (NOT p.cdc_enabled AND t.block_key_id IS NULL AND NOT p.compression_enabled AND NOT p.pack_enabled),p.cdc_enabled FROM storages t JOIN storage_profiles p ON p.storage_id=t.id WHERE t.id=$1 AND p.revision=$2 AND t.enabled FOR SHARE OF t,p")
        .bind(storage).bind(revision)
}

pub(super) fn input(unit: Id, input: &Input) -> Statement<'_> {
    sqlx::query(
        "UPDATE transfer_sources SET input=$2 WHERE id=$1 AND state='selected' AND scanned_bytes=0",
    )
    .bind(unit)
    .bind(Json(input))
}

pub(super) fn discard_unselected(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_sources u WHERE task_id=$1 AND source_type<>'raw' AND input IS NULL AND NOT EXISTS(SELECT 1 FROM transfer_members m WHERE m.unit_id=u.id)")
        .bind(task)
}

pub(super) fn raw_output(unit: Id, source: &crate::model::raw::RawSource) -> Statement<'static> {
    sqlx::query(
        "UPDATE transfer_sources SET output=$2 WHERE id=$1 AND output IS NULL AND state='copying'",
    )
    .bind(unit)
    .bind(Json(Output::Raw(source.clone())))
}
pub(super) fn raw_upload(unit: Id, upload: Option<&str>) -> Statement<'_> {
    sqlx::query("UPDATE transfer_sources SET upload_id=$2 WHERE id=$1 AND state='copying'")
        .bind(unit)
        .bind(upload)
}
pub(super) fn target_upload(source: Id, candidate: Id, upload: Option<&str>) -> Statement<'_> {
    sqlx::query(
        "UPDATE raw_sources SET upload_id=$3 WHERE id=$1 AND owner_stream=$2 AND state='writing'",
    )
    .bind(source)
    .bind(candidate)
    .bind(upload)
}
pub(super) fn raw_parts(unit: Id) -> Rows<'static, (i32, i64, i64, Vec<u8>, Option<String>)> {
    sqlx::query_as("SELECT number,source_offset,length,digest,etag FROM transfer_parts WHERE unit_id=$1 ORDER BY number LIMIT 10000").bind(unit)
}
pub(super) fn raw_part(unit: Id, number: u16) -> Rows<'static, (i64, i64, Option<String>)> {
    sqlx::query_as(
        "SELECT source_offset,length,etag FROM transfer_parts WHERE unit_id=$1 AND number=$2",
    )
    .bind(unit)
    .bind(i32::from(number))
}
pub(super) fn part_receipt(unit: Id, number: u16, etag: &str) -> Statement<'_> {
    sqlx::query("UPDATE transfer_parts SET etag=$3 WHERE unit_id=$1 AND number=$2 AND etag IS NULL")
        .bind(unit)
        .bind(i32::from(number))
        .bind(etag)
}
pub(super) fn reset_parts(unit: Id) -> Statement<'static> {
    sqlx::query("UPDATE transfer_parts SET etag=NULL WHERE unit_id=$1").bind(unit)
}
pub(super) fn raw_written(unit: Id, size: i64, done: bool) -> Statement<'static> {
    sqlx::query("UPDATE transfer_sources SET written_bytes=$2,state=CASE WHEN $3 THEN 'ready' ELSE 'copying' END,upload_id=CASE WHEN $3 THEN NULL ELSE upload_id END WHERE id=$1 AND state='copying'").bind(unit).bind(size).bind(done)
}
pub(super) fn empty(unit: Id) -> Statement<'static> {
    sqlx::query("UPDATE transfer_sources SET state='ready' WHERE id=$1 AND state='copying' AND written_bytes=0 AND output IS NULL").bind(unit)
}
pub(super) fn eligible(unit: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT CASE source_type WHEN 'chunk' THEN EXISTS(SELECT 1 FROM chunk_encodings e WHERE e.encoding_id=u.source_encoding AND e.state IN ('ready','retired') AND e.quarantined_at IS NULL) WHEN 'pack' THEN EXISTS(SELECT 1 FROM packs p WHERE p.id=u.source_pack AND p.state IN ('ready','retired') AND p.quarantined_at IS NULL) ELSE EXISTS(SELECT 1 FROM raw_sources r WHERE r.id=u.source_raw AND r.state='ready' AND r.quarantine_code IS NULL AND NOT r.pending_upload) END FROM transfer_sources u WHERE u.id=$1").bind(unit)
}

pub(super) fn project_raw(
    candidate: Id,
    rows: &[(i64, i64, Id, i64)],
) -> sqlx::QueryBuilder<Postgres> {
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
