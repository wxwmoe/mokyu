use super::super::native_parts::Offer;
use super::{Rows, Scalar, Statement};
use crate::{
    cluster::NodeState,
    model::{Id, multipart::RemoteUpload, node_native as native, session::Run},
};
use sqlx::types::Json;
use uuid::Uuid;
pub(in super::super) fn upload(id: Uuid) -> Rows<'static, RemoteUpload> {
    sqlx::query_as("SELECT n.upload_id AS remote_upload,u.state AS remote_state,n.source_location AS remote_source,n.storage_id AS remote_storage,n.profile_revision AS remote_profile,n.upstream AS remote_upstream,n.manifest_hash AS remote_manifest FROM native_uploads n JOIN uploads u ON u.id=n.upload_id WHERE n.upload_id=?").bind(id)
}
pub(in super::super) fn open(
    id: Uuid,
    storage: Uuid,
    revision: i64,
    location: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT OR IGNORE INTO native_uploads(upload_id,storage_id,profile_revision,source_location) VALUES(?,?,?,?)").bind(id).bind(storage).bind(revision).bind(location)
}
pub(in super::super) fn new_file(
    file: Uuid,
    upload: Uuid,
    storage: Uuid,
    node: Uuid,
    stream: Id,
) -> Statement<'static> {
    sqlx::query("INSERT INTO multipart_files(id,upload_id,storage_id,node_id,part_stream) VALUES(?,?,?,?,?)").bind(file).bind(upload).bind(storage).bind(node).bind(stream)
}
pub(in super::super) fn file(stream: Id, node: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM multipart_files WHERE part_stream=? AND node_id=?")
        .bind(stream)
        .bind(node)
}
pub(in super::super) fn file_state(
    stream: Id,
    node: Uuid,
) -> Rows<'static, (Uuid, String, Option<Vec<u8>>)> {
    sqlx::query_as(
        "SELECT id,state,request_hash FROM multipart_files WHERE part_stream=? AND node_id=?",
    )
    .bind(stream)
    .bind(node)
}
pub(in super::super) fn seal(file: Uuid, hash: &[u8]) -> Statement<'_> {
    sqlx::query(
        "UPDATE multipart_files SET state='sealed',request_hash=? WHERE id=? AND state='open'",
    )
    .bind(hash)
    .bind(file)
}
pub(in super::super) fn queue(
    upload: Uuid,
    number: i32,
    stream: Id,
    file: Option<Uuid>,
    node: Uuid,
    size: i64,
    cached: bool,
) -> Statement<'static> {
    sqlx::query("INSERT INTO native_parts(upload_id,part_number,stream_id,file_id,node_id,size,cached) VALUES(?,?,?,?,?,?,?) ON CONFLICT(upload_id,part_number) DO UPDATE SET stream_id=excluded.stream_id,file_id=excluded.file_id,node_id=excluded.node_id,size=excluded.size,cached=excluded.cached,token=NULL,expires_at=NULL,remote_etag=NULL,error=NULL,retry_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(upload).bind(number).bind(stream).bind(file).bind(node).bind(size).bind(cached)
}
pub(in super::super) fn ready(node: Uuid, stream: Id) -> Rows<'static, (bool, Option<String>)> {
    sqlx::query_as(
        "SELECT remote_etag IS NOT NULL,error FROM native_parts WHERE stream_id=? AND node_id=?",
    )
    .bind(stream)
    .bind(node)
}
pub(in super::super) fn due(
    node: Uuid,
    target: Option<Id>,
) -> Rows<'static, (Uuid, i32, Id, Option<Uuid>, i64, String)> {
    sqlx::query_as("SELECT p.upload_id,p.part_number,p.stream_id,p.file_id,p.size,s.etag FROM native_parts p JOIN native_uploads n ON n.upload_id=p.upload_id JOIN parts a ON a.upload_id=p.upload_id AND a.part_number=p.part_number AND a.stream_id=p.stream_id JOIN streams s ON s.id=p.stream_id JOIN uploads u ON u.id=p.upload_id WHERE p.node_id=? AND (? IS NULL OR p.stream_id=?) AND u.state IN ('active','completing') AND p.remote_etag IS NULL AND p.retry_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER) AND (p.expires_at IS NULL OR p.expires_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER)) AND (n.upstream IS NOT NULL OR n.expires_at IS NULL OR n.expires_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER)) ORDER BY p.retry_at,p.upload_id,p.part_number LIMIT 1").bind(node).bind(target).bind(target)
}
pub(in super::super) fn claim_init(upload: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE native_uploads SET token=?,expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000 WHERE upload_id=? AND upstream IS NULL AND (expires_at IS NULL OR expires_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(token).bind(upload)
}
pub(in super::super) fn claim_part(
    upload: Uuid,
    number: i32,
    stream: Id,
    token: Uuid,
) -> Statement<'static> {
    sqlx::query("UPDATE native_parts SET token=?,expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000 WHERE upload_id=? AND part_number=? AND stream_id=? AND remote_etag IS NULL AND (expires_at IS NULL OR expires_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(token).bind(upload).bind(number).bind(stream)
}
pub(in super::super) fn set_upstream<'a>(
    upload: Uuid,
    token: Uuid,
    upstream: Option<&'a str>,
) -> Statement<'a> {
    sqlx::query("UPDATE native_uploads SET upstream=?,token=NULL,expires_at=NULL WHERE upload_id=? AND token=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(upstream).bind(upload).bind(token)
}
pub(in super::super) fn set_part<'a>(
    upload: Uuid,
    number: i32,
    stream: Id,
    token: Uuid,
    etag: Option<&'a str>,
    error: Option<&'a str>,
) -> Statement<'a> {
    sqlx::query("UPDATE native_parts SET remote_etag=?,error=?,token=NULL,expires_at=NULL,retry_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+5000000 WHERE upload_id=? AND part_number=? AND stream_id=? AND token=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(etag).bind(error).bind(upload).bind(number).bind(stream).bind(token)
}
pub(in super::super) fn abort_due() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT n.upload_id FROM native_uploads n JOIN uploads u ON u.id=n.upload_id WHERE u.state IN ('aborted','completed') AND n.upstream IS NOT NULL AND (n.expires_at IS NULL OR n.expires_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER)) ORDER BY u.touched_at,n.upload_id LIMIT 1",
    )
}
pub(in super::super) fn claim_abort(upload: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE native_uploads SET token=?,expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000 WHERE upload_id=? AND upstream IS NOT NULL AND (expires_at IS NULL OR expires_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(token).bind(upload)
}
pub(in super::super) fn work(node: Uuid, op: Uuid) -> Rows<'static, Offer> {
    sqlx::query_as("SELECT w.*,max(0,(w.expires_at-CAST(unixepoch('subsec')*1000000 AS INTEGER))/1000) AS remaining FROM node_native_work w WHERE w.node_id=? AND w.operation_id=?").bind(node).bind(op)
}
pub(in super::super) fn save<'a>(
    node: &NodeState,
    input: &native::Claim,
    token: Uuid,
    value: &'a native::Work,
) -> Statement<'a> {
    sqlx::query("INSERT INTO node_native_work(node_id,operation_id,credential_generation,boot_id,fence,target_stream,token,storage_id,work,expires_at) VALUES(?,?,?,?,?,?,?,?,?,CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000)").bind(node.node_id).bind(input.operation).bind(node.credential_generation).bind(input.run.boot).bind(input.run.fence).bind(input.stream).bind(token).bind(value.upload.storage).bind(Json(value))
}
pub(in super::super) fn done(node: Uuid, op: Uuid, hash: &[u8]) -> Statement<'_> {
    sqlx::query("UPDATE node_native_work SET result_hash=?,expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE node_id=? AND operation_id=? AND result_hash IS NULL").bind(hash).bind(node).bind(op)
}
pub(in super::super) fn count(node: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_native_work WHERE node_id=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER) AND result_hash IS NULL").bind(node)
}
pub(in super::super) fn storage_permit(
    node: Uuid,
    token: Uuid,
    storage: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_permits(token,storage_id,direction,holder,expires_at) VALUES(?,?,'upload',?,CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000)").bind(token).bind(storage).bind(node)
}
pub(in super::super) fn drop_permit(token: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM storage_permits WHERE token=?").bind(token)
}
pub(in super::super) fn manifest(upload: Uuid, hash: &str) -> Statement<'_> {
    sqlx::query("UPDATE native_uploads SET manifest_hash=? WHERE upload_id=? AND (manifest_hash IS NULL OR manifest_hash=?)").bind(hash).bind(upload).bind(hash)
}
pub(in super::super) fn manifest_parts(upload: Uuid) -> Rows<'static, native::Part> {
    sqlx::query_as("SELECT c.part_number AS number,c.size,c.etag,p.remote_etag FROM node_completion_parts c LEFT JOIN native_parts p ON p.upload_id=c.upload_id AND p.part_number=c.part_number AND p.stream_id=c.stream_id WHERE c.upload_id=? ORDER BY c.ordinal").bind(upload)
}
pub(in super::super) fn bind_raw(upload: Uuid, source: Id) -> Statement<'static> {
    sqlx::query("UPDATE native_uploads SET raw_id=? WHERE upload_id=? AND raw_id IS NULL")
        .bind(source)
        .bind(upload)
}
pub(in super::super) fn raw(upload: Uuid) -> Scalar<'static, Option<Id>> {
    sqlx::query_scalar("SELECT raw_id FROM native_uploads WHERE upload_id=?").bind(upload)
}
pub(in super::super) fn has_error(upload: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM node_completion_parts c JOIN native_parts p ON p.upload_id=c.upload_id AND p.part_number=c.part_number AND p.stream_id=c.stream_id WHERE c.upload_id=? AND p.error IN ('local_source_missing','local_source_corrupt'))").bind(upload)
}
pub(in super::super) fn retry_parts(upload: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE native_parts SET remote_etag=NULL,error=NULL,token=NULL,expires_at=NULL,retry_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE upload_id=? AND EXISTS(SELECT 1 FROM node_completion_parts c WHERE c.upload_id=native_parts.upload_id AND c.part_number=native_parts.part_number AND c.stream_id=native_parts.stream_id)").bind(upload)
}

pub(in super::super) fn retry_file(node: Uuid, file: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE native_parts SET error=NULL,retry_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE file_id=? AND node_id=? AND error IN ('local_source_missing','local_source_corrupt')").bind(file).bind(node)
}
pub(in super::super) fn inventory(
    node: Uuid,
    file: Uuid,
) -> Rows<'static, (Uuid, i64, bool, bool, bool)> {
    sqlx::query_as("SELECT m.storage_id,COALESCE(s.size,p.size,0),EXISTS(SELECT 1 FROM native_parts n JOIN uploads u ON u.id=n.upload_id WHERE n.file_id=m.id AND n.node_id=? AND n.remote_etag IS NULL AND u.state IN ('active','completing')) OR EXISTS(SELECT 1 FROM node_writes w WHERE w.stream_id=m.part_stream AND w.phase='active' AND w.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)),EXISTS(SELECT 1 FROM node_read_sources nr JOIN node_read_windows nw ON nw.id=nr.window_id JOIN node_reads r ON r.read_id=nw.read_id JOIN fragments f ON f.location_id=nr.location_id WHERE f.file_id=m.id AND r.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)) OR m.source_id IS NOT NULL OR EXISTS(SELECT 1 FROM fragments f WHERE f.file_id=m.id) OR EXISTS(SELECT 1 FROM parts a WHERE a.stream_id=m.part_stream) OR EXISTS(SELECT 1 FROM native_part_reads np WHERE np.stream_id=m.part_stream AND np.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)),COALESCE(m.node_id<>?,TRUE) FROM multipart_files m LEFT JOIN streams s ON s.id=m.part_stream LEFT JOIN native_parts p ON p.file_id=m.id WHERE m.id=?").bind(node).bind(node).bind(file)
}
pub(in super::super) fn read_load(storage: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM storage_permits WHERE direction='read' AND storage_id=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(storage)
}
pub(in super::super) fn read_permit(node: Uuid, op: Uuid) -> Rows<'static, (Uuid, i64)> {
    sqlx::query_as("SELECT p.token,max(0,(p.expires_at-CAST(unixepoch('subsec')*1000000 AS INTEGER))/1000) FROM storage_permits p JOIN node_write_permits w ON w.token=p.token WHERE w.node_id=? AND w.operation_id=? AND p.direction='read' AND p.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER) LIMIT 1").bind(node).bind(op)
}
pub(in super::super) fn add_read_permit(
    token: Uuid,
    storage: Uuid,
    node: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_permits(token,storage_id,direction,holder,expires_at) VALUES(?,?,'read',?,CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000)").bind(token).bind(storage).bind(node)
}
pub(in super::super) fn renew_native(
    node: Uuid,
    run: Run,
    token: Uuid,
    renew: bool,
) -> Statement<'static> {
    sqlx::query(if renew {"UPDATE node_native_work SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000 WHERE node_id=? AND boot_id=? AND fence=? AND token=? AND result_hash IS NULL AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)"}else{"UPDATE node_native_work SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE node_id=? AND boot_id=? AND fence=? AND token=? AND result_hash IS NULL"}).bind(node).bind(run.boot).bind(run.fence).bind(token)
}
pub(in super::super) fn charge_part(token: Uuid, renew: bool) -> Statement<'static> {
    sqlx::query(if renew {"UPDATE native_parts SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000 WHERE token=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)"}else{"UPDATE native_parts SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE token=?"}).bind(token)
}
pub(in super::super) fn charge_upload(token: Uuid, renew: bool) -> Statement<'static> {
    sqlx::query(if renew {"UPDATE native_uploads SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000 WHERE token=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)"}else{"UPDATE native_uploads SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE token=?"}).bind(token)
}
pub(in super::super) fn charge_storage(token: Uuid, renew: bool) -> Statement<'static> {
    sqlx::query(if renew {"UPDATE storage_permits SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000 WHERE token=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)"}else{"DELETE FROM storage_permits WHERE token=?"}).bind(token)
}
pub(in super::super) fn touch_read(
    node: Uuid,
    run: Run,
    token: Uuid,
    renew: bool,
) -> Statement<'static> {
    sqlx::query(if renew {"UPDATE storage_permits SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000 WHERE token=? AND direction='read' AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER) AND EXISTS(SELECT 1 FROM node_write_permits p JOIN node_writes w ON w.node_id=p.node_id AND w.operation_id=p.operation_id WHERE p.token=storage_permits.token AND w.node_id=? AND w.boot_id=? AND w.fence=? AND w.phase='active' AND w.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))"}else{"DELETE FROM storage_permits WHERE token=? AND direction='read' AND EXISTS(SELECT 1 FROM node_write_permits p JOIN node_writes w ON w.node_id=p.node_id AND w.operation_id=p.operation_id WHERE p.token=storage_permits.token AND w.node_id=? AND w.boot_id=? AND w.fence=?)"}).bind(token).bind(node).bind(run.boot).bind(run.fence)
}

pub(in super::super) fn parts(upload: Uuid, streams: &[Id]) -> Rows<'_, (Id, i32, Option<String>)> {
    sqlx::query_as("SELECT p.stream_id,p.part_number,p.remote_etag FROM native_parts p JOIN parts a ON a.upload_id=p.upload_id AND a.part_number=p.part_number AND a.stream_id=p.stream_id WHERE p.upload_id=? AND p.stream_id IN (SELECT CAST(value AS INTEGER) FROM json_each(?)) ORDER BY p.part_number").bind(upload).bind(Json(streams))
}
pub(in super::super) fn resend(upload: Uuid, streams: &[Id]) -> Statement<'_> {
    sqlx::query("UPDATE native_parts SET remote_etag=NULL,error=NULL,retry_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE upload_id=? AND stream_id IN (SELECT CAST(value AS INTEGER) FROM json_each(?))").bind(upload).bind(Json(streams))
}
pub(in super::super) fn charges(node: Uuid, after: Id) -> Rows<'static, (Id, Uuid, i64)> {
    sqlx::query_as("SELECT p.stream_id,n.storage_id,p.size FROM native_parts p JOIN native_uploads n ON n.upload_id=p.upload_id JOIN uploads u ON u.id=p.upload_id WHERE p.node_id=? AND p.cached AND p.remote_etag IS NULL AND p.stream_id>? AND u.state IN ('active','completing') ORDER BY p.stream_id LIMIT 128").bind(node).bind(after)
}
pub(in super::super) fn completion_owner(upload: Uuid, token: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM uploads WHERE id=? AND state='completing' AND completion_token=? AND completion_expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(upload).bind(token)
}
pub(in super::super) fn pin(stream: Id, token: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO native_part_reads(token,stream_id,expires_at) VALUES(?,?,CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000)").bind(token).bind(stream)
}
pub(in super::super) fn touch_pin(token: Uuid, renew: bool) -> Statement<'static> {
    sqlx::query(if renew{"UPDATE native_part_reads SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000 WHERE token=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)"}else{"DELETE FROM native_part_reads WHERE token=?"}).bind(token)
}
pub(in super::super) fn unpin(token: Uuid) -> Statement<'static> {
    touch_pin(token, false)
}
pub(in super::super) fn prune() -> Statement<'static> {
    sqlx::query(
        "DELETE FROM node_native_work WHERE (node_id,operation_id) IN (SELECT node_id,operation_id FROM node_native_work WHERE expires_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-900000000 ORDER BY expires_at LIMIT 128)",
    )
}
pub(in super::super) fn prune_pins() -> Statement<'static> {
    sqlx::query(
        "DELETE FROM native_part_reads WHERE token IN (SELECT token FROM native_part_reads WHERE expires_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER) ORDER BY expires_at LIMIT 128)",
    )
}
pub(in super::super) fn rebind(source: Id, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET owner_stream=? WHERE id=? AND state='writing' AND medium='shared_s3'").bind(stream).bind(source)
}

pub(in super::super) fn unknown_file(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM fragments WHERE location_id=?)").bind(id)
}
pub(in super::super) fn forget_file(id: Uuid, node: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM multipart_files WHERE id=? AND node_id=? AND source_id IS NULL AND NOT EXISTS(SELECT 1 FROM fragments WHERE file_id=multipart_files.id) AND NOT EXISTS(SELECT 1 FROM parts WHERE stream_id=multipart_files.part_stream) AND NOT EXISTS(SELECT 1 FROM native_part_reads pr WHERE pr.stream_id=multipart_files.part_stream AND pr.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)) AND NOT EXISTS(SELECT 1 FROM node_writes w WHERE w.stream_id=multipart_files.part_stream AND w.phase='active' AND w.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(id).bind(node)
}
