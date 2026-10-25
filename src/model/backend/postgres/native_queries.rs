use super::*;
use crate::model::node_native as native;

pub(crate) fn pending(node: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM parts p JOIN uploads u ON u.id=p.upload_id WHERE p.remote_node_id=$1 AND p.stream_id IS NOT NULL AND p.remote_etag IS NULL AND p.remote_retry_at<=clock_timestamp() AND (p.remote_expires_at IS NULL OR p.remote_expires_at<=clock_timestamp()) AND u.state IN ('active','completing')) OR EXISTS(SELECT 1 FROM uploads u WHERE u.raw_source_id IS NOT NULL AND u.state IN ('aborted','completed') AND (u.remote_expires_at IS NULL OR u.remote_expires_at<=clock_timestamp()) AND (u.completion_expires_at IS NULL OR u.completion_expires_at<=clock_timestamp()))").bind(node)
}

pub(crate) fn create_file(
    stream: Id,
    upload: Uuid,
    storage: Uuid,
    node: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO multipart_files(id,upload_id,storage_id,node_id) VALUES($1,$2,$3,$4)")
        .bind(stream)
        .bind(upload)
        .bind(storage)
        .bind(node)
}

pub(crate) fn local_part(node: Uuid, operation: Uuid) -> Rows<'static, (Vec<u8>, i64)> {
    sqlx::query_as(
        "SELECT request_hash,size FROM node_write_local WHERE node_id=$1 AND operation_id=$2",
    )
    .bind(node)
    .bind(operation)
}

pub(crate) fn local_fragments<'a>(stream: Id, size: i64, hashes: &'a [Vec<u8>]) -> Statement<'a> {
    sqlx::query("INSERT INTO fragments(id,size,hash,owner_stream,file_id,file_offset,sealed) SELECT gen_random_uuid(),LEAST(4194304,$2-(ordinal-1)*4194304)::integer,hash,$1,$1,(ordinal-1)*4194304,true FROM unnest($3::bytea[]) WITH ORDINALITY AS manifest(hash,ordinal)")
        .bind(stream).bind(size).bind(hashes)
}

pub(crate) fn local_extents(stream: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,fragment_id) SELECT $1,file_offset,size,id FROM fragments WHERE file_id=$1 AND owner_stream=$1 ORDER BY file_offset").bind(stream)
}

pub(crate) fn seal_file(stream: Id, node: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE multipart_files SET state='sealed' WHERE id=$1 AND node_id=$2 AND state='open'",
    )
    .bind(stream)
    .bind(node)
}

pub(crate) fn record_local<'a>(
    node: Uuid,
    input: &'a native::LocalPart,
    hash: &'a [u8],
) -> Statement<'a> {
    sqlx::query(
        "INSERT INTO node_write_local(node_id,operation_id,request_hash,size) VALUES($1,$2,$3,$4)",
    )
    .bind(node)
    .bind(input.operation)
    .bind(hash)
    .bind(input.size)
}

pub(crate) fn queue_part(
    upload: Uuid,
    number: i32,
    stream: Id,
    node: Uuid,
    cached: bool,
) -> Statement<'static> {
    sqlx::query("UPDATE parts SET remote_node_id=$4,remote_etag=NULL,remote_token=NULL,remote_expires_at=NULL,remote_retry_at=clock_timestamp(),remote_error=NULL,remote_cached=$5 WHERE upload_id=$1 AND part_number=$2 AND stream_id=$3 AND EXISTS(SELECT 1 FROM uploads WHERE id=$1 AND raw_source_id IS NOT NULL AND remote_manifest_hash IS NULL)")
        .bind(upload).bind(number).bind(stream).bind(node).bind(cached)
}

#[derive(sqlx::FromRow)]
pub(crate) struct Work {
    pub credential_generation: i64,
    pub boot_id: Uuid,
    pub fence: Uuid,
    pub target_stream: Option<Uuid>,
    pub token: Uuid,
    pub storage_id: Uuid,
    pub read_lease: Option<Uuid>,
    pub work: sqlx::types::Json<native::Work>,
    pub valid_for_ms: i64,
    pub result_hash: Option<Vec<u8>>,
}

pub(crate) fn work(node: Uuid, operation: Uuid) -> Rows<'static, Work> {
    sqlx::query_as("SELECT credential_generation,boot_id,fence,target_stream,token,storage_id,read_lease,work,GREATEST(0,(extract(epoch FROM(expires_at-clock_timestamp()))*1000)::bigint) AS valid_for_ms,result_hash FROM node_native_work WHERE node_id=$1 AND operation_id=$2 FOR UPDATE")
        .bind(node).bind(operation)
}

pub(crate) fn work_count(node: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_native_work WHERE node_id=$1 AND expires_at>clock_timestamp() AND result_hash IS NULL").bind(node)
}

pub(crate) fn cleanup(node: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM node_native_work WHERE node_id=$1 AND expires_at<clock_timestamp()-interval '24 hours'").bind(node)
}

pub(crate) fn part_ready(node: Uuid, stream: Uuid) -> Rows<'static, (bool, bool)> {
    sqlx::query_as("SELECT p.remote_etag IS NOT NULL,coalesce(p.remote_error IN ('local_source_missing','local_source_corrupt'),false) FROM parts p JOIN uploads u ON u.id=p.upload_id WHERE p.stream_id=$2 AND p.remote_node_id=$1 AND u.state IN ('active','completing')")
        .bind(node).bind(stream)
}

pub(crate) fn initialize_due(node: Uuid, stream: Option<Uuid>) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT u.id FROM uploads u JOIN raw_sources r ON r.id=u.raw_source_id WHERE u.state IN ('active','completing') AND r.upload_id IS NULL AND r.state='writing' AND (u.remote_expires_at IS NULL OR u.remote_expires_at<=clock_timestamp()) AND EXISTS(SELECT 1 FROM parts p JOIN multipart_files m ON m.id=p.stream_id WHERE p.upload_id=u.id AND p.remote_node_id=$1 AND ($2::uuid IS NULL OR p.stream_id=$2) AND m.node_id=$1 AND m.state='sealed') ORDER BY u.touched_at LIMIT 1 FOR UPDATE OF u SKIP LOCKED")
        .bind(node).bind(stream)
}

pub(crate) fn part_file(stream: Id, node: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM multipart_files WHERE id=$1 AND node_id=$2 AND state='sealed'",
    )
    .bind(stream)
    .bind(node)
}

pub(crate) fn insert_work<'a>(
    node: &'a NodeState,
    input: &'a native::Claim,
    token: Uuid,
    work: &'a native::Work,
    lease: Option<Uuid>,
) -> Statement<'a> {
    sqlx::query("INSERT INTO node_native_work(node_id,operation_id,credential_generation,boot_id,fence,target_stream,token,storage_id,read_lease,work,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,clock_timestamp()+interval '60 seconds')")
        .bind(node.node_id).bind(input.operation).bind(node.credential_generation).bind(input.run.boot).bind(input.run.fence).bind(input.stream).bind(token).bind(work.upload.storage).bind(lease).bind(sqlx::types::Json(work))
}

pub(crate) fn result<'a>(node: Uuid, operation: Uuid, hash: &'a [u8]) -> Statement<'a> {
    sqlx::query("UPDATE node_native_work SET result_hash=$3,expires_at=clock_timestamp() WHERE node_id=$1 AND operation_id=$2 AND result_hash IS NULL").bind(node).bind(operation).bind(hash)
}

pub(crate) fn finish_part<'a>(
    work: &'a native::Work,
    token: Uuid,
    node: Uuid,
    value: Option<&'a str>,
    source_error: Option<&'a str>,
) -> Statement<'a> {
    let native::Action::Part { stream, number, .. } = &work.action else {
        unreachable!()
    };
    sqlx::query("UPDATE parts p SET remote_etag=$6,remote_token=NULL,remote_expires_at=NULL,remote_retry_at=CASE WHEN $7::text IS NOT NULL THEN 'infinity'::timestamptz ELSE clock_timestamp()+interval '3 seconds' END,remote_error=coalesce($7,CASE WHEN $6::text IS NULL THEN 'native transfer will retry' END) FROM uploads u WHERE p.upload_id=$1 AND p.part_number=$2 AND p.stream_id=$3 AND p.remote_token=$4 AND p.remote_node_id=$5 AND p.remote_expires_at>clock_timestamp() AND u.id=p.upload_id AND u.state IN ('active','completing')")
        .bind(work.upload.upload).bind(number).bind(stream).bind(token).bind(node).bind(value).bind(source_error)
}

pub(crate) fn failed(upload: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM parts p JOIN node_completion_parts c ON c.upload_id=p.upload_id AND c.part_number=p.part_number AND c.stream_id=p.stream_id WHERE p.upload_id=$1 AND p.remote_etag IS NULL AND p.remote_error IN ('local_source_missing','local_source_corrupt'))").bind(upload)
}

pub(crate) fn recover<'a>(node: Uuid, ids: &'a [Uuid]) -> Statement<'a> {
    sqlx::query("UPDATE parts p SET remote_error=NULL,remote_retry_at=clock_timestamp() FROM multipart_files m,uploads u WHERE p.stream_id=m.id AND m.id=ANY($2) AND p.remote_node_id=$1 AND m.node_id=$1 AND m.state='sealed' AND u.id=p.upload_id AND u.state IN ('active','completing') AND p.remote_etag IS NULL AND p.remote_error IN ('local_source_missing','local_source_corrupt')").bind(node).bind(ids)
}

pub(crate) fn clean_remote(upload: Uuid, token: Uuid, retry: bool) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET raw_source_id=CASE WHEN $3 THEN raw_source_id END,remote_token=NULL,remote_expires_at=CASE WHEN $3 THEN clock_timestamp()+interval '3 seconds' END WHERE id=$1 AND remote_token=$2 AND remote_expires_at>clock_timestamp() AND state IN ('aborted','completed')")
        .bind(upload).bind(token).bind(retry)
}

pub(crate) fn renew<'a>(
    node: &'a NodeState,
    run: crate::model::session::Run,
    ids: &'a [Uuid],
    release: bool,
) -> Rows<'a, (Uuid, i64)> {
    sqlx::query_as("WITH owned AS (UPDATE node_native_work w SET expires_at=clock_timestamp()+CASE WHEN $6 THEN interval '0 seconds' ELSE interval '60 seconds' END WHERE node_id=$1 AND credential_generation=$2 AND boot_id=$3 AND fence=$4 AND token=ANY($5) AND expires_at>clock_timestamp() AND result_hash IS NULL AND EXISTS(SELECT 1 FROM storage_permits p WHERE p.token=w.token AND p.expires_at>clock_timestamp()) AND (EXISTS(SELECT 1 FROM parts p JOIN uploads u ON u.id=p.upload_id WHERE p.remote_token=w.token AND p.remote_node_id=$1 AND p.remote_expires_at>clock_timestamp() AND u.state IN ('active','completing') AND p.stream_id=NULLIF(w.work->'action'->>'stream','')::uuid) OR EXISTS(SELECT 1 FROM uploads u WHERE u.remote_token=w.token AND u.remote_expires_at>clock_timestamp())) RETURNING token,read_lease,expires_at), permits AS (UPDATE storage_permits p SET expires_at=o.expires_at FROM owned o WHERE p.token=o.token), reads AS (UPDATE stream_leases l SET expires_at=o.expires_at FROM owned o WHERE l.id=o.read_lease), parts AS (UPDATE parts p SET remote_expires_at=o.expires_at FROM owned o WHERE p.remote_token=o.token), uploads AS (UPDATE uploads u SET remote_expires_at=o.expires_at FROM owned o WHERE u.remote_token=o.token) SELECT token,CASE WHEN $6 THEN 0 ELSE 60000 END::bigint FROM owned")
        .bind(node.node_id).bind(node.credential_generation).bind(run.boot).bind(run.fence).bind(ids).bind(release)
}

pub(crate) fn removable<'a>(node: Uuid, ids: &'a [Uuid]) -> Scalar<'a, Uuid> {
    sqlx::query_scalar("WITH selected AS (SELECT m.id FROM multipart_files m WHERE m.node_id=$1 AND m.source_id IS NULL AND m.id=ANY($2) AND NOT EXISTS(SELECT 1 FROM fragments f WHERE f.file_id=m.id) AND NOT EXISTS(SELECT 1 FROM stream_leases l WHERE l.stream_id=m.id AND l.expires_at>clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM parts p WHERE p.stream_id=m.id) FOR UPDATE), removed AS (DELETE FROM multipart_files m USING selected s WHERE m.id=s.id RETURNING m.id) SELECT id FROM removed UNION SELECT candidate.id FROM unnest($2::uuid[]) AS candidate(id) WHERE NOT EXISTS(SELECT 1 FROM multipart_files m WHERE m.id=candidate.id) AND NOT EXISTS(SELECT 1 FROM fragments f WHERE f.id=candidate.id)")
        .bind(node).bind(ids)
}

pub(crate) fn completion_parts(upload: Uuid) -> Rows<'static, native::Part> {
    sqlx::query_as("SELECT c.part_number AS number,c.size,c.etag,p.remote_etag FROM node_completion_parts c JOIN parts p ON p.upload_id=c.upload_id AND p.part_number=c.part_number AND p.stream_id=c.stream_id WHERE c.upload_id=$1 ORDER BY c.ordinal").bind(upload)
}

pub(crate) fn resend(upload: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE parts p SET remote_etag=NULL,remote_retry_at=clock_timestamp() FROM node_completion_parts c WHERE c.upload_id=$1 AND p.upload_id=c.upload_id AND p.part_number=c.part_number AND p.stream_id=c.stream_id").bind(upload)
}

pub(crate) fn read_permit(node: Uuid, operation: Uuid) -> Rows<'static, (Uuid, i64)> {
    sqlx::query_as("SELECT p.token,GREATEST(0,(extract(epoch FROM(p.expires_at-clock_timestamp()))*1000)::bigint) FROM node_write_permits n JOIN storage_permits p ON p.token=n.token WHERE n.node_id=$1 AND n.operation_id=$2 AND n.batch_id IS NULL AND p.direction='read' AND p.expires_at>clock_timestamp()").bind(node).bind(operation)
}

pub(crate) fn release_reads<'a>(
    node: &'a NodeState,
    run: crate::model::session::Run,
    ids: &'a [Uuid],
) -> Statement<'a> {
    sqlx::query("DELETE FROM storage_permits p USING node_write_permits n,node_writes w WHERE p.token=n.token AND w.node_id=n.node_id AND w.operation_id=n.operation_id AND w.node_id=$1 AND w.credential_generation=$2 AND w.boot_id=$3 AND w.fence=$4 AND p.token=ANY($5) AND p.direction='read'")
        .bind(node.node_id).bind(node.credential_generation).bind(run.boot).bind(run.fence).bind(ids)
}
