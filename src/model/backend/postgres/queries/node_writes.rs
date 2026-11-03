use super::*;

#[derive(Clone, sqlx::FromRow)]
pub(in super::super) struct NodeWrite {
    pub operation_id: Uuid,
    pub bucket_id: Option<Uuid>,
    pub credential_generation: i64,
    pub boot_id: Uuid,
    pub fence: Uuid,
    pub request_hash: Vec<u8>,
    pub stream_id: Option<Uuid>,
    pub lease_id: Option<Uuid>,
    pub storage_id: Option<Uuid>,
    pub block_key_id: Option<Uuid>,
    pub expected_size: Option<i64>,
    pub next_batch: i64,
    pub next_offset: i64,
    pub publish_hash: Option<Vec<u8>>,
    pub result: Option<sqlx::types::Json<crate::model::node_write::Published>>,
    pub profile: sqlx::types::Json<crate::storage::profile::StorageProfile>,
    pub copy_source: Option<sqlx::types::Json<crate::model::node_write::CopySource>>,
    pub upload_id: Option<Uuid>,
    pub part_number: Option<i32>,
    pub checksum_algorithm: Option<String>,
    pub completion_upload: Option<Uuid>,
    pub owner_task_id: Option<Uuid>,
    pub owner_lease_token: Option<Uuid>,
    pub state: String,
    pub valid_for_ms: i64,
}

pub(in super::super) fn node_write(node: Uuid, operation: Uuid) -> Rows<'static, NodeWrite> {
    sqlx::query_as("SELECT w.*,s.bucket_id AS bucket_id,s.owner_task_id,s.owner_lease_token,CASE WHEN w.state='active' AND s.state='writing' AND ((w.upload_id IS NULL AND (w.completion_upload IS NULL OR EXISTS(SELECT 1 FROM uploads c WHERE c.id=w.completion_upload AND c.state='completing' AND c.completion_token=w.operation_id AND c.completion_expires_at>clock_timestamp())) AND (EXISTS(SELECT 1 FROM objects o WHERE o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.write_token=s.id) OR EXISTS(SELECT 1 FROM tasks t WHERE t.id=s.owner_task_id AND t.lease_token=s.owner_lease_token AND t.lease_node_id=w.node_id AND t.state='running' AND t.lease_expires_at>clock_timestamp() AND (t.task_type='storage_migration' OR t.task_type='upload' AND t.detail->>'operation'='cdc')))) OR (w.upload_id IS NOT NULL AND EXISTS(SELECT 1 FROM parts a JOIN uploads u ON u.id=a.upload_id WHERE a.upload_id=w.upload_id AND a.part_number=w.part_number AND a.write_token=s.id AND u.state='active' AND u.remote_manifest_hash IS NULL))) THEN GREATEST(0,(extract(epoch FROM (l.expires_at-clock_timestamp()))*1000)::bigint) ELSE 0 END AS valid_for_ms FROM node_writes w LEFT JOIN stream_leases l ON l.id=w.lease_id LEFT JOIN streams s ON s.id=w.stream_id WHERE w.node_id=$1 AND w.operation_id=$2 FOR UPDATE OF w")
        .bind(node).bind(operation)
}

pub(in super::super) fn node_write_count(node: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_writes w JOIN stream_leases l ON l.id=w.lease_id WHERE w.node_id=$1 AND w.state='active' AND l.expires_at>clock_timestamp()").bind(node)
}

#[derive(sqlx::FromRow)]
pub(in super::super) struct NodeTail {
    pub request_hash: Vec<u8>,
    pub fragment_id: Uuid,
    pub storage_id: Uuid,
    pub object_key: String,
    pub offset_bytes: i64,
    pub size: i32,
    pub committed: bool,
}

impl NodeTail {
    pub(in super::super) fn reply(&self) -> crate::model::node_tail::Prepared {
        crate::model::node_tail::Prepared {
            fragment: self.fragment_id,
            object_key: self.object_key.clone(),
            end: self.offset_bytes + i64::from(self.size),
            committed: self.committed,
            materials: Vec::new(),
            storage: None,
            grants: Vec::new(),
        }
    }
}

pub(in super::super) fn node_write_tail(node: Uuid, operation: Uuid) -> Rows<'static, NodeTail> {
    sqlx::query_as("SELECT * FROM node_write_tails WHERE node_id=$1 AND operation_id=$2")
        .bind(node)
        .bind(operation)
}

pub(in super::super) fn node_staging_settings()
-> Scalar<'static, sqlx::types::Json<crate::config::UploadSettings>> {
    sqlx::query_scalar("SELECT value FROM runtime_settings WHERE section='uploads' FOR SHARE")
}

pub(in super::super) fn insert_node_tail<'q>(
    node: Uuid,
    input: &crate::model::node_tail::Prepare,
    hash: &'q [u8],
    fragment: Uuid,
    storage: Uuid,
    path: &'q str,
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_write_tails(node_id,operation_id,request_hash,fragment_id,storage_id,object_key,offset_bytes,size) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
        .bind(node).bind(input.operation).bind(hash).bind(fragment).bind(storage).bind(path).bind(input.offset).bind(input.size)
}

pub(in super::super) fn commit_node_tail(node: Uuid, operation: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE node_write_tails SET committed=true WHERE node_id=$1 AND operation_id=$2 AND NOT committed")
        .bind(node).bind(operation)
}

pub(in super::super) fn finish_node_tail(
    node: Uuid,
    operation: Uuid,
    end: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE node_writes SET next_offset=$3 WHERE node_id=$1 AND operation_id=$2")
        .bind(node)
        .bind(operation)
        .bind(end)
}

pub(in super::super) fn node_object_change(
    node: Uuid,
    operation: Uuid,
) -> Rows<
    'static,
    (
        Vec<u8>,
        i64,
        sqlx::types::Json<crate::model::node_object::Outcome>,
    ),
> {
    sqlx::query_as("SELECT request_hash,credential_generation,outcome FROM node_object_changes WHERE node_id=$1 AND operation_id=$2")
        .bind(node).bind(operation)
}

pub(in super::super) fn save_node_object_change<'q>(
    node: &crate::cluster::NodeState,
    operation: Uuid,
    hash: &'q [u8],
    outcome: crate::model::node_object::Outcome,
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_object_changes(node_id,operation_id,credential_generation,request_hash,outcome) VALUES($1,$2,$3,$4,$5)")
        .bind(node.node_id).bind(operation).bind(node.credential_generation).bind(hash).bind(sqlx::types::Json(outcome))
}

pub(in super::super) fn expire_node_object_changes(node: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM node_object_changes WHERE node_id=$1 AND operation_id IN (SELECT operation_id FROM node_object_changes WHERE node_id=$1 AND created_at<clock_timestamp()-interval '1 day' ORDER BY created_at LIMIT 64)").bind(node)
}

pub(in super::super) fn node_write_credential(access: &str) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT authorization_revision FROM s3_credentials WHERE access_key_id=$1 AND enabled AND (expires_at IS NULL OR expires_at>clock_timestamp()) FOR SHARE").bind(access)
}

pub(in super::super) fn node_write_storage(id: Uuid) -> Rows<'static, (String, Option<Uuid>)> {
    sqlx::query_as("SELECT type,block_key_id FROM storages WHERE id=$1 AND enabled FOR SHARE")
        .bind(id)
}

pub(in super::super) fn node_write_limit(id: Uuid) -> Scalar<'static, Option<i32>> {
    sqlx::query_scalar(
        "SELECT upload_concurrency FROM storage_profiles WHERE storage_id=$1 FOR SHARE",
    )
    .bind(id)
}

pub(in super::super) fn node_write_cleanup(node: Uuid) -> Statement<'static> {
    sqlx::query(
        "WITH expired AS MATERIALIZED (
            SELECT w.operation_id FROM node_writes w
            LEFT JOIN stream_leases l ON l.id=w.lease_id
            WHERE w.node_id=$1 AND (w.state<>'active' OR w.publish_hash IS NULL)
              AND COALESCE(w.completed_at,w.created_at)<clock_timestamp()-interval '1 day'
              AND (l.id IS NULL OR l.expires_at<=clock_timestamp())
            ORDER BY w.created_at LIMIT 64
        )
        DELETE FROM node_writes w USING expired
        WHERE w.node_id=$1 AND w.operation_id=expired.operation_id",
    )
    .bind(node)
}

#[allow(clippy::too_many_arguments)]
pub(in super::super) fn insert_node_write<'q>(
    state: &'q crate::cluster::NodeState,
    input: &'q crate::model::node_write::Begin,
    hash: &'q [u8],
    stream: Id,
    profile: &'q crate::storage::profile::StorageProfile,
    block_key: Option<Uuid>,
    source: Option<&'q crate::model::node_write::CopySource>,
    algorithm: Option<&'q str>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_writes(node_id,operation_id,credential_generation,boot_id,fence,request_hash,stream_id,lease_id,storage_id,block_key_id,profile,expected_size,copy_source,upload_id,part_number,checksum_algorithm) SELECT $1,$2,$3,$4,$5,$6,$7,l.id,$8,$9,$10,$11,$12,$13,$14,$15 FROM stream_leases l WHERE l.stream_id=$7 AND l.lease_type='write'")
        .bind(state.node_id).bind(input.operation).bind(state.credential_generation).bind(input.run.boot).bind(input.run.fence)
        .bind(hash).bind(stream).bind(profile.storage_id).bind(block_key).bind(sqlx::types::Json(profile)).bind(input.intent.size).bind(source.map(sqlx::types::Json))
        .bind(input.intent.part.as_ref().map(|part| part.upload)).bind(input.intent.part.as_ref().map(|part| part.number)).bind(algorithm)
}

pub(in super::super) fn node_copy_source(
    source: &crate::model::node_write::CopySource,
) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT l.id FROM stream_leases l JOIN streams s ON s.id=l.stream_id WHERE l.id=$1 AND l.stream_id=$2 AND l.lease_type='read' AND l.expires_at>clock_timestamp() AND s.payload_revision=$3 AND s.state IN ('ready','retired') FOR SHARE OF l,s")
        .bind(source.read).bind(source.stream).bind(source.revision)
}

pub(in super::super) fn save_node_copy_batch<'q>(
    node: Uuid,
    input: &crate::model::node_write::CopyBatch,
    hash: &'q [u8],
    end: i64,
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_write_batches(node_id,operation_id,batch_id,request_hash,end_bytes,plan,ready,commit_hash) VALUES($1,$2,$3,$4,$5,'[]'::jsonb,true,$4)")
        .bind(node).bind(input.operation).bind(input.batch).bind(hash).bind(end)
}

pub(in super::super) fn abort_node_writes<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    operations: &'q [Uuid],
    leases: &'q [Uuid],
) -> Rows<'q, (Option<Uuid>, Option<Uuid>)> {
    sqlx::query_as("UPDATE node_writes SET state='aborted',completed_at=clock_timestamp() WHERE node_id=$1 AND credential_generation=$2 AND boot_id=$3 AND fence=$4 AND state='active' AND publish_hash IS NULL AND (operation_id=ANY($5) OR lease_id=ANY($6)) RETURNING stream_id,lease_id")
        .bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(operations).bind(leases)
}

pub(in super::super) fn abandon_node_streams(ids: &[Uuid]) -> Statement<'_> {
    sqlx::query("UPDATE streams SET state='abandoned',touched_at=clock_timestamp() WHERE id=ANY($1) AND state='writing' AND NOT EXISTS(SELECT 1 FROM replica_publications rp WHERE rp.source_pin=streams.id AND rp.phase='preparing') AND NOT EXISTS(SELECT 1 FROM replica_targets rt WHERE rt.candidate=streams.id AND rt.phase IN ('preparing','prepared'))").bind(ids)
}

pub(in super::super) fn release_node_write_leases(ids: &[Uuid]) -> Statement<'_> {
    sqlx::query("DELETE FROM stream_leases WHERE id=ANY($1) AND lease_type='write'").bind(ids)
}

#[derive(sqlx::FromRow)]
pub(in super::super) struct NodeWriteBatch {
    pub packing: Option<sqlx::types::Json<crate::model::node_pack::Batch>>,
    pub request_hash: Vec<u8>,
    pub end_bytes: i64,
    pub plan: sqlx::types::Json<Vec<crate::model::node_chunk::Item>>,
    pub ready: bool,
    pub commit_hash: Option<Vec<u8>>,
}

#[derive(sqlx::FromRow)]
pub(in super::super) struct NodeRawWrite {
    pub request_hash: Vec<u8>,
    pub source_id: Uuid,
    pub size: i64,
    pub digest: Vec<u8>,
    pub content_hash: Option<Vec<u8>>,
    pub committed: bool,
}

pub(in super::super) fn node_raw_write(node: Uuid, operation: Uuid) -> Rows<'static, NodeRawWrite> {
    sqlx::query_as("SELECT request_hash,source_id,size,digest,content_hash,committed FROM node_write_raw WHERE node_id=$1 AND operation_id=$2 FOR UPDATE").bind(node).bind(operation)
}

pub(in super::super) fn insert_node_raw_write<'q>(
    node: Uuid,
    input: &'q crate::model::node_raw::Prepare,
    hash: &'q [u8],
    source: Uuid,
    committed: bool,
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_write_raw(node_id,operation_id,request_hash,source_id,size,digest,content_hash,committed) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
        .bind(node).bind(input.operation).bind(hash).bind(source).bind(input.input.size)
        .bind(input.input.digest.as_slice()).bind(input.input.hash.as_ref().map(|hash| hash.as_slice())).bind(committed)
}

pub(in super::super) fn commit_node_raw_write(node: Uuid, operation: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE node_write_raw SET committed=true WHERE node_id=$1 AND operation_id=$2 AND NOT committed").bind(node).bind(operation)
}

pub(in super::super) fn node_write_batch(
    node: Uuid,
    operation: Uuid,
    batch: i64,
) -> Rows<'static, NodeWriteBatch> {
    sqlx::query_as("SELECT request_hash,end_bytes,plan,packing,ready,commit_hash FROM node_write_batches WHERE node_id=$1 AND operation_id=$2 AND batch_id=$3 FOR UPDATE")
        .bind(node).bind(operation).bind(batch)
}

pub(in super::super) fn node_write_pending(node: Uuid, operation: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT (SELECT count(*) FROM node_write_batches WHERE node_id=$1 AND operation_id=$2 AND commit_hash IS NULL)+(SELECT count(*) FROM node_write_raw WHERE node_id=$1 AND operation_id=$2 AND NOT committed)").bind(node).bind(operation)
}

pub(in super::super) fn insert_node_write_batch<'q>(
    node: Uuid,
    input: &'q crate::model::node_chunk::Prepare,
    hash: &'q [u8],
    end: i64,
    plan: &'q [crate::model::node_chunk::Item],
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_write_batches(node_id,operation_id,batch_id,request_hash,end_bytes,plan) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(node).bind(input.operation).bind(input.batch).bind(hash).bind(end).bind(sqlx::types::Json(plan))
}

pub(in super::super) fn advance_node_write(
    node: Uuid,
    operation: Uuid,
    end: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE node_writes SET next_batch=next_batch+1,next_offset=$3 WHERE node_id=$1 AND operation_id=$2 AND state='active'").bind(node).bind(operation).bind(end)
}

pub(in super::super) fn activate_node_write_batch<'q>(
    node: Uuid,
    operation: Uuid,
    batch: i64,
    plan: &'q [crate::model::node_chunk::Item],
) -> Statement<'q> {
    sqlx::query("UPDATE node_write_batches SET ready=true,plan=$4 WHERE node_id=$1 AND operation_id=$2 AND batch_id=$3 AND NOT ready").bind(node).bind(operation).bind(batch).bind(sqlx::types::Json(plan))
}

pub(in super::super) fn commit_node_write_batch<'q>(
    node: Uuid,
    operation: Uuid,
    batch: i64,
    hash: &'q [u8],
) -> Statement<'q> {
    sqlx::query("UPDATE node_write_batches SET commit_hash=$4,plan='[]'::jsonb,packing=NULL WHERE node_id=$1 AND operation_id=$2 AND batch_id=$3 AND ready AND commit_hash IS NULL").bind(node).bind(operation).bind(batch).bind(hash)
}

pub(in super::super) fn node_write_size(stream: Id) -> Rows<'static, (i64, DateTime<Utc>)> {
    sqlx::query_as("SELECT size,touched_at FROM streams WHERE id=$1 FOR UPDATE").bind(stream)
}

pub(in super::super) fn stage_node_write<'q>(
    node: Uuid,
    operation: Uuid,
    hash: &'q [u8],
) -> Statement<'q> {
    sqlx::query("UPDATE node_writes SET publish_hash=$3 WHERE node_id=$1 AND operation_id=$2 AND state='active' AND publish_hash IS NULL")
        .bind(node).bind(operation).bind(hash)
}

pub(in super::super) fn publish_node_write<'q>(
    node: Uuid,
    operation: Uuid,
    hash: &'q [u8],
    result: &'q crate::model::node_write::Published,
) -> Statement<'q> {
    sqlx::query("UPDATE node_writes SET state='published',completed_at=clock_timestamp(),publish_hash=$3,result=$4 WHERE node_id=$1 AND operation_id=$2 AND state='active'")
        .bind(node).bind(operation).bind(hash).bind(sqlx::types::Json(result))
}

pub(in super::super) fn node_write_permits(
    node: Uuid,
    operation: Uuid,
    batch: Option<i64>,
) -> Rows<'static, (Uuid, i64)> {
    sqlx::query_as("SELECT p.token,GREATEST(0,(extract(epoch FROM (p.expires_at-clock_timestamp()))*1000)::bigint) FROM storage_permits p JOIN node_write_permits w ON w.token=p.token WHERE w.node_id=$1 AND w.operation_id=$2 AND w.batch_id IS NOT DISTINCT FROM $3 AND p.direction='upload' AND p.expires_at>clock_timestamp() ORDER BY p.token")
        .bind(node).bind(operation).bind(batch)
}

pub(in super::super) fn node_write_own_permit(
    node: Uuid,
    operation: Uuid,
    batch: Option<i64>,
    token: Uuid,
) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO node_write_permits(token,node_id,operation_id,batch_id) VALUES($4,$1,$2,$3)",
    )
    .bind(node)
    .bind(operation)
    .bind(batch)
    .bind(token)
}

pub(in super::super) fn release_write_upload_permits(
    node: Uuid,
    operation: Uuid,
    batch: Option<i64>,
) -> Statement<'static> {
    sqlx::query("DELETE FROM storage_permits p USING node_write_permits n WHERE p.token=n.token AND n.node_id=$1 AND n.operation_id=$2 AND n.batch_id IS NOT DISTINCT FROM $3").bind(node).bind(operation).bind(batch)
}

pub(in super::super) fn node_write_release_permits<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    streams: &'q [Uuid],
    permits: &'q [Uuid],
) -> Statement<'q> {
    sqlx::query("DELETE FROM storage_permits p USING node_write_permits n,node_writes w WHERE p.token=n.token AND w.node_id=n.node_id AND w.operation_id=n.operation_id AND w.node_id=$1 AND w.credential_generation=$2 AND w.boot_id=$3 AND w.fence=$4 AND (w.stream_id=ANY($5) OR p.token=ANY($6))")
        .bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(streams).bind(permits)
}

pub(in super::super) fn node_multipart_operation(
    node: Uuid,
    operation: Uuid,
) -> Rows<
    'static,
    (
        Vec<u8>,
        i64,
        sqlx::types::Json<crate::model::node_multipart::Outcome>,
    ),
> {
    sqlx::query_as("SELECT request_hash,credential_generation,outcome FROM node_multipart_operations WHERE node_id=$1 AND operation_id=$2").bind(node).bind(operation)
}

pub(in super::super) fn save_node_multipart_operation<'q>(
    node: &crate::cluster::NodeState,
    operation: Uuid,
    hash: &'q [u8],
    outcome: &'q crate::model::node_multipart::Outcome,
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_multipart_operations(node_id,operation_id,credential_generation,request_hash,outcome) VALUES($1,$2,$3,$4,$5)").bind(node.node_id).bind(operation).bind(node.credential_generation).bind(hash).bind(sqlx::types::Json(outcome))
}

pub(in super::super) fn expire_node_multipart_operations(node: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM node_multipart_operations WHERE node_id=$1 AND operation_id IN (SELECT operation_id FROM node_multipart_operations WHERE node_id=$1 AND created_at<clock_timestamp()-interval '1 day' ORDER BY created_at LIMIT 64)").bind(node)
}

pub(in super::super) fn node_upload_page<'q>(
    bucket: Uuid,
    access: &'q str,
    prefix: &'q str,
    after: &'q str,
    inclusive: bool,
    id: Option<Uuid>,
) -> Rows<'q, crate::model::multipart::UploadSummary> {
    sqlx::query_as("SELECT id,object_key,created_at,checksum_algorithm,checksum_type FROM uploads WHERE bucket_id=$1 AND access_key=$2 AND state IN ('active','completing') AND starts_with(object_key,$3) AND (object_key>$4 OR (object_key=$4 AND ($5 OR id>$6))) ORDER BY object_key,id LIMIT 64").bind(bucket).bind(access).bind(prefix).bind(after).bind(inclusive).bind(id)
}

#[derive(sqlx::FromRow)]
pub(in super::super) struct NodeCompletion {
    pub manifest_hash: String,
    pub conditions: sqlx::types::Json<crate::model::node_complete::Conditions>,
    pub summary: sqlx::types::Json<crate::model::node_complete::Summary>,
}

pub(in super::super) fn node_completion(upload: Uuid) -> Rows<'static, NodeCompletion> {
    sqlx::query_as(
        "SELECT manifest_hash,conditions,summary FROM node_completions WHERE upload_id=$1",
    )
    .bind(upload)
}

pub(in super::super) fn node_completion_summary(
    upload: Uuid,
    node: Uuid,
) -> Rows<
    'static,
    (
        sqlx::types::Json<crate::model::node_complete::Summary>,
        Option<Value>,
    ),
> {
    sqlx::query_as(r#"
        SELECT c.summary || jsonb_build_object('pending_sources', u.state='completing' AND (
            EXISTS(SELECT 1 FROM node_completion_parts n JOIN parts p ON p.upload_id=n.upload_id AND p.part_number=n.part_number AND p.stream_id=n.stream_id
                WHERE n.upload_id=c.upload_id AND u.raw_source_id IS NOT NULL AND p.remote_etag IS NULL
                AND coalesce(p.remote_error NOT IN ('local_source_missing','local_source_corrupt'),true))
            OR EXISTS(SELECT 1 FROM node_completion_parts n JOIN raw_ranges e ON e.stream_id=n.stream_id
                JOIN raw_sources r ON r.id=e.source_id JOIN raw_uploads a ON a.source_id=r.id
                WHERE n.upload_id=c.upload_id AND r.medium='local' AND r.pending_upload AND r.state='ready'
                AND r.quarantine_code IS NULL AND a.node_id<>$2)
        )),u.result FROM node_completions c JOIN uploads u ON u.id=c.upload_id WHERE c.upload_id=$1
    "#).bind(upload).bind(node)
}

pub(in super::super) fn node_selected_parts<'q>(
    upload: Uuid,
    numbers: &'q [i32],
) -> Rows<'q, crate::model::node_complete::Selected> {
    sqlx::query_as("SELECT p.part_number AS number,s.size,s.etag,s.checksums FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=$1 AND p.part_number=ANY($2) AND s.state='ready' ORDER BY p.part_number FOR SHARE OF p,s").bind(upload).bind(numbers)
}

pub(in super::super) fn save_node_completion<'q>(
    upload: Uuid,
    hash: &'q str,
    conditions: &'q crate::model::node_complete::Conditions,
    summary: &'q crate::model::node_complete::Summary,
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_completions(upload_id,manifest_hash,conditions,summary) VALUES($1,$2,$3,$4)").bind(upload).bind(hash).bind(sqlx::types::Json(conditions)).bind(sqlx::types::Json(summary))
}

pub(in super::super) fn freeze_node_completion<'q>(
    upload: Uuid,
    numbers: &'q [i32],
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_completion_parts(upload_id,ordinal,part_number,stream_id,size,etag,checksums) SELECT $1,n.ordinal-1,p.part_number,s.id,s.size,s.etag,s.checksums FROM unnest($2::integer[]) WITH ORDINALITY AS n(number,ordinal) JOIN parts p ON p.upload_id=$1 AND p.part_number=n.number JOIN streams s ON s.id=p.stream_id").bind(upload).bind(numbers)
}

pub(in super::super) fn node_completion_checksums(upload: Uuid) -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT checksums FROM node_completion_parts WHERE upload_id=$1 ORDER BY ordinal",
    )
    .bind(upload)
}

pub(in super::super) fn node_completion_sources(
    upload: Uuid,
    first: i32,
    count: i64,
) -> Rows<'static, crate::app::StoredStream> {
    sqlx::query_as("SELECT s.* FROM node_completion_parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=$1 AND p.ordinal>=$2 AND p.ordinal<$2::integer+$3::bigint AND s.state IN ('ready','retired') ORDER BY p.ordinal LIMIT $3 FOR SHARE OF s").bind(upload).bind(first).bind(count)
}

pub(in super::super) fn bind_node_completion(
    node: Uuid,
    operation: Uuid,
    upload: Uuid,
    size: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE node_writes SET completion_upload=$3,expected_size=$4 WHERE node_id=$1 AND operation_id=$2").bind(node).bind(operation).bind(upload).bind(size)
}

pub(in super::super) fn renew_node_completions<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    leases: &'q [Uuid],
) -> Statement<'q> {
    sqlx::query("UPDATE uploads u SET completion_expires_at=clock_timestamp()+interval '120 seconds',touched_at=clock_timestamp() FROM node_writes w,stream_leases l WHERE w.completion_upload=u.id AND u.completion_token=w.operation_id AND u.state='completing' AND u.completion_expires_at>clock_timestamp() AND w.state='active' AND w.node_id=$1 AND w.credential_generation=$2 AND w.boot_id=$3 AND w.fence=$4 AND l.id=w.lease_id AND l.id=ANY($5) AND l.expires_at>clock_timestamp()").bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(leases)
}

pub(in super::super) fn release_node_completions(streams: &[Uuid]) -> Statement<'_> {
    sqlx::query("UPDATE uploads u SET completion_expires_at=clock_timestamp() FROM node_writes w WHERE w.completion_upload=u.id AND u.completion_token=w.operation_id AND u.state='completing' AND w.stream_id=ANY($1) AND w.state='aborted'").bind(streams)
}

pub(in super::super) fn raw_gc_candidates(
    storage: Uuid,
    grace: i64,
    minimum: i64,
    limit: i64,
) -> Rows<'static, RawSource> {
    sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=2 AND tl.physical_id=r.id) AS tier_material,r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r LEFT JOIN tier_locations th ON th.unit=2 AND th.physical_id=r.id WHERE COALESCE(th.storage_id,r.storage_id)=$1 AND r.owner_stream IS NULL AND r.unreferenced_at<now()-$2*interval '1 second' AND r.created_at<now()-$3*interval '1 second' AND NOT EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.kind='raw' AND n.location_id=COALESCE(th.address,r.id) AND l.expires_at>clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=r.id) AND NOT EXISTS(SELECT 1 FROM transfer_sources p JOIN transfer_runs t ON t.task_id=p.task_id WHERE p.source_type='raw' AND p.source_id=r.id AND t.phase NOT IN ('complete','cancelled')) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=r.id) AND NOT EXISTS(SELECT 1 FROM uploads WHERE raw_source_id=r.id) AND NOT EXISTS(SELECT 1 FROM tier_moves mv WHERE mv.unit=2 AND mv.physical_id=r.id) AND NOT EXISTS(SELECT 1 FROM tier_locations th WHERE th.unit=2 AND th.physical_id=r.id AND th.activated_at>floor(extract(epoch from clock_timestamp()))::bigint-$3::bigint) ORDER BY r.unreferenced_at,r.id LIMIT $4").bind(storage).bind(grace).bind(minimum).bind(limit)
}

pub(in super::super) fn raw_gc_claim(
    source: Id,
    revision: i64,
    grace: i64,
    minimum: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET state='deleting',revision=revision+1 WHERE id=$1 AND revision=$2 AND owner_stream IS NULL AND unreferenced_at<now()-$3*interval '1 second' AND created_at<now()-$4*interval '1 second' AND NOT EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.kind='raw' AND n.location_id=COALESCE((SELECT th.address FROM tier_locations th WHERE th.unit=2 AND th.physical_id=raw_sources.id),raw_sources.id) AND l.expires_at>clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.task_type='integrity' AND t.state='running' AND t.detail->>'operation' IN ('recheck','verify') AND t.detail->>'source_location'=raw_sources.id::text AND ((t.lease_node_id IS NULL AND t.lease_expires_at IS NULL) OR t.lease_expires_at>clock_timestamp())) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM transfer_sources p JOIN transfer_runs t ON t.task_id=p.task_id WHERE p.source_type='raw' AND p.source_id=raw_sources.id AND t.phase NOT IN ('complete','cancelled')) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM uploads WHERE raw_source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM tier_moves mv WHERE mv.unit=2 AND mv.physical_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM tier_locations th WHERE th.unit=2 AND th.physical_id=raw_sources.id AND th.activated_at>floor(extract(epoch from clock_timestamp()))::bigint-$4::bigint)").bind(source).bind(revision).bind(grace).bind(minimum)
}

pub(in super::super) fn raw_gc_finish(source: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_sources WHERE id=$1 AND state='deleting' AND NOT EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.kind='raw' AND n.location_id=raw_sources.id AND l.expires_at>clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.task_type='integrity' AND t.state='running' AND t.detail->>'operation' IN ('recheck','verify') AND t.detail->>'source_location'=raw_sources.id::text AND ((t.lease_node_id IS NULL AND t.lease_expires_at IS NULL) OR t.lease_expires_at>clock_timestamp())) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM transfer_sources p JOIN transfer_runs t ON t.task_id=p.task_id WHERE p.source_type='raw' AND p.source_id=raw_sources.id AND t.phase NOT IN ('complete','cancelled')) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM uploads WHERE raw_source_id=raw_sources.id)").bind(source)
}

pub(in super::super) fn node_upload_backlog(
    node: Option<Uuid>,
) -> Rows<'static, (i64, i64, Option<DateTime<Utc>>)> {
    sqlx::query_as("SELECT count(*),COALESCE(sum(size),0)::bigint,min(created_at) FROM (SELECT staged_size AS size,created_at FROM pending_uploads WHERE ($1::uuid IS NULL OR node_id=$1) UNION ALL SELECT r.size,r.created_at FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE ($1::uuid IS NULL OR u.node_id=$1) UNION ALL SELECT s.size,s.created_at FROM parts p JOIN streams s ON s.id=p.stream_id JOIN uploads u ON u.id=p.upload_id WHERE p.remote_node_id IS NOT NULL AND p.remote_etag IS NULL AND ($1::uuid IS NULL OR p.remote_node_id=$1) AND u.state IN ('active','completing')) waiting").bind(node)
}
