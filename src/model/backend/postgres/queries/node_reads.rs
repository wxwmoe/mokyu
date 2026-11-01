use super::*;

pub(in super::super) fn credential_buckets<'q>(
    access: &'q str,
    prefix: &'q str,
    after: &'q str,
    region: Option<&'q str>,
    expected_region: &'q str,
    limit: i64,
) -> Rows<'q, Bucket> {
    sqlx::query_as("SELECT b.* FROM buckets b JOIN s3_credential_grants g ON g.bucket_id=b.id JOIN s3_credentials c ON c.access_key_id=g.access_key WHERE g.access_key=$1 AND c.enabled AND (c.expires_at IS NULL OR c.expires_at>now()) AND c.project_id=b.project_id AND 'bucket.list'=ANY(g.actions) AND starts_with(b.name,$2) AND b.name>$3 AND ($4::text IS NULL OR $4=$5) ORDER BY b.name LIMIT $6")
        .bind(access).bind(prefix).bind(after).bind(region).bind(expected_region).bind(limit)
}

pub(in super::super) fn node_catalog_objects<'q>(
    bucket: Uuid,
    prefix: &'q str,
    key: &'q str,
    inclusive: bool,
) -> Rows<'q, crate::model::node_request::Object> {
    let pattern = format!(
        "{}%",
        prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    );
    sqlx::query_as("SELECT o.object_key AS key,s.size,s.etag,s.touched_at AS modified_at FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=$1 AND o.object_key LIKE $2 AND (o.object_key>$3 OR ($4 AND o.object_key=$3)) ORDER BY o.object_key LIMIT 64").bind(bucket).bind(pattern).bind(key).bind(inclusive)
}

pub(in super::super) fn node_object_acl(bucket: Uuid, key: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT s.public_read FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=$1 AND o.object_key=$2 AND s.state='ready'").bind(bucket).bind(key)
}

pub(in super::super) fn node_catalog_buckets<'q>(
    access: &'q str,
    prefix: &'q str,
    after: &'q str,
    region: Option<&'q str>,
    expected_region: &'q str,
    limit: i64,
) -> Rows<'q, crate::model::node_request::Bucket> {
    sqlx::query_as("SELECT b.name,b.created_at FROM buckets b JOIN s3_credential_grants g ON g.bucket_id=b.id JOIN s3_credentials c ON c.access_key_id=g.access_key WHERE g.access_key=$1 AND c.enabled AND (c.expires_at IS NULL OR c.expires_at>now()) AND c.project_id=b.project_id AND 'bucket.list'=ANY(g.actions) AND starts_with(b.name,$2) AND b.name>$3 AND ($4::text IS NULL OR $4=$5) ORDER BY b.name LIMIT $6")
        .bind(access).bind(prefix).bind(after).bind(region).bind(expected_region).bind(limit)
}

pub(in super::super) fn node_read_extents(
    stream: Id,
    start: i64,
    end: i64,
    limit: i64,
) -> Rows<'static, Extent> {
    sqlx::query_as("SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=$2-4194304 AND offset_bytes+length>$2 AND offset_bytes<$3 ORDER BY offset_bytes LIMIT $4 FOR SHARE")
        .bind(stream).bind(start).bind(end).bind(limit)
}

#[derive(sqlx::FromRow)]
pub(in super::super) struct NodeReadChunk {
    #[sqlx(flatten)]
    pub chunk: Chunk,
    pub pack_id: Option<Id>,
}

pub(in super::super) fn node_read_chunks(ids: &[Id]) -> Rows<'_, NodeReadChunk> {
    sqlx::query_as("SELECT *, '00000000-0000-0000-0000-000000000000'::uuid AS location_id FROM chunks WHERE id=ANY($1) AND state='ready' ORDER BY id FOR SHARE")
        .bind(ids)
}

pub(in super::super) fn node_read_encodings(ids: &[Id]) -> Rows<'_, Chunk> {
    sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=0 AND tl.physical_id=l.encoding_id) AS tier_material,c.id,l.encoding_id AS current_encoding_id,l.encoding_id AS location_id,l.storage_id,c.hash,c.raw_size,l.standalone_size,l.encryption_algorithm,l.block_key_id,l.standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunks c JOIN LATERAL (SELECT * FROM chunk_encodings e WHERE e.chunk_id=c.id AND e.state='ready' AND e.quarantined_at IS NULL ORDER BY e.encoding_id LIMIT 1 FOR SHARE) l ON true WHERE c.id=ANY($1) ORDER BY c.id")
        .bind(ids)
}

pub(in super::super) fn node_read_packs(ids: &[Id]) -> Rows<'_, crate::content::pack::Pack> {
    sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=1 AND tl.physical_id=p.id) AS tier_material,p.* FROM packs p WHERE id=ANY($1) AND state='ready' AND quarantined_at IS NULL ORDER BY id FOR SHARE").bind(ids)
}

pub(in super::super) fn node_read_pending(ids: &[Id]) -> Rows<'_, (Id, Option<Uuid>)> {
    sqlx::query_as("SELECT chunk_id,node_id FROM pending_uploads WHERE chunk_id=ANY($1) AND staging_storage_id IS NULL AND source_pack_id IS NULL ORDER BY chunk_id FOR SHARE").bind(ids)
}

#[derive(sqlx::FromRow)]
pub(in super::super) struct NodeReadFragment {
    #[sqlx(flatten)]
    pub source: FragmentSource,
    pub node_id: Option<Uuid>,
}

pub(in super::super) fn node_read_fragments(ids: &[Id]) -> Rows<'_, NodeReadFragment> {
    sqlx::query_as("SELECT f.id AS fragment_location,f.size,f.hash,f.staging_storage_id,f.staging_object_key,f.file_id,f.file_offset,m.storage_id AS file_storage,CASE WHEN m.source_id IS NOT NULL THEN m.id END AS raw_source,m.node_id FROM fragments f LEFT JOIN multipart_files m ON m.id=f.file_id WHERE f.id=ANY($1) ORDER BY f.id FOR SHARE OF f")
        .bind(ids)
}

#[derive(sqlx::FromRow)]
pub(in super::super) struct NodeReadRaw {
    #[sqlx(flatten)]
    pub range: crate::model::raw::RawRange,
    pub node_id: Option<Uuid>,
}

pub(in super::super) fn node_read_raw(
    stream: Id,
    start: i64,
    end: i64,
    limit: i64,
) -> Rows<'static, NodeReadRaw> {
    sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=2 AND tl.physical_id=r.id) AS tier_material,r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,e.offset_bytes,e.length,e.source_offset,u.node_id FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id LEFT JOIN raw_uploads u ON u.source_id=r.id WHERE e.stream_id=$1 AND e.offset_bytes>=COALESCE((SELECT offset_bytes FROM raw_ranges WHERE stream_id=$1 AND offset_bytes<=$2 ORDER BY offset_bytes DESC LIMIT 1),$2) AND e.offset_bytes+e.length>$2 AND e.offset_bytes<$3 ORDER BY e.offset_bytes LIMIT $4 FOR SHARE OF e,r")
        .bind(stream).bind(start).bind(end).bind(limit)
}

pub(in super::super) fn node_read_storages(
    ids: &[Uuid],
) -> Rows<'_, crate::storage::model::Record> {
    sqlx::query_as("SELECT s.* FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=ANY($1) AND s.enabled AND s.type='s3' ORDER BY s.id FOR SHARE OF s,p").bind(ids)
}

pub(in super::super) fn node_read_profiles(
    ids: &[Uuid],
) -> Rows<'_, crate::storage::profile::StorageProfile> {
    sqlx::query_as(
        "SELECT * FROM storage_profiles WHERE storage_id=ANY($1) ORDER BY storage_id FOR SHARE",
    )
    .bind(ids)
}

pub(in super::super) fn node_read_keys(ids: &[Uuid]) -> Rows<'_, ManagedKeyMaterial> {
    sqlx::query_as("SELECT id,name,purpose,key_algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE id=ANY($1) AND purpose='block' AND secret_encrypted IS NOT NULL ORDER BY id FOR SHARE").bind(ids)
}

#[derive(sqlx::FromRow)]
pub(in super::super) struct NodeReadLease {
    pub read_id: Uuid,
    pub stream_id: Id,
    pub payload_revision: i64,
    pub offset_bytes: i64,
    pub end_bytes: i64,
    pub request_hash: Vec<u8>,
    pub replica_allowed: bool,
    pub valid_for_ms: i64,
}

pub(in super::super) fn node_read_existing<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    operation: Uuid,
) -> Rows<'q, NodeReadLease> {
    sqlx::query_as("SELECT r.*,l.stream_id,GREATEST(0,(extract(epoch FROM (l.expires_at-clock_timestamp()))*1000)::bigint) AS valid_for_ms FROM node_reads r JOIN stream_leases l ON l.id=r.read_id WHERE r.node_id=$1 AND r.credential_generation=$2 AND r.boot_id=$3 AND r.fence=$4 AND r.operation_id=$5 FOR SHARE OF r,l")
        .bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(operation)
}

pub(in super::super) fn node_read_owned(
    state: &crate::cluster::NodeState,
    run: crate::model::session::Run,
    id: Uuid,
) -> Rows<'_, NodeReadLease> {
    sqlx::query_as("SELECT r.*,l.stream_id,GREATEST(0,(extract(epoch FROM (l.expires_at-clock_timestamp()))*1000)::bigint) AS valid_for_ms FROM node_reads r JOIN stream_leases l ON l.id=r.read_id WHERE r.node_id=$1 AND r.credential_generation=$2 AND r.boot_id=$3 AND r.fence=$4 AND r.read_id=$5 AND l.expires_at>clock_timestamp() FOR SHARE OF r,l")
        .bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(id)
}

pub(in super::super) fn node_read_count(node: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_reads r JOIN stream_leases l ON l.id=r.read_id WHERE r.node_id=$1 AND l.expires_at>clock_timestamp()").bind(node)
}

pub(in super::super) fn node_read_insert<'q>(
    state: &'q crate::cluster::NodeState,
    input: &'q crate::model::node_read::Query,
    id: Uuid,
    stream: &crate::app::StoredStream,
    start: i64,
    end: i64,
    hash: &'q [u8],
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_reads(read_id,node_id,credential_generation,boot_id,fence,operation_id,request_hash,payload_revision,offset_bytes,end_bytes,replica_allowed) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)")
        .bind(id).bind(state.node_id).bind(state.credential_generation).bind(input.run.boot).bind(input.run.fence).bind(input.operation).bind(hash).bind(stream.payload_revision).bind(start).bind(end)
        .bind(input.access.is_none() && stream.public_read && stream.payload_type == "chunks")
}

pub(in super::super) fn node_read_stream(id: Id) -> Rows<'static, crate::app::StoredStream> {
    sqlx::query_as("SELECT * FROM streams WHERE id=$1 AND state IN ('ready','retired') FOR SHARE")
        .bind(id)
}

pub(in super::super) fn node_read_window(
    read: Uuid,
    start: i64,
) -> Rows<'static, (Uuid, sqlx::types::Json<crate::model::node_read::Mapping>)> {
    sqlx::query_as(
        "SELECT id,mapping FROM node_read_windows WHERE read_id=$1 AND offset_bytes=$2 FOR SHARE",
    )
    .bind(read)
    .bind(start)
}

pub(in super::super) fn node_read_window_count(read: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_read_windows WHERE read_id=$1").bind(read)
}

pub(in super::super) fn node_read_report_window(
    read: Uuid,
    window: Uuid,
) -> Scalar<'static, sqlx::types::Json<crate::model::node_read::Mapping>> {
    sqlx::query_scalar("SELECT mapping FROM node_read_windows WHERE read_id=$1 AND id=$2 FOR SHARE")
        .bind(read)
        .bind(window)
}

pub(in super::super) fn node_read_reported(task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=$1)").bind(task)
}

pub(in super::super) fn node_read_verifications() -> Scalar<'static, i64> {
    sqlx::query_scalar(
        "SELECT count(*) FROM tasks WHERE task_type='integrity' AND detail->>'operation'='verify' AND state IN ('queued','running','paused')",
    )
}

pub(in super::super) fn node_read_insert_window(
    read: Uuid,
    mapping: &crate::model::node_read::Mapping,
) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("INSERT INTO node_read_windows(read_id,offset_bytes,end_bytes,mapping) VALUES($1,$2,$3,$4) RETURNING id")
        .bind(read).bind(mapping.start).bind(mapping.end).bind(sqlx::types::Json(mapping))
}

pub(in super::super) fn node_read_pin<'q>(
    window: Uuid,
    kinds: &'q [String],
    ids: &'q [Uuid],
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_read_sources(window_id,kind,location_id) SELECT $1,unnest($2::text[]),unnest($3::uuid[]) ON CONFLICT DO NOTHING")
        .bind(window).bind(kinds).bind(ids)
}

pub(in super::super) fn node_read_permit(read: Uuid, storage: Uuid) -> Rows<'static, (Uuid, i64)> {
    sqlx::query_as("SELECT p.token,GREATEST(0,(extract(epoch FROM (p.expires_at-clock_timestamp()))*1000)::bigint) FROM node_read_permits n JOIN storage_permits p ON p.token=n.token WHERE n.read_id=$1 AND p.storage_id=$2 AND p.direction='read' AND p.expires_at>clock_timestamp() FOR SHARE OF p")
        .bind(read).bind(storage)
}

pub(in super::super) fn node_read_own_permit(read: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO node_read_permits(token,read_id) VALUES($1,$2) ON CONFLICT DO NOTHING")
        .bind(token)
        .bind(read)
}

pub(in super::super) fn node_read_shared_permits<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    tokens: &'q [Uuid],
) -> Rows<'q, (Uuid, Uuid, i64)> {
    sqlx::query_as("SELECT p.token,p.storage_id,GREATEST(0,(extract(epoch FROM(p.expires_at-clock_timestamp()))*1000)::bigint) FROM storage_permits p WHERE p.direction='read' AND p.token=ANY($5) AND p.expires_at>clock_timestamp() AND EXISTS(SELECT 1 FROM node_read_permits n JOIN node_reads r ON r.read_id=n.read_id JOIN stream_leases l ON l.id=r.read_id WHERE n.token=p.token AND r.node_id=$1 AND r.credential_generation=$2 AND r.boot_id=$3 AND r.fence=$4 AND l.expires_at>clock_timestamp()) FOR SHARE OF p")
        .bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(tokens)
}

pub(in super::super) fn node_read_permit_count(read: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_read_permits n JOIN storage_permits p ON p.token=n.token WHERE n.read_id=$1 AND p.expires_at>clock_timestamp()").bind(read)
}

pub(in super::super) fn node_read_renew<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    r#type: crate::model::node_read::GrantType,
    ids: &'q [Uuid],
) -> Rows<'q, (Uuid, i64)> {
    use crate::model::node_read::GrantType;
    let query = match r#type {
        GrantType::Native => return native::renew(state, run, ids, false),
        GrantType::Work => return work::renew(state, run, ids, false),
        GrantType::Read => {
            "UPDATE stream_leases l SET expires_at=clock_timestamp()+interval '90 seconds' FROM node_reads r WHERE l.id=r.read_id AND r.node_id=$1 AND r.credential_generation=$2 AND r.boot_id=$3 AND r.fence=$4 AND l.id=ANY($5) AND l.expires_at>clock_timestamp() RETURNING l.id,90000::bigint"
        }
        GrantType::StorageRead => {
            "UPDATE storage_permits p SET expires_at=clock_timestamp()+interval '1 minute' WHERE p.token=ANY($5) AND p.direction='read' AND p.expires_at>clock_timestamp() AND (EXISTS(SELECT 1 FROM node_read_permits n JOIN node_reads r ON r.read_id=n.read_id JOIN stream_leases l ON l.id=r.read_id WHERE n.token=p.token AND r.node_id=$1 AND r.credential_generation=$2 AND r.boot_id=$3 AND r.fence=$4 AND l.expires_at>clock_timestamp()) OR EXISTS(SELECT 1 FROM node_write_permits n JOIN node_writes w ON w.node_id=n.node_id AND w.operation_id=n.operation_id JOIN stream_leases l ON l.id=w.lease_id JOIN uploads u ON u.id=w.completion_upload WHERE n.token=p.token AND w.node_id=$1 AND w.credential_generation=$2 AND w.boot_id=$3 AND w.fence=$4 AND w.state='active' AND l.expires_at>clock_timestamp() AND u.state='completing' AND u.completion_token=w.operation_id AND u.completion_expires_at>clock_timestamp())) RETURNING p.token,60000::bigint"
        }
        GrantType::Window => {
            "SELECT w.id,GREATEST(0,(extract(epoch FROM (l.expires_at-clock_timestamp()))*1000)::bigint) FROM node_read_windows w JOIN node_reads r ON r.read_id=w.read_id JOIN stream_leases l ON l.id=r.read_id WHERE r.node_id=$1 AND r.credential_generation=$2 AND r.boot_id=$3 AND r.fence=$4 AND w.id=ANY($5) AND l.expires_at>clock_timestamp()"
        }
        GrantType::Write => {
            "UPDATE stream_leases l SET expires_at=clock_timestamp()+interval '90 seconds' FROM node_writes w,streams s WHERE l.id=w.lease_id AND s.id=w.stream_id AND ((w.upload_id IS NULL AND (w.completion_upload IS NULL OR EXISTS(SELECT 1 FROM uploads c WHERE c.id=w.completion_upload AND c.state='completing' AND c.completion_token=w.operation_id AND c.completion_expires_at>clock_timestamp())) AND (EXISTS(SELECT 1 FROM objects o WHERE o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.write_token=s.id) OR EXISTS(SELECT 1 FROM tasks t WHERE t.id=s.owner_task_id AND t.lease_token=s.owner_lease_token AND t.lease_node_id=w.node_id AND t.state='running' AND t.lease_expires_at>clock_timestamp() AND (t.task_type='storage_migration' OR t.task_type='upload' AND t.detail->>'operation'='cdc')))) OR (w.upload_id IS NOT NULL AND EXISTS(SELECT 1 FROM parts a JOIN uploads u ON u.id=a.upload_id WHERE a.upload_id=w.upload_id AND a.part_number=w.part_number AND a.write_token=s.id AND u.state='active' AND u.remote_manifest_hash IS NULL))) AND s.state='writing' AND w.state='active' AND w.node_id=$1 AND w.credential_generation=$2 AND w.boot_id=$3 AND w.fence=$4 AND l.id=ANY($5) AND l.expires_at>clock_timestamp() RETURNING l.id,90000::bigint"
        }
        GrantType::StorageWrite => {
            "UPDATE storage_permits p SET expires_at=clock_timestamp()+interval '1 minute' FROM node_write_permits n,node_writes w,stream_leases l,streams s WHERE p.token=n.token AND w.node_id=n.node_id AND w.operation_id=n.operation_id AND ((n.batch_id IS NOT NULL AND EXISTS(SELECT 1 FROM node_write_batches b WHERE b.node_id=n.node_id AND b.operation_id=n.operation_id AND b.batch_id=n.batch_id AND b.commit_hash IS NULL)) OR (n.batch_id IS NULL AND EXISTS(SELECT 1 FROM node_write_raw r WHERE r.node_id=n.node_id AND r.operation_id=n.operation_id AND NOT r.committed)) OR (n.batch_id IS NULL AND EXISTS(SELECT 1 FROM node_write_tails t WHERE t.node_id=n.node_id AND t.operation_id=n.operation_id AND NOT t.committed)) OR (n.batch_id IS NULL AND w.completion_upload IS NOT NULL AND EXISTS(SELECT 1 FROM uploads u WHERE u.id=w.completion_upload AND u.raw_source_id IS NOT NULL))) AND l.id=w.lease_id AND s.id=w.stream_id AND s.state='writing' AND ((w.upload_id IS NULL AND (w.completion_upload IS NULL OR EXISTS(SELECT 1 FROM uploads c WHERE c.id=w.completion_upload AND c.state='completing' AND c.completion_token=w.operation_id AND c.completion_expires_at>clock_timestamp())) AND (EXISTS(SELECT 1 FROM objects o WHERE o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.write_token=s.id) OR EXISTS(SELECT 1 FROM tasks t WHERE t.id=s.owner_task_id AND t.lease_token=s.owner_lease_token AND t.lease_node_id=w.node_id AND t.state='running' AND t.lease_expires_at>clock_timestamp() AND (t.task_type='storage_migration' OR t.task_type='upload' AND t.detail->>'operation'='cdc')))) OR (w.upload_id IS NOT NULL AND EXISTS(SELECT 1 FROM parts a JOIN uploads u ON u.id=a.upload_id WHERE a.upload_id=w.upload_id AND a.part_number=w.part_number AND a.write_token=s.id AND u.state='active' AND u.remote_manifest_hash IS NULL))) AND w.state='active' AND w.node_id=$1 AND w.credential_generation=$2 AND w.boot_id=$3 AND w.fence=$4 AND p.token=ANY($5) AND p.expires_at>clock_timestamp() AND l.expires_at>clock_timestamp() RETURNING p.token,60000::bigint"
        }
    };
    sqlx::query_as(query)
        .bind(state.node_id)
        .bind(state.credential_generation)
        .bind(run.boot)
        .bind(run.fence)
        .bind(ids)
}

pub(in super::super) fn node_read_release_permits<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    reads: &'q [Uuid],
    permits: &'q [Uuid],
) -> Statement<'q> {
    sqlx::query("DELETE FROM storage_permits p USING node_read_permits n,node_reads r WHERE p.token=n.token AND r.read_id=n.read_id AND r.node_id=$1 AND r.credential_generation=$2 AND r.boot_id=$3 AND r.fence=$4 AND (p.token=ANY($6) OR (r.read_id=ANY($5) AND NOT EXISTS(SELECT 1 FROM node_read_permits other WHERE other.token=p.token AND NOT(other.read_id=ANY($5)))))")
        .bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(reads).bind(permits)
}

pub(in super::super) fn node_read_release_windows<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    ids: &'q [Uuid],
) -> Statement<'q> {
    sqlx::query("DELETE FROM node_read_windows w USING node_reads r WHERE w.read_id=r.read_id AND r.node_id=$1 AND r.credential_generation=$2 AND r.boot_id=$3 AND r.fence=$4 AND w.id=ANY($5)")
        .bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(ids)
}
