//! PostgreSQL statements, row decoding, and nontransactional operations.

#[path = "notification_queries.rs"]
pub(super) mod notifications;

#[path = "pack_queries.rs"]
pub(super) mod pack_work;
use crate::cluster::NodeState;
use crate::model::Id;
use crate::model::multipart::{RemoteCommit, RemoteOutput, RemotePart, RemoteUpload};
use crate::model::task::{Execution, TaskFinish};

use super::{Backend, connection, normalize_error, transactions};
use crate::app::{Bucket, Extent};
use crate::authorization::{Action, Permit, Principal};
use crate::content::codec::Chunk;
use crate::model::account::{
    AccountStore, AuthSession, Preferences, Profile, RefreshedSession, Session, SessionLogin,
    SessionRefresh, SessionTimes, TokenIdentity, VerifiedAccount,
};
use crate::model::administration::{
    AdministrationStore, BucketGrant, Member, Membership, Project, ProjectInput, User, UserCreate,
    UserPatch,
};
use crate::model::bucket::{
    BucketImpact, BucketProject, BucketStorage, BucketStore, BucketUpdate, BucketView,
    CreateBucket, Domain, TransferInput, TransferProject, Website,
};
use crate::model::credential::{
    Credential, CredentialInput, CredentialMaterial, CredentialSettings, CredentialStore,
    RotateCredential, Token, TokenInput, TokenMaterial,
};
use crate::model::key::{ManagedKeyCreate, ManagedKeyMaterial, ManagedKeyView};
use crate::model::multipart::{
    MultipartCreate, MultipartStore, PartStitch, ReceivedPart, SeedConsumption, Transfer,
    TransferFilter, Upload, UploadPart, WebUpload,
};
use crate::model::settings::{ControlSettingsPatch, RuntimeSettingsRow};
use crate::model::{
    ChunkCreate, ChunkStore, FragmentSource, ManagedKeyRecord, ManagedKeyStore, ReadExtent,
    RuntimeSettingsStore, StorageProfileStore, StorageRecord, StorageRecordStore, StreamCreate,
    TaskClaim, TaskControlStore, TaskExecutionStore, TaskRecord,
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use s3s::s3_error;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::{
    Postgres,
    postgres::PgArguments,
    query::{Query, QueryAs, QueryScalar},
};
use uuid::Uuid;

#[path = "native_queries.rs"]
pub(super) mod native;
#[path = "work_queries.rs"]
pub(super) mod work;

type Statement<'q> = Query<'q, Postgres, PgArguments>;
pub(super) type Rows<'q, T> = QueryAs<'q, Postgres, T, PgArguments>;
type Scalar<'q, T> = QueryScalar<'q, Postgres, T, PgArguments>;
type TaskFenceRow = (String, Option<Uuid>, Option<Uuid>, String, Value);

pub(super) const MIGRATION_SETUP: &str = "SET synchronous_commit = on; SET lock_timeout = '30s'";

pub(super) fn storage_for_stream(stream: Id) -> Scalar<'static, Option<Uuid>> {
    sqlx::query_scalar(
        "SELECT b.storage_id FROM streams s JOIN buckets b ON b.id=s.bucket_id WHERE s.id=$1",
    )
    .bind(stream)
}

pub(super) fn extents_for_copy(stream: Id, from: i64) -> Rows<'static, Extent> {
    sqlx::query_as("SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=$2 ORDER BY offset_bytes LIMIT 500")
        .bind(stream)
        .bind(from)
}

fn ready_chunk(id: Id) -> Rows<'static, Chunk> {
    sqlx::query_as("SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at FROM chunks c LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE c.id=$1 AND c.state='ready'")
        .bind(id)
}

fn fragment_source(id: Id) -> Rows<'static, FragmentSource> {
    sqlx::query_as(
        "SELECT f.id AS fragment_location,f.size,f.hash,f.staging_storage_id,f.staging_object_key,f.file_id,f.file_offset,m.storage_id AS file_storage,CASE WHEN m.source_id IS NOT NULL THEN m.id END AS raw_source FROM fragments f LEFT JOIN multipart_files m ON m.id=f.file_id WHERE f.id=$1",
    )
    .bind(id)
}

fn extents(stream: Id, from: i64, until: i64) -> Rows<'static, Extent> {
    sqlx::query_as("SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=$2 AND offset_bytes<$3 ORDER BY offset_bytes LIMIT 64")
        .bind(stream)
        .bind(from)
        .bind(until)
}

fn read_extents(stream: Id, from: i64, until: i64) -> Rows<'static, ReadExtent> {
    // Page before joining; a missing ready chunk must fail row decoding.
    sqlx::query_as("SELECT e.*,c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at FROM (SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=$2 AND offset_bytes<$3 ORDER BY offset_bytes LIMIT 64) e LEFT JOIN chunks c ON c.id=e.chunk_id AND c.state='ready' LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' ORDER BY e.offset_bytes")
        .bind(stream)
        .bind(from)
        .bind(until)
}

#[derive(Clone, sqlx::FromRow)]
pub(super) struct ReplicaIdentity {
    pub deployment_id: Uuid,
    pub schema_version: i32,
    pub deployment_key_fingerprint: Vec<u8>,
    pub deployment_mode: String,
}

pub(super) fn replica_identity() -> Rows<'static, ReplicaIdentity> {
    sqlx::query_as(
        "SELECT deployment_id,schema_version,deployment_key_fingerprint,deployment_mode FROM mokyu_meta WHERE singleton",
    )
}

pub(super) fn check_replica(identity: &ReplicaIdentity) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT pg_is_in_recovery() AND current_setting('server_version_num')::int>=180000 AND EXISTS(SELECT 1 FROM mokyu_meta WHERE singleton AND deployment_id=$1 AND schema_version=$2 AND deployment_key_fingerprint=$3 AND deployment_mode='cluster')")
        .bind(identity.deployment_id).bind(identity.schema_version).bind(&identity.deployment_key_fingerprint)
}

pub(super) async fn replica_page(
    connection: &mut sqlx::PgConnection,
    identity: &ReplicaIdentity,
    stream: Id,
    lease: Uuid,
    from: i64,
    until: i64,
) -> Result<Option<Vec<ReadExtent>>> {
    use sqlx::{FromRow, Row};
    // The lease marker and the page share one snapshot; an older lease cannot authorize this request.
    let rows = sqlx::query("WITH checked AS MATERIALIZED (SELECT pg_is_in_recovery() AND EXISTS(SELECT 1 FROM mokyu_meta WHERE singleton AND deployment_id=$5 AND schema_version=$6 AND deployment_key_fingerprint=$7 AND deployment_mode='cluster') AS valid,EXISTS(SELECT 1 FROM stream_leases WHERE id=$4 AND stream_id=$1 AND lease_type='read') AS visible) SELECT checked.valid,checked.visible,e.*,c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at FROM checked LEFT JOIN LATERAL (SELECT * FROM extents WHERE checked.valid AND checked.visible AND stream_id=$1 AND offset_bytes>=$2 AND offset_bytes<$3 ORDER BY offset_bytes LIMIT 64) e ON TRUE LEFT JOIN chunks c ON c.id=e.chunk_id AND c.state='ready' LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' ORDER BY e.offset_bytes")
        .bind(stream).bind(from).bind(until).bind(lease)
        .bind(identity.deployment_id).bind(identity.schema_version).bind(&identity.deployment_key_fingerprint)
        .fetch_all(connection).await?;
    let first = rows.first().context("replica validation returned no row")?;
    ensure!(
        first.try_get::<bool, _>("valid")?,
        "replica identity or role changed"
    );
    if !first.try_get::<bool, _>("visible")? {
        return Ok(None);
    }
    let mut page = Vec::with_capacity(rows.len());
    let mut position = from;
    for row in &rows {
        let row = ReadExtent::from_row(row)?;
        let extent = &row.extent;
        ensure!(
            extent.stream_id == stream
                && extent.offset_bytes == position
                && extent.length > 0
                && extent.fragment_id.is_none()
                && extent.chunk_id == Some(row.chunk.id)
                && extent.source_offset >= 0
                && i64::from(extent.source_offset) + i64::from(extent.length)
                    <= i64::from(row.chunk.raw_size)
                && row.chunk.hash.len() == 32
                && row.chunk.state == "ready",
            "invalid replica mapping"
        );
        position = position
            .checked_add(i64::from(extent.length))
            .context("replica mapping overflow")?;
        page.push(row);
    }
    ensure!(
        page.len() == 64 || position >= until,
        "replica mapping is incomplete"
    );
    Ok(Some(page))
}

pub(super) async fn node_replica_page(
    connection: &mut sqlx::PgConnection,
    identity: &ReplicaIdentity,
    cursor: &crate::model::node_read::Cursor,
    limit: usize,
) -> Result<Option<Vec<Extent>>> {
    use sqlx::{FromRow, Row};
    // A replicated read marker binds this page to the primary's leased payload revision.
    let rows = sqlx::query("WITH checked AS MATERIALIZED (SELECT pg_is_in_recovery() AND EXISTS(SELECT 1 FROM mokyu_meta WHERE singleton AND deployment_id=$1 AND schema_version=$2 AND deployment_key_fingerprint=$3 AND deployment_mode='cluster') AS valid,EXISTS(SELECT 1 FROM node_reads n JOIN stream_leases l ON l.id=n.read_id JOIN streams s ON s.id=l.stream_id WHERE n.read_id=$4 AND n.replica_allowed AND l.stream_id=$5 AND l.lease_type='read' AND n.payload_revision=$6 AND s.payload_revision=$6 AND s.payload_type='chunks' AND n.offset_bytes<=$7 AND n.end_bytes=$8) AS visible) SELECT checked.valid,checked.visible,e.* FROM checked LEFT JOIN LATERAL (SELECT * FROM extents WHERE checked.valid AND checked.visible AND stream_id=$5 AND offset_bytes>=$7-4194304 AND offset_bytes+length>$7 AND offset_bytes<$8 ORDER BY offset_bytes LIMIT $9) e ON true ORDER BY e.offset_bytes")
        .bind(identity.deployment_id).bind(identity.schema_version).bind(&identity.deployment_key_fingerprint)
        .bind(cursor.lease).bind(cursor.stream).bind(cursor.revision).bind(cursor.offset).bind(cursor.end).bind(limit as i64)
        .fetch_all(connection).await?;
    let first = rows.first().context("replica validation returned no row")?;
    ensure!(
        first.try_get::<bool, _>("valid")?,
        "replica identity or role changed"
    );
    if !first.try_get::<bool, _>("visible")? {
        return Ok(None);
    }
    let mut page = Vec::with_capacity(rows.len());
    let mut position = cursor.offset;
    for row in &rows {
        let extent = Extent::from_row(row)?;
        if extent.fragment_id.is_some() {
            return Ok(None);
        }
        let end = extent
            .offset_bytes
            .checked_add(i64::from(extent.length))
            .context("replica mapping overflow")?;
        ensure!(
            extent.stream_id == Id::Uuid(cursor.stream)
                && extent.chunk_id.is_some()
                && extent.offset_bytes <= position
                && end > position
                && extent.length > 0
                && extent.source_offset >= 0
                && i64::from(extent.source_offset) + i64::from(extent.length)
                    <= crate::content::codec::MAX as i64,
            "invalid replica mapping"
        );
        position = end.min(cursor.end);
        page.push(extent);
    }
    ensure!(
        page.len() == limit || position == cursor.end,
        "replica mapping is incomplete"
    );
    Ok(Some(page))
}

fn read_start(stream: Id, position: i64) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT offset_bytes FROM extents WHERE stream_id=$1 AND offset_bytes<=$2 ORDER BY offset_bytes DESC LIMIT 1")
        .bind(stream)
        .bind(position)
}

pub(super) fn claim_chunk_hash(storage: Uuid, hash: &[u8]) -> Statement<'_> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::uuid::text || ':' || encode($2,'hex'), 734922709851001))")
        .bind(storage)
        .bind(hash)
}

pub(super) fn try_claim_chunk_hash(storage: Uuid, hash: &[u8]) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1::uuid::text || ':' || encode($2,'hex'), 734922709851001))")
        .bind(storage).bind(hash)
}

pub(super) fn fail_chunk_allocations(input: &ChunkCreate, single: bool) -> Statement<'_> {
    sqlx::query("UPDATE chunks SET state='failed',unreferenced_at=now(),owner_stream=NULL WHERE storage_id=$1 AND hash=$2 AND raw_size=$3 AND state IN ('preparing','uploading') AND NOT EXISTS (SELECT 1 FROM streams s LEFT JOIN tasks t ON t.id=s.owner_task_id WHERE s.id=chunks.owner_stream AND s.state='writing' AND ((s.owner_task_id IS NULL AND NOT $4) OR (t.state='running' AND t.lease_token=s.owner_lease_token AND (t.lease_expires_at IS NULL OR t.lease_expires_at>clock_timestamp()))) AND ($4 OR EXISTS(SELECT 1 FROM stream_leases l WHERE l.stream_id=s.id AND l.lease_type='write' AND l.expires_at>clock_timestamp()))) AND NOT EXISTS(SELECT 1 FROM transfer_sources t WHERE t.candidate=chunks.owner_stream)")
        .bind(input.storage)
        .bind(input.hash.as_slice())
        .bind(input.raw_size)
        .bind(single)
}

pub(super) fn reusable_chunk(input: &ChunkCreate) -> Rows<'_, Chunk> {
    sqlx::query_as("SELECT * FROM chunks WHERE storage_id=$1 AND hash=$2 AND raw_size=$3 AND state='ready' ORDER BY id LIMIT 1")
        .bind(input.storage)
        .bind(input.hash.as_slice())
        .bind(input.raw_size)
}

pub(super) fn insert_chunk(input: &ChunkCreate) -> Rows<'_, Chunk> {
    sqlx::query_as("INSERT INTO chunks(storage_id,hash,raw_size,encryption_algorithm,block_key_id,owner_stream,state,unreferenced_at) VALUES($1,$2,$3,$4,$5,$6,'preparing',now()) ON CONFLICT (storage_id,hash,raw_size) WHERE state IN ('preparing','uploading','ready') DO NOTHING RETURNING *,current_encoding_id AS location_id")
        .bind(input.storage)
        .bind(input.hash.as_slice())
        .bind(input.raw_size)
        .bind(&input.algorithm)
        .bind(input.key)
        .bind(input.stream)
}

pub(super) fn can_stage_chunk_reuse(id: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT pack_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=$1) FROM chunks WHERE id=$1")
        .bind(id)
}

pub(super) fn mark_chunk_uploading(stream: Id, chunk: &Chunk) -> Statement<'_> {
    sqlx::query("UPDATE chunks SET standalone_size=$2,standalone_compressed=$3,nonce=$4,state='uploading' WHERE id=$1 AND state='preparing' AND owner_stream=$5 AND current_encoding_id=$6 AND current_encoding_id=$7 AND storage_id=$8")
        .bind(chunk.id)
        .bind(chunk.stored_size)
        .bind(chunk.compressed)
        .bind(&chunk.nonce)
        .bind(stream)
        .bind(chunk.encoding_id)
        .bind(chunk.location_id)
        .bind(chunk.storage_id)
}

pub(super) fn owned_chunk(stream: Id, chunk: &Chunk) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT owner_stream=$2 AND current_encoding_id=$3 AND current_encoding_id=$4 AND storage_id=$5 FROM chunks WHERE id=$1")
        .bind(chunk.id).bind(stream).bind(chunk.encoding_id).bind(chunk.location_id).bind(chunk.storage_id)
}

pub(super) fn register_encoding(chunk: &Chunk, owner: Option<Id>) -> Statement<'_> {
    sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,storage_id,standalone_size,standalone_compressed,nonce,nonce_id,state,created_at,unreferenced_at,encryption_algorithm,block_key_id,owner_stream,recovery_pending) VALUES($1,$2,$3,$4,$5,$6,$10,'uploading',$7,now(),$8,$9,$11,$11::uuid IS NOT NULL)").bind(chunk.encoding_id).bind(chunk.id).bind(chunk.storage_id).bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(chunk.created_at).bind(&chunk.algorithm).bind(chunk.block_key_id).bind(crate::content::nonce::id(chunk.nonce.as_deref())).bind(owner)
}

pub(super) fn lock_chunk(id: Id) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT state FROM chunks WHERE id=$1 FOR NO KEY UPDATE").bind(id)
}

pub(super) fn insert_chunk_extent(
    stream: Id,
    offset: i64,
    length: i32,
    chunk: Id,
) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES($1,$2,$3,$4)")
        .bind(stream)
        .bind(offset)
        .bind(length)
        .bind(chunk)
}

pub(super) fn reference_chunk(id: Id) -> Statement<'static> {
    sqlx::query(
        "UPDATE chunks SET unreferenced_at=NULL WHERE id=$1 AND unreferenced_at IS NOT NULL",
    )
    .bind(id)
}

pub(super) fn pin_chunk(id: Id, owner: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO cache_pins(chunk_id,pin_type,owner_id) SELECT chunk_id,CASE WHEN source_pack_id IS NULL THEN 'upload' ELSE 'pack' END,$2 FROM pending_uploads WHERE chunk_id=$1 ON CONFLICT DO NOTHING")
        .bind(id)
        .bind(owner)
}

pub(super) fn attach_pending_upload(chunk: Id, stream: Id, offset: i64) -> Statement<'static> {
    sqlx::query("UPDATE pending_uploads SET stream_id=$2,offset_bytes=$3 WHERE chunk_id=$1 AND owner_task IS NULL AND source_pack_id IS NULL AND EXISTS(SELECT 1 FROM streams WHERE id=$2 AND stream_type='object' AND state='writing')")
        .bind(chunk)
        .bind(stream)
        .bind(offset)
}

pub(super) fn extend_writing_stream(id: Id, size: i64) -> Statement<'static> {
    sqlx::query("UPDATE streams SET size=GREATEST(size,$2),touched_at=now() WHERE id=$1 AND state='writing'")
        .bind(id)
        .bind(size)
}

pub(super) fn complete_chunk(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET state='ready',unreferenced_at=NULL,owner_stream=NULL WHERE id=$1 AND state='uploading'")
        .bind(id)
}

pub(super) fn complete_encoding(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET sync_pending=CASE WHEN boot_id IS NOT NULL THEN FALSE ELSE sync_pending END,boot_id=NULL,state='ready',owner_stream=NULL,recovery_pending=FALSE,unreferenced_at=NULL,stored_at=clock_timestamp() WHERE encoding_id=$1 AND state='uploading'")
        .bind(id)
}

pub(super) fn resize_writing_stream(id: Id, size: i64) -> Statement<'static> {
    sqlx::query("UPDATE streams SET size=$2,touched_at=now() WHERE id=$1 AND state='writing'")
        .bind(id)
        .bind(size)
}

pub(super) fn lock_ready_chunks<'q>(ids: &'q [Id]) -> Scalar<'q, Id> {
    sqlx::query_scalar(
        "SELECT id FROM chunks WHERE id=ANY($1) AND state='ready' ORDER BY id FOR NO KEY UPDATE",
    )
    .bind(ids)
}

pub(super) fn copy_extents(
    destination: Id,
    source: Id,
    offset: i64,
    end: i64,
) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) SELECT $1,offset_bytes,length,chunk_id,source_offset FROM extents WHERE stream_id=$2 AND offset_bytes>=$3 AND offset_bytes<$4")
        .bind(destination)
        .bind(source)
        .bind(offset)
        .bind(end)
}

pub(super) fn reference_chunks<'q>(ids: &'q [Id]) -> Statement<'q> {
    sqlx::query(
        "UPDATE chunks SET unreferenced_at=NULL WHERE id=ANY($1) AND unreferenced_at IS NOT NULL",
    )
    .bind(ids)
}

pub(super) fn pin_chunks<'q>(ids: &'q [Id], owner: Id) -> Statement<'q> {
    sqlx::query("INSERT INTO cache_pins(chunk_id,pin_type,owner_id) SELECT chunk_id,CASE WHEN source_pack_id IS NULL THEN 'upload' ELSE 'pack' END,$2 FROM pending_uploads WHERE chunk_id=ANY($1) ON CONFLICT DO NOTHING")
        .bind(ids)
        .bind(owner)
}

pub(super) fn enabled_storages() -> Rows<'static, StorageRecord> {
    sqlx::query_as(
        "SELECT s.id,s.type AS store_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.access_key_encrypted AS access,s.secret_key_encrypted AS secret,s.identity_marker,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.enabled ORDER BY s.created_at,s.id",
    )
}

pub(super) fn enabled_storage(id: Uuid) -> Rows<'static, StorageRecord> {
    sqlx::query_as("SELECT s.id,s.type AS store_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.access_key_encrypted AS access,s.secret_key_encrypted AS secret,s.identity_marker,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=$1 AND s.enabled")
        .bind(id)
}

pub(super) fn storage_key(id: Uuid) -> Rows<'static, ManagedKeyRecord> {
    sqlx::query_as("SELECT k.id,k.name,k.key_algorithm AS algorithm FROM storages s JOIN managed_keys k ON k.id=s.block_key_id WHERE s.id=$1 AND k.purpose='block' AND k.revoked_at IS NULL")
        .bind(id)
}

pub(super) fn managed_key<'q>(name: &'q str) -> Rows<'q, ManagedKeyMaterial> {
    sqlx::query_as("SELECT id,name,purpose,key_algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE name=$1 AND secret_encrypted IS NOT NULL")
        .bind(name)
}

pub(super) fn ensure_runtime_setting<'q>(section: &'q str, value: &'q Value) -> Statement<'q> {
    sqlx::query("INSERT INTO runtime_settings(section,value) VALUES($1,$2) ON CONFLICT (section) DO NOTHING")
        .bind(section)
        .bind(value)
}

pub(super) fn runtime_settings() -> Rows<'static, (String, Value, i64)> {
    sqlx::query_as("SELECT section,value,revision FROM runtime_settings ORDER BY section")
}

pub(super) fn runtime_setting<'q>(section: &'q str) -> Scalar<'q, Value> {
    sqlx::query_scalar("SELECT value FROM runtime_settings WHERE section=$1").bind(section)
}

pub(super) fn managed_keys() -> Rows<'static, ManagedKeyMaterial> {
    sqlx::query_as(
        "SELECT id,name,purpose,key_algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE secret_encrypted IS NOT NULL ORDER BY id",
    )
}

pub(super) fn reserve_quota(
    stream: Id,
    size: i64,
    padding: bool,
    upload: Option<Uuid>,
    part: Option<i32>,
) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT quota_reserve($1,$2,$3,$4,$5)")
        .bind(stream)
        .bind(size)
        .bind(padding)
        .bind(upload)
        .bind(part)
}

pub(super) fn lock_completing_upload(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT bucket_id FROM uploads WHERE id=$1 AND state='completing' FOR SHARE")
        .bind(id)
}

pub(super) fn lock_stream(id: Id) -> Statement<'static> {
    sqlx::query("SELECT id FROM streams WHERE id=$1 FOR SHARE").bind(id)
}

pub(super) fn lock_quota(bucket: Uuid) -> Statement<'static> {
    sqlx::query("SELECT quota_lock($1)").bind(bucket)
}

pub(super) fn complete_quota(upload: Uuid, stream: Id, size: i64) -> Statement<'static> {
    sqlx::query(
        "UPDATE quota_reservations SET result_stream_id=$2 WHERE id=$1 AND logical_bytes>=$3",
    )
    .bind(upload)
    .bind(stream)
    .bind(size)
}

pub(super) fn write_authorization(id: Id) -> Scalar<'static, sqlx::types::Json<Permit>> {
    sqlx::query_scalar("SELECT write_authorization FROM streams WHERE id=$1").bind(id)
}

pub(super) fn lock_writing_stream(id: Id) -> Rows<'static, (Uuid, String, String, i64, String)> {
    sqlx::query_as(
        "SELECT bucket_id,object_key,state,size,payload_type FROM streams WHERE id=$1 FOR UPDATE",
    )
    .bind(id)
}

pub(super) fn lock_bucket_state(id: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT state FROM buckets WHERE id=$1 FOR SHARE").bind(id)
}

pub(super) fn lock_object_version<'q>(bucket: Uuid, key: &'q str) -> Rows<'q, (Option<Id>, Id)> {
    sqlx::query_as(
        "SELECT stream_id,write_token FROM objects WHERE bucket_id=$1 AND object_key=$2 FOR UPDATE",
    )
    .bind(bucket)
    .bind(key)
}

pub(super) fn web_upload_expectation<'q>(id: &'q Uuid) -> Rows<'q, (Option<Id>, i64)> {
    sqlx::query_as("SELECT expected_stream_id,expected_size FROM web_uploads WHERE upload_id=$1")
        .bind(id)
}

pub(super) fn stream_etag(id: Id) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT etag FROM streams WHERE id=$1").bind(id)
}

pub(super) fn incomplete_extents(stream: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=$1 AND (e.fragment_id IS NOT NULL OR c.state<>'ready'))")
        .bind(stream)
}

pub(super) fn extent_continuity(stream: Id) -> Rows<'static, (i64, bool)> {
    sqlx::query_as("SELECT COALESCE(max(offset_bytes+length),0),COALESCE(bool_and(offset_bytes=previous),true) FROM (SELECT offset_bytes,length,lag(offset_bytes+length,1,0::bigint) OVER (ORDER BY offset_bytes) AS previous FROM extents WHERE stream_id=$1) e")
        .bind(stream)
}

pub(super) fn task_fence(id: Uuid) -> Rows<'static, TaskFenceRow> {
    sqlx::query_as("SELECT t.state,t.lease_token,t.parent_task_id,m.deployment_mode,t.detail FROM tasks t CROSS JOIN mokyu_meta m WHERE t.id=$1")
        .bind(id)
}

pub(super) fn pause_owned_task(id: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='paused',lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2")
        .bind(id)
        .bind(token)
}

pub(super) fn storage_profile_revision(id: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT revision FROM storage_profiles WHERE storage_id=$1").bind(id)
}

pub(super) fn pause_stale_profile_task(id: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='paused',error='storage profile changed; resume to refresh',lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2")
        .bind(id)
        .bind(token)
}

pub(super) fn task_state(id: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT state FROM tasks WHERE id=$1").bind(id)
}

pub(super) fn task_record(id: Uuid) -> Rows<'static, TaskRecord> {
    sqlx::query_as(
        "SELECT task_type,state,bucket_id,cursor,detail,lease_token FROM tasks WHERE id=$1",
    )
    .bind(id)
}

pub(super) fn defer_pack_changes(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE pack_changes SET next_check_at=(SELECT now()+(left(sp.pack_reuse_interval,-1)::bigint*CASE right(sp.pack_reuse_interval,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END)*interval '1 second' FROM packs p JOIN storage_profiles sp ON sp.storage_id=p.storage_id WHERE p.id=pack_changes.pack_id) WHERE pack_id IN (SELECT c.pack_id FROM pack_inputs i JOIN chunks c ON c.id=i.chunk_id WHERE i.task_id=$1)").bind(id)
}

pub(super) fn defer_pack_maintenance(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE pack_maintenance SET next_check_at=(SELECT now()+(left(sp.pack_reuse_interval,-1)::bigint*CASE right(sp.pack_reuse_interval,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END)*interval '1 second' FROM streams s JOIN buckets b ON b.id=s.bucket_id JOIN storage_profiles sp ON sp.storage_id=b.storage_id WHERE s.id=pack_maintenance.stream_id) WHERE stream_id IN (SELECT e.stream_id FROM extents e JOIN pack_inputs i ON i.chunk_id=e.chunk_id WHERE i.task_id=$1)").bind(id)
}

pub(super) fn lock_stale_tasks() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM tasks WHERE state='running' AND lease_node_id IS NOT NULL AND (lease_expires_at IS NULL OR lease_expires_at<now()) FOR UPDATE",
    )
}

pub(super) fn abandon_task_streams<'q>(ids: &'q [Uuid]) -> Statement<'q> {
    sqlx::query("UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_lease_token=NULL,touched_at=now() WHERE state='writing' AND owner_task_id=ANY($1) AND NOT EXISTS(SELECT 1 FROM transfer_sources t WHERE t.candidate=streams.id)")
        .bind(ids)
}

pub(super) fn requeue_stale_tasks<'q>(ids: &'q [Uuid]) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET state='queued',lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=ANY($1) AND state='running'")
        .bind(ids)
}

pub(super) fn extend_task_lease(id: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET lease_expires_at=now()+interval '5 minutes' WHERE id=$1 AND state='running' AND lease_token=$2 AND lease_expires_at>now()")
        .bind(id)
        .bind(token)
}

pub(super) fn confirm_claimed_task(id: Uuid, token: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("UPDATE tasks SET lease_expires_at=now()+interval '5 minutes' WHERE id=$1 AND state='running' AND lease_token=$2 AND lease_expires_at>clock_timestamp() RETURNING true")
        .bind(id)
        .bind(token)
}

pub(super) fn release_task_lease(id: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND lease_token=$2").bind(id).bind(token)
}

pub(super) fn requeue_owned_task(id: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='queued',lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2")
        .bind(id)
        .bind(token)
}

pub(super) fn fail_owned_task<'q>(id: Uuid, error: &'q str, token: Uuid) -> Statement<'q> {
    sqlx::query("WITH failed AS (UPDATE tasks SET state='failed',error=$2,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$3 RETURNING parent_task_id) UPDATE tasks SET state='failed',error=$2,updated_at=now() WHERE id=(SELECT parent_task_id FROM failed) AND state IN ('queued','running')")
        .bind(id)
        .bind(error)
        .bind(token)
}

pub(super) fn update_parent_progress(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks p SET batch_completed=(SELECT count(*) FROM tasks c WHERE c.parent_task_id=p.id AND c.state='completed'),processed=(SELECT count(*) FROM tasks c WHERE c.parent_task_id=p.id AND c.state='completed'),state=CASE WHEN (SELECT count(*) FROM tasks c WHERE c.parent_task_id=p.id AND c.state='completed')>=coalesce(p.batch_total,0) THEN 'completed' ELSE p.state END,updated_at=now() WHERE p.id=$1 AND p.state IN ('queued','running')")
        .bind(id)
}

pub(super) fn claim_task<'q>(
    class: i32,
    active: &'q [Uuid],
    node: Option<Uuid>,
    token: Uuid,
    draining: bool,
    allowed: &'q [String],
    metadata_only: Option<bool>,
    cache_storage: Option<Uuid>,
) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("UPDATE tasks SET state='running',started_at=coalesce(started_at,now()),lease_node_id=$3,lease_token=$4,lease_expires_at=CASE WHEN $3::uuid IS NOT NULL THEN now()+interval '5 minutes' END,updated_at=now() WHERE id=(SELECT t.id FROM tasks t CROSS JOIN mokyu_meta m WHERE (t.state='queued' OR (t.state='failed' AND t.detail->>'operation' IN ('cdc','deduplicate','cache') AND t.updated_at<now()-interval '30 seconds') OR ($3::uuid IS NOT NULL AND t.state='running' AND t.lease_expires_at<now())) AND NOT EXISTS(SELECT 1 FROM raw_sources r WHERE r.processing_task=t.id AND (r.owner_stream IS NOT NULL OR EXISTS(SELECT 1 FROM raw_ranges e JOIN streams s ON s.id=e.stream_id WHERE e.source_id=r.id AND s.state='writing'))) AND CASE WHEN t.task_type='storage_migration' AND t.detail->>'operation'='replication' AND t.detail->>'phase'<>'verify' AND NOT coalesce((t.detail->>'external')::boolean,false) THEN 0 WHEN t.task_type IN ('pack','unpack') THEN 1 WHEN t.task_type IN ('upload','cache_flush','storage_migration','rewrite') THEN 2 ELSE 0 END=$1 AND (NOT $5 OR (t.task_type IN ('upload','cache_flush') AND coalesce(t.detail->>'operation' NOT IN ('cdc','deduplicate'),true))) AND ((t.detail->>'operation' IS DISTINCT FROM 'deduplicate' AND t.detail->>'operation' IS DISTINCT FROM 'cdc') OR (NOT m.maintenance AND (NOT EXISTS(SELECT 1 FROM raw_processing w WHERE w.token=t.id) OR EXISTS(SELECT 1 FROM raw_processing w WHERE w.token=t.id AND w.next_attempt_at<=clock_timestamp())))) AND (t.detail->>'operation' IS DISTINCT FROM 'cdc' OR t.state<>'queued' OR NOT t.detail ? 'node_conversion' OR t.updated_at<clock_timestamp()-interval '1 second') AND (t.detail->>'operation' IS DISTINCT FROM 'cache' OR NOT EXISTS(SELECT 1 FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id JOIN storage_profiles p ON p.storage_id=r.storage_id WHERE r.id::text=t.detail->>'source_id' AND (u.next_attempt_at>clock_timestamp() OR u.token IS NOT NULL AND u.lease_expires_at>clock_timestamp() OR r.owner_stream IS NOT NULL OR EXISTS(SELECT 1 FROM raw_ranges e JOIN streams s ON s.id=e.stream_id WHERE e.source_id=r.id AND s.state='writing') OR NOT $5 AND NOT m.maintenance AND r.created_at+p.write_backlog_max_age*interval '1 second'>clock_timestamp() AND EXISTS(SELECT 1 FROM tasks c WHERE c.id=r.processing_task AND c.state<>'completed')))) AND ($8::uuid IS NULL OR (t.task_type='upload' AND t.detail->>'operation'='cache' AND t.detail->>'storage_id'=$8::uuid::text)) AND NOT(t.id=ANY($2)) AND (($3::uuid IS NULL AND (t.execution_scope<>'node' OR t.target_node_id IS NULL)) OR (t.execution_scope='node' AND t.target_node_id=$3) OR (t.execution_scope<>'node' AND EXISTS(SELECT 1 FROM nodes n WHERE n.id=$3 AND n.enabled AND n.role IN ('control','worker','node')))) AND (cardinality($6::text[])=0 OR (t.task_type=ANY($6) AND ($7 IS DISTINCT FROM false OR t.task_type<>'upload' OR t.detail->>'operation' IN ('cdc','deduplicate','cache')) AND (t.task_type='sweep' OR NOT EXISTS(SELECT 1 FROM tasks sweep WHERE sweep.task_type='sweep' AND sweep.detail->>'dry_run'='false' AND sweep.state IN ('queued','running'))) AND (t.task_type<>'integrity' OR t.detail->>'operation'='external_test' OR t.detail ? 'source_operation' OR (t.detail->>'mode' IN ('head','full') AND t.detail->>'phase' IN ('raw','chunks'))))) AND ($7::bool IS NULL OR $7=((t.task_type='storage_migration' AND t.detail->>'operation'='replication' AND t.detail->>'phase'<>'verify' AND NOT coalesce((t.detail->>'external')::boolean,false)) OR t.task_type IN ('usage','purge') OR (t.task_type='cleanup' AND t.detail->>'storage_id' IS NULL) OR coalesce(t.task_type='integrity' AND NOT t.detail ? 'source_operation' AND (t.detail->>'mode'='metadata' OR t.detail->>'phase'='metadata'),false))) AND t.detail->>'role' IS DISTINCT FROM 'parent' AND NOT (m.maintenance AND t.task_type IN ('pack','unpack','purge','gc','storage_migration','rewrite','usage')) AND NOT EXISTS(SELECT 1 FROM maintenance_controls c WHERE c.policy=CASE WHEN t.task_type='pack' THEN t.detail->>'policy' ELSE t.task_type END AND CASE WHEN c.policy='gc' THEN m.gc_paused ELSE c.paused END) AND NOT (t.task_type='pack' AND t.detail->>'policy' IN ('pack','repack') AND m.pack_creation_paused) ORDER BY t.updated_at,t.id LIMIT 1) AND (state='queued' OR (state='failed' AND detail->>'operation' IN ('cdc','deduplicate','cache') AND updated_at<now()-interval '30 seconds') OR ($3::uuid IS NOT NULL AND state='running' AND lease_expires_at<now())) RETURNING id")
        .bind(class)
        .bind(active)
        .bind(node)
        .bind(token)
        .bind(draining)
        .bind(allowed)
        .bind(metadata_only)
        .bind(cache_storage)
}

pub(super) fn lock_storage_permits(key: String) -> Statement<'static> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))").bind(key)
}

pub(super) fn expire_storage_permits<'q>(storage: Uuid, direction: &'q str) -> Statement<'q> {
    sqlx::query(
        "DELETE FROM storage_permits WHERE storage_id=$1 AND direction=$2 AND expires_at<=now()",
    )
    .bind(storage)
    .bind(direction)
}

pub(super) fn storage_permit_count<'q>(storage: Uuid, direction: &'q str) -> Scalar<'q, i64> {
    sqlx::query_scalar("SELECT count(*) FROM storage_permits WHERE storage_id=$1 AND direction=$2")
        .bind(storage)
        .bind(direction)
}

pub(super) fn insert_storage_permit<'q>(
    token: Uuid,
    storage: Uuid,
    direction: &'q str,
    holder: Uuid,
) -> Statement<'q> {
    sqlx::query("INSERT INTO storage_permits(token,storage_id,direction,holder_id,expires_at) VALUES($1,$2,$3,$4,now()+interval '1 minute')")
        .bind(token)
        .bind(storage)
        .bind(direction)
        .bind(holder)
}

pub(super) fn renew_storage_permit(token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_permits SET expires_at=now()+interval '1 minute' WHERE token=$1")
        .bind(token)
}

pub(super) fn release_storage_permit(token: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM storage_permits WHERE token=$1").bind(token)
}

pub(super) fn storage_profile(id: Uuid) -> Rows<'static, crate::storage::profile::StorageProfile> {
    sqlx::query_as("SELECT storage_id,revision,cdc_enabled,deduplication_enabled,write_processing,CASE WHEN (SELECT deployment_mode FROM mokyu_meta WHERE singleton)='single' THEN write_durability END AS write_durability,write_flush_interval,write_backlog_max_age,background_scheduling,public_cache_validation,cors_cache_enabled,public_cache_refresh_interval,public_cache_max_age,read_source_validation,source_validation_max_age,read_integrity,file_delivery,file_delivery_prefix,connect_timeout,request_timeout,max_retries,retry_timeout,priority_aging,min_storage_duration,unreferenced_grace,deleted_chunk_retention,upload_retention,multipart_idle_timeout,min_free_space,cache_enabled,cache_soft_limit,cache_entries,upload_cache,upload_cache_size,upload_cache_overflow,compression_enabled,compression_strategy,compression_level,compression_sample_level,compression_min_savings_percent,compression_min_savings_bytes,pack_enabled,pack_max_size,pack_upload_flush_timeout,pack_interval,pack_reuse_interval,pack_reclaim_interval,pack_repack_interval,pack_repack_cooldown,pack_reclaim_min_savings_bytes,pack_range_enabled,pack_range_interval,pack_range_window,pack_range_min_downloads,pack_range_min_savings_percent,pack_range_min_savings_bytes,pack_range_repack_after,pack_range_repack_window,pack_range_repack_retry_interval,pack_concurrency,integrity_concurrency,integrity_rps,integrity_bandwidth,integrity_batch_timeout,read_concurrency,upload_concurrency,control_concurrency,pack_byte_budget,integrity_byte_budget,maintenance_concurrency,maintenance_byte_budget,migration_concurrency,migration_byte_budget FROM storage_profiles WHERE storage_id=$1")
        .bind(id)
}

pub(super) fn insert_task<'q>(
    id: Uuid,
    task_type: &'q str,
    bucket: Option<Uuid>,
    detail: Value,
    audit: Option<Id>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO tasks(id,task_type,bucket_id,state,detail,created_by_label,source,execution_scope,target_node_id) VALUES($1,$2,$3,'queued',$4,coalesce((SELECT actor_label FROM audit_events WHERE id=$5),'Scheduler'),coalesce((SELECT source FROM audit_events WHERE id=$5),'system'),CASE WHEN $4->>'source_node' IS NULL THEN 'cluster' ELSE 'node' END,($4->>'source_node')::uuid)")
        .bind(id)
        .bind(task_type)
        .bind(bucket)
        .bind(detail)
        .bind(audit)
}

pub(super) fn notify_task(id: String) -> Statement<'static> {
    sqlx::query("SELECT pg_notify('mokyu_tasks',$1) FROM mokyu_meta WHERE singleton AND deployment_mode='cluster'").bind(id)
}

pub(super) fn notify_work(id: Uuid) -> Statement<'static> {
    notify_task(format!("task:{id}"))
}

pub(super) fn finish_stream<'q>(
    id: Id,
    size: i64,
    etag: &'q str,
    checksums: Value,
) -> Statement<'q> {
    sqlx::query("UPDATE streams SET size=$2,etag=$3,checksums=$4,touched_at=now() WHERE id=$1 AND state='writing'")
        .bind(id)
        .bind(size)
        .bind(etag)
        .bind(checksums)
}

pub(super) fn publish_object<'q>(bucket: Uuid, key: &'q str, stream: Id) -> Statement<'q> {
    sqlx::query("UPDATE objects SET stream_id=$3 WHERE bucket_id=$1 AND object_key=$2")
        .bind(bucket)
        .bind(key)
        .bind(stream)
}

pub(super) fn publish_stream(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams s SET state='ready',touched_at=now(),http_compression=CASE WHEN stream_type='object' THEN (SELECT CASE WHEN COALESCE(sum(e.length),0)=s.size THEN jsonb_build_object('estimated_size',COALESCE(sum(CASE WHEN e.source_offset=0 AND e.length=c.raw_size AND c.pack_id IS NULL AND COALESCE(l.standalone_size,c.standalone_size) IS NOT NULL THEN COALESCE(l.standalone_size,c.standalone_size) ELSE e.length END),0)) ELSE '{}'::jsonb END FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE e.stream_id=s.id) ELSE '{}'::jsonb END WHERE id=$1").bind(id)
}

pub(super) fn learn_compression(
    observation: crate::model::CompressionObservation,
    node: Option<Uuid>,
) -> Statement<'static> {
    sqlx::query("WITH allowed AS MATERIALIZED (SELECT 1 FROM mokyu_meta WHERE singleton AND NOT maintenance AND deployment_mode=CASE WHEN $5::uuid IS NULL THEN 'single' ELSE 'cluster' END FOR SHARE), active_node AS MATERIALIZED (SELECT 1 FROM nodes WHERE id=$5 AND enabled AND role IN ('data','node') FOR SHARE) UPDATE streams SET http_compression=jsonb_set(CASE WHEN COALESCE((http_compression->>'version')::bigint,0)<$2 THEN jsonb_strip_nulls(jsonb_build_object('version',$2::bigint,'estimated_size',CASE WHEN jsonb_typeof(http_compression->'estimated_size')='number' AND (http_compression->>'estimated_size') ~ '^[0-9]+$' THEN CASE WHEN (http_compression->>'estimated_size')::numeric<=9223372036854775807 THEN http_compression->'estimated_size' END END)) ELSE http_compression END,ARRAY[$3::text],to_jsonb($4::bigint)) WHERE id=$1 AND state='ready' AND COALESCE((http_compression->>'version')::bigint,0)<=$2 AND (COALESCE((http_compression->>'version')::bigint,0)<$2 OR NOT http_compression ? $3) AND EXISTS(SELECT 1 FROM allowed) AND ($5::uuid IS NULL OR EXISTS(SELECT 1 FROM active_node))")
        .bind(observation.stream).bind(i64::from(observation.version)).bind(observation.encoding).bind(observation.size).bind(node)
}

pub(super) fn touch_credentials(accesses: &[String]) -> Statement<'_> {
    sqlx::query("UPDATE s3_credentials SET last_used_at=now() WHERE access_key_id=ANY($1) AND (last_used_at IS NULL OR last_used_at<now()-interval '5 minutes')").bind(accesses)
}

pub(super) fn retire_stream(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1").bind(id)
}

pub(super) fn complete_upload<'q>(
    id: &'q Uuid,
    hash: &'q str,
    result: &'q Value,
    stream: Id,
    token: Uuid,
) -> Statement<'q> {
    sqlx::query("UPDATE uploads SET state='completed',result=$3,output_stream=$4,touched_at=now() WHERE id=$1 AND state='completing' AND manifest_hash=$2 AND completion_token=$5 AND completion_expires_at>clock_timestamp()")
        .bind(id)
        .bind(hash)
        .bind(result)
        .bind(stream)
        .bind(token)
}

pub(super) fn retire_upload_parts<'q>(id: &'q Uuid) -> Statement<'q> {
    sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id IN (SELECT stream_id FROM parts WHERE upload_id=$1)")
        .bind(id)
}

pub(super) fn delete_upload_parts<'q>(id: &'q Uuid) -> Statement<'q> {
    sqlx::query("DELETE FROM parts WHERE upload_id=$1").bind(id)
}

pub(super) fn task_control(id: Uuid) -> Rows<'static, (bool, String, Value)> {
    sqlx::query_as(
        "SELECT coalesce(detail->>'role'='parent',false),task_type,detail FROM tasks WHERE id=$1",
    )
    .bind(id)
}

pub(super) fn change_task_state(id: Uuid, parent: bool, resume: bool) -> Statement<'static> {
    let query = if resume {
        if parent {
            "UPDATE tasks SET state='queued',error=NULL,updated_at=now() WHERE (id=$1 OR parent_task_id=$1) AND state IN ('paused','failed') AND coalesce(detail->>'test_stopped','false')<>'true'"
        } else {
            "UPDATE tasks SET state='queued',error=NULL,updated_at=now() WHERE id=$1 AND state IN ('paused','failed') AND coalesce(detail->>'test_stopped','false')<>'true'"
        }
    } else if parent {
        "UPDATE tasks SET state='paused',updated_at=now() WHERE (id=$1 OR parent_task_id=$1) AND state IN ('queued','running')"
    } else {
        "UPDATE tasks SET state='paused',updated_at=now() WHERE id=$1 AND state IN ('queued','running')"
    };
    sqlx::query(query).bind(id)
}

pub(super) fn refresh_task_profiles(id: Uuid, parent: bool) -> Statement<'static> {
    sqlx::query("UPDATE tasks t SET detail=jsonb_set(t.detail,'{profile_revision}',to_jsonb(p.revision)) FROM storage_profiles p WHERE (t.id=$1 OR ($2 AND t.parent_task_id=$1)) AND t.state IN ('paused','failed') AND t.detail ? 'profile_revision' AND p.storage_id=NULLIF(t.detail->>'storage_id','')::uuid")
        .bind(id)
        .bind(parent)
}

pub(super) fn set_stream_public(id: Id, public: bool) -> Statement<'static> {
    sqlx::query("UPDATE streams SET public_read=$2 WHERE id=$1")
        .bind(id)
        .bind(public)
}

pub(super) fn remove_object<'q>(bucket: Uuid, key: &'q str, token: Id) -> Statement<'q> {
    sqlx::query("INSERT INTO objects(bucket_id,object_key,write_token) VALUES($1,$2,$3) ON CONFLICT(bucket_id,object_key) DO UPDATE SET stream_id=NULL,write_token=excluded.write_token")
        .bind(bucket)
        .bind(key)
        .bind(token)
}

pub(super) fn lock_object_stream<'q>(bucket: Uuid, key: &'q str) -> Scalar<'q, Option<Id>> {
    sqlx::query_scalar(
        "SELECT stream_id FROM objects WHERE bucket_id=$1 AND object_key=$2 FOR UPDATE",
    )
    .bind(bucket)
    .bind(key)
}

pub(super) fn ensure_object(bucket: Uuid, key: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO objects(bucket_id,object_key,write_token) VALUES($1,$2,$3) ON CONFLICT(bucket_id,object_key) DO NOTHING")
        .bind(bucket).bind(key).bind(Id::default())
}

pub(super) fn extend_read_lease(id: Uuid, ttl: i64) -> Statement<'static> {
    sqlx::query("UPDATE stream_leases SET expires_at=now()+$2*interval '1 second' WHERE id=$1 AND lease_type='read' AND expires_at>now()")
        .bind(id)
        .bind(ttl)
}

pub(super) fn delete_read_lease(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM stream_leases WHERE id=$1 AND lease_type='read'").bind(id)
}

pub(super) fn extend_write_lease(stream: Id, ttl: i64) -> Statement<'static> {
    sqlx::query("UPDATE stream_leases SET expires_at=now()+$2*interval '1 second' WHERE stream_id=$1 AND lease_type='write' AND expires_at>now() AND EXISTS(SELECT 1 FROM streams WHERE id=$1 AND state='writing')")
        .bind(stream)
        .bind(ttl)
}

pub(super) fn share_object_stream<'q>(bucket: Uuid, key: &'q str) -> Scalar<'q, Option<Id>> {
    sqlx::query_scalar(
        "SELECT stream_id FROM objects WHERE bucket_id=$1 AND object_key=$2 FOR SHARE",
    )
    .bind(bucket)
    .bind(key)
}

pub(super) fn current_stream<'q>(
    bucket: Uuid,
    key: &'q str,
    lock: bool,
) -> Rows<'q, crate::app::StoredStream> {
    sqlx::query_as(if lock {
        "SELECT s.* FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=$1 AND o.object_key=$2 AND s.state='ready' FOR SHARE"
    } else {
        "SELECT s.* FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=$1 AND o.object_key=$2 AND s.state='ready'"
    }).bind(bucket).bind(key)
}

pub(super) async fn read_snapshot(
    pool: &sqlx::PgPool,
    route: &crate::model::ReadRoute,
    access: Option<&str>,
) -> Result<Option<crate::model::ReadSnapshot>> {
    use crate::model::ReadRoute;
    use sqlx::{FromRow, Row};
    let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "SELECT b.*,s.id AS stream_id,s.bucket_id AS stream_bucket_id,s.object_key AS stream_object_key,s.stream_type AS stream_stream_type,s.payload_type AS stream_payload_type,s.payload_revision AS stream_payload_revision,s.state AS stream_state,s.size AS stream_size,s.etag AS stream_etag,s.metadata AS stream_metadata,s.public_read AS stream_public_read,s.checksums AS stream_checksums,s.http_compression AS stream_http_compression,s.created_at AS stream_created_at,s.touched_at AS stream_touched_at,r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,e.offset_bytes,e.length,e.source_offset,c.secret_encrypted AS read_secret,EXISTS(SELECT 1 FROM s3_credential_grants g WHERE g.access_key=c.access_key_id AND g.bucket_id=b.id AND c.project_id=b.project_id AND 'object.read'=ANY(g.actions)) AS read_allowed FROM buckets b LEFT JOIN objects o ON o.bucket_id=b.id AND o.object_key=",
    );
    match route {
        ReadRoute::Bucket {
            key,
            alias: Some((alias, alias_key)),
            ..
        } => {
            query
                .push("CASE WHEN b.name=")
                .push_bind(alias)
                .push(" THEN ")
                .push_bind(alias_key)
                .push(" ELSE ")
                .push_bind(key)
                .push(" END");
        }
        ReadRoute::Bucket { key, .. } | ReadRoute::Host { key, .. } => {
            query.push_bind(key);
        }
    }
    query.push(" LEFT JOIN streams s ON s.id=o.stream_id AND s.state='ready' LEFT JOIN raw_ranges e ON e.stream_id=s.id AND e.offset_bytes=0 AND e.length=s.size AND NOT EXISTS(SELECT 1 FROM raw_ranges other WHERE other.stream_id=s.id AND other.offset_bytes<>0) LEFT JOIN raw_sources r ON r.id=e.source_id LEFT JOIN s3_credentials c ON c.access_key_id=")
        .push_bind(access).push(" AND c.enabled AND (c.expires_at IS NULL OR c.expires_at>now()) WHERE b.id=(");
    match route {
        ReadRoute::Host { host, .. } => {
            query
                .push("SELECT bucket_id FROM bucket_domains WHERE host=")
                .push_bind(host);
        }
        ReadRoute::Bucket {
            name, alias: None, ..
        } => {
            query
                .push("SELECT id FROM buckets WHERE name=")
                .push_bind(name);
        }
        ReadRoute::Bucket {
            name,
            alias: Some((alias, _)),
            ..
        } => {
            query
                .push("SELECT id FROM buckets WHERE name IN (")
                .push_bind(name)
                .push(",")
                .push_bind(alias)
                .push(") ORDER BY CASE WHEN name=")
                .push_bind(alias)
                .push(" THEN 0 ELSE 1 END LIMIT 1");
        }
    }
    query.push(")");
    let Some(row) = query.build().fetch_optional(pool).await? else {
        return Ok(None);
    };
    let stream = if row.try_get::<Option<Id>, _>("stream_id")?.is_some() {
        Some(crate::app::StoredStream {
            id: row.try_get("stream_id")?,
            bucket_id: row.try_get("stream_bucket_id")?,
            object_key: row.try_get("stream_object_key")?,
            stream_type: row.try_get("stream_stream_type")?,
            payload_type: row.try_get("stream_payload_type")?,
            payload_revision: row.try_get("stream_payload_revision")?,
            state: row.try_get("stream_state")?,
            size: row.try_get("stream_size")?,
            etag: row.try_get("stream_etag")?,
            metadata: row.try_get("stream_metadata")?,
            public_read: row.try_get("stream_public_read")?,
            checksums: row.try_get("stream_checksums")?,
            http_compression: row.try_get("stream_http_compression")?,
            created_at: row.try_get("stream_created_at")?,
            touched_at: row.try_get("stream_touched_at")?,
        })
    } else {
        None
    };
    let raw = if row.try_get::<Option<Id>, _>("raw_id")?.is_some() {
        Some(crate::model::raw::RawRange::from_row(&row)?)
    } else {
        None
    };
    Ok(Some(crate::model::ReadSnapshot {
        bucket: Bucket::from_row(&row)?,
        stream,
        raw,
        secret: row.try_get("read_secret")?,
        allowed: row.try_get("read_allowed")?,
    }))
}

pub(super) async fn cached_stream(
    pool: &PgPool,
    bucket: Uuid,
    key: &str,
) -> Result<Option<crate::model::CachedStream>> {
    use sqlx::{FromRow, Row};
    let rows = sqlx::query("SELECT s.id,s.bucket_id,s.object_key,s.stream_type,s.state,s.size,s.etag,s.metadata,s.public_read,s.checksums,s.http_compression,s.created_at,s.touched_at,s.payload_type,s.payload_revision,b.settings_revision AS bucket_revision,(b.state='active') AS bucket_active,e.offset_bytes AS cache_offset,e.length AS cache_length,e.source_offset AS cache_source,e.chunk_id AS cache_chunk,c.id AS cache_ready,c.raw_size AS cache_size,c.hash AS cache_hash FROM objects o JOIN streams s ON s.id=o.stream_id JOIN buckets b ON b.id=o.bucket_id LEFT JOIN LATERAL (SELECT * FROM extents WHERE stream_id=s.id ORDER BY offset_bytes LIMIT 65) e ON TRUE LEFT JOIN chunks c ON c.id=e.chunk_id AND c.state='ready' WHERE o.bucket_id=$1 AND o.object_key=$2 AND s.state='ready' ORDER BY e.offset_bytes LIMIT 65").bind(bucket).bind(key).fetch_all(pool).await?;
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    Ok(Some(crate::model::CachedStream {
        stream: crate::app::StoredStream::from_row(first)?,
        bucket_revision: first.try_get("bucket_revision")?,
        bucket_active: first.try_get("bucket_active")?,
        extents: rows
            .iter()
            .map(crate::model::CachedExtent::from_row)
            .collect::<Result<_, _>>()?,
    }))
}

pub(super) fn lock_owned_task<'q>(id: &'q Uuid, token: &'q Uuid) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()) FOR SHARE")
        .bind(id)
        .bind(token)
}

pub(super) fn lock_bucket_uploads(id: Uuid) -> Rows<'static, (String, bool)> {
    sqlx::query_as("SELECT state,uploads_paused FROM buckets WHERE id=$1 FOR SHARE").bind(id)
}

pub(super) fn completing_upload<'q>(
    id: Option<Uuid>,
    bucket: Uuid,
    key: &'q str,
) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM uploads WHERE id=$1 AND bucket_id=$2 AND object_key=$3 AND state='completing')")
        .bind(id)
        .bind(bucket)
        .bind(key)
}

pub(super) fn insert_stream(input: &StreamCreate) -> Scalar<'_, Id> {
    sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,metadata,public_read,write_authorization,owner_task_id,owner_lease_token) VALUES($1,$2,$3,'writing',$4,$5,$6,$7,$8) RETURNING id")

        .bind(input.bucket)
        .bind(&input.key)
        .bind(&input.stream_type)
        .bind(&input.metadata)
        .bind(input.public)
        .bind(sqlx::types::Json(&input.authority))
        .bind(input.owner.map(|value| value.0))
        .bind(input.owner.map(|value| value.1))
}

pub(super) fn claim_object<'q>(bucket: Uuid, key: &'q str, token: Id) -> Statement<'q> {
    sqlx::query("INSERT INTO objects(bucket_id,object_key,write_token) VALUES($1,$2,$3) ON CONFLICT(bucket_id,object_key) DO UPDATE SET write_token=excluded.write_token")
        .bind(bucket)
        .bind(key)
        .bind(token)
}

pub(super) fn insert_fragment(tail: &crate::model::StreamTail) -> Statement<'_> {
    sqlx::query("INSERT INTO fragments(id,size,hash,owner_stream,staging_storage_id,staging_object_key,file_id,file_offset) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
        .bind(tail.fragment).bind(tail.size).bind(tail.hash.as_slice()).bind(tail.stream)
        .bind(tail.staging_storage).bind(tail.staging_key.as_deref()).bind(tail.file).bind(tail.file_offset)
}

pub(super) fn seal_fragment(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE fragments SET sealed=true WHERE id=$1").bind(id)
}

pub(super) fn insert_fragment_extent(
    stream: Id,
    offset: i64,
    length: i32,
    fragment: Id,
) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO extents(stream_id,offset_bytes,length,fragment_id) VALUES($1,$2,$3,$4)",
    )
    .bind(stream)
    .bind(offset)
    .bind(length)
    .bind(fragment)
}

pub(super) fn resize_stream(id: Id, size: i64) -> Statement<'static> {
    sqlx::query("UPDATE streams SET size=$2,touched_at=now() WHERE id=$1")
        .bind(id)
        .bind(size)
}

pub(super) fn create_read_lease(stream: Id, ttl: i64) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("INSERT INTO stream_leases(stream_id,lease_type,expires_at) VALUES($1,'read',now()+$2*interval '1 second') RETURNING id")

        .bind(stream)
        .bind(ttl)
}

pub(super) fn create_write_lease(stream: Id, ttl: i64) -> Statement<'static> {
    sqlx::query("INSERT INTO stream_leases(stream_id,lease_type,expires_at) VALUES($1,'write',now()+$2*interval '1 second')")

        .bind(stream)
        .bind(ttl)
}

pub(super) fn server_version() -> sqlx::query::QueryAs<'static, Postgres, (i32, String), PgArguments>
{
    sqlx::query_as(
        "SELECT current_setting('server_version_num')::integer, current_setting('server_version')",
    )
}

pub(super) fn synchronous_commit() -> Statement<'static> {
    sqlx::query("SET synchronous_commit = on")
}

pub(super) fn statement_timeout() -> Statement<'static> {
    sqlx::query("SET statement_timeout = '30s'")
}

pub(super) fn fsync() -> Scalar<'static, String> {
    sqlx::query_scalar("SHOW fsync")
}

pub(super) const SCHEMA: &str = include_str!("../../../../migrations/schema/postgres.sql");

pub(super) fn flush_time() -> Scalar<'static, DateTime<Utc>> {
    sqlx::query_scalar("SELECT clock_timestamp()")
}

pub(super) fn upgrade_tables() -> Scalar<'static, String> {
    sqlx::query_scalar(
        "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename NOT IN ('_sqlx_migrations','mokyu_meta','key_fingerprints','runtime_settings','maintenance_controls','access_state') ORDER BY tablename",
    )
}

pub(super) fn upgrade_has_data(table: &str) -> Scalar<'static, bool> {
    // Initialization rows and configuration are not stored objects or user-created records.
    let predicate = match table {
        "projects" => "id<>'00000000-0000-0000-0000-000000000001'",
        "quota_scopes" => {
            "scope<>'project' OR scope_id<>'00000000-0000-0000-0000-000000000001' OR used_bytes<>0 OR reserved_bytes<>0 OR inflight_bytes<>0 OR object_count<>0 OR bucket_count<>0 OR cached_bytes<>0"
        }
        "catalog_build" => "scanned<>0 OR cursor_bucket IS NOT NULL OR cursor_key IS NOT NULL",
        "usage_summary" => {
            "id<>'global' OR as_of IS NOT NULL OR data IS NOT NULL OR bucket_ids IS NOT NULL"
        }
        _ => "TRUE",
    };
    let table = format!("public.\"{}\"", table.replace('"', "\"\""));
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT EXISTS(SELECT 1 FROM {table} WHERE {predicate})"
    )))
}

pub(super) fn lock_upgrade_table(table: &str) -> Statement<'static> {
    let table = table.replace('"', "\"\"");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "LOCK TABLE public.\"{table}\" IN SHARE MODE"
    )))
}

pub(super) fn schema_empty() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT NOT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname NOT IN ('public','information_schema')) AND NOT EXISTS(SELECT 1 FROM pg_depend d JOIN pg_namespace n ON d.refclassid='pg_namespace'::regclass AND d.refobjid=n.oid WHERE n.nspname='public') AND NOT EXISTS(SELECT 1 FROM pg_extension WHERE extname<>'plpgsql') AND NOT EXISTS(SELECT 1 FROM pg_event_trigger)",
    )
}

pub(super) fn initialize_meta(version: i32) -> Statement<'static> {
    sqlx::query("INSERT INTO mokyu_meta(schema_version,deployment_id) VALUES($1,gen_random_uuid())")
        .bind(version)
}

#[cfg(test)]
pub(super) fn metadata_table() -> Scalar<'static, Option<String>> {
    sqlx::query_scalar("SELECT to_regclass('public.mokyu_meta')::text")
}

pub(super) fn schema_version() -> Scalar<'static, i32> {
    sqlx::query_scalar("SELECT schema_version FROM mokyu_meta WHERE singleton")
}

pub(super) fn claim_runtime(single: bool) -> Scalar<'static, bool> {
    sqlx::query_scalar(if single {
        "SELECT pg_try_advisory_lock(734922709851001)"
    } else {
        "SELECT pg_try_advisory_lock_shared(734922709851001)"
    })
}

pub(super) fn release_runtime() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT pg_advisory_unlock(734922709851001)")
}

#[cfg(test)]
pub(super) fn migration_identity<'q>(value: &'q str) -> Statement<'q> {
    sqlx::query("SELECT set_config('media_gateway.backend_identity',$1,false)").bind(value)
}

pub(super) fn deployment_fingerprint() -> Scalar<'static, Option<Vec<u8>>> {
    sqlx::query_scalar("SELECT deployment_key_fingerprint FROM mokyu_meta WHERE singleton")
}

pub(super) fn set_deployment_fingerprint<'q>(value: &'q [u8]) -> Statement<'q> {
    sqlx::query("UPDATE mokyu_meta SET deployment_key_fingerprint=$1 WHERE singleton").bind(value)
}

pub(super) fn register_key_fingerprint<'q>(
    id: &'q str,
    algorithm: &'q str,
    hash: &'q [u8],
) -> Statement<'q> {
    sqlx::query("INSERT INTO key_fingerprints(key_id,algorithm,fingerprint) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
        .bind(id)
        .bind(algorithm)
        .bind(hash)
}

pub(super) fn key_fingerprint<'q>(id: &'q str) -> Rows<'q, (String, Vec<u8>)> {
    sqlx::query_as("SELECT algorithm,fingerprint FROM key_fingerprints WHERE key_id=$1").bind(id)
}

pub(super) fn required_keys() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT DISTINCT block_key_id FROM (SELECT block_key_id FROM chunk_encodings WHERE encryption_algorithm<>'none' AND state<>'deleted' UNION SELECT block_key_id FROM packs WHERE encryption_algorithm<>'none' AND state<>'deleted' UNION SELECT block_key_id FROM chunks WHERE encryption_algorithm<>'none' AND EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=chunks.id)) keys WHERE block_key_id IS NOT NULL",
    )
}

#[async_trait::async_trait]
impl ChunkStore for Backend {
    async fn prepare_repair(
        &self,
        input: ChunkCreate,
        node: Option<crate::model::recovery::RepairNode>,
    ) -> Result<crate::model::recovery::RepairAllocation> {
        transactions::prepare_repair(&self.pool, input, node).await
    }
    async fn repair_metadata(&self, repair: &crate::model::recovery::Repair) -> Result<()> {
        transactions::repair_metadata(&self.pool, repair).await
    }
    async fn finish_repair(
        &self,
        repair: &crate::model::recovery::Repair,
        offset: i64,
    ) -> Result<Chunk> {
        transactions::finish_repair(&self.pool, repair, offset).await
    }

    async fn storage_for_stream(&self, stream: Id) -> Result<Option<Uuid>> {
        Ok(storage_for_stream(stream).fetch_one(&self.pool).await?)
    }

    async fn can_stage_reuse(&self, id: Id) -> Result<bool> {
        Ok(can_stage_chunk_reuse(id).fetch_one(&self.pool).await?)
    }

    async fn start_upload(&self, stream: Id, chunk: &Chunk) -> Result<()> {
        transactions::start_chunk_upload(&self.pool, stream, chunk, self.single).await
    }

    async fn allocate(&self, input: ChunkCreate) -> Result<(Chunk, bool)> {
        transactions::allocate_chunk(&self.pool, input, self.single).await
    }

    async fn reference(&self, stream: Id, offset: i64, chunk_id: Id, raw_size: i32) -> Result<()> {
        transactions::reference_chunk(&self.pool, stream, offset, chunk_id, raw_size, self.single)
            .await
    }

    async fn complete(&self, stream: Id, offset: i64, chunk: &Chunk) -> Result<()> {
        transactions::complete_chunk_write(&self.pool, stream, offset, chunk, self.single).await
    }

    async fn copy_references(
        &self,
        destination: Id,
        source: Id,
        offset: i64,
        end: i64,
        chunks: &[Id],
    ) -> Result<()> {
        transactions::copy_chunk_references(&self.pool, destination, source, offset, end, chunks)
            .await
    }

    async fn extents_for_copy(&self, stream: Id, from: i64) -> Result<Vec<Extent>> {
        Ok(extents_for_copy(stream, from).fetch_all(&self.pool).await?)
    }

    async fn ready_chunk(&self, id: Id) -> Result<Option<Chunk>> {
        Ok(ready_chunk(id).fetch_optional(&self.pool).await?)
    }

    async fn fragment(&self, id: Id) -> Result<FragmentSource> {
        Ok(fragment_source(id).fetch_one(&self.pool).await?)
    }

    async fn extents(&self, stream: Id, from: i64, until: i64) -> Result<Vec<Extent>> {
        Ok(extents(stream, from, until).fetch_all(&self.pool).await?)
    }

    async fn read_extents(&self, stream: Id, from: i64, until: i64) -> Result<Vec<ReadExtent>> {
        Ok(read_extents(stream, from, until)
            .fetch_all(&self.pool)
            .await?)
    }

    async fn read_start(&self, stream: Id, position: i64) -> Result<Option<i64>> {
        Ok(read_start(stream, position)
            .fetch_optional(&self.pool)
            .await?)
    }
}

#[async_trait::async_trait]
impl StorageRecordStore for Backend {
    async fn list(&self) -> Result<Vec<StorageView>> {
        Ok(sqlx::query_as(VIEW_ORDERED).fetch_all(&self.pool).await?)
    }
    async fn view(&self, id: Uuid) -> Result<StorageView> {
        Ok(sqlx::query_as(VIEW_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await?)
    }
    async fn named(&self, name: &str) -> Result<Option<StorageView>> {
        Ok(sqlx::query_as(VIEW_BY_NAME)
            .bind(name)
            .fetch_optional(&self.pool)
            .await?)
    }
    async fn id(&self, name: &str, parsed: Option<Uuid>) -> Result<Option<Uuid>> {
        Ok(
            sqlx::query_scalar("SELECT id FROM storages WHERE name=$1 OR id=$2::uuid")
                .bind(name)
                .bind(parsed)
                .fetch_optional(&self.pool)
                .await?,
        )
    }
    async fn record(&self, name: &str) -> Result<Option<StorageRow>> {
        Ok(sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,access_key_encrypted,secret_key_encrypted,identity_marker,block_key_id,enabled FROM storages WHERE name=$1").bind(name).fetch_optional(&self.pool).await?)
    }
    async fn creation(&self) -> Result<StorageCreation> {
        transactions::storage_creation(&self.pool).await
    }
    async fn edit(&self, name: &str) -> Result<StorageEdit> {
        transactions::storage_edit(&self.pool, name).await
    }
    async fn current_key(&self, name: &str, key: Option<&str>) -> Result<Uuid> {
        transactions::storage_current_key(&self.pool, name, key).await
    }

    async fn enabled(&self) -> Result<Vec<StorageRecord>> {
        Ok(enabled_storages().fetch_all(&self.pool).await?)
    }

    async fn enabled_by_id(&self, id: Uuid) -> Result<Option<StorageRecord>> {
        Ok(enabled_storage(id).fetch_optional(&self.pool).await?)
    }
}

#[async_trait::async_trait]
impl ManagedKeyStore for Backend {
    async fn replacement_preview(
        &self,
        principal: &Principal,
        source: Uuid,
        input: KeyReplacementInput,
    ) -> Result<KeyReplacementPreview> {
        transactions::key_replacement_preview(&self.pool, principal, source, input).await
    }
    async fn replacement(&self, principal: &Principal, id: Uuid) -> Result<ReplacementRow> {
        transactions::key_replacement(&self.pool, principal, id).await
    }
    async fn replace(
        &self,
        principal: &Principal,
        id: Uuid,
        input: Option<ManagedKeyCreate>,
    ) -> Result<KeyReplacementResult> {
        transactions::replace_keys(&self.pool, principal, id, input).await
    }

    async fn nonce_observations(&self) -> Result<Vec<crate::model::key_usage::NonceRow>> {
        Ok(sqlx::query_as("SELECT k.id,k.name,c.reserved_id,p.reserved_id,(SELECT count(*) FROM storages s WHERE s.block_key_id=k.id),EXISTS(SELECT 1 FROM chunk_encodings e WHERE e.block_key_id=k.id AND e.state='uploading') OR EXISTS(SELECT 1 FROM packs p WHERE p.block_key_id=k.id AND p.state='preparing') OR EXISTS(SELECT 1 FROM chunks c WHERE c.block_key_id=k.id AND c.state IN ('preparing','uploading')) OR EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.block_key_id=k.id) OR EXISTS(SELECT 1 FROM tasks t WHERE t.detail->>'target_key_id'=k.id::text AND t.state IN ('queued','running','paused','failed')),o.data,o.revision FROM managed_keys k LEFT JOIN nonce_counters c ON c.block_key_id=k.id AND c.type_tag=0 LEFT JOIN nonce_counters p ON p.block_key_id=k.id AND p.type_tag=1 LEFT JOIN nonce_observations o ON o.block_key_id=k.id WHERE k.purpose='block' AND k.revoked_at IS NULL ORDER BY k.id").fetch_all(&self.pool).await?)
    }
    async fn save_nonce_history(
        &self,
        key: Uuid,
        previous: Option<i64>,
        history: crate::model::key_usage::NonceHistory,
    ) -> Result<()> {
        sqlx::query("INSERT INTO nonce_observations(block_key_id,data,revision) VALUES($1,$2,1) ON CONFLICT(block_key_id) DO UPDATE SET data=EXCLUDED.data,revision=nonce_observations.revision+1 WHERE nonce_observations.revision=$3").bind(key).bind(sqlx::types::Json(history)).bind(previous.unwrap_or(0)).execute(&self.pool).await?;
        Ok(())
    }

    async fn reserve_nonce(&self, key: Uuid, type_tag: u8, units: u32) -> Result<(i64, i64)> {
        transactions::reserve_nonce(&self.pool, key, type_tag, units).await
    }

    async fn resolve_block(&self, name: &str) -> Result<Option<crate::model::ManagedKeyRecord>> {
        Ok(resolve_block_key(name)
            .fetch_optional(&self.pool)
            .await?
            .map(|(id, name, algorithm)| crate::model::ManagedKeyRecord {
                id,
                name,
                algorithm,
            }))
    }
    async fn active_block(&self, id: Uuid) -> Result<Option<(String, String)>> {
        Ok(active_block_key(id).fetch_optional(&self.pool).await?)
    }

    async fn for_storage(&self, storage_id: Uuid) -> Result<Option<ManagedKeyRecord>> {
        Ok(storage_key(storage_id).fetch_optional(&self.pool).await?)
    }

    async fn material(&self, name: &str) -> Result<Option<ManagedKeyMaterial>> {
        Ok(managed_key(name).fetch_optional(&self.pool).await?)
    }
    async fn list(&self, principal: &Principal) -> Result<Vec<ManagedKeyView>> {
        transactions::list_managed_keys(&self.pool, principal).await
    }
    async fn create(
        &self,
        principal: &Principal,
        input: ManagedKeyCreate,
    ) -> Result<ManagedKeyView> {
        transactions::create_managed_key(&self.pool, principal, input).await
    }
    async fn revoke(&self, principal: &Principal, id: Uuid) -> Result<()> {
        transactions::revoke_managed_key(&self.pool, principal, id).await
    }
}

#[async_trait::async_trait]
impl RuntimeSettingsStore for Backend {
    async fn revisions(&self) -> Result<Vec<(String, i64)>> {
        let revisions: Vec<(String, i64)> =
            sqlx::query_as("SELECT section,revision FROM runtime_settings")
                .fetch_all(&self.pool)
                .await?;
        Ok(revisions)
    }
    async fn inspect(&self) -> Result<Vec<(String, Value, i64, bool)>> {
        let runtime: Vec<(String, serde_json::Value, i64, bool)> = sqlx::query_as(
        "SELECT section,value,revision,updated_by_user_id IS NULL FROM runtime_settings ORDER BY section",
    )
            .fetch_all(&self.pool)
            .await?;
        Ok(runtime)
    }

    async fn row(&self, section: &str) -> Result<RuntimeSettingsRow> {
        Ok(runtime_settings_row(section).fetch_one(&self.pool).await?)
    }
    async fn sections(&self, principal: &Principal) -> Result<Vec<(String, Value, i64)>> {
        transactions::runtime_settings_sections(&self.pool, principal).await
    }
    async fn section(&self, principal: &Principal, section: &str) -> Result<RuntimeSettingsRow> {
        transactions::runtime_settings_section(&self.pool, principal, section).await
    }
    async fn save(
        &self,
        principal: &Principal,
        section: &str,
        revision: Option<i64>,
        value: Value,
    ) -> Result<RuntimeSettingsRow> {
        transactions::save_runtime_settings(&self.pool, principal, section, revision, value).await
    }
    async fn reset(
        &self,
        principal: &Principal,
        section: &str,
        value: Value,
    ) -> Result<RuntimeSettingsRow> {
        transactions::reset_runtime_settings(&self.pool, principal, section, value).await
    }
    async fn update_web(
        &self,
        principal: &Principal,
        input: ControlSettingsPatch,
    ) -> Result<(crate::config::WebSettings, i64)> {
        transactions::update_web_settings(&self.pool, principal, input).await
    }

    async fn ensure(&self, defaults: &[(&str, Value)]) -> Result<()> {
        for (section, value) in defaults {
            ensure_runtime_setting(section, value)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn load(&self) -> Result<Vec<(String, Value, i64)>> {
        Ok(runtime_settings().fetch_all(&self.pool).await?)
    }

    async fn value(&self, section: &str) -> Result<Value> {
        Ok(runtime_setting(section).fetch_one(&self.pool).await?)
    }
}

#[async_trait::async_trait]
impl StorageProfileStore for Backend {
    async fn update(
        &self,
        name: String,
        profile: crate::storage::profile::StorageProfile,
    ) -> Result<Uuid> {
        transactions::update_storage_profile(&self.pool, name, profile).await
    }

    async fn enabled_profiles(&self) -> Result<Vec<StorageProfile>> {
        Ok(sqlx::query_as("SELECT p.* FROM storage_profiles p JOIN storages s ON s.id=p.storage_id WHERE s.enabled ORDER BY s.id").fetch_all(&self.pool).await?)
    }
    async fn load(
        &self,
        storage_id: uuid::Uuid,
    ) -> Result<crate::storage::profile::StorageProfile> {
        Ok(storage_profile(storage_id).fetch_one(&self.pool).await?)
    }
}

fn task_value(mut value: Value, single: bool) -> Value {
    if let Some(task) = value.as_object_mut() {
        task.remove("lease_token");
        if single {
            for field in [
                "parent_task_id",
                "execution_scope",
                "target_node_id",
                "lease_node_id",
                "lease_expires_at",
                "batch_cursor",
                "batch_total",
                "batch_completed",
            ] {
                task.remove(field);
            }
        }
    }
    value
}

#[async_trait::async_trait]
impl TaskControlStore for Backend {
    async fn unpack_status(&self, pack: Id) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar("SELECT to_jsonb(t) FROM tasks t WHERE task_type='unpack' AND detail->>'pack_id'=$1 ORDER BY created_at DESC LIMIT 1").bind(pack.to_string()).fetch_optional(&self.pool).await?.map(|value| task_value(value, self.single)))
    }
    async fn rewrite_status(&self, storage: Uuid) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar("SELECT to_jsonb(t) FROM tasks t WHERE task_type='rewrite' AND parent_task_id IS NULL AND detail->>'storage_id'=$1 ORDER BY created_at DESC LIMIT 1").bind(storage.to_string()).fetch_optional(&self.pool).await?.map(|value| task_value(value, self.single)))
    }
    async fn rewrite_task(&self, storage: Uuid) -> Result<Option<Uuid>> {
        Ok(sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='rewrite' AND parent_task_id IS NULL AND detail->>'storage_id'=$1 ORDER BY created_at DESC LIMIT 1").bind(storage.to_string()).fetch_optional(&self.pool).await?)
    }

    async fn all(&self) -> Result<Vec<Value>> {
        let tasks: Vec<Value> = sqlx::query_scalar(
            "SELECT to_jsonb(t) FROM tasks t ORDER BY created_at DESC LIMIT 100",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(tasks
            .into_iter()
            .map(|value| task_value(value, self.single))
            .collect())
    }
    async fn raw(&self, task: Uuid) -> Result<Option<Value>> {
        Ok(
            sqlx::query_scalar("SELECT to_jsonb(t) FROM tasks t WHERE id=$1")
                .bind(task)
                .fetch_optional(&self.pool)
                .await?
                .map(|value| task_value(value, self.single)),
        )
    }

    async fn page(
        &self,
        principal: &Principal,
        filter: JobQuery,
        cursor: Option<(DateTime<Utc>, Uuid)>,
        limit: i64,
    ) -> Result<Vec<Value>> {
        transactions::task_page(&self.pool, principal, filter, cursor, limit).await
    }
    async fn view(&self, principal: &Principal, task: Uuid) -> Result<Value> {
        transactions::task_view(&self.pool, principal, task).await
    }

    async fn other_integrity(&self, task: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='integrity' AND NOT (detail ? 'source_operation') AND detail->>'operation' IS DISTINCT FROM 'external_test' AND state IN ('queued','running') AND id<>$1)").bind(task).fetch_one(&self.pool).await?)
    }

    async fn enqueue(&self, task_type: &str, bucket: Option<Uuid>, detail: Value) -> Result<Uuid> {
        transactions::enqueue_task(
            &mut *self.pool.acquire().await?,
            task_type,
            bucket,
            detail,
            false,
        )
        .await
    }
    async fn sweep_preview(&self, task: Uuid) -> Result<Option<Value>> {
        Ok(sweep_preview(task).fetch_optional(&self.pool).await?)
    }
    async fn pause(&self, task: Execution) -> Result<()> {
        pause_active_task(task).execute(&self.pool).await?;
        Ok(())
    }
    async fn running(&self, task: Execution) -> Result<bool> {
        Ok(task_running(task).fetch_one(&self.pool).await?)
    }
    async fn complete(&self, task: Execution) -> Result<()> {
        complete_running_task(task).execute(&self.pool).await?;
        Ok(())
    }
    async fn progress(
        &self,
        task: Execution,
        cursor: &str,
        processed: i64,
        detail: Value,
    ) -> Result<()> {
        task_progress(task, cursor, processed, detail)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn load(&self, id: uuid::Uuid) -> Result<Option<(bool, String, Value)>> {
        Ok(task_control(id).fetch_optional(&self.pool).await?)
    }

    async fn change_state(&self, id: uuid::Uuid, parent: bool, resume: bool) -> Result<bool> {
        transactions::change_task_state(&self.pool, id, parent, resume).await
    }
}

#[async_trait::async_trait]
impl TaskExecutionStore for Backend {
    async fn claim(&self, input: TaskClaim<'_>) -> Result<Option<Uuid>> {
        transactions::claim_task(&self.pool, input, None).await
    }

    async fn load(&self, id: Uuid) -> Result<TaskRecord> {
        Ok(task_record(id).fetch_one(&self.pool).await?)
    }

    async fn fence(&self, expected_mode: &str, id: uuid::Uuid, token: uuid::Uuid) -> Result<bool> {
        fence_task(&self.pool, expected_mode, id, token).await
    }

    async fn confirm(&self, id: uuid::Uuid, token: uuid::Uuid) -> Result<bool> {
        if self.single {
            Ok(task_running(Execution { id, token })
                .fetch_one(&self.pool)
                .await?)
        } else {
            confirm_task_lease(&self.pool, id, token).await
        }
    }

    async fn finish(&self, input: TaskFinish) -> Result<()> {
        transactions::finish_task(&self.pool, input).await
    }
}

async fn fence_task(
    pool: &PgPool,
    expected_mode: &str,
    id: uuid::Uuid,
    token: uuid::Uuid,
) -> Result<bool> {
    let row = task_fence(id).fetch_optional(pool).await?;
    let Some((state, lease, parent, mode, detail)) = row else {
        return Ok(false);
    };
    if mode != expected_mode {
        pause_owned_task(id, token).execute(pool).await?;
        return Ok(false);
    }
    if let (Some(storage), Some(revision)) = (
        detail["storage_id"]
            .as_str()
            .and_then(|value| value.parse::<uuid::Uuid>().ok()),
        detail["profile_revision"].as_i64(),
    ) {
        let current: Option<i64> = storage_profile_revision(storage)
            .fetch_optional(pool)
            .await?;
        if current != Some(revision) {
            pause_stale_profile_task(id, token).execute(pool).await?;
            return Ok(false);
        }
    }
    if state != "running" || lease != Some(token) {
        return Ok(false);
    }
    if let Some(parent) = parent {
        let parent_state: Option<String> = task_state(parent).fetch_optional(pool).await?;
        if !matches!(parent_state.as_deref(), Some("queued" | "running")) {
            pause_owned_task(id, token).execute(pool).await?;
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) async fn renew_task_lease(
    pool: &PgPool,
    id: uuid::Uuid,
    token: uuid::Uuid,
) -> Result<bool> {
    Ok(extend_task_lease(id, token)
        .execute(pool)
        .await?
        .rows_affected()
        == 1)
}

pub(super) async fn confirm_task_lease(
    pool: &PgPool,
    id: uuid::Uuid,
    token: uuid::Uuid,
) -> Result<bool> {
    Ok(confirm_claimed_task(id, token)
        .fetch_optional(pool)
        .await?
        .is_some_and(|value| value))
}

pub(super) async fn renew_read_lease(
    pool: &PgPool,
    id: uuid::Uuid,
    ttl_seconds: i64,
) -> Result<()> {
    let changed = extend_read_lease(id, ttl_seconds)
        .execute(pool)
        .await?
        .rows_affected();
    anyhow::ensure!(changed == 1, "read lease expired");
    Ok(())
}

pub(super) async fn release_read_lease(pool: &PgPool, id: uuid::Uuid) -> Result<()> {
    delete_read_lease(id).execute(pool).await?;
    Ok(())
}

pub(super) async fn renew_write_lease(pool: &PgPool, stream: Id, ttl_seconds: i64) -> Result<()> {
    let changed = extend_write_lease(stream, ttl_seconds)
        .execute(pool)
        .await?
        .rows_affected();
    anyhow::ensure!(changed == 1, "write lease expired");
    Ok(())
}

pub(super) async fn validate_identity(
    principal: &crate::authorization::Principal,
    connection: &mut sqlx::PgConnection,
    lock: bool,
) -> Result<bool> {
    match principal {
        crate::authorization::Principal::System | crate::authorization::Principal::Bootstrap => {
            Ok(true)
        }
        crate::authorization::Principal::Local => {
            let query = if lock {
                "SELECT deployment_mode='single' FROM mokyu_meta WHERE singleton FOR SHARE"
            } else {
                "SELECT deployment_mode='single' FROM mokyu_meta WHERE singleton"
            };
            if sqlx::query_scalar(query).fetch_one(connection).await? {
                Ok(true)
            } else {
                Err(s3_error!(AccessDenied).into())
            }
        }
        crate::authorization::Principal::Node { id, role } => {
            let query = if lock {
                "SELECT role FROM nodes WHERE id=$1 AND enabled FOR SHARE"
            } else {
                "SELECT role FROM nodes WHERE id=$1 AND enabled"
            };
            let current: Option<String> = sqlx::query_scalar(query)
                .bind(id)
                .fetch_optional(&mut *connection)
                .await?;
            Ok(current.as_deref() == Some(role.as_str()) && role == "control")
        }
        crate::authorization::Principal::User {
            id,
            session,
            revision,
        } => {
            let query = if lock {
                "SELECT u.role,u.authorization_revision FROM web_users u JOIN sessions s ON s.user_id=u.id WHERE u.id=$1 AND s.id=$2 AND u.enabled AND NOT u.must_change_password AND s.expires_at>now() AND s.auth_revision=u.auth_revision FOR SHARE OF u"
            } else {
                "SELECT u.role,u.authorization_revision FROM web_users u JOIN sessions s ON s.user_id=u.id WHERE u.id=$1 AND s.id=$2 AND u.enabled AND NOT u.must_change_password AND s.expires_at>now() AND s.auth_revision=u.auth_revision"
            };
            let row: Option<(String, i64)> = sqlx::query_as(query)
                .bind(id)
                .bind(session)
                .fetch_optional(connection)
                .await?;
            match row {
                Some((role, current)) if current == *revision => Ok(role == "admin"),
                _ => Err(s3_error!(AccessDenied).into()),
            }
        }
        crate::authorization::Principal::Service {
            access_key,
            revision,
        } => {
            let query = if lock {
                "SELECT authorization_revision FROM s3_credentials WHERE access_key_id=$1 AND enabled AND (expires_at IS NULL OR expires_at>now()) FOR SHARE"
            } else {
                "SELECT authorization_revision FROM s3_credentials WHERE access_key_id=$1 AND enabled AND (expires_at IS NULL OR expires_at>now())"
            };
            let current: Option<i64> = sqlx::query_scalar(query)
                .bind(access_key)
                .fetch_optional(connection)
                .await?;
            if current == Some(*revision) {
                Ok(false)
            } else {
                Err(s3_error!(AccessDenied).into())
            }
        }
        crate::authorization::Principal::Token {
            id,
            user,
            revision,
            user_revision,
        } => {
            let user_query = if lock {
                "SELECT role,authorization_revision,auth_revision FROM web_users WHERE id=$1 AND enabled AND NOT must_change_password FOR SHARE"
            } else {
                "SELECT role,authorization_revision,auth_revision FROM web_users WHERE id=$1 AND enabled AND NOT must_change_password"
            };
            let current: Option<(String, i64, i64)> = sqlx::query_as(user_query)
                .bind(user)
                .fetch_optional(&mut *connection)
                .await?;
            let (role, current, auth) = current.ok_or_else(|| s3_error!(AccessDenied))?;
            if current != *user_revision {
                return Err(s3_error!(AccessDenied).into());
            }
            let token_query = if lock {
                "SELECT system,authorization_revision FROM api_tokens WHERE id=$1 AND user_id=$2 AND auth_revision=$3 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at>now()) FOR SHARE"
            } else {
                "SELECT system,authorization_revision FROM api_tokens WHERE id=$1 AND user_id=$2 AND auth_revision=$3 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at>now())"
            };
            let token: Option<(bool, i64)> = sqlx::query_as(token_query)
                .bind(id)
                .bind(user)
                .bind(auth)
                .fetch_optional(connection)
                .await?;
            match token {
                Some((system, current)) if current == *revision => Ok(system && role == "admin"),
                _ => Err(s3_error!(AccessDenied).into()),
            }
        }
    }
}

pub(super) async fn bucket_actions(
    principal: &crate::authorization::Principal,
    connection: &mut sqlx::PgConnection,
    bucket: Uuid,
    admin: bool,
) -> Result<Vec<Action>> {
    if admin {
        return Ok(Action::ALL.to_vec());
    }
    let actions = match principal {
        crate::authorization::Principal::Token { id, .. } => {
            let row: Option<Vec<String>> = sqlx::query_scalar(
                "SELECT actions FROM token_bucket_access WHERE token_id=$1 AND bucket_id=$2",
            )
            .bind(id)
            .bind(bucket)
            .fetch_optional(connection)
            .await?;
            Action::ALL
                .into_iter()
                .filter(|a| {
                    row.as_ref()
                        .is_some_and(|row| row.iter().any(|s| s == a.name()))
                })
                .collect()
        }
        crate::authorization::Principal::User { id, .. } => {
            let row: Option<Vec<String>> = sqlx::query_scalar(
                "SELECT actions FROM user_bucket_access WHERE bucket_id=$1 AND user_id=$2",
            )
            .bind(bucket)
            .bind(id)
            .fetch_optional(connection)
            .await?;
            Action::ALL
                .into_iter()
                .filter(|a| {
                    row.as_ref()
                        .is_some_and(|row| row.iter().any(|s| s == a.name()))
                })
                .collect()
        }
        crate::authorization::Principal::Service { access_key, .. } => {
            let row: Option<Vec<String>> = sqlx::query_scalar("SELECT g.actions FROM s3_credential_grants g JOIN buckets b ON b.id=g.bucket_id JOIN s3_credentials c ON c.access_key_id=g.access_key AND c.project_id=b.project_id WHERE g.access_key=$1 AND g.bucket_id=$2")
                .bind(access_key).bind(bucket).fetch_optional(connection).await?;
            Action::ALL
                .into_iter()
                .filter(|a| {
                    row.as_ref()
                        .is_some_and(|row| row.iter().any(|s| s == a.name()))
                })
                .collect()
        }
        crate::authorization::Principal::System
        | crate::authorization::Principal::Bootstrap
        | crate::authorization::Principal::Local
        | crate::authorization::Principal::Node { .. } => Action::ALL.to_vec(),
    };
    Ok(actions)
}

pub(super) fn permission_bucket(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM buckets WHERE id=$1 FOR SHARE").bind(id)
}

#[async_trait::async_trait]
impl crate::model::AuthorizationStore for Backend {
    async fn admin(&self, principal: &Principal) -> Result<Box<dyn crate::model::AdminGuard>> {
        transactions::hold_admin(&self.pool, principal).await
    }

    async fn service(&self, access_key: &str) -> Result<crate::authorization::Principal> {
        let revision = sqlx::query_scalar(
            "SELECT authorization_revision FROM s3_credentials WHERE access_key_id=$1 AND enabled AND (expires_at IS NULL OR expires_at>now())",
        )
        .bind(access_key)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| s3_error!(AccessDenied))?;
        Ok(crate::authorization::Principal::Service {
            access_key: access_key.into(),
            revision,
        })
    }

    async fn actions(
        &self,
        principal: &crate::authorization::Principal,
        bucket: Uuid,
    ) -> Result<Vec<Action>> {
        let mut connection = self.pool.acquire().await?;
        let admin = validate_identity(principal, &mut connection, false).await?;
        bucket_actions(principal, &mut connection, bucket, admin).await
    }
}

#[async_trait::async_trait]
impl crate::model::AuditStore for Backend {
    async fn begin(
        &self,
        principal: &Principal,
        action: &str,
        request_id: Option<&str>,
    ) -> Result<Id> {
        let actor = principal.user_id();
        let label = if let Some(id) = actor {
            sqlx::query_scalar("SELECT username FROM web_users WHERE id=$1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?
                .unwrap_or_else(|| "Deleted account".to_string())
        } else {
            "Local CLI".to_string()
        };
        let source = if principal.token_id().is_some() {
            "token"
        } else if matches!(
            principal,
            Principal::System | Principal::Bootstrap | Principal::Local | Principal::Node { .. }
        ) {
            "cli"
        } else {
            "web"
        };
        Ok(sqlx::query_scalar("INSERT INTO audit_events(actor_id,actor_label,token_id,source,action,request_id) VALUES($1,$2,$3,$4,$5,$6) RETURNING id")
            .bind(actor).bind(label).bind(principal.token_id()).bind(source).bind(action).bind(request_id).fetch_one(&self.pool).await?)
    }

    async fn finish(&self, id: Id, status: u16, partial: bool) -> Result<()> {
        let outcome = if partial {
            "partial"
        } else if status >= 400 {
            "failed"
        } else {
            "succeeded"
        };
        sqlx::query("UPDATE audit_events SET outcome=CASE WHEN outcome='unknown' THEN $2 ELSE outcome END,finished_at=COALESCE(finished_at,now()),http_status=$3 WHERE id=$1")
            .bind(id).bind(outcome).bind(i32::from(status)).execute(&self.pool).await?;
        Ok(())
    }

    async fn events(
        &self,
        admin: bool,
        user: Option<Uuid>,
        before: Option<Id>,
        filter: crate::model::audit::Filter,
        limit: i64,
        export: bool,
    ) -> Result<Vec<crate::model::audit::Event>> {
        Ok(sqlx::query_as("SELECT e.id::text,e.created_at,e.finished_at,e.actor_id,e.actor_label,e.token_id,e.source,e.action,e.target,e.project_id,e.bucket_id,e.outcome,e.request_id,e.http_status,CASE WHEN $13 OR octet_length(e.detail::text)<=8192 THEN e.detail ELSE '{\"details_available\":true}'::jsonb END AS detail FROM audit_events e WHERE ($1 OR (actor_id=$2 AND ((e.bucket_id IS NULL AND (starts_with(e.action,'account.') OR starts_with(e.action,'session.') OR starts_with(e.action,'token.') OR (e.outcome='failed' AND e.target='' AND e.detail='{}'::jsonb))) OR EXISTS(SELECT 1 FROM user_bucket_access a WHERE a.user_id=$2 AND a.bucket_id=e.bucket_id AND 'bucket.list'=ANY(a.actions))))) AND ($3::uuid IS NULL OR e.id<$3) AND position(lower($4) in lower(e.actor_label))>0 AND starts_with(e.action,$5) AND ($6::text IS NULL OR e.source=$6) AND ($7::text IS NULL OR e.outcome=$7) AND ($8::uuid IS NULL OR e.project_id=$8) AND ($9::uuid IS NULL OR e.bucket_id=$9) AND ($10::timestamptz IS NULL OR e.created_at>=$10) AND ($11::timestamptz IS NULL OR e.created_at<=$11) ORDER BY e.id DESC LIMIT $12")
        .bind(admin).bind(user).bind(before).bind(filter.actor.unwrap_or_default()).bind(filter.action.unwrap_or_default()).bind(filter.source).bind(filter.outcome).bind(filter.project).bind(filter.bucket).bind(filter.since).bind(filter.until).bind(limit+1).bind(export).fetch_all(&self.pool).await?)
    }

    async fn event(
        &self,
        id: Id,
        admin: bool,
        user: Uuid,
    ) -> Result<Option<crate::model::audit::Event>> {
        Ok(sqlx::query_as("SELECT e.id::text,e.created_at,e.finished_at,e.actor_id,e.actor_label,e.token_id,e.source,e.action,e.target,e.project_id,e.bucket_id,e.outcome,e.request_id,e.http_status,e.detail FROM audit_events e WHERE e.id=$1 AND ($2 OR (e.actor_id=$3 AND ((e.bucket_id IS NULL AND (starts_with(e.action,'account.') OR starts_with(e.action,'session.') OR starts_with(e.action,'token.') OR (e.outcome='failed' AND e.target='' AND e.detail='{}'::jsonb))) OR EXISTS(SELECT 1 FROM user_bucket_access a WHERE a.user_id=$3 AND a.bucket_id=e.bucket_id AND 'bucket.list'=ANY(a.actions)))))")
        .bind(id).bind(admin).bind(user).fetch_optional(&self.pool).await?)
    }
}

pub(super) fn checkpoint_audit<'q>(
    id: Id,
    action: &'q str,
    target: &'q str,
    project: Option<Uuid>,
    bucket: Option<Uuid>,
    detail: Value,
) -> Statement<'q> {
    sqlx::query("UPDATE audit_events SET action=$2,target=$3,project_id=$4,bucket_id=$5,detail=$6,outcome='succeeded',finished_at=now() WHERE id=$1")
        .bind(id).bind(action).bind(target).bind(project).bind(bucket).bind(detail)
}

pub(super) fn partial_audit(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE audit_events SET outcome='partial' WHERE id=$1").bind(id)
}

pub(super) fn lock_audit_user(id: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM web_users WHERE id=$1 FOR UPDATE").bind(id)
}

pub(super) fn security_audit<'q>(
    user: Uuid,
    action: &'q str,
    success: bool,
    request: &'q str,
) -> Statement<'q> {
    sqlx::query("INSERT INTO audit_events(actor_id,actor_label,source,action,target,outcome,request_id,finished_at,detail) SELECT id,username,'web',$2,id::text,$3,$4,now(),jsonb_build_object('identity_verified',$5::boolean) FROM web_users WHERE id=$1 AND ($5 OR NOT EXISTS(SELECT 1 FROM audit_events WHERE actor_id=$1 AND action=$2 AND outcome='failed' AND created_at>now()-interval '1 minute'))")
        .bind(user).bind(action).bind(if success {"succeeded"} else {"failed"}).bind(request).bind(success)
}

#[derive(sqlx::FromRow)]
pub(super) struct RefreshToken {
    pub(super) id: Uuid,
    pub(super) session_id: Uuid,
    pub(super) family_id: Uuid,
    pub(super) expires_at: DateTime<Utc>,
    pub(super) revoked_at: Option<DateTime<Utc>>,
    pub(super) replaced_by: Option<Uuid>,
}

pub(super) fn insert_refresh<'q>(session: Uuid, family: Uuid, hash: &'q [u8]) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("INSERT INTO session_refresh_tokens(session_id,family_id,token_hash,expires_at) VALUES($1,$2,$3,now()+$4*interval '1 day') RETURNING id").bind(session).bind(family).bind(hash).bind(30_f64)
}

pub(super) fn revoke_refreshes(session: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE session_refresh_tokens SET revoked_at=now() WHERE session_id=$1 AND revoked_at IS NULL").bind(session)
}

pub(super) fn recent_session(session: Uuid, user: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=$1 AND user_id=$2 AND reauthenticated_at>now()-interval '5 minutes')").bind(session).bind(user)
}

pub(super) fn authenticate_token<'q>(hash: &'q [u8]) -> Rows<'q, TokenIdentity> {
    sqlx::query_as("SELECT t.id,t.user_id,u.role,t.system,t.authorization_revision,u.authorization_revision FROM api_tokens t JOIN web_users u ON u.id=t.user_id WHERE t.token_hash=$1 AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at>now()) AND t.auth_revision=u.auth_revision AND u.enabled AND NOT u.must_change_password").bind(hash)
}

pub(super) fn touch_api_token(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE api_tokens SET last_used_at=now() WHERE id=$1 AND (last_used_at IS NULL OR last_used_at<now()-interval '5 minutes')").bind(id)
}

pub(super) fn authenticate_session<'q>(hash: &'q [u8]) -> Rows<'q, AuthSession> {
    sqlx::query_as("SELECT u.id AS user_id,s.id AS session_id,u.role,u.must_change_password,u.authorization_revision,s.csrf_hash,s.last_seen_at FROM sessions s JOIN web_users u ON u.id=s.user_id WHERE s.token_hash=$1 AND s.expires_at>now() AND u.enabled AND s.auth_revision=u.auth_revision").bind(hash)
}

pub(super) fn touch_session<'q>(hash: &'q [u8]) -> Statement<'q> {
    sqlx::query("UPDATE sessions SET last_seen_at=now() WHERE token_hash=$1 AND last_seen_at<now()-interval '5 minutes'").bind(hash)
}

pub(super) fn login_user<'q>(username: &'q str) -> Rows<'q, VerifiedAccount> {
    sqlx::query_as(
        "SELECT id,password_hash,auth_revision FROM web_users WHERE username=$1 AND enabled",
    )
    .bind(username)
}

pub(super) fn lock_login_user(user: Uuid) -> Rows<'static, (String, bool, i64)> {
    sqlx::query_as(
        "SELECT password_hash,enabled,auth_revision FROM web_users WHERE id=$1 FOR UPDATE",
    )
    .bind(user)
}

pub(super) fn insert_session<'q>(input: &'q SessionLogin<'q>) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("INSERT INTO sessions(token_hash,user_id,csrf_hash,expires_at,auth_revision,user_agent) VALUES($1,$2,$3,now()+$4*interval '1 second',$5,$6) RETURNING id").bind(input.access_hash).bind(input.verified.0).bind(input.csrf_hash).bind(input.seconds as f64).bind(input.verified.2).bind(input.user_agent)
}

pub(super) fn delete_access_session<'q>(hash: &'q [u8]) -> Statement<'q> {
    sqlx::query("DELETE FROM sessions WHERE token_hash=$1").bind(hash)
}

pub(super) fn refresh_user<'q>(hash: &'q [u8]) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT s.user_id FROM session_refresh_tokens r JOIN sessions s ON s.id=r.session_id WHERE r.token_hash=$1").bind(hash)
}

pub(super) fn lock_refresh_user(user: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT auth_revision FROM web_users WHERE id=$1 AND enabled FOR UPDATE")
        .bind(user)
}

pub(super) fn lock_refresh<'q>(hash: &'q [u8]) -> Rows<'q, RefreshToken> {
    sqlx::query_as("SELECT id,session_id,family_id,expires_at,revoked_at,replaced_by FROM session_refresh_tokens WHERE token_hash=$1 FOR UPDATE").bind(hash)
}

pub(super) fn lock_refresh_successor(id: Option<Uuid>) -> Rows<'static, RefreshToken> {
    sqlx::query_as("SELECT id,session_id,family_id,expires_at,revoked_at,replaced_by FROM session_refresh_tokens WHERE id=$1 AND revoked_at IS NULL AND expires_at>now() FOR UPDATE").bind(id)
}

pub(super) fn active_refresh_family(session: Uuid, family: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_refresh_tokens WHERE session_id=$1 AND family_id=$2 AND revoked_at IS NULL)").bind(session).bind(family)
}

pub(super) fn delete_refresh_session(session: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM sessions WHERE id=$1").bind(session)
}

pub(super) fn lock_refresh_session(
    session: Uuid,
    user: Uuid,
    revision: i64,
) -> Scalar<'static, DateTime<Utc>> {
    sqlx::query_scalar("SELECT expires_at FROM sessions WHERE id=$1 AND user_id=$2 AND auth_revision=$3 FOR UPDATE").bind(session).bind(user).bind(revision)
}

pub(super) fn revoke_refresh(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE session_refresh_tokens SET revoked_at=now() WHERE id=$1").bind(id)
}

pub(super) fn replace_refresh(id: Uuid, replacement: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE session_refresh_tokens SET replaced_by=$2 WHERE id=$1")
        .bind(id)
        .bind(replacement)
}

pub(super) fn refresh_session<'q>(
    session: Uuid,
    access: &'q [u8],
    csrf: &'q [u8],
    expires: DateTime<Utc>,
) -> Statement<'q> {
    sqlx::query("UPDATE sessions SET token_hash=$2,csrf_hash=$3,expires_at=$4,last_seen_at=now() WHERE id=$1").bind(session).bind(access).bind(csrf).bind(expires)
}

pub(super) fn touch_refresh(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE session_refresh_tokens SET last_used_at=now() WHERE id=$1").bind(id)
}

pub(super) fn session_times<'q>(hash: &'q [u8]) -> Rows<'q, SessionTimes> {
    sqlx::query_as("SELECT id,expires_at,reauthenticated_at FROM sessions WHERE token_hash=$1")
        .bind(hash)
}

pub(super) fn account_profile(user: Uuid) -> Rows<'static, Profile> {
    sqlx::query_as("SELECT u.id,u.username,u.role,u.must_change_password,m.project_management,u.display_name,u.locale,u.theme,u.avatar_email,u.avatar_enabled FROM web_users u CROSS JOIN mokyu_meta m WHERE u.id=$1 AND u.enabled").bind(user)
}

pub(super) fn account_preferences<'q>(user: Uuid, input: &'q Preferences) -> Statement<'q> {
    sqlx::query("UPDATE web_users SET display_name=$2,locale=$3,theme=$4,avatar_email=$5,avatar_enabled=$6 WHERE id=$1 AND enabled").bind(user).bind(input.display_name.trim()).bind(input.locale.as_deref()).bind(input.theme.as_deref()).bind(input.avatar_email.trim().to_lowercase()).bind(input.avatar_enabled)
}

pub(super) fn account_password(user: Uuid) -> Rows<'static, (String, i64)> {
    sqlx::query_as("SELECT password_hash,auth_revision FROM web_users WHERE id=$1 AND enabled")
        .bind(user)
}

pub(super) fn lock_session_user(user: Uuid) -> Rows<'static, (String, i64, bool)> {
    sqlx::query_as(
        "SELECT password_hash,auth_revision,enabled FROM web_users WHERE id=$1 FOR UPDATE",
    )
    .bind(user)
}

pub(super) fn active_session<'q>(user: Uuid, hash: &'q [u8], revision: i64) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE user_id=$1 AND token_hash=$2 AND expires_at>now() AND auth_revision=$3)").bind(user).bind(hash).bind(revision)
}

pub(super) fn change_password<'q>(user: Uuid, hash: &'q str) -> Statement<'q> {
    sqlx::query("UPDATE web_users SET password_hash=$2,auth_revision=auth_revision+1,must_change_password=false WHERE id=$1").bind(user).bind(hash)
}

pub(super) fn delete_other_sessions<'q>(user: Uuid, hash: &'q [u8]) -> Statement<'q> {
    sqlx::query("DELETE FROM sessions WHERE user_id=$1 AND token_hash<>$2")
        .bind(user)
        .bind(hash)
}

pub(super) fn session_revision<'q>(hash: &'q [u8], revision: i64) -> Statement<'q> {
    sqlx::query("UPDATE sessions SET auth_revision=$2,reauthenticated_at=now() WHERE token_hash=$1")
        .bind(hash)
        .bind(revision)
}

pub(super) fn session_id<'q>(hash: &'q [u8]) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT id FROM sessions WHERE token_hash=$1").bind(hash)
}

pub(super) fn reauthenticate_session<'q>(hash: &'q [u8]) -> Statement<'q> {
    sqlx::query("UPDATE sessions SET reauthenticated_at=now() WHERE token_hash=$1").bind(hash)
}

pub(super) fn account_sessions<'q>(user: Uuid, hash: &'q [u8]) -> Rows<'q, Session> {
    sqlx::query_as("SELECT id,created_at,last_seen_at,expires_at,user_agent,(token_hash=$2) AS current FROM sessions s WHERE user_id=$1 AND (expires_at>now() OR EXISTS(SELECT 1 FROM session_refresh_tokens r WHERE r.session_id=s.id AND r.revoked_at IS NULL AND r.expires_at>now())) ORDER BY created_at DESC,id DESC LIMIT 101").bind(user).bind(hash)
}

pub(super) fn delete_account_session(session: Uuid, user: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM sessions WHERE id=$1 AND user_id=$2")
        .bind(session)
        .bind(user)
}

pub(super) fn has_accounts() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM web_users)")
}

pub(super) fn delete_setup_tokens() -> Statement<'static> {
    sqlx::query("DELETE FROM setup_tokens")
}

pub(super) fn lock_setup_metadata() -> Statement<'static> {
    sqlx::query("SELECT singleton FROM mokyu_meta WHERE singleton FOR UPDATE")
}

pub(super) fn save_setup_token<'q>(hash: &'q [u8]) -> Statement<'q> {
    sqlx::query("INSERT INTO setup_tokens(singleton,token_hash,expires_at) VALUES(true,$1,now()+interval '1 hour') ON CONFLICT(singleton) DO UPDATE SET token_hash=excluded.token_hash,created_at=now(),expires_at=excluded.expires_at").bind(hash)
}

pub(super) fn lock_accounts() -> Statement<'static> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(super::USER_LOCK)
}

pub(super) fn insert_initial_admin<'q>(
    username: &'q str,
    password: &'q str,
    require_change: bool,
) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("INSERT INTO web_users(username,password_hash,role,must_change_password) VALUES($1,$2,'admin',$3) RETURNING id").bind(username).bind(password).bind(require_change)
}

pub(super) fn setup_required() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM web_users)")
}

pub(super) fn setup_hash() -> Scalar<'static, Vec<u8>> {
    sqlx::query_scalar("SELECT token_hash FROM setup_tokens WHERE singleton AND expires_at>now()")
}

pub(super) fn setup_allowed<'q>(hash: &'q [u8]) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM web_users) AND EXISTS(SELECT 1 FROM setup_tokens WHERE token_hash=$1 AND expires_at>clock_timestamp())").bind(hash)
}

pub(super) fn insert_setup_admin<'q>(username: &'q str, password: &'q str) -> Scalar<'q, Uuid> {
    sqlx::query_scalar(
        "INSERT INTO web_users(username,password_hash,role) VALUES($1,$2,'admin') RETURNING id",
    )
    .bind(username)
    .bind(password)
}

#[async_trait::async_trait]
impl AccountStore for Backend {
    async fn recent(&self, session: Uuid, user: Uuid) -> Result<bool> {
        Ok(recent_session(session, user).fetch_one(&self.pool).await?)
    }
    async fn authenticate_token(&self, hash: &[u8]) -> Result<Option<TokenIdentity>> {
        let row = authenticate_token(hash).fetch_optional(&self.pool).await?;
        if let Some(row) = &row {
            touch_api_token(row.0).execute(&self.pool).await?;
        }
        Ok(row)
    }
    async fn authenticate_session(&self, hash: &[u8]) -> Result<Option<AuthSession>> {
        let row = authenticate_session(hash)
            .fetch_optional(&self.pool)
            .await?;
        if row
            .as_ref()
            .is_some_and(|row| (Utc::now() - row.last_seen_at).num_seconds() >= 300)
        {
            touch_session(hash).execute(&self.pool).await?;
        }
        Ok(row)
    }
    async fn login_user(&self, username: &str) -> Result<Option<VerifiedAccount>> {
        Ok(login_user(username).fetch_optional(&self.pool).await?)
    }
    async fn password(&self, user: Uuid) -> Result<Option<(String, i64)>> {
        Ok(account_password(user).fetch_optional(&self.pool).await?)
    }
    async fn login(&self, input: SessionLogin<'_>) -> Result<()> {
        transactions::login_session(&self.pool, input).await
    }
    async fn failed_login(&self, user: Uuid, request_id: &str) -> Result<()> {
        transactions::failed_login(&self.pool, user, request_id).await
    }
    async fn refresh(&self, input: SessionRefresh<'_>) -> Result<RefreshedSession> {
        transactions::refresh_session(&self.pool, input).await
    }
    async fn session(&self, hash: &[u8]) -> Result<Option<SessionTimes>> {
        Ok(session_times(hash).fetch_optional(&self.pool).await?)
    }
    async fn logout(&self, user: Uuid, hash: &[u8]) -> Result<()> {
        transactions::logout_session(&self.pool, user, hash).await
    }
    async fn profile(&self, user: Uuid) -> Result<Option<Profile>> {
        Ok(account_profile(user).fetch_optional(&self.pool).await?)
    }
    async fn preferences(&self, user: Uuid, hash: &[u8], input: Preferences) -> Result<()> {
        transactions::account_preferences(&self.pool, user, hash, input).await
    }
    async fn change_password(
        &self,
        verified: &VerifiedAccount,
        current: &[u8],
        password: &str,
        refresh: &[u8],
    ) -> Result<()> {
        transactions::account_password(&self.pool, verified, current, password, refresh).await
    }
    async fn reauthenticate(
        &self,
        verified: &VerifiedAccount,
        current: &[u8],
        refresh: &[u8],
    ) -> Result<()> {
        transactions::reauthenticate_account(&self.pool, verified, current, refresh).await
    }
    async fn sessions(&self, user: Uuid, current: &[u8]) -> Result<Vec<Session>> {
        Ok(account_sessions(user, current)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn revoke_sessions(
        &self,
        user: Uuid,
        current: &[u8],
        session: Option<Uuid>,
    ) -> Result<()> {
        transactions::revoke_account_sessions(&self.pool, user, current, session).await
    }
    async fn initialize(&self) -> Result<()> {
        if has_accounts().fetch_one(&self.pool).await? {
            delete_setup_tokens().execute(&self.pool).await?;
        }
        Ok(())
    }
    async fn setup_token(&self, hash: &[u8]) -> Result<()> {
        transactions::account_setup_token(&self.pool, hash).await
    }
    async fn initialize_admin(
        &self,
        username: &str,
        password: &str,
        require_change: bool,
    ) -> Result<Uuid> {
        transactions::initialize_admin(&self.pool, username, password, require_change).await
    }
    async fn setup_required(&self) -> Result<bool> {
        Ok(setup_required().fetch_one(&self.pool).await?)
    }
    async fn setup_hash(&self) -> Result<Option<Vec<u8>>> {
        Ok(setup_hash().fetch_optional(&self.pool).await?)
    }
    async fn setup(
        &self,
        username: &str,
        password: &str,
        supplied: &[u8],
        request_id: &str,
    ) -> Result<()> {
        transactions::setup_account(&self.pool, username, password, supplied, request_id).await
    }
}

pub(super) fn find_user<'q>(name: &'q str) -> Rows<'q, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE username=$1").bind(name)
}

pub(super) fn enable_project_mode() -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET project_management=true")
}

pub(super) fn has_admin() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM web_users WHERE enabled AND role='admin')")
}

pub(super) fn insert_user<'q>(input: &'q UserCreate) -> Rows<'q, User> {
    sqlx::query_as("INSERT INTO web_users(username,password_hash,role,must_change_password) VALUES($1,$2,$3,$4) ON CONFLICT(username) DO NOTHING RETURNING *").bind(&input.username).bind(&input.password_hash).bind(&input.role).bind(input.must_change_password)
}

pub(super) fn admin_count() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM web_users WHERE enabled AND role='admin'")
}

pub(super) fn lock_project_mode() -> Statement<'static> {
    sqlx::query("SELECT singleton FROM mokyu_meta FOR UPDATE")
}

pub(super) fn lock_user(id: Uuid) -> Rows<'static, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE id=$1 FOR UPDATE").bind(id)
}

pub(super) fn delete_user_memberships(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM project_members WHERE user_id=$1").bind(id)
}

pub(super) fn update_user<'q>(id: Uuid, role: &'q str, enabled: bool) -> Rows<'q, User> {
    sqlx::query_as("UPDATE web_users SET role=$2,enabled=$3,auth_revision=auth_revision+1 WHERE id=$1 RETURNING *").bind(id).bind(role).bind(enabled)
}

pub(super) fn delete_user_sessions(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM sessions WHERE user_id=$1").bind(id)
}

pub(super) fn delete_user(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM web_users WHERE id=$1").bind(id)
}

pub(super) fn reset_user_password<'q>(
    id: Uuid,
    hash: &'q str,
    require_change: bool,
) -> Statement<'q> {
    sqlx::query("UPDATE web_users SET password_hash=$2,auth_revision=auth_revision+1,must_change_password=$3 WHERE id=$1").bind(id).bind(hash).bind(require_change)
}

pub(super) fn list_users<'q>(
    search: &'q str,
    after: &'q str,
    limit: i64,
    role: Option<&'q str>,
) -> Rows<'q, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE (position(lower($1) in lower(username))>0 OR position(lower($1) in lower(display_name))>0) AND username COLLATE \"C\">$2 AND ($4::text IS NULL OR role=$4) ORDER BY username COLLATE \"C\" LIMIT $3").bind(search).bind(after).bind(limit).bind(role)
}

pub(super) fn user(id: Uuid) -> Rows<'static, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE id=$1").bind(id)
}

pub(super) fn lock_member(user: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT role FROM web_users WHERE id=$1 FOR UPDATE").bind(user)
}

pub(super) fn project_exists(project: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM projects WHERE id=$1)").bind(project)
}

pub(super) fn lock_project_buckets<'q>(buckets: &'q [Uuid], project: Uuid) -> Scalar<'q, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM buckets WHERE id=ANY($1) AND project_id=$2 ORDER BY id FOR SHARE",
    )
    .bind(buckets)
    .bind(project)
}

pub(super) fn save_membership<'q>(
    user: Uuid,
    project: Uuid,
    role: &'q str,
    scope: &'q str,
) -> Statement<'q> {
    sqlx::query("INSERT INTO project_members(user_id,project_id,role,scope) VALUES($1,$2,$3,$4) ON CONFLICT(user_id,project_id) DO UPDATE SET role=excluded.role,scope=excluded.scope").bind(user).bind(project).bind(role).bind(scope)
}

pub(super) fn delete_member_grants(user: Uuid, project: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM member_bucket_grants WHERE user_id=$1 AND project_id=$2")
        .bind(user)
        .bind(project)
}

pub(super) fn insert_member_grant<'q>(
    user: Uuid,
    project: Uuid,
    grant: &'q BucketGrant,
) -> Statement<'q> {
    sqlx::query("INSERT INTO member_bucket_grants(user_id,project_id,bucket_id,actions) VALUES($1,$2,$3,$4)").bind(user).bind(project).bind(grant.bucket_id).bind(grant.actions.iter().map(|a| a.name()).collect::<Vec<_>>())
}

pub(super) fn delete_membership(user: Uuid, project: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM project_members WHERE user_id=$1 AND project_id=$2")
        .bind(user)
        .bind(project)
}

pub(super) fn project_members(id: Uuid) -> Rows<'static, Member> {
    sqlx::query_as("SELECT m.user_id,u.username,u.display_name,u.enabled,m.role,m.scope,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM member_bucket_grants g WHERE g.user_id=m.user_id AND g.project_id=m.project_id),'[]'::jsonb) AS grants,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM member_external_grants g WHERE g.user_id=m.user_id AND g.project_id=m.project_id),'[]'::jsonb) AS external_grants FROM project_members m JOIN web_users u ON u.id=m.user_id WHERE m.project_id=$1 ORDER BY u.username LIMIT 1000").bind(id)
}

pub(super) fn projects() -> Rows<'static, Project> {
    sqlx::query_as("SELECT * FROM projects ORDER BY builtin DESC,name LIMIT 1000")
}

pub(super) fn duplicate_project<'q>(name: &'q str, id: Option<Uuid>) -> Scalar<'q, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE name=$1 AND ($2::uuid IS NULL OR id<>$2))",
    )
    .bind(name)
    .bind(id)
}

pub(super) fn update_project<'q>(id: Uuid, input: &'q ProjectInput) -> Rows<'q, Project> {
    sqlx::query_as(
        "UPDATE projects SET name=$2,description=$3,allow_bucket_create=$4 WHERE id=$1 RETURNING *",
    )
    .bind(id)
    .bind(input.name.trim())
    .bind(&input.description)
    .bind(input.allow_bucket_create)
}

pub(super) fn insert_project<'q>(input: &'q ProjectInput) -> Rows<'q, Project> {
    sqlx::query_as(
        "INSERT INTO projects(name,description,allow_bucket_create) VALUES($1,$2,$3) RETURNING *",
    )
    .bind(input.name.trim())
    .bind(&input.description)
    .bind(input.allow_bucket_create)
}

pub(super) fn lock_project(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM projects WHERE id=$1 FOR UPDATE").bind(id)
}

pub(super) fn project_dependents(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM buckets WHERE project_id=$1) OR EXISTS(SELECT 1 FROM s3_credentials WHERE project_id=$1) OR EXISTS(SELECT 1 FROM project_members WHERE project_id=$1) OR EXISTS(SELECT 1 FROM external_buckets WHERE project_id=$1)").bind(id)
}

pub(super) fn delete_project(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM projects WHERE id=$1").bind(id)
}

pub(super) fn project_mode_dependents() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE NOT builtin) OR EXISTS(SELECT 1 FROM web_users WHERE role='member') OR EXISTS(SELECT 1 FROM project_members)",
    )
}

pub(super) fn set_project_mode(enabled: bool) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET project_management=$1").bind(enabled)
}

pub(super) fn visible_projects(
    user: Uuid,
    admin: bool,
    token: Option<Uuid>,
) -> Rows<'static, Project> {
    sqlx::query_as("SELECT p.* FROM projects p WHERE $2 OR CASE WHEN $3::uuid IS NOT NULL THEN EXISTS(SELECT 1 FROM token_bucket_access t JOIN buckets b ON b.id=t.bucket_id WHERE t.token_id=$3 AND b.project_id=p.id AND 'bucket.list'=ANY(t.actions)) OR EXISTS(SELECT 1 FROM token_external_access t JOIN external_buckets b ON b.id=t.bucket_id WHERE t.token_id=$3 AND b.project_id=p.id AND 'bucket.list'=ANY(t.actions)) ELSE EXISTS(SELECT 1 FROM project_members m WHERE m.project_id=p.id AND m.user_id=$1) END ORDER BY p.builtin DESC,p.name LIMIT 1000").bind(user).bind(admin).bind(token)
}

pub(super) fn project_mode() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT project_management FROM mokyu_meta")
}

#[async_trait::async_trait]
impl AdministrationStore for Backend {
    async fn all_users(&self) -> Result<Vec<User>> {
        Ok(
            sqlx::query_as("SELECT * FROM web_users ORDER BY username LIMIT 1000")
                .fetch_all(&self.pool)
                .await?,
        )
    }

    async fn find_user(&self, name: &str) -> Result<Option<User>> {
        Ok(find_user(name).fetch_optional(&self.pool).await?)
    }
    async fn user(&self, id: Uuid) -> Result<Option<User>> {
        Ok(user(id).fetch_optional(&self.pool).await?)
    }
    async fn users(
        &self,
        search: &str,
        after: &str,
        limit: i64,
        role: Option<&str>,
    ) -> Result<Vec<User>> {
        Ok(list_users(search, after, limit, role)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn create_user(&self, principal: &Principal, input: UserCreate) -> Result<User> {
        transactions::create_user(&self.pool, principal, input).await
    }
    async fn update_user(&self, principal: &Principal, id: Uuid, input: UserPatch) -> Result<User> {
        transactions::update_user(&self.pool, principal, id, input).await
    }
    async fn delete_user(&self, principal: &Principal, id: Uuid) -> Result<()> {
        transactions::delete_user(&self.pool, principal, id).await
    }
    async fn reset_password(
        &self,
        principal: &Principal,
        id: Uuid,
        hash: &str,
        require_change: bool,
    ) -> Result<()> {
        transactions::reset_password(&self.pool, principal, id, hash, require_change).await
    }
    async fn save_member(
        &self,
        principal: &Principal,
        project: Uuid,
        user: Uuid,
        input: Option<Membership>,
    ) -> Result<()> {
        transactions::save_member(&self.pool, principal, project, user, input).await
    }
    async fn members(&self, project: Uuid) -> Result<Vec<Member>> {
        Ok(project_members(project).fetch_all(&self.pool).await?)
    }
    async fn projects(&self) -> Result<Vec<Project>> {
        Ok(projects().fetch_all(&self.pool).await?)
    }
    async fn visible_projects(
        &self,
        user: Uuid,
        admin: bool,
        token: Option<Uuid>,
    ) -> Result<Vec<Project>> {
        Ok(visible_projects(user, admin, token)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn save_project(
        &self,
        principal: &Principal,
        id: Option<Uuid>,
        input: ProjectInput,
    ) -> Result<Project> {
        transactions::save_project(&self.pool, principal, id, input).await
    }
    async fn delete_project(&self, principal: &Principal, id: Uuid) -> Result<()> {
        transactions::delete_project(&self.pool, principal, id).await
    }
    async fn project_mode(&self) -> Result<bool> {
        Ok(project_mode().fetch_one(&self.pool).await?)
    }
    async fn set_project_mode(&self, principal: &Principal, enabled: bool) -> Result<()> {
        transactions::set_project_mode(&self.pool, principal, enabled).await
    }
}

pub(super) fn credential_record<'q>(access: &'q str) -> Rows<'q, Credential> {
    sqlx::query_as("SELECT c.*,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM s3_credential_grants g WHERE g.access_key=c.access_key_id),'[]'::jsonb) AS grants FROM s3_credentials c WHERE c.access_key_id=$1").bind(access)
}

pub(super) fn delete_credential_grants<'q>(access: &'q str) -> Statement<'q> {
    sqlx::query("DELETE FROM s3_credential_grants WHERE access_key=$1").bind(access)
}

pub(super) fn insert_credential_grant<'q>(
    access: &'q str,
    grant: &'q BucketGrant,
) -> Statement<'q> {
    sqlx::query("INSERT INTO s3_credential_grants(access_key,bucket_id,actions) VALUES($1,$2,$3)")
        .bind(access)
        .bind(grant.bucket_id)
        .bind(grant.actions.iter().map(|a| a.name()).collect::<Vec<_>>())
}

pub(super) fn credential_project(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM projects WHERE id=$1 FOR SHARE").bind(id)
}

pub(super) fn insert_credential<'q>(
    material: &'q CredentialMaterial,
    input: &'q CredentialInput,
    expires: Option<DateTime<Utc>>,
    user: Option<Uuid>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO s3_credentials(access_key_id,secret_encrypted,project_id,label,expires_at,created_by) VALUES($1,$2,$3,$4,$5,$6)").bind(&material.access).bind(&material.encrypted).bind(input.project_id).bind(input.label.trim()).bind(expires).bind(user)
}

pub(super) fn update_credential<'q>(
    access: &'q str,
    input: &'q CredentialSettings,
    expires: Option<DateTime<Utc>>,
) -> Statement<'q> {
    sqlx::query("UPDATE s3_credentials SET label=$2,enabled=$3,expires_at=CASE WHEN $5 THEN expires_at ELSE $4 END WHERE access_key_id=$1").bind(access).bind(input.label.trim()).bind(input.enabled).bind(expires).bind(input.keep_expiry)
}

pub(super) fn lock_credential_project<'q>(access: &'q str) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT project_id FROM s3_credentials WHERE access_key_id=$1 FOR UPDATE")
        .bind(access)
}

pub(super) fn delete_credential<'q>(access: &'q str) -> Statement<'q> {
    sqlx::query("DELETE FROM s3_credentials WHERE access_key_id=$1").bind(access)
}

pub(super) fn enable_credential<'q>(access: &'q str, enabled: bool) -> Statement<'q> {
    sqlx::query("UPDATE s3_credentials SET enabled=$2 WHERE access_key_id=$1")
        .bind(access)
        .bind(enabled)
}

pub(super) fn lock_credential<'q>(access: &'q str) -> Statement<'q> {
    sqlx::query(
        "SELECT access_key_id AS access_key FROM s3_credentials WHERE access_key_id=$1 FOR UPDATE",
    )
    .bind(access)
}

pub(super) fn expire_credential<'q>(access: &'q str, expires: DateTime<Utc>) -> Statement<'q> {
    sqlx::query("UPDATE s3_credentials SET expires_at=$2 WHERE access_key_id=$1")
        .bind(access)
        .bind(expires)
}

pub(super) fn credential_records<'q>(
    project: Option<Uuid>,
    after: &'q str,
    limit: i64,
) -> Rows<'q, Credential> {
    sqlx::query_as("SELECT c.*,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM s3_credential_grants g WHERE g.access_key=c.access_key_id),'[]'::jsonb) AS grants FROM s3_credentials c WHERE ($1::uuid IS NULL OR c.project_id=$1) AND c.access_key_id>$2 ORDER BY c.access_key_id LIMIT $3").bind(project).bind(after).bind(limit)
}

pub(super) fn token_record(id: Uuid) -> Rows<'static, Token> {
    sqlx::query_as("SELECT t.id,t.user_id,t.label,t.prefix,t.system,t.created_at,t.expires_at,t.last_used_at,t.revoked_at,u.enabled AND NOT u.must_change_password AND t.auth_revision=u.auth_revision AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at>now()) AS active,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM token_bucket_grants g WHERE g.token_id=t.id),'[]'::jsonb) AS grants,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM token_external_grants g WHERE g.token_id=t.id),'[]'::jsonb) AS external_grants FROM api_tokens t JOIN web_users u ON u.id=t.user_id WHERE t.id=$1").bind(id)
}

pub(super) fn lock_token_owner(user: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM web_users WHERE id=$1 FOR SHARE").bind(user)
}

pub(super) fn token_owner_revision(user: Uuid) -> Rows<'static, (String, i64)> {
    sqlx::query_as("SELECT role,auth_revision FROM web_users WHERE id=$1 AND enabled AND NOT must_change_password").bind(user)
}

pub(super) fn token_owner_actions(user: Uuid, bucket: Uuid) -> Scalar<'static, Vec<String>> {
    sqlx::query_scalar("SELECT actions FROM user_bucket_access WHERE user_id=$1 AND bucket_id=$2")
        .bind(user)
        .bind(bucket)
}

pub(super) fn delete_token_grants(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM token_bucket_grants WHERE token_id=$1").bind(id)
}

pub(super) fn insert_token_grant<'q>(id: Uuid, grant: &'q BucketGrant) -> Statement<'q> {
    sqlx::query("INSERT INTO token_bucket_grants(token_id,bucket_id,actions) VALUES($1,$2,$3)")
        .bind(id)
        .bind(grant.bucket_id)
        .bind(grant.actions.iter().map(|a| a.name()).collect::<Vec<_>>())
}

pub(super) fn insert_token<'q>(
    user: Uuid,
    input: &'q TokenInput,
    material: &'q TokenMaterial,
    revision: i64,
    expires: Option<DateTime<Utc>>,
) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("INSERT INTO api_tokens(user_id,label,prefix,token_hash,system,auth_revision,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id").bind(user).bind(input.label.trim()).bind(&material.prefix).bind(material.hash.as_slice()).bind(input.system).bind(revision).bind(expires)
}

pub(super) fn lock_token(id: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM api_tokens WHERE id=$1 FOR UPDATE").bind(id)
}

pub(super) fn update_token<'q>(
    id: Uuid,
    input: &'q TokenInput,
    expires: Option<DateTime<Utc>>,
) -> Statement<'q> {
    sqlx::query("UPDATE api_tokens SET label=$2,system=$3,expires_at=CASE WHEN $5 THEN expires_at ELSE $4 END WHERE id=$1").bind(id).bind(input.label.trim()).bind(input.system).bind(expires).bind(input.keep_expiry)
}

pub(super) fn token_user(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT user_id FROM api_tokens WHERE id=$1").bind(id)
}

pub(super) fn revoke_token(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE api_tokens SET revoked_at=COALESCE(revoked_at,now()) WHERE id=$1").bind(id)
}

pub(super) fn token_records(user: Uuid, after: Option<Uuid>, limit: i64) -> Rows<'static, Token> {
    sqlx::query_as("SELECT t.id,t.user_id,t.label,t.prefix,t.system,t.created_at,t.expires_at,t.last_used_at,t.revoked_at,u.enabled AND NOT u.must_change_password AND t.auth_revision=u.auth_revision AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at>now()) AS active,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM token_bucket_grants g WHERE g.token_id=t.id),'[]'::jsonb) AS grants,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM token_external_grants g WHERE g.token_id=t.id),'[]'::jsonb) AS external_grants FROM api_tokens t JOIN web_users u ON u.id=t.user_id WHERE t.user_id=$1 AND ($2::uuid IS NULL OR t.id>$2) ORDER BY t.id LIMIT $3").bind(user).bind(after).bind(limit)
}

#[async_trait::async_trait]
impl CredentialStore for Backend {
    async fn token_user(&self, id: Uuid) -> Result<Option<Uuid>> {
        Ok(token_user(id).fetch_optional(&self.pool).await?)
    }

    async fn encrypted_secret(&self, access: &str) -> Result<Option<Vec<u8>>> {
        Ok(sqlx::query_scalar("SELECT secret_encrypted FROM s3_credentials WHERE access_key_id=$1 AND enabled AND (expires_at IS NULL OR expires_at>now())").bind(access).fetch_optional(&self.pool).await?)
    }
    async fn touch_credential(&self, access: &str) -> Result<()> {
        touch_credentials(&[access.to_owned()])
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn buckets(
        &self,
        access: &str,
        prefix: &str,
        after: &str,
        region: Option<&str>,
        expected_region: &str,
        limit: i64,
    ) -> Result<Vec<crate::app::Bucket>> {
        Ok(
            credential_buckets(access, prefix, after, region, expected_region, limit)
                .fetch_all(&self.pool)
                .await?,
        )
    }

    async fn credential(&self, access: &str) -> Result<Credential> {
        Ok(credential_record(access)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| s3_error!(NoSuchKey))?)
    }
    async fn credentials(
        &self,
        project: Option<Uuid>,
        after: &str,
        limit: i64,
    ) -> Result<Vec<Credential>> {
        Ok(credential_records(project, after, limit)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn create_credential(
        &self,
        principal: &Principal,
        input: CredentialInput,
        material: CredentialMaterial,
    ) -> Result<Credential> {
        transactions::create_credential(&self.pool, principal, input, material).await
    }
    async fn update_credential(
        &self,
        principal: &Principal,
        access: &str,
        input: CredentialSettings,
    ) -> Result<Credential> {
        transactions::update_credential(&self.pool, principal, access, input).await
    }
    async fn set_grants(
        &self,
        principal: &Principal,
        access: &str,
        grants: Vec<BucketGrant>,
    ) -> Result<Credential> {
        transactions::set_grants(&self.pool, principal, access, grants).await
    }
    async fn revoke_credential(&self, principal: &Principal, access: &str) -> Result<()> {
        transactions::revoke_credential(&self.pool, principal, access).await
    }
    async fn enable_credential(
        &self,
        principal: &Principal,
        access: &str,
        enabled: bool,
    ) -> Result<()> {
        transactions::enable_credential(&self.pool, principal, access, enabled).await
    }
    async fn change_grant(
        &self,
        principal: &Principal,
        access: &str,
        bucket: Uuid,
        actions: Vec<Action>,
    ) -> Result<()> {
        transactions::change_grant(&self.pool, principal, access, bucket, actions).await
    }
    async fn rotate_credential(
        &self,
        principal: &Principal,
        access: &str,
        input: RotateCredential,
        material: CredentialMaterial,
    ) -> Result<(Credential, DateTime<Utc>)> {
        transactions::rotate_credential(&self.pool, principal, access, input, material).await
    }
    async fn create_token(
        &self,
        principal: &Principal,
        user: Uuid,
        input: TokenInput,
        material: TokenMaterial,
    ) -> Result<Token> {
        transactions::create_token(&self.pool, principal, user, input, material).await
    }
    async fn update_token(
        &self,
        principal: &Principal,
        user: Uuid,
        id: Uuid,
        input: TokenInput,
    ) -> Result<Token> {
        transactions::update_token(&self.pool, principal, user, id, input).await
    }
    async fn revoke_token(&self, principal: &Principal, id: Uuid) -> Result<()> {
        transactions::revoke_token(&self.pool, principal, id).await
    }
    async fn tokens(&self, user: Uuid, after: Option<Uuid>, limit: i64) -> Result<Vec<Token>> {
        Ok(token_records(user, after, limit)
            .fetch_all(&self.pool)
            .await?)
    }
}

pub(super) fn lock_bucket_id(bucket: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM buckets WHERE id=$1 FOR UPDATE").bind(bucket)
}

pub(super) fn bucket_creation_policy(project: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT allow_bucket_create FROM projects WHERE id=$1 FOR SHARE")
        .bind(project)
}

pub(super) fn bucket_creator_membership(
    project: Uuid,
    user: Option<Uuid>,
) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT role,scope FROM project_members WHERE project_id=$1 AND user_id=$2")
        .bind(project)
        .bind(user)
}

pub(super) fn bucket_available_storages() -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM storages WHERE enabled ORDER BY created_at,id")
}

pub(super) fn insert_bucket<'q>(
    name: &'q str,
    project: Uuid,
    storage_id: Uuid,
) -> Rows<'q, Bucket> {
    sqlx::query_as("INSERT INTO buckets(name,project_id,storage_id) VALUES($1,$2,$3) ON CONFLICT(name) DO NOTHING RETURNING *").bind(name).bind(project).bind(storage_id)
}

pub(super) fn grant_bucket_creator(
    user: Option<Uuid>,
    project: Uuid,
    bucket: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO member_bucket_grants(user_id,project_id,bucket_id,actions) VALUES($1,$2,$3,$4)").bind(user).bind(project).bind(bucket).bind(Action::ALL.map(|a| a.name()).to_vec())
}

pub(super) fn lock_bucket_members(bucket: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM web_users WHERE id IN (SELECT user_id FROM member_bucket_grants WHERE bucket_id=$1) ORDER BY id FOR UPDATE").bind(bucket)
}

pub(super) fn lock_bucket_credentials(bucket: Uuid) -> Statement<'static> {
    sqlx::query("SELECT access_key_id AS access_key FROM s3_credentials WHERE access_key_id IN (SELECT access_key FROM s3_credential_grants WHERE bucket_id=$1) ORDER BY access_key_id FOR UPDATE").bind(bucket)
}

pub(super) fn lock_bucket_tokens(bucket: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM api_tokens WHERE id IN (SELECT token_id FROM token_bucket_grants WHERE bucket_id=$1) ORDER BY id FOR UPDATE").bind(bucket)
}

pub(super) fn bucket_impact(bucket: Uuid) -> Rows<'static, BucketImpact> {
    sqlx::query_as("SELECT b.id AS bucket_id,b.name,b.project_id,b.state,b.settings_revision::text AS revision,b.uploads_paused,q.object_count::text AS objects,q.used_bytes::text AS logical_bytes,q.reserved_bytes::text,q.inflight_bytes::text,(SELECT count(*)::text FROM uploads WHERE bucket_id=b.id AND state IN ('active','completing')) AS active_uploads,(SELECT count(*)::text FROM streams WHERE bucket_id=b.id AND state='writing') AS writing_streams,(SELECT count(*)::text FROM member_bucket_grants WHERE bucket_id=b.id) AS member_bucket_grants,(SELECT count(*)::text FROM s3_credential_grants WHERE bucket_id=b.id) AS service_grants,(SELECT count(*)::text FROM token_bucket_grants WHERE bucket_id=b.id) AS token_bucket_grants,ARRAY(SELECT host FROM bucket_domains WHERE bucket_id=b.id ORDER BY host) AS domains FROM buckets b JOIN quota_scopes q ON q.scope='bucket' AND q.scope_id=b.id WHERE b.id=$1").bind(bucket)
}

pub(super) fn lock_bucket(bucket: Uuid) -> Rows<'static, Bucket> {
    sqlx::query_as("SELECT * FROM buckets WHERE id=$1 FOR UPDATE").bind(bucket)
}

pub(super) fn bucket_has_streams(bucket: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM streams WHERE bucket_id=$1) OR EXISTS(SELECT 1 FROM uploads WHERE bucket_id=$1)").bind(bucket)
}

pub(super) fn delete_bucket_objects(bucket: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM objects WHERE bucket_id=$1").bind(bucket)
}

pub(super) fn delete_bucket(bucket: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM buckets WHERE id=$1").bind(bucket)
}

pub(super) fn bucket_domain_owner<'q>(value: &'q str) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT bucket_id FROM bucket_domains WHERE host=$1").bind(value)
}

pub(super) fn bucket_domain_state(id: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT state FROM buckets WHERE id=$1 FOR UPDATE").bind(id)
}

pub(super) fn insert_bucket_domain<'q>(value: &'q str, id: Uuid) -> Statement<'q> {
    sqlx::query(
        "INSERT INTO bucket_domains(host,bucket_id) VALUES($1,$2) ON CONFLICT(host) DO NOTHING",
    )
    .bind(value)
    .bind(id)
}

pub(super) fn delete_bucket_domain<'q>(value: &'q str) -> Statement<'q> {
    sqlx::query("DELETE FROM bucket_domains WHERE host=$1").bind(value)
}

pub(super) fn bucket_domains(bucket: Option<Uuid>) -> Rows<'static, Domain> {
    sqlx::query_as("SELECT host,bucket_id FROM bucket_domains WHERE ($1::uuid IS NULL OR bucket_id=$1) ORDER BY host").bind(bucket)
}

pub(super) fn bucket_snapshot() -> Statement<'static> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
}

pub(super) fn bucket_by_id(bucket: Uuid) -> Rows<'static, Bucket> {
    sqlx::query_as("SELECT * FROM buckets WHERE id=$1").bind(bucket)
}

pub(super) fn bucket_domain_names(bucket: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT host FROM bucket_domains WHERE bucket_id=$1 ORDER BY host")
        .bind(bucket)
}

pub(super) fn bucket_creation_projects(
    admin: bool,
    user: Uuid,
    session: bool,
) -> Rows<'static, BucketProject> {
    sqlx::query_as("SELECT id,name FROM projects p WHERE $1 OR ($3 AND allow_bucket_create AND EXISTS(SELECT 1 FROM project_members WHERE project_id=p.id AND user_id=$2 AND role='maintainer')) ORDER BY builtin DESC,name LIMIT 1000").bind(admin).bind(user).bind(session)
}

pub(super) fn bucket_creation_storages() -> Rows<'static, BucketStorage> {
    sqlx::query_as("SELECT id,name,type AS storage_type FROM storages WHERE enabled ORDER BY name")
}

pub(super) fn bucket_domains_in_use<'q>(domains: &'q [String], bucket: Uuid) -> Scalar<'q, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM bucket_domains WHERE host=ANY($1) AND bucket_id<>$2)",
    )
    .bind(domains)
    .bind(bucket)
}

pub(super) fn remove_bucket_domains<'q>(bucket: Uuid, domains: &'q [String]) -> Statement<'q> {
    sqlx::query("DELETE FROM bucket_domains WHERE bucket_id=$1 AND NOT(host=ANY($2))")
        .bind(bucket)
        .bind(domains)
}

pub(super) fn insert_bucket_domains<'q>(domains: &'q [String], bucket: Uuid) -> Statement<'q> {
    sqlx::query("INSERT INTO bucket_domains(host,bucket_id) SELECT unnest($1::text[]),$2 ON CONFLICT DO NOTHING").bind(domains).bind(bucket)
}

pub(super) fn save_bucket_settings<'q>(bucket: Uuid, input: &'q BucketUpdate) -> Statement<'q> {
    sqlx::query("UPDATE buckets SET cors=$2,website_enabled=$3,index_document=$4,error_document=$5,public_base_url=$6,uploads_paused=$7 WHERE id=$1").bind(bucket).bind(&input.cors).bind(input.website_enabled).bind(&input.index_document).bind(&input.error_document).bind(&input.public_base_url).bind(input.uploads_paused)
}

pub(super) fn bucket_transfer_target(id: Uuid) -> Rows<'static, TransferProject> {
    sqlx::query_as("SELECT p.id,p.name,q.used_bytes::text,q.reserved_bytes::text,q.logical_bytes_limit::text,q.bucket_count::text,q.bucket_count_limit::text,(SELECT count(*)::text FROM project_members WHERE project_id=p.id) AS members FROM projects p JOIN quota_scopes q ON q.scope='project' AND q.scope_id=p.id WHERE p.id=$1").bind(id)
}

pub(super) fn bucket_project_id(bucket: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT project_id FROM buckets WHERE id=$1").bind(bucket)
}

pub(super) fn lock_bucket_transfer_users(source: Uuid, target: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM web_users WHERE id IN (SELECT user_id FROM project_members WHERE project_id=ANY($1)) ORDER BY id FOR UPDATE").bind(vec![source, target])
}

pub(super) fn revoke_bucket_members(bucket: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM member_bucket_grants WHERE bucket_id=$1").bind(bucket)
}

pub(super) fn revoke_bucket_credentials(bucket: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM s3_credential_grants WHERE bucket_id=$1").bind(bucket)
}

pub(super) fn revoke_bucket_tokens(bucket: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM token_bucket_grants WHERE bucket_id=$1").bind(bucket)
}

pub(super) fn revoke_bucket_transfer_users(source: Uuid, target: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE web_users SET authorization_revision=authorization_revision+1 WHERE id IN (SELECT user_id FROM project_members WHERE project_id=ANY($1))").bind(vec![source, target])
}

pub(super) fn transfer_bucket(bucket: Uuid, target: Uuid) -> Rows<'static, Bucket> {
    sqlx::query_as("UPDATE buckets SET project_id=$2,uploads_paused=false WHERE id=$1 RETURNING *")
        .bind(bucket)
        .bind(target)
}

pub(super) fn set_bucket_cors(bucket: Uuid, rules: Value) -> Scalar<'static, Value> {
    sqlx::query_scalar("UPDATE buckets SET cors=$2 WHERE id=$1 AND state='active' RETURNING cors")
        .bind(bucket)
        .bind(rules)
}

pub(super) fn bucket_by_name<'q>(name: &'q str) -> Rows<'q, Bucket> {
    sqlx::query_as("SELECT * FROM buckets WHERE name=$1").bind(name)
}

pub(super) fn list_bucket_objects<'q>(
    bucket: Uuid,
    prefix: &'q str,
    key: &'q str,
    inclusive: bool,
) -> Rows<'q, crate::app::StoredStream> {
    let pattern = format!(
        "{}%",
        prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    );
    sqlx::query_as("SELECT s.* FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=$1 AND o.object_key LIKE $2 AND (o.object_key>$3 OR ($4 AND o.object_key=$3)) ORDER BY o.object_key LIMIT 64").bind(bucket).bind(pattern).bind(key).bind(inclusive)
}

pub(super) fn active_bucket_by_name<'q>(name: &'q str) -> Rows<'q, Bucket> {
    sqlx::query_as("SELECT * FROM buckets WHERE name=$1 AND state='active'").bind(name)
}

pub(super) fn bucket_by_host<'q>(host: &'q str) -> Rows<'q, Bucket> {
    sqlx::query_as(
        "SELECT b.* FROM buckets b JOIN bucket_domains d ON d.bucket_id=b.id WHERE d.host=$1",
    )
    .bind(host)
}

pub(super) fn visible_buckets(
    user: Uuid,
    admin: bool,
    token: Option<Uuid>,
) -> Rows<'static, BucketView> {
    sqlx::query_as("SELECT b.*,b.settings_revision::text AS revision,CASE WHEN $2 THEN ARRAY['bucket.list','object.read','object.write','object.delete','object.acl','bucket.settings','storage.inspect'] WHEN $3::uuid IS NOT NULL THEN t.actions ELSE a.actions END AS actions FROM buckets b LEFT JOIN user_bucket_access a ON a.bucket_id=b.id AND a.user_id=$1 LEFT JOIN token_bucket_access t ON t.bucket_id=b.id AND t.token_id=$3 WHERE $2 OR 'bucket.list'=ANY(CASE WHEN $3::uuid IS NOT NULL THEN t.actions ELSE a.actions END) ORDER BY b.name LIMIT 1000").bind(user).bind(admin).bind(token)
}

pub(super) fn bucket_cors(bucket: Uuid) -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT cors FROM buckets WHERE id=$1").bind(bucket)
}

pub(super) fn bucket_website(bucket: Uuid) -> Rows<'static, Website> {
    sqlx::query_as("SELECT website_enabled,index_document,error_document FROM buckets WHERE id=$1")
        .bind(bucket)
}

pub(super) fn set_bucket_website<'q>(bucket: Uuid, input: &'q Website) -> Rows<'q, Website> {
    sqlx::query_as("UPDATE buckets SET website_enabled=$2,index_document=$3,error_document=$4 WHERE id=$1 AND state='active' RETURNING website_enabled,index_document,error_document").bind(bucket).bind(input.website_enabled).bind(&input.index_document).bind(&input.error_document)
}

#[async_trait::async_trait]
impl BucketStore for Backend {
    async fn domain_names(&self) -> Result<Vec<(String, String)>> {
        Ok(sqlx::query_as("SELECT d.host,b.name FROM bucket_domains d JOIN buckets b ON b.id=d.bucket_id ORDER BY d.host").fetch_all(&self.pool).await?)
    }

    async fn purge_counts(&self, bucket: Uuid) -> Result<(i64, i64)> {
        Ok(purge_counts(bucket).fetch_one(&self.pool).await?)
    }
    async fn start_purge(
        &self,
        principal: &Principal,
        name: &str,
        id: Uuid,
        confirmation: Option<&str>,
    ) -> Result<Value> {
        transactions::start_purge(&self.pool, principal, name, id, confirmation).await
    }
    async fn purge_uploads(&self, bucket: Uuid, default_batch: i32) -> Result<(i64, Vec<Uuid>)> {
        let batch = purge_batch_limit(bucket)
            .fetch_one(&self.pool)
            .await?
            .unwrap_or(default_batch)
            .max(1) as i64;
        Ok((
            batch,
            purge_uploads(bucket, batch).fetch_all(&self.pool).await?,
        ))
    }
    async fn purge_batch(
        &self,
        task: Execution,
        bucket: Uuid,
        cursor: Option<&str>,
        batch: i64,
        active: &[Uuid],
    ) -> Result<bool> {
        transactions::purge_batch(&self.pool, task, bucket, cursor, batch, active).await
    }

    async fn by_name(&self, name: &str, active: bool) -> Result<Option<Bucket>> {
        Ok(if active {
            active_bucket_by_name(name)
                .fetch_optional(&self.pool)
                .await?
        } else {
            bucket_by_name(name).fetch_optional(&self.pool).await?
        })
    }
    async fn by_host(&self, host: &str) -> Result<Option<Bucket>> {
        Ok(bucket_by_host(host).fetch_optional(&self.pool).await?)
    }
    async fn all(&self) -> Result<Vec<Bucket>> {
        Ok(sqlx::query_as("SELECT * FROM buckets ORDER BY name")
            .fetch_all(&self.pool)
            .await?)
    }
    async fn visible(
        &self,
        user: Uuid,
        admin: bool,
        token: Option<Uuid>,
    ) -> Result<Vec<BucketView>> {
        Ok(visible_buckets(user, admin, token)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn list_objects(
        &self,
        bucket: Uuid,
        prefix: &str,
        key: &str,
        inclusive: bool,
    ) -> Result<Vec<crate::app::StoredStream>> {
        Ok(list_bucket_objects(bucket, prefix, key, inclusive)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn creation_options(
        &self,
        admin: bool,
        user: Uuid,
        session: bool,
    ) -> Result<(Vec<BucketProject>, Vec<BucketStorage>)> {
        Ok((
            bucket_creation_projects(admin, user, session)
                .fetch_all(&self.pool)
                .await?,
            bucket_creation_storages().fetch_all(&self.pool).await?,
        ))
    }
    async fn cors(&self, bucket: Uuid) -> Result<Value> {
        Ok(bucket_cors(bucket)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?)
    }
    async fn website(&self, bucket: Uuid) -> Result<Website> {
        Ok(bucket_website(bucket)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?)
    }
    async fn create(&self, principal: &Principal, input: CreateBucket) -> Result<Bucket> {
        transactions::create_bucket(&self.pool, principal, input).await
    }
    async fn delete(
        &self,
        principal: &Principal,
        bucket: Uuid,
        name: &str,
        confirmation: Option<&str>,
    ) -> Result<()> {
        transactions::delete_bucket(&self.pool, principal, bucket, name, confirmation).await
    }
    async fn domains(&self, principal: &Principal, bucket: Option<Uuid>) -> Result<Vec<Domain>> {
        transactions::bucket_domains(&self.pool, principal, bucket).await
    }
    async fn change_domain(
        &self,
        principal: &Principal,
        host: &str,
        bucket: Option<Uuid>,
    ) -> Result<()> {
        transactions::domain_change(&self.pool, principal, host, bucket).await
    }
    async fn settings(&self, bucket: Uuid) -> Result<(Bucket, Vec<String>)> {
        transactions::bucket_settings(&self.pool, bucket).await
    }
    async fn save_settings(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: BucketUpdate,
    ) -> Result<()> {
        transactions::save_bucket_settings(&self.pool, principal, bucket, input).await
    }
    async fn impact(&self, principal: &Principal, bucket: Uuid) -> Result<BucketImpact> {
        transactions::preview_bucket(&self.pool, principal, bucket).await
    }
    async fn transfer_preview(
        &self,
        principal: &Principal,
        bucket: Uuid,
        target: Uuid,
    ) -> Result<(BucketImpact, TransferProject)> {
        transactions::preview_bucket_transfer(&self.pool, principal, bucket, target).await
    }
    async fn transfer(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: TransferInput,
    ) -> Result<Bucket> {
        transactions::transfer_bucket(&self.pool, principal, bucket, input).await
    }
    async fn set_cors(&self, principal: &Principal, bucket: Uuid, rules: Value) -> Result<Value> {
        transactions::set_bucket_cors(&self.pool, principal, bucket, rules).await
    }
    async fn set_website(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: Website,
    ) -> Result<Website> {
        transactions::set_bucket_website(&self.pool, principal, bucket, input).await
    }
}

pub(super) fn runtime_settings_row(section: &str) -> Rows<'_, RuntimeSettingsRow> {
    sqlx::query_as("SELECT value,revision FROM runtime_settings WHERE section=$1").bind(section)
}
pub(super) fn lock_runtime_settings(section: &str) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT revision FROM runtime_settings WHERE section=$1 FOR UPDATE")
        .bind(section)
}
pub(super) fn save_runtime_settings<'q>(
    section: &'q str,
    value: Value,
    user: Option<Uuid>,
    revision: i64,
) -> Rows<'q, RuntimeSettingsRow> {
    sqlx::query_as("UPDATE runtime_settings SET value=$2,revision=revision+1,updated_at=now(),updated_by_user_id=$3 WHERE section=$1 AND revision=$4 RETURNING value,revision").bind(section).bind(value).bind(user).bind(revision)
}
pub(super) fn reset_runtime_settings<'q>(
    section: &'q str,
    value: Value,
    user: Option<Uuid>,
) -> Rows<'q, RuntimeSettingsRow> {
    sqlx::query_as("UPDATE runtime_settings SET value=$2,revision=revision+1,updated_at=now(),updated_by_user_id=$3 WHERE section=$1 RETURNING value,revision").bind(section).bind(value).bind(user)
}
pub(super) fn lock_web_settings() -> Rows<'static, RuntimeSettingsRow> {
    sqlx::query_as("SELECT value,revision FROM runtime_settings WHERE section='web' FOR UPDATE")
}
pub(super) fn save_web_settings(
    value: Value,
    next: i64,
    user: Option<Uuid>,
    revision: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE runtime_settings SET value=$1,revision=$2,updated_at=now(),updated_by_user_id=$3 WHERE section='web' AND revision=$4").bind(value).bind(next).bind(user).bind(revision)
}

pub(super) type ManagedKeyViewRow = (
    Uuid,
    String,
    String,
    String,
    bool,
    Vec<u8>,
    DateTime<Utc>,
    Option<DateTime<Utc>>,
);
pub(super) fn managed_key_views() -> Rows<'static, ManagedKeyViewRow> {
    sqlx::query_as(
        "SELECT k.id,k.name,k.purpose,k.key_algorithm AS algorithm,EXISTS(SELECT 1 FROM storages s WHERE s.block_key_id=k.id) OR EXISTS(SELECT 1 FROM chunk_encodings e WHERE e.block_key_id=k.id AND e.state<>'deleted') OR EXISTS(SELECT 1 FROM packs p WHERE p.block_key_id=k.id AND p.state<>'deleted') OR EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.block_key_id=k.id) OR EXISTS(SELECT 1 FROM node_writes w JOIN stream_leases l ON l.id=w.lease_id WHERE w.block_key_id=k.id AND w.state='active' AND l.expires_at>clock_timestamp()) OR EXISTS(SELECT 1 FROM tasks t WHERE t.detail->>'target_key_id'=k.id::text AND t.state IN ('queued','running','paused','failed')),k.fingerprint,k.created_at,k.revoked_at FROM managed_keys k ORDER BY k.name",
    )
}
pub(super) fn managed_key_in_use(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storages WHERE block_key_id=$1) OR EXISTS(SELECT 1 FROM chunk_encodings WHERE block_key_id=$1 AND state<>'deleted') OR EXISTS(SELECT 1 FROM packs WHERE block_key_id=$1 AND state<>'deleted') OR EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.block_key_id=$1) OR EXISTS(SELECT 1 FROM node_writes w JOIN stream_leases l ON l.id=w.lease_id WHERE w.block_key_id=$1 AND w.state='active' AND l.expires_at>clock_timestamp()) OR EXISTS(SELECT 1 FROM tasks WHERE detail->>'target_key_id'=$1::text AND state IN ('queued','running','paused','failed')) FROM managed_keys WHERE id=$1").bind(id)
}
pub(super) fn lock_managed_key(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT true FROM managed_keys WHERE id=$1 FOR UPDATE").bind(id)
}
pub(super) fn insert_managed_key(input: &ManagedKeyCreate) -> Rows<'_, ManagedKeyViewRow> {
    sqlx::query_as("INSERT INTO managed_keys(id,name,purpose,key_algorithm,secret_encrypted,fingerprint) VALUES($1,$2,$3,$4,$5,$6) RETURNING id,name,purpose,key_algorithm AS algorithm,false,fingerprint,created_at,revoked_at").bind(input.id).bind(input.name.trim()).bind(&input.purpose).bind(&input.algorithm).bind(&input.protected).bind(input.fingerprint.as_slice())
}
pub(super) fn revoke_managed_key(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE managed_keys SET revoked_at=coalesce(revoked_at,now()),secret_encrypted=NULL WHERE id=$1").bind(id)
}

pub(super) fn managed_key_view(
    (id, name, purpose, algorithm, active, fingerprint, created_at, revoked_at): ManagedKeyViewRow,
) -> ManagedKeyView {
    ManagedKeyView {
        nonce_capacity: None,
        id,
        name,
        purpose,
        algorithm,
        in_use: active,
        fingerprint: hex::encode(fingerprint),
        created_at,
        revoked_at,
    }
}

pub(super) fn multipart_upload<'q>(
    id: Uuid,
    bucket: Uuid,
    key: &'q str,
    access: &'q str,
) -> Rows<'q, Upload> {
    sqlx::query_as("SELECT * FROM uploads WHERE id=$1 AND bucket_id=$2 AND object_key=$3 AND access_key=$4 AND state<>'aborted'").bind(id).bind(bucket).bind(key).bind(access)
}

pub(super) fn multipart_bucket(bucket: Uuid) -> Rows<'static, (String, bool)> {
    sqlx::query_as("SELECT state,uploads_paused FROM buckets WHERE id=$1 FOR SHARE").bind(bucket)
}

pub(super) fn web_upload_identity(user: Uuid, client: Uuid) -> Rows<'static, (Uuid, Vec<u8>)> {
    sqlx::query_as(
        "SELECT upload_id,request_hash FROM web_uploads WHERE user_id=$1 AND client_id=$2",
    )
    .bind(user)
    .bind(client)
}

pub(super) fn web_upload_expected<'q>(bucket: Uuid, key: &'q str) -> Scalar<'q, Option<Id>> {
    sqlx::query_scalar("SELECT stream_id FROM objects WHERE bucket_id=$1 AND object_key=$2")
        .bind(bucket)
        .bind(key)
}

pub(super) fn lock_upload_admission() -> Statement<'static> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('mokyu:uploads:max-active', 0))")
}

pub(super) fn active_upload_count() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM uploads WHERE state IN ('active','completing')")
}

pub(super) fn insert_multipart<'q>(input: &'q MultipartCreate<'_>) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("INSERT INTO uploads(bucket_id,object_key,access_key,metadata,public_read,checksum_algorithm,checksum_type) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id").bind(input.bucket).bind(input.key).bind(input.access).bind(&input.metadata).bind(input.public).bind(&input.algorithm).bind(&input.checksum_type)
}

pub(super) fn insert_web_upload<'q>(
    id: Uuid,
    web: &'q WebUpload,
    expected: Option<Id>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO web_uploads(upload_id,user_id,client_id,request_hash,file_name,expected_size,part_size,modified_at,expected_stream_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)").bind(id).bind(web.user).bind(web.client_id).bind(&web.request_hash).bind(&web.file_name).bind(web.size).bind(web.part_size).bind(web.modified_at).bind(expected)
}

pub(super) fn lock_upload_state(upload: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT CASE WHEN remote_manifest_hash IS NOT NULL AND state='active' THEN 'completing' ELSE state END FROM uploads WHERE id=$1 FOR UPDATE").bind(upload)
}

pub(super) fn begin_upload_part(upload: Uuid, number: i32, stream: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO parts(upload_id,part_number,write_token) VALUES($1,$2,$3) ON CONFLICT(upload_id,part_number) DO UPDATE SET write_token=excluded.write_token").bind(upload).bind(number).bind(stream)
}

pub(super) fn multipart_bucket_state(bucket: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT state FROM buckets WHERE id=$1 FOR SHARE").bind(bucket)
}

pub(super) fn lock_upload_part(upload: Uuid, number: i32) -> Rows<'static, (Option<Id>, Id)> {
    sqlx::query_as(
        "SELECT stream_id,write_token FROM parts WHERE upload_id=$1 AND part_number=$2 FOR UPDATE",
    )
    .bind(upload)
    .bind(number)
}

pub(super) fn publish_upload_part(upload: Uuid, number: i32, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE parts SET stream_id=$3 WHERE upload_id=$1 AND part_number=$2")
        .bind(upload)
        .bind(number)
        .bind(stream)
}

pub(super) fn ready_upload_stream(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='ready' WHERE id=$1").bind(stream)
}

pub(super) fn retire_upload_stream(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1").bind(stream)
}

pub(super) fn touch_upload(upload: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET touched_at=now() WHERE id=$1").bind(upload)
}

pub(super) fn upload_active(upload: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT state='active' FROM uploads WHERE id=$1").bind(upload)
}

pub(super) fn upload_part(upload: Uuid, number: i32) -> Rows<'static, crate::app::StoredStream> {
    sqlx::query_as("SELECT s.* FROM parts p JOIN streams s ON p.stream_id=s.id WHERE p.upload_id=$1 AND p.part_number=$2 AND s.state='ready'").bind(upload).bind(number)
}

pub(super) fn upload_stream_tail(stream: Id) -> Rows<'static, Extent> {
    sqlx::query_as("SELECT * FROM extents WHERE stream_id=$1 ORDER BY offset_bytes DESC LIMIT 1")
        .bind(stream)
}

pub(super) fn lock_upload_active(upload: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT state='active' FROM uploads WHERE id=$1 FOR SHARE").bind(upload)
}

pub(super) fn remove_upload_extent(stream: Id, offset: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM extents WHERE stream_id=$1 AND offset_bytes=$2")
        .bind(stream)
        .bind(offset)
}

pub(super) fn slice_upload_extent(
    stream: Id,
    offset: i64,
    prefix: i32,
    length: i32,
) -> Statement<'static> {
    sqlx::query(
        "UPDATE extents SET source_offset=$3,length=$4 WHERE stream_id=$1 AND offset_bytes=$2",
    )
    .bind(stream)
    .bind(offset)
    .bind(prefix)
    .bind(length)
}

pub(super) fn resize_upload_stream(stream: Id, size: i64) -> Statement<'static> {
    sqlx::query("UPDATE streams SET size=$2 WHERE id=$1")
        .bind(stream)
        .bind(size)
}

pub(super) fn active_upload_metadata(upload: Uuid) -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT metadata FROM uploads WHERE id=$1 AND state='active'").bind(upload)
}

pub(super) fn upload_part_pair(
    upload: Uuid,
    number: i32,
) -> Rows<'static, crate::app::StoredStream> {
    sqlx::query_as("SELECT s.* FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=$1 AND p.part_number IN ($2,$2+1) AND s.state='ready' ORDER BY p.part_number").bind(upload).bind(number)
}

pub(super) fn begin_upload_completion<'a>(
    input: &'a crate::model::multipart::Completion<'a>,
) -> Statement<'a> {
    sqlx::query("UPDATE uploads SET state='completing',manifest_hash=$2,completion_token=$3,completion_expires_at=clock_timestamp()+interval '120 seconds',touched_at=clock_timestamp() WHERE id=$1").bind(input.upload).bind(input.hash).bind(input.token)
}

pub(super) fn abort_multipart(id: Uuid, expired: bool) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET state='aborted',touched_at=now() WHERE id=$1 AND state IN ('active','completing') AND (NOT $2 OR (state='active' AND NOT EXISTS(SELECT 1 FROM parts part JOIN stream_leases l ON l.stream_id=part.write_token WHERE part.upload_id=uploads.id AND l.expires_at>now()) AND EXISTS(SELECT 1 FROM buckets b JOIN storage_profiles p ON p.storage_id=b.storage_id WHERE b.id=uploads.bucket_id AND uploads.touched_at<now()-(left(p.multipart_idle_timeout,-1)::bigint*CASE right(p.multipart_idle_timeout,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END)*interval '1 second')))").bind(id).bind(expired)
}

pub(super) fn upload_bucket(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT bucket_id FROM uploads WHERE id=$1").bind(id)
}

pub(super) fn upload_parts_page(upload: Uuid, after: i32, limit: i64) -> Rows<'static, UploadPart> {
    sqlx::query_as("SELECT p.part_number,s.size,s.etag,s.touched_at,s.checksums FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=$1 AND p.part_number>$2 ORDER BY p.part_number LIMIT $3").bind(upload).bind(after).bind(limit)
}

pub(super) fn multipart_page<'q>(
    bucket: Uuid,
    access: &'q str,
    prefix: &'q str,
    after: &'q str,
    inclusive: bool,
    id: Option<Uuid>,
) -> Rows<'q, Upload> {
    sqlx::query_as("SELECT * FROM uploads WHERE bucket_id=$1 AND access_key=$2 AND state IN ('active','completing') AND starts_with(object_key,$3) AND (object_key>$4 OR (object_key=$4 AND ($5 OR id>$6))) ORDER BY object_key,id LIMIT 64").bind(bucket).bind(access).bind(prefix).bind(after).bind(inclusive).bind(id)
}

pub(super) fn lock_upload_extents(stream: Id, offset: i64, end: i64) -> Rows<'static, Extent> {
    sqlx::query_as("SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=GREATEST(0,$2-4194304) AND offset_bytes<$3 AND offset_bytes+length>$2 ORDER BY offset_bytes FOR UPDATE").bind(stream).bind(offset).bind(end)
}

pub(super) fn insert_upload_chunk_extent(
    stream: Id,
    offset: i64,
    length: i32,
    chunk: Id,
    source: i32,
) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) VALUES($1,$2,$3,$4,$5)").bind(stream).bind(offset).bind(length).bind(chunk).bind(source)
}

pub(super) fn insert_upload_extent<'q>(
    e: &'q Extent,
    offset: i64,
    length: i32,
    source: i32,
) -> Statement<'q> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,fragment_id,source_offset) VALUES($1,$2,$3,$4,$5,$6)").bind(e.stream_id).bind(offset).bind(length).bind(e.chunk_id).bind(e.fragment_id).bind(source)
}

pub(super) fn touch_active_upload(upload: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET touched_at=now() WHERE id=$1 AND state='active'").bind(upload)
}

pub(super) fn durable_uploads<'q>(ids: &'q [Uuid]) -> Statement<'q> {
    sqlx::query("UPDATE uploads SET durable_at=now() WHERE id=ANY($1) AND durable_at IS NULL")
        .bind(ids)
}

pub(super) fn web_upload_bucket_name(bucket: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT name FROM buckets WHERE id=$1").bind(bucket)
}

pub(super) fn web_upload_parts(upload: Uuid, after: i32) -> Rows<'static, ReceivedPart> {
    sqlx::query_as("SELECT p.part_number number,s.size::text,s.etag,s.checksums->>'sha256' sha256 FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=$1 AND p.part_number>$2 ORDER BY p.part_number LIMIT 1001").bind(upload).bind(after)
}

pub(super) fn mark_unreferenced_chunks(chunks: &[Id]) -> Statement<'_> {
    sqlx::query("UPDATE chunks c SET unreferenced_at=COALESCE(unreferenced_at,now()) WHERE id=ANY($1) AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)").bind(chunks)
}
const TRANSFER_VIEW: &str = "SELECT u.id,u.bucket_id,b.name bucket_name,u.object_key,u.state,CASE WHEN w.upload_id IS NULL THEN 's3' ELSE 'web' END source,w.user_id,COALESCE(NULLIF(a.display_name,''),NULLIF(a.username,''),c.label,'S3 application') owner,w.file_name,w.expected_size::text,w.part_size::text,w.modified_at,CASE WHEN u.state='completed' THEN COALESCE(s.size,w.expected_size,0) ELSE COALESCE((SELECT sum(quota_size) FROM parts WHERE upload_id=u.id),0) END::text received_bytes,(SELECT count(*) FROM parts WHERE upload_id=u.id) parts,CASE WHEN u.state<>'completed' THEN 'pending' WHEN u.durable_at IS NOT NULL THEN 'stored' WHEN u.output_stream IS NULL THEN 'unavailable' WHEN EXISTS(SELECT 1 FROM extents e JOIN chunks ch ON ch.id=e.chunk_id WHERE e.stream_id=u.output_stream AND NOT EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=ch.id AND l.state='ready') AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.id=ch.pack_id AND p.state='ready')) THEN 'pending' ELSE 'stored' END remote_state,u.created_at,u.touched_at,p.multipart_idle_timeout FROM uploads u JOIN buckets b ON b.id=u.bucket_id JOIN storage_profiles p ON p.storage_id=b.storage_id LEFT JOIN web_uploads w ON w.upload_id=u.id LEFT JOIN web_users a ON a.id=w.user_id LEFT JOIN s3_credentials c ON c.access_key_id=u.access_key LEFT JOIN streams s ON s.id=u.output_stream WHERE ";
pub(super) fn upload_transfers(
    admin: bool,
    user: Uuid,
    token: Option<Uuid>,
    q: &TransferFilter,
    cursor: Option<(DateTime<Utc>, Uuid)>,
    id: Option<Uuid>,
    limit: i64,
) -> sqlx::QueryBuilder<Postgres> {
    let mut sql = sqlx::QueryBuilder::<Postgres>::new(TRANSFER_VIEW);
    if admin {
        sql.push("true");
    } else {
        if let Some(token) = token {
            sql.push("EXISTS(SELECT 1 FROM token_bucket_access g WHERE g.token_id=")
                .push_bind(token);
        } else {
            sql.push("EXISTS(SELECT 1 FROM user_bucket_access g WHERE g.user_id=")
                .push_bind(user);
        }
        sql.push(" AND g.bucket_id=u.bucket_id AND 'object.write'=ANY(g.actions) AND (w.user_id=")
            .push_bind(user)
            .push(" OR 'bucket.settings'=ANY(g.actions)))");
    }
    if let Some(id) = id {
        sql.push(" AND u.id=").push_bind(id);
    } else {
        match q.state.as_deref() {
            Some("all") => {}
            Some(state) => {
                sql.push(" AND u.state=").push_bind(state);
            }
            None => {
                sql.push(" AND u.state IN ('active','completing')");
            }
        }
    }
    if let Some(bucket) = q.bucket {
        sql.push(" AND u.bucket_id=").push_bind(bucket);
    }
    if let Some(source) = &q.source {
        sql.push(if source == "web" {
            " AND w.upload_id IS NOT NULL"
        } else {
            " AND w.upload_id IS NULL"
        });
    }
    if q.own == Some(true) {
        sql.push(" AND w.user_id=").push_bind(user);
    }
    if let Some(cursor) = cursor {
        sql.push(" AND (u.created_at,u.id)<(")
            .push_bind(cursor.0)
            .push(",")
            .push_bind(cursor.1)
            .push(")");
    }
    sql.push(" ORDER BY u.created_at DESC,u.id DESC LIMIT ")
        .push_bind(limit + 1);
    sql
}

#[async_trait::async_trait]
impl MultipartStore for Backend {
    async fn pending_files(
        &self,
        storage: Option<Uuid>,
        node: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<crate::model::multipart::MultipartFile>> {
        Ok(sqlx::query_as("SELECT id,upload_id,storage_id,source_id,state FROM multipart_files WHERE node_id IS NOT DISTINCT FROM $3 AND reclaim_pending AND state='sealed' AND ($1::uuid IS NULL OR storage_id=$1) ORDER BY id LIMIT $2").bind(storage).bind(limit).bind(node).fetch_all(&self.pool).await?)
    }
    async fn clear_reclaim(&self, id: Uuid) -> Result<()> {
        sqlx::query(
            "UPDATE multipart_files SET reclaim_pending=false WHERE id=$1 AND state='sealed'",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
    async fn file_location(
        &self,
        id: Uuid,
    ) -> Result<Option<crate::model::multipart::MultipartFile>> {
        Ok(sqlx::query_as(
            "SELECT id,upload_id,storage_id,source_id,state FROM multipart_files WHERE id=$1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?)
    }
    async fn file_ranges(&self, id: Uuid) -> Result<Vec<(i64, i32)>> {
        Ok(sqlx::query_as("SELECT file_offset,size FROM fragments WHERE file_id=$1 ORDER BY file_offset LIMIT 4097").bind(id).fetch_all(&self.pool).await?)
    }
    async fn remap_file(
        &self,
        upload: Uuid,
        source: Id,
        ranges: &[crate::model::multipart::FileRemap],
    ) -> Result<()> {
        transactions::remap_multipart_file(&self.pool, upload, source, ranges).await
    }
    async fn file(&self, upload: Uuid, node: Option<Uuid>) -> Result<Uuid> {
        transactions::multipart_file(&self.pool, upload, node).await
    }
    async fn seal_files(&self, upload: Uuid) -> Result<()> {
        sqlx::query(
            "UPDATE multipart_files SET state='sealed' WHERE upload_id=$1 AND state='open'",
        )
        .bind(upload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
    async fn local_ranges(
        &self,
        stream: Id,
        after: i64,
    ) -> Result<Vec<crate::model::multipart::LocalPartRange>> {
        Ok(sqlx::query_as("SELECT e.offset_bytes,e.length,e.source_offset,CASE WHEN f.staging_storage_id IS NULL THEN e.fragment_id END AS fragment_id,f.id AS fragment_location,f.size,f.hash,f.file_id,f.file_offset,m.storage_id AS file_storage,CASE WHEN m.source_id IS NOT NULL THEN m.id END AS raw_source FROM extents e LEFT JOIN fragments f ON f.id=e.fragment_id LEFT JOIN multipart_files m ON m.id=f.file_id WHERE e.stream_id=$1 AND e.offset_bytes>=$2 ORDER BY e.offset_bytes LIMIT 64").bind(stream).bind(after).fetch_all(&self.pool).await?)
    }
    async fn unused_files(
        &self,
        storage: Option<Uuid>,
        node: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<crate::model::multipart::MultipartFile>> {
        Ok(sqlx::query_as("SELECT f.id,f.upload_id,f.storage_id,f.source_id,f.state FROM multipart_files f WHERE f.node_id IS NOT DISTINCT FROM $3 AND ($1::uuid IS NULL OR storage_id=$1) AND NOT EXISTS(SELECT 1 FROM fragments WHERE file_id=f.id) AND (f.state<>'open' OR NOT EXISTS(SELECT 1 FROM uploads WHERE id=f.upload_id AND state IN ('active','completing'))) ORDER BY f.id LIMIT $2").bind(storage).bind(limit).bind(node).fetch_all(&self.pool).await?)
    }
    async fn claim_file(&self, id: Uuid) -> Result<bool> {
        Ok(sqlx::query("UPDATE multipart_files AS f SET state='deleting' WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM fragments WHERE file_id=f.id) AND (f.state<>'open' OR NOT EXISTS(SELECT 1 FROM uploads WHERE id=f.upload_id AND state IN ('active','completing')))").bind(id).execute(&self.pool).await?.rows_affected()>0)
    }
    async fn remove_file(&self, id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM multipart_files WHERE id=$1 AND state='deleting'")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn upload(&self, id: Uuid, bucket: Uuid, key: &str, access: &str) -> Result<Upload> {
        Ok(multipart_upload(id, bucket, key, access)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchUpload))?)
    }
    async fn active(&self, upload: Uuid) -> Result<bool> {
        Ok(upload_active(upload).fetch_one(&self.pool).await?)
    }
    async fn part(&self, upload: Uuid, number: i32) -> Result<Option<crate::app::StoredStream>> {
        Ok(upload_part(upload, number)
            .fetch_optional(&self.pool)
            .await?)
    }
    async fn tail(&self, stream: Id) -> Result<Option<Extent>> {
        Ok(upload_stream_tail(stream)
            .fetch_optional(&self.pool)
            .await?)
    }
    async fn pair(
        &self,
        upload: Uuid,
        number: i32,
    ) -> Result<Option<(Value, Vec<crate::app::StoredStream>)>> {
        let Some(metadata) = active_upload_metadata(upload)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some((
            metadata,
            upload_part_pair(upload, number)
                .fetch_all(&self.pool)
                .await?,
        )))
    }
    async fn retire_work(&self, stream: Id) -> Result<()> {
        retire_upload_stream(stream).execute(&self.pool).await?;
        Ok(())
    }
    async fn retry_complete(&self, upload: Uuid, token: Uuid) -> Result<()> {
        sqlx::query("UPDATE uploads SET state='active',completion_token=NULL,completion_expires_at=NULL,touched_at=clock_timestamp() WHERE id=$1 AND completion_token=$2 AND state='completing'").bind(upload).bind(token).execute(&self.pool).await?;
        Ok(())
    }
    async fn renew_complete(
        &self,
        upload: Uuid,
        token: Uuid,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::renew_completion(&self.pool, upload, token, node).await
    }
    async fn parts(&self, upload: Uuid, after: i32, limit: i64) -> Result<Vec<UploadPart>> {
        Ok(upload_parts_page(upload, after, limit)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn uploads(
        &self,
        bucket: Uuid,
        access: &str,
        prefix: &str,
        after: &str,
        inclusive: bool,
        id: Option<Uuid>,
    ) -> Result<Vec<Upload>> {
        Ok(multipart_page(bucket, access, prefix, after, inclusive, id)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn touch(&self, upload: Uuid) -> Result<()> {
        if touch_active_upload(upload)
            .execute(&self.pool)
            .await?
            .rows_affected()
            != 1
        {
            return Err(s3s::s3_error!(NoSuchUpload).into());
        }
        Ok(())
    }
    async fn bucket_name(&self, bucket: Uuid) -> Result<String> {
        Ok(web_upload_bucket_name(bucket).fetch_one(&self.pool).await?)
    }
    async fn web_parts(&self, upload: Uuid, after: i32) -> Result<Vec<ReceivedPart>> {
        Ok(web_upload_parts(upload, after)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn create(&self, input: MultipartCreate<'_>) -> Result<(Uuid, bool)> {
        transactions::create_multipart(&self.pool, input).await
    }
    async fn begin_part(
        &self,
        authority: &Permit,
        upload: Uuid,
        number: i32,
        stream: Id,
    ) -> Result<()> {
        transactions::begin_upload_part(&self.pool, authority, upload, number, stream).await
    }
    async fn commit_part(
        &self,
        authority: &Permit,
        bucket: Uuid,
        upload: Uuid,
        number: i32,
        stream: Id,
    ) -> Result<()> {
        transactions::commit_upload_part(
            &self.pool, authority, bucket, upload, number, stream, None,
        )
        .await
    }
    async fn consume_seed(&self, input: SeedConsumption<'_>) -> Result<()> {
        transactions::consume_upload_seed(&self.pool, input).await
    }
    async fn stitch(&self, input: PartStitch) -> Result<()> {
        transactions::stitch_upload_parts(&self.pool, input).await
    }
    async fn begin_complete(&self, input: crate::model::multipart::Completion<'_>) -> Result<bool> {
        transactions::begin_upload_completion(&self.pool, input).await
    }
    async fn abort(&self, authority: &Permit, upload: Uuid) -> Result<()> {
        transactions::abort_multipart(&self.pool, authority, upload, false)
            .await
            .map(|_| ())
    }
    async fn expire(&self, upload: Uuid) -> Result<bool> {
        transactions::abort_multipart(&self.pool, &Permit::system(), upload, true).await
    }
    async fn transfers(
        &self,
        principal: &Principal,
        user: Uuid,
        filter: &TransferFilter,
        after: Option<(DateTime<Utc>, Uuid)>,
        id: Option<Uuid>,
        limit: i64,
    ) -> Result<(Vec<Transfer>, bool)> {
        transactions::upload_transfers(&self.pool, principal, user, filter, after, id, limit).await
    }
}

use crate::model::cache::{CacheBatch, CacheRelease, CacheStage, UploadCacheStore};
pub(super) fn cache_bypass(stream: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT upload_cache_bypass FROM streams WHERE id=$1").bind(stream)
}
pub(super) fn bypass_upload_cache(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET upload_cache_bypass=true WHERE id=$1").bind(stream)
}
pub(super) fn lock_cache_source(chunk: Id) -> Rows<'static, (String, Option<Id>)> {
    sqlx::query_as("SELECT state,pack_id FROM chunks WHERE id=$1 FOR UPDATE").bind(chunk)
}
pub(super) fn insert_pending_upload<'q>(
    input: &'q CacheStage<'q>,
    source: Option<Id>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO pending_uploads(chunk_id,stream_id,offset_bytes,staged_size,source_pack_id,staging_storage_id,staging_object_key,next_retry_at,node_id,encoding_policy) VALUES($1,$2,$3,$4,$5,$6,$7,now()+$8*interval '1 second',$9,$10)").bind(input.chunk.id).bind(input.stream).bind(input.offset).bind(input.size).bind(source).bind(input.storage).bind(input.key).bind(input.timeout as f64).bind(input.node.map(|n| n.0.node_id)).bind(input.encoding)
}
pub(super) fn insert_upload_pin(chunk: Id, source: Option<Id>, stream: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO cache_pins(chunk_id,pin_type,owner_id) VALUES($1,$2,$3)")
        .bind(chunk)
        .bind(if source.is_some() { "pack" } else { "upload" })
        .bind(stream)
}
pub(super) fn ready_cached_chunk(chunk: Id) -> Statement<'static> {
    sqlx::query(
        "UPDATE chunks SET state='ready',unreferenced_at=NULL,owner_stream=NULL WHERE id=$1",
    )
    .bind(chunk)
}
pub(super) fn insert_cached_extent<'q>(input: &'q CacheStage<'q>) -> Statement<'q> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES($1,$2,$3,$4)")
        .bind(input.stream)
        .bind(input.offset)
        .bind(input.chunk.raw_size)
        .bind(input.chunk.id)
}
pub(super) fn resize_cached_stream(stream: Id, size: i64) -> Statement<'static> {
    sqlx::query("UPDATE streams SET size=GREATEST(size,$2),touched_at=now() WHERE id=$1 AND state='writing'").bind(stream).bind(size)
}
pub(super) fn seal_stream_uploads(stream: Id) -> sqlx::RawSql {
    // A typed UUID allows a fresh plan in one roundtrip as the queue grows.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "UPDATE pending_uploads SET next_retry_at=LEAST(next_retry_at,now()) WHERE owner_task IS NULL AND chunk_id IN (SELECT chunk_id FROM extents WHERE stream_id='{stream}'::uuid)"
    )))
}
pub(super) fn unsealed_uploads(stream: Id) -> Rows<'static, (Id, i64, i32, String)> {
    sqlx::query_as("SELECT p.chunk_id,p.offset_bytes,c.raw_size,COALESCE(p.encoding_policy->>'pack_max_size',sp.pack_max_size) FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id JOIN storage_profiles sp ON sp.storage_id=c.storage_id WHERE p.stream_id=$1 AND p.owner_task IS NULL ORDER BY p.offset_bytes LIMIT 1026").bind(stream)
}
pub(super) fn seal_upload_chunks<'q>(ids: &'q [Id]) -> Statement<'q> {
    sqlx::query("UPDATE pending_uploads SET next_retry_at=LEAST(next_retry_at,now()) WHERE chunk_id=ANY($1) AND owner_task IS NULL").bind(ids)
}
pub(super) fn upload_cache_summary() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT jsonb_build_object('entries',count(*),'bytes',COALESCE(sum(staged_size),0),'oldest_at',min(created_at),'failed_entries',count(*) FILTER(WHERE last_error IS NOT NULL)) FROM pending_uploads",
    )
}
pub(super) fn upload_cache_pins() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT jsonb_build_object('chunk_id',p.chunk_id::text,'pin_type',p.pin_type,'owner_id',p.owner_id,'created_at',p.created_at,'last_error',u.last_error,'attempts',u.attempts,'next_retry_at',u.next_retry_at) FROM cache_pins p JOIN pending_uploads u USING(chunk_id) ORDER BY p.created_at,p.chunk_id LIMIT 100",
    )
}

pub(super) fn retryable_upload() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT p.owner_task FROM pending_uploads p JOIN tasks t ON t.id=p.owner_task WHERE p.next_retry_at<=now() AND t.task_type='upload' AND t.state='failed' ORDER BY p.next_retry_at,p.chunk_id LIMIT 1 FOR UPDATE OF t SKIP LOCKED",
    )
}
pub(super) fn queue_upload_retry(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='queued',error=NULL,updated_at=now() WHERE id=$1 AND state='failed'").bind(task)
}
fn write_delay(
    query: &mut sqlx::QueryBuilder<sqlx::Postgres>,
    created: &str,
    storage: &str,
    urgent: &[Uuid],
) {
    query.push(format!(" AND (sp.write_flush_interval=0 OR {created}+(LEAST(sp.write_flush_interval,sp.write_backlog_max_age))*interval '1 second'<=clock_timestamp()"));
    if !urgent.is_empty() {
        query.push(format!(" OR {storage} IN ("));
        let mut values = query.separated(",");
        for id in urgent {
            values.push_bind(*id);
        }
        query.push(")");
    }
    query.push(")");
}

pub(super) fn due_upload(urgent: &[Uuid]) -> sqlx::QueryBuilder<sqlx::Postgres> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT p.chunk_id,p.stream_id,p.source_pack_id,p.node_id FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id JOIN storage_profiles sp ON sp.storage_id=c.storage_id WHERE p.owner_task IS NULL AND p.next_retry_at<=clock_timestamp()",
    );
    write_delay(&mut query, "p.created_at", "c.storage_id", urgent);
    query.push(" ORDER BY CASE WHEN p.created_at+(sp.write_backlog_max_age)*interval '1 second'<=clock_timestamp() THEN 0 ELSE 1 END,p.next_retry_at,p.chunk_id LIMIT 1 FOR UPDATE OF p SKIP LOCKED");
    query
}

pub(super) fn pending_upload_segment(
    first: Id,
    stream: Option<Id>,
    source: Option<Id>,
) -> Rows<'static, (Id, i64, i32)> {
    sqlx::query_as("SELECT p.chunk_id,p.offset_bytes,c.raw_size FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE p.encoding_policy IS NOT DISTINCT FROM (SELECT encoding_policy FROM pending_uploads WHERE chunk_id=$1) AND p.owner_task IS NULL AND p.node_id IS NOT DISTINCT FROM (SELECT node_id FROM pending_uploads WHERE chunk_id=$1) AND (p.chunk_id=$1 OR ($2::uuid IS NOT NULL AND p.stream_id=$2 AND p.source_pack_id IS NOT DISTINCT FROM $3)) ORDER BY p.offset_bytes LIMIT 1026").bind(first).bind(stream).bind(source)
}
pub(super) fn claim_pending_uploads<'q>(ids: &'q [Id], task: Uuid) -> Statement<'q> {
    sqlx::query("UPDATE pending_uploads SET owner_task=$2 WHERE chunk_id=ANY($1)")
        .bind(ids)
        .bind(task)
}
pub(super) fn pause_upload_task(task: Execution) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='paused',updated_at=now() WHERE id=$1 AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())").bind(task.id)
        .bind(task.token)
}
pub(super) fn flush_candidate<'q>(detail: &'q Value, task: Uuid) -> Scalar<'q, Id> {
    sqlx::query_scalar("SELECT chunk_id FROM pending_uploads u WHERE u.node_id IS NOT DISTINCT FROM ($1->>'node_id')::uuid AND created_at<=($1->>'upper_time')::timestamptz AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=u.chunk_id) AND (owner_task IS NULL OR owner_task=$2 OR owner_task IN (SELECT id FROM tasks WHERE state IN ('paused','failed'))) ORDER BY created_at,chunk_id LIMIT 1").bind(detail).bind(task)
}
pub(super) fn flush_pending<'q>(detail: &'q Value) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE (($1->>'coordinator')::boolean IS TRUE OR node_id IS NOT DISTINCT FROM ($1->>'node_id')::uuid) AND created_at<=($1->>'upper_time')::timestamptz)").bind(detail)
}
pub(super) fn upload_is_durable(chunk: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE chunk_id=$1 AND (EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=$1 AND state='ready') OR (c.pack_id IS NOT NULL AND c.pack_id IS DISTINCT FROM u.source_pack_id)))").bind(chunk)
}
pub(super) fn upload_is_unused(chunk: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=$1)").bind(chunk)
}
pub(super) fn pending_upload(
    chunk: Id,
    timeout: u64,
) -> Rows<'static, (Option<Id>, i64, Option<Id>, bool)> {
    sqlx::query_as("SELECT stream_id,offset_bytes,source_pack_id,created_at<=now()-$2*interval '1 second' FROM pending_uploads WHERE chunk_id=$1").bind(chunk).bind(timeout as f64)
}
pub(super) fn pending_pack_uploads(pack: Id, task: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT chunk_id FROM pending_uploads WHERE source_pack_id=$1 AND (owner_task IS NULL OR owner_task=$2)").bind(pack).bind(task)
}
pub(super) fn pending_upload_mappings(
    first: Id,
    stream: Option<Id>,
    offset: i64,
    task: Uuid,
    flush: bool,
) -> Rows<'static, crate::model::Mapping> {
    sqlx::query_as("SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at,p.offset_bytes,c.pack_id FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE p.encoding_policy IS NOT DISTINCT FROM (SELECT encoding_policy FROM pending_uploads WHERE chunk_id=$1) AND p.node_id IS NOT DISTINCT FROM (SELECT node_id FROM pending_uploads WHERE chunk_id=$1) AND (p.chunk_id=$1 OR ($2::uuid IS NOT NULL AND p.stream_id=$2 AND p.offset_bytes>$3 AND p.source_pack_id IS NULL)) AND (p.owner_task IS NULL OR p.owner_task=$4 OR ($5 AND p.owner_task IN (SELECT id FROM tasks WHERE state IN ('paused','failed')))) ORDER BY p.offset_bytes LIMIT 1026").bind(first).bind(stream).bind(offset).bind(task).bind(flush)
}
pub(super) fn upload_stream_metadata(stream: Id) -> Rows<'static, (Value, String)> {
    sqlx::query_as("SELECT metadata,object_key FROM streams WHERE id=$1").bind(stream)
}
pub(super) fn lock_cache_inputs<'q>(
    ids: &'q [Id],
    task: Uuid,
    flush: bool,
) -> Rows<'q, (Id, Id, Option<Id>, Option<Id>, i64, Option<Id>)> {
    sqlx::query_as("SELECT c.id,c.current_encoding_id,c.pack_id,p.stream_id,p.offset_bytes,p.source_pack_id FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE p.chunk_id=ANY($1) AND (p.owner_task IS NULL OR p.owner_task=$2 OR ($3 AND p.owner_task IN (SELECT id FROM tasks WHERE state IN ('paused','failed')))) ORDER BY c.id FOR UPDATE OF p")
        .bind(ids).bind(task).bind(flush)
}
pub(super) fn count_upload_batch(task: Execution) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET processed=processed+1,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())").bind(task.id)
        .bind(task.token)
}
pub(super) fn unclaim_uploads(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE pending_uploads SET owner_task=NULL WHERE owner_task=$1").bind(task)
}
pub(super) fn complete_upload_task(task: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET state='completed',updated_at=now() WHERE id=$1 AND state='running'",
    )
    .bind(task)
}
pub(super) fn lock_pending_chunks<'q>(
    ids: &'q [Id],
) -> Rows<'q, (Id, Option<String>, Option<Uuid>)> {
    sqlx::query_as("SELECT c.id,u.staging_object_key,CASE WHEN c.state='ready' THEN COALESCE((SELECT e.encoding_id FROM chunk_encodings e WHERE e.encoding_id=c.current_encoding_id AND e.state='ready'),CASE WHEN EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.state='ready') THEN c.id END) END FROM chunks c LEFT JOIN pending_uploads u ON u.chunk_id=c.id WHERE c.id=ANY($1) ORDER BY c.id FOR UPDATE OF c").bind(ids)
}
pub(super) fn pending_staging_objects<'q>(ids: &'q [Id]) -> Rows<'q, (Id, Uuid, String)> {
    sqlx::query_as("SELECT chunk_id,staging_storage_id,staging_object_key FROM pending_uploads WHERE chunk_id=ANY($1) AND staging_storage_id IS NOT NULL AND staging_object_key LIKE '__mokyu/multipart/%'").bind(ids)
}
pub(super) fn can_release_pending(chunk: Id, existing_pack: bool) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=$1 AND state='ready') OR EXISTS(SELECT 1 FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE c.id=$1 AND p.state='ready' AND ($2 OR c.pack_id IS DISTINCT FROM (SELECT source_pack_id FROM pending_uploads WHERE chunk_id=$1))) OR NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=$1)").bind(chunk).bind(existing_pack)
}
pub(super) fn schedule_uploaded_pack(chunk: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO pack_maintenance(stream_id,reason) SELECT DISTINCT e.stream_id,'pack' FROM extents e JOIN streams s ON s.id=e.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE e.chunk_id=$1 AND s.state='ready' AND c.pack_id IS NULL ON CONFLICT(stream_id) DO UPDATE SET cursor=0,generation=pack_maintenance.generation+1,updated_at=now()").bind(chunk)
}
pub(super) fn delete_pending_upload(chunk: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM pending_uploads WHERE chunk_id=$1").bind(chunk)
}

pub(super) fn backoff_pending_uploads<'q>(task: Uuid, error: &'q str) -> Statement<'q> {
    sqlx::query("UPDATE pending_uploads SET owner_task=CASE WHEN (SELECT task_type FROM tasks WHERE id=$1)='upload' THEN owner_task ELSE NULL END,attempts=LEAST(attempts+1,30),last_error=$2,next_retry_at=now()+LEAST(300,power(2,LEAST(attempts,8))) * interval '1 second' WHERE owner_task=$1").bind(task).bind(error)
}
pub(super) fn lock_upload_pins<'q>(names: &'q [String], storage: Option<Uuid>) -> Statement<'q> {
    sqlx::query("SELECT u.chunk_id FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE u.staging_object_key=ANY($1) AND ($2::uuid IS NULL OR c.storage_id=$2) ORDER BY u.chunk_id FOR UPDATE").bind(names).bind(storage)
}
pub(super) fn owned_upload_pins<'q>(
    names: &'q [String],
    storage: Option<Uuid>,
) -> Scalar<'q, String> {
    sqlx::query_scalar("SELECT u.staging_object_key FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE u.staging_object_key=ANY($1) AND ($2::uuid IS NULL OR c.storage_id=$2)").bind(names).bind(storage)
}
pub(super) fn unused_pending_uploads(storage: Option<Uuid>) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT u.chunk_id FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE ($1::uuid IS NULL OR c.storage_id=$1) AND u.owner_task IS NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=u.chunk_id) ORDER BY u.chunk_id LIMIT 128").bind(storage)
}
pub(crate) fn same_references(
    a: Id,
    b: Id,
    distance: i64,
) -> sqlx::query::QueryScalar<'static, sqlx::Postgres, bool, sqlx::postgres::PgArguments> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM extents a JOIN chunks c ON c.id=a.chunk_id WHERE a.chunk_id=$1 AND (a.source_offset<>0 OR a.length<>c.raw_size OR NOT EXISTS(SELECT 1 FROM extents b JOIN chunks d ON d.id=b.chunk_id WHERE b.chunk_id=$2 AND b.stream_id=a.stream_id AND b.offset_bytes=a.offset_bytes+$3 AND b.source_offset=0 AND b.length=d.raw_size))) AND NOT EXISTS(SELECT 1 FROM extents b JOIN chunks c ON c.id=b.chunk_id WHERE b.chunk_id=$2 AND (b.source_offset<>0 OR b.length<>c.raw_size OR NOT EXISTS(SELECT 1 FROM extents a JOIN chunks d ON d.id=a.chunk_id WHERE a.chunk_id=$1 AND a.stream_id=b.stream_id AND a.offset_bytes=b.offset_bytes-$3 AND a.source_offset=0 AND a.length=d.raw_size)))")
            .bind(a).bind(b).bind(distance)
}

#[async_trait::async_trait]
impl UploadCacheStore for Backend {
    async fn bypass(&self, stream: Id) -> Result<bool> {
        Ok(cache_bypass(stream).fetch_one(&self.pool).await?)
    }
    async fn disable(&self, stream: Id) -> Result<()> {
        bypass_upload_cache(stream).execute(&self.pool).await?;
        Ok(())
    }
    async fn status(&self) -> Result<(Value, Vec<Value>)> {
        Ok((
            upload_cache_summary().fetch_one(&self.pool).await?,
            upload_cache_pins().fetch_all(&self.pool).await?,
        ))
    }
    async fn pause(&self, task: Execution) -> Result<()> {
        pause_upload_task(task).execute(&self.pool).await?;
        Ok(())
    }
    async fn metadata(&self, stream: Id) -> Result<Option<(Value, String)>> {
        Ok(upload_stream_metadata(stream)
            .fetch_optional(&self.pool)
            .await?)
    }
    async fn processed(&self, task: Execution) -> Result<()> {
        count_upload_batch(task).execute(&self.pool).await?;
        Ok(())
    }
    async fn unused(&self, storage: Option<Uuid>) -> Result<Vec<Id>> {
        Ok(unused_pending_uploads(storage)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn stage(&self, input: CacheStage<'_>) -> Result<bool> {
        transactions::cache_stage(&self.pool, input, self.single).await
    }
    async fn seal(&self, stream: Id, eof: bool) -> Result<bool> {
        transactions::cache_seal(&self.pool, stream, eof).await
    }
    async fn flush(&self, node: Option<Uuid>) -> Result<Uuid> {
        transactions::cache_flush(&self.pool, node).await
    }
    async fn backlog(&self, node: Option<Uuid>) -> Result<Value> {
        let (entries, bytes, oldest) = node_upload_backlog(node).fetch_one(&self.pool).await?;
        Ok(serde_json::json!({"entries": entries, "bytes": bytes, "oldest_at": oldest}))
    }
    async fn next_attempt(&self) -> Result<Option<DateTime<Utc>>> {
        Ok(sqlx::query_scalar("SELECT LEAST((SELECT min(next_retry_at) FROM pending_uploads WHERE owner_task IS NULL),(SELECT min(p.next_retry_at) FROM pending_uploads p JOIN tasks t ON t.id=p.owner_task WHERE t.task_type='upload' AND t.state='failed'))")
            .fetch_one(&self.pool).await?)
    }
    async fn enqueue(&self, urgent: &[Uuid]) -> Result<bool> {
        transactions::cache_enqueue(&self.pool, urgent).await
    }
    async fn batch(&self, task: Execution, detail: &Value, flush: bool) -> Result<CacheBatch> {
        transactions::cache_batch(&self.pool, task, detail, flush).await
    }
    async fn finish(&self, task: Execution) -> Result<()> {
        transactions::cache_finish(&self.pool, task).await
    }
    async fn release(&self, ids: &[Id], existing_pack: bool) -> Result<CacheRelease> {
        transactions::cache_release(&self.pool, ids, existing_pack).await
    }

    async fn pins(&self, names: &[String], storage: Option<Uuid>) -> Result<Vec<String>> {
        transactions::cache_pins(&self.pool, names, storage).await
    }
}

pub(super) fn retire_task_packs(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE packs SET state='retired',unreferenced_at=now() WHERE owner_task=$1 AND state='preparing'").bind(task)
}
pub(super) fn retire_task_encodings(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=now() WHERE owner_task=$1 AND state='uploading'").bind(task)
}
pub(super) fn release_task_inputs(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM pack_inputs WHERE task_id=$1").bind(task)
}

use crate::model::physical::{PhysicalSource, PhysicalStore, StagedUpload};
#[derive(sqlx::FromRow)]
struct InspectionMember {
    #[sqlx(flatten)]
    chunk: Chunk,
    offset_bytes: i64,
}

#[async_trait::async_trait]
impl PhysicalStore for Backend {
    async fn recover_memory(&self, boot: Uuid) -> Result<()> {
        transactions::recover_chunk_memory(&self.pool, boot).await
    }
    async fn buffer_chunk(&self, chunk: &Chunk, boot: Option<Uuid>) -> Result<()> {
        ensure!(
            buffer_chunk(chunk, boot)
                .execute(&self.pool)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        Ok(())
    }
    async fn buffered_chunks(
        &self,
        after: Id,
        before: Option<chrono::DateTime<chrono::Utc>>,
        stream: Option<Id>,
        chunk: Option<Id>,
        files_only: bool,
        urgent: &[Uuid],
    ) -> Result<Vec<Chunk>> {
        let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "SELECT c.id,c.current_encoding_id,l.encoding_id AS location_id,c.storage_id,c.hash,c.raw_size,l.standalone_size,l.encryption_algorithm,l.block_key_id,l.standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunks c JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id",
        );
        query.push(" JOIN storage_profiles sp ON sp.storage_id=c.storage_id");
        query.push(" WHERE c.state='ready' AND l.sync_pending AND l.quarantined_at IS NULL AND l.state IN ('ready','uploading') AND c.id>").push_bind(after);
        if files_only {
            query.push(" AND l.boot_id IS NULL");
            write_delay(&mut query, "l.created_at", "c.storage_id", urgent);
        }
        if let Some(before) = before {
            query.push(" AND l.created_at<=").push_bind(before);
        }
        if let Some(stream) = stream {
            query
                .push(" AND EXISTS(SELECT 1 FROM extents e WHERE e.chunk_id=c.id AND e.stream_id=")
                .push_bind(stream)
                .push(")");
        }
        if let Some(chunk) = chunk {
            query.push(" AND c.id=").push_bind(chunk);
        }
        query.push(" ORDER BY c.id LIMIT 128");
        Ok(query.build_query_as().fetch_all(&self.pool).await?)
    }
    async fn synced_chunk(&self, chunk: &Chunk, failure: Option<&str>) -> Result<()> {
        sqlx::query("UPDATE chunk_encodings SET sync_pending=CASE WHEN $2::text IS NULL THEN FALSE ELSE sync_pending END,boot_id=CASE WHEN $2::text IS NULL THEN NULL ELSE boot_id END,quarantine_code=$2,quarantined_at=CASE WHEN $2::text IS NULL THEN NULL ELSE clock_timestamp() END,quarantine_revision=quarantine_revision+CASE WHEN $2::text IS NULL THEN 0 ELSE 1 END WHERE encoding_id=$1 AND sync_pending AND quarantined_at IS NULL AND state IN ('ready','uploading')").bind(chunk.encoding_id).bind(failure).execute(&self.pool).await?;
        Ok(())
    }
    async fn buffered_status(&self) -> Result<Value> {
        let (files, bytes, oldest, volatile): (i64,i64,Option<String>,i64) = sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(c.raw_size),0) AS BIGINT),MIN(l.created_at)::text,CAST(COALESCE(SUM(CASE WHEN l.boot_id IS NOT NULL THEN 1 ELSE 0 END),0) AS BIGINT) FROM chunk_encodings l JOIN chunks c ON c.current_encoding_id=l.encoding_id WHERE c.state='ready' AND l.sync_pending AND l.state IN ('ready','uploading') AND l.quarantined_at IS NULL").fetch_one(&self.pool).await?;
        Ok(serde_json::json!({"pending":files,"bytes":bytes,"oldest":oldest,"volatile":volatile}))
    }

    async fn source_snapshot(
        &self,
        scope: &crate::model::source::SourceScope,
    ) -> Result<crate::model::source::SourceSnapshot> {
        transactions::read_source_snapshot(&self.pool, scope).await
    }
    async fn quarantines(
        &self,
        storage: Uuid,
        packed: bool,
        after: Id,
        limit: i64,
    ) -> Result<Vec<crate::model::source::QuarantinedSource>> {
        quarantines(
            &mut *self.pool.acquire().await?,
            storage,
            packed,
            after,
            limit,
        )
        .await
    }
    async fn source_update(&self, update: crate::model::source::SourceUpdate<'_>) -> Result<()> {
        transactions::source_update(&self.pool, update).await
    }

    async fn write_backlog(&self) -> Result<Vec<(Uuid, chrono::DateTime<chrono::Utc>)>> {
        Ok(sqlx::query_as("SELECT storage_id,MIN(created_at) AS oldest FROM (SELECT c.storage_id,p.created_at FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id UNION ALL SELECT r.storage_id,r.created_at FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE r.pending_upload AND r.state='ready' AND r.quarantine_code IS NULL AND EXISTS(SELECT 1 FROM raw_ranges e WHERE e.source_id=r.id) UNION ALL SELECT r.storage_id,r.created_at FROM raw_sources r WHERE r.durability IN ('buffered','volatile') AND r.state='ready' AND r.quarantine_code IS NULL AND EXISTS(SELECT 1 FROM raw_ranges e WHERE e.source_id=r.id) UNION ALL SELECT l.storage_id,l.created_at FROM chunk_encodings l WHERE l.sync_pending AND l.quarantined_at IS NULL AND l.state IN ('ready','uploading') AND EXISTS(SELECT 1 FROM chunks c JOIN extents e ON e.chunk_id=c.id WHERE c.id=l.chunk_id AND c.current_encoding_id=l.encoding_id)) waiting GROUP BY storage_id").fetch_all(&self.pool).await?)
    }
    async fn has_chunks(&self) -> Result<bool> {
        Ok(chunks_indexed().fetch_one(&self.pool).await?)
    }
    async fn indexed(&self, location: Uuid) -> Result<bool> {
        Ok(physical_indexed(location).fetch_one(&self.pool).await?)
    }

    async fn upload_pins(
        &self,
        after: Id,
        owner: Option<Uuid>,
    ) -> Result<Vec<crate::model::physical::StagedUpload>> {
        Ok(sqlx::query_as("SELECT u.chunk_id,u.staging_storage_id,u.staging_object_key,u.staged_size,c.storage_id FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE u.chunk_id>$1 AND u.node_id IS NOT DISTINCT FROM $2 ORDER BY u.chunk_id LIMIT 512").bind(after).bind(owner).fetch_all(&self.pool).await?)
    }
    async fn pending_source(&self, chunk: Id) -> Result<Option<(Uuid, String)>> {
        Ok(sqlx::query_as("SELECT staging_storage_id,staging_object_key FROM pending_uploads WHERE chunk_id=$1 AND staging_storage_id IS NOT NULL").bind(chunk).fetch_optional(&self.pool).await?)
    }
    async fn pending_only(&self, chunk: Id) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE chunk_id=$1 AND c.pack_id IS NULL AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=$1 AND state='ready'))").bind(chunk).fetch_one(&self.pool).await?)
    }
    async fn register_encoding(&self, chunk: &Chunk) -> Result<()> {
        register_encoding(chunk, None).execute(&self.pool).await?;
        Ok(())
    }
    async fn pack_chunks(&self, pack: Id) -> Result<Vec<Chunk>> {
        Ok(sqlx::query_as("SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at FROM chunks c LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE c.id IN (SELECT chunk_id FROM pack_members WHERE pack_id=$1) AND c.state='ready'").bind(pack).fetch_all(&self.pool).await?)
    }
    async fn pack_contains(&self, pack: Id, chunk: Id) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pack_members WHERE pack_id=$1 AND chunk_id=$2)",
        )
        .bind(pack)
        .bind(chunk)
        .fetch_one(&self.pool)
        .await?)
    }
    async fn inspection(&self, task: Uuid, pack: Id) -> Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar(
            "SELECT error_code FROM integrity_packs WHERE task_id=$1 AND pack_id=$2",
        )
        .bind(task)
        .bind(pack)
        .fetch_optional(&self.pool)
        .await?)
    }
    async fn inspection_members(&self, pack: Id) -> Result<Vec<(Chunk, i64)>> {
        Ok(sqlx::query_as::<_, InspectionMember>("SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at,m.offset_bytes FROM pack_members m JOIN chunks c ON c.id=m.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE m.pack_id=$1 ORDER BY m.ordinal LIMIT 4097").bind(pack).fetch_all(&self.pool).await?.into_iter().map(|m| (m.chunk,m.offset_bytes)).collect())
    }
    async fn record_inspection(&self, task: Uuid, pack: Id, code: Option<&str>) -> Result<()> {
        sqlx::query("INSERT INTO integrity_packs(task_id,pack_id,error_code) VALUES($1,$2,$3) ON CONFLICT DO NOTHING").bind(task).bind(pack).bind(code).execute(&self.pool).await?;
        Ok(())
    }
    async fn source(&self, chunk: Id, inspection: bool) -> Result<PhysicalSource> {
        physical_source(&mut *self.pool.acquire().await?, chunk, inspection).await
    }
}

pub(super) fn lock_staging(key: String) -> Statement<'static> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))").bind(key)
}
pub(super) fn staged_size<'q>(storage: Uuid, key: &'q str) -> Scalar<'q, i64> {
    sqlx::query_scalar(
        "SELECT bytes FROM staging_objects WHERE storage_id=$1 AND object_key=$2 FOR UPDATE",
    )
    .bind(storage)
    .bind(key)
}
pub(super) fn staged_usage(storage: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar(
        "SELECT COALESCE(sum(bytes),0)::bigint FROM staging_objects WHERE storage_id=$1",
    )
    .bind(storage)
}
pub(super) fn insert_staged_object<'q>(storage: Uuid, key: &'q str, bytes: i64) -> Statement<'q> {
    sqlx::query("INSERT INTO staging_objects(storage_id,object_key,bytes) VALUES($1,$2,$3)")
        .bind(storage)
        .bind(key)
        .bind(bytes)
}

use crate::model::packing::{PackCandidate, PackOutput, PackingStore};
#[derive(sqlx::FromRow)]
struct PackCandidateRow {
    #[sqlx(flatten)]
    row: Mapping,
    eligible: bool,
    unit_count: i64,
}

pub(super) fn pack_ready_inputs<'q>(ids: &'q [Id]) -> Scalar<'q, Id> {
    sqlx::query_scalar(
        "SELECT id FROM chunks WHERE id=ANY($1) AND state='ready' ORDER BY id FOR UPDATE",
    )
    .bind(ids)
}
pub(super) fn insert_pack_inputs<'q>(task: Uuid, ids: &'q [Id]) -> Statement<'q> {
    sqlx::query("INSERT INTO pack_inputs(task_id,chunk_id) SELECT $1,unnest($2::uuid[]) ON CONFLICT(task_id,chunk_id) DO NOTHING").bind(task).bind(ids)
}
pub(super) fn lock_pack_policy(key: String) -> Statement<'static> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))").bind(key)
}
pub(super) fn active_pack_task<'q>(policy: &'q str) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='pack' AND detail->>'policy'=$1 AND state IN ('queued','running') LIMIT 1").bind(policy)
}
pub(super) fn pack_storages(policy: Option<&str>) -> Rows<'_, (Uuid, String, i64)> {
    sqlx::query_as(
        "SELECT s.id,s.type,sp.revision FROM storages s JOIN storage_profiles sp ON sp.storage_id=s.id WHERE s.enabled AND CASE WHEN $1 IN ('pack','repack') THEN sp.pack_enabled WHEN $1='range' THEN sp.pack_range_enabled ELSE true END ORDER BY s.created_at,s.id",
    ).bind(policy)
}
pub(super) fn scope_pack_task<'q>(
    child: Uuid,
    parent: Uuid,
    scope: &'q str,
    node: Option<Uuid>,
) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET parent_task_id=$2,batch_total=1,execution_scope=$3,target_node_id=$4 WHERE id=$1").bind(child).bind(parent).bind(scope).bind(node)
}
pub(super) fn pack_parent_total(task: Uuid, count: i64) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET batch_total=$2,state=CASE WHEN $2=0 THEN 'completed' ELSE state END WHERE id=$1").bind(task).bind(count)
}
pub(super) fn active_unpack_task(all: String, pack: Option<String>) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='unpack' AND state IN ('queued','running','paused','failed') AND detail->>'all'=$1 AND detail->>'pack_id' IS NOT DISTINCT FROM $2 LIMIT 1").bind(all).bind(pack)
}
pub(super) fn unpack_storage(pack: Id) -> Rows<'static, (Uuid, String, i64)> {
    sqlx::query_as("SELECT s.id,s.type,sp.revision FROM storages s JOIN storage_profiles sp ON sp.storage_id=s.id JOIN packs p ON p.storage_id=s.id WHERE p.id=$1").bind(pack)
}
pub(super) fn insert_pack_member(
    pack: Id,
    ordinal: i32,
    chunk: Id,
    offset: i64,
) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO pack_members(pack_id,ordinal,chunk_id,offset_bytes) VALUES($1,$2,$3,$4)",
    )
    .bind(pack)
    .bind(ordinal)
    .bind(chunk)
    .bind(offset)
}
pub(super) fn lock_pack_layout<'q>(ids: &'q [Id]) -> Rows<'q, (Id, Option<Id>)> {
    sqlx::query_as(
        "SELECT id,pack_id FROM chunks WHERE id=ANY($1) AND state='ready' ORDER BY id FOR UPDATE",
    )
    .bind(ids)
}
pub(super) fn pack_layout_bytes<'q>(ids: &'q [Id]) -> Scalar<'q, i64> {
    sqlx::query_scalar("SELECT COALESCE(sum(bytes),0)::bigint FROM (SELECT standalone_size bytes FROM chunk_encodings WHERE chunk_id=ANY($1) AND state='ready' UNION ALL SELECT stored_size FROM packs WHERE id IN (SELECT pack_id FROM chunks WHERE id=ANY($1))) sources").bind(ids)
}
pub(super) fn pack_added_bytes(task: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT COALESCE(sum(bytes),0)::bigint FROM (SELECT stored_size bytes FROM packs WHERE owner_task=$1 AND state='preparing' UNION ALL SELECT standalone_size FROM chunk_encodings WHERE owner_task=$1 AND state='uploading') sources").bind(task)
}
pub(super) fn pack_is_upload(task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT task_type IN ('upload','cache_flush') FROM tasks WHERE id=$1")
        .bind(task)
}
pub(super) fn pack_is_range(task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT task_type='pack' AND detail->>'policy'='range' FROM tasks WHERE id=$1",
    )
    .bind(task)
}
pub(super) fn pack_exclusive<'q>(
    ids: &'q [Id],
    cooldown: f64,
    window: f64,
    flush: f64,
) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM chunks c CROSS JOIN mokyu_meta g WHERE c.id=ANY($1) AND ((SELECT count(*) FROM extents WHERE chunk_id=c.id)<>1 OR NOT COALESCE(c.repack_after<=now(),true) OR (c.range_split_at IS NOT NULL AND (g.access_coverage_since IS NULL OR g.access_coverage_since>date_trunc('hour',now()-$3*interval '1 second') OR g.access_flushed_at IS NULL OR g.access_flushed_at<now()-$4*interval '1 second' OR EXISTS(SELECT 1 FROM chunk_access_windows w WHERE w.chunk_id=c.id AND w.range_reads>0 AND w.window_start>=date_trunc('hour',now()-$3*interval '1 second')))) OR (c.split_at IS NOT NULL AND c.reference_changed_at>now()-$2*interval '1 second')))").bind(ids).bind(cooldown).bind(window).bind(flush)
}
pub(super) fn publish_prepared_pack(pack: Id) -> Statement<'static> {
    sqlx::query("UPDATE packs SET state='ready',unreferenced_at=NULL,stored_at=clock_timestamp() WHERE id=$1 AND state='preparing'").bind(pack)
}
pub(super) fn retire_pack_encodings<'q>(ids: &'q [Id]) -> Statement<'q> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=now() WHERE chunk_id=ANY($1) AND state='ready'").bind(ids)
}
pub(super) fn publish_chunk_encoding<'q>(chunk: &'q Chunk) -> Statement<'q> {
    sqlx::query("UPDATE chunks SET current_encoding_id=$2,standalone_size=$3,standalone_compressed=$4,nonce=$5,encryption_algorithm=$6,block_key_id=$7 WHERE id=$1 AND state='ready'").bind(chunk.id).bind(chunk.encoding_id).bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(&chunk.algorithm).bind(chunk.block_key_id)
}
pub(super) fn pack_has_references<'q>(ids: &'q [Id]) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents WHERE chunk_id=ANY($1))").bind(ids)
}
pub(super) fn fail_dead_pack_chunks<'q>(ids: &'q [Id]) -> Statement<'q> {
    sqlx::query("UPDATE chunks SET state='failed',pack_id=NULL,unreferenced_at=COALESCE(unreferenced_at,now()) WHERE id=ANY($1)").bind(ids)
}
pub(super) fn schedule_repacked_streams<'q>(ids: &'q [Id], cooldown: f64) -> Statement<'q> {
    sqlx::query("INSERT INTO pack_maintenance(stream_id,reason,next_check_at) SELECT DISTINCT e.stream_id,'repack',now()+$2*interval '1 second' FROM extents e JOIN streams s ON s.id=e.stream_id WHERE e.chunk_id=ANY($1) AND s.state='ready' ON CONFLICT(stream_id) DO UPDATE SET generation=pack_maintenance.generation+1,cursor=0,reason='repack',updated_at=now(),next_check_at=EXCLUDED.next_check_at").bind(ids).bind(cooldown)
}
pub(super) fn retire_replaced_packs(ids: Vec<Id>) -> Statement<'static> {
    sqlx::query("UPDATE packs p SET state='retired',unreferenced_at=now() WHERE id=ANY($1) AND state='ready' AND NOT EXISTS(SELECT 1 FROM chunks WHERE pack_id=p.id AND state='ready')").bind(ids)
}
pub(super) fn record_pack_rewrite(task: Uuid, detail: Value) -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET detail=detail||jsonb_build_object('last_rewrite',$2::jsonb) WHERE id=$1",
    )
    .bind(task)
    .bind(detail)
}
pub(super) fn mark_range_checked(pack: Id) -> Statement<'static> {
    sqlx::query("UPDATE packs SET range_checked_at=now() WHERE id=$1").bind(pack)
}
pub(super) fn record_range_result(
    task: Uuid,
    cursor: String,
    processed: i64,
    decision: Value,
) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET cursor=$2,processed=processed+$3,detail=detail||jsonb_build_object('last_range',$4::jsonb,'evaluated',COALESCE((detail->>'evaluated')::bigint,0)+1),updated_at=now() WHERE id=$1 AND state='running'").bind(task).bind(cursor).bind(processed).bind(decision)
}

#[async_trait::async_trait]
impl PackingStore for Backend {
    async fn in_storage(&self, pack: Id, storage: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM packs WHERE id=$1 AND storage_id=$2 AND state='ready')",
        )
        .bind(pack)
        .bind(storage)
        .fetch_one(&self.pool)
        .await?)
    }

    async fn scoped_page(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        after: Id,
        limit: i64,
        state: Option<&str>,
    ) -> Result<Vec<PackSummary>> {
        transactions::scoped_pack_page(&self.pool, principal, scope, after, limit, state).await
    }
    async fn scoped_detail(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        id: Id,
        after: Id,
        limit: i64,
    ) -> Result<(PackSummary, Vec<PackMember>, bool)> {
        transactions::scoped_pack_detail(&self.pool, principal, scope, id, after, limit).await
    }
    async fn scoped_objects(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        id: Id,
        cursor: Option<(Uuid, String)>,
    ) -> Result<Vec<PackObject>> {
        transactions::scoped_pack_objects(&self.pool, principal, scope, id, cursor).await
    }

    async fn page(&self, after: Id, limit: i64) -> Result<Vec<Value>> {
        let rows:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(p)||jsonb_build_object('id',p.id::text) FROM packs p WHERE id>$1 ORDER BY id LIMIT $2")
        .bind(after).bind(limit).fetch_all(&self.pool).await?;
        Ok(rows)
    }
    async fn detail(&self, id: Id) -> Result<(Value, Vec<Value>)> {
        let pack: Value = sqlx::query_scalar(
            "SELECT to_jsonb(p)||jsonb_build_object('id',p.id::text) FROM packs p WHERE id=$1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        let members:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('ordinal',m.ordinal,'chunk_id',m.chunk_id::text,'offset',m.offset_bytes,'raw_size',c.raw_size,'current',c.pack_id=m.pack_id) FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=$1 ORDER BY ordinal LIMIT 4096").bind(id).fetch_all(&self.pool).await?;
        Ok((pack, members))
    }

    async fn rewrite_counts(
        &self,
        storage: Uuid,
        algorithm: &str,
        key: Option<Uuid>,
    ) -> Result<(i64, i64)> {
        Ok(rewrite_counts(storage, algorithm, key)
            .fetch_one(&self.pool)
            .await?)
    }
    async fn start_rewrite(&self, job: &RewriteTask, node: Option<Uuid>) -> Result<(Uuid, bool)> {
        transactions::start_rewrite(&self.pool, job, node, self.single).await
    }
    async fn rewrite_candidates(
        &self,
        after: Id,
        job: &RewriteTask,
    ) -> Result<(Option<Chunk>, Option<(Id, Id)>)> {
        let loose = rewrite_loose_candidate(&self.pool, after, job).await?;
        let pack = rewrite_pack_candidate(&self.pool, after, job).await?;
        Ok((loose, pack))
    }
    async fn rewrite_pack_inputs(&self, pack: Id) -> Result<(Pack, Vec<Chunk>, Vec<Id>)> {
        let old = rewrite_pack(pack).fetch_one(&self.pool).await?;
        let members = rewrite_members(pack).fetch_all(&self.pool).await?;
        anyhow::ensure!(
            members.len() == old.member_count as usize,
            "pack member count changed during rewrite"
        );
        let current = rewrite_current_members(pack).fetch_all(&self.pool).await?;
        Ok((old, members, current))
    }
    async fn commit_rewrite_chunk(
        &self,
        task: Uuid,
        token: Uuid,
        source: &Chunk,
        physical: &Chunk,
    ) -> Result<()> {
        transactions::commit_rewrite_chunk(&self.pool, task, token, source, physical).await
    }
    async fn commit_rewrite_pack(
        &self,
        task: Uuid,
        token: Uuid,
        old: &Pack,
        new: &Pack,
        current: &[Id],
        members: &[Chunk],
        first: Id,
    ) -> Result<()> {
        transactions::commit_rewrite_pack(
            &self.pool, task, token, old, new, current, members, first,
        )
        .await
    }
    async fn complete_rewrite(&self, task: Uuid, token: Uuid, after: Id) -> Result<()> {
        complete_rewrite(task, token, after)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn pack_totals(&self) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT jsonb_build_object('state',state,'count',count(*),'raw_bytes',COALESCE(sum(raw_size),0),'stored_bytes',COALESCE(sum(stored_size),0)) FROM packs GROUP BY state").fetch_all(&self.pool).await?)
    }
    async fn unpack_totals(&self, pack: Option<Id>) -> Result<(i64, i64)> {
        Ok(sqlx::query_as("SELECT count(*),COALESCE(sum(raw_size),0)::bigint FROM packs WHERE state='ready' AND ($1::uuid IS NULL OR id=$1)").bind(pack).fetch_one(&self.pool).await?)
    }
    async fn unpack_identity(&self, pack: Option<Id>) -> Result<(String, String)> {
        Ok(sqlx::query_as("SELECT coalesce(max(id::text COLLATE \"C\"),''),coalesce(bit_xor(hashtextextended(id::text,0)),0)::text FROM packs WHERE state='ready' AND ($1::uuid IS NULL OR id=$1)").bind(pack).fetch_one(&self.pool).await?)
    }
    async fn preparing_packs(&self) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM packs WHERE state='preparing')")
                .fetch_one(&self.pool)
                .await?,
        )
    }
    async fn ready_encoding(&self, chunk: Id) -> Result<Option<(Id, Uuid, i32)>> {
        pack_work::ready_encoding(&self.pool, chunk).await
    }

    async fn prepare_encoding(&self, physical: &Chunk, task: Execution) -> Result<Id> {
        sqlx::query_scalar("INSERT INTO chunk_encodings(chunk_id,storage_id,standalone_compressed,state,created_at,unreferenced_at,owner_task,encryption_algorithm,block_key_id) SELECT $1,$2,false,'uploading',$3,now(),$4,$5,$6 FROM (SELECT id FROM tasks WHERE id=$4 AND state='running' AND lease_token=$7 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()) FOR UPDATE) owner RETURNING encoding_id").bind(physical.id).bind(physical.storage_id).bind(physical.created_at).bind(task.id).bind(&physical.algorithm).bind(physical.block_key_id).bind(task.token).fetch_optional(&self.pool).await?.context("task execution expired")
    }
    async fn encoding_metadata(&self, physical: &Chunk) -> Result<()> {
        sqlx::query("UPDATE chunk_encodings SET standalone_size=$2,standalone_compressed=$3,nonce=$4,nonce_id=$5 WHERE encoding_id=$1").bind(physical.encoding_id).bind(physical.stored_size).bind(physical.compressed).bind(&physical.nonce).bind(crate::content::nonce::id(physical.nonce.as_deref())).execute(&self.pool).await?;
        Ok(())
    }
    async fn prepare_pack(
        &self,
        storage: Uuid,
        algorithm: &str,
        key: Option<Uuid>,
        bytes: i64,
        count: i32,
        task: Execution,
    ) -> Result<Pack> {
        Ok(sqlx::query_as("INSERT INTO packs(storage_id,encryption_algorithm,block_key_id,raw_size,member_count,state,unreferenced_at,owner_task) SELECT $1,$2,$3,$4,$5,'preparing',now(),$6 FROM (SELECT id FROM tasks WHERE id=$6 AND state='running' AND lease_token=$7 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()) FOR UPDATE) owner RETURNING *").bind(storage).bind(algorithm).bind(key).bind(bytes).bind(count).bind(task.id).bind(task.token).fetch_one(&self.pool).await?)
    }
    async fn discard_pack(&self, pack: Id) -> Result<()> {
        sqlx::query("UPDATE packs SET state='retired' WHERE id=$1")
            .bind(pack)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn pack_metadata(&self, pack: &Pack) -> Result<()> {
        sqlx::query(
            "UPDATE packs SET stored_size=$2,pack_compressed=$3,nonce=$4,digest=$5,nonce_id=$6 WHERE id=$1",
        )
        .bind(pack.id)
        .bind(pack.stored_size)
        .bind(pack.compressed)
        .bind(&pack.nonce)
        .bind(&pack.digest)
        .bind(crate::content::nonce::id(pack.nonce.as_deref()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }
    async fn pack_has_pending(&self, pack: Id) -> Result<bool> {
        pack_work::pack_has_pending(&self.pool, pack).await
    }

    async fn pack_layout(&self, pack: Id) -> Result<Vec<Mapping>> {
        pack_work::pack_layout(&self.pool, pack).await
    }

    async fn pack_has_writers(&self, pack: Id) -> Result<bool> {
        pack_work::pack_has_writers(&self.pool, pack).await
    }

    async fn pack_raw_size(&self, pack: Id) -> Result<i64> {
        pack_work::pack_raw_size(&self.pool, pack).await
    }

    async fn chunk_has_references(&self, chunk: Id) -> Result<bool> {
        pack_work::chunk_has_references(&self.pool, chunk).await
    }

    async fn pack_stored_size(&self, pack: Id) -> Result<i64> {
        pack_work::pack_stored_size(&self.pool, pack).await
    }

    async fn pack_candidates(
        &self,
        stream: Id,
        offset: i64,
        cooldown: f64,
    ) -> Result<Vec<PackCandidate>> {
        pack_work::pack_candidates(&self.pool, stream, offset, cooldown).await
    }

    async fn pack_member_count(&self, pack: Id) -> Result<i64> {
        pack_work::pack_member_count(&self.pool, pack).await
    }

    async fn pack_stream_metadata(&self, stream: Id) -> Result<(Value, String)> {
        Ok(
            sqlx::query_as("SELECT metadata,object_key FROM streams WHERE id=$1")
                .bind(stream)
                .fetch_one(&self.pool)
                .await?,
        )
    }
    async fn physical_bytes(&self, ids: &[Id]) -> Result<i64> {
        pack_work::physical_bytes(&self.pool, ids).await
    }

    async fn pause_pack_task(&self, task: Execution) -> Result<()> {
        sqlx::query(
            "UPDATE tasks SET state='paused',updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())",
        )
        .bind(task.id)
        .bind(task.token).execute(&self.pool)
        .await?;
        Ok(())
    }
    async fn next_pack(
        &self,
        after: Id,
        pack: Option<Id>,
        upper: Option<DateTime<Utc>>,
        storage: Option<Uuid>,
    ) -> Result<Option<Id>> {
        pack_work::next_pack(&self.pool, after, pack, upper, storage).await
    }

    async fn next_pack_stream(
        &self,
        after: Id,
        offset: i64,
        upper: DateTime<Utc>,
        storage: Option<Uuid>,
    ) -> Result<Option<Id>> {
        pack_work::next_pack_stream(&self.pool, after, offset, upper, storage).await
    }

    async fn advance_pack_task(
        &self,
        task: Execution,
        next: &Option<String>,
        processed: i64,
    ) -> Result<()> {
        if self.single && next.is_none() {
            return transactions::advance_pack_storage(&self.pool, task, processed).await;
        }
        sqlx::query("UPDATE tasks SET cursor=$2,processed=processed+$3,state=CASE WHEN $2::text IS NULL THEN 'completed' ELSE state END,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$4 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())").bind(task.id).bind(next).bind(processed).bind(task.token).execute(&self.pool).await?;
        Ok(())
    }
    async fn next_pack_change(
        &self,
        cutoff: DateTime<Utc>,
        policy: &str,
        storage: Option<Uuid>,
    ) -> Result<Option<(Id, i64)>> {
        pack_work::next_pack_change(&self.pool, cutoff, policy, storage).await
    }

    async fn finish_pack_change(&self, task: Execution, pack: Id, generation: i64) -> Result<()> {
        pack_work::finish_pack_change(&self.pool, task, pack, generation).await
    }

    async fn next_pack_queue(
        &self,
        cutoff: DateTime<Utc>,
        policy: &str,
        storage: Option<Uuid>,
    ) -> Result<Option<(Id, i64, i64)>> {
        pack_work::next_pack_queue(&self.pool, cutoff, policy, storage).await
    }

    async fn stream_repack_after(
        &self,
        stream: Id,
        cooldown: f64,
    ) -> Result<Option<DateTime<Utc>>> {
        pack_work::stream_repack_after(&self.pool, stream, cooldown).await
    }

    async fn defer_pack_queue(
        &self,
        task: Execution,
        stream: Id,
        generation: i64,
        later: DateTime<Utc>,
    ) -> Result<()> {
        pack_work::defer_pack_queue(&self.pool, task, stream, generation, later).await
    }

    async fn finish_pack_queue(&self, task: Execution, stream: Id, generation: i64) -> Result<()> {
        pack_work::finish_pack_queue(&self.pool, task, stream, generation).await
    }

    async fn advance_pack_queue(
        &self,
        task: Execution,
        stream: Id,
        generation: i64,
        cursor: i64,
    ) -> Result<()> {
        pack_work::advance_pack_queue(&self.pool, task, stream, generation, cursor).await
    }

    async fn pin_pack_inputs(&self, task: Execution, rows: &[Mapping]) -> Result<()> {
        transactions::pin_pack_inputs(&self.pool, task, rows).await
    }

    async fn start_pack(&self, policy: &str, automatic: bool, node: Option<Uuid>) -> Result<Value> {
        transactions::start_pack(&self.pool, self.single, policy, automatic, node).await
    }
    async fn start_unpack(&self, pack: Option<Id>, all: bool, node: Option<Uuid>) -> Result<Value> {
        transactions::start_unpack(&self.pool, self.single, pack, all, node).await
    }
    async fn insert_pack_members(&self, pack: Id, rows: &[Mapping]) -> Result<()> {
        transactions::insert_pack_members(&self.pool, pack, rows).await
    }
    async fn commit_pack_outputs(
        &self,
        task: Execution,
        rows: &[Mapping],
        outputs: &[PackOutput],
        split: bool,
        settings: &crate::content::pack::Config,
        flush_seconds: f64,
    ) -> Result<()> {
        transactions::commit_pack_outputs(
            &self.pool,
            task,
            rows,
            outputs,
            split,
            settings,
            flush_seconds,
        )
        .await
    }
    async fn same_references(&self, a: Id, b: Id, distance: i64) -> Result<bool> {
        Ok(same_references(a, b, distance)
            .fetch_one(&self.pool)
            .await?)
    }
    async fn next_range_pack(
        &self,
        storage: Uuid,
        cutoff: DateTime<Utc>,
        window: f64,
        minimum: i64,
    ) -> Result<Option<Id>> {
        pack_work::next_range_pack(&self.pool, storage, cutoff, window, minimum).await
    }

    async fn complete_range_task(&self, task: Execution) -> Result<()> {
        if self.single {
            return transactions::advance_pack_storage(&self.pool, task, 0).await;
        }
        sqlx::query(
            "UPDATE tasks SET state='completed',updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())",
        )
        .bind(task.id)
        .bind(task.token).execute(&self.pool)
        .await?;
        Ok(())
    }
    async fn range_pack_blocked(&self, pack: Id) -> Result<bool> {
        pack_work::range_pack_blocked(&self.pool, pack).await
    }

    async fn pack_range_totals(&self, pack: Id, window: f64) -> Result<(i64, i64, i64)> {
        pack_work::pack_range_totals(&self.pool, pack, window).await
    }

    async fn pack_member_hits(&self, pack: Id, window: f64) -> Result<Vec<(Id, i64)>> {
        pack_work::pack_member_hits(&self.pool, pack, window).await
    }

    async fn pack_dimensions(&self, pack: Id) -> Result<(i64, i64, i32)> {
        pack_work::pack_dimensions(&self.pool, pack).await
    }

    async fn range_pack_layout(&self, pack: Id) -> Result<Vec<Mapping>> {
        pack_work::range_pack_layout(&self.pool, pack).await
    }

    async fn defer_range_repack(
        &self,
        stream: Id,
        window: f64,
        flush: f64,
        retry: f64,
        offset: i64,
    ) -> Result<()> {
        pack_work::defer_range_repack(&self.pool, stream, window, flush, retry, offset).await
    }

    async fn record_range(
        &self,
        task: Execution,
        id: Id,
        processed: usize,
        decision: Value,
    ) -> Result<()> {
        transactions::record_range(&self.pool, task, id, processed, decision).await
    }
}

use crate::content::pack::Pack;

use crate::model::Mapping;

use crate::model::packing::RewriteTask;
pub(super) fn resolve_block_key<'q>(name: &'q str) -> Rows<'q, (Uuid, String, String)> {
    sqlx::query_as("SELECT id,name,key_algorithm FROM managed_keys WHERE (id=$1::uuid OR name=$2) AND purpose='block' AND revoked_at IS NULL").bind(name.parse::<Uuid>().ok()).bind(name)
}
pub(super) fn rewrite_counts<'q>(
    storage: Uuid,
    algorithm: &'q str,
    key: Option<Uuid>,
) -> Rows<'q, (i64, i64)> {
    sqlx::query_as("SELECT count(*) FILTER (WHERE c.pack_id IS NULL AND (l.encoding_id IS NULL OR l.encoding_fingerprint IS DISTINCT FROM digest(concat_ws(':',$3::uuid::text,$2::text,l.standalone_compressed::text,coalesce(sp.compression_enabled::text,''),coalesce(sp.compression_strategy,''),coalesce(sp.compression_level::text,''),coalesce(sp.compression_sample_level::text,''),coalesce(sp.compression_min_savings_percent::text,''),coalesce(sp.compression_min_savings_bytes::text,''),'standalone'),'sha256') OR ((NOT sp.compression_enabled OR sp.compression_strategy='never') AND l.standalone_compressed))), count(*) FILTER (WHERE c.pack_id IS NOT NULL AND (p.pack_fingerprint IS DISTINCT FROM digest(concat_ws(':',$3::uuid::text,$2::text,p.pack_compressed::text,p.member_count::text,coalesce(sp.compression_enabled::text,''),coalesce(sp.compression_strategy,''),coalesce(sp.compression_level::text,''),coalesce(sp.compression_sample_level::text,''),coalesce(sp.compression_min_savings_percent::text,''),coalesce(sp.compression_min_savings_bytes::text,''),coalesce(sp.pack_max_size,''),coalesce(sp.pack_range_enabled::text,''),coalesce(sp.pack_range_min_downloads::text,''),coalesce(sp.pack_range_min_savings_percent::text,''),coalesce(sp.pack_range_min_savings_bytes,''),'pack'),'sha256') OR ((NOT sp.compression_enabled OR sp.compression_strategy='never') AND p.pack_compressed))) FROM chunks c JOIN storage_profiles sp ON sp.storage_id=c.storage_id LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready' WHERE c.storage_id=$1 AND c.state='ready'").bind(storage).bind(algorithm).bind(key)
}
pub(super) fn lock_rewrite_storage(storage: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM storages WHERE id=$1 AND enabled FOR UPDATE").bind(storage)
}
pub(super) fn active_rewrite(storage: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='rewrite' AND parent_task_id IS NULL AND detail->>'storage_id'=$1 AND state IN ('queued','running','paused') LIMIT 1").bind(storage.to_string())
}
pub(super) fn rewrite_upper(storage: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT COALESCE((SELECT id FROM chunks WHERE storage_id=$1 ORDER BY id DESC LIMIT 1),'00000000-0000-0000-0000-000000000000'::uuid)").bind(storage)
}
pub(super) fn rewrite_is_local(storage: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT type='local' FROM storages WHERE id=$1").bind(storage)
}
pub(super) fn scope_rewrite<'q>(
    id: Uuid,
    scope: &'q str,
    node: Option<Uuid>,
    shards: i64,
) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET execution_scope=$2,target_node_id=$3,batch_total=$4 WHERE id=$1")
        .bind(id)
        .bind(scope)
        .bind(node)
        .bind(shards)
}
pub(super) fn scope_rewrite_child<'q>(
    id: Uuid,
    parent: Uuid,
    scope: &'q str,
    node: Option<Uuid>,
    shards: i64,
) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET parent_task_id=$2,execution_scope=$3,target_node_id=$4,batch_total=$5 WHERE id=$1").bind(id).bind(parent).bind(scope).bind(node).bind(shards)
}
pub(super) fn complete_empty_rewrite(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='completed',batch_completed=0,updated_at=now() WHERE id=$1")
        .bind(id)
}
pub(super) fn active_block_key(id: Uuid) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT name,key_algorithm FROM managed_keys WHERE id=$1 AND purpose='block' AND revoked_at IS NULL").bind(id)
}
pub(super) async fn rewrite_loose_candidate(
    db: impl sqlx::PgExecutor<'_>,
    after: Id,
    job: &RewriteTask,
) -> Result<Option<Chunk>> {
    let mut query = sqlx::QueryBuilder::<Postgres>::new(
        "SELECT c.id,l.encoding_id AS current_encoding_id,l.encoding_id AS location_id,c.storage_id,c.hash,c.raw_size,l.standalone_size AS standalone_size,l.encryption_algorithm AS encryption_algorithm,l.block_key_id,l.standalone_compressed AS standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunks c JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' JOIN storage_profiles sp ON sp.storage_id=c.storage_id WHERE c.id>",
    );
    query.push_bind(after).push(" AND c.id<=");
    query
        .push_bind(job.upper_chunk_id)
        .push(" AND c.storage_id=");
    query.push_bind(job.storage_id).push(" AND c.state='ready' AND c.pack_id IS NULL AND (l.encoding_fingerprint IS DISTINCT FROM digest(concat_ws(':',");
    query.push_bind(job.target_key_id).push("::uuid::text,");
    query.push_bind(&job.target_algorithm).push("::text,l.standalone_compressed::text,coalesce(sp.compression_enabled::text,''),coalesce(sp.compression_strategy,''),coalesce(sp.compression_level::text,''),coalesce(sp.compression_sample_level::text,''),coalesce(sp.compression_min_savings_percent::text,''),coalesce(sp.compression_min_savings_bytes::text,''),'standalone'),'sha256') OR ((NOT sp.compression_enabled OR sp.compression_strategy='never') AND l.standalone_compressed))");
    if job.shards > 1 {
        query
            .push(" AND ((hashtextextended(c.id::text,0) & 9223372036854775807) % ")
            .push_bind(job.shards)
            .push(")=")
            .push_bind(job.shard);
    }
    query.push(" ORDER BY c.id LIMIT 1");
    Ok(query.build_query_as().fetch_optional(db).await?)
}
pub(super) async fn rewrite_pack_candidate(
    db: impl sqlx::PgExecutor<'_>,
    after: Id,
    job: &RewriteTask,
) -> Result<Option<(Id, Id)>> {
    let mut query = sqlx::QueryBuilder::<Postgres>::new(
        "SELECT c.id,p.id FROM packs p JOIN storage_profiles sp ON sp.storage_id=p.storage_id JOIN LATERAL (SELECT id,state,storage_id FROM chunks WHERE pack_id=p.id AND storage_id=p.storage_id AND state='ready' ORDER BY id LIMIT 1) c ON true WHERE p.state='ready' AND c.storage_id=",
    );
    query.push_bind(job.storage_id).push(
        " AND c.state='ready' AND (p.pack_fingerprint IS DISTINCT FROM digest(concat_ws(':',",
    );
    query.push_bind(job.target_key_id).push("::uuid::text,");
    query.push_bind(&job.target_algorithm).push("::text,p.pack_compressed::text,p.member_count::text,coalesce(sp.compression_enabled::text,''),coalesce(sp.compression_strategy,''),coalesce(sp.compression_level::text,''),coalesce(sp.compression_sample_level::text,''),coalesce(sp.compression_min_savings_percent::text,''),coalesce(sp.compression_min_savings_bytes::text,''),coalesce(sp.pack_max_size,''),coalesce(sp.pack_range_enabled::text,''),coalesce(sp.pack_range_min_downloads::text,''),coalesce(sp.pack_range_min_savings_percent::text,''),coalesce(sp.pack_range_min_savings_bytes,''),'pack'),'sha256') OR ((NOT sp.compression_enabled OR sp.compression_strategy='never') AND p.pack_compressed)) AND c.id>");
    query.push_bind(after).push(" AND c.id<=");
    query.push_bind(job.upper_chunk_id).push("");
    if job.shards > 1 {
        query
            .push(" AND ((hashtextextended(c.id::text,0) & 9223372036854775807) % ")
            .push_bind(job.shards)
            .push(")=")
            .push_bind(job.shard);
    }
    query.push(" ORDER BY c.id LIMIT 1");
    Ok(query.build_query_as().fetch_optional(db).await?)
}
pub(super) fn lock_rewrite_chunk(id: Id) -> Scalar<'static, Option<Id>> {
    sqlx::query_scalar("SELECT pack_id FROM chunks WHERE id=$1 AND state='ready' FOR UPDATE")
        .bind(id)
}
pub(super) fn retire_rewrite_encoding(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=now() WHERE encoding_id=$1 AND state='ready'").bind(id)
}
pub(super) fn publish_rewrite_encoding<'q>(physical: &'q Chunk) -> Statement<'q> {
    sqlx::query("UPDATE chunk_encodings SET standalone_size=$2,standalone_compressed=$3,nonce=$4,nonce_id=$5,state='ready',unreferenced_at=NULL,stored_at=clock_timestamp(),owner_task=NULL WHERE encoding_id=$1 AND state='uploading'").bind(physical.encoding_id).bind(physical.stored_size).bind(physical.compressed).bind(&physical.nonce).bind(crate::content::nonce::id(physical.nonce.as_deref()))
}
pub(super) fn publish_rewrite_chunk<'q>(physical: &'q Chunk) -> Statement<'q> {
    sqlx::query("UPDATE chunks SET current_encoding_id=$2,standalone_size=$3,standalone_compressed=$4,nonce=$5,encryption_algorithm=$6,block_key_id=$7 WHERE id=$1 AND state='ready'").bind(physical.id).bind(physical.encoding_id).bind(physical.stored_size).bind(physical.compressed).bind(&physical.nonce).bind(&physical.algorithm).bind(physical.block_key_id)
}
pub(super) fn advance_rewrite(id: Uuid, cursor: String) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET cursor=$2,processed=processed+1,updated_at=now() WHERE id=$1")
        .bind(id)
        .bind(cursor)
}
pub(super) fn rewrite_pack(id: Id) -> Rows<'static, Pack> {
    sqlx::query_as("SELECT * FROM packs WHERE id=$1 AND state='ready'").bind(id)
}
pub(super) fn rewrite_members(id: Id) -> Rows<'static, Chunk> {
    sqlx::query_as("SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at FROM pack_members m JOIN chunks c ON c.id=m.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE m.pack_id=$1 ORDER BY m.ordinal").bind(id)
}
pub(super) fn rewrite_current_members(id: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM chunks WHERE pack_id=$1 AND state='ready' ORDER BY id")
        .bind(id)
}
pub(super) fn lock_rewrite_members(id: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT id FROM chunks WHERE pack_id=$1 AND state='ready' ORDER BY id FOR UPDATE",
    )
    .bind(id)
}
pub(super) fn publish_rewrite_pack<'q>(pack: &'q Pack) -> Statement<'q> {
    sqlx::query("UPDATE packs SET stored_size=$2,pack_compressed=$3,nonce=$4,nonce_id=$6,digest=$5,state='ready',unreferenced_at=NULL,stored_at=clock_timestamp(),owner_task=NULL WHERE id=$1 AND state='preparing'").bind(pack.id).bind(pack.stored_size).bind(pack.compressed).bind(&pack.nonce).bind(&pack.digest).bind(crate::content::nonce::id(pack.nonce.as_deref()))
}
pub(super) fn replace_rewrite_pack(old: Id, new: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET pack_id=$2 WHERE pack_id=$1 AND state='ready'")
        .bind(old)
        .bind(new)
}
pub(super) fn retire_rewrite_pack(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE packs SET state='retired',unreferenced_at=now(),owner_task=NULL WHERE id=$1 AND state='ready'").bind(id)
}
pub(super) fn complete_rewrite(id: Uuid, token: Uuid, after: Id) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET cursor=$2,state='completed',updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$3 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())").bind(id).bind(after.to_string()).bind(token)
}
pub(super) fn lock_task_execution(id: Uuid, token: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()) FOR UPDATE").bind(id).bind(token)
}

pub(super) fn purge_counts(bucket: Uuid) -> Rows<'static, (i64, i64)> {
    sqlx::query_as("SELECT (SELECT count(*) FROM objects WHERE bucket_id=$1 AND stream_id IS NOT NULL),(SELECT count(*) FROM uploads WHERE bucket_id=$1 AND state IN ('active','completing'))").bind(bucket)
}
pub(super) fn lock_purge_bucket<'q>(name: &'q str, id: Uuid) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT id FROM buckets WHERE name=$1 AND id=$2 FOR UPDATE")
        .bind(name)
        .bind(id)
}
pub(super) fn existing_purge(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM tasks WHERE task_type='purge' AND bucket_id=$1 AND state<>'completed'",
    )
    .bind(id)
}
pub(super) fn seal_purge_bucket(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE buckets SET state='purging' WHERE id=$1").bind(id)
}
pub(super) fn chunks_indexed() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunks)")
}
pub(super) fn sweep_preview(id: Uuid) -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT detail FROM tasks WHERE id=$1 AND task_type='sweep' AND state='completed'",
    )
    .bind(id)
}
pub(super) fn pause_active_task(id: Execution) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='paused',updated_at=now() WHERE id=$1 AND state IN ('queued','running') AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())").bind(id.id)
        .bind(id.token)
}
pub(super) fn task_running(task: Execution) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()))").bind(task.id).bind(task.token)
}
pub(super) fn purge_batch_limit(bucket: Uuid) -> Scalar<'static, Option<i32>> {
    sqlx::query_scalar("SELECT maintenance_concurrency FROM storage_profiles WHERE storage_id=(SELECT storage_id FROM buckets WHERE id=$1)").bind(bucket)
}
pub(super) fn purge_uploads(bucket: Uuid, batch: i64) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM uploads WHERE bucket_id=$1 AND state IN ('active','completing') ORDER BY id LIMIT $2").bind(bucket).bind(batch)
}
pub(super) fn lock_purge_objects<'q>(
    bucket: Uuid,
    cursor: Option<&'q str>,
    batch: i64,
) -> Rows<'q, (String, Option<Id>)> {
    sqlx::query_as("SELECT object_key,stream_id FROM objects WHERE bucket_id=$1 AND ($2::text IS NULL OR object_key>$2) ORDER BY object_key LIMIT $3 FOR UPDATE").bind(bucket).bind(cursor).bind(batch)
}
pub(super) fn purge_object<'q>(bucket: Uuid, key: &'q str, token: Id) -> Statement<'q> {
    sqlx::query(
        "UPDATE objects SET stream_id=NULL,write_token=$3 WHERE bucket_id=$1 AND object_key=$2",
    )
    .bind(bucket)
    .bind(key)
    .bind(token)
}
pub(super) fn advance_task<'q>(id: Uuid, cursor: &'q str, processed: i64) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET cursor=$2,processed=processed+$3,updated_at=now() WHERE id=$1")
        .bind(id)
        .bind(cursor)
        .bind(processed)
}
pub(super) fn purge_upload_history<'q>(
    bucket: Uuid,
    active: &'q [Uuid],
    batch: i64,
) -> Statement<'q> {
    sqlx::query("DELETE FROM uploads WHERE id IN (SELECT id FROM uploads WHERE bucket_id=$1 AND state IN ('completed','aborted') AND NOT (id=ANY($2)) LIMIT $3)").bind(bucket).bind(active).bind(batch)
}
pub(super) fn purge_waiting(bucket: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM streams WHERE bucket_id=$1) OR EXISTS(SELECT 1 FROM uploads WHERE bucket_id=$1)").bind(bucket)
}
pub(super) fn purge_object_rows(bucket: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM objects WHERE bucket_id=$1").bind(bucket)
}
pub(super) fn purge_bucket_row(bucket: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM buckets WHERE id=$1 AND state='purging'").bind(bucket)
}
pub(super) fn complete_task(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='completed',updated_at=now() WHERE id=$1").bind(id)
}
pub(super) fn complete_running_task(id: Execution) -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET state='completed',updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())",
    )
    .bind(id.id)
        .bind(id.token)
}
pub(super) fn physical_indexed(location: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunk_encodings WHERE encoding_id=$1) OR EXISTS(SELECT 1 FROM packs WHERE id=$1)").bind(location)
}
pub(super) fn task_progress<'q>(
    id: Execution,
    cursor: &'q str,
    processed: i64,
    detail: Value,
) -> Statement<'q> {
    sqlx::query(
        "UPDATE tasks SET cursor=$2,processed=processed+$3,detail=$4,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$5 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())",
    )
    .bind(id.id)
    .bind(cursor)
    .bind(processed)
    .bind(detail)
        .bind(id.token)
}

pub(super) fn gc_candidates(storage: Uuid, grace: f64, batch: i64) -> Rows<'static, (Id, i32)> {
    sqlx::query_as("SELECT id,raw_size FROM chunks WHERE storage_id=$1 AND state IN ('ready','failed','deleting') AND unreferenced_at<now()-$2*interval '1 second' AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=chunks.id) AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=chunks.id) AND NOT EXISTS(SELECT 1 FROM transfer_members p WHERE p.source_chunk=chunks.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=chunks.id) ORDER BY unreferenced_at,id LIMIT $3").bind(storage).bind(grace).bind(batch)
}
pub(super) fn reset_access_coverage() -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET access_coverage_since=now(),access_flushed_at=now()")
}
pub(super) fn abandon_startup_streams() -> Statement<'static> {
    sqlx::query(
        "UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_lease_token=NULL,touched_at=now() WHERE state='writing' AND NOT EXISTS(SELECT 1 FROM transfer_sources t WHERE t.candidate=streams.id)",
    )
}
pub(super) fn fail_startup_chunks() -> Statement<'static> {
    sqlx::query(
        "UPDATE chunks SET state='failed',unreferenced_at=COALESCE(unreferenced_at,now()) WHERE state IN ('preparing','uploading') AND NOT EXISTS(SELECT 1 FROM transfer_sources t WHERE t.candidate=chunks.owner_stream)",
    )
}
pub(super) fn retire_startup_encodings() -> Statement<'static> {
    sqlx::query(
        "UPDATE chunk_encodings SET owner_stream=NULL,state='retired',unreferenced_at=COALESCE(unreferenced_at,now()) WHERE state='uploading' AND NOT (boot_id IS NOT NULL AND quarantined_at IS NOT NULL AND encoding_id IN (SELECT current_encoding_id FROM chunks WHERE state='ready'))",
    )
}
pub(super) fn retire_startup_packs() -> Statement<'static> {
    sqlx::query(
        "UPDATE packs SET state='retired',unreferenced_at=COALESCE(unreferenced_at,now()) WHERE state='preparing'",
    )
}
pub(super) fn clear_pack_inputs() -> Statement<'static> {
    sqlx::query("DELETE FROM pack_inputs")
}
pub(super) fn reset_completing_uploads() -> Statement<'static> {
    sqlx::query("UPDATE uploads SET state='active' WHERE state='completing'")
}
pub(super) fn reset_startup_tasks(maintenance: bool) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state=CASE WHEN task_type IN ('purge','pack','unpack','upload','cache_flush','storage_migration','rewrite') AND $1 THEN 'paused' ELSE 'queued' END,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE state='running' OR (task_type IN ('purge','pack','unpack','upload','cache_flush','storage_migration','rewrite') AND state='queued' AND $1)").bind(maintenance)
}
pub(super) fn fragment_indexed(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM fragments WHERE id=$1) OR EXISTS(SELECT 1 FROM multipart_files WHERE id=$1)").bind(id)
}
pub(super) fn recovery_fragments(after: Id) -> Rows<'static, RecoveryFragment> {
    sqlx::query_as("SELECT f.id,f.id AS fragment_location,f.size,f.hash,f.staging_storage_id,f.staging_object_key,f.file_id,f.file_offset,m.storage_id AS file_storage,CASE WHEN m.source_id IS NOT NULL THEN m.id END AS raw_source FROM fragments f LEFT JOIN multipart_files m ON m.id=f.file_id WHERE f.id>$1 AND EXISTS(SELECT 1 FROM extents WHERE fragment_id=f.id) ORDER BY f.id LIMIT 128").bind(after)
}
pub(super) fn fragment_uploads(id: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT DISTINCT p.upload_id FROM parts p JOIN extents e ON e.stream_id=p.stream_id WHERE e.fragment_id=$1").bind(id)
}
pub(super) fn expire_stream_leases() -> Statement<'static> {
    sqlx::query("DELETE FROM stream_leases WHERE expires_at<=now()")
}
pub(super) fn expired_uploads(batch: i64, storage: Option<Uuid>) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT u.id FROM uploads u JOIN buckets b ON b.id=u.bucket_id JOIN storage_profiles p ON p.storage_id=b.storage_id WHERE ($2::uuid IS NULL OR b.storage_id=$2) AND u.state='active' AND u.touched_at<now()-(left(p.multipart_idle_timeout,-1)::bigint*CASE right(p.multipart_idle_timeout,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END)*interval '1 second' ORDER BY u.touched_at,u.id LIMIT $1").bind(batch).bind(storage)
}

fn stale_stream_filter(query: &mut QueryBuilder<Postgres>, retention: Option<f64>, single: bool) {
    query
        .push(" AND ((s.state IN ('retired','abandoned') AND (")
        .push_bind(retention)
        .push("::double precision IS NULL OR s.touched_at<now()-")
        .push_bind(retention)
        .push("*interval '1 second')) OR s.state='writing')");
    if !single {
        query.push(" AND NOT EXISTS(SELECT 1 FROM stream_leases l WHERE l.stream_id=s.id AND l.expires_at>now())");
    }
    query.push(" AND NOT EXISTS(SELECT 1 FROM objects WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM parts WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM transfer_items p WHERE p.source_pin=s.id) AND NOT EXISTS(SELECT 1 FROM transfer_items p WHERE p.candidate=s.id) AND NOT EXISTS(SELECT 1 FROM transfer_sources p WHERE p.candidate=s.id) AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=s.owner_task_id AND t.state='running' AND t.lease_token=s.owner_lease_token AND ((t.lease_node_id IS NULL AND t.lease_expires_at IS NULL) OR t.lease_expires_at>now()))");
}
pub(super) fn stale_streams(
    active: &[Id],
    batch: i64,
    storage: Option<Uuid>,
    retention: Option<f64>,
    single: bool,
) -> QueryBuilder<Postgres> {
    let mut query = QueryBuilder::new(
        "SELECT s.id FROM streams s JOIN buckets b ON b.id=s.bucket_id WHERE NOT(s.id=ANY(",
    );
    query.push_bind(active).push("))");
    if let Some(storage) = storage {
        query.push(" AND b.storage_id=").push_bind(storage);
    }
    stale_stream_filter(&mut query, retention, single);
    query
        .push(" ORDER BY s.touched_at,s.id LIMIT ")
        .push_bind(batch);
    query
}
pub(super) fn claim_stale_stream(
    id: Id,
    retention: Option<f64>,
    storage: Option<Uuid>,
    single: bool,
) -> QueryBuilder<Postgres> {
    let mut query = QueryBuilder::new(
        "UPDATE streams s SET state='abandoned',owner_task_id=NULL,owner_lease_token=NULL WHERE id=",
    );
    query.push_bind(id);
    if let Some(storage) = storage {
        query
            .push(" AND EXISTS(SELECT 1 FROM buckets b WHERE b.id=s.bucket_id AND b.storage_id=")
            .push_bind(storage)
            .push(")");
    }
    stale_stream_filter(&mut query, retention, single);
    query
}

pub(super) fn detach_stream_chunks(id: Id, batch: i64) -> Statement<'static> {
    sqlx::query("UPDATE chunks c SET owner_stream=NULL,state=CASE WHEN state IN ('preparing','uploading') THEN 'failed' ELSE state END,unreferenced_at=CASE WHEN NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id) THEN COALESCE(unreferenced_at,now()) ELSE unreferenced_at END WHERE id IN (SELECT id FROM chunks WHERE owner_stream=$1 ORDER BY id LIMIT $2)").bind(id).bind(batch)
}
pub(super) fn remove_stream_extents(id: Id, batch: i64) -> Scalar<'static, Option<Id>> {
    sqlx::query_scalar("DELETE FROM extents WHERE stream_id=$1 AND offset_bytes IN (SELECT offset_bytes FROM extents WHERE stream_id=$1 ORDER BY offset_bytes LIMIT $2) RETURNING chunk_id").bind(id).bind(batch)
}
pub(super) fn remove_empty_stream(id: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM streams s WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM extents WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM chunks WHERE owner_stream=s.id) AND NOT EXISTS(SELECT 1 FROM fragments WHERE owner_stream=s.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE owner_stream=s.id)").bind(id)
}
pub(super) fn stale_fragments<'q>(
    batch: i64,
    active: &'q [Id],
    storage: Option<Uuid>,
) -> Scalar<'q, Id> {
    sqlx::query_scalar("SELECT f.id FROM fragments f WHERE ($3::uuid IS NULL OR f.staging_storage_id=$3 OR EXISTS(SELECT 1 FROM streams s JOIN buckets b ON b.id=s.bucket_id WHERE s.id=f.owner_stream AND b.storage_id=$3)) AND (f.sealed OR f.owner_stream IS NULL OR (NOT (f.owner_stream=ANY($2)) AND NOT EXISTS(SELECT 1 FROM streams s WHERE s.id=f.owner_stream AND s.state='writing'))) AND NOT EXISTS(SELECT 1 FROM extents WHERE fragment_id=f.id) ORDER BY f.created_at,f.id LIMIT $1").bind(batch).bind(active).bind(storage)
}
pub(super) fn lock_cleanup_fragment(
    id: Id,
) -> Rows<
    'static,
    (
        Uuid,
        bool,
        Option<Id>,
        Option<Uuid>,
        Option<String>,
        Option<Uuid>,
    ),
> {
    sqlx::query_as("SELECT id,sealed,owner_stream,staging_storage_id,staging_object_key,file_id FROM fragments f WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.kind='fragment' AND n.location_id=f.id AND l.expires_at>clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM extents WHERE fragment_id=f.id) FOR UPDATE SKIP LOCKED").bind(id)
}
pub(super) fn remove_fragment_row(id: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM fragments WHERE id=$1").bind(id)
}
pub(super) fn remove_tombstones(batch: i64, storage: Option<Uuid>) -> Statement<'static> {
    sqlx::query("DELETE FROM objects WHERE (bucket_id,object_key) IN (SELECT o.bucket_id,o.object_key FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE ($2::uuid IS NULL OR b.storage_id=$2) AND o.stream_id IS NULL AND NOT EXISTS(SELECT 1 FROM streams s WHERE s.bucket_id=o.bucket_id AND s.object_key=o.object_key AND s.state='writing') ORDER BY o.bucket_id,o.object_key LIMIT $1 FOR UPDATE OF o SKIP LOCKED)").bind(batch).bind(storage)
}
pub(super) fn gc_paused() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT gc_paused OR maintenance FROM mokyu_meta")
}
pub(super) fn profile_storages() -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT storage_id FROM storage_profiles ORDER BY storage_id")
}
pub(super) fn lock_reclaim_chunk(
    id: Id,
    storage: Uuid,
    grace: f64,
) -> Rows<'static, (Option<Id>,)> {
    sqlx::query_as("SELECT owner_stream FROM chunks WHERE id=$1 AND storage_id=$2 AND state IN ('ready','failed','deleting') AND unreferenced_at<now()-$3*interval '1 second' AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=chunks.id) AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=chunks.id) AND NOT EXISTS(SELECT 1 FROM transfer_members p WHERE p.source_chunk=chunks.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=chunks.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=chunks.id AND owner_stream IS NOT NULL) FOR UPDATE").bind(id).bind(storage).bind(grace)
}
pub(super) fn claim_reclaim_chunk(id: Id, storage: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET state='deleting',pack_id=NULL WHERE id=$1 AND storage_id=$2")
        .bind(id)
        .bind(storage)
}
pub(super) fn reclaim_locations(id: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT encoding_id FROM chunk_encodings WHERE chunk_id=$1 AND state IN ('ready','uploading','retired','deleting')").bind(id)
}
pub(super) fn retire_chunk_encodings(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings l SET state='retired',unreferenced_at=COALESCE(l.unreferenced_at,c.unreferenced_at,now()) FROM chunks c WHERE l.chunk_id=c.id AND c.id=$1 AND l.state IN ('ready','uploading')").bind(id)
}
pub(super) fn finish_reclaim_chunk(id: Id, storage: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET state='deleted',deleted_at=now() WHERE id=$1 AND storage_id=$2 AND state='deleting'").bind(id).bind(storage)
}
pub(super) fn retire_empty_packs(storage: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE packs p SET state='retired',unreferenced_at=COALESCE(unreferenced_at,(SELECT max(c.unreferenced_at) FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=p.id),now()) WHERE state='ready' AND NOT EXISTS(SELECT 1 FROM chunks WHERE pack_id=p.id AND state='ready') AND p.storage_id=$1").bind(storage)
}
pub(super) fn gc_counts() -> Rows<'static, (String, i64)> {
    sqlx::query_as("SELECT state,count(*) FROM chunks GROUP BY state")
}
pub(super) fn gc_state() -> Rows<'static, (bool, bool)> {
    sqlx::query_as("SELECT gc_paused,maintenance FROM mokyu_meta")
}
pub(super) fn mark_orphan_chunks() -> Statement<'static> {
    sqlx::query(
        "UPDATE chunks c SET unreferenced_at=clock_timestamp() WHERE c.state='ready' AND c.owner_stream IS NULL AND c.unreferenced_at IS NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)",
    )
}
pub(super) fn cleanup_statement_timeout(timeout: String) -> Statement<'static> {
    sqlx::query("SELECT set_config('statement_timeout',$1,true)").bind(timeout)
}
pub(super) fn cleanup_history<'q>(
    section: &str,
    age: f64,
    batch: i64,
    active: &'q [Uuid],
) -> Result<Statement<'q>> {
    let sql = match section {
        "key_replacements" => {
            "DELETE FROM key_replacements WHERE id IN (SELECT id FROM key_replacements WHERE expires_at<now()-$1*interval '1 second' ORDER BY expires_at LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "maintenance_previews" => {
            "DELETE FROM maintenance_previews WHERE id IN (SELECT id FROM maintenance_previews WHERE expires_at<now()-$1*interval '1 second' ORDER BY expires_at LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "media_operations" => {
            "DELETE FROM media_operations WHERE id IN (SELECT id FROM media_operations WHERE created_at<now()-$1*interval '1 second' ORDER BY created_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "audit_events" => {
            "DELETE FROM audit_events WHERE id IN (SELECT id FROM audit_events WHERE created_at<now()-$1*interval '1 second' ORDER BY created_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "chunks" => {
            "DELETE FROM chunks WHERE id IN (SELECT id FROM chunks c WHERE state='deleted' AND deleted_at<now()-((SELECT (left(sp.deleted_chunk_retention,-1)::bigint*CASE right(sp.deleted_chunk_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=c.storage_id)+$1::double precision*0)*interval '1 second' AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=c.id AND (state<>'deleted' OR recovery_pending OR owner_stream IS NOT NULL)) AND NOT EXISTS(SELECT 1 FROM pack_members m JOIN packs p ON p.id=m.pack_id WHERE m.chunk_id=c.id AND p.state<>'deleted') ORDER BY deleted_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "packs" => {
            "DELETE FROM packs WHERE id IN (SELECT id FROM packs p WHERE state='deleted' AND deleted_at<now()-((SELECT (left(sp.deleted_chunk_retention,-1)::bigint*CASE right(sp.deleted_chunk_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=p.storage_id)+$1::double precision*0)*interval '1 second' AND NOT EXISTS(SELECT 1 FROM chunks WHERE pack_id=p.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE source_pack_id=p.id) ORDER BY deleted_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "chunk_encodings" => {
            "DELETE FROM chunk_encodings WHERE encoding_id IN (SELECT encoding_id FROM chunk_encodings WHERE state='deleted' AND NOT recovery_pending AND owner_stream IS NULL AND deleted_at<now()-((SELECT (left(sp.deleted_chunk_retention,-1)::bigint*CASE right(sp.deleted_chunk_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=chunk_encodings.storage_id)+$1::double precision*0)*interval '1 second' ORDER BY deleted_at,encoding_id LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "uploads" => {
            "DELETE FROM uploads WHERE id IN (SELECT id FROM uploads u WHERE state IN ('completed','aborted') AND touched_at<now()-((SELECT (left(sp.upload_retention,-1)::bigint*CASE right(sp.upload_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=(SELECT storage_id FROM buckets WHERE id=u.bucket_id))+$1::double precision*0)*interval '1 second' AND raw_source_id IS NULL AND NOT (id=ANY($3)) AND NOT EXISTS(SELECT 1 FROM parts WHERE upload_id=u.id) ORDER BY touched_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "integrity_issues" => {
            "DELETE FROM integrity_issues WHERE id IN (SELECT i.id FROM integrity_issues i JOIN tasks t ON t.id=i.task_id WHERE t.state='completed' AND t.updated_at<now()-$1*interval '1 second' ORDER BY i.id LIMIT $2 FOR UPDATE OF i SKIP LOCKED)"
        }
        "tasks" => {
            "DELETE FROM tasks WHERE id IN (SELECT id FROM tasks t WHERE state='completed' AND updated_at<now()-$1*interval '1 second' AND NOT EXISTS(SELECT 1 FROM integrity_issues WHERE task_id=t.id) AND NOT EXISTS(SELECT 1 FROM external_tests e WHERE e.id=t.id AND e.cleanup_required) ORDER BY updated_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "sessions" => {
            "DELETE FROM sessions WHERE token_hash IN (SELECT token_hash FROM sessions s WHERE expires_at<now()-$1*interval '1 second' AND NOT EXISTS(SELECT 1 FROM session_refresh_tokens r WHERE r.session_id=s.id AND r.revoked_at IS NULL AND r.expires_at>now()) ORDER BY expires_at,token_hash LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "refresh_tokens" => {
            "DELETE FROM session_refresh_tokens WHERE id IN (SELECT id FROM session_refresh_tokens WHERE revoked_at<now()-$1*interval '1 second' AND NOT EXISTS(SELECT 1 FROM session_refresh_tokens live WHERE live.session_id=session_refresh_tokens.session_id AND live.family_id=session_refresh_tokens.family_id AND live.revoked_at IS NULL AND live.expires_at>now()) ORDER BY expires_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        _ => anyhow::bail!("unknown cleanup section"),
    };
    let query = sqlx::query(sql).bind(age).bind(batch);
    Ok(if section == "uploads" {
        query.bind(active)
    } else {
        query
    })
}
pub(super) async fn history_limits(db: &mut sqlx::PgConnection) -> Result<()> {
    sqlx::raw_sql("SET LOCAL lock_timeout='100ms'; SET LOCAL work_mem='4MB'")
        .execute(db)
        .await?;
    Ok(())
}
pub(super) async fn cleanup_limits(db: &mut sqlx::PgConnection) -> Result<()> {
    sqlx::raw_sql("SET LOCAL lock_timeout='100ms'; SET LOCAL statement_timeout='1s'")
        .execute(db)
        .await?;
    Ok(())
}
fn physical_table(packed: bool) -> (&'static str, &'static str, &'static str) {
    if packed {
        (
            "packs",
            "id",
            "NOT EXISTS(SELECT 1 FROM stream_leases l JOIN extents e ON e.stream_id=l.stream_id JOIN pack_members m ON m.chunk_id=e.chunk_id WHERE m.pack_id=r.id AND l.expires_at>now()) AND NOT EXISTS(SELECT 1 FROM transfer_sources p WHERE p.source_pack=r.id)",
        )
    } else {
        (
            "chunk_encodings",
            "encoding_id",
            "NOT EXISTS(SELECT 1 FROM stream_leases l JOIN extents e ON e.stream_id=l.stream_id JOIN chunk_encodings ce ON ce.chunk_id=e.chunk_id WHERE ce.encoding_id=r.encoding_id AND l.expires_at>now()) AND NOT EXISTS(SELECT 1 FROM transfer_sources p WHERE p.source_encoding=r.encoding_id)",
        )
    }
}
fn node_read_source_guard(kind: &str, location: &str) -> String {
    format!(
        "NOT EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.kind='{kind}' AND n.location_id={location} AND l.expires_at>clock_timestamp())"
    )
}
pub(super) fn physical_gc_candidates(
    packed: bool,
    grace: f64,
    batch: i64,
    minimum: f64,
    storage: Uuid,
    single: bool,
) -> Rows<'static, (Id, Uuid, bool)> {
    let (table, id_column, lease_guard) = physical_table(packed);
    let state_guard = if packed {
        "r.state IN ('retired','deleting')"
    } else {
        "(r.state IN ('retired','deleting') OR (r.state='deleted' AND r.recovery_pending)) AND r.owner_stream IS NULL"
    };
    let unknown = if packed {
        "r.stored_at IS NULL"
    } else {
        "r.stored_at IS NULL OR r.recovery_pending"
    };
    let lease_guard = format!(
        "({}) AND ({})",
        if single {
            if packed {
                "NOT EXISTS(SELECT 1 FROM transfer_sources p WHERE p.source_pack=r.id)"
            } else {
                "NOT EXISTS(SELECT 1 FROM transfer_sources p WHERE p.source_encoding=r.encoding_id)"
            }
        } else {
            lease_guard
        },
        node_read_source_guard(
            if packed { "pack" } else { "chunk" },
            &format!("r.{id_column}")
        )
    );
    sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT r.{id_column},r.{id_column},({unknown}) FROM {table} r WHERE {state_guard} AND r.unreferenced_at<now()-$1*interval '1 second' AND ($3::double precision=0 OR r.stored_at IS NULL OR r.stored_at<=now()-$3*interval '1 second') AND r.storage_id=$4 AND {lease_guard} AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.task_type='integrity' AND t.state='running' AND t.detail->>'operation' IN ('recheck','verify') AND t.detail->>'source_location'=r.{id_column}::text AND ((t.lease_node_id IS NULL AND t.lease_expires_at IS NULL) OR t.lease_expires_at>clock_timestamp())) ORDER BY r.unreferenced_at,r.{id_column} LIMIT $2"))).bind(grace).bind(batch).bind(minimum).bind(storage)
}
pub(super) fn physical_gc_age(packed: bool, id: Id, stored: DateTime<Utc>) -> Statement<'static> {
    if !packed {
        return sqlx::query("UPDATE chunk_encodings SET stored_at=CASE WHEN recovery_pending THEN $2 ELSE COALESCE(stored_at,$2) END WHERE encoding_id=$1").bind(id).bind(stored);
    }
    let (table, id_column, _lease_guard) = physical_table(packed);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {table} SET stored_at=COALESCE(stored_at,$2) WHERE {id_column}=$1"
    )))
    .bind(id)
    .bind(stored)
}
pub(super) fn claim_physical_gc(
    packed: bool,
    id: Id,
    grace: f64,
    minimum: f64,
) -> Statement<'static> {
    let (table, id_column, lease_guard) = physical_table(packed);
    let lease_guard = format!(
        "({lease_guard}) AND ({})",
        node_read_source_guard(
            if packed { "pack" } else { "chunk" },
            &format!("r.{id_column}")
        )
    );
    let state_guard = if packed {
        "state IN ('retired','deleting')"
    } else {
        "(state IN ('retired','deleting') OR (state='deleted' AND recovery_pending)) AND owner_stream IS NULL"
    };
    sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {table} r SET state='deleting' WHERE {lease_guard} AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.task_type='integrity' AND t.state='running' AND t.detail->>'operation' IN ('recheck','verify') AND t.detail->>'source_location'=r.{id_column}::text AND ((t.lease_node_id IS NULL AND t.lease_expires_at IS NULL) OR t.lease_expires_at>clock_timestamp())) AND {id_column}=$1 AND {state_guard} AND unreferenced_at<now()-$2*interval '1 second' AND ($3::double precision=0 OR stored_at IS NULL OR stored_at<=now()-$3*interval '1 second')"))).bind(id).bind(grace).bind(minimum)
}
pub(super) fn finish_physical_gc(packed: bool, id: Id, retention: f64) -> Statement<'static> {
    if !packed {
        return sqlx::query("UPDATE chunk_encodings SET recovery_pending=recovery_pending AND (deleted_at IS NULL OR deleted_at>clock_timestamp()-$2*interval '1 second'),state='deleted',unreferenced_at=clock_timestamp(),deleted_at=COALESCE(deleted_at,clock_timestamp()) WHERE encoding_id=$1 AND state='deleting'").bind(id).bind(retention);
    }
    let (table, id_column, _lease_guard) = physical_table(packed);
    sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {table} SET state='deleted',deleted_at=now() WHERE {id_column}=$1 AND state='deleting'"))).bind(id)
}

use crate::model::maintenance::{FragmentCleanup, MaintenanceStore, RecoveryFragment};
#[async_trait::async_trait]
impl MaintenanceStore for Backend {
    async fn destructive_sweep(&self) -> Result<bool> {
        Ok(work::sweep_pending().fetch_one(&self.pool).await?)
    }
    async fn mode(&self, enabled: bool) -> Result<()> {
        transactions::maintenance_mode(&self.pool, enabled).await
    }

    async fn preview(
        &self,
        principal: &Principal,
        action: &str,
        parameters: Value,
        impact: Value,
        confirmation: String,
    ) -> Result<MaintenancePreview> {
        transactions::create_maintenance_preview(
            &self.pool,
            principal,
            action,
            parameters,
            impact,
            confirmation,
        )
        .await
    }
    async fn execution(
        &self,
        principal: &Principal,
        id: Uuid,
        action: &str,
    ) -> Result<Option<MaintenanceExecution>> {
        transactions::maintenance_execution(&self.pool, principal, id, action).await
    }

    async fn pack_creation(&self, paused: bool) -> Result<()> {
        transactions::change_pack_creation(&self.pool, paused).await
    }
    async fn policy_paused(&self, policy: &str) -> Result<bool> {
        Ok(maintenance_policy_paused(policy)
            .fetch_one(&self.pool)
            .await?)
    }
    async fn controls(&self) -> Result<Vec<Value>> {
        Ok(maintenance_controls().fetch_all(&self.pool).await?)
    }
    async fn preparing_packs(&self) -> Result<String> {
        Ok(preparing_pack_count().fetch_one(&self.pool).await?)
    }
    async fn change_policy(&self, policy: &str, paused: bool) -> Result<()> {
        transactions::change_maintenance_policy(&self.pool, policy, paused).await
    }
    async fn start_policy(&self, policy: &str, node: Option<Uuid>) -> Result<(Uuid, bool)> {
        transactions::start_lifecycle_policy(&self.pool, self.single, policy, node).await
    }
    async fn finish_policy(
        &self,
        id: Execution,
        count: i64,
        again: bool,
        report: Value,
    ) -> Result<()> {
        if self.single {
            return transactions::finish_single_lifecycle(&self.pool, id, count, again, report)
                .await;
        }
        finish_lifecycle_task(id, count, again, report)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn schedule(&self) -> Result<Vec<(String, bool, Option<f64>)>> {
        Ok(maintenance_schedule().fetch_all(&self.pool).await?)
    }
    async fn reuse_due(&self) -> Result<bool> {
        Ok(due_pack_reuse().fetch_one(&self.pool).await?)
    }
    async fn scheduled(&self, policy: &str) -> Result<()> {
        scheduled_maintenance(policy).execute(&self.pool).await?;
        Ok(())
    }

    async fn chunk_candidates(
        &self,
        storage: Uuid,
        grace: f64,
        batch: i64,
    ) -> Result<Vec<(Id, i32)>> {
        Ok(gc_candidates(storage, grace, batch)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn cleanup_history(
        &self,
        section: &str,
        age: f64,
        batch: i64,
        active: &[Uuid],
        timeout_ms: u128,
    ) -> Result<u64> {
        transactions::cleanup_history(&self.pool, section, age, batch, active, timeout_ms).await
    }
    async fn recover(&self, maintenance: bool) -> Result<()> {
        transactions::recover_runtime(&self.pool, maintenance).await
    }
    async fn fragment_exists(&self, id: Uuid) -> Result<bool> {
        Ok(fragment_indexed(id).fetch_one(&self.pool).await?)
    }
    async fn recovery_fragments(&self, after: Id) -> Result<Vec<RecoveryFragment>> {
        Ok(recovery_fragments(after).fetch_all(&self.pool).await?)
    }
    async fn fragment_uploads(&self, id: Id) -> Result<Vec<Uuid>> {
        Ok(fragment_uploads(id).fetch_all(&self.pool).await?)
    }
    async fn expired_uploads(&self, batch: i64, storage: Option<Uuid>) -> Result<Vec<Uuid>> {
        Ok(expired_uploads(batch, storage)
            .fetch_all(&self.pool)
            .await?)
    }

    async fn stale_streams(
        &self,
        active: &[Id],
        batch: i64,
        storage: Option<Uuid>,
        retention: Option<f64>,
    ) -> Result<Vec<Id>> {
        Ok(
            stale_streams(active, batch, storage, retention, self.single)
                .build_query_scalar::<Id>()
                .fetch_all(&self.pool)
                .await?,
        )
    }
    async fn claim_stream(
        &self,
        id: Id,
        storage: Option<Uuid>,
        retention: Option<f64>,
    ) -> Result<bool> {
        transactions::claim_stale_stream(&self.pool, id, storage, retention, self.single).await
    }
    async fn cleanup_stream(&self, id: Id, batch: i64) -> Result<usize> {
        transactions::cleanup_stream(&self.pool, id, batch).await
    }
    async fn stale_fragments(
        &self,
        batch: i64,
        active: &[Id],
        storage: Option<Uuid>,
    ) -> Result<Vec<Id>> {
        Ok(stale_fragments(batch, active, storage)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn claim_fragment(&self, id: Id) -> Result<Option<FragmentCleanup>> {
        transactions::claim_fragment(&self.pool, id).await
    }
    async fn cleanup_tombstones(&self, storage: Option<Uuid>, batch: i64) -> Result<u64> {
        transactions::cleanup_tombstones(&self.pool, storage, batch).await
    }
    async fn gc_paused(&self) -> Result<bool> {
        Ok(gc_paused().fetch_one(&self.pool).await?)
    }
    async fn storages(&self) -> Result<Vec<Uuid>> {
        Ok(profile_storages().fetch_all(&self.pool).await?)
    }
    async fn retire_chunk(
        &self,
        id: Id,
        storage: Uuid,
        grace: f64,
        active: &[Id],
    ) -> Result<Option<Vec<Uuid>>> {
        transactions::retire_chunk(&self.pool, id, storage, grace, active).await
    }
    async fn delete_chunk(&self, id: Id, storage: Uuid) -> Result<()> {
        finish_reclaim_chunk(id, storage)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn retire_packs(&self, storage: Uuid) -> Result<()> {
        retire_empty_packs(storage).execute(&self.pool).await?;
        Ok(())
    }
    async fn physical_candidates(
        &self,
        packed: bool,
        grace: f64,
        batch: i64,
        minimum: f64,
        storage: Uuid,
    ) -> Result<Vec<(Id, Uuid, bool)>> {
        Ok(
            physical_gc_candidates(packed, grace, batch, minimum, storage, self.single)
                .fetch_all(&self.pool)
                .await?,
        )
    }
    async fn physical_age(&self, packed: bool, id: Id, stored: DateTime<Utc>) -> Result<()> {
        physical_gc_age(packed, id, stored)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn claim_physical(&self, packed: bool, id: Id, grace: f64, minimum: f64) -> Result<bool> {
        Ok(claim_physical_gc(packed, id, grace, minimum)
            .execute(&self.pool)
            .await?
            .rows_affected()
            != 0)
    }
    async fn delete_physical(&self, packed: bool, id: Id, retention: f64) -> Result<()> {
        finish_physical_gc(packed, id, retention)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn gc_status(&self) -> Result<(Vec<(String, i64)>, bool, bool)> {
        let counts = gc_counts().fetch_all(&self.pool).await?;
        let (paused, maintenance) = gc_state().fetch_one(&self.pool).await?;
        Ok((counts, paused, maintenance))
    }
    async fn mark_orphans(&self) -> Result<usize> {
        transactions::mark_orphan_chunks(&self.pool).await
    }
}

pub(super) fn initialize_integrity_rate<'q>(
    storage: Uuid,
    metric: &'q str,
    tokens: String,
) -> Statement<'q> {
    sqlx::query("INSERT INTO storage_rate_state(storage_id,metric,tokens,replenished_at,updated_at) VALUES($1,$2,$3::text::numeric,now(),now()) ON CONFLICT(storage_id,metric) DO NOTHING").bind(storage).bind(metric).bind(tokens)
}
pub(super) fn lock_integrity_rate<'q>(
    storage: Uuid,
    metric: &'q str,
) -> Rows<'q, (f64, DateTime<Utc>)> {
    sqlx::query_as("SELECT tokens::double precision,replenished_at FROM storage_rate_state WHERE storage_id=$1 AND metric=$2 FOR UPDATE").bind(storage).bind(metric)
}
pub(super) fn update_integrity_rate<'q>(
    storage: Uuid,
    metric: &'q str,
    tokens: String,
) -> Statement<'q> {
    sqlx::query("UPDATE storage_rate_state SET tokens=$3::text::numeric,replenished_at=now(),updated_at=now() WHERE storage_id=$1 AND metric=$2").bind(storage).bind(metric).bind(tokens)
}
pub(super) fn integrity_busy() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='integrity' AND NOT (detail ? 'source_operation') AND state IN ('queued','running'))",
    )
}
pub(super) fn integrity_storage(storage: Uuid) -> Rows<'static, (Uuid, String)> {
    sqlx::query_as("SELECT id,type FROM storages WHERE id=$1 AND enabled").bind(storage)
}
pub(super) fn integrity_storages() -> Rows<'static, (Uuid, String)> {
    sqlx::query_as("SELECT id,type FROM storages WHERE enabled ORDER BY created_at,id")
}
pub(super) fn integrity_upper_chunk(storage: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT COALESCE((SELECT id FROM chunks WHERE storage_id=$1 ORDER BY id DESC LIMIT 1),'00000000-0000-0000-0000-000000000000'::uuid)").bind(storage)
}
pub(super) fn integrity_upper_chunk_all() -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT COALESCE((SELECT id FROM chunks ORDER BY id DESC LIMIT 1),'00000000-0000-0000-0000-000000000000'::uuid)",
    )
}
pub(super) fn scope_integrity_task<'q>(
    child: Uuid,
    parent: Uuid,
    total: i64,
    scope: &'q str,
    node: Option<Uuid>,
) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET parent_task_id=$2,batch_total=$3,execution_scope=$4,target_node_id=$5 WHERE id=$1").bind(child).bind(parent).bind(total).bind(scope).bind(node)
}
pub(super) fn task_batch_total(parent: Uuid, count: i64) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET batch_total=$2 WHERE id=$1")
        .bind(parent)
        .bind(count)
}
pub(super) fn insert_integrity_finding<'q>(task: Uuid, issue: &'q Finding) -> Statement<'q> {
    sqlx::query("INSERT INTO integrity_issues(task_id,subject,code,chunk_id,location_id,stream_id,bucket_id,object_key,detail) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(task_id,subject,code) DO NOTHING").bind(task).bind(&issue.subject).bind(&issue.code).bind(issue.chunk_id).bind(issue.location_id).bind(issue.stream_id).bind(issue.bucket_id).bind(&issue.key).bind(&issue.detail)
}
pub(super) fn integrity_checkpoint(
    task: Uuid,
    cursor: String,
    detail: Value,
    processed: i64,
    done: bool,
) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET cursor=$2,detail=$3,processed=$4,updated_at=now(),state=CASE WHEN $5 AND state='running' THEN 'completed' ELSE state END WHERE id=$1").bind(task).bind(cursor).bind(detail).bind(processed).bind(done)
}
fn integrity_ranges(stream: Id, after: i64) -> Rows<'static, IntegrityRange> {
    sqlx::query_as("SELECT e.*,c.state,c.raw_size FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=$1 AND e.offset_bytes>$2 ORDER BY e.offset_bytes LIMIT 65").bind(stream).bind(after)
}
pub(super) fn integrity_current<'q>(bucket: Uuid, key: &'q str, stream: Id) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM objects WHERE bucket_id=$1 AND object_key=$2 AND stream_id=$3)").bind(bucket).bind(key).bind(stream)
}
pub(super) fn integrity_task_exists(task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=$1 AND task_type='integrity')")
        .bind(task)
}
pub(super) fn integrity_findings<'q>(
    task: Uuid,
    after: Id,
    limit: i64,
    code: Option<&'q str>,
) -> Scalar<'q, Value> {
    sqlx::query_scalar("SELECT to_jsonb(i)||jsonb_build_object('id',i.id::text,'chunk_id',i.chunk_id::text) FROM integrity_issues i WHERE task_id=$1 AND id>$2 AND ($4::text IS NULL OR code=$4) ORDER BY id LIMIT $3").bind(task).bind(after).bind(limit).bind(code)
}
pub(super) fn integrity_groups(task: Uuid) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT code,count(*)::text FROM integrity_issues WHERE task_id=$1 GROUP BY code ORDER BY code").bind(task)
}
pub(super) fn integrity_issue_source(
    task: Uuid,
    issue: Id,
) -> Rows<'static, (Option<Id>, Option<Id>)> {
    sqlx::query_as("SELECT chunk_id,stream_id FROM integrity_issues WHERE task_id=$1 AND id=$2")
        .bind(task)
        .bind(issue)
}
pub(super) async fn integrity_upper_object(
    db: &mut sqlx::PgConnection,
    detail: &Detail,
) -> Result<Option<(Uuid, String)>> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT o.bucket_id,o.object_key FROM objects o WHERE o.stream_id IS NOT NULL",
    );
    integrity_scope(&mut query, detail);
    query.push(" ORDER BY o.bucket_id DESC,o.object_key DESC LIMIT 1");
    Ok(query.build_query_as().fetch_optional(db).await?)
}
pub(super) fn integrity_chunks(
    d: &Detail,
    after: Id,
    limit: i64,
) -> Result<sqlx::QueryBuilder<sqlx::Postgres>> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at FROM chunks c LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE c.state='ready' AND c.id>",
    );
    query.push_bind(after).push(" AND c.id<=").push_bind(d.upper_chunk_id.parse::<Id>()?).push(" AND EXISTS(SELECT 1 FROM extents e JOIN objects o ON o.stream_id=e.stream_id WHERE e.chunk_id=c.id");
    integrity_scope(&mut query, d);
    query.push(") ORDER BY c.id LIMIT ").push_bind(limit);
    Ok(query)
}

pub(super) fn integrity_chunk_stream(chunk: Id, d: &Detail) -> sqlx::QueryBuilder<sqlx::Postgres> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT o.stream_id FROM extents e JOIN objects o ON o.stream_id=e.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE c.state='ready' AND c.id=",
    );
    query.push_bind(chunk);
    integrity_scope(&mut query, d);
    query.push(" LIMIT 1");
    query
}

pub(super) async fn physical_source(
    db: &mut sqlx::PgConnection,
    chunk: Id,
    inspection: bool,
) -> Result<PhysicalSource> {
    if let Some(physical) = sqlx::query_as("SELECT c.id,l.encoding_id AS current_encoding_id,l.encoding_id AS location_id,c.storage_id,c.hash,c.raw_size,l.standalone_size AS standalone_size,l.encryption_algorithm AS encryption_algorithm,l.block_key_id,l.standalone_compressed AS standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE c.id=$1 AND l.state='ready' AND c.state='ready' AND ($2 OR l.quarantined_at IS NULL)").bind(chunk).bind(inspection).fetch_optional(&mut *db).await? { return Ok(PhysicalSource::Chunk(physical)); }
    let pack = sqlx::query_as("SELECT p.* FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE c.id=$1 AND c.state='ready' AND p.state='ready' AND ($2 OR p.quarantined_at IS NULL)").bind(chunk).bind(inspection).fetch_optional(&mut *db).await?.context(crate::model::Error::SourceUnavailable)?;
    Ok(PhysicalSource::Pack(pack))
}

pub(super) fn integrity_scope(query: &mut sqlx::QueryBuilder<sqlx::Postgres>, d: &Detail) {
    if let Some(storage_id) = d.storage_id {
        query
            .push(
                " AND EXISTS (SELECT 1 FROM buckets sb WHERE sb.id=o.bucket_id AND sb.storage_id=",
            )
            .push_bind(storage_id)
            .push(")");
    }
    if let Some(bucket) = d.bucket_id {
        query.push(" AND o.bucket_id=").push_bind(bucket);
    }
    if let Some(key) = &d.key {
        query.push(" AND o.object_key=").push_bind(key);
    }
    if let Some((bucket, key)) = &d.upper_object {
        query
            .push(" AND (o.bucket_id,o.object_key)<=(")
            .push_bind(*bucket)
            .push(",")
            .push_bind(key)
            .push(")");
    }
}

use crate::app::StoredStream;
use crate::model::integrity::{Cursor, Detail, Finding, IntegrityStore, MappingRange};
use crate::worker::integrity::Request as IntegrityRequest;
#[derive(sqlx::FromRow)]
struct IntegrityRange {
    offset_bytes: i64,
    length: i32,
    source_offset: i32,
    fragment_id: Option<Id>,
    chunk_id: Option<Id>,
    state: Option<String>,
    raw_size: Option<i32>,
}

#[async_trait::async_trait]
impl IntegrityStore for Backend {
    async fn start(
        &self,
        input: IntegrityRequest,
        bucket: Option<Bucket>,
        node: Option<Uuid>,
    ) -> Result<(Uuid, usize)> {
        transactions::start_integrity(&self.pool, self.single, input, bucket, node).await
    }
    async fn chunks(&self, d: &Detail, after: Id, limit: i64) -> Result<Vec<Chunk>> {
        Ok(integrity_chunks(d, after, limit)?
            .build_query_as()
            .fetch_all(&self.pool)
            .await?)
    }
    async fn mapping(
        &self,
        d: &Detail,
        cursor: &Cursor,
    ) -> Result<Option<(StoredStream, Uuid, String)>> {
        let mut query = sqlx::QueryBuilder::new(
            "SELECT s.*,o.bucket_id AS visible_bucket,o.object_key AS visible_key FROM objects o JOIN streams s ON s.id=o.stream_id WHERE true",
        );
        integrity_scope(&mut query, d);
        if let Some(stream) = cursor.stream {
            query.push(" AND o.stream_id=").push_bind(stream);
        } else if let (Some(bucket), Some(key)) = (cursor.bucket, &cursor.key) {
            query
                .push(" AND (o.bucket_id,o.object_key)>(")
                .push_bind(bucket)
                .push(",")
                .push_bind(key)
                .push(")");
        }
        query.push(" ORDER BY o.bucket_id,o.object_key LIMIT 1");
        #[derive(sqlx::FromRow)]
        struct Object {
            #[sqlx(flatten)]
            stream: StoredStream,
            visible_bucket: Uuid,
            visible_key: String,
        }
        let row: Option<Object> = query.build_query_as().fetch_optional(&self.pool).await?;
        Ok(row.map(|r| (r.stream, r.visible_bucket, r.visible_key)))
    }
    async fn ranges(&self, stream: Id, after: i64) -> Result<Vec<MappingRange>> {
        Ok(integrity_ranges(stream, after)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| MappingRange {
                offset_bytes: r.offset_bytes,
                length: r.length,
                source_offset: r.source_offset,
                fragment_id: r.fragment_id,
                chunk_id: r.chunk_id,
                state: r.state,
                raw_size: r.raw_size,
            })
            .collect())
    }
    async fn current(&self, bucket: Uuid, key: &str, stream: Id) -> Result<bool> {
        Ok(integrity_current(bucket, key, stream)
            .fetch_one(&self.pool)
            .await?)
    }
    async fn chunk_stream(&self, chunk: Id, d: &Detail) -> Result<Option<Id>> {
        Ok(integrity_chunk_stream(chunk, d)
            .build_query_scalar()
            .fetch_optional(&self.pool)
            .await?)
    }
    async fn checkpoint(
        &self,
        id: Execution,
        cursor: String,
        detail: Detail,
        issues: Vec<Finding>,
    ) -> Result<()> {
        transactions::checkpoint_integrity(&self.pool, id, cursor, detail, issues).await
    }
    async fn exists(&self, task: Uuid) -> Result<bool> {
        Ok(integrity_task_exists(task).fetch_one(&self.pool).await?)
    }
    async fn report_task(&self, task: Uuid) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT to_jsonb(t) FROM tasks t WHERE id=$1 AND task_type='integrity'",
        )
        .bind(task)
        .fetch_optional(&self.pool)
        .await?
        .map(|value| task_value(value, self.single)))
    }
    async fn report_upper(&self, task: Uuid) -> Result<Id> {
        Ok(
            sqlx::query_scalar("SELECT COALESCE((SELECT id FROM integrity_issues WHERE task_id=$1 ORDER BY id DESC LIMIT 1),'00000000-0000-0000-0000-000000000000'::uuid)")
                .bind(task)
                .fetch_one(&self.pool)
                .await?,
        )
    }
    async fn report_page(&self, task: Uuid, after: Id, upper: Id) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT to_jsonb(i)||jsonb_build_object('id',i.id::text,'chunk_id',i.chunk_id::text) FROM integrity_issues i WHERE task_id=$1 AND id>$2 AND id<=$3 ORDER BY id LIMIT 100")
            .bind(task).bind(after).bind(upper).fetch_all(&self.pool).await?)
    }
    async fn findings(
        &self,
        task: Uuid,
        after: Id,
        limit: i64,
        code: Option<&str>,
    ) -> Result<Vec<Value>> {
        Ok(integrity_findings(task, after, limit, code)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn groups(&self, task: Uuid) -> Result<Vec<(String, String)>> {
        Ok(integrity_groups(task).fetch_all(&self.pool).await?)
    }
    async fn objects(
        &self,
        id: Uuid,
        issue: Id,
        after: Option<(Uuid, String)>,
    ) -> Result<Vec<Value>> {
        let row: Option<(Option<Id>, Option<Id>)> = integrity_issue_source(id, issue)
            .fetch_optional(&self.pool)
            .await?;
        let (chunk, stream) = row.ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        let mut query = sqlx::QueryBuilder::new(
            "SELECT jsonb_build_object('bucket_id',o.bucket_id,'bucket',b.name,'key',o.object_key,'version',o.stream_id) FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE ",
        );
        if let Some(chunk) = chunk {
            query
                .push(
                    "EXISTS(SELECT 1 FROM extents e WHERE e.stream_id=o.stream_id AND e.chunk_id=",
                )
                .push_bind(chunk)
                .push(")");
        } else {
            query.push("o.stream_id=").push_bind(stream);
        }
        if let Some((bucket, key)) = after {
            query
                .push(" AND (o.bucket_id,o.object_key)>(")
                .push_bind(bucket)
                .push(",")
                .push_bind(key)
                .push(")");
        }
        query.push(" ORDER BY o.bucket_id,o.object_key LIMIT 101");
        let rows: Vec<Value> = query.build_query_scalar().fetch_all(&self.pool).await?;
        Ok(rows)
    }
}

pub(super) fn unpacking_all() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='unpack' AND detail->>'all'='true' AND state<>'completed')",
    )
}
pub(super) fn pause_pack_creation(paused: bool) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET pack_creation_paused=$1").bind(paused)
}
pub(super) fn maintenance_policy_paused<'q>(policy: &'q str) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT CASE WHEN c.policy='gc' THEN m.gc_paused ELSE c.paused END FROM maintenance_controls c CROSS JOIN mokyu_meta m WHERE c.policy=$1").bind(policy)
}
pub(super) fn maintenance_controls() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT to_jsonb(c)||jsonb_build_object('paused',CASE WHEN c.policy='gc' THEN m.gc_paused ELSE c.paused END,'latest',latest.value,'active',active.value) FROM maintenance_controls c CROSS JOIN mokyu_meta m LEFT JOIN LATERAL (SELECT to_jsonb(t) value FROM tasks t WHERE coalesce(t.detail->>'policy',t.task_type)=c.policy ORDER BY created_at DESC,id DESC LIMIT 1) latest ON true LEFT JOIN LATERAL (SELECT to_jsonb(t) value FROM tasks t WHERE coalesce(t.detail->>'policy',t.task_type)=c.policy AND t.state IN ('queued','running','paused') ORDER BY created_at,id LIMIT 1) active ON true ORDER BY c.policy",
    )
}
pub(super) fn preparing_pack_count() -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT count(*)::text FROM packs WHERE state='preparing'")
}
pub(super) fn pause_gc(paused: bool) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET gc_paused=$1").bind(paused)
}
pub(super) fn pause_maintenance_policy<'q>(policy: &'q str, paused: bool) -> Statement<'q> {
    sqlx::query("UPDATE maintenance_controls SET paused=$2,updated_at=now() WHERE policy=$1")
        .bind(policy)
        .bind(paused)
}
pub(super) fn active_lifecycle_task<'q>(policy: &'q str) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE task_type=$1 AND state IN ('queued','running','paused') ORDER BY created_at LIMIT 1").bind(policy)
}
pub(super) fn lifecycle_storages() -> Rows<'static, (Uuid, String, i64)> {
    sqlx::query_as(
        "SELECT s.id,s.type,sp.revision FROM storages s JOIN storage_profiles sp ON sp.storage_id=s.id WHERE s.enabled ORDER BY s.created_at,s.id",
    )
}
pub(super) fn scope_history_task(child: Uuid, parent: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET parent_task_id=$2,batch_total=1,execution_scope='cluster' WHERE id=$1",
    )
    .bind(child)
    .bind(parent)
}
pub(super) fn scheduled_maintenance<'q>(policy: &'q str) -> Statement<'q> {
    sqlx::query("UPDATE maintenance_controls SET last_scheduled_at=now() WHERE policy=$1")
        .bind(policy)
}
pub(super) fn finish_lifecycle_task(
    id: Execution,
    count: i64,
    again: bool,
    report: Value,
) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET processed=processed+$2,state=CASE WHEN $3 THEN state ELSE 'completed' END,detail=detail||jsonb_build_object('report',$4::jsonb),updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$5 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())").bind(id.id).bind(count).bind(again).bind(report)
        .bind(id.token)
}
pub(super) fn maintenance_schedule() -> Rows<'static, (String, bool, Option<f64>)> {
    sqlx::query_as(
        "SELECT c.policy,CASE WHEN c.policy='gc' THEN m.gc_paused ELSE c.paused END,extract(epoch FROM now()-c.last_scheduled_at)::double precision FROM maintenance_controls c CROSS JOIN mokyu_meta m",
    )
}
pub(super) fn due_pack_reuse() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pack_changes WHERE reason='reuse' AND next_check_at<=now())",
    )
}

use crate::model::object::{ObjectChange, ObjectStore, ObjectTransfer, Outcome, StorageMigration};

pub(super) fn save_media_operation<'q>(id: Uuid, outcome: &'q Outcome) -> Statement<'q> {
    sqlx::query("UPDATE media_operations SET result=$2 WHERE id=$1 AND result IS NULL")
        .bind(id)
        .bind(sqlx::types::Json(outcome))
}

pub(super) fn insert_media_operation<'q>(
    user: Uuid,
    token: Option<Uuid>,
    client: Uuid,
    hash: &'q [u8],
) -> Statement<'q> {
    sqlx::query("INSERT INTO media_operations(user_id,token_id,client_id,request_hash) VALUES($1,$2,$3,$4) ON CONFLICT(user_id,token_id,client_id) DO NOTHING").bind(user).bind(token).bind(client).bind(hash)
}

pub(super) fn find_media_operation(
    user: Uuid,
    token: Option<Uuid>,
    client: Uuid,
) -> Rows<'static, (Uuid, Vec<u8>)> {
    sqlx::query_as("SELECT id,request_hash FROM media_operations WHERE user_id=$1 AND token_id IS NOT DISTINCT FROM $2 AND client_id=$3").bind(user).bind(token).bind(client)
}

pub(super) fn media_operation(id: Uuid) -> Scalar<'static, Option<Value>> {
    sqlx::query_scalar("SELECT result FROM media_operations WHERE id=$1").bind(id)
}

pub(super) fn blocked_media_buckets<'q>(buckets: &'q [Uuid]) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM buckets WHERE id=ANY($1) AND state<>'active')")
        .bind(buckets)
}

pub(super) fn lock_media_projects<'q>(buckets: &'q [Uuid]) -> Statement<'q> {
    sqlx::query("SELECT scope_id FROM quota_scopes WHERE scope='project' AND scope_id IN (SELECT project_id FROM buckets WHERE id=ANY($1)) ORDER BY scope_id FOR UPDATE").bind(buckets)
}

pub(super) fn media_bucket_scopes<'q>(buckets: &'q [Uuid]) -> Statement<'q> {
    sqlx::query("SELECT scope_id FROM quota_scopes WHERE scope='bucket' AND scope_id=ANY($1) ORDER BY scope_id").bind(buckets)
}

pub(super) fn object_storage(bucket: Uuid) -> Scalar<'static, Option<Uuid>> {
    sqlx::query_scalar("SELECT storage_id FROM buckets WHERE id=$1").bind(bucket)
}

pub(super) fn stream_public(stream: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT public_read FROM streams WHERE id=$1").bind(stream)
}

pub(super) fn tombstone_object<'q>(bucket: Uuid, key: &'q str, token: Uuid) -> Statement<'q> {
    sqlx::query(
        "UPDATE objects SET stream_id=NULL,write_token=$3 WHERE bucket_id=$1 AND object_key=$2",
    )
    .bind(bucket)
    .bind(key)
    .bind(token)
}

pub(super) fn move_stream<'q>(stream: Id, bucket: Uuid, key: &'q str) -> Statement<'q> {
    sqlx::query("UPDATE streams SET bucket_id=$2,object_key=$3 WHERE id=$1")
        .bind(stream)
        .bind(bucket)
        .bind(key)
}

pub(super) fn upsert_object<'q>(bucket: Uuid, key: &'q str, stream: Id) -> Statement<'q> {
    sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,$2,$3,$3) ON CONFLICT(bucket_id,object_key) DO UPDATE SET stream_id=excluded.stream_id,write_token=excluded.write_token").bind(bucket).bind(key).bind(stream)
}

pub(super) fn publish_copy_stream(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='ready',touched_at=now() WHERE id=$1 AND state='writing'")
        .bind(stream)
}

pub(super) fn abandon_copy_stream(stream: Id) -> Statement<'static> {
    sqlx::query(
        "UPDATE streams SET state='abandoned',touched_at=now() WHERE id=$1 AND state='writing'",
    )
    .bind(stream)
}

pub(super) fn lock_migration_task(task: Uuid, token: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()) FOR UPDATE").bind(task).bind(token)
}

pub(super) fn publish_migration_stream(stream: Id, task: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='ready',owner_task_id=NULL,owner_lease_token=NULL,touched_at=now() WHERE id=$1 AND state='writing' AND owner_task_id=$2 AND owner_lease_token=$3").bind(stream).bind(task).bind(token)
}

pub(super) fn complete_migration_task(task: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='completed',processed=processed+1,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())").bind(task).bind(token)
}

pub(super) fn abandon_migration_stream(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_lease_token=NULL,touched_at=now() WHERE id=$1 AND state='writing'").bind(stream)
}

#[async_trait::async_trait]
impl ObjectStore for Backend {
    async fn browse(
        &self,
        authority: &Permit,
        bucket: Uuid,
        filter: &mut CatalogFilter,
        cursor: Option<CatalogCursor>,
        sizes: (Option<i64>, Option<i64>),
    ) -> Result<Vec<MediaItem>> {
        transactions::browse_catalog(&self.pool, authority, bucket, filter, cursor, sizes).await
    }
    async fn chunks(&self, stream: Id, after: i64, limit: i64) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT jsonb_build_object('id',c.id::text,'offset_bytes',e.offset_bytes::text,'length',e.length,'source_offset',e.source_offset,'raw_size',c.raw_size,'stored_size',l.standalone_size,'independent_size_hint',c.standalone_size,'payload_size',c.standalone_size-CASE WHEN c.encryption_algorithm='none' THEN 0 ELSE 16 END,'compression',CASE WHEN c.standalone_compressed THEN 'zstd' ELSE 'none' END,'algorithm',c.encryption_algorithm,'block_key_id',c.block_key_id,'pack_id',c.pack_id::text,'source',CASE WHEN l.encoding_id IS NOT NULL THEN 'chunk' WHEN c.pack_id IS NOT NULL THEN 'pack' ELSE 'pending' END,'reads',COALESCE(a.reads,0)::text,'range_reads',COALESCE(a.range_reads,0)::text) FROM extents e JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' LEFT JOIN chunk_access_stats a ON a.chunk_id=c.id WHERE e.stream_id=$1 AND e.offset_bytes>$2 ORDER BY e.offset_bytes LIMIT $3").bind(stream).bind(after).bind(limit).fetch_all(&self.pool).await?)
    }

    async fn change(&self, input: ObjectChange) -> Result<bool> {
        transactions::change_object(&self.pool, input).await
    }
    async fn begin_operation(
        &self,
        principal: &Principal,
        user: Uuid,
        client: Uuid,
        hash: &[u8],
    ) -> Result<(Uuid, Vec<u8>)> {
        transactions::begin_media_operation(&self.pool, principal, user, client, hash).await
    }
    async fn operation(&self, id: Uuid) -> Result<Option<Outcome>> {
        media_operation(id)
            .fetch_one(&self.pool)
            .await?
            .map(serde_json::from_value)
            .transpose()
            .map_err(Into::into)
    }
    async fn fail_operation(&self, id: Uuid, outcome: &Outcome) -> Result<()> {
        save_media_operation(id, outcome)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn audit_batch(&self, bucket: Uuid, detail: Value, partial: bool) -> Result<()> {
        transactions::audit_object_batch(&self.pool, bucket, detail, partial).await
    }
    async fn storage(&self, bucket: Uuid) -> Result<Option<Uuid>> {
        Ok(object_storage(bucket)
            .fetch_optional(&self.pool)
            .await?
            .flatten())
    }
    async fn move_object(&self, permit: &Permit, input: ObjectTransfer<'_>) -> Result<()> {
        transactions::move_object(&self.pool, permit, input).await
    }
    async fn publish_copy(
        &self,
        permit: &Permit,
        input: ObjectTransfer<'_>,
        stream: Id,
        public: bool,
        expected_public: Option<bool>,
    ) -> Result<()> {
        transactions::publish_copy(&self.pool, permit, input, stream, public, expected_public).await
    }
    async fn queue_migration(&self, permit: &Permit, job: &StorageMigration) -> Result<()> {
        transactions::queue_storage_migration(&self.pool, permit, job).await
    }
    async fn complete_migration(
        &self,
        task: Uuid,
        token: Uuid,
        stream: Id,
        job: &StorageMigration,
    ) -> Result<()> {
        transactions::complete_storage_migration(&self.pool, task, token, stream, job).await
    }
    async fn abandon_copy(&self, stream: Id) -> Result<()> {
        abandon_copy_stream(stream).execute(&self.pool).await?;
        Ok(())
    }
    async fn abandon_migration(&self, stream: Id) -> Result<()> {
        abandon_migration_stream(stream).execute(&self.pool).await?;
        Ok(())
    }
}

use crate::model::object::{CatalogCursor, CatalogFilter, MediaItem};
use sqlx::{PgConnection, QueryBuilder, Row};

pub(super) fn catalog_search_settings() -> sqlx::RawSql {
    sqlx::raw_sql("SET LOCAL statement_timeout='2s'; SET LOCAL work_mem='4MB'")
}
impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for MediaItem {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            object_key: row.try_get("object_key")?,
            size: row.try_get("size")?,
            content_type: row.try_get("content_type")?,
            public_read: row.try_get("public_read")?,
            modified_at: row.try_get("modified_at")?,
        })
    }
}
fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}
fn invalid() -> anyhow::Error {
    s3s::s3_error!(InvalidArgument).into()
}
pub(super) async fn browse_catalog(
    db: &mut PgConnection,
    bucket: Uuid,
    q: &mut CatalogFilter,
    cursor: Option<CatalogCursor>,
    (min, max): (Option<i64>, Option<i64>),
) -> Result<Vec<MediaItem>> {
    if q.mode == "contains" && !q.q.is_empty() {
        let usable: bool = sqlx::query_scalar("SELECT $1 COLLATE \"default\" ~ '[[:alnum:]]{3}'")
            .bind(&q.q)
            .fetch_one(&mut *db)
            .await?;
        if !usable {
            q.mode = "prefix".into();
        }
    }
    let mut sql = QueryBuilder::<Postgres>::new(
        "SELECT stream_id id,object_key,catalog_size::text size,catalog_type content_type,catalog_public public_read,catalog_modified modified_at FROM objects WHERE stream_id IS NOT NULL AND bucket_id=",
    );
    sql.push_bind(bucket);
    if !q.prefix.is_empty() {
        sql.push(" AND object_key>=").push_bind(&q.prefix);
        if let Some(end) = crate::app::listing::successor(&q.prefix) {
            sql.push(" AND object_key<").push_bind(end);
        }
    }
    if !q.q.is_empty() {
        match q.mode.as_str() {
            "exact" => {
                sql.push(" AND object_key=")
                    .push_bind(format!("{}{}", q.prefix, q.q));
            }
            "prefix" => {
                let prefix = format!("{}{}", q.prefix, q.q);
                sql.push(" AND object_key>=").push_bind(prefix.clone());
                if let Some(end) = crate::app::listing::successor(&prefix) {
                    sql.push(" AND object_key<").push_bind(end);
                }
            }
            _ => {
                let pattern = format!("%{}%", escape(&q.q));
                sql.push(" AND object_key COLLATE \"default\" ILIKE ")
                    .push_bind(pattern.clone());
                if q.search_in == "name" {
                    sql.push(
                        " AND regexp_replace(object_key,'^.*/','') COLLATE \"default\" ILIKE ",
                    )
                    .push_bind(pattern);
                }
            }
        }
    }
    if let Some(category) = &q.category {
        sql.push(" AND catalog_category=").push_bind(category);
    }
    if let Some(public) = q.public {
        sql.push(" AND catalog_public=").push_bind(public);
    }
    if let Some(min) = min {
        sql.push(" AND catalog_size>=").push_bind(min);
    }
    if let Some(max) = max {
        sql.push(" AND catalog_size<=").push_bind(max);
    }
    if let Some(since) = q.since {
        sql.push(" AND catalog_modified>=").push_bind(since);
    }
    if let Some(until) = q.until {
        sql.push(" AND catalog_modified<=").push_bind(until);
    }
    let column = match q.sort.as_str() {
        "size" => "catalog_size",
        "modified" => "catalog_modified",
        _ => "object_key",
    };
    let comparison = if q.order == "asc" { " > " } else { " < " };
    if let Some(c) = cursor {
        if column == "object_key" {
            sql.push(" AND object_key")
                .push(comparison)
                .push_bind(c.key);
        } else {
            sql.push(" AND (")
                .push(column)
                .push(",object_key)")
                .push(comparison)
                .push("(");
            if column == "catalog_size" {
                sql.push_bind(
                    c.size
                        .parse::<i64>()
                        .ok()
                        .filter(|n| *n >= 0)
                        .ok_or_else(invalid)?,
                );
            } else {
                sql.push_bind(c.modified);
            }
            sql.push(",").push_bind(c.key).push(")");
        }
    }
    let direction = if q.order == "asc" { " ASC" } else { " DESC" };
    sql.push(" ORDER BY ").push(column).push(direction);
    if column != "object_key" {
        sql.push(",object_key").push(direction);
    }
    sql.push(" LIMIT ").push_bind(q.limit as i64 + 1);
    Ok(sql.build_query_as().fetch_all(db).await?)
}

pub(super) fn quota_membership(user: Option<Uuid>, project: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT scope FROM project_members WHERE user_id=$1 AND project_id=$2")
        .bind(user)
        .bind(project)
}

pub(super) fn quota_scope<'q>(scope: &'q str, id: Uuid, visible: bool) -> Rows<'q, Quota> {
    sqlx::query_as("SELECT scope,scope_id,CASE WHEN $3 THEN used_bytes::text END used_bytes,CASE WHEN $3 THEN reserved_bytes::text END reserved_bytes,CASE WHEN $3 THEN inflight_bytes::text END inflight_bytes,CASE WHEN $3 THEN object_count::text END object_count,CASE WHEN $3 THEN bucket_count::text END bucket_count,logical_bytes_limit::text logical_bytes_limit,inflight_bytes_limit::text inflight_bytes_limit,bucket_count_limit::text bucket_count_limit FROM quota_scopes WHERE scope=$1 AND scope_id=$2").bind(scope).bind(id).bind(visible)
}

pub(super) fn update_quota<'q>(
    scope: &'q str,
    id: Uuid,
    bytes: Option<i64>,
    inflight: Option<i64>,
    buckets: Option<i64>,
) -> Statement<'q> {
    sqlx::query("UPDATE quota_scopes SET logical_bytes_limit=$3,inflight_bytes_limit=$4,bucket_count_limit=$5 WHERE scope=$1 AND scope_id=$2").bind(scope).bind(id).bind(bytes).bind(inflight).bind(buckets)
}

use crate::model::quota::Quota;
impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for Quota {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            scope: row.try_get("scope")?,
            scope_id: row.try_get("scope_id")?,
            used_bytes: row.try_get("used_bytes")?,
            reserved_bytes: row.try_get("reserved_bytes")?,
            inflight_bytes: row.try_get("inflight_bytes")?,
            object_count: row.try_get("object_count")?,
            bucket_count: row.try_get("bucket_count")?,
            logical_bytes_limit: row.try_get("logical_bytes_limit")?,
            inflight_bytes_limit: row.try_get("inflight_bytes_limit")?,
            bucket_count_limit: row.try_get("bucket_count_limit")?,
        })
    }
}

use crate::model::metrics::{
    AccessBatch, MetricsStore, RuntimeHistory, RuntimePoint, ScopeQuery, StorageUsage,
    UsageSnapshot, WorkSummary,
};

#[derive(Default, sqlx::FromRow)]
pub(super) struct SavedStorage {
    pub(super) as_of: Option<DateTime<Utc>>,
    pub(super) data: Option<sqlx::types::Json<StorageUsage>>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for WorkSummary {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            task_type: row.try_get("task_type")?,
            state: row.try_get("state")?,
            processed: row.try_get("processed")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

pub(super) fn inventory_isolation() -> Statement<'static> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
}

pub(super) fn inventory_limits() -> sqlx::RawSql {
    sqlx::raw_sql(
        "SET LOCAL work_mem='16MB'; SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL lock_timeout='1s'",
    )
}

pub(super) fn inventory_timeout(timeout: String) -> Statement<'static> {
    sqlx::query("SELECT set_config('statement_timeout',$1,true)").bind(timeout)
}

pub(super) fn inventory_time() -> Scalar<'static, DateTime<Utc>> {
    sqlx::query_scalar("SELECT transaction_timestamp()")
}

pub(super) fn inventory_buckets() -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT jsonb_build_object('id',b.id,'name',b.name,'objects',count(o.stream_id),'logical_bytes',COALESCE(sum(s.size),0))
         FROM buckets b LEFT JOIN objects o ON o.bucket_id=b.id AND o.stream_id IS NOT NULL
         LEFT JOIN streams s ON s.id=o.stream_id
         GROUP BY GROUPING SETS ((b.id,b.name),()) ORDER BY b.id NULLS FIRST LIMIT 1002")
}

pub(super) fn inventory_chunks() -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT jsonb_build_object(
            'states',COALESCE(jsonb_object_agg(state,n),'{}'),
            'stored_bytes',COALESCE(sum(bytes) FILTER(WHERE state IN ('ready','deleting')),0),
            'unconfirmed_bytes',COALESCE(sum(bytes) FILTER(WHERE state IN ('uploading','failed')),0))
         FROM (SELECT state,count(*) n,COALESCE(sum(standalone_size),0) bytes FROM chunks GROUP BY state) c")
}

pub(super) fn inventory_physical() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(to_jsonb(p)),'[]') FROM (SELECT source_type,state,count(*) objects,COALESCE(sum(stored_size),0) stored_bytes FROM (SELECT 'chunk' source_type,state,standalone_size AS stored_size FROM chunk_encodings UNION ALL SELECT 'pack',state,stored_size FROM packs UNION ALL SELECT 'raw',state,size FROM raw_sources) sources GROUP BY source_type,state ORDER BY source_type,state) p",
    )
}

pub(super) fn inventory_live() -> Scalar<'static, Value> {
    sqlx::query_scalar("WITH refs AS (
            SELECT e.chunk_id,sum(e.length) bytes FROM objects o JOIN extents e ON e.stream_id=o.stream_id
            WHERE e.chunk_id IS NOT NULL GROUP BY e.chunk_id),
         raw_live AS (SELECT id,size FROM raw_sources r WHERE EXISTS(SELECT 1 FROM raw_ranges e JOIN objects o ON o.stream_id=e.stream_id WHERE e.source_id=r.id)), physical AS (
            SELECT l.standalone_size bytes,l.standalone_size-CASE WHEN l.encryption_algorithm='none' THEN 0 ELSE 16 END payload
            FROM refs r JOIN chunks c ON c.id=r.chunk_id JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready'
            UNION ALL SELECT p.stored_size,p.stored_size-CASE WHEN p.encryption_algorithm='none' THEN 0 ELSE 16 END
            FROM packs p WHERE state='ready' AND EXISTS(SELECT 1 FROM refs r JOIN chunks c ON c.id=r.chunk_id WHERE c.pack_id=p.id) UNION ALL SELECT size,size FROM raw_live)
         SELECT jsonb_build_object('raw_objects',(SELECT COUNT(*) FROM raw_live),'chunks',count(*),'reference_bytes',(COALESCE(sum(r.bytes),0)+(SELECT COALESCE(SUM(e.length),0) FROM raw_ranges e JOIN objects o ON o.stream_id=e.stream_id)),
            'raw_bytes',(COALESCE(sum(c.raw_size),0)+(SELECT COALESCE(SUM(size),0) FROM raw_live)),'pending_raw_bytes',COALESCE(sum(c.raw_size) FILTER(WHERE c.pack_id IS NULL AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=c.id AND state='ready')),0),'stored_bytes',(SELECT COALESCE(sum(bytes),0) FROM physical),
            'payload_bytes',(SELECT COALESCE(sum(payload),0) FROM physical))
         FROM refs r JOIN chunks c ON c.id=r.chunk_id")
}

pub(super) fn inventory_unreferenced() -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT jsonb_build_object('chunks',count(*),'stored_bytes',COALESCE(sum(standalone_size),0),
            'eligible_chunks',count(*) FILTER(WHERE unreferenced_at<now()-((SELECT (left(sp.unreferenced_grace,-1)::bigint*CASE right(sp.unreferenced_grace,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=c.storage_id))*interval '1 second' AND owner_stream IS NULL),
            'eligible_bytes',COALESCE(sum(standalone_size) FILTER(WHERE unreferenced_at<now()-((SELECT (left(sp.unreferenced_grace,-1)::bigint*CASE right(sp.unreferenced_grace,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=c.storage_id))*interval '1 second' AND owner_stream IS NULL),0))
         FROM chunks c WHERE state IN ('ready','failed','deleting') AND unreferenced_at IS NOT NULL
            AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)")
}

pub(super) fn inventory_reclaimable() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT jsonb_build_object('bytes',COALESCE(sum(bytes),0),'eligible',COALESCE(sum(bytes) FILTER(WHERE since<now()-((SELECT (left(sp.unreferenced_grace,-1)::bigint*CASE right(sp.unreferenced_grace,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=p.storage_id))*interval '1 second'),0)) FROM (SELECT l.standalone_size bytes,COALESCE(l.unreferenced_at,c.unreferenced_at) since,l.storage_id FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE l.state IN ('retired','deleting') OR (l.state='ready' AND c.unreferenced_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)) UNION ALL SELECT stored_size,unreferenced_at,storage_id FROM packs WHERE state IN ('retired','deleting') UNION ALL SELECT size,unreferenced_at,storage_id FROM raw_sources r WHERE r.owner_stream IS NULL AND NOT EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.kind='raw' AND n.location_id=r.id AND l.expires_at>clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=r.id) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=r.id) AND NOT EXISTS(SELECT 1 FROM uploads WHERE raw_source_id=r.id)) p",
    )
}

pub(super) fn inventory_tasks() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT COALESCE(jsonb_object_agg(state,n),'{}') FROM (SELECT state,count(*) n FROM tasks GROUP BY state) t",
    )
}

pub(super) fn inventory_uploads() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT COALESCE(jsonb_object_agg(state,n),'{}') FROM (SELECT state,count(*) n FROM uploads WHERE state IN ('active','completing') GROUP BY state) u",
    )
}

pub(super) fn inventory_cleanup(tasks: f64) -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT jsonb_object_agg(source_type,jsonb_build_object('eligible',n,'oldest_at',oldest)) FROM (
            SELECT 'chunks' source_type,count(*) n,min(deleted_at) oldest FROM chunks c
                WHERE state='deleted' AND deleted_at<now()-((SELECT (left(sp.deleted_chunk_retention,-1)::bigint*CASE right(sp.deleted_chunk_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=c.storage_id))*interval '1 second'
                AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)
            UNION ALL SELECT 'uploads',count(*),min(touched_at) FROM uploads u
                WHERE state IN ('completed','aborted') AND touched_at<now()-((SELECT (left(sp.upload_retention,-1)::bigint*CASE right(sp.upload_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=(SELECT storage_id FROM buckets WHERE id=u.bucket_id)))*interval '1 second'
                AND NOT EXISTS(SELECT 1 FROM parts WHERE upload_id=u.id)
            UNION ALL SELECT 'tasks',count(*),min(updated_at) FROM tasks
                WHERE state='completed' AND updated_at<now()-$1*interval '1 second'
            UNION ALL SELECT 'integrity_issues',count(*),min(t.updated_at) FROM integrity_issues i JOIN tasks t ON t.id=i.task_id
                WHERE t.state='completed' AND t.updated_at<now()-$1*interval '1 second'
            UNION ALL SELECT 'sessions',count(*),min(expires_at) FROM sessions WHERE expires_at<now()
            UNION ALL SELECT 'packs',count(*),min(deleted_at) FROM packs p WHERE state='deleted' AND deleted_at<now()-((SELECT (left(sp.deleted_chunk_retention,-1)::bigint*CASE right(sp.deleted_chunk_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=p.storage_id))*interval '1 second'
            UNION ALL SELECT 'chunk_encodings',count(*),min(deleted_at) FROM chunk_encodings ce WHERE state='deleted' AND deleted_at<now()-((SELECT (left(sp.deleted_chunk_retention,-1)::bigint*CASE right(sp.deleted_chunk_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=ce.storage_id))*interval '1 second'
        ) backlog").bind(tasks)
}

pub(super) fn inventory_database() -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT jsonb_build_object('table',relname,'total_bytes',pg_total_relation_size(relid),
            'index_bytes',pg_indexes_size(relid),'live_rows_estimate',n_live_tup,
            'dead_rows_estimate',n_dead_tup,'last_autovacuum',last_autovacuum,'last_autoanalyze',last_autoanalyze)
         FROM pg_stat_user_tables WHERE schemaname='public'
            AND relname IN ('chunks','extents','raw_sources','raw_ranges','streams','objects','uploads','parts','fragments','multipart_files','tasks','sessions','session_refresh_tokens','integrity_issues','chunk_encodings','packs','pack_members','pack_maintenance','pack_changes','pack_inputs','chunk_access_stats','chunk_access_windows','pack_access_windows','pack_member_access_windows','integrity_packs','pending_uploads','cache_pins')
         ORDER BY relname")
}

pub(super) fn storage_inventory_limits() -> sqlx::RawSql {
    sqlx::raw_sql(
        "SET LOCAL work_mem='8MB'; SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL statement_timeout='5s'",
    )
}

pub(super) fn storage_inventory_buckets(storage: Uuid) -> Rows<'static, (i64, i64, i64)> {
    sqlx::query_as("SELECT count(DISTINCT b.id),count(o.stream_id),coalesce(sum(s.size),0)::bigint FROM buckets b LEFT JOIN objects o ON o.bucket_id=b.id AND o.stream_id IS NOT NULL LEFT JOIN streams s ON s.id=o.stream_id WHERE b.storage_id=$1").bind(storage)
}

pub(super) fn storage_inventory_chunks(storage: Uuid) -> Rows<'static, (i64, i64)> {
    sqlx::query_as("SELECT count(*),coalesce(sum(standalone_size),0)::bigint FROM chunks WHERE storage_id=$1 AND state IN ('ready','deleting')").bind(storage)
}

pub(super) fn storage_inventory_physical(storage: Uuid) -> Rows<'static, (i64, i64)> {
    sqlx::query_as("SELECT count(*),coalesce(sum(bytes),0)::bigint FROM (SELECT standalone_size::bigint bytes FROM chunk_encodings WHERE storage_id=$1 AND state IN ('ready','retired','deleting') UNION ALL SELECT stored_size::bigint FROM packs WHERE storage_id=$1 AND state IN ('ready','retired','deleting') UNION ALL SELECT size FROM raw_sources WHERE storage_id=$1 AND state<>'writing') p").bind(storage)
}

pub(super) fn active_usage_task() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM tasks WHERE task_type='usage' AND state IN ('queued','running','paused') ORDER BY created_at LIMIT 1",
    )
}

async fn complete_usage(pool: &PgPool, id: Execution) -> Result<bool> {
    let changed = sqlx::query("UPDATE tasks SET state='completed',processed=processed+1,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())")
            .bind(id.id)
            .bind(id.token).execute(pool)
            .await?.rows_affected();
    Ok(changed == 1)
}

pub(super) fn usage_scope_buckets(
    user: Option<Uuid>,
    admin: bool,
    token: Option<Uuid>,
    bucket: Option<Uuid>,
    project: Option<Uuid>,
) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT b.id FROM buckets b LEFT JOIN user_bucket_access a ON a.bucket_id=b.id AND a.user_id=$1 LEFT JOIN token_bucket_access t ON t.bucket_id=b.id AND t.token_id=$3 WHERE ($2 OR 'storage.inspect'=ANY(CASE WHEN $3::uuid IS NOT NULL THEN t.actions ELSE a.actions END)) AND ($4::uuid IS NULL OR b.id=$4) AND ($5::uuid IS NULL OR b.project_id=$5) ORDER BY b.id").bind(user).bind(admin).bind(token).bind(bucket).bind(project)
}

pub(super) fn usage_exists<'q>(key: &'q str) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM usage_summary WHERE id=$1)").bind(key)
}

pub(super) fn usage_scope_lock() -> Statement<'static> {
    sqlx::query("SELECT pg_advisory_xact_lock(734922709851030)")
}

pub(super) fn usage_scope_evict(keep: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM usage_summary WHERE id IN (SELECT id FROM usage_summary WHERE id<>'global' ORDER BY requested_at DESC,id OFFSET $1)").bind(keep)
}

pub(super) fn usage_scope_create<'q>(key: &'q str, ids: &'q [Uuid]) -> Statement<'q> {
    sqlx::query("INSERT INTO usage_summary(id,bucket_ids) VALUES($1,$2) ON CONFLICT DO NOTHING")
        .bind(key)
        .bind(ids)
}

pub(super) fn usage_scope_touch<'q>(key: &'q str) -> Statement<'q> {
    sqlx::query("UPDATE usage_summary SET requested_at=now() WHERE id=$1 AND requested_at<now()-interval '1 hour'").bind(key)
}

pub(super) fn usage_saved<'q>(key: &'q str) -> Rows<'q, SavedStorage> {
    sqlx::query_as("SELECT as_of,data FROM usage_summary WHERE id=$1").bind(key)
}

pub(super) fn usage_history<'q>(
    key: &'q str,
) -> Rows<'q, (DateTime<Utc>, sqlx::types::Json<StorageUsage>)> {
    sqlx::query_as("SELECT snapshot_at,data FROM (SELECT snapshot_at,data FROM usage_history WHERE scope_id=$1 ORDER BY snapshot_at DESC LIMIT 2048) p ORDER BY snapshot_at").bind(key)
}

pub(super) const USAGE_SUMMARY: &str = r#"WITH ranges AS MATERIALIZED (
    SELECT o.bucket_id,b.project_id,e.chunk_id,
           range_agg(int8range(e.source_offset::bigint,e.source_offset::bigint+e.length)) AS spans
    FROM objects o JOIN buckets b ON b.id=o.bucket_id JOIN extents e ON e.stream_id=o.stream_id
    WHERE e.chunk_id IS NOT NULL GROUP BY o.bucket_id,b.project_id,e.chunk_id
), boundaries AS (
    SELECT chunk_id,lower(span) AS point FROM ranges CROSS JOIN LATERAL unnest(spans) span
    UNION SELECT chunk_id,upper(span) FROM ranges CROSS JOIN LATERAL unnest(spans) span
), segments AS (
    SELECT chunk_id,point AS lo,lead(point) OVER(PARTITION BY chunk_id ORDER BY point) AS hi FROM boundaries
), coverage AS MATERIALIZED (
    SELECT s.chunk_id,s.lo,s.hi,r.bucket_id,r.project_id
    FROM segments s JOIN ranges r ON r.chunk_id=s.chunk_id AND r.spans @> int8range(s.lo,s.hi)
    WHERE s.hi IS NOT NULL
), project_counts AS (
    SELECT chunk_id,lo,count(DISTINCT project_id) AS n FROM coverage GROUP BY chunk_id,lo
), bucket_counts AS (
    SELECT chunk_id,lo,project_id,count(*) AS n FROM coverage GROUP BY chunk_id,lo,project_id
), allocated AS (
    SELECT c.bucket_id,c.chunk_id,sum((hi-lo)::numeric/p.n/b.n) AS raw_bytes
    FROM coverage c JOIN project_counts p USING(chunk_id,lo)
    JOIN bucket_counts b USING(chunk_id,lo,project_id) GROUP BY c.bucket_id,c.chunk_id
), chunk_unique AS (
    SELECT chunk_id,sum(hi-lo) AS bytes FROM (SELECT DISTINCT chunk_id,lo,hi FROM coverage) u GROUP BY chunk_id
), sources AS MATERIALIZED (
    SELECT c.id AS chunk_id,CASE WHEN l.encoding_id IS NOT NULL THEN 'chunk' WHEN p.id IS NOT NULL THEN 'pack' ELSE 'pending' END AS source_type,
           coalesce(l.encoding_id,p.id,c.id) AS source_id,coalesce(l.standalone_size,p.stored_size) AS stored_size,u.bytes
    FROM chunk_unique u JOIN chunks c ON c.id=u.chunk_id
    LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready'
    LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready'
), source_totals AS MATERIALIZED (
    SELECT source_type,source_id,sum(bytes) AS raw_bytes,max(stored_size) AS stored_size FROM sources GROUP BY source_type,source_id
), attribution AS MATERIALIZED (
    SELECT a.bucket_id,a.chunk_id,a.raw_bytes,s.source_type,s.source_id,
           a.raw_bytes/t.raw_bytes*t.stored_size AS encoded_bytes
    FROM allocated a JOIN sources s USING(chunk_id) JOIN source_totals t USING(source_type,source_id)
), scopes AS MATERIALIZED (
    SELECT id,bucket_ids FROM usage_summary
), scope_ranges AS (
    SELECT s.id,r.chunk_id,range_agg(r.spans) AS spans FROM scopes s JOIN ranges r ON s.bucket_ids IS NULL OR r.bucket_id=ANY(s.bucket_ids)
    GROUP BY s.id,r.chunk_id
), scope_unique AS (
    SELECT r.id,count(DISTINCT r.chunk_id) AS chunks,
           count(DISTINCT s.source_id) FILTER(WHERE s.source_type='pack') AS packs,
           sum(upper(span)-lower(span)) AS bytes,
           coalesce(sum(upper(span)-lower(span)) FILTER(WHERE s.source_type='pack'),0) AS packed_bytes,
           coalesce(sum(upper(span)-lower(span)) FILTER(WHERE s.source_type='pending'),0) AS pending_bytes
    FROM scope_ranges r JOIN sources s USING(chunk_id) CROSS JOIN LATERAL unnest(r.spans) span GROUP BY r.id
), scope_allocated AS (
    SELECT s.id,sum(a.raw_bytes) AS raw_bytes,coalesce(sum(a.encoded_bytes),0) AS encoded_bytes,
           bool_and(a.encoded_bytes IS NOT NULL) AS complete
    FROM scopes s JOIN attribution a ON s.bucket_ids IS NULL OR a.bucket_id=ANY(s.bucket_ids) GROUP BY s.id
), scope_logical AS (
    SELECT s.id,count(b.id) AS buckets,coalesce(sum(q.used_bytes),0) AS bytes,coalesce(sum(q.object_count),0) AS objects
    FROM scopes s LEFT JOIN buckets b ON s.bucket_ids IS NULL OR b.id=ANY(s.bucket_ids)
    LEFT JOIN quota_scopes q ON q.scope='bucket' AND q.scope_id=b.id GROUP BY s.id
), raw_references AS (
    SELECT DISTINCT o.bucket_id,b.project_id,r.id AS source_id,r.size
    FROM objects o JOIN buckets b ON b.id=o.bucket_id JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id
), raw_project_counts AS (
    SELECT source_id,COUNT(DISTINCT project_id) AS n FROM raw_references GROUP BY source_id
), raw_allocated AS (
    SELECT r.*,r.size::numeric/p.n/COUNT(*) OVER(PARTITION BY r.source_id,r.project_id) AS attributed
    FROM raw_references r JOIN raw_project_counts p USING(source_id)
), raw_scopes AS (
    SELECT id,SUM(size) AS bytes,SUM(attributed) AS attributed FROM (
        SELECT s.id AS id,r.source_id,MAX(r.size) AS size,SUM(r.attributed) AS attributed
        FROM scopes s JOIN raw_allocated r ON s.bucket_ids IS NULL OR r.bucket_id=ANY(s.bucket_ids) GROUP BY s.id,r.source_id
    ) scoped_raw GROUP BY id
), raw_inventory AS (
    SELECT r.*,
        EXISTS(SELECT 1 FROM raw_ranges e JOIN objects o ON o.stream_id=e.stream_id WHERE e.source_id=r.id) AS selected,
        r.owner_stream IS NULL AND NOT EXISTS(SELECT 1 FROM raw_ranges e WHERE e.source_id=r.id) AS garbage
    FROM raw_sources r
), raw_physical AS (
    SELECT COALESCE(SUM(CASE WHEN state<>'writing' THEN size ELSE 0 END),0) AS indexed_bytes,
        COALESCE(SUM(CASE WHEN selected THEN size ELSE 0 END),0) AS selected_bytes,
        COALESCE(SUM(CASE WHEN NOT selected AND NOT garbage AND state<>'writing' THEN size ELSE 0 END),0) AS retained_bytes,
        COALESCE(SUM(CASE WHEN NOT selected AND garbage THEN size ELSE 0 END),0) AS gc_bytes,
        COALESCE(SUM(CASE WHEN state='writing' THEN size ELSE 0 END),0) AS unconfirmed_bytes,
        COUNT(*) AS raw_objects FROM raw_inventory
), physical AS (
    SELECT 'chunk' AS source_type,l.encoding_id AS source_id,l.state,l.standalone_size AS stored_size,
           l.state IN ('retired','deleting') OR (c.unreferenced_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)) AS garbage
    FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE l.state IN ('ready','retired','deleting','uploading')
    UNION ALL SELECT 'pack',id,state,stored_size,state IN ('retired','deleting') FROM packs WHERE state<>'deleted'
), bridge AS (
    SELECT jsonb_build_object(
        'indexed_bytes',(coalesce(sum(p.stored_size) FILTER(WHERE state IN ('ready','retired','deleting')),0)+(SELECT indexed_bytes FROM raw_physical))::text,
        'selected_bytes',(coalesce(sum(p.stored_size) FILTER(WHERE t.source_id IS NOT NULL),0)+(SELECT selected_bytes FROM raw_physical))::text,
        'retained_bytes',(coalesce(sum(p.stored_size) FILTER(WHERE t.source_id IS NULL AND state IN ('ready','retired','deleting') AND NOT garbage),0)+(SELECT retained_bytes FROM raw_physical))::text,
        'gc_bytes',(coalesce(sum(p.stored_size) FILTER(WHERE t.source_id IS NULL AND garbage),0)+(SELECT gc_bytes FROM raw_physical))::text,
        'unconfirmed_bytes',(coalesce(sum(p.stored_size) FILTER(WHERE state IN ('preparing','uploading')),0)+(SELECT unconfirmed_bytes FROM raw_physical))::text,
        'chunk_objects',count(*) FILTER(WHERE p.source_type='chunk' AND state IN ('ready','retired','deleting'))::text,
        'raw_objects',(SELECT raw_objects FROM raw_physical)::text,
        'pack_objects',count(*) FILTER(WHERE p.source_type='pack' AND state IN ('ready','retired','deleting'))::text
    ) AS data FROM physical p LEFT JOIN source_totals t ON t.source_type=p.source_type AND t.source_id=p.source_id AND t.source_type<>'pending'
), summaries AS (
    SELECT l.id,jsonb_build_object(
        'buckets',l.buckets::text,'objects',l.objects::text,'logical_bytes',l.bytes::text,
        'unique_bytes',(coalesce(u.bytes,0)+COALESCE(r.bytes,0))::text,'attributed_raw_bytes',round((coalesce(a.raw_bytes,0)+COALESCE(r.attributed,0)),6)::text,
        'encoded_bytes',CASE WHEN coalesce(a.complete,true) THEN round((coalesce(a.encoded_bytes,0)+COALESCE(r.attributed,0)),6)::text END,
        'encoded_known_bytes',round((coalesce(a.encoded_bytes,0)+COALESCE(r.attributed,0)),6)::text,
        'local_savings', (l.bytes-(coalesce(u.bytes,0)+COALESCE(r.bytes,0)))::text,
        'shared_savings',round((coalesce(u.bytes,0)+COALESCE(r.bytes,0))-(coalesce(a.raw_bytes,0)+COALESCE(r.attributed,0)),6)::text,
        'encoding_savings',CASE WHEN coalesce(a.complete,true) THEN round((coalesce(a.raw_bytes,0)+COALESCE(r.attributed,0))-(coalesce(a.encoded_bytes,0)+COALESCE(r.attributed,0)),6)::text END,
        'chunks',coalesce(u.chunks,0)::text,'packs',coalesce(u.packs,0)::text,
        'packed_bytes',coalesce(u.packed_bytes,0)::text,'pending_bytes',coalesce(u.pending_bytes,0)::text,
        'physical',CASE WHEN l.id='global' THEN (SELECT data FROM bridge) END
    ) AS data FROM scope_logical l LEFT JOIN scope_unique u USING(id) LEFT JOIN scope_allocated a USING(id) LEFT JOIN raw_scopes r ON r.id=l.id
)
UPDATE usage_summary i SET as_of=transaction_timestamp(),data=s.data FROM summaries s WHERE s.id=i.id;
"#;

pub(super) fn usage_scope_expire(retention: f64) -> Statement<'static> {
    sqlx::query("DELETE FROM usage_summary WHERE id<>'global' AND requested_at<now()-$1*interval '1 second'").bind(retention)
}

pub(super) fn collect_usage() -> sqlx::RawSql {
    sqlx::raw_sql(USAGE_SUMMARY)
}

pub(super) fn usage_history_append(interval: f64) -> Statement<'static> {
    sqlx::query("INSERT INTO usage_history(scope_id,snapshot_at,data) SELECT i.id,i.as_of,i.data FROM usage_summary i WHERE i.as_of IS NOT NULL AND NOT EXISTS(SELECT 1 FROM usage_history h WHERE h.scope_id=i.id AND h.snapshot_at>i.as_of-$1*interval '1 second') ON CONFLICT DO NOTHING").bind(interval)
}

pub(super) fn usage_history_expire(retention: f64) -> Statement<'static> {
    sqlx::query("DELETE FROM usage_history WHERE snapshot_at<now()-$1*interval '1 second'")
        .bind(retention)
}

pub(super) fn runtime_history() -> Scalar<'static, sqlx::types::Json<RuntimePoint>> {
    sqlx::query_scalar(
        "SELECT data FROM (SELECT sampled_at,data FROM runtime_samples WHERE data->>'scope' IS NULL AND (SELECT deployment_mode FROM mokyu_meta)='single' ORDER BY sampled_at DESC LIMIT 2048) h ORDER BY sampled_at",
    )
}

pub(super) fn runtime_work() -> Rows<'static, WorkSummary> {
    sqlx::query_as(
        "SELECT id,coalesce(detail->>'policy',task_type) task_type,state,processed::text,updated_at FROM tasks WHERE state IN ('queued','running','paused') ORDER BY CASE state WHEN 'running' THEN 0 WHEN 'queued' THEN 1 ELSE 2 END,updated_at DESC LIMIT 5",
    )
}

pub(super) fn runtime_pending() -> Rows<'static, (String, String, Option<DateTime<Utc>>, String)> {
    sqlx::query_as(
        "SELECT count(*)::text,coalesce(sum(staged_size),0)::text,min(created_at),count(*) FILTER(WHERE last_error IS NOT NULL)::text FROM pending_uploads",
    )
}

pub(super) fn runtime_sample(
    at: DateTime<Utc>,
    data: sqlx::types::Json<RuntimePoint>,
) -> Statement<'static> {
    sqlx::query("INSERT INTO runtime_samples(sampled_at,data) VALUES($1,$2)")
        .bind(at)
        .bind(data)
}

pub(super) fn runtime_expire(retention: f64) -> Statement<'static> {
    sqlx::query("DELETE FROM runtime_samples WHERE sampled_at<now()-$1*interval '1 second'")
        .bind(retention)
}

pub(super) fn access_chunks<'q>(
    ids: &'q [Id],
    reads: &'q [i64],
    ranges: &'q [i64],
    bytes: &'q [i64],
) -> Statement<'q> {
    sqlx::query("INSERT INTO chunk_access_stats(chunk_id,reads,range_reads,bytes,last_read_at) SELECT c.id,sum(s.reads),sum(s.ranges),sum(s.bytes),now() FROM unnest($1::uuid[],$2::bigint[],$3::bigint[],$4::bigint[]) s(id,reads,ranges,bytes) JOIN chunks c ON c.id=s.id GROUP BY c.id ON CONFLICT(chunk_id) DO UPDATE SET reads=chunk_access_stats.reads+EXCLUDED.reads,range_reads=chunk_access_stats.range_reads+EXCLUDED.range_reads,bytes=chunk_access_stats.bytes+EXCLUDED.bytes,last_read_at=EXCLUDED.last_read_at").bind(ids).bind(reads).bind(ranges).bind(bytes)
}

pub(super) fn access_chunk_windows<'q>(
    ids: &'q [Id],
    hours: Vec<DateTime<Utc>>,
    reads: Vec<i64>,
    ranges: Vec<i64>,
    bytes: Vec<i64>,
    origins: Vec<i64>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO chunk_access_windows(chunk_id,window_start,reads,range_reads,bytes,range_origin_reads) SELECT c.id,s.hour,s.reads,s.ranges,s.bytes,s.origins FROM unnest($1::uuid[],$2::timestamptz[],$3::bigint[],$4::bigint[],$5::bigint[],$6::bigint[]) s(id,hour,reads,ranges,bytes,origins) JOIN chunks c ON c.id=s.id ON CONFLICT(window_start,chunk_id) DO UPDATE SET reads=chunk_access_windows.reads+EXCLUDED.reads,range_reads=chunk_access_windows.range_reads+EXCLUDED.range_reads,bytes=chunk_access_windows.bytes+EXCLUDED.bytes,range_origin_reads=chunk_access_windows.range_origin_reads+EXCLUDED.range_origin_reads").bind(ids).bind(hours).bind(reads).bind(ranges).bind(bytes).bind(origins)
}

pub(super) fn access_packs(
    ids: Vec<Id>,
    hours: Vec<DateTime<Utc>>,
    reads: Vec<i64>,
    bytes: Vec<i64>,
    partial: Vec<i64>,
    useful: Vec<i64>,
    partial_bytes: Vec<i64>,
) -> Statement<'static> {
    sqlx::query("INSERT INTO pack_access_windows(pack_id,window_start,downloads,downloaded_bytes,partial_downloads,useful_bytes,partial_bytes) SELECT p.id,s.hour,s.reads,s.bytes,s.partial,s.useful,s.partial_bytes FROM unnest($1::uuid[],$2::timestamptz[],$3::bigint[],$4::bigint[],$5::bigint[],$6::bigint[],$7::bigint[]) s(id,hour,reads,bytes,partial,useful,partial_bytes) JOIN packs p ON p.id=s.id ON CONFLICT(window_start,pack_id) DO UPDATE SET downloads=pack_access_windows.downloads+EXCLUDED.downloads,downloaded_bytes=pack_access_windows.downloaded_bytes+EXCLUDED.downloaded_bytes,partial_downloads=pack_access_windows.partial_downloads+EXCLUDED.partial_downloads,useful_bytes=pack_access_windows.useful_bytes+EXCLUDED.useful_bytes,partial_bytes=pack_access_windows.partial_bytes+EXCLUDED.partial_bytes,updated_at=now()").bind(ids).bind(hours).bind(reads).bind(bytes).bind(partial).bind(useful).bind(partial_bytes)
}

pub(super) fn access_members(
    packs: Vec<Id>,
    chunks: Vec<Id>,
    hours: Vec<DateTime<Utc>>,
    counts: Vec<i64>,
) -> Statement<'static> {
    sqlx::query("INSERT INTO pack_member_access_windows(pack_id,chunk_id,window_start,downloads) SELECT p.id,c.id,s.hour,s.n FROM unnest($1::uuid[],$2::uuid[],$3::timestamptz[],$4::bigint[]) s(p,c,hour,n) JOIN packs p ON p.id=s.p JOIN chunks c ON c.id=s.c ON CONFLICT(window_start,pack_id,chunk_id) DO UPDATE SET downloads=pack_member_access_windows.downloads+EXCLUDED.downloads").bind(packs).bind(chunks).bind(hours).bind(counts)
}

pub(super) fn observation_limits() -> sqlx::RawSql {
    sqlx::raw_sql("SET LOCAL lock_timeout='100ms'; SET LOCAL statement_timeout='1s'")
}

pub(super) fn node_access(node: Uuid) -> Rows<'static, (Uuid, i64, Vec<u8>, DateTime<Utc>)> {
    sqlx::query_as("SELECT boot_id,sequence,request_hash,gap_since FROM node_access WHERE node_id=$1 FOR UPDATE").bind(node)
}

pub(super) fn node_access_saved<'a>(
    node: Uuid,
    boot: Uuid,
    sequence: i64,
    hash: &'a [u8],
    now: DateTime<Utc>,
    gap: DateTime<Utc>,
) -> Statement<'a> {
    sqlx::query("INSERT INTO node_access(node_id,boot_id,sequence,request_hash,received_at,gap_since) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(node_id) DO UPDATE SET boot_id=EXCLUDED.boot_id,sequence=EXCLUDED.sequence,request_hash=EXCLUDED.request_hash,received_at=EXCLUDED.received_at,gap_since=EXCLUDED.gap_since")
        .bind(node).bind(boot).bind(sequence).bind(hash).bind(now).bind(gap)
}

pub(super) fn access_coverage(dropped: bool, stale: f64) -> Statement<'static> {
    sqlx::query("WITH coverage AS (SELECT count(*) expected,count(a.node_id) FILTER(WHERE a.boot_id=s.boot_id AND s.credential_generation=n.credential_generation AND s.expires_at>clock_timestamp() AND a.received_at>=now()-$2*interval '1 second') reported,min(a.received_at) flushed,max(a.gap_since) gap FROM nodes n LEFT JOIN node_sessions s ON s.node_id=n.id LEFT JOIN node_access a ON a.node_id=n.id WHERE n.enabled AND n.role IN ('data','node')) UPDATE mokyu_meta m SET access_coverage_since=CASE WHEN $1 OR m.access_flushed_at<now()-$2*interval '1 second' OR (m.deployment_mode='cluster' AND c.expected<>c.reported) THEN now() WHEN m.deployment_mode='cluster' THEN GREATEST(m.access_coverage_since,c.gap) ELSE m.access_coverage_since END,access_flushed_at=CASE WHEN m.deployment_mode<>'cluster' THEN now() WHEN c.expected=c.reported THEN COALESCE(c.flushed,now()) ELSE LEAST(m.access_flushed_at,now()-($2+1)*interval '1 second') END FROM coverage c WHERE m.singleton").bind(dropped).bind(stale)
}

async fn expire_access(pool: &PgPool, retention: f64, batch: i64) -> Result<()> {
    for sql in [
        "DELETE FROM chunk_access_windows WHERE (window_start,chunk_id) IN (SELECT window_start,chunk_id FROM chunk_access_windows WHERE window_start<now()-$1*interval '1 second' ORDER BY window_start LIMIT $2)",
        "DELETE FROM pack_access_windows WHERE (window_start,pack_id) IN (SELECT window_start,pack_id FROM pack_access_windows WHERE window_start<now()-$1*interval '1 second' ORDER BY window_start LIMIT $2)",
        "DELETE FROM pack_member_access_windows WHERE (window_start,pack_id,chunk_id) IN (SELECT window_start,pack_id,chunk_id FROM pack_member_access_windows WHERE window_start<now()-$1*interval '1 second' ORDER BY window_start LIMIT $2)",
    ] {
        if let Err(e) = sqlx::query(sql)
            .bind(retention)
            .bind(batch)
            .execute(pool)
            .await
        {
            tracing::warn!(error=%e,"access statistics cleanup failed");
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl MetricsStore for Backend {
    async fn inventory(
        &self,
        metrics: &crate::config::MetricsSettings,
        maintenance: &crate::config::Maintenance,
    ) -> Result<Value> {
        transactions::collect_inventory(&self.pool, metrics, maintenance).await
    }
    async fn storage_inventory(&self, storage: Uuid) -> Result<Value> {
        transactions::collect_storage(&self.pool, storage).await
    }
    async fn usage(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        limit: usize,
    ) -> Result<UsageSnapshot> {
        transactions::read_usage(&self.pool, principal, scope, limit).await
    }
    async fn runtime(&self, principal: &Principal) -> Result<RuntimeHistory> {
        transactions::read_runtime(&self.pool, principal).await
    }
    async fn sample_runtime(&self, point: RuntimePoint, retention: f64) -> Result<()> {
        transactions::sample_runtime(&self.pool, point, retention).await
    }
    async fn flush_access(&self, pending: AccessBatch, stale: f64) -> Result<()> {
        transactions::flush_access(&self.pool, pending, stale).await
    }
    async fn expire_access(&self, retention: f64, batch: i64) -> Result<()> {
        expire_access(&self.pool, retention, batch).await
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for PackSummary {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            state: row.try_get("state")?,
            compressed: row.try_get("compressed")?,
            raw_size: row.try_get("raw_size")?,
            stored_size: row.try_get("stored_size")?,
            member_count: row.try_get("member_count")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for PackMember {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            chunk_id: row.try_get("chunk_id")?,
            ordinal: row.try_get("ordinal")?,
            raw_size: row.try_get("raw_size")?,
            visible_bytes: row.try_get("visible_bytes")?,
            current_source: row.try_get("current_source")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for PackObject {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            bucket_id: row.try_get("bucket_id")?,
            bucket_name: row.try_get("bucket_name")?,
            key: row.try_get("key")?,
            version: row.try_get("version")?,
            size: row.try_get("size")?,
            content_type: row.try_get("content_type")?,
            public_read: row.try_get("public_read")?,
        })
    }
}

use crate::model::packing::{PackMember, PackObject, PackSummary};

pub(super) fn scoped_pack_limits() -> sqlx::RawSql {
    sqlx::raw_sql(
        "SET LOCAL statement_timeout='5s';SET LOCAL lock_timeout='1s';SET LOCAL work_mem='4MB'",
    )
}

pub(super) fn scoped_packs<'q>(
    after: Id,
    ids: &'q [Uuid],
    admin: bool,
    all: bool,
    state: Option<&'q str>,
    limit: i64,
) -> Rows<'q, PackSummary> {
    sqlx::query_as("SELECT p.id::text,p.state,p.pack_compressed AS compressed,CASE WHEN $3 THEN p.raw_size::text END raw_size,CASE WHEN $3 THEN p.stored_size::text END stored_size,CASE WHEN $3 THEN p.member_count END member_count,p.created_at FROM packs p WHERE p.id>$1 AND ($5::text IS NULL OR p.state=$5) AND ($3 OR p.state='ready') AND ($4 OR EXISTS(SELECT 1 FROM pack_members m JOIN chunks c ON c.id=m.chunk_id JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.stream_id=s.id WHERE m.pack_id=p.id AND c.pack_id=p.id AND o.bucket_id=ANY($2))) ORDER BY p.id LIMIT $6").bind(after).bind(ids).bind(admin).bind(all).bind(state).bind(limit)
}

pub(super) fn scoped_pack<'q>(
    id: Id,
    ids: &'q [Uuid],
    admin: bool,
    all: bool,
) -> Rows<'q, PackSummary> {
    sqlx::query_as("SELECT p.id::text,p.state,p.pack_compressed AS compressed,CASE WHEN $3 THEN p.raw_size::text END raw_size,CASE WHEN $3 THEN p.stored_size::text END stored_size,CASE WHEN $3 THEN p.member_count END member_count,p.created_at FROM packs p WHERE p.id=$1 AND ($3 OR p.state='ready') AND ($4 OR EXISTS(SELECT 1 FROM pack_members m JOIN chunks c ON c.id=m.chunk_id JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.stream_id=s.id WHERE m.pack_id=p.id AND c.pack_id=p.id AND o.bucket_id=ANY($2)))").bind(id).bind(ids).bind(admin).bind(all)
}

pub(super) fn scoped_pack_members<'q>(
    id: Id,
    ids: &'q [Uuid],
    admin: bool,
    all: bool,
    after: Id,
    limit: i64,
) -> Rows<'q, PackMember> {
    sqlx::query_as("SELECT c.id::text chunk_id,CASE WHEN $3 THEN m.ordinal END ordinal,CASE WHEN $3 THEN c.raw_size END raw_size,coalesce((SELECT sum(upper(span)-lower(span))::text FROM unnest(v.spans) span),'0') AS visible_bytes,coalesce(c.pack_id=m.pack_id AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=c.id AND state='ready'),false) AS current_source FROM pack_members m JOIN chunks c ON c.id=m.chunk_id LEFT JOIN LATERAL (SELECT range_agg(int8range(e.source_offset::bigint,e.source_offset::bigint+e.length)) spans FROM extents e JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.stream_id=s.id WHERE e.chunk_id=c.id AND o.bucket_id=ANY($2)) v ON true WHERE m.pack_id=$1 AND c.id>$5 AND ($4 OR (v.spans IS NOT NULL AND c.pack_id=m.pack_id)) ORDER BY c.id LIMIT $6").bind(id).bind(ids).bind(admin).bind(all).bind(after).bind(limit)
}

pub(super) fn scoped_pack_objects<'q>(
    id: Id,
    ids: Vec<Uuid>,
    user: Option<Uuid>,
    token: Option<Uuid>,
    admin: bool,
    last_bucket: Option<Uuid>,
    last_key: Option<&'q String>,
) -> Rows<'q, PackObject> {
    sqlx::query_as("SELECT DISTINCT o.bucket_id,b.name AS bucket_name,o.object_key AS key,s.id AS version,s.size::text,coalesce(s.metadata->>'content_type','application/octet-stream') AS content_type,s.public_read FROM pack_members m JOIN chunks c ON c.id=m.chunk_id JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.stream_id=s.id JOIN buckets b ON b.id=o.bucket_id LEFT JOIN user_bucket_access a ON a.bucket_id=b.id AND a.user_id=$3 LEFT JOIN token_bucket_access t ON t.bucket_id=b.id AND t.token_id=$4 WHERE m.pack_id=$1 AND o.bucket_id=ANY($2) AND ($5 OR (c.pack_id=$1 AND 'bucket.list'=ANY(CASE WHEN $4::uuid IS NOT NULL THEN t.actions ELSE a.actions END))) AND ($6::uuid IS NULL OR (o.bucket_id,o.object_key)>($6,$7)) ORDER BY o.bucket_id,o.object_key LIMIT 101").bind(id).bind(ids).bind(user).bind(token).bind(admin).bind(last_bucket).bind(last_key)
}

use crate::model::maintenance::{MaintenanceExecution, MaintenancePreview};

pub(super) fn remove_maintenance_preview<'q>(
    actor: Option<Uuid>,
    token: Option<Uuid>,
    action: &'q str,
) -> Statement<'q> {
    sqlx::query("DELETE FROM maintenance_previews WHERE actor_id=$1 AND token_id IS NOT DISTINCT FROM $2 AND action=$3 AND task_id IS NULL").bind(actor).bind(token).bind(action)
}

pub(super) fn create_maintenance_preview<'q>(
    actor: Option<Uuid>,
    token: Option<Uuid>,
    action: &'q str,
    parameters: Value,
    fingerprint: String,
) -> Rows<'q, (Uuid, DateTime<Utc>)> {
    sqlx::query_as("INSERT INTO maintenance_previews(actor_id,token_id,action,parameters,fingerprint) VALUES($1,$2,$3,$4,$5) RETURNING id,expires_at").bind(actor).bind(token).bind(action).bind(parameters).bind(fingerprint)
}

pub(super) fn lock_maintenance_preview<'q>(
    id: Uuid,
    actor: Option<Uuid>,
    token: Option<Uuid>,
    action: &'q str,
) -> Rows<'q, (Value, String, Option<Uuid>)> {
    sqlx::query_as("SELECT parameters,fingerprint,task_id FROM maintenance_previews WHERE id=$1 AND actor_id=$2 AND token_id IS NOT DISTINCT FROM $3 AND action=$4 AND expires_at>now() FOR UPDATE").bind(id).bind(actor).bind(token).bind(action)
}

pub(super) fn maintenance_preview_task(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=$1)").bind(id)
}

pub(super) fn record_maintenance_preview(id: Uuid, task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE maintenance_previews SET task_id=$2 WHERE id=$1")
        .bind(id)
        .bind(task)
}

const JOB_SQL: &str = "SELECT to_jsonb(t)||jsonb_build_object('bucket_name',b.name,'blocked',CASE WHEN c.paused OR (coalesce(c.policy,'')='gc' AND m.gc_paused) THEN 'PolicyPaused' WHEN m.maintenance AND t.task_type IN ('pack','unpack','purge','gc','upload','storage_migration') THEN 'MaintenanceMode' WHEN m.pack_creation_paused AND t.task_type='pack' AND t.detail->>'policy' IN ('pack','repack') THEN 'PackCreationStopped' END) FROM tasks t LEFT JOIN buckets b ON b.id=t.bucket_id CROSS JOIN mokyu_meta m LEFT JOIN maintenance_controls c ON c.policy=CASE WHEN t.task_type='pack' THEN t.detail->>'policy' ELSE t.task_type END";

use crate::model::task::JobQuery;

pub(super) fn task_page(
    q: JobQuery,
    cursor: Option<(DateTime<Utc>, Uuid)>,
    limit: i64,
) -> sqlx::QueryBuilder<Postgres> {
    let mut sql = sqlx::QueryBuilder::new(JOB_SQL);
    sql.push(" WHERE true");
    if let Some(value) = q.state {
        sql.push(" AND t.state=").push_bind(value);
    }
    if let Some(value) = q.policy {
        sql.push(" AND coalesce(t.detail->>'policy',t.task_type)=")
            .push_bind(value);
    }
    if let Some(value) = q.bucket {
        sql.push(" AND t.bucket_id=").push_bind(value);
    }
    if let Some(value) = q.storage {
        sql.push(" AND (b.storage_id=")
            .push_bind(value)
            .push(" OR t.detail->>'storage_id'=")
            .push_bind(value.to_string())
            .push(" OR t.detail->'storages' @> ")
            .push_bind(serde_json::json!([{"storage_id":value}]))
            .push(')');
    }
    if let Some(value) = q.actor {
        sql.push(" AND position(lower(")
            .push_bind(value)
            .push(") in lower(t.created_by_label))>0");
    }
    if let Some(cursor) = cursor {
        sql.push(" AND (t.created_at,t.id)<(")
            .push_bind(cursor.0)
            .push(",")
            .push_bind(cursor.1)
            .push(")");
    }
    sql.push(" ORDER BY t.created_at DESC,t.id DESC LIMIT ")
        .push_bind(limit);
    sql
}

pub(super) fn task_view(id: Uuid) -> sqlx::QueryBuilder<Postgres> {
    let mut query = sqlx::QueryBuilder::new(JOB_SQL);
    query.push(" WHERE t.id=").push_bind(id);
    query
}

pub(super) fn task_page_limits() -> sqlx::RawSql {
    sqlx::raw_sql("SET LOCAL statement_timeout='5s';SET LOCAL work_mem='4MB'")
}

pub(super) fn set_maintenance_mode(enabled: bool) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET maintenance=$1").bind(enabled)
}

pub(super) fn pause_maintenance_tasks() -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET state='paused',updated_at=now() WHERE task_type IN ('purge','pack','unpack','storage_migration','rewrite') AND state IN ('queued','running')",
    )
}

use crate::model::storage::{StorageCreation, StorageEdit, StorageRow, StorageView};
use crate::storage::model::Record as BackendRecord;
use crate::storage::profile::StorageProfile;

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for StorageView {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            storage_type: row.try_get("storage_type")?,
            path: row.try_get("path")?,
            endpoint: row.try_get("endpoint")?,
            bucket: row.try_get("bucket")?,
            region: row.try_get("region")?,
            prefix: row.try_get("prefix")?,
            block_key_id: row.try_get("block_key_id")?,
            profile_revision: row.try_get("profile_revision")?,
            read_concurrency: row.try_get("read_concurrency")?,
            upload_concurrency: row.try_get("upload_concurrency")?,
            control_concurrency: row.try_get("control_concurrency")?,
            enabled: row.try_get("enabled")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for StorageRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            r#type: row.try_get("type")?,
            path: row.try_get("path")?,
            endpoint_url: row.try_get("endpoint_url")?,
            remote_bucket_name: row.try_get("remote_bucket_name")?,
            region: row.try_get("region")?,
            prefix: row.try_get("prefix")?,
            access_key_encrypted: row.try_get("access_key_encrypted")?,
            secret_key_encrypted: row.try_get("secret_key_encrypted")?,
            identity_marker: row.try_get("identity_marker")?,
            block_key_id: row.try_get("block_key_id")?,
            enabled: row.try_get("enabled")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for StorageRecord {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            store_type: row.try_get("store_type")?,
            path: row.try_get("path")?,
            endpoint: row.try_get("endpoint")?,
            bucket: row.try_get("bucket")?,
            region: row.try_get("region")?,
            prefix: row.try_get("prefix")?,
            access: row.try_get("access")?,
            secret: row.try_get("secret")?,
            identity_marker: row.try_get("identity_marker")?,
            revision: row.try_get("revision")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::storage::profile::StorageProfile {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            storage_id: row.try_get("storage_id")?,
            revision: row.try_get("revision")?,
            cdc_enabled: row.try_get("cdc_enabled")?,
            deduplication_enabled: row.try_get("deduplication_enabled")?,
            write_processing: row.try_get("write_processing")?,
            write_durability: row.try_get("write_durability")?,
            write_flush_interval: format!("{}s", row.try_get::<i64, _>("write_flush_interval")?),
            write_backlog_max_age: format!("{}s", row.try_get::<i64, _>("write_backlog_max_age")?),
            background_scheduling: row.try_get("background_scheduling")?,
            public_cache_validation: row.try_get("public_cache_validation")?,
            cors_cache_enabled: row.try_get("cors_cache_enabled")?,
            public_cache_refresh_interval: row.try_get("public_cache_refresh_interval")?,
            public_cache_max_age: row.try_get("public_cache_max_age")?,
            read_source_validation: row.try_get("read_source_validation")?,
            source_validation_max_age: row.try_get("source_validation_max_age")?,
            read_integrity: row.try_get("read_integrity")?,
            file_delivery: row.try_get("file_delivery")?,
            file_delivery_prefix: row.try_get("file_delivery_prefix")?,
            connect_timeout: row.try_get("connect_timeout")?,
            request_timeout: row.try_get("request_timeout")?,
            max_retries: row.try_get("max_retries")?,
            retry_timeout: row.try_get("retry_timeout")?,
            priority_aging: row.try_get("priority_aging")?,
            min_storage_duration: row.try_get("min_storage_duration")?,
            unreferenced_grace: row.try_get("unreferenced_grace")?,
            deleted_chunk_retention: row.try_get("deleted_chunk_retention")?,
            upload_retention: row.try_get("upload_retention")?,
            multipart_idle_timeout: row.try_get("multipart_idle_timeout")?,
            min_free_space: row.try_get("min_free_space")?,
            cache_enabled: row.try_get("cache_enabled")?,
            cache_soft_limit: row.try_get("cache_soft_limit")?,
            cache_entries: row.try_get("cache_entries")?,
            upload_cache: row.try_get("upload_cache")?,
            upload_cache_size: row.try_get("upload_cache_size")?,
            upload_cache_overflow: row.try_get("upload_cache_overflow")?,
            compression_enabled: row.try_get("compression_enabled")?,
            compression_strategy: row.try_get("compression_strategy")?,
            compression_level: row.try_get("compression_level")?,
            compression_sample_level: row.try_get("compression_sample_level")?,
            compression_min_savings_percent: row.try_get("compression_min_savings_percent")?,
            compression_min_savings_bytes: row.try_get("compression_min_savings_bytes")?,
            pack_enabled: row.try_get("pack_enabled")?,
            pack_max_size: row.try_get("pack_max_size")?,
            pack_upload_flush_timeout: row.try_get("pack_upload_flush_timeout")?,
            pack_interval: row.try_get("pack_interval")?,
            pack_reuse_interval: row.try_get("pack_reuse_interval")?,
            pack_reclaim_interval: row.try_get("pack_reclaim_interval")?,
            pack_repack_interval: row.try_get("pack_repack_interval")?,
            pack_repack_cooldown: row.try_get("pack_repack_cooldown")?,
            pack_reclaim_min_savings_bytes: row.try_get("pack_reclaim_min_savings_bytes")?,
            pack_range_enabled: row.try_get("pack_range_enabled")?,
            pack_range_interval: row.try_get("pack_range_interval")?,
            pack_range_window: row.try_get("pack_range_window")?,
            pack_range_min_downloads: row.try_get("pack_range_min_downloads")?,
            pack_range_min_savings_percent: row.try_get("pack_range_min_savings_percent")?,
            pack_range_min_savings_bytes: row.try_get("pack_range_min_savings_bytes")?,
            pack_range_repack_after: row.try_get("pack_range_repack_after")?,
            pack_range_repack_window: row.try_get("pack_range_repack_window")?,
            pack_range_repack_retry_interval: row.try_get("pack_range_repack_retry_interval")?,
            pack_concurrency: row.try_get("pack_concurrency")?,
            integrity_concurrency: row.try_get("integrity_concurrency")?,
            integrity_rps: row.try_get("integrity_rps")?,
            integrity_bandwidth: row.try_get("integrity_bandwidth")?,
            integrity_batch_timeout: row.try_get("integrity_batch_timeout")?,
            read_concurrency: row.try_get("read_concurrency")?,
            upload_concurrency: row.try_get("upload_concurrency")?,
            control_concurrency: row.try_get("control_concurrency")?,
            pack_byte_budget: row.try_get("pack_byte_budget")?,
            integrity_byte_budget: row.try_get("integrity_byte_budget")?,
            maintenance_concurrency: row.try_get("maintenance_concurrency")?,
            maintenance_byte_budget: row.try_get("maintenance_byte_budget")?,
            migration_concurrency: row.try_get("migration_concurrency")?,
            migration_byte_budget: row.try_get("migration_byte_budget")?,
        })
    }
}

const PROFILE_COLUMNS: &[&str] = &[
    "cdc_enabled",
    "deduplication_enabled",
    "write_processing",
    "write_durability",
    "write_flush_interval",
    "write_backlog_max_age",
    "background_scheduling",
    "public_cache_validation",
    "cors_cache_enabled",
    "public_cache_refresh_interval",
    "public_cache_max_age",
    "read_source_validation",
    "source_validation_max_age",
    "read_integrity",
    "file_delivery",
    "file_delivery_prefix",
    "connect_timeout",
    "request_timeout",
    "max_retries",
    "retry_timeout",
    "priority_aging",
    "min_storage_duration",
    "unreferenced_grace",
    "deleted_chunk_retention",
    "upload_retention",
    "multipart_idle_timeout",
    "min_free_space",
    "cache_enabled",
    "cache_soft_limit",
    "cache_entries",
    "upload_cache",
    "upload_cache_size",
    "upload_cache_overflow",
    "compression_enabled",
    "compression_strategy",
    "compression_level",
    "compression_sample_level",
    "compression_min_savings_percent",
    "compression_min_savings_bytes",
    "pack_enabled",
    "pack_max_size",
    "pack_upload_flush_timeout",
    "pack_interval",
    "pack_reuse_interval",
    "pack_reclaim_interval",
    "pack_repack_interval",
    "pack_repack_cooldown",
    "pack_reclaim_min_savings_bytes",
    "pack_range_enabled",
    "pack_range_interval",
    "pack_range_window",
    "pack_range_min_downloads",
    "pack_range_min_savings_percent",
    "pack_range_min_savings_bytes",
    "pack_range_repack_after",
    "pack_range_repack_window",
    "pack_range_repack_retry_interval",
    "pack_concurrency",
    "integrity_concurrency",
    "integrity_rps",
    "integrity_bandwidth",
    "integrity_batch_timeout",
    "read_concurrency",
    "upload_concurrency",
    "control_concurrency",
    "pack_byte_budget",
    "integrity_byte_budget",
    "maintenance_concurrency",
    "maintenance_byte_budget",
    "migration_concurrency",
    "migration_byte_budget",
];

pub(super) fn save_storage_profile(
    profile: &crate::storage::profile::StorageProfile,
) -> Result<Statement<'static>> {
    let columns = PROFILE_COLUMNS.join(",");
    let requested = PROFILE_COLUMNS
        .iter()
        .map(|column| format!("requested.{column}"))
        .collect::<Vec<_>>()
        .join(",");
    let statement = format!(
        "UPDATE storage_profiles AS current SET ({columns})=(SELECT {requested} FROM jsonb_populate_record(current,$2::jsonb) AS requested),revision=revision+1,updated_at=now() WHERE current.storage_id=$1 AND current.revision=$3"
    );
    let mut value = serde_json::to_value(profile)?;
    value["write_flush_interval"] = serde_json::json!(profile.write_flush_seconds()?);
    value["write_backlog_max_age"] = serde_json::json!(profile.write_backlog_seconds()?);
    value["write_durability"] =
        serde_json::json!(profile.write_durability.as_deref().unwrap_or("durable"));
    Ok(sqlx::query(sqlx::AssertSqlSafe(statement))
        .bind(profile.storage_id)
        .bind(value)
        .bind(profile.revision))
}

pub(super) fn valid_storage_key(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM managed_keys WHERE id=$1 AND purpose='block' AND revoked_at IS NULL)").bind(id)
}

pub(super) fn storage_mode_lock() -> Rows<'static, (Uuid, String)> {
    sqlx::query_as(
        "SELECT deployment_id,deployment_mode FROM mokyu_meta WHERE singleton FOR UPDATE",
    )
}

pub(super) fn storage_relocation_ready() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT maintenance AND NOT EXISTS(SELECT 1 FROM tasks WHERE state IN ('queued','running','paused')) AND NOT EXISTS(SELECT 1 FROM uploads WHERE state IN ('active','completing')) AND NOT EXISTS(SELECT 1 FROM streams WHERE state='writing') AND NOT EXISTS(SELECT 1 FROM pending_uploads) AND NOT EXISTS(SELECT 1 FROM extents e JOIN streams s ON s.id=e.stream_id WHERE e.fragment_id IS NOT NULL AND s.state IN ('ready','writing')) AND NOT EXISTS(SELECT 1 FROM raw_sources WHERE state='writing' OR pending_upload OR durability<>'durable' OR medium='memory' OR upload_id IS NOT NULL) FROM mokyu_meta WHERE singleton AND deployment_mode='single'",
    )
}

pub(super) fn relocate_raw_sources(id: Uuid, medium: &'static str) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET medium=$1,revision=revision+1 WHERE storage_id=$2 AND medium IN ('local','shared_s3')").bind(medium).bind(id)
}

pub(super) fn storage_row_lock<'q>(name: &'q str) -> Rows<'q, crate::model::storage::StorageRow> {
    sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,access_key_encrypted,secret_key_encrypted,identity_marker,block_key_id,enabled FROM storages WHERE name=$1 FOR UPDATE").bind(name)
}

const VIEW_ORDERED: &str = "SELECT s.id,s.name,s.type AS storage_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.block_key_id,p.revision AS profile_revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.created_at,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id ORDER BY s.name";
const VIEW_BY_ID: &str = "SELECT s.id,s.name,s.type AS storage_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.block_key_id,p.revision AS profile_revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.created_at,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=$1";

pub(super) fn storage_profile_target<'q>(name: &'q str) -> Rows<'q, (Uuid, String, Option<Uuid>)> {
    sqlx::query_as("SELECT id,type,block_key_id FROM storages WHERE name=$1 FOR UPDATE").bind(name)
}

pub(super) fn insert_storage_record<'q>(
    record: &'q BackendRecord,
    key: Option<Uuid>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO storages(id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,identity_marker,block_key_id,access_key_encrypted,secret_key_encrypted) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)").bind(record.id).bind(&record.name).bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket_name).bind(&record.region).bind(&record.prefix).bind(&record.identity_marker).bind(key).bind(&record.access_key_encrypted).bind(&record.secret_key_encrypted)
}

pub(super) fn insert_storage_profile(
    id: Uuid,
    read: Option<i32>,
    upload: Option<i32>,
    control: Option<i32>,
    remote: bool,
) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_profiles(storage_id,read_concurrency,upload_concurrency,control_concurrency,cache_enabled,upload_cache,compression_enabled,pack_enabled,pack_range_enabled,min_free_space) VALUES($1,$2,$3,$4,$5,$5,$5,$5,$5,$6)").bind(id).bind(read).bind(upload).bind(control).bind(remote).bind((!remote).then_some("1GiB"))
}

pub(super) fn storage_profile_lock(id: Uuid) -> Rows<'static, StorageProfile> {
    sqlx::query_as("SELECT storage_id,revision,cdc_enabled,deduplication_enabled,write_processing,CASE WHEN (SELECT deployment_mode FROM mokyu_meta WHERE singleton)='single' THEN write_durability END AS write_durability,write_flush_interval,write_backlog_max_age,background_scheduling,public_cache_validation,cors_cache_enabled,public_cache_refresh_interval,public_cache_max_age,read_source_validation,source_validation_max_age,read_integrity,file_delivery,file_delivery_prefix,connect_timeout,request_timeout,max_retries,retry_timeout,priority_aging,min_storage_duration,unreferenced_grace,deleted_chunk_retention,upload_retention,multipart_idle_timeout,min_free_space,cache_enabled,cache_soft_limit,cache_entries,upload_cache,upload_cache_size,upload_cache_overflow,compression_enabled,compression_strategy,compression_level,compression_sample_level,compression_min_savings_percent,compression_min_savings_bytes,pack_enabled,pack_max_size,pack_upload_flush_timeout,pack_interval,pack_reuse_interval,pack_reclaim_interval,pack_repack_interval,pack_repack_cooldown,pack_reclaim_min_savings_bytes,pack_range_enabled,pack_range_interval,pack_range_window,pack_range_min_downloads,pack_range_min_savings_percent,pack_range_min_savings_bytes,pack_range_repack_after,pack_range_repack_window,pack_range_repack_retry_interval,pack_concurrency,integrity_concurrency,integrity_rps,integrity_bandwidth,integrity_batch_timeout,read_concurrency,upload_concurrency,control_concurrency,pack_byte_budget,integrity_byte_budget,maintenance_concurrency,maintenance_byte_budget,migration_concurrency,migration_byte_budget FROM storage_profiles WHERE storage_id=$1 FOR UPDATE").bind(id)
}

pub(super) fn update_storage_record<'q>(
    record: &'q BackendRecord,
    key: Option<Uuid>,
) -> Statement<'q> {
    sqlx::query("UPDATE storages SET type=$2,path=$3,endpoint_url=$4,remote_bucket_name=$5,region=$6,prefix=$7,block_key_id=$8,access_key_encrypted=$9,secret_key_encrypted=$10,enabled=$11,updated_at=now() WHERE id=$1").bind(record.id).bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket_name).bind(&record.region).bind(&record.prefix).bind(key).bind(&record.access_key_encrypted).bind(&record.secret_key_encrypted).bind(record.enabled)
}

pub(super) fn current_storage_key<'q>(id: Option<Uuid>, name: &'q str) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT id FROM managed_keys WHERE ((($1::uuid IS NOT NULL AND id=$1) OR name=$2) AND purpose='block' AND revoked_at IS NULL)").bind(id).bind(name)
}

pub(super) fn update_storage_key(id: Uuid, key: Option<Uuid>) -> Statement<'static> {
    sqlx::query("UPDATE storages SET block_key_id=$2,updated_at=now() WHERE id=$1")
        .bind(id)
        .bind(key)
}

pub(super) fn bump_storage_profile(id: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE storage_profiles SET revision=revision+1,updated_at=now() WHERE storage_id=$1",
    )
    .bind(id)
}

const VIEW_BY_NAME: &str = "SELECT s.id,s.name,s.type AS storage_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.block_key_id,p.revision AS profile_revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.created_at,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.name=$1";

use crate::model::cluster::{ClusterStatus, ClusterStore, Leader, NodeView};
use crate::model::node::{
    CONTACT_SECONDS, NodeCount, NodeFilter, NodePage, NodeReport, SavedReport, SettingCount,
};

const NODE_OBSERVATIONS: &str = r#"
WITH reports AS (
    SELECT n.id,n.name,n.role,n.enabled,n.last_seen_at,n.created_at,r.metadata,r.status,r.sampled_at,r.received_at,
        CASE WHEN NOT n.enabled THEN 'released' WHEN n.last_seen_at IS NULL THEN 'not_reported'
            WHEN n.last_seen_at < now()-$2*interval '1 second' THEN 'unresponsive' ELSE 'online' END AS contact,
        COALESCE(n.enabled AND n.last_seen_at >= now()-$2*interval '1 second'
            AND r.sampled_at IS NOT NULL AND r.received_at >= now()-$2*interval '1 second',false) AS report_fresh
    FROM nodes n LEFT JOIN node_reports r ON r.node_id=n.id
), observed AS (
    SELECT r.id,r.name,r.role,r.enabled,r.last_seen_at,r.created_at,r.sampled_at,r.received_at,r.contact,r.report_fresh,
        CASE WHEN NOT r.enabled THEN 'unknown'
            WHEN r.status->>'work'='starting' AND r.received_at>=now()-$2*interval '1 second' THEN 'starting'
            WHEN r.report_fresh AND r.metadata->>'format'='1' AND r.status->>'work' IN ('normal','maintenance','restricted','recovery') THEN r.status->>'work'
            ELSE 'unknown' END AS work,
        COALESCE(s.settings,'{}'::jsonb) AS settings
    FROM reports r LEFT JOIN LATERAL (
        SELECT jsonb_object_agg(section,CASE
            WHEN desired->'value'='{}'::jsonb THEN 'not_applicable'
            WHEN NOT r.report_fresh OR r.metadata->>'format' IS DISTINCT FROM '1' THEN 'unknown'
            WHEN NOT COALESCE((r.metadata->'loaded'->section) ?& ARRAY(SELECT jsonb_object_keys(desired->'value')),false) THEN 'unknown'
            WHEN NOT ((r.metadata->'loaded'->section) @> (desired->'value')) THEN 'restart_required'
            WHEN r.metadata->'revisions'->section=desired->'revision' THEN 'applied'
            ELSE 'equivalent' END) AS settings
        FROM jsonb_each($1::jsonb->r.role) AS setting(section,desired)
    ) s ON true
), states AS (
    SELECT *,CASE WHEN settings='{}'::jsonb OR EXISTS(SELECT 1 FROM jsonb_each_text(settings) v WHERE v.value='unknown') THEN 'unknown'
        WHEN EXISTS(SELECT 1 FROM jsonb_each_text(settings) v WHERE v.value='restart_required') THEN 'restart_required'
        WHEN EXISTS(SELECT 1 FROM jsonb_each_text(settings) v WHERE v.value='equivalent') THEN 'equivalent'
        ELSE 'applied' END AS configuration FROM observed
)
"#;

pub(super) fn begin_node_report(
    id: Uuid,
    boot: Uuid,
    started: DateTime<Utc>,
) -> Statement<'static> {
    sqlx::query("INSERT INTO node_reports(node_id,boot_id,started_at,status) VALUES($1,$2,$3,'{\"work\":\"starting\"}') ON CONFLICT(node_id) DO UPDATE SET boot_id=$2,sequence=0,started_at=$3,sampled_at=NULL,received_at=now(),metadata='{}',status=EXCLUDED.status")
        .bind(id).bind(boot).bind(started)
}
pub(super) fn save_node_report(id: Uuid, report: &NodeReport) -> Statement<'_> {
    sqlx::query("UPDATE node_reports SET sequence=$3,sampled_at=$4,received_at=now(),metadata=COALESCE($5,metadata),status=$6 WHERE node_id=$1 AND boot_id=$2 AND sequence<$3")
        .bind(id).bind(report.boot_id).bind(report.sequence).bind(report.sampled_at).bind(&report.metadata).bind(&report.status)
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for NodeView {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            role: row.try_get("role")?,
            enabled: row.try_get("enabled")?,
            last_seen_at: row.try_get("last_seen_at")?,
            created_at: row.try_get("created_at")?,
            contact: row.try_get("contact")?,
            work: row.try_get("work")?,
            configuration: row.try_get("configuration")?,
            settings: row.try_get("settings")?,
            sampled_at: row.try_get("sampled_at")?,
            received_at: row.try_get("received_at")?,
            report_fresh: row.try_get("report_fresh")?,
        })
    }
}

#[async_trait::async_trait]
impl ClusterStore for Backend {
    async fn claim_notifications(
        &self,
        consumers: &[crate::model::notification::Consumer],
    ) -> Result<Vec<crate::model::notification::Delivery>> {
        transactions::notifications::claim(&self.pool, consumers).await
    }
    async fn acknowledge_notifications(
        &self,
        consumer: crate::model::notification::Consumer,
        ids: &[Uuid],
    ) -> Result<u64> {
        transactions::notifications::acknowledge(&self.pool, consumer, ids).await
    }
    async fn maintain_notifications(&self) -> Result<bool> {
        transactions::notifications::maintain(&self.pool).await
    }
    async fn task_targets(&self, tasks: &[Uuid]) -> Result<Vec<Option<Uuid>>> {
        anyhow::ensure!(tasks.len() <= 256, "too many task hints");
        Ok(sqlx::query_scalar("SELECT DISTINCT CASE WHEN t.execution_scope='node' THEN t.target_node_id END FROM tasks t WHERE t.id=ANY($1) AND (t.execution_scope<>'node' OR t.target_node_id IS NOT NULL) AND NOT (t.task_type IN ('usage','purge') OR (t.task_type='cleanup' AND t.detail->>'storage_id' IS NULL) OR coalesce(t.task_type='integrity' AND NOT t.detail ? 'source_operation' AND (t.detail->>'mode'='metadata' OR t.detail->>'phase'='metadata'),false))").bind(tasks).fetch_all(&self.pool).await?)
    }
    async fn notification_changes(
        &self,
    ) -> Result<Box<dyn crate::model::cluster::NotificationChanges>> {
        connection::notification_changes(&self.pool).await
    }
    async fn read_migration(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_work::ReadMigration,
    ) -> Result<crate::model::node_read::Reply> {
        transactions::work::migration::read(&self.pool, node, key, input).await
    }
    async fn exchange_work(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_work::Exchange,
    ) -> Result<crate::model::node_work::Reply> {
        transactions::work::exchange(&self.pool, node, key, input).await
    }
    async fn checkpoint_transfer(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        run: crate::model::session::Run,
        operation: Uuid,
        progress: &crate::model::transfer::RawProgress,
    ) -> Result<()> {
        transactions::work::checkpoint_transfer(&self.pool, node, key, run, operation, progress)
            .await
    }
    async fn prepare_local_part(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_native::LocalPart,
    ) -> Result<crate::model::node_write::Reply> {
        transactions::native::prepare_local_part(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn claim_native(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_native::Claim,
    ) -> Result<crate::model::node_native::Reply> {
        transactions::native::claim(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn finish_native(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_native::Finish,
    ) -> Result<()> {
        transactions::native::finish(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn check_local_files(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_native::Inventory,
    ) -> Result<Vec<crate::model::node_raw::File>> {
        transactions::native::inventory(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn check_raw_files(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_native::Inventory,
    ) -> Result<Vec<crate::model::node_raw::File>> {
        transactions::raw_cache::inventory(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn prepare_native_complete(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_native::Complete,
    ) -> Result<crate::model::node_native::PreparedCompletion> {
        transactions::native::complete(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn adopt_native_complete(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_native::Adopt,
    ) -> Result<crate::model::node_write::Reply> {
        transactions::native::adopt(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn completion_status(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_complete::Lookup,
    ) -> Result<(
        crate::model::node_write::Phase,
        crate::model::node_complete::Summary,
    )> {
        transactions::node_completion_status(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn complete_sources(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_complete::Sources,
    ) -> Result<crate::model::node_complete::SourceBatch> {
        transactions::node_complete_sources(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn prepare_tail(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_tail::Prepare,
    ) -> Result<crate::model::node_tail::Prepared> {
        transactions::prepare_node_tail(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn commit_tail(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_write::Lookup,
    ) -> Result<crate::model::node_tail::Prepared> {
        transactions::commit_node_tail(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn multipart_operation(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_multipart::Request,
        max_active: Option<i64>,
    ) -> Result<crate::model::node_multipart::Outcome> {
        transactions::node_multipart_operation(&self.pool, node, key, input, max_active).await
    }

    async fn change_objects(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_object::Request,
    ) -> Result<crate::model::node_object::Outcome> {
        transactions::change_node_objects(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn prepare_raw(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_raw::Prepare,
    ) -> Result<crate::model::node_raw::Prepared> {
        transactions::prepare_node_raw(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn commit_raw(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_raw::Commit,
    ) -> Result<crate::model::node_raw::Prepared> {
        transactions::commit_node_raw(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn prepare_request(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_request::Prepare,
    ) -> Result<crate::model::node_request::Reply> {
        super::transactions::prepare_node_request(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn prepare_chunks(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_chunk::Prepare,
    ) -> Result<crate::model::node_chunk::Assembly> {
        super::transactions::prepare_node_chunks(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn activate_chunks(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_chunk::Prepare,
        nonces: Vec<crate::model::node_chunk::Nonce>,
    ) -> Result<crate::model::node_chunk::Prepared> {
        super::transactions::activate_node_chunks(&self.pool, node, key, input, nonces)
            .await
            .map_err(normalize_error)
    }
    async fn commit_chunks(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_chunk::Commit,
    ) -> Result<i64> {
        super::transactions::commit_node_chunks(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn publish_write(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_write::Publish,
    ) -> Result<crate::model::node_write::Reply> {
        super::transactions::publish_node_write(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn copy_write_references(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_write::CopyBatch,
    ) -> Result<i64> {
        transactions::copy_node_references(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn begin_write(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_write::Begin,
    ) -> Result<crate::model::node_write::Reply> {
        transactions::begin_node_write(&self.pool, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn write_status(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_write::Lookup,
        abort: bool,
    ) -> Result<crate::model::node_write::Reply> {
        transactions::node_write_status(&self.pool, node, key, input, abort)
            .await
            .map_err(normalize_error)
    }
    async fn report_observations(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_observation::Batch,
    ) -> Result<Vec<bool>> {
        transactions::report_node_observations(&self.pool, node, key, input).await
    }
    async fn report_read(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_read::Report,
    ) -> Result<Vec<Uuid>> {
        transactions::report_node_read(&self.pool, node, key, input).await
    }
    async fn open_read(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_read::Query,
    ) -> Result<crate::model::node_read::Reply> {
        transactions::open_node_read(&self.pool, node, key, input).await
    }
    async fn refresh_reads(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_refresh::Request,
    ) -> Result<crate::model::node_refresh::Reply> {
        super::refresh::request(&self.pool, self.refresh.clone(), node, key, input).await
    }
    fn refresh_status(&self) -> serde_json::Value {
        self.refresh.snapshot()
    }
    async fn read_window(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_read::Window,
    ) -> Result<crate::model::node_read::Reply> {
        transactions::node_read_window(&self.pool, self.replica.as_ref(), node, key, input).await
    }
    async fn provision_node(
        &self,
        principal: &Principal,
        name: &str,
        role: &str,
        key: &crate::content::codec::Key,
    ) -> Result<crate::cluster::NodeState> {
        transactions::provision_node(&self.pool, principal, name, Some(role), key).await
    }
    async fn rotate_node(
        &self,
        principal: &Principal,
        name: &str,
        key: &crate::content::codec::Key,
    ) -> Result<crate::cluster::NodeState> {
        transactions::provision_node(&self.pool, principal, name, None, key).await
    }
    async fn close_session(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: crate::model::session::Run,
    ) -> Result<()> {
        transactions::close_node_session(&self.pool, node, key, run).await
    }
    async fn report_session(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: crate::model::session::Run,
        report: &NodeReport,
        sample_interval: u64,
    ) -> Result<bool> {
        transactions::report_node_session(&self.pool, node, key, run, report, sample_interval).await
    }
    async fn open_session(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::session::Open,
    ) -> Result<crate::model::session::Snapshot> {
        transactions::open_node_session(&self.pool, node, key, input).await
    }
    async fn sync_node(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: crate::model::session::Run,
    ) -> Result<crate::model::session::Snapshot> {
        transactions::sync_node(&self.pool, node, key, run).await
    }
    async fn pulse_session(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: crate::model::session::Run,
        epoch: i64,
        sequence: i64,
        draining: bool,
        resources: crate::model::node_read::KeepAlive,
    ) -> Result<crate::model::session::Snapshot> {
        transactions::pulse_node_session(
            &self.pool, node, key, run, epoch, sequence, draining, resources,
        )
        .await
    }
    async fn maintain_session(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: crate::model::session::Run,
        epoch: i64,
        sequence: i64,
        resources: crate::model::node_read::KeepAlive,
    ) -> Result<(i64, Vec<crate::model::node_read::GrantResult>)> {
        transactions::maintain_node_session(&self.pool, node, key, run, epoch, sequence, resources)
            .await
    }

    async fn remote_upload(&self, upload: Uuid) -> Result<Option<RemoteUpload>> {
        Ok(remote_upload(upload).fetch_optional(&self.pool).await?)
    }
    async fn begin_remote(
        &self,
        upload: Uuid,
        storage: Uuid,
        revision: i64,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<RemoteUpload> {
        transactions::begin_remote(&self.pool, upload, storage, revision, node).await
    }
    async fn claim_remote(
        &self,
        upload: Uuid,
        token: Uuid,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::claim_remote(&self.pool, upload, token, node).await
    }
    async fn initialized_remote(
        &self,
        upload: Uuid,
        token: Uuid,
        upstream: Option<&str>,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::initialized_remote(&self.pool, upload, token, upstream, node).await
    }
    async fn commit_remote_part(&self, input: RemoteCommit<'_>) -> Result<()> {
        transactions::commit_upload_part(
            &self.pool,
            input.authority,
            input.bucket,
            input.upload,
            input.number,
            input.stream,
            Some(&input),
        )
        .await
    }
    async fn claim_remote_part(
        &self,
        token: Uuid,
        node: crate::model::raw::Processor<'_>,
        stream: Option<Id>,
    ) -> Result<Option<RemotePart>> {
        transactions::claim_remote_part(&self.pool, token, node, stream).await
    }
    async fn renew_remote_part(
        &self,
        work: &RemotePart,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::finish_remote_part(&self.pool, work, None, node).await
    }
    async fn finish_remote_part(
        &self,
        work: &RemotePart,
        etag: &str,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        anyhow::ensure!(
            !etag.is_empty() && etag.len() <= 1024,
            "invalid remote part ETag"
        );
        transactions::finish_remote_part(&self.pool, work, Some(etag), node).await
    }
    async fn retry_remote_part(&self, work: &RemotePart, error: &str) -> Result<()> {
        sqlx::query("UPDATE parts SET remote_token=NULL,remote_expires_at=NULL,remote_retry_at=clock_timestamp()+interval '2 seconds',remote_error=left($5,1024) WHERE upload_id=$1 AND part_number=$2 AND stream_id=$3 AND remote_token=$4")
            .bind(work.upload.upload).bind(work.part_number).bind(work.stream.id).bind(work.remote_token).bind(error).execute(&self.pool).await?;
        Ok(())
    }
    async fn remote_parts(
        &self,
        upload: Uuid,
        streams: &[Id],
    ) -> Result<Vec<(Id, i32, Option<String>)>> {
        Ok(sqlx::query_as("SELECT stream_id,part_number,remote_etag FROM parts WHERE upload_id=$1 AND stream_id=ANY($2) ORDER BY part_number").bind(upload).bind(streams).fetch_all(&self.pool).await?)
    }
    async fn freeze_remote(&self, upload: Uuid, token: Uuid, hash: &str) -> Result<RemoteUpload> {
        transactions::freeze_remote(&self.pool, upload, token, hash).await
    }
    async fn resend_remote(&self, upload: Uuid, token: Uuid, streams: &[Id]) -> Result<()> {
        sqlx::query("UPDATE parts p SET remote_etag=NULL,remote_retry_at=clock_timestamp() FROM uploads u WHERE u.id=$1 AND u.state='completing' AND u.completion_token=$2 AND u.completion_expires_at>clock_timestamp() AND p.upload_id=u.id AND p.stream_id=ANY($3)").bind(upload).bind(token).bind(streams).execute(&self.pool).await?;
        Ok(())
    }
    async fn adopt_remote(&self, input: RemoteOutput<'_>) -> Result<Uuid> {
        transactions::adopt_remote(&self.pool, input).await
    }
    async fn remote_charges(&self, node: Uuid, after: Id) -> Result<Vec<(Id, Uuid, i64)>> {
        Ok(sqlx::query_as("SELECT s.id,r.storage_id,s.size FROM parts p JOIN streams s ON s.id=p.stream_id JOIN uploads u ON u.id=p.upload_id JOIN raw_sources r ON r.id=u.raw_source_id WHERE p.remote_node_id=$1 AND p.remote_cached AND p.remote_etag IS NULL AND s.id>$2 AND u.state IN ('active','completing') ORDER BY s.id LIMIT 128").bind(node).bind(after).fetch_all(&self.pool).await?)
    }
    async fn claim_remote_cleanup(
        &self,
        token: Uuid,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<Option<RemoteUpload>> {
        transactions::claim_remote_cleanup(&self.pool, token, node).await
    }
    async fn clean_remote(&self, upload: Uuid, token: Uuid) -> Result<()> {
        sqlx::query("UPDATE uploads SET raw_source_id=NULL,remote_token=NULL,remote_expires_at=NULL WHERE id=$1 AND remote_token=$2 AND remote_expires_at>clock_timestamp() AND state IN ('aborted','completed')").bind(upload).bind(token).execute(&self.pool).await?;
        Ok(())
    }

    async fn pending_read(&self, streams: &[Id], node: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw_ranges rr JOIN raw_sources r ON r.id=rr.source_id JOIN raw_uploads u ON u.source_id=r.id WHERE rr.stream_id=ANY($1) AND r.pending_upload AND u.node_id IS DISTINCT FROM $2) OR EXISTS(SELECT 1 FROM extents e JOIN pending_uploads u ON u.chunk_id=e.chunk_id JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=ANY($1) AND u.node_id IS DISTINCT FROM $2 AND c.pack_id IS NULL AND NOT EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=c.id AND l.state='ready'))").bind(streams).bind(node).fetch_one(&self.pool).await?)
    }
    async fn prioritize_uploads(&self, streams: &[Id]) -> Result<()> {
        sqlx::query("UPDATE pending_uploads SET next_retry_at=LEAST(next_retry_at,clock_timestamp()) WHERE owner_task IS NULL AND chunk_id IN (SELECT chunk_id FROM extents WHERE stream_id=ANY($1) AND chunk_id IS NOT NULL)").bind(streams).execute(&self.pool).await?;
        Ok(())
    }
    async fn read_page(
        &self,
        stream: Id,
        lease: Uuid,
        from: i64,
        until: i64,
    ) -> Result<Vec<ReadExtent>> {
        if let Some(replica) = &self.replica
            && let Some(page) = replica.read(stream, lease, from, until).await
        {
            return Ok(page);
        }
        Ok(read_extents(stream, from, until)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn release_staging(&self, storage: Uuid, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM staging_objects WHERE storage_id=$1 AND object_key=$2")
            .bind(storage)
            .bind(key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn stale_staging(&self, storage: Option<Uuid>) -> Result<Vec<(Uuid, String)>> {
        Ok(sqlx::query_as("SELECT s.storage_id,s.object_key FROM staging_objects s WHERE ($1::uuid IS NULL OR s.storage_id=$1) AND s.created_at < now()-interval '5 minutes' AND NOT EXISTS (SELECT 1 FROM pending_uploads p WHERE p.staging_storage_id=s.storage_id AND p.staging_object_key=s.object_key) AND NOT EXISTS (SELECT 1 FROM fragments f WHERE f.staging_storage_id=s.storage_id AND f.staging_object_key=s.object_key) AND NOT EXISTS (SELECT 1 FROM uploads u WHERE u.staging_storage_id=s.storage_id AND u.staging_object_key=s.object_key) ORDER BY s.created_at LIMIT 128").bind(storage).fetch_all(&self.pool).await?)
    }

    async fn claim_staging(&self, storage: Uuid, key: &str, bytes: u64, limit: u64) -> Result<()> {
        transactions::claim_staging(&self.pool, storage, key, bytes, limit).await
    }

    async fn claim_task(
        &self,
        input: TaskClaim<'_>,
        node: &crate::cluster::NodeState,
        role: &str,
        key: &crate::content::codec::Key,
    ) -> Result<Option<Uuid>> {
        transactions::claim_task(&self.pool, input, Some((node, role, key))).await
    }
    async fn recover_tasks(&self) -> Result<u64> {
        transactions::recover_stale(&self.pool).await
    }
    async fn renew_task(&self, id: Uuid, token: Uuid) -> Result<bool> {
        renew_task_lease(&self.pool, id, token).await
    }

    async fn start_usage(&self, node: Option<Uuid>) -> Result<Value> {
        transactions::start_usage(&self.pool, node).await
    }
    async fn complete_usage(&self, task: Execution) -> Result<bool> {
        complete_usage(&self.pool, task).await
    }

    async fn acquire_integrity_rate(
        &self,
        storage: Uuid,
        rates: &[(&str, f64, f64)],
    ) -> Result<bool> {
        transactions::acquire_integrity_rate(&self.pool, storage, rates).await
    }
    async fn acquire_storage_permit(
        &self,
        storage_id: uuid::Uuid,
        direction: &str,
        limit: i32,
        holder: uuid::Uuid,
    ) -> Result<Option<uuid::Uuid>> {
        transactions::try_acquire_storage_permit(&self.pool, storage_id, direction, limit, holder)
            .await
    }

    async fn renew_storage_permit(&self, token: uuid::Uuid) -> Result<bool> {
        transactions::renew_storage_permit(&self.pool, token).await
    }

    async fn release_storage_permit(&self, token: uuid::Uuid) -> Result<()> {
        transactions::release_storage_permit(&self.pool, token).await
    }
    async fn current_stream(
        &self,
        bucket: Uuid,
        key: &str,
        ttl_seconds: i64,
    ) -> Result<(crate::app::StoredStream, Uuid)> {
        transactions::current_stream(&self.pool, bucket, key, ttl_seconds).await
    }
    async fn create_stream(
        &self,
        input: crate::model::StreamCreate,
        ttl_seconds: i64,
    ) -> Result<Id> {
        transactions::create_stream(&self.pool, input, Some(ttl_seconds)).await
    }
    async fn renew_read(&self, id: uuid::Uuid, ttl_seconds: i64) -> Result<()> {
        renew_read_lease(&self.pool, id, ttl_seconds).await
    }

    async fn release_read(&self, id: uuid::Uuid) -> Result<()> {
        release_read_lease(&self.pool, id).await
    }

    async fn renew_write(&self, stream: Id, ttl_seconds: i64) -> Result<()> {
        renew_write_lease(&self.pool, stream, ttl_seconds).await
    }
    async fn expire_stream_leases(&self) -> Result<()> {
        expire_stream_leases().execute(&self.pool).await?;
        Ok(())
    }
    async fn mode(&self) -> Result<String> {
        Ok(deployment_mode().fetch_one(&self.pool).await?)
    }
    async fn status(&self) -> Result<ClusterStatus> {
        Ok(ClusterStatus {
            mode: ClusterStore::mode(self).await?,
            enabled_nodes: sqlx::query_scalar("SELECT count(*) FROM nodes WHERE enabled")
                .fetch_one(&self.pool)
                .await?,
            local_storages: sqlx::query_scalar("SELECT count(*) FROM storages WHERE type='local'")
                .fetch_one(&self.pool)
                .await?,
        })
    }
    async fn nodes(&self, filter: &NodeFilter, desired: &Value) -> Result<NodePage> {
        let cursor = filter.cursor()?;
        let mut items: Vec<NodeView> = sqlx::query_as(sqlx::AssertSqlSafe(format!("{NODE_OBSERVATIONS} SELECT * FROM states WHERE ($3::text IS NULL OR role=$3) AND (($4::text IS NULL AND enabled) OR contact=$4) AND ($5::text IS NULL OR work=$5) AND ($6::text IS NULL OR CASE WHEN $7::text IS NULL THEN configuration ELSE settings->>$7 END=$6) AND ($8::uuid IS NULL OR id>$8) AND ($10::text IS NULL OR strpos(lower(name),lower($10))>0) ORDER BY id LIMIT $9")))
            .bind(desired).bind(CONTACT_SECONDS).bind(&filter.role).bind(&filter.contact).bind(&filter.work)
            .bind(&filter.configuration).bind(&filter.section).bind(cursor).bind(filter.limit+1).bind(&filter.name)
            .fetch_all(&self.pool).await?;
        let more = items.len() > filter.limit as usize;
        items.truncate(filter.limit as usize);
        let next_cursor = if more {
            items.last().map(|node| filter.next(node.id)).transpose()?
        } else {
            None
        };
        Ok(NodePage { items, next_cursor })
    }
    async fn node(&self, id: Uuid, desired: &Value) -> Result<Option<NodeView>> {
        Ok(sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "{NODE_OBSERVATIONS} SELECT * FROM states WHERE id=$3"
        )))
        .bind(desired)
        .bind(CONTACT_SECONDS)
        .bind(id)
        .fetch_optional(&self.pool)
        .await?)
    }
    async fn node_counts(&self, desired: &Value) -> Result<Vec<NodeCount>> {
        let rows: Vec<(String,String,String,String,String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!("{NODE_OBSERVATIONS} SELECT role,contact,work,configuration,count(*)::text FROM states GROUP BY role,contact,work,configuration ORDER BY role,contact,work,configuration")))
            .bind(desired).bind(CONTACT_SECONDS).fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|(role, contact, work, configuration, count)| NodeCount {
                role,
                contact,
                work,
                configuration,
                count,
            })
            .collect())
    }
    async fn setting_counts(&self, desired: &Value) -> Result<Vec<SettingCount>> {
        let rows: Vec<(String,String,String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!("{NODE_OBSERVATIONS} SELECT s.key,s.value,count(*)::text FROM states CROSS JOIN LATERAL jsonb_each_text(settings) s WHERE enabled GROUP BY s.key,s.value ORDER BY s.key,s.value")))
            .bind(desired).bind(CONTACT_SECONDS).fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|(section, state, count)| SettingCount {
                section,
                state,
                count,
            })
            .collect())
    }
    async fn node_report(&self, id: Uuid) -> Result<Option<SavedReport>> {
        let row: Option<(Uuid, DateTime<Utc>, Value, Value)> = sqlx::query_as(
            "SELECT boot_id,started_at,metadata,status FROM node_reports WHERE node_id=$1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(
            row.map(|(boot_id, started_at, metadata, status)| SavedReport {
                boot_id,
                started_at,
                metadata,
                status,
            }),
        )
    }
    async fn sample_node_runtime(
        &self,
        state: &crate::cluster::NodeState,
        role: &str,
        key: &crate::content::codec::Key,
        sample: &crate::model::metrics::NodeSample,
    ) -> Result<bool> {
        transactions::sample_node_runtime(&self.pool, state, role, key, sample)
            .await
            .map_err(normalize_error)
    }
    async fn aggregate_runtime(&self, interval: u64, retention: u64) -> Result<()> {
        transactions::aggregate_runtime(&self.pool, interval, retention)
            .await
            .map_err(normalize_error)
    }
    async fn cluster_runtime(
        &self,
        query: &crate::model::metrics::RuntimeQuery,
    ) -> Result<Vec<crate::model::metrics::ClusterRuntimePoint>> {
        query.validate()?;
        let mut tx = self.pool.begin().await?;
        runtime_diagnostic_limits().execute(&mut *tx).await?;
        let rows = cluster_runtime_history(query)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| normalize_error(e.into()))?;
        tx.commit().await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }
    async fn node_runtime(
        &self,
        id: Uuid,
        query: &crate::model::metrics::RuntimeQuery,
    ) -> Result<Vec<crate::model::metrics::NodeRuntimePoint>> {
        query.validate()?;
        let mut tx = self.pool.begin().await?;
        runtime_diagnostic_limits().execute(&mut *tx).await?;
        let rows = node_runtime_history(id, query)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| normalize_error(e.into()))?;
        tx.commit().await?;
        Ok(rows
            .into_iter()
            .map(
                |(node_id, received_at, sample)| crate::model::metrics::NodeRuntimePoint {
                    node_id,
                    received_at,
                    sample: sample.0,
                },
            )
            .collect())
    }
    async fn node_tasks(&self, id: Uuid) -> Result<Vec<crate::model::metrics::WorkSummary>> {
        Ok(sqlx::query_as("SELECT id,task_type,state,processed::text,updated_at FROM tasks WHERE lease_node_id=$1 AND state='running' ORDER BY updated_at DESC,id LIMIT 21").bind(id).fetch_all(&self.pool).await?)
    }
    async fn begin_report(
        &self,
        state: &crate::cluster::NodeState,
        role: &str,
        key: &crate::content::codec::Key,
        boot: Uuid,
        started: DateTime<Utc>,
    ) -> Result<()> {
        transactions::begin_report(&self.pool, state, role, key, boot, started).await
    }
    async fn report(
        &self,
        state: &crate::cluster::NodeState,
        role: &str,
        key: &crate::content::codec::Key,
        report: &NodeReport,
    ) -> Result<bool> {
        transactions::report_node(&self.pool, state, role, key, report).await
    }
    async fn credential(&self, id: Uuid) -> Result<Option<(String, Vec<u8>)>> {
        Ok(sqlx::query_as("SELECT role,credential_encrypted FROM nodes WHERE id=$1 AND role='control' AND enabled AND credential_encrypted IS NOT NULL").bind(id).fetch_optional(&self.pool).await?)
    }
    async fn set_mode(&self, principal: &Principal, mode: &str) -> Result<()> {
        connection::set_mode(self, principal, mode).await
    }
    async fn release(&self, name: &str) -> Result<()> {
        transactions::release_node(&self.pool, name).await
    }
    async fn recover(&self, name: &str, role: &str) -> Result<()> {
        transactions::recover_node(&self.pool, name, role).await
    }
    async fn heartbeat(
        &self,
        state: &crate::cluster::NodeState,
        role: &str,
        key: &crate::content::codec::Key,
    ) -> Result<()> {
        transactions::heartbeat(&self.pool, state, role, key).await
    }
    async fn leader(&self, key: i64) -> Result<Option<Box<dyn Leader>>> {
        transactions::cluster_leader(&self.pool, key).await
    }
    async fn maintenance_state(&self) -> Result<(Value, bool, DateTime<Utc>)> {
        Ok(maintenance_state().fetch_one(&self.pool).await?)
    }
    async fn register(
        &self,
        name: &str,
        role: &str,
        credential: String,
        key: &crate::content::codec::Key,
    ) -> Result<Option<crate::cluster::NodeState>> {
        transactions::register_node(&self.pool, name, role, credential, key).await
    }
}
pub(super) fn deployment_mode() -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton")
}

pub(super) fn deployment_mode_lock() -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton FOR UPDATE")
}

pub(super) fn cluster_local_storages() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storages WHERE type='local')")
}

pub(super) fn cluster_staging_id() -> Scalar<'static, Option<Uuid>> {
    sqlx::query_scalar(
        "SELECT NULLIF(value->>'staging_storage_id','')::uuid FROM runtime_settings WHERE section='uploads'",
    )
}

pub(super) fn cluster_staging_valid(id: Option<Uuid>) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM storages WHERE id=$1 AND enabled AND type='s3')",
    )
    .bind(id)
}

pub(super) fn cluster_other_nodes(id: Option<Uuid>) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM nodes WHERE enabled AND ($1::uuid IS NULL OR id<>$1))",
    )
    .bind(id)
}

pub(super) fn mode_transition_state() -> Rows<'static, (bool, bool, bool, bool)> {
    sqlx::query_as(
        "SELECT maintenance,EXISTS(SELECT 1 FROM tasks WHERE state IN ('queued','running','paused') OR lease_token IS NOT NULL),EXISTS(SELECT 1 FROM uploads WHERE state IN ('active','completing')) OR EXISTS(SELECT 1 FROM streams WHERE state='writing') OR EXISTS(SELECT 1 FROM stream_leases WHERE expires_at>clock_timestamp()),EXISTS(SELECT 1 FROM pending_uploads) OR EXISTS(SELECT 1 FROM raw_uploads) OR EXISTS(SELECT 1 FROM extents e JOIN streams s ON s.id=e.stream_id WHERE e.fragment_id IS NOT NULL AND s.state IN ('ready','writing')) FROM mokyu_meta WHERE singleton",
    )
}

pub(super) fn preserve_failed_task_mode(mode: &str) -> Statement<'_> {
    sqlx::query("UPDATE tasks SET detail=jsonb_set(detail,'{execution_mode}',to_jsonb($1::text)) WHERE state='failed' AND NOT detail ? 'execution_mode'").bind(mode)
}

pub(super) fn release_node_id<'q>(id: &'q Uuid) -> Statement<'q> {
    sqlx::query("UPDATE nodes SET enabled=false,credential_encrypted=NULL,last_seen_at=NULL,status='released',recovery_required=false,updated_at=now() WHERE id=$1").bind(id)
}

pub(super) fn release_node_tasks<'q>(id: &'q Uuid) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET state=CASE WHEN execution_scope='node' THEN 'paused' ELSE 'queued' END,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE lease_node_id=$1 AND state='running'").bind(id)
}

pub(super) fn release_node_permits<'q>(id: &'q Uuid) -> Statement<'q> {
    sqlx::query("DELETE FROM storage_permits WHERE holder_id=$1").bind(id)
}

pub(super) fn update_deployment_mode<'q>(mode: &'q str) -> Statement<'q> {
    sqlx::query("UPDATE mokyu_meta SET deployment_mode=$1,deployment_mode_revision=deployment_mode_revision+1 WHERE singleton").bind(mode)
}

pub(super) fn release_node_name<'q>(name: &'q str) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("UPDATE nodes SET enabled=false,credential_encrypted=NULL,last_seen_at=NULL,status='released',recovery_required=false,updated_at=now() WHERE name=$1 AND enabled RETURNING id").bind(name)
}

pub(super) fn node_seen(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE nodes SET last_seen_at=now(),updated_at=now() WHERE id=$1").bind(id)
}

pub(super) fn recover_node_lock<'q>(name: &'q str) -> Rows<'q, (Uuid, String)> {
    sqlx::query_as("SELECT id,role FROM nodes WHERE name=$1 FOR UPDATE").bind(name)
}

pub(super) fn try_cluster_leader(key: i64) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)").bind(key)
}

pub(super) fn maintenance_state() -> Rows<'static, (Value, bool, DateTime<Utc>)> {
    sqlx::query_as(
        "SELECT jsonb_build_array(m.maintenance,m.gc_paused,m.pack_creation_paused,(SELECT jsonb_agg(jsonb_build_array(c.policy,c.paused,c.last_scheduled_at) ORDER BY c.policy) FROM maintenance_controls c),(SELECT jsonb_agg(jsonb_build_array(s.id,s.enabled,p.revision) ORDER BY s.id) FROM storages s JOIN storage_profiles p ON p.storage_id=s.id)),NOT m.maintenance AND EXISTS(SELECT 1 FROM maintenance_controls WHERE policy='reuse' AND NOT paused) AND EXISTS(SELECT 1 FROM storages WHERE enabled) AND EXISTS(SELECT 1 FROM pack_changes WHERE reason='reuse' AND next_check_at<=clock_timestamp()),clock_timestamp() FROM mokyu_meta m WHERE singleton",
    )
}
use crate::model::runtime::{RuntimeState, RuntimeStore};

#[async_trait::async_trait]
impl RuntimeStore for Backend {
    fn database_info(&self) -> Option<&crate::model::runtime::DatabaseInfo> {
        self.database_info.as_ref()
    }
    fn read_replica(&self) -> Option<Value> {
        self.replica.as_ref().map(connection::Replica::snapshot)
    }
    async fn initialize(&self, maintenance: bool) -> Result<RuntimeState> {
        if maintenance {
            set_maintenance_mode(true).execute(&self.pool).await?;
        }
        RuntimeStore::state(self).await
    }

    async fn state(&self) -> Result<RuntimeState> {
        let (gc_paused, maintenance, pack_creation_paused, mode, mode_revision) =
            runtime_state().fetch_one(&self.pool).await?;
        Ok(RuntimeState {
            gc_paused,
            maintenance,
            pack_creation_paused,
            mode,
            mode_revision,
        })
    }

    async fn pending_uploads(&self) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads)")
                .fetch_one(&self.pool)
                .await?,
        )
    }

    async fn health(&self) -> Result<()> {
        health().execute(&self.pool).await?;
        Ok(())
    }

    fn pool(&self) -> (u32, usize) {
        (self.pool.size(), self.pool.num_idle())
    }
}

pub(super) fn health() -> Statement<'static> {
    sqlx::query("SELECT 1")
}

fn runtime_state() -> Rows<'static, (bool, bool, bool, String, i64)> {
    sqlx::query_as(
        "SELECT gc_paused,maintenance,pack_creation_paused,deployment_mode,deployment_mode_revision FROM mokyu_meta",
    )
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::app::Bucket {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            project_id: row.try_get("project_id")?,
            name: row.try_get("name")?,
            storage_id: row.try_get("storage_id")?,
            state: row.try_get("state")?,
            settings_revision: row.try_get("settings_revision")?,
            uploads_paused: row.try_get("uploads_paused")?,
            public_base_url: row.try_get("public_base_url")?,
            cors: row.try_get::<serde_json::Value, _>("cors")?.into(),
            website_enabled: row.try_get("website_enabled")?,
            index_document: row.try_get("index_document")?,
            error_document: row.try_get("error_document")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::app::StoredStream {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            bucket_id: row.try_get("bucket_id")?,
            object_key: row.try_get("object_key")?,
            stream_type: row.try_get("stream_type")?,
            payload_type: row.try_get("payload_type")?,
            payload_revision: row.try_get("payload_revision")?,
            state: row.try_get("state")?,
            size: row.try_get("size")?,
            etag: row.try_get("etag")?,
            metadata: row.try_get("metadata")?,
            public_read: row.try_get("public_read")?,
            checksums: row.try_get("checksums")?,
            http_compression: row.try_get("http_compression")?,
            created_at: row.try_get("created_at")?,
            touched_at: row.try_get("touched_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::app::Extent {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            stream_id: row.try_get("stream_id")?,
            offset_bytes: row.try_get("offset_bytes")?,
            length: row.try_get("length")?,
            chunk_id: row.try_get("chunk_id")?,
            fragment_id: row.try_get("fragment_id")?,
            source_offset: row.try_get("source_offset")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::content::codec::Chunk {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            uuid: row.try_get("id")?,
            encoding_id: row.try_get("current_encoding_id")?,
            location_id: match row.try_get("location_id") {
                Ok(value) => value,
                Err(sqlx::Error::ColumnNotFound(_)) => Default::default(),
                Err(error) => return Err(error),
            },
            storage_id: row.try_get("storage_id")?,
            hash: row.try_get("hash")?,
            raw_size: row.try_get("raw_size")?,
            stored_size: row.try_get("standalone_size")?,
            algorithm: row.try_get("encryption_algorithm")?,
            block_key_id: row.try_get("block_key_id")?,
            compressed: row.try_get("standalone_compressed")?,
            nonce: row.try_get("nonce")?,
            format: row.try_get("format")?,
            state: row.try_get("state")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::content::pack::Pack {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            location_id: row.try_get("id")?,
            storage_id: row.try_get("storage_id")?,
            algorithm: row.try_get("encryption_algorithm")?,
            block_key_id: row.try_get("block_key_id")?,
            raw_size: row.try_get("raw_size")?,
            stored_size: row.try_get("stored_size")?,
            compressed: row.try_get("pack_compressed")?,
            nonce: row.try_get("nonce")?,
            digest: row.try_get("digest")?,
            member_count: row.try_get("member_count")?,
            state: row.try_get("state")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::account::Profile {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            username: row.try_get("username")?,
            role: row.try_get("role")?,
            must_change_password: row.try_get("must_change_password")?,
            project_management: row.try_get("project_management")?,
            display_name: row.try_get("display_name")?,
            locale: row.try_get("locale")?,
            theme: row.try_get("theme")?,
            avatar_email: row.try_get("avatar_email")?,
            avatar_enabled: row.try_get("avatar_enabled")?,
            avatar_url: Default::default(),
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::account::Session {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            created_at: row.try_get("created_at")?,
            last_seen_at: row.try_get("last_seen_at")?,
            expires_at: row.try_get("expires_at")?,
            user_agent: row.try_get("user_agent")?,
            current: row.try_get("current")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::account::AuthSession {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            user_id: row.try_get("user_id")?,
            session_id: row.try_get("session_id")?,
            role: row.try_get("role")?,
            must_change_password: row.try_get("must_change_password")?,
            authorization_revision: row.try_get("authorization_revision")?,
            csrf_hash: row.try_get("csrf_hash")?,
            last_seen_at: row.try_get("last_seen_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::administration::User {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            username: row.try_get("username")?,
            display_name: row.try_get("display_name")?,
            role: row.try_get("role")?,
            enabled: row.try_get("enabled")?,
            must_change_password: row.try_get("must_change_password")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::administration::Member {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            user_id: row.try_get("user_id")?,
            username: row.try_get("username")?,
            display_name: row.try_get("display_name")?,
            enabled: row.try_get("enabled")?,
            role: row.try_get("role")?,
            scope: row.try_get("scope")?,
            grants: row.try_get::<sqlx::types::Json<_>, _>("grants")?.0,
            external_grants: row.try_get::<sqlx::types::Json<_>, _>("external_grants")?.0,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::administration::Project {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            description: row.try_get("description")?,
            builtin: row.try_get("builtin")?,
            allow_bucket_create: row.try_get("allow_bucket_create")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::audit::Event {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            created_at: row.try_get("created_at")?,
            finished_at: row.try_get("finished_at")?,
            actor_id: row.try_get("actor_id")?,
            actor_label: row.try_get("actor_label")?,
            token_id: row.try_get("token_id")?,
            source: row.try_get("source")?,
            action: row.try_get("action")?,
            target: row.try_get("target")?,
            project_id: row.try_get("project_id")?,
            bucket_id: row.try_get("bucket_id")?,
            outcome: row.try_get("outcome")?,
            request_id: row.try_get("request_id")?,
            status: row.try_get("http_status")?,
            detail: row.try_get("detail")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::bucket::BucketProject {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::bucket::BucketStorage {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            storage_type: row.try_get("storage_type")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::bucket::Domain {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            host: row.try_get("host")?,
            bucket_id: row.try_get("bucket_id")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::bucket::BucketImpact {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            bucket_id: row.try_get("bucket_id")?,
            name: row.try_get("name")?,
            project_id: row.try_get("project_id")?,
            state: row.try_get("state")?,
            revision: row.try_get("revision")?,
            uploads_paused: row.try_get("uploads_paused")?,
            objects: row.try_get("objects")?,
            logical_bytes: row.try_get("logical_bytes")?,
            reserved_bytes: row.try_get("reserved_bytes")?,
            inflight_bytes: row.try_get("inflight_bytes")?,
            active_uploads: row.try_get("active_uploads")?,
            writing_streams: row.try_get("writing_streams")?,
            member_bucket_grants: row.try_get("member_bucket_grants")?,
            service_grants: row.try_get("service_grants")?,
            token_bucket_grants: row.try_get("token_bucket_grants")?,
            domains: row.try_get("domains")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::bucket::TransferProject {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            used_bytes: row.try_get("used_bytes")?,
            reserved_bytes: row.try_get("reserved_bytes")?,
            logical_bytes_limit: row.try_get("logical_bytes_limit")?,
            bucket_count: row.try_get("bucket_count")?,
            bucket_count_limit: row.try_get("bucket_count_limit")?,
            members: row.try_get("members")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::bucket::BucketView {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            project_id: row.try_get("project_id")?,
            storage_id: row.try_get("storage_id")?,
            actions: row.try_get("actions")?,
            name: row.try_get("name")?,
            state: row.try_get("state")?,
            revision: row.try_get("revision")?,
            uploads_paused: row.try_get("uploads_paused")?,
            public_base_url: row.try_get("public_base_url")?,
            cors: row.try_get("cors")?,
            website_enabled: row.try_get("website_enabled")?,
            index_document: row.try_get("index_document")?,
            error_document: row.try_get("error_document")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::bucket::Website {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            website_enabled: row.try_get("website_enabled")?,
            index_document: row.try_get("index_document")?,
            error_document: row.try_get("error_document")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::chunk::ReadExtent {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            extent: sqlx::FromRow::from_row(row)?,
            chunk: sqlx::FromRow::from_row(row)?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::chunk::FragmentSource {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            location_id: row.try_get("fragment_location")?,
            size: row.try_get("size")?,
            hash: row.try_get("hash")?,
            file_id: row.try_get("file_id")?,
            file_offset: row.try_get("file_offset")?,
            file_storage: row.try_get("file_storage")?,
            raw_source: row.try_get("raw_source")?,
            staging_storage_id: row.try_get("staging_storage_id")?,
            staging_object_key: row.try_get("staging_object_key")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::chunk::Mapping {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            chunk: sqlx::FromRow::from_row(row)?,
            offset_bytes: row.try_get("offset_bytes")?,
            pack_id: row.try_get("pack_id")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::credential::Credential {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            access_key: row.try_get("access_key_id")?,
            project_id: row.try_get("project_id")?,
            label: row.try_get("label")?,
            enabled: row.try_get("enabled")?,
            created_at: row.try_get("created_at")?,
            expires_at: row.try_get("expires_at")?,
            last_used_at: row.try_get("last_used_at")?,
            created_by: row.try_get("created_by")?,
            grants: row.try_get::<sqlx::types::Json<_>, _>("grants")?.0,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::credential::Token {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            user_id: row.try_get("user_id")?,
            label: row.try_get("label")?,
            prefix: row.try_get("prefix")?,
            system: row.try_get("system")?,
            active: row.try_get("active")?,
            created_at: row.try_get("created_at")?,
            expires_at: row.try_get("expires_at")?,
            last_used_at: row.try_get("last_used_at")?,
            revoked_at: row.try_get("revoked_at")?,
            grants: row.try_get::<sqlx::types::Json<_>, _>("grants")?.0,
            external_grants: row.try_get::<sqlx::types::Json<_>, _>("external_grants")?.0,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::key::ManagedKeyRecord {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            algorithm: row.try_get("algorithm")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::key::ManagedKeyMaterial {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            purpose: row.try_get("purpose")?,
            algorithm: row.try_get("key_algorithm")?,
            encrypted: row.try_get("secret_encrypted")?,
            fingerprint: row.try_get("fingerprint")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::multipart::Upload {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            bucket_id: row.try_get("bucket_id")?,
            object_key: row.try_get("object_key")?,
            state: row.try_get("state")?,
            metadata: row.try_get("metadata")?,
            public_read: row.try_get("public_read")?,
            checksum_algorithm: row.try_get("checksum_algorithm")?,
            checksum_type: row.try_get("checksum_type")?,
            manifest_hash: row.try_get("manifest_hash")?,
            result: row.try_get("result")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::multipart::Transfer {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            bucket_id: row.try_get("bucket_id")?,
            bucket_name: row.try_get("bucket_name")?,
            object_key: row.try_get("object_key")?,
            state: row.try_get("state")?,
            source: row.try_get("source")?,
            user_id: row.try_get("user_id")?,
            owner: row.try_get("owner")?,
            file_name: row.try_get("file_name")?,
            expected_size: row.try_get("expected_size")?,
            part_size: row.try_get("part_size")?,
            modified_at: row.try_get("modified_at")?,
            received_bytes: row.try_get("received_bytes")?,
            parts: row.try_get("parts")?,
            remote_state: row.try_get("remote_state")?,
            created_at: row.try_get("created_at")?,
            touched_at: row.try_get("touched_at")?,
            multipart_idle_timeout: row.try_get("multipart_idle_timeout")?,
            expires_at: match row.try_get("expires_at") {
                Ok(value) => value,
                Err(sqlx::Error::ColumnNotFound(_)) => Default::default(),
                Err(error) => return Err(error),
            },
            can_resume: match row.try_get("can_resume") {
                Ok(value) => value,
                Err(sqlx::Error::ColumnNotFound(_)) => Default::default(),
                Err(error) => return Err(error),
            },
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::multipart::ReceivedPart {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            number: row.try_get("number")?,
            size: row.try_get("size")?,
            etag: row.try_get("etag")?,
            sha256: row.try_get("sha256")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::settings::RuntimeSettingsRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            value: row.try_get("value")?,
            revision: row.try_get("revision")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::model::task::TaskRecord {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            task_type: row.try_get("task_type")?,
            state: row.try_get("state")?,
            bucket_id: row.try_get("bucket_id")?,
            cursor: row.try_get("cursor")?,
            detail: row.try_get("detail")?,
            execution_token: row.try_get("lease_token")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::storage::model::Record {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            r#type: row.try_get("type")?,
            path: row.try_get("path")?,
            endpoint_url: row.try_get("endpoint_url")?,
            remote_bucket_name: row.try_get("remote_bucket_name")?,
            region: row.try_get("region")?,
            prefix: row.try_get("prefix")?,
            access_key_encrypted: row.try_get("access_key_encrypted")?,
            secret_key_encrypted: row.try_get("secret_key_encrypted")?,
            identity_marker: row.try_get("identity_marker")?,
            enabled: row.try_get("enabled")?,
        })
    }
}

pub(super) fn lock_task_finish(task: Execution) -> Rows<'static, (String, String, Option<Uuid>)> {
    sqlx::query_as("SELECT task_type,state,parent_task_id FROM tasks WHERE id=$1 AND lease_token=$2 AND (state IN ('completed','paused') OR (state='running' AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()))) FOR UPDATE").bind(task.id).bind(task.token)
}

pub(super) fn lock_task_detail(task: Execution) -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT detail FROM tasks WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()) FOR UPDATE")
        .bind(task.id).bind(task.token)
}

pub(super) fn advance_storage_task(
    id: Uuid,
    detail: Value,
    processed: i64,
    done: bool,
) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET cursor=NULL,detail=$2,processed=processed+$3,state=CASE WHEN $4 THEN 'completed' ELSE state END,updated_at=now() WHERE id=$1")
        .bind(id).bind(detail).bind(processed).bind(done)
}

pub(super) fn lock_task_family(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM tasks WHERE id=$1 OR parent_task_id=$1 ORDER BY (id=$1),id FOR UPDATE",
    )
    .bind(id)
}

pub(super) fn runtime_diagnostic_limits() -> sqlx::RawSql {
    sqlx::raw_sql(
        "SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='100ms'; SET LOCAL work_mem='4MB'",
    )
}
pub(super) fn runtime_aggregate_lock() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(734922709851032)")
}
pub(super) fn runtime_aggregate_due(interval: f64) -> Rows<'static, (DateTime<Utc>, i64, bool)> {
    sqlx::query_as("SELECT now(),(SELECT count(*) FROM nodes WHERE enabled),NOT EXISTS(SELECT 1 FROM runtime_samples WHERE data->>'scope'='cluster' AND sampled_at>now()-$1*interval '1 second')").bind(interval)
}
pub(super) fn node_runtime_sample<'a>(
    id: Uuid,
    sample: &'a crate::model::metrics::NodeSample,
) -> Statement<'a> {
    sqlx::query("INSERT INTO node_runtime_samples(node_id,boot_id,sequence,uptime_seconds,data) SELECT $1,$2,$3,$4,$5 FROM node_reports r WHERE r.node_id=$1 AND r.boot_id=$2 AND r.sequence>=$3 AND NOT EXISTS(SELECT 1 FROM node_runtime_samples s WHERE s.node_id=$1 AND s.boot_id=$2 AND (s.sequence>=$3 OR s.uptime_seconds>=$4)) ON CONFLICT DO NOTHING")
        .bind(id).bind(sample.boot_id).bind(sample.sequence).bind(sample.uptime_seconds as i64).bind(sqlx::types::Json(sample))
}
type NodeRuntimeRow = (
    Uuid,
    DateTime<Utc>,
    sqlx::types::Json<crate::model::metrics::NodeSample>,
);
pub(super) fn runtime_node_window(
    seconds: f64,
) -> Rows<
    'static,
    (
        Option<Uuid>,
        Option<DateTime<Utc>>,
        Option<sqlx::types::Json<crate::model::metrics::NodeSample>>,
        i64,
    ),
> {
    // The cap bounds coordinator memory; omitted nodes remain in expected coverage.
    sqlx::query_as("WITH members AS (SELECT id FROM nodes WHERE enabled AND last_seen_at>=now()-$2*interval '1 second' ORDER BY id LIMIT 4096), samples AS (SELECT n.id,s.received_at,s.data,s.sequence FROM members n JOIN node_reports r ON r.node_id=n.id CROSS JOIN LATERAL (SELECT received_at,data,sequence FROM node_runtime_samples s WHERE s.node_id=n.id AND s.boot_id=r.boot_id AND s.received_at>=now()-$1*interval '1 second' ORDER BY received_at DESC,sequence DESC LIMIT 2) s) SELECT s.id,s.received_at,s.data,e.expected FROM (SELECT count(*) AS expected FROM nodes WHERE enabled) e LEFT JOIN samples s ON true ORDER BY s.id,s.received_at DESC,s.sequence DESC").bind(seconds).bind(CONTACT_SECONDS)
}
pub(super) fn node_runtime_history(
    id: Uuid,
    query: &crate::model::metrics::RuntimeQuery,
) -> Rows<'static, NodeRuntimeRow> {
    sqlx::query_as("SELECT node_id,received_at,data FROM (SELECT node_id,received_at,sequence,data FROM node_runtime_samples WHERE node_id=$1 AND received_at>=now()-$2*interval '1 hour' ORDER BY received_at DESC,sequence DESC LIMIT $3) s ORDER BY received_at,sequence").bind(id).bind(f64::from(query.hours)).bind(query.limit)
}
pub(super) fn cluster_runtime_history(
    query: &crate::model::metrics::RuntimeQuery,
) -> Scalar<'static, sqlx::types::Json<crate::model::metrics::ClusterRuntimePoint>> {
    sqlx::query_scalar("SELECT data FROM (SELECT sampled_at,data FROM runtime_samples WHERE data->>'scope'='cluster' AND sampled_at>=now()-$1*interval '1 hour' ORDER BY sampled_at DESC LIMIT $2) s ORDER BY sampled_at").bind(f64::from(query.hours)).bind(query.limit)
}
pub(super) fn cluster_runtime_sample(
    point: &crate::model::metrics::ClusterRuntimePoint,
) -> Statement<'_> {
    sqlx::query("INSERT INTO runtime_samples(sampled_at,data) VALUES($1,$2) ON CONFLICT DO NOTHING")
        .bind(point.at)
        .bind(sqlx::types::Json(point))
}
pub(super) fn node_runtime_expire(retention: f64) -> Statement<'static> {
    sqlx::query("DELETE FROM node_runtime_samples WHERE (node_id,boot_id,sequence) IN (SELECT node_id,boot_id,sequence FROM node_runtime_samples WHERE received_at<now()-$1*interval '1 second' ORDER BY received_at LIMIT 1000)").bind(retention)
}
pub(super) fn cluster_runtime_expire(retention: f64) -> Statement<'static> {
    sqlx::query("DELETE FROM runtime_samples WHERE sampled_at IN (SELECT sampled_at FROM runtime_samples WHERE data->>'scope'='cluster' AND sampled_at<now()-$1*interval '1 second' ORDER BY sampled_at LIMIT 1000)").bind(retention)
}

fn source_scope(query: &mut sqlx::QueryBuilder<Postgres>, stream: Option<Id>, chunks: &[Id]) {
    query.push("c.id IN (");
    if let Some(stream) = stream {
        query
            .push("SELECT chunk_id FROM extents WHERE stream_id=")
            .push_bind(stream);
    } else if chunks.is_empty() {
        query.push("NULL");
    } else {
        let mut values = query.separated(",");
        for id in chunks {
            values.push_bind(*id);
        }
    }
    query.push(")");
}

pub(super) fn source_locks(
    stream: Option<Id>,
    chunks: &[Id],
    phase: u8,
) -> sqlx::QueryBuilder<Postgres> {
    let mut query = sqlx::QueryBuilder::new(match phase {
        // Stream generic plans may scan unrelated chunks; avoid fetching their pack column.
        0 if stream.is_some() => "SELECT TRUE FROM chunks c WHERE ",
        0 => "SELECT c.pack_id IS NOT NULL FROM chunks c WHERE ",
        1 => {
            "SELECT TRUE FROM chunk_encodings l WHERE l.state='ready' AND l.chunk_id IN (SELECT c.id FROM chunks c WHERE "
        }
        2 => {
            "SELECT TRUE FROM packs p WHERE p.state='ready' AND p.id IN (SELECT c.pack_id FROM chunks c WHERE "
        }
        _ => unreachable!(),
    });
    source_scope(&mut query, stream, chunks);
    query.push(match phase {
        0 => " ORDER BY c.id FOR KEY SHARE",
        1 => ") ORDER BY l.encoding_id FOR SHARE",
        _ => ") ORDER BY p.id FOR SHARE",
    });
    query
}

pub(super) fn unavailable_sources(
    stream: Option<Id>,
    chunks: &[Id],
) -> sqlx::QueryBuilder<Postgres> {
    let mut query = sqlx::QueryBuilder::new("SELECT EXISTS(SELECT 1 FROM chunks c WHERE ");
    source_scope(&mut query, stream, chunks);
    query.push(" AND (c.state<>'ready' OR (NOT EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=c.id AND l.state='ready' AND l.quarantined_at IS NULL) AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.state='ready' AND p.quarantined_at IS NULL) AND NOT (EXISTS(SELECT 1 FROM pending_uploads u WHERE u.chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=c.id AND l.quarantined_at IS NOT NULL) AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.quarantined_at IS NOT NULL)))))");
    query
}

pub(super) fn source_writing_stream(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=$1 AND state='writing' FOR UPDATE")
        .bind(stream)
}

pub(super) async fn source_snapshot(
    db: &mut sqlx::PgConnection,
    scope: &crate::model::source::SourceScope,
    lock: bool,
) -> Result<crate::model::source::SourceSnapshot> {
    use crate::model::source::{SourceSnapshot, manifest_digest};
    let suffix = if lock { " FOR SHARE" } else { "" };
    let (storage_revision, marker): (i64, sqlx::types::Json<Value>) = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT p.revision,s.identity_marker FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=$1 AND s.enabled{suffix}")))
        .bind(scope.storage_id).fetch_one(&mut *db).await?;
    let suffix = if lock { " FOR UPDATE" } else { "" };
    if scope.source == crate::model::source::SourceType::Raw {
        let raw: RawSource = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id=$1 AND r.storage_id=$2{suffix}")))
            .bind(scope.identity()?).bind(scope.storage_id).fetch_one(&mut *db).await?;
        return Ok(SourceSnapshot {
            scope: scope.clone(),
            state: raw.state.clone(),
            revision: raw.revision,
            quarantine_code: raw.quarantine_code.clone(),
            physical: PhysicalSource::Raw(raw),
            storage_revision,
            identity_marker: marker.0,
            manifest_digest: None,
        });
    }
    let id = scope.identity()?;
    let (table, column) = if scope.packed() {
        ("packs", "id")
    } else {
        ("chunk_encodings", "encoding_id")
    };
    let (state, revision, quarantine_code): (String, i64, Option<String>) = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT state,quarantine_revision,quarantine_code FROM {table} WHERE {column}=$1 AND storage_id=$2{suffix}")))
        .bind(id).bind(scope.storage_id).fetch_optional(&mut *db).await?.context(crate::model::Error::OperationAborted)?;
    let physical = if scope.packed() {
        PhysicalSource::Pack(
            sqlx::query_as("SELECT * FROM packs WHERE id=$1 AND storage_id=$2")
                .bind(id)
                .bind(scope.storage_id)
                .fetch_one(&mut *db)
                .await?,
        )
    } else {
        PhysicalSource::Chunk(sqlx::query_as("SELECT c.id,l.encoding_id AS current_encoding_id,l.encoding_id AS location_id,c.storage_id,c.hash,c.raw_size,l.standalone_size,l.encryption_algorithm,l.block_key_id,l.standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE l.encoding_id=$1 AND l.storage_id=$2").bind(id).bind(scope.storage_id).fetch_one(&mut *db).await?)
    };
    let manifest_digest = if let PhysicalSource::Pack(pack) = &physical {
        let members: Vec<(Id, i64, i32, Vec<u8>)> = sqlx::query_as("SELECT m.chunk_id,m.offset_bytes,c.raw_size,c.hash FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=$1 ORDER BY m.ordinal LIMIT 4097").bind(id).fetch_all(&mut *db).await?;
        Some(manifest_digest(pack, &members)?)
    } else {
        None
    };
    Ok(SourceSnapshot {
        scope: scope.clone(),
        physical,
        state,
        revision,
        quarantine_code,
        storage_revision,
        identity_marker: marker.0,
        manifest_digest,
    })
}

pub(super) async fn quarantines(
    db: &mut sqlx::PgConnection,
    storage: Uuid,
    packed: bool,
    after: Id,
    limit: i64,
) -> Result<Vec<crate::model::source::QuarantinedSource>> {
    use crate::model::source::{QuarantinedSource, SourceScope, SourceType};
    let (table, id) = if packed {
        ("packs", "id")
    } else {
        ("chunk_encodings", "encoding_id")
    };
    let rows: Vec<(Id, Uuid, String, String, String, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT {id},{id},state,to_char(quarantined_at AT TIME ZONE 'UTC','YYYY-MM-DD') || 'T' || to_char(quarantined_at AT TIME ZONE 'UTC','HH24:MI:SS.US') || 'Z',quarantine_code,quarantine_revision FROM {table} WHERE storage_id=$1 AND quarantined_at IS NOT NULL AND {id}>$2 ORDER BY {id} LIMIT $3")))
        .bind(storage).bind(after).bind(limit).fetch_all(db).await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, location_id, state, quarantined_at, code, revision)| QuarantinedSource {
                scope: SourceScope {
                    storage_id: storage,
                    source: if packed {
                        SourceType::Pack
                    } else {
                        SourceType::Chunk
                    },
                    id: id.to_string(),
                },
                location_id,
                state,
                quarantined_at,
                code,
                revision: revision.to_string(),
            },
        )
        .collect())
}

pub(super) fn change_source<'a>(
    snapshot: &crate::model::source::SourceSnapshot,
    code: Option<&'a str>,
) -> Result<Statement<'a>> {
    if snapshot.scope.source == crate::model::source::SourceType::Raw {
        return Ok(sqlx::query("UPDATE raw_sources SET quarantined_at=CASE WHEN $1 IS NULL THEN NULL ELSE now() END,quarantine_code=$2,revision=revision+1 WHERE id=$3 AND storage_id=$4 AND revision=$5 AND state='ready'")
            .bind(code).bind(code).bind(snapshot.scope.identity()?).bind(snapshot.scope.storage_id).bind(snapshot.revision));
    }
    let (table, id) = if snapshot.scope.packed() {
        ("packs", "id")
    } else {
        ("chunk_encodings", "encoding_id")
    };
    Ok(sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {table} SET quarantined_at=CASE WHEN $1::text IS NULL THEN NULL ELSE now() END,quarantine_code=$1,quarantine_revision=quarantine_revision+1 WHERE {id}=$2 AND storage_id=$3 AND quarantine_revision=$4 AND state='ready'")))
        .bind(code).bind(snapshot.scope.identity()?).bind(snapshot.scope.storage_id).bind(snapshot.revision))
}

pub(super) fn source_mode(mode: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT deployment_mode=$1 FROM mokyu_meta FOR SHARE").bind(mode)
}

pub(super) fn source_result(task: Uuid, result: Value) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET detail=detail||$2::jsonb,processed=processed+1 WHERE id=$1")
        .bind(task)
        .bind(result)
}

pub(super) fn quarantined_inputs(chunks: &[Id]) -> sqlx::QueryBuilder<Postgres> {
    let mut query = sqlx::QueryBuilder::new("SELECT EXISTS(SELECT 1 FROM chunks c WHERE ");
    source_scope(&mut query, None, chunks);
    query.push(" AND (EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=c.id AND l.state='ready' AND l.quarantined_at IS NOT NULL) OR EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.state='ready' AND p.quarantined_at IS NOT NULL)))");
    query
}

pub(super) fn splice_locks(
    streams: &[Id],
    chunk: Id,
    physical: bool,
) -> sqlx::QueryBuilder<Postgres> {
    let mut query = sqlx::QueryBuilder::new(if physical {
        "SELECT c.id FROM chunks c WHERE c.id="
    } else {
        "SELECT id FROM streams WHERE id IN ("
    });
    if physical {
        query
            .push_bind(chunk)
            .push(" OR c.id IN (SELECT chunk_id FROM extents WHERE stream_id IN (");
    }
    let mut values = query.separated(",");
    for stream in streams {
        values.push_bind(*stream);
    }
    query.push(if physical {
        ")) ORDER BY c.id FOR UPDATE"
    } else {
        ") ORDER BY id FOR UPDATE"
    });
    query
}

pub(super) fn allocated_source(stream: Id, chunk: &Chunk) -> sqlx::QueryBuilder<Postgres> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT l.encoding_id FROM chunks c JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.chunk_id=c.id WHERE c.state='uploading' AND l.state='uploading' AND l.quarantined_at IS NULL AND l.owner_task IS NULL AND c.id=",
    );
    query
        .push_bind(chunk.id)
        .push(" AND c.owner_stream=")
        .push_bind(stream)
        .push(" AND l.encoding_id=")
        .push_bind(chunk.encoding_id)
        .push(" AND l.encoding_id=")
        .push_bind(chunk.location_id)
        .push(" FOR UPDATE");
    query
}
pub(super) fn source_executor(task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT enabled AND NOT recovery_required AND status NOT IN ('released','draining') FROM nodes WHERE id=(SELECT lease_node_id FROM tasks WHERE id=$1) FOR SHARE").bind(task)
}
pub(super) fn source_input_versions(chunks: &[Id]) -> sqlx::QueryBuilder<Postgres> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT c.id,l.encoding_id,l.quarantine_revision,p.id,p.quarantine_revision FROM chunks c LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND (l.state='ready' OR (l.state='uploading' AND l.boot_id IS NOT NULL AND l.quarantined_at IS NOT NULL)) LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready' WHERE ",
    );
    source_scope(&mut query, None, chunks);
    query.push(" ORDER BY c.id,l.encoding_id,p.id");
    query
}
pub(super) fn save_source_inputs(task: Uuid, stamp: &Value) -> Statement<'_> {
    sqlx::query("UPDATE tasks SET detail=jsonb_set(detail,'{source_inputs}',$2::jsonb) WHERE id=$1")
        .bind(task)
        .bind(stamp)
}
pub(super) fn saved_source_inputs(task: Uuid) -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT detail->'source_inputs' FROM tasks WHERE id=$1").bind(task)
}

// Recovery candidates are private to their writing stream until full verification.
pub(super) fn repair_mode() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT deployment_mode='single' AND NOT maintenance FROM mokyu_meta FOR SHARE",
    )
}
pub(super) fn repair_chunk(input: &ChunkCreate) -> Rows<'_, Chunk> {
    sqlx::query_as("SELECT * FROM chunks WHERE storage_id=$1 AND hash=$2 AND raw_size=$3 AND state='ready' ORDER BY id LIMIT 1 FOR UPDATE")
        .bind(input.storage).bind(input.hash.as_slice()).bind(input.raw_size)
}
pub(super) fn repair_cluster_mode() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT deployment_mode='cluster' AND NOT maintenance FROM mokyu_meta FOR SHARE",
    )
}
pub(super) fn repair_node(node: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM nodes WHERE id=$1 AND enabled AND NOT recovery_required AND status NOT IN ('released','draining') FOR SHARE").bind(node)
}
pub(super) fn repair_lease(stream: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM stream_leases WHERE stream_id=$1 AND lease_type='write' AND expires_at>clock_timestamp() FOR SHARE").bind(stream)
}
pub(super) fn repair_stream(
    stream: Id,
) -> Rows<'static, (Uuid, String, Option<Uuid>, Option<Uuid>)> {
    sqlx::query_as("SELECT bucket_id,stream_type,owner_task_id,owner_lease_token FROM streams WHERE id=$1 AND state='writing' FOR UPDATE").bind(stream)
}
pub(super) fn repair_object(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT o.write_token FROM objects o JOIN streams s ON s.bucket_id=o.bucket_id AND s.object_key=o.object_key WHERE s.id=$1 FOR UPDATE").bind(stream)
}
pub(super) fn repair_part(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT write_token FROM parts WHERE write_token=$1 FOR UPDATE").bind(stream)
}
pub(super) fn repair_target(storage: Uuid, key: Option<Uuid>, algorithm: &str) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=$1 AND s.enabled AND s.block_key_id IS NOT DISTINCT FROM $2 AND ((s.block_key_id IS NULL AND $3='none') OR EXISTS(SELECT 1 FROM managed_keys k WHERE k.id=s.block_key_id AND k.key_algorithm=$4 AND k.revoked_at IS NULL)) FOR SHARE").bind(storage).bind(key).bind(algorithm).bind(algorithm)
}
pub(super) fn repair_pending(chunk: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=$1)").bind(chunk)
}
pub(super) fn insert_repair(chunk: &Chunk, stream: Id) -> Scalar<'_, Id> {
    sqlx::query_scalar("INSERT INTO chunk_encodings(chunk_id,storage_id,state,created_at,unreferenced_at,owner_stream,recovery_pending,encryption_algorithm,block_key_id,standalone_compressed) VALUES($1,$2,'uploading',$3,clock_timestamp(),$4,TRUE,$5,$6,FALSE) RETURNING encoding_id").bind(chunk.id).bind(chunk.storage_id).bind(chunk.created_at).bind(stream).bind(&chunk.algorithm).bind(chunk.block_key_id)
}
pub(super) fn repair_candidate(encoding: Id, stream: Id) -> Rows<'static, Chunk> {
    sqlx::query_as("SELECT c.id,l.encoding_id AS current_encoding_id,l.encoding_id AS location_id,c.storage_id,c.hash,c.raw_size,l.standalone_size,l.encryption_algorithm,l.block_key_id,l.standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE l.encoding_id=$1 AND l.owner_stream=$2 AND l.owner_task IS NULL AND l.state='uploading' AND l.recovery_pending AND l.quarantined_at IS NULL FOR UPDATE").bind(encoding).bind(stream)
}
pub(super) fn save_repair_metadata(chunk: &Chunk, stream: Id) -> Statement<'_> {
    sqlx::query("UPDATE chunk_encodings SET standalone_size=$1,standalone_compressed=$2,nonce=$3,nonce_id=$6 WHERE encoding_id=$4 AND owner_stream=$5 AND state='uploading'").bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(chunk.encoding_id).bind(stream).bind(crate::content::nonce::id(chunk.nonce.as_deref()))
}
pub(super) fn retire_repair_sources(chunk: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',sync_pending=FALSE,boot_id=NULL,unreferenced_at=clock_timestamp() WHERE chunk_id=$1 AND (state='ready' OR (state='uploading' AND boot_id IS NOT NULL)) AND quarantined_at IS NOT NULL").bind(chunk)
}
pub(super) fn publish_repair(encoding: Id, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET sync_pending=CASE WHEN boot_id IS NOT NULL THEN FALSE ELSE sync_pending END,boot_id=NULL,state='ready',owner_stream=NULL,recovery_pending=FALSE,unreferenced_at=NULL,stored_at=clock_timestamp() WHERE encoding_id=$1 AND owner_stream=$2 AND state='uploading' AND quarantined_at IS NULL").bind(encoding).bind(stream)
}
pub(super) fn discard_verified_repair(encoding: Id, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',owner_stream=NULL,recovery_pending=FALSE,unreferenced_at=clock_timestamp(),stored_at=clock_timestamp() WHERE encoding_id=$1 AND owner_stream=$2 AND state='uploading'").bind(encoding).bind(stream)
}
pub(super) fn publish_repaired_chunk(chunk: &Chunk) -> Statement<'_> {
    sqlx::query("UPDATE chunks SET current_encoding_id=$1,pack_id=NULL,standalone_size=$2,standalone_compressed=$3,nonce=$4,encryption_algorithm=$5,block_key_id=$6,unreferenced_at=NULL WHERE id=$7 AND state='ready'").bind(chunk.encoding_id).bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(&chunk.algorithm).bind(chunk.block_key_id).bind(chunk.id)
}

pub(super) fn retire_stream_encodings(stream: Id, batch: i64) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET owner_stream=NULL,state='retired',unreferenced_at=clock_timestamp() WHERE encoding_id IN (SELECT encoding_id FROM chunk_encodings WHERE owner_stream=$1 AND state='uploading' ORDER BY encoding_id LIMIT $2 FOR UPDATE)").bind(stream).bind(batch)
}

pub(super) fn repair_owner(
    stream: Id,
) -> Rows<'static, (Uuid, String, Option<Uuid>, Option<Uuid>)> {
    sqlx::query_as("SELECT bucket_id,stream_type,owner_task_id,owner_lease_token FROM streams WHERE id=$1 AND state='writing'").bind(stream)
}
pub(super) fn repair_upload(stream: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM uploads WHERE id=(SELECT r.upload_id FROM quota_writes w JOIN quota_reservations r ON r.id=w.reservation_id WHERE w.stream_id=$1) AND state='active' FOR UPDATE").bind(stream)
}

pub(super) fn source_audit<'a>(
    principal: &'a Principal,
    action: &'a str,
    target: &'a str,
    mut detail: Value,
) -> Result<Statement<'a>> {
    let (source, fallback, access_key) = match principal {
        Principal::Service { access_key, .. } => {
            detail["access_key_id"] = serde_json::json!(access_key);
            ("s3", "S3 credential", Some(access_key.as_str()))
        }
        Principal::User { .. } => ("web", "Deleted account", None),
        Principal::Token { .. } => ("token", "Deleted account", None),
        Principal::Local => ("cli", "Local CLI", None),
        _ => ("cli", "Internal operation", None),
    };
    if let Some(id) = crate::model::audit::current() {
        detail["request_audit_id"] = serde_json::json!(id.to_string());
    }
    ensure!(
        serde_json::to_vec(&detail)?.len() <= 262144,
        "audit detail exceeds its bound"
    );
    Ok(sqlx::query("INSERT INTO audit_events(actor_id,actor_label,token_id,source,action,target,outcome,finished_at,detail) VALUES($1,COALESCE((SELECT username FROM web_users WHERE id=$1),NULLIF(substr((SELECT label FROM s3_credentials WHERE access_key_id=$2),1,64),''),$3),$4,$5,$6,$7,'succeeded',now(),$8)")
        .bind(principal.user_id()).bind(access_key).bind(fallback).bind(principal.token_id()).bind(source).bind(action).bind(target).bind(detail))
}

use crate::model::raw::{RawProcessing, RawRange, RawSource, RawStore, RawUpload};
const RAW_COPY_READY: &str = "r.state='ready' AND r.quarantine_code IS NULL AND r.durability='durable' AND NOT r.pending_upload AND r.medium IN ('local','shared_s3') AND r.size>=0 AND length(r.digest)=16";

pub(super) fn raw_page(stream: Id, start: i64, end: i64) -> Rows<'static, RawRange> {
    sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,e.offset_bytes,e.length,e.source_offset FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id WHERE e.stream_id=$1 AND e.offset_bytes+e.length>=$2 AND e.offset_bytes<=$3 ORDER BY e.offset_bytes LIMIT 128")
            .bind(stream).bind(start).bind(end)
}

pub(super) fn raw_copyable(stream: Id, storage: Uuid) -> sqlx::QueryBuilder<Postgres> {
    let mut q = sqlx::QueryBuilder::new("SELECT EXISTS(SELECT 1 FROM raw_ranges WHERE stream_id=");
    q.push_bind(stream).push(") AND NOT EXISTS(SELECT 1 FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id WHERE e.stream_id=");
    q.push_bind(stream)
        .push(" AND NOT (r.storage_id=")
        .push_bind(storage)
        .push(" AND ")
        .push(RAW_COPY_READY)
        .push("))");
    q
}

pub(super) fn raw_copy_sources(storage: Uuid, sources: &[Id]) -> sqlx::QueryBuilder<Postgres> {
    let mut q = sqlx::QueryBuilder::new("SELECT r.id FROM raw_sources r WHERE r.storage_id=");
    q.push_bind(storage)
        .push(" AND ")
        .push(RAW_COPY_READY)
        .push(" AND r.id IN (");
    let mut list = q.separated(",");
    for id in sources {
        list.push_bind(*id);
    }
    q.push(") ORDER BY r.id FOR UPDATE");
    q
}

pub(super) fn raw_copy_position(stream: Id, offset: i64) -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT id FROM streams WHERE id=$1 AND size=$2 AND state='writing' AND payload_type='raw'",
    )
    .bind(stream)
    .bind(offset)
}

pub(super) fn raw_copy_ranges(
    destination: Id,
    source: Id,
    offset: i64,
    end: i64,
    sources: &[Id],
) -> sqlx::QueryBuilder<Postgres> {
    let mut q = sqlx::QueryBuilder::new(
        "INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id,source_offset) SELECT ",
    );
    q.push_bind(destination).push(",e.offset_bytes,e.length,e.source_id,e.source_offset FROM raw_ranges e WHERE e.stream_id=").push_bind(source);
    q.push(" AND e.offset_bytes>=")
        .push_bind(offset)
        .push(" AND e.offset_bytes<=")
        .push_bind(end);
    q.push(" AND (e.offset_bytes<")
        .push_bind(end)
        .push(" OR e.length=0) AND e.source_id IN (");
    let mut list = q.separated(",");
    for id in sources {
        list.push_bind(*id);
    }
    q.push(")");
    q
}

pub(super) fn raw_copy_reference(sources: &[Id]) -> sqlx::QueryBuilder<Postgres> {
    let mut q = sqlx::QueryBuilder::new("UPDATE raw_sources SET revision=revision+1 WHERE id IN (");
    let mut list = q.separated(",");
    for id in sources {
        list.push_bind(*id);
    }
    q.push(")");
    q
}

pub(super) fn raw_target(
    stream: Id,
    storage: Uuid,
) -> Rows<'static, (String, bool, i64, bool, bool)> {
    // Keep settings stable without blocking another writer's shared bucket lock.
    sqlx::query_as("SELECT t.type,(t.block_key_id IS NULL AND NOT p.compression_enabled AND NOT p.pack_enabled),p.revision,p.cdc_enabled,((p.deduplication_enabled OR p.cdc_enabled) AND p.write_processing='background') FROM streams s JOIN buckets b ON b.id=s.bucket_id JOIN storages t ON t.id=b.storage_id JOIN storage_profiles p ON p.storage_id=t.id WHERE s.id=$1 AND t.id=$2 AND t.enabled AND s.state='writing' FOR UPDATE OF s FOR SHARE OF b,t,p").bind(stream).bind(storage)
}
pub(super) fn raw_prepare(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET payload_type='raw' WHERE id=$1 AND state='writing' AND payload_type='chunks' AND NOT EXISTS(SELECT 1 FROM extents WHERE stream_id=$2) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE stream_id=$3)").bind(stream).bind(stream).bind(stream)
}
pub(super) fn raw_insert(
    stream: Id,
    storage: Uuid,
    revision: i64,
    medium: &'static str,
    buffered: bool,
) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "INSERT INTO raw_sources(owner_stream,storage_id,profile_revision,medium,durability) VALUES($1,$2,$3,$4,$5) RETURNING id",
    )

    .bind(stream)
    .bind(storage)
    .bind(revision).bind(medium).bind(if buffered { "buffered" } else { "durable" })
}
pub(super) fn raw_finish(
    stream: Id,
    source: Id,
    size: i64,
    digest: &[u8],
    final_source: bool,
) -> Statement<'_> {
    sqlx::query("UPDATE raw_sources SET size=$1,digest=$2,durability=CASE WHEN $5 AND medium='shared_s3' THEN 'durable' ELSE durability END,state='ready',owner_stream=NULL,upload_id=NULL WHERE id=$3 AND owner_stream=$4 AND state='writing'").bind(size).bind(digest).bind(source).bind(stream).bind(final_source)
}
pub(super) fn raw_attach(stream: Id, source: Id, size: i64) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id) VALUES($1,0,$2,$3)",
    )
    .bind(stream)
    .bind(size)
    .bind(source)
}
pub(super) fn raw_sources_lock(stream: Id) -> Rows<'static, RawSource> {
    sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id IN (SELECT source_id FROM raw_ranges WHERE stream_id=$1) ORDER BY r.id FOR UPDATE").bind(stream)
}
pub(super) fn raw_continuity(stream: Id) -> Rows<'static, (i64, bool)> {
    sqlx::query_as("SELECT COALESCE(MAX(e.offset_bytes+e.length),0),COALESCE(bool_and(e.offset_bytes=e.previous_end AND e.source_offset+e.length<=r.size),TRUE) AND NOT EXISTS(SELECT 1 FROM extents WHERE stream_id=$1) FROM (SELECT *,LAG(offset_bytes+length,1,0) OVER(ORDER BY offset_bytes) AS previous_end FROM raw_ranges WHERE stream_id=$2) e JOIN raw_sources r ON r.id=e.source_id").bind(stream).bind(stream)
}
pub(super) fn raw_cleanup_ranges(stream: Id, batch: i64) -> Rows<'static, (i64, Id)> {
    sqlx::query_as("SELECT offset_bytes,source_id FROM raw_ranges WHERE stream_id=$1 ORDER BY offset_bytes LIMIT $2").bind(stream).bind(batch)
}
pub(super) fn raw_touch_unreferenced(source: Id) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET unreferenced_at=now() WHERE id=$1").bind(source)
}
pub(super) fn raw_detach(stream: Id, end: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_ranges WHERE stream_id=$1 AND offset_bytes<=$2")
        .bind(stream)
        .bind(end)
}

#[async_trait::async_trait]
impl RawStore for Backend {
    async fn recover_memory(&self, boot: Uuid) -> Result<()> {
        transactions::recover_raw_memory(&self.pool, boot).await
    }
    async fn buffered_status(&self) -> Result<serde_json::Value> {
        let (pending, bytes, oldest, volatile): (i64,i64,Option<chrono::DateTime<chrono::Utc>>,i64) = sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(size),0) AS BIGINT),MIN(created_at),CAST(COALESCE(SUM(CASE WHEN medium='memory' THEN 1 ELSE 0 END),0) AS BIGINT) FROM raw_sources WHERE durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL").fetch_one(&self.pool).await?;
        Ok(serde_json::json!({"pending":pending,"bytes":bytes,"oldest":oldest,"volatile":volatile}))
    }

    async fn buffered_source(&self, source: Id) -> Result<Option<RawSource>> {
        Ok(sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id=$1 AND r.durability IN ('buffered','volatile') AND r.state='ready' AND r.quarantine_code IS NULL").bind(source).fetch_optional(&self.pool).await?)
    }
    async fn buffered_duplicate(
        &self,
        storage: Uuid,
        hash: &[u8],
        size: i64,
    ) -> Result<Option<RawSource>> {
        Ok(sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.storage_id=$1 AND r.content_hash=$2 AND r.size=$3 AND r.durability IN ('buffered','volatile') AND r.state='ready' AND r.quarantine_code IS NULL").bind(storage).bind(hash).bind(size).fetch_optional(&self.pool).await?)
    }
    async fn buffer(&self, source: Id) -> Result<()> {
        ensure!(sqlx::query("UPDATE raw_sources SET durability='buffered' WHERE id=$1 AND state='writing' AND owner_stream IS NOT NULL").bind(source).execute(&self.pool).await?.rows_affected()==1, crate::model::Error::OperationAborted);
        Ok(())
    }
    async fn buffered(
        &self,
        after: Id,
        before: Option<chrono::DateTime<chrono::Utc>>,
        stream: Option<Id>,
        normal: bool,
        urgent: &[Uuid],
    ) -> Result<Vec<RawSource>> {
        let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r JOIN storage_profiles sp ON sp.storage_id=r.storage_id WHERE r.durability IN ('buffered','volatile') AND r.state='ready' AND r.quarantine_code IS NULL AND r.id>",
        );
        query.push_bind(after);
        if let Some(before) = before {
            query.push(" AND r.created_at<=").push_bind(before);
        }
        if let Some(stream) = stream {
            query.push(" AND EXISTS(SELECT 1 FROM raw_ranges e WHERE e.source_id=r.id AND e.stream_id=").push_bind(stream).push(")");
        }
        if normal {
            query.push(" AND r.medium<>'memory'");
            write_delay(&mut query, "r.created_at", "r.storage_id", urgent);
        }
        query.push(" ORDER BY r.id LIMIT 128");
        Ok(query.build_query_as().fetch_all(&self.pool).await?)
    }
    async fn synced(&self, source: &RawSource, failure: Option<&str>) -> Result<()> {
        if let Some(code) = failure {
            ensure!(
                crate::model::source::quarantine_code(code),
                "invalid source quarantine code"
            );
            sqlx::query("UPDATE raw_sources SET quarantine_code=$1,quarantined_at=clock_timestamp(),revision=revision+1 WHERE id=$2 AND revision=$3 AND durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL").bind(code).bind(source.id).bind(source.revision).execute(&self.pool).await?;
        } else {
            sqlx::query("UPDATE raw_sources SET durability='durable',medium=CASE WHEN medium='memory' THEN 'local' ELSE medium END,boot_id=NULL WHERE id=$1 AND revision=$2 AND durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL").bind(source.id).bind(source.revision).execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn inline_conversion(
        &self,
        stream: Id,
        source: Id,
    ) -> Result<crate::model::cache::EncodingPolicy> {
        transactions::inline_conversion(&self.pool, stream, source).await
    }
    async fn begin_conversion(
        &self,
        stream: Id,
        storage: Uuid,
        revision: i64,
        encoding: &crate::model::cache::EncodingPolicy,
        buffered: bool,
    ) -> Result<(Id, Uuid)> {
        transactions::begin_raw(
            &self.pool,
            stream,
            storage,
            revision,
            Some(encoding),
            buffered,
        )
        .await
    }
    async fn commit_conversion_batch(
        &self,
        task: Execution,
        candidate: Id,
        rows: &[crate::model::Mapping],
        outputs: &[crate::model::packing::PackOutput],
    ) -> Result<()> {
        transactions::commit_conversion_batch(&self.pool, task, candidate, rows, outputs).await
    }
    async fn finish_conversion(
        &self,
        task: Execution,
        job: &crate::model::raw::Conversion,
        source: &RawSource,
        candidate: Id,
        lease: Option<Uuid>,
    ) -> Result<bool> {
        transactions::finish_conversion(&self.pool, task, job, source, candidate, lease).await
    }
    async fn uploads_before(
        &self,
        before: chrono::DateTime<chrono::Utc>,
        owner: Option<Uuid>,
    ) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE r.created_at<=$1 AND ($2::uuid IS NULL OR u.node_id=$2)) OR EXISTS(SELECT 1 FROM parts p JOIN streams s ON s.id=p.stream_id JOIN uploads u ON u.id=p.upload_id WHERE p.remote_node_id IS NOT NULL AND p.remote_etag IS NULL AND ($2::uuid IS NULL OR p.remote_node_id=$2) AND s.created_at<=$1 AND u.state IN ('active','completing'))").bind(before).bind(owner).fetch_one(&self.pool).await?)
    }
    async fn upload_status(&self) -> Result<Value> {
        let (pending,bytes,failed):(i64,i64,i64)=sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(size),0) AS bigint),CAST(COALESCE(SUM(CASE WHEN last_error IS NOT NULL THEN 1 ELSE 0 END),0) AS bigint) FROM (SELECT r.size,u.last_error FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id UNION ALL SELECT s.size,p.remote_error FROM parts p JOIN streams s ON s.id=p.stream_id JOIN uploads u ON u.id=p.upload_id WHERE p.remote_node_id IS NOT NULL AND p.remote_etag IS NULL AND u.state IN ('active','completing')) waiting").fetch_one(&self.pool).await?;
        Ok(serde_json::json!({"pending":pending,"bytes":bytes,"failed":failed}))
    }
    async fn abandon_upload(
        &self,
        work: &RawUpload,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::abandon_raw_upload(&self.pool, work, node).await
    }

    async fn cached_checkpoint(&self, location: Uuid) -> Result<Option<Id>> {
        Ok(sqlx::query_scalar("SELECT source_id FROM multipart_files m WHERE m.id=$1 AND source_id IS NOT NULL AND m.state<>'deleting' AND EXISTS(SELECT 1 FROM fragments WHERE file_id=m.id)").bind(location).fetch_optional(&self.pool).await?)
    }
    async fn cached_checkpoints(&self, after: Uuid) -> Result<Vec<(Uuid, Id)>> {
        Ok(sqlx::query_as("SELECT m.id,m.source_id FROM multipart_files m JOIN storages s ON s.id=m.storage_id WHERE m.id>$1 AND s.type='s3' AND m.source_id IS NOT NULL AND m.state<>'deleting' AND EXISTS(SELECT 1 FROM fragments WHERE file_id=m.id) ORDER BY m.id LIMIT 128").bind(after).fetch_all(&self.pool).await?)
    }
    async fn has_upload(&self, source: Id) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw_uploads WHERE source_id=$1)")
                .bind(source)
                .fetch_one(&self.pool)
                .await?,
        )
    }
    async fn stage_upload(
        &self,
        stream: Id,
        source: Id,
        size: i64,
        digest: &[u8],
        revision: i64,
        deduplicate: bool,
        background: bool,
        boot: Option<Uuid>,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<()> {
        transactions::finish_raw(
            &self.pool,
            stream,
            source,
            size,
            digest,
            Some((revision, deduplicate, background, boot)),
            node,
        )
        .await
    }
    async fn uploads(&self, after: Id, owner: Option<Uuid>) -> Result<Vec<RawUpload>> {
        Ok(raw_uploads(after, owner).fetch_all(&self.pool).await?)
    }
    async fn claim_upload(
        &self,
        token: Uuid,
        before: Option<chrono::DateTime<chrono::Utc>>,
        node: crate::model::raw::Processor<'_>,
        urgent: &[Uuid],
    ) -> Result<Option<RawUpload>> {
        transactions::claim_raw_upload(&self.pool, token, before, node, urgent).await
    }
    async fn renew_upload(
        &self,
        work: &RawUpload,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::renew_raw_upload(&self.pool, work, node).await
    }
    async fn retry_upload(&self, work: &RawUpload, error: Option<&str>) -> Result<()> {
        raw_upload_retry(work, error).execute(&self.pool).await?;
        Ok(())
    }
    async fn set_pending_upload(
        &self,
        source: Id,
        token: Uuid,
        upload: Option<&str>,
    ) -> Result<()> {
        anyhow::ensure!(
            upload.is_none_or(|s| !s.is_empty() && s.len() <= 4096),
            "invalid raw multipart upload id"
        );
        anyhow::ensure!(
            raw_upload_id(source, token, upload)
                .execute(&self.pool)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        Ok(())
    }
    async fn upload_streams(&self, source: Id) -> Result<Vec<Id>> {
        Ok(raw_upload_streams(source).fetch_all(&self.pool).await?)
    }
    async fn reuse_upload(
        &self,
        work: &RawUpload,
        hash: &[u8],
        streams: &[Id],
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::reuse_raw_upload(&self.pool, work, hash, streams, node).await
    }
    async fn complete_upload(
        &self,
        work: &RawUpload,
        hash: Option<&[u8]>,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<()> {
        transactions::complete_raw_upload(&self.pool, work, hash, node).await
    }
    async fn current(&self, source: Id) -> Result<Option<RawSource>> {
        Ok(raw_source(source).fetch_optional(&self.pool).await?)
    }

    async fn reuse(
        &self,
        stream: Id,
        source: Id,
        storage: Uuid,
        size: i64,
        digest: &[u8],
        hash: &[u8],
    ) -> Result<bool> {
        transactions::reuse_raw(&self.pool, stream, source, storage, size, digest, hash).await
    }

    async fn claim_processing(
        &self,
        token: Uuid,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<Option<RawProcessing>> {
        transactions::claim_raw_processing(&self.pool, node, token).await
    }
    async fn renew_processing(
        &self,
        work: &RawProcessing,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::renew_raw_processing(&self.pool, work, node).await
    }
    async fn save_processing_hash(
        &self,
        work: &RawProcessing,
        revision: i64,
        hash: &[u8],
    ) -> Result<()> {
        anyhow::ensure!(hash.len() == 32, "invalid raw processing hash");
        anyhow::ensure!(sqlx::query("UPDATE raw_processing SET content_hash=$1,source_revision=$2 WHERE stream_id=$3 AND token=$4 AND (lease_expires_at IS NULL OR lease_expires_at>clock_timestamp())")
            .bind(hash).bind(revision).bind(work.stream).bind(work.token).execute(&self.pool).await?.rows_affected()==1,
            crate::model::Error::OperationAborted);
        Ok(())
    }
    async fn retry_processing(&self, work: &RawProcessing, error: Option<&str>) -> Result<()> {
        let error = error.map(|message| message.chars().take(1024).collect::<String>());
        sqlx::query("UPDATE raw_processing SET token=NULL,lease_expires_at=NULL,last_error=$1,attempts=attempts+$2,next_attempt_at=clock_timestamp()+$3*interval '1 second' WHERE stream_id=$4 AND token=$5")
            .bind(&error).bind(i32::from(error.is_some())).bind(if error.is_some() {30i64} else {1}).bind(work.stream).bind(work.token).execute(&self.pool).await?;
        Ok(())
    }
    async fn cancel_processing(&self, work: &RawProcessing) -> Result<()> {
        raw_processing_delete(work).execute(&self.pool).await?;
        Ok(())
    }
    async fn finish_processing(
        &self,
        work: &RawProcessing,
        source: &RawSource,
        hash: &[u8],
        lease: Option<Uuid>,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::finish_raw_processing(&self.pool, work, source, hash, lease, node).await
    }
    async fn processing_status(&self) -> Result<Value> {
        let (pending,failed,bytes): (i64,i64,i64)=sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM((w.last_error IS NOT NULL)::integer),0) AS bigint),CAST(COALESCE(SUM(r.size),0) AS bigint) FROM raw_processing w JOIN raw_sources r ON r.id=w.source_id")
            .fetch_one(&self.pool).await?;
        Ok(serde_json::json!({"pending":pending,"failed":failed,"bytes":bytes}))
    }

    async fn set_upload(&self, source: Id, upload: &str) -> Result<()> {
        anyhow::ensure!(
            !upload.is_empty() && upload.len() <= 4096,
            "invalid raw multipart upload id"
        );
        let changed = sqlx::query("UPDATE raw_sources SET upload_id=$1 WHERE id=$2 AND state='writing' AND medium='shared_s3' AND upload_id IS NULL")
            .bind(upload).bind(source).execute(&self.pool).await?.rows_affected();
        anyhow::ensure!(changed == 1, crate::model::Error::OperationAborted);
        Ok(())
    }
    async fn upload_id(&self, source: Id) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT upload_id FROM raw_sources WHERE id=$1")
                .bind(source)
                .fetch_optional(&self.pool)
                .await?
                .flatten(),
        )
    }
    async fn deduplicate(&self, stream: Id, source: Id, hash: &[u8]) -> Result<bool> {
        transactions::deduplicate_raw(&self.pool, stream, source, hash).await
    }

    async fn quarantines(
        &self,
        storage: Uuid,
        after: Id,
        limit: i64,
    ) -> Result<Vec<crate::model::source::QuarantinedSource>> {
        use crate::model::source::{QuarantinedSource, SourceScope, SourceType};
        let rows: Vec<(Id,Uuid,String,String,String,i64)> = sqlx::query_as("SELECT id,id,state,to_char(quarantined_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),quarantine_code,revision FROM raw_sources WHERE storage_id=$1 AND id>$2 AND quarantined_at IS NOT NULL ORDER BY id LIMIT $3")
            .bind(storage).bind(after).bind(limit).fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(
                |(id, location_id, state, quarantined_at, code, revision)| QuarantinedSource {
                    scope: SourceScope {
                        storage_id: storage,
                        source: SourceType::Raw,
                        id: id.to_string(),
                    },
                    location_id,
                    state,
                    quarantined_at,
                    code,
                    revision: revision.to_string(),
                },
            )
            .collect())
    }
    async fn begin(
        &self,
        stream: Id,
        storage: Uuid,
        profile_revision: i64,
        buffered: bool,
    ) -> Result<(Id, Uuid)> {
        transactions::begin_raw(
            &self.pool,
            stream,
            storage,
            profile_revision,
            None,
            buffered,
        )
        .await
    }
    async fn finish(&self, stream: Id, source: Id, size: i64, digest: &[u8]) -> Result<()> {
        transactions::finish_raw(&self.pool, stream, source, size, digest, None, None).await
    }
    async fn copyable(&self, stream: Id, storage: Uuid) -> Result<bool> {
        Ok(raw_copyable(stream, storage)
            .build_query_scalar()
            .fetch_one(&self.pool)
            .await?)
    }
    async fn copy_references(
        &self,
        destination: Id,
        source: Id,
        storage: Uuid,
        revision: i64,
        rows: &[RawRange],
    ) -> Result<()> {
        transactions::copy_raw_references(&self.pool, destination, source, storage, revision, rows)
            .await
    }
    async fn page(&self, stream: Id, start: i64, end: i64) -> Result<Vec<RawRange>> {
        Ok(raw_page(stream, start, end).fetch_all(&self.pool).await?)
    }
    async fn candidates(
        &self,
        storage: Uuid,
        grace: i64,
        minimum: i64,
        limit: i64,
    ) -> Result<Vec<RawSource>> {
        anyhow::ensure!(
            (1..=128).contains(&limit) && grace >= 0 && minimum >= 0,
            "invalid raw cleanup bounds"
        );
        Ok(raw_gc_candidates(storage, grace, minimum, limit)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn claim(&self, source: Id, revision: i64, grace: i64, minimum: i64) -> Result<bool> {
        Ok(raw_gc_claim(source, revision, grace, minimum)
            .execute(&self.pool)
            .await?
            .rows_affected()
            == 1)
    }
    async fn removed(&self, source: Id) -> Result<()> {
        raw_gc_finish(source).execute(&self.pool).await?;
        Ok(())
    }
}

pub(super) fn raw_lock_stream(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT id FROM streams WHERE id=$1 AND state='writing' AND payload_type='raw' FOR UPDATE ",
    )
    .bind(stream)
}

pub(super) fn raw_source(id: Id) -> Rows<'static, RawSource> {
    sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id=$1 FOR UPDATE").bind(id)
}

pub(super) fn raw_duplicate(storage: Uuid, hash: &[u8], size: i64) -> Rows<'_, RawSource> {
    sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.storage_id=$1 AND r.content_hash=$2 AND r.size=$3 FOR UPDATE")
        .bind(storage).bind(hash).bind(size)
}
pub(super) fn raw_hash(id: Id, hash: Option<&[u8]>) -> Statement<'_> {
    sqlx::query("UPDATE raw_sources SET content_hash=$1 WHERE id=$2")
        .bind(hash)
        .bind(id)
}
pub(super) fn raw_share(
    stream: Id,
    previous: Id,
    replacement: Id,
    size: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE raw_ranges SET source_id=$1 WHERE stream_id=$2 AND source_id=$3 AND offset_bytes=0 AND source_offset=0 AND length=$4")
        .bind(replacement).bind(stream).bind(previous).bind(size)
}

pub(super) fn raw_reference(source: Id) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET revision=revision+1 WHERE id=$1").bind(source)
}

pub(super) fn lock_local_upload(upload: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT CASE WHEN remote_manifest_hash IS NOT NULL AND state='active' THEN 'completing' ELSE state END FROM uploads WHERE id=$1 FOR UPDATE").bind(upload)
}
pub(super) fn current_multipart_file(upload: Uuid, node: Option<Uuid>) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM multipart_files WHERE upload_id=$1 AND node_id IS NOT DISTINCT FROM $2 AND state='open' ORDER BY id LIMIT 1",
    )
    .bind(upload).bind(node)
}
pub(super) fn insert_multipart_file(upload: Uuid, node: Option<Uuid>) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("INSERT INTO multipart_files(upload_id,storage_id,node_id) SELECT u.id,b.storage_id,$2 FROM uploads u JOIN buckets b ON b.id=u.bucket_id JOIN storages s ON s.id=b.storage_id WHERE u.id=$1 AND u.state='active' AND EXISTS(SELECT 1 FROM mokyu_meta WHERE singleton AND (deployment_mode='cluster')=($2::uuid IS NOT NULL)) RETURNING id").bind(upload).bind(node)
}

pub(super) fn insert_multipart_checkpoint(upload: Uuid, source: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO multipart_files(id,upload_id,storage_id,source_id,state) SELECT r.id,$2,r.storage_id,r.id,'sealed' FROM raw_sources r WHERE r.id=$1 AND r.state='writing' AND EXISTS(SELECT 1 FROM mokyu_meta WHERE singleton AND deployment_mode='single') AND r.owner_stream IS NOT NULL ON CONFLICT(id) DO NOTHING").bind(source).bind(upload)
}
pub(super) fn remap_multipart_range<'a>(
    upload: Uuid,
    source: Id,
    range: &'a crate::model::multipart::FileRemap,
) -> Statement<'a> {
    sqlx::query("UPDATE fragments SET file_id=$1,file_offset=$2 WHERE id=$3 AND file_id=$4 AND file_offset=$5 AND size=$6 AND hash=$7 AND sealed AND EXISTS(SELECT 1 FROM extents e JOIN parts p ON p.stream_id=e.stream_id WHERE e.fragment_id=fragments.id AND p.upload_id=$8)").bind(source).bind(range.destination).bind(range.fragment).bind(range.file).bind(range.offset).bind(range.size).bind(range.hash.as_slice()).bind(upload)
}

pub(super) fn pending_multipart_reclaim(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE multipart_files SET reclaim_pending=true WHERE id=$1 AND state='sealed'")
        .bind(id)
}

pub(super) fn cache_policy(chunk: Id) -> Rows<'static, (String, String, Option<Value>)> {
    sqlx::query_as("SELECT COALESCE(u.encoding_policy->>'pack_max_size',p.pack_max_size),COALESCE(u.encoding_policy->>'pack_flush_timeout',p.pack_upload_flush_timeout),u.encoding_policy FROM chunks c JOIN storage_profiles p ON p.storage_id=c.storage_id JOIN pending_uploads u ON u.chunk_id=c.id WHERE c.id=$1").bind(chunk)
}

pub(super) fn pack_policy_due(storage: Uuid, policy: &str, seconds: u64) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM storage_maintenance WHERE storage_id=$1 AND policy=$2 AND last_scheduled_at>now()-$3*interval '1 second')").bind(storage).bind(policy).bind(seconds as i64)
}
pub(super) fn pack_policy_scheduled(storage: Uuid, policy: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_maintenance(storage_id,policy,last_scheduled_at) VALUES($1,$2,now()) ON CONFLICT(storage_id,policy) DO UPDATE SET last_scheduled_at=EXCLUDED.last_scheduled_at").bind(storage).bind(policy)
}

pub(super) fn raw_enqueue_processing(stream: Id, source: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO raw_processing(stream_id,source_id,payload_revision) SELECT id,$1,payload_revision FROM streams WHERE id=$2").bind(source).bind(stream)
}

pub(super) fn raw_processing_due(single: bool) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT w.stream_id FROM raw_processing w JOIN streams s ON s.id=w.stream_id WHERE NOT EXISTS(SELECT 1 FROM raw_uploads u WHERE u.source_id=w.source_id) AND s.state<>'writing' AND (s.state<>'ready' OR EXISTS(SELECT 1 FROM objects o WHERE o.stream_id=s.id)) AND w.next_attempt_at<=clock_timestamp() AND ($1 OR w.token IS NULL OR w.lease_expires_at IS NULL OR w.lease_expires_at<clock_timestamp()) ORDER BY w.next_attempt_at,w.stream_id LIMIT 1 FOR UPDATE OF w SKIP LOCKED").bind(single)
}

pub(super) fn raw_processing_claim(stream: Id, token: Uuid, single: bool) -> Statement<'static> {
    sqlx::query("UPDATE raw_processing SET token=$1,lease_expires_at=CASE WHEN $2 THEN NULL ELSE clock_timestamp()+interval '120 seconds' END WHERE stream_id=$3").bind(token).bind(single).bind(stream)
}

pub(super) fn raw_processing_record(stream: Id) -> Rows<'static, RawProcessing> {
    sqlx::query_as("SELECT w.stream_id AS stream,w.source_id AS source,s.bucket_id AS bucket,s.object_key AS object_key,w.payload_revision,w.content_hash,w.source_revision,w.token FROM raw_processing w JOIN streams s ON s.id=w.stream_id WHERE w.stream_id=$1").bind(stream)
}

pub(super) fn raw_processing_owned(work: &RawProcessing) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT source_id FROM raw_processing WHERE stream_id=$1 AND token=$2 AND (lease_expires_at IS NULL OR lease_expires_at>clock_timestamp()) FOR UPDATE").bind(work.stream).bind(work.token)
}

pub(super) fn raw_processing_delete(work: &RawProcessing) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_processing WHERE stream_id=$1 AND token=$2")
        .bind(work.stream)
        .bind(work.token)
}

pub(super) fn raw_processing_stream(work: &RawProcessing) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=$1 AND state='ready' AND payload_type='raw' AND payload_revision=$2 AND EXISTS(SELECT 1 FROM objects WHERE stream_id=streams.id) FOR UPDATE").bind(work.stream).bind(work.payload_revision)
}

pub(super) fn raw_processing_changed(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET payload_revision=payload_revision+1 WHERE id=$1").bind(stream)
}

pub(super) fn raw_processing_readers(stream: Id, lease: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM stream_leases WHERE stream_id=$1 AND lease_type='read' AND id<>$2 AND expires_at>clock_timestamp())").bind(stream).bind(lease)
}

pub(super) fn renew_raw_processing(work: &RawProcessing) -> Statement<'_> {
    sqlx::query("UPDATE raw_processing SET lease_expires_at=clock_timestamp()+interval '120 seconds' WHERE stream_id=$1 AND token=$2 AND (lease_expires_at IS NULL OR lease_expires_at>clock_timestamp())")
        .bind(work.stream).bind(work.token)
}

pub(super) fn nonce_counter(key: Uuid, type_tag: u8) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT n.reserved_id FROM nonce_counters n JOIN managed_keys k ON k.id=n.block_key_id WHERE n.block_key_id=$1 AND n.type_tag=$2 AND k.purpose='block' AND k.revoked_at IS NULL FOR UPDATE OF n,k").bind(key).bind(i64::from(type_tag))
}

pub(super) fn advance_nonce(
    key: Uuid,
    type_tag: u8,
    previous: i64,
    last: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE nonce_counters SET reserved_id=$1 WHERE block_key_id=$2 AND type_tag=$3 AND reserved_id=$4").bind(last).bind(key).bind(i64::from(type_tag)).bind(previous)
}

pub(super) fn initialize_nonce_counters(key: Uuid) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO nonce_counters(block_key_id,type_tag,reserved_id) VALUES($1,0,0),($1,1,0)",
    )
    .bind(key)
}

use crate::model::key_replacement::{
    KeyReplacementInput, KeyReplacementPreview, KeyReplacementResult, KeyReplacementStorage,
    ReplacementRow,
};

pub(super) fn replacement_storages(source: Uuid) -> Rows<'static, KeyReplacementStorage> {
    sqlx::query_as("SELECT s.id,s.name,s.enabled,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.block_key_id=$1 ORDER BY s.id FOR UPDATE OF s,p").bind(source)
}
pub(super) fn replacement_key(id: Uuid) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT name,key_algorithm FROM managed_keys WHERE id=$1 AND purpose='block' AND revoked_at IS NULL FOR UPDATE").bind(id)
}
pub(super) fn replacement_capacity(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT count(*)=2 AND min(reserved_id)>=0 AND max(reserved_id)<=9223372036854775743 FROM nonce_counters WHERE block_key_id=$1").bind(id)
}
pub(super) fn insert_key_replacement<'q>(
    actor: &'q str,
    plan: &'q KeyReplacementPreview,
) -> Statement<'q> {
    sqlx::query("INSERT INTO key_replacements(id,actor,plan,expires_at) VALUES($1,$2,$3,$4)")
        .bind(plan.id)
        .bind(actor)
        .bind(sqlx::types::Json(plan))
        .bind(plan.expires_at)
}
pub(super) fn key_replacement<'q>(id: Uuid, actor: &'q str) -> Rows<'q, ReplacementRow> {
    sqlx::query_as("SELECT plan,result FROM key_replacements WHERE id=$1 AND actor=$2 FOR UPDATE")
        .bind(id)
        .bind(actor)
}
pub(super) fn complete_key_replacement(result: &KeyReplacementResult) -> Statement<'_> {
    sqlx::query("UPDATE key_replacements SET result=$1,expires_at=clock_timestamp()+interval '7 days' WHERE id=$2 AND result IS NULL").bind(sqlx::types::Json(result)).bind(result.preview.id)
}

pub(super) fn raw_receiving(source: Id, stream: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT storage_id FROM raw_sources WHERE id=$1 AND owner_stream=$2 AND state='writing' AND medium='shared_s3' AND upload_id IS NULL FOR UPDATE").bind(source).bind(stream)
}
pub(super) fn raw_discard(source: Id, stream: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_sources WHERE id=$1 AND owner_stream=$2 AND state='writing' AND upload_id IS NULL AND NOT EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.kind='raw' AND n.location_id=raw_sources.id AND l.expires_at>clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM uploads WHERE raw_source_id=raw_sources.id)").bind(source).bind(stream)
}

pub(super) fn raw_reusable<'a>(
    storage: Uuid,
    hash: &'a [u8],
    size: i64,
    digest: &'a [u8],
) -> Scalar<'a, Id> {
    sqlx::query_scalar("SELECT id FROM raw_sources WHERE storage_id=$1 AND content_hash=$2 AND size=$3 AND digest=$4 AND state='ready' AND medium='shared_s3' AND NOT pending_upload AND quarantine_code IS NULL").bind(storage).bind(hash).bind(size).bind(digest)
}

pub(super) fn raw_uploads(after: Id, owner: Option<Uuid>) -> Rows<'static, RawUpload> {
    sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,u.token,u.deduplicate,u.background,u.content_hash FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE u.source_id>$1 AND u.node_id IS NOT DISTINCT FROM $2 ORDER BY u.source_id LIMIT 128").bind(after).bind(owner)
}
pub(super) fn raw_upload_record(source: Id) -> Rows<'static, RawUpload> {
    sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,u.token,u.deduplicate,u.background,u.content_hash FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE u.source_id=$1 FOR UPDATE").bind(source)
}
pub(super) fn raw_upload_due(
    before: Option<chrono::DateTime<chrono::Utc>>,
    owner: Option<Uuid>,
    urgent: &[Uuid],
) -> sqlx::QueryBuilder<sqlx::Postgres> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT u.source_id FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id JOIN storage_profiles sp ON sp.storage_id=r.storage_id WHERE u.next_attempt_at<=clock_timestamp() AND r.state='ready'",
    );
    query
        .push(" AND u.node_id IS NOT DISTINCT FROM ")
        .push_bind(owner)
        .push(" AND (")
        .push_bind(owner)
        .push("::uuid IS NULL OR u.token IS NULL OR u.lease_expires_at<clock_timestamp())");
    if let Some(before) = before {
        query.push(" AND r.created_at<=").push_bind(before);
    } else {
        query.push(" AND (NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=r.processing_task AND t.state<>'completed') OR r.created_at+sp.write_backlog_max_age*interval '1 second'<=clock_timestamp())");
        write_delay(&mut query, "r.created_at", "r.storage_id", urgent);
    }
    query.push(" ORDER BY CASE WHEN r.created_at+(sp.write_backlog_max_age)*interval '1 second'<=clock_timestamp() THEN 0 ELSE 1 END,u.background,u.next_attempt_at,u.source_id LIMIT 1 FOR UPDATE OF u SKIP LOCKED");
    query
}

pub(super) fn raw_upload_claim(source: Id, token: Uuid, node: Option<Uuid>) -> Statement<'static> {
    sqlx::query("UPDATE raw_uploads SET token=$1,node_id=$3,lease_expires_at=CASE WHEN $3::uuid IS NULL THEN NULL ELSE clock_timestamp()+interval '120 seconds' END WHERE source_id=$2").bind(token).bind(source).bind(node)
}
pub(super) fn raw_queue(
    source: Id,
    deduplicate: bool,
    background: bool,
    owner: Option<Uuid>,
) -> Statement<'static> {
    sqlx::query(
        "WITH scoped AS (UPDATE tasks SET execution_scope=CASE WHEN $4::uuid IS NULL THEN 'cluster' ELSE 'node' END,target_node_id=$4 WHERE id=(SELECT processing_task FROM raw_sources WHERE id=$1)) INSERT INTO raw_uploads(source_id,deduplicate,background,node_id) VALUES($1,$2,$3,$4)",
    )
    .bind(source)
    .bind(deduplicate)
    .bind(background)
    .bind(owner)
}
pub(super) fn raw_stage(source: Id, revision: i64, boot: Option<Uuid>) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET pending_upload=true,medium=CASE WHEN $3::uuid IS NULL THEN 'local' ELSE 'memory' END,boot_id=$3,durability=CASE WHEN $3::uuid IS NULL THEN durability ELSE 'volatile' END WHERE id=$1 AND state='ready' AND upload_id IS NULL AND profile_revision=$2").bind(source).bind(revision).bind(boot)
}
pub(super) fn raw_upload_delete(source: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_uploads WHERE source_id=$1").bind(source)
}
pub(super) fn raw_optional_delete(source: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_processing WHERE source_id=$1").bind(source)
}
pub(super) fn raw_upload_streams(source: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT DISTINCT stream_id FROM raw_ranges WHERE source_id=$1 ORDER BY stream_id LIMIT 128",
    )
    .bind(source)
}
pub(super) fn raw_upload_stream_lock(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=$1 FOR UPDATE").bind(stream)
}
pub(super) fn raw_upload_share(stream: Id, source: Id, candidate: Id) -> Statement<'static> {
    sqlx::query("UPDATE raw_ranges SET source_id=$1 WHERE stream_id=$2 AND source_id=$3 AND NOT EXISTS(SELECT 1 FROM stream_leases WHERE stream_id=$2 AND lease_type='read' AND expires_at>clock_timestamp())")
        .bind(candidate)
        .bind(stream)
        .bind(source)
}
pub(super) fn raw_upload_references(source: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=$1)").bind(source)
}
pub(super) fn raw_uploaded(source: Id) -> Statement<'static> {
    sqlx::query("WITH released AS (UPDATE tasks SET execution_scope='cluster',target_node_id=NULL WHERE id=(SELECT processing_task FROM raw_sources WHERE id=$1)) UPDATE raw_sources SET pending_upload=false,medium='shared_s3',boot_id=NULL,durability='durable',upload_id=NULL,revision=revision+1 WHERE id=$1 AND pending_upload").bind(source)
}
pub(super) fn raw_upload_hash(source: Id, hash: Option<&[u8]>) -> Statement<'_> {
    sqlx::query("UPDATE raw_uploads SET content_hash=$1,token=NULL,lease_expires_at=NULL,next_attempt_at=clock_timestamp(),last_error=NULL WHERE source_id=$2").bind(hash).bind(source)
}
pub(super) fn raw_upload_retry<'a>(work: &'a RawUpload, error: Option<&'a str>) -> Statement<'a> {
    sqlx::query("UPDATE raw_uploads SET token=NULL,lease_expires_at=NULL,last_error=$1,attempts=attempts+$2,next_attempt_at=clock_timestamp()+interval '1 second' WHERE source_id=$3 AND token=$4").bind(error).bind(i32::from(error.is_some())).bind(work.source.id).bind(work.token)
}
pub(super) fn raw_upload_id(source: Id, token: Uuid, upload: Option<&str>) -> Statement<'_> {
    sqlx::query("UPDATE raw_sources SET upload_id=$1 WHERE id=$2 AND state='ready' AND pending_upload AND EXISTS(SELECT 1 FROM raw_uploads u WHERE u.source_id=raw_sources.id AND u.token=$3 AND (u.lease_expires_at IS NULL OR u.lease_expires_at>clock_timestamp()))").bind(upload).bind(source).bind(token)
}

pub(super) fn raw_upload_mode(cluster: bool) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT deployment_mode=$1 FROM mokyu_meta FOR SHARE").bind(if cluster {
        "cluster"
    } else {
        "single"
    })
}
pub(super) fn raw_upload_abandon(source: Id) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET state='deleting',revision=revision+1 WHERE id=$1 AND owner_stream IS NULL AND NOT EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.kind='raw' AND n.location_id=raw_sources.id AND l.expires_at>clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.task_type='integrity' AND t.state='running' AND t.detail->>'operation' IN ('recheck','verify') AND t.detail->>'source_location'=raw_sources.id::text AND ((t.lease_node_id IS NULL AND t.lease_expires_at IS NULL) OR t.lease_expires_at>clock_timestamp())) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM uploads WHERE raw_source_id=raw_sources.id)").bind(source)
}

pub(super) fn raw_upload_owned(work: &RawUpload, node: Option<Uuid>) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT token IS NOT NULL AND token=$2 AND node_id IS NOT DISTINCT FROM $3 AND (lease_expires_at IS NULL OR lease_expires_at>clock_timestamp()) FROM raw_uploads WHERE source_id=$1 FOR UPDATE").bind(work.source.id).bind(work.token).bind(node)
}

pub(super) fn renew_raw_upload(work: &RawUpload, node: Option<Uuid>) -> Statement<'static> {
    sqlx::query("UPDATE raw_uploads SET lease_expires_at=clock_timestamp()+interval '120 seconds' WHERE source_id=$1 AND token=$2 AND node_id=$3 AND lease_expires_at>clock_timestamp()").bind(work.source.id).bind(work.token).bind(node)
}

pub(super) fn release_raw_uploads(node: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE raw_uploads SET token=NULL,lease_expires_at=NULL,next_attempt_at=clock_timestamp() WHERE node_id=$1").bind(node)
}

pub(super) fn cache_nodes() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT node_id FROM pending_uploads WHERE node_id IS NOT NULL UNION SELECT node_id FROM raw_uploads WHERE node_id IS NOT NULL UNION SELECT remote_node_id FROM parts WHERE remote_node_id IS NOT NULL AND remote_etag IS NULL",
    )
}

pub(super) fn scope_upload_task(
    task: Uuid,
    node: Uuid,
    parent: Option<Uuid>,
) -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET execution_scope='node',target_node_id=$2,parent_task_id=$3 WHERE id=$1",
    )
    .bind(task)
    .bind(node)
    .bind(parent)
}

pub(super) fn completion_state(upload: Uuid) -> Rows<'static, (String, Option<String>, bool)> {
    sqlx::query_as("SELECT state,manifest_hash,COALESCE(completion_expires_at>clock_timestamp(),false) FROM uploads WHERE id=$1 FOR UPDATE").bind(upload)
}
pub(super) fn completion_parts(upload: Uuid) -> Rows<'static, (i32, Id)> {
    sqlx::query_as("SELECT part_number,stream_id FROM parts WHERE upload_id=$1 AND stream_id IS NOT NULL ORDER BY part_number").bind(upload)
}

pub(super) fn renew_completion(upload: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET completion_expires_at=clock_timestamp()+interval '120 seconds',touched_at=clock_timestamp() WHERE id=$1 AND completion_token=$2 AND state='completing' AND completion_expires_at>clock_timestamp()").bind(upload).bind(token)
}

pub(super) fn remote_upload(upload: Uuid) -> Rows<'static, RemoteUpload> {
    sqlx::query_as("SELECT u.id AS remote_upload,u.state AS remote_state,r.id AS remote_source,r.storage_id AS remote_storage,r.profile_revision AS remote_profile,r.upload_id AS remote_upstream,u.remote_manifest_hash AS remote_manifest FROM uploads u JOIN raw_sources r ON r.id=u.raw_source_id WHERE u.id=$1").bind(upload)
}
pub(super) fn remote_upload_lock(upload: Uuid) -> Rows<'static, (String, Option<Uuid>)> {
    sqlx::query_as("SELECT state,raw_source_id FROM uploads WHERE id=$1 FOR UPDATE").bind(upload)
}
pub(super) fn insert_remote(upload: Uuid, storage: Uuid, revision: i64) -> Statement<'static> {
    sqlx::query("WITH candidate AS (INSERT INTO raw_sources(storage_id,profile_revision,medium) SELECT b.storage_id,p.revision,'shared_s3' FROM uploads u JOIN buckets b ON b.id=u.bucket_id JOIN storages s ON s.id=b.storage_id JOIN storage_profiles p ON p.storage_id=s.id WHERE u.id=$1 AND b.storage_id=$2 AND p.revision=$3 AND s.type='s3' AND NOT p.cdc_enabled AND NOT p.compression_enabled AND s.block_key_id IS NULL AND NOT p.pack_enabled AND s.enabled AND NOT EXISTS(SELECT 1 FROM parts WHERE upload_id=u.id AND stream_id IS NOT NULL) RETURNING id) UPDATE uploads SET raw_source_id=(SELECT id FROM candidate) WHERE id=$1 AND EXISTS(SELECT 1 FROM candidate)").bind(upload).bind(storage).bind(revision)
}
pub(super) fn claim_remote(upload: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE uploads u SET remote_token=$2,remote_expires_at=clock_timestamp()+interval '120 seconds' FROM raw_sources r WHERE u.id=$1 AND u.state IN ('active','completing') AND r.id=u.raw_source_id AND r.upload_id IS NULL AND r.state='writing' AND (u.remote_expires_at IS NULL OR u.remote_expires_at<=clock_timestamp())").bind(upload).bind(token)
}
pub(super) fn remote_initialized(
    upload: Uuid,
    token: Uuid,
    upstream: Option<&str>,
) -> Scalar<'_, bool> {
    sqlx::query_scalar("WITH owned AS (UPDATE uploads SET remote_token=NULL,remote_expires_at=NULL WHERE id=$1 AND remote_token=$2 AND remote_expires_at>clock_timestamp() AND state IN ('active','completing') RETURNING raw_source_id), written AS (UPDATE raw_sources SET upload_id=$3 WHERE id=(SELECT raw_source_id FROM owned) AND upload_id IS NULL RETURNING id) SELECT EXISTS(SELECT 1 FROM written)").bind(upload).bind(token).bind(upstream)
}
pub(super) fn remote_part_queue(input: &RemoteCommit<'_>) -> Statement<'static> {
    sqlx::query("UPDATE parts SET remote_node_id=$4,remote_etag=NULL,remote_token=NULL,remote_expires_at=NULL,remote_retry_at=clock_timestamp(),remote_error=NULL,remote_cached=$5 WHERE upload_id=$1 AND part_number=$2 AND write_token=$3 AND EXISTS(SELECT 1 FROM uploads WHERE id=$1 AND raw_source_id IS NOT NULL AND remote_manifest_hash IS NULL)").bind(input.upload).bind(input.number).bind(input.stream).bind(input.node.map(|n| n.0.node_id)).bind(input.cached)
}
pub(super) fn remote_part_due(node: Uuid, stream: Option<Id>) -> Rows<'static, (Uuid, i32)> {
    sqlx::query_as("SELECT p.upload_id,p.part_number FROM parts p JOIN uploads u ON u.id=p.upload_id JOIN raw_sources r ON r.id=u.raw_source_id WHERE p.remote_node_id=$1 AND p.stream_id IS NOT NULL AND ($2::uuid IS NULL OR p.stream_id=$2) AND p.remote_etag IS NULL AND p.remote_retry_at<=clock_timestamp() AND (p.remote_expires_at IS NULL OR p.remote_expires_at<=clock_timestamp()) AND u.state IN ('active','completing') AND r.upload_id IS NOT NULL ORDER BY p.remote_retry_at,p.upload_id,p.part_number LIMIT 1 FOR UPDATE OF p SKIP LOCKED").bind(node).bind(stream)
}
pub(super) fn remote_part_claim(upload: Uuid, number: i32, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE parts SET remote_token=$3,remote_expires_at=clock_timestamp()+interval '120 seconds' WHERE upload_id=$1 AND part_number=$2").bind(upload).bind(number).bind(token)
}
pub(super) async fn remote_part(
    db: &mut sqlx::PgConnection,
    upload: Uuid,
    number: i32,
) -> Result<RemotePart> {
    Ok(sqlx::query_as("SELECT u.id AS remote_upload,u.state AS remote_state,r.id AS remote_source,r.storage_id AS remote_storage,r.profile_revision AS remote_profile,r.upload_id AS remote_upstream,u.remote_manifest_hash AS remote_manifest,s.*,p.part_number,p.remote_token FROM parts p JOIN uploads u ON u.id=p.upload_id JOIN raw_sources r ON r.id=u.raw_source_id JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=$1 AND p.part_number=$2").bind(upload).bind(number).fetch_one(db).await?)
}
pub(super) fn remote_part_finish<'a>(
    work: &'a RemotePart,
    etag: Option<&'a str>,
    node: Uuid,
) -> Statement<'a> {
    sqlx::query("UPDATE parts p SET remote_etag=COALESCE($6,remote_etag),remote_error=NULL,remote_token=CASE WHEN $6 IS NULL THEN p.remote_token END,remote_expires_at=CASE WHEN $6 IS NULL THEN clock_timestamp()+interval '120 seconds' END FROM uploads u WHERE p.upload_id=$1 AND p.part_number=$2 AND p.stream_id=$3 AND p.remote_token=$4 AND p.remote_node_id=$5 AND p.remote_expires_at>clock_timestamp() AND u.id=p.upload_id AND u.state IN ('active','completing')").bind(work.upload.upload).bind(work.part_number).bind(work.stream.id).bind(work.remote_token).bind(node).bind(etag)
}
pub(super) fn freeze_remote(upload: Uuid, token: Uuid, hash: &str) -> Statement<'_> {
    sqlx::query("UPDATE uploads SET remote_manifest_hash=$3 WHERE id=$1 AND state='completing' AND completion_token=$2 AND completion_expires_at>clock_timestamp() AND manifest_hash=$3 AND (remote_manifest_hash IS NULL OR remote_manifest_hash=$3) AND raw_source_id IS NOT NULL").bind(upload).bind(token).bind(hash)
}
pub(super) fn remote_output_lock(upload: Uuid, token: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT raw_source_id FROM uploads WHERE id=$1 AND state='completing' AND completion_token=$2 AND completion_expires_at>clock_timestamp() AND remote_manifest_hash=manifest_hash FOR UPDATE").bind(upload).bind(token)
}
pub(super) fn remote_source_finish<'a>(source: Uuid, input: &'a RemoteOutput<'a>) -> Statement<'a> {
    sqlx::query("UPDATE raw_sources SET state='ready',size=$2,digest=$3,upload_id=NULL WHERE id=$1 AND quarantine_code IS NULL AND (state='writing' OR (state='ready' AND size=$2 AND digest=$3 AND (content_hash IS NULL OR content_hash=$4)))").bind(source).bind(input.size).bind(input.digest).bind(input.hash)
}
pub(super) fn remote_cleanup_due() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM uploads u WHERE u.raw_source_id IS NOT NULL AND u.state IN ('aborted','completed') AND (u.remote_expires_at IS NULL OR u.remote_expires_at<=clock_timestamp()) AND (u.completion_expires_at IS NULL OR u.completion_expires_at<=clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM parts p WHERE p.upload_id=u.id AND p.remote_expires_at>clock_timestamp()) ORDER BY touched_at LIMIT 1 FOR UPDATE SKIP LOCKED",
    )
}
pub(super) fn remote_cleanup_claim(upload: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET remote_token=$2,remote_expires_at=clock_timestamp()+interval '120 seconds' WHERE id=$1").bind(upload).bind(token)
}

pub(super) fn remote_processing_hash(stream: Id, hash: &[u8]) -> Statement<'_> {
    sqlx::query("UPDATE raw_processing SET content_hash=$2 WHERE stream_id=$1")
        .bind(stream)
        .bind(hash)
}

pub(super) fn conversion_stream(stream: Id) -> Rows<'static, (Uuid, String, i64)> {
    sqlx::query_as("SELECT bucket_id,object_key,payload_revision FROM streams WHERE id=$1")
        .bind(stream)
}

pub(super) fn conversion_key(storage: Uuid, key: Option<Uuid>) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT block_key_id IS NOT DISTINCT FROM $1 FROM storages WHERE id=$2")
        .bind(key)
        .bind(storage)
}

pub(super) fn conversion_link(source: Id, task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET processing_task=$1 WHERE id=$2")
        .bind(task)
        .bind(source)
}

pub(super) fn conversion_owned(candidate: Id, task: Uuid, token: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=$1 AND state='writing' AND owner_task_id=$2 AND owner_lease_token=$3 FOR UPDATE").bind(candidate).bind(task).bind(token)
}

pub(super) fn conversion_chunk(chunk: Id, candidate: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT id FROM chunks WHERE id=$1 AND state='preparing' AND owner_stream=$2 FOR UPDATE",
    )
    .bind(chunk)
    .bind(candidate)
}

pub(super) fn conversion_ready(chunk: Id, candidate: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET state='ready',owner_stream=NULL,unreferenced_at=NULL WHERE id=$1 AND owner_stream=$2 AND state='preparing'").bind(chunk).bind(candidate)
}

pub(super) fn conversion_retire_encoding(encoding: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=clock_timestamp() WHERE encoding_id=$1 AND state IN ('preparing','uploading')").bind(encoding)
}

pub(super) fn conversion_output_pack(pack: Id, task: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM packs WHERE id=$1 AND owner_task=$2 AND state='preparing' AND stored_size IS NOT NULL AND digest IS NOT NULL FOR UPDATE").bind(pack).bind(task)
}

pub(super) fn conversion_output_encoding(
    encoding: Id,
    task: Uuid,
    chunk: Id,
) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT encoding_id FROM chunk_encodings WHERE encoding_id=$1 AND owner_task=$2 AND chunk_id=$3 AND state='uploading' AND standalone_size IS NOT NULL FOR UPDATE").bind(encoding).bind(task).bind(chunk)
}

pub(super) fn conversion_publish_encoding(encoding: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET sync_pending=CASE WHEN boot_id IS NOT NULL THEN FALSE ELSE sync_pending END,boot_id=NULL,state='ready',owner_task=NULL,unreferenced_at=NULL,stored_at=clock_timestamp() WHERE encoding_id=$1 AND state='uploading'").bind(encoding)
}

pub(super) fn conversion_layout(chunk: Id, pack: Option<Id>) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET pack_id=$1 WHERE id=$2")
        .bind(pack)
        .bind(chunk)
}

pub(super) fn conversion_current(stream: Id, revision: i64) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=$1 AND payload_revision=$2 AND payload_type='raw' AND state='ready' AND (EXISTS(SELECT 1 FROM objects WHERE stream_id=streams.id) OR EXISTS(SELECT 1 FROM parts p JOIN uploads u ON u.id=p.upload_id WHERE p.stream_id=streams.id AND u.state IN ('active','completing'))) FOR UPDATE").bind(stream).bind(revision)
}

pub(super) fn conversion_move(candidate: Id, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE extents SET stream_id=$1 WHERE stream_id=$2")
        .bind(stream)
        .bind(candidate)
}

pub(super) fn conversion_detach(stream: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_ranges WHERE stream_id=$1").bind(stream)
}

pub(super) fn conversion_changed(stream: Id) -> Statement<'static> {
    sqlx::query(
        "UPDATE streams SET payload_type='chunks',payload_revision=payload_revision+1 WHERE id=$1",
    )
    .bind(stream)
}

pub(super) fn conversion_abandon(candidate: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='abandoned',size=0,owner_task_id=NULL,owner_lease_token=NULL,touched_at=clock_timestamp() WHERE id=$1").bind(candidate)
}

pub(super) fn conversion_unpin(candidate: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM cache_pins WHERE owner_id=$1").bind(candidate)
}

pub(super) fn conversion_no_pending(candidate: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM extents e JOIN pending_uploads p ON p.chunk_id=e.chunk_id WHERE e.stream_id=$1)").bind(candidate)
}

pub(super) fn conversion_cancel(source: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM tasks WHERE id=(SELECT processing_task FROM raw_sources WHERE id=$1) AND state<>'running'").bind(source)
}

pub(super) fn conversion_inline(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET payload_type='chunks' WHERE id=$1 AND state='writing' AND payload_type='raw'").bind(stream)
}

pub(super) fn conversion_job(
    source: Id,
) -> Scalar<'static, sqlx::types::Json<crate::model::raw::Conversion>> {
    sqlx::query_scalar("SELECT detail FROM tasks WHERE id=(SELECT processing_task FROM raw_sources WHERE id=$1) FOR UPDATE").bind(source)
}

pub(super) fn buffer_chunk(chunk: &Chunk, boot: Option<Uuid>) -> Statement<'_> {
    sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,storage_id,standalone_size,standalone_compressed,nonce,nonce_id,state,created_at,unreferenced_at,encryption_algorithm,block_key_id,sync_pending,boot_id) VALUES($1,$2,$3,$4,$5,$6,$10,'uploading',$7,now(),$8,$9,TRUE,$11) ON CONFLICT(encoding_id) DO UPDATE SET sync_pending=TRUE,boot_id=EXCLUDED.boot_id WHERE chunk_encodings.state='uploading'").bind(chunk.encoding_id).bind(chunk.id).bind(chunk.storage_id).bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(chunk.created_at).bind(&chunk.algorithm).bind(chunk.block_key_id).bind(crate::content::nonce::id(chunk.nonce.as_deref())).bind(boot)
}

pub(super) fn release_memory_pins(boot: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM cache_pins WHERE chunk_id IN (SELECT c.id FROM chunks c JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id WHERE l.boot_id<>$1 AND l.sync_pending AND l.state IN ('ready','uploading') AND l.quarantined_at IS NULL)").bind(boot)
}

pub(super) fn release_memory_chunks(boot: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM pending_uploads WHERE chunk_id IN (SELECT c.id FROM chunks c JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id WHERE l.boot_id<>$1 AND l.sync_pending AND l.state IN ('ready','uploading') AND l.quarantined_at IS NULL)").bind(boot)
}

pub(super) fn retire_memory_chunks(boot: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',sync_pending=FALSE,boot_id=NULL,unreferenced_at=clock_timestamp() WHERE boot_id<>$1 AND sync_pending AND state IN ('ready','uploading') AND quarantined_at IS NULL AND encoding_id IN (SELECT c.current_encoding_id FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE p.state='ready' AND p.quarantined_at IS NULL)").bind(boot)
}

pub(super) fn quarantine_memory_chunks(boot: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET quarantine_code='remote_missing',quarantined_at=clock_timestamp(),quarantine_revision=quarantine_revision+1 WHERE boot_id<>$1 AND sync_pending AND state IN ('ready','uploading') AND quarantined_at IS NULL AND encoding_id IN (SELECT current_encoding_id FROM chunks)").bind(boot)
}

pub(super) fn release_memory_raw(boot: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_uploads WHERE source_id IN (SELECT id FROM raw_sources WHERE durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL AND medium='memory' AND boot_id<>$1)").bind(boot)
}

pub(super) fn release_memory_processing(boot: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_processing WHERE source_id IN (SELECT id FROM raw_sources WHERE durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL AND medium='memory' AND boot_id<>$1)").bind(boot)
}

pub(super) fn quarantine_memory_raw(boot: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET pending_upload=FALSE,quarantine_code='remote_missing',quarantined_at=clock_timestamp(),revision=revision+1 WHERE durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL AND medium='memory' AND boot_id<>$1 AND quarantine_code IS NULL").bind(boot)
}

#[derive(sqlx::FromRow)]
pub(super) struct RegisteredNode {
    pub role: String,
    pub enabled: bool,
    pub credential_encrypted: Option<Vec<u8>>,
    pub credential_generation: i64,
    pub deployment_id: Uuid,
    pub deployment_mode: String,
}

pub(super) fn registered_node(id: Uuid, exclusive: bool) -> Rows<'static, RegisteredNode> {
    let sql = if exclusive {
        "SELECT n.role,n.enabled,n.credential_encrypted,n.credential_generation,m.deployment_id,m.deployment_mode FROM nodes n CROSS JOIN mokyu_meta m WHERE n.id=$1 AND m.singleton FOR UPDATE OF n"
    } else {
        "SELECT n.role,n.enabled,n.credential_encrypted,n.credential_generation,m.deployment_id,m.deployment_mode FROM nodes n CROSS JOIN mokyu_meta m WHERE n.id=$1 AND m.singleton FOR SHARE OF n"
    };
    sqlx::query_as(sql).bind(id)
}

pub(super) fn node_registration_lock(name: &str) -> Statement<'_> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,724))").bind(name)
}

pub(super) fn node_registration(name: &str) -> Rows<'_, (Uuid, String, bool, i64)> {
    sqlx::query_as(
        "SELECT id,role,enabled,credential_generation FROM nodes WHERE name=$1 FOR UPDATE",
    )
    .bind(name)
}

pub(super) fn node_deployment() -> Rows<'static, (Uuid, String)> {
    sqlx::query_as("SELECT deployment_id,deployment_mode FROM mokyu_meta WHERE singleton")
}

pub(super) fn new_node_id() -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT uuidv7()")
}

pub(super) fn save_node_identity<'q>(
    state: &'q crate::cluster::NodeState,
    name: &'q str,
    encrypted: &'q [u8],
) -> Statement<'q> {
    sqlx::query("INSERT INTO nodes(id,name,role,credential_encrypted,credential_generation) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET credential_encrypted=EXCLUDED.credential_encrypted,credential_generation=EXCLUDED.credential_generation,enabled=true,status='registered',recovery_required=false,last_seen_at=NULL,updated_at=now()")
        .bind(state.node_id).bind(name).bind(&state.role).bind(encrypted).bind(state.credential_generation)
}

pub(super) fn node_registration_conflict(node: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE nodes SET status='recovery_required',recovery_required=true,updated_at=now() WHERE id=$1").bind(node)
}

pub(super) fn clear_node_session(node: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM node_sessions WHERE node_id=$1").bind(node)
}

pub(super) fn clear_node_boots(node: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM node_boots WHERE node_id=$1").bind(node)
}

pub(super) fn node_session(
    node: Uuid,
    exclusive: bool,
) -> Rows<'static, crate::model::session::Session> {
    let sql = if exclusive {
        "SELECT credential_generation,boot_id,fence,epoch,connection_id,expires_at,sequence,draining FROM node_sessions WHERE node_id=$1 FOR UPDATE"
    } else {
        "SELECT credential_generation,boot_id,fence,epoch,connection_id,expires_at,sequence,draining FROM node_sessions WHERE node_id=$1 FOR SHARE"
    };
    sqlx::query_as(sql).bind(node)
}

pub(super) fn node_clock() -> Scalar<'static, DateTime<Utc>> {
    sqlx::query_scalar("SELECT clock_timestamp()")
}

pub(super) fn node_read_admission() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT NOT maintenance AND deployment_mode='cluster' FROM mokyu_meta WHERE singleton FOR SHARE",
    )
}

pub(super) fn node_read_credential(access: &str) -> Rows<'_, (i64, Vec<u8>)> {
    sqlx::query_as("SELECT authorization_revision,secret_encrypted FROM s3_credentials WHERE access_key_id=$1 AND enabled AND (expires_at IS NULL OR expires_at>clock_timestamp()) FOR SHARE").bind(access)
}

pub(super) fn node_read_bucket(route: &crate::model::ReadRoute) -> Rows<'_, Bucket> {
    match route {
        crate::model::ReadRoute::Bucket { name, alias, .. } => sqlx::query_as("SELECT * FROM buckets WHERE id=(SELECT id FROM buckets WHERE name IN ($1,$2) ORDER BY CASE WHEN name=$2 THEN 0 ELSE 1 END LIMIT 1) FOR SHARE")
            .bind(name).bind(alias.as_ref().map(|(name, _)| name)),
        crate::model::ReadRoute::Host { host, .. } => sqlx::query_as("SELECT b.* FROM buckets b JOIN bucket_domains d ON d.bucket_id=b.id WHERE d.host=$1 FOR SHARE OF b,d").bind(host),
    }
}

pub(super) fn node_read_allowed(access: &str, project: Uuid, bucket: Uuid) -> Scalar<'_, bool> {
    node_request_allowed(access, project, bucket, "object.read")
}

pub(super) fn node_request_allowed<'q>(
    access: &'q str,
    project: Uuid,
    bucket: Uuid,
    action: &'q str,
) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM s3_credential_grants g JOIN s3_credentials c ON c.access_key_id=g.access_key WHERE g.access_key=$1 AND c.project_id=$2 AND g.bucket_id=$3 AND $4=ANY(g.actions))")
        .bind(access).bind(project).bind(bucket).bind(action)
}

pub(super) fn credential_buckets<'q>(
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

pub(super) fn node_catalog_objects<'q>(
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

pub(super) fn node_object_acl(bucket: Uuid, key: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT s.public_read FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=$1 AND o.object_key=$2 AND s.state='ready'").bind(bucket).bind(key)
}

pub(super) fn node_catalog_buckets<'q>(
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

pub(super) fn node_read_extents(
    stream: Id,
    start: i64,
    end: i64,
    limit: i64,
) -> Rows<'static, Extent> {
    sqlx::query_as("SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=$2-4194304 AND offset_bytes+length>$2 AND offset_bytes<$3 ORDER BY offset_bytes LIMIT $4 FOR SHARE")
        .bind(stream).bind(start).bind(end).bind(limit)
}

#[derive(sqlx::FromRow)]
pub(super) struct NodeReadChunk {
    #[sqlx(flatten)]
    pub chunk: Chunk,
    pub pack_id: Option<Id>,
}

pub(super) fn node_read_chunks(ids: &[Id]) -> Rows<'_, NodeReadChunk> {
    sqlx::query_as("SELECT *, '00000000-0000-0000-0000-000000000000'::uuid AS location_id FROM chunks WHERE id=ANY($1) AND state='ready' ORDER BY id FOR SHARE")
        .bind(ids)
}

pub(super) fn node_read_encodings(ids: &[Id]) -> Rows<'_, Chunk> {
    sqlx::query_as("SELECT c.id,l.encoding_id AS current_encoding_id,l.encoding_id AS location_id,l.storage_id,c.hash,c.raw_size,l.standalone_size,l.encryption_algorithm,l.block_key_id,l.standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunks c JOIN LATERAL (SELECT * FROM chunk_encodings e WHERE e.chunk_id=c.id AND e.state='ready' AND e.quarantined_at IS NULL ORDER BY e.encoding_id LIMIT 1 FOR SHARE) l ON true WHERE c.id=ANY($1) ORDER BY c.id")
        .bind(ids)
}

pub(super) fn node_read_packs(ids: &[Id]) -> Rows<'_, crate::content::pack::Pack> {
    sqlx::query_as("SELECT * FROM packs WHERE id=ANY($1) AND state='ready' AND quarantined_at IS NULL ORDER BY id FOR SHARE").bind(ids)
}

pub(super) fn node_read_pending(ids: &[Id]) -> Rows<'_, (Id, Option<Uuid>)> {
    sqlx::query_as("SELECT chunk_id,node_id FROM pending_uploads WHERE chunk_id=ANY($1) AND staging_storage_id IS NULL AND source_pack_id IS NULL ORDER BY chunk_id FOR SHARE").bind(ids)
}

#[derive(sqlx::FromRow)]
pub(super) struct NodeReadFragment {
    #[sqlx(flatten)]
    pub source: FragmentSource,
    pub node_id: Option<Uuid>,
}

pub(super) fn node_read_fragments(ids: &[Id]) -> Rows<'_, NodeReadFragment> {
    sqlx::query_as("SELECT f.id AS fragment_location,f.size,f.hash,f.staging_storage_id,f.staging_object_key,f.file_id,f.file_offset,m.storage_id AS file_storage,CASE WHEN m.source_id IS NOT NULL THEN m.id END AS raw_source,m.node_id FROM fragments f LEFT JOIN multipart_files m ON m.id=f.file_id WHERE f.id=ANY($1) ORDER BY f.id FOR SHARE OF f")
        .bind(ids)
}

#[derive(sqlx::FromRow)]
pub(super) struct NodeReadRaw {
    #[sqlx(flatten)]
    pub range: crate::model::raw::RawRange,
    pub node_id: Option<Uuid>,
}

pub(super) fn node_read_raw(
    stream: Id,
    start: i64,
    end: i64,
    limit: i64,
) -> Rows<'static, NodeReadRaw> {
    sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,e.offset_bytes,e.length,e.source_offset,u.node_id FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id LEFT JOIN raw_uploads u ON u.source_id=r.id WHERE e.stream_id=$1 AND e.offset_bytes>=COALESCE((SELECT offset_bytes FROM raw_ranges WHERE stream_id=$1 AND offset_bytes<=$2 ORDER BY offset_bytes DESC LIMIT 1),$2) AND e.offset_bytes+e.length>$2 AND e.offset_bytes<$3 ORDER BY e.offset_bytes LIMIT $4 FOR SHARE OF e,r")
        .bind(stream).bind(start).bind(end).bind(limit)
}

pub(super) fn node_read_storages(ids: &[Uuid]) -> Rows<'_, crate::storage::model::Record> {
    sqlx::query_as("SELECT s.* FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=ANY($1) AND s.enabled AND s.type='s3' ORDER BY s.id FOR SHARE OF s,p").bind(ids)
}

pub(super) fn node_read_profiles(
    ids: &[Uuid],
) -> Rows<'_, crate::storage::profile::StorageProfile> {
    sqlx::query_as(
        "SELECT * FROM storage_profiles WHERE storage_id=ANY($1) ORDER BY storage_id FOR SHARE",
    )
    .bind(ids)
}

pub(super) fn node_read_keys(ids: &[Uuid]) -> Rows<'_, ManagedKeyMaterial> {
    sqlx::query_as("SELECT id,name,purpose,key_algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE id=ANY($1) AND purpose='block' AND secret_encrypted IS NOT NULL ORDER BY id FOR SHARE").bind(ids)
}

#[derive(sqlx::FromRow)]
pub(super) struct NodeReadLease {
    pub read_id: Uuid,
    pub stream_id: Id,
    pub payload_revision: i64,
    pub offset_bytes: i64,
    pub end_bytes: i64,
    pub request_hash: Vec<u8>,
    pub replica_allowed: bool,
    pub valid_for_ms: i64,
}

pub(super) fn node_read_existing<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    operation: Uuid,
) -> Rows<'q, NodeReadLease> {
    sqlx::query_as("SELECT r.*,l.stream_id,GREATEST(0,(extract(epoch FROM (l.expires_at-clock_timestamp()))*1000)::bigint) AS valid_for_ms FROM node_reads r JOIN stream_leases l ON l.id=r.read_id WHERE r.node_id=$1 AND r.credential_generation=$2 AND r.boot_id=$3 AND r.fence=$4 AND r.operation_id=$5 FOR SHARE OF r,l")
        .bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(operation)
}

pub(super) fn node_read_owned(
    state: &crate::cluster::NodeState,
    run: crate::model::session::Run,
    id: Uuid,
) -> Rows<'_, NodeReadLease> {
    sqlx::query_as("SELECT r.*,l.stream_id,GREATEST(0,(extract(epoch FROM (l.expires_at-clock_timestamp()))*1000)::bigint) AS valid_for_ms FROM node_reads r JOIN stream_leases l ON l.id=r.read_id WHERE r.node_id=$1 AND r.credential_generation=$2 AND r.boot_id=$3 AND r.fence=$4 AND r.read_id=$5 AND l.expires_at>clock_timestamp() FOR SHARE OF r,l")
        .bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(id)
}

pub(super) fn node_read_count(node: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_reads r JOIN stream_leases l ON l.id=r.read_id WHERE r.node_id=$1 AND l.expires_at>clock_timestamp()").bind(node)
}

pub(super) fn node_read_insert<'q>(
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

pub(super) fn node_read_stream(id: Id) -> Rows<'static, crate::app::StoredStream> {
    sqlx::query_as("SELECT * FROM streams WHERE id=$1 AND state IN ('ready','retired') FOR SHARE")
        .bind(id)
}

pub(super) fn node_read_window(
    read: Uuid,
    start: i64,
) -> Rows<'static, (Uuid, sqlx::types::Json<crate::model::node_read::Mapping>)> {
    sqlx::query_as(
        "SELECT id,mapping FROM node_read_windows WHERE read_id=$1 AND offset_bytes=$2 FOR SHARE",
    )
    .bind(read)
    .bind(start)
}

pub(super) fn node_read_window_count(read: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_read_windows WHERE read_id=$1").bind(read)
}

pub(super) fn node_read_report_window(
    read: Uuid,
    window: Uuid,
) -> Scalar<'static, sqlx::types::Json<crate::model::node_read::Mapping>> {
    sqlx::query_scalar("SELECT mapping FROM node_read_windows WHERE read_id=$1 AND id=$2 FOR SHARE")
        .bind(read)
        .bind(window)
}

pub(super) fn node_read_reported(task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=$1)").bind(task)
}

pub(super) fn node_read_verifications() -> Scalar<'static, i64> {
    sqlx::query_scalar(
        "SELECT count(*) FROM tasks WHERE task_type='integrity' AND detail->>'operation'='verify' AND state IN ('queued','running','paused')",
    )
}

pub(super) fn node_read_insert_window(
    read: Uuid,
    mapping: &crate::model::node_read::Mapping,
) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("INSERT INTO node_read_windows(read_id,offset_bytes,end_bytes,mapping) VALUES($1,$2,$3,$4) RETURNING id")
        .bind(read).bind(mapping.start).bind(mapping.end).bind(sqlx::types::Json(mapping))
}

pub(super) fn node_read_pin<'q>(
    window: Uuid,
    kinds: &'q [String],
    ids: &'q [Uuid],
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_read_sources(window_id,kind,location_id) SELECT $1,unnest($2::text[]),unnest($3::uuid[]) ON CONFLICT DO NOTHING")
        .bind(window).bind(kinds).bind(ids)
}

pub(super) fn node_read_permit(read: Uuid, storage: Uuid) -> Rows<'static, (Uuid, i64)> {
    sqlx::query_as("SELECT p.token,GREATEST(0,(extract(epoch FROM (p.expires_at-clock_timestamp()))*1000)::bigint) FROM node_read_permits n JOIN storage_permits p ON p.token=n.token WHERE n.read_id=$1 AND p.storage_id=$2 AND p.direction='read' AND p.expires_at>clock_timestamp() FOR SHARE OF p")
        .bind(read).bind(storage)
}

pub(super) fn node_read_own_permit(read: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO node_read_permits(token,read_id) VALUES($1,$2) ON CONFLICT DO NOTHING")
        .bind(token)
        .bind(read)
}

pub(super) fn node_read_shared_permits<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    tokens: &'q [Uuid],
) -> Rows<'q, (Uuid, Uuid, i64)> {
    sqlx::query_as("SELECT p.token,p.storage_id,GREATEST(0,(extract(epoch FROM(p.expires_at-clock_timestamp()))*1000)::bigint) FROM storage_permits p WHERE p.direction='read' AND p.token=ANY($5) AND p.expires_at>clock_timestamp() AND EXISTS(SELECT 1 FROM node_read_permits n JOIN node_reads r ON r.read_id=n.read_id JOIN stream_leases l ON l.id=r.read_id WHERE n.token=p.token AND r.node_id=$1 AND r.credential_generation=$2 AND r.boot_id=$3 AND r.fence=$4 AND l.expires_at>clock_timestamp()) FOR SHARE OF p")
        .bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(tokens)
}

pub(super) fn node_read_permit_count(read: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_read_permits n JOIN storage_permits p ON p.token=n.token WHERE n.read_id=$1 AND p.expires_at>clock_timestamp()").bind(read)
}

pub(super) fn node_read_renew<'q>(
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

pub(super) fn node_read_release_permits<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    reads: &'q [Uuid],
    permits: &'q [Uuid],
) -> Statement<'q> {
    sqlx::query("DELETE FROM storage_permits p USING node_read_permits n,node_reads r WHERE p.token=n.token AND r.read_id=n.read_id AND r.node_id=$1 AND r.credential_generation=$2 AND r.boot_id=$3 AND r.fence=$4 AND (p.token=ANY($6) OR (r.read_id=ANY($5) AND NOT EXISTS(SELECT 1 FROM node_read_permits other WHERE other.token=p.token AND NOT(other.read_id=ANY($5)))))")
        .bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(reads).bind(permits)
}

pub(super) fn node_read_release_windows<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    ids: &'q [Uuid],
) -> Statement<'q> {
    sqlx::query("DELETE FROM node_read_windows w USING node_reads r WHERE w.read_id=r.read_id AND r.node_id=$1 AND r.credential_generation=$2 AND r.boot_id=$3 AND r.fence=$4 AND w.id=ANY($5)")
        .bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(ids)
}

pub(super) fn node_read_release<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    ids: &'q [Uuid],
) -> Statement<'q> {
    sqlx::query("DELETE FROM stream_leases l USING node_reads r WHERE l.id=r.read_id AND r.node_id=$1 AND r.credential_generation=$2 AND r.boot_id=$3 AND r.fence=$4 AND l.id=ANY($5)")
        .bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(ids)
}

pub(super) fn node_boot_retired(node: Uuid, boot: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM node_boots WHERE node_id=$1 AND boot_id=$2 AND retired_at IS NOT NULL)").bind(node).bind(boot)
}

pub(super) fn close_node_session(node: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE node_sessions SET expires_at=clock_timestamp(),draining=true WHERE node_id=$1",
    )
    .bind(node)
}

pub(super) fn node_boot_known(node: Uuid, boot: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM node_boots WHERE node_id=$1 AND boot_id=$2)")
        .bind(node)
        .bind(boot)
}

pub(super) fn node_boot_count(node: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_boots WHERE node_id=$1").bind(node)
}

pub(super) fn retire_node_boots(node: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE node_boots SET retired_at=clock_timestamp() WHERE node_id=$1 AND retired_at IS NULL").bind(node)
}

pub(super) fn insert_node_boot(node: Uuid, generation: i64, boot: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO node_boots(node_id,credential_generation,boot_id) VALUES($1,$2,$3)")
        .bind(node)
        .bind(generation)
        .bind(boot)
}

pub(super) fn save_node_session(
    node: Uuid,
    generation: i64,
    run: crate::model::session::Run,
    epoch: i64,
    connection: Uuid,
    draining: bool,
) -> Statement<'static> {
    sqlx::query("INSERT INTO node_sessions(node_id,credential_generation,boot_id,fence,epoch,connection_id,expires_at,draining) VALUES($1,$2,$3,$4,$5,$6,clock_timestamp()+$7*interval '1 second',$8) ON CONFLICT(node_id) DO UPDATE SET credential_generation=EXCLUDED.credential_generation,boot_id=EXCLUDED.boot_id,fence=EXCLUDED.fence,epoch=EXCLUDED.epoch,connection_id=EXCLUDED.connection_id,expires_at=EXCLUDED.expires_at,sequence=0,draining=EXCLUDED.draining")
        .bind(node).bind(generation).bind(run.boot).bind(run.fence).bind(epoch).bind(connection).bind(crate::model::session::LEASE_SECONDS).bind(draining)
}

pub(super) fn pulse_node_session(node: Uuid, sequence: i64, draining: bool) -> Statement<'static> {
    sqlx::query("UPDATE node_sessions SET sequence=$2,draining=draining OR $3,expires_at=clock_timestamp()+$4*interval '1 second' WHERE node_id=$1")
        .bind(node).bind(sequence).bind(draining).bind(crate::model::session::LEASE_SECONDS)
}

pub(super) fn maintain_node_session(node: Uuid, sequence: i64) -> Scalar<'static, i64> {
    sqlx::query_scalar("UPDATE node_sessions SET sequence=$2 WHERE node_id=$1 RETURNING greatest(0,floor(extract(epoch FROM (expires_at-clock_timestamp()))*1000))::bigint").bind(node).bind(sequence)
}

pub(super) fn node_session_status(node: Uuid) -> Rows<'static, (Uuid, Uuid, i64, i64, bool, bool)> {
    sqlx::query_as("SELECT s.boot_id,s.fence,s.epoch,GREATEST(0,(extract(epoch FROM (s.expires_at-clock_timestamp()))*1000)::bigint),m.maintenance,s.draining FROM node_sessions s CROSS JOIN mokyu_meta m WHERE s.node_id=$1 AND m.singleton AND m.deployment_mode='cluster'").bind(node)
}

pub(super) fn node_settings() -> Rows<'static, (String, Value, i64)> {
    sqlx::query_as("SELECT section,value,revision FROM runtime_settings WHERE section=ANY($1) ORDER BY section").bind(crate::model::session::SECTIONS)
}

#[derive(Clone, sqlx::FromRow)]
pub(super) struct NodeWrite {
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

pub(super) fn node_write(node: Uuid, operation: Uuid) -> Rows<'static, NodeWrite> {
    sqlx::query_as("SELECT w.*,s.bucket_id AS bucket_id,s.owner_task_id,s.owner_lease_token,CASE WHEN w.state='active' AND s.state='writing' AND ((w.upload_id IS NULL AND (w.completion_upload IS NULL OR EXISTS(SELECT 1 FROM uploads c WHERE c.id=w.completion_upload AND c.state='completing' AND c.completion_token=w.operation_id AND c.completion_expires_at>clock_timestamp())) AND (EXISTS(SELECT 1 FROM objects o WHERE o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.write_token=s.id) OR EXISTS(SELECT 1 FROM tasks t WHERE t.id=s.owner_task_id AND t.lease_token=s.owner_lease_token AND t.lease_node_id=w.node_id AND t.state='running' AND t.lease_expires_at>clock_timestamp() AND (t.task_type='storage_migration' OR t.task_type='upload' AND t.detail->>'operation'='cdc')))) OR (w.upload_id IS NOT NULL AND EXISTS(SELECT 1 FROM parts a JOIN uploads u ON u.id=a.upload_id WHERE a.upload_id=w.upload_id AND a.part_number=w.part_number AND a.write_token=s.id AND u.state='active' AND u.remote_manifest_hash IS NULL))) THEN GREATEST(0,(extract(epoch FROM (l.expires_at-clock_timestamp()))*1000)::bigint) ELSE 0 END AS valid_for_ms FROM node_writes w LEFT JOIN stream_leases l ON l.id=w.lease_id LEFT JOIN streams s ON s.id=w.stream_id WHERE w.node_id=$1 AND w.operation_id=$2 FOR UPDATE OF w")
        .bind(node).bind(operation)
}

pub(super) fn node_write_count(node: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_writes w JOIN stream_leases l ON l.id=w.lease_id WHERE w.node_id=$1 AND w.state='active' AND l.expires_at>clock_timestamp()").bind(node)
}

#[derive(sqlx::FromRow)]
pub(super) struct NodeTail {
    pub request_hash: Vec<u8>,
    pub fragment_id: Uuid,
    pub storage_id: Uuid,
    pub object_key: String,
    pub offset_bytes: i64,
    pub size: i32,
    pub committed: bool,
}

impl NodeTail {
    pub(super) fn reply(&self) -> crate::model::node_tail::Prepared {
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

pub(super) fn node_write_tail(node: Uuid, operation: Uuid) -> Rows<'static, NodeTail> {
    sqlx::query_as("SELECT * FROM node_write_tails WHERE node_id=$1 AND operation_id=$2")
        .bind(node)
        .bind(operation)
}

pub(super) fn node_staging_settings()
-> Scalar<'static, sqlx::types::Json<crate::config::UploadSettings>> {
    sqlx::query_scalar("SELECT value FROM runtime_settings WHERE section='uploads' FOR SHARE")
}

pub(super) fn insert_node_tail<'q>(
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

pub(super) fn commit_node_tail(node: Uuid, operation: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE node_write_tails SET committed=true WHERE node_id=$1 AND operation_id=$2 AND NOT committed")
        .bind(node).bind(operation)
}

pub(super) fn finish_node_tail(node: Uuid, operation: Uuid, end: i64) -> Statement<'static> {
    sqlx::query("UPDATE node_writes SET next_offset=$3 WHERE node_id=$1 AND operation_id=$2")
        .bind(node)
        .bind(operation)
        .bind(end)
}

pub(super) fn node_object_change(
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

pub(super) fn save_node_object_change<'q>(
    node: &crate::cluster::NodeState,
    operation: Uuid,
    hash: &'q [u8],
    outcome: crate::model::node_object::Outcome,
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_object_changes(node_id,operation_id,credential_generation,request_hash,outcome) VALUES($1,$2,$3,$4,$5)")
        .bind(node.node_id).bind(operation).bind(node.credential_generation).bind(hash).bind(sqlx::types::Json(outcome))
}

pub(super) fn expire_node_object_changes(node: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM node_object_changes WHERE node_id=$1 AND operation_id IN (SELECT operation_id FROM node_object_changes WHERE node_id=$1 AND created_at<clock_timestamp()-interval '1 day' ORDER BY created_at LIMIT 64)").bind(node)
}

pub(super) fn node_write_credential(access: &str) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT authorization_revision FROM s3_credentials WHERE access_key_id=$1 AND enabled AND (expires_at IS NULL OR expires_at>clock_timestamp()) FOR SHARE").bind(access)
}

pub(super) fn node_write_storage(id: Uuid) -> Rows<'static, (String, Option<Uuid>)> {
    sqlx::query_as("SELECT type,block_key_id FROM storages WHERE id=$1 AND enabled FOR SHARE")
        .bind(id)
}

pub(super) fn node_write_limit(id: Uuid) -> Scalar<'static, Option<i32>> {
    sqlx::query_scalar(
        "SELECT upload_concurrency FROM storage_profiles WHERE storage_id=$1 FOR SHARE",
    )
    .bind(id)
}

pub(super) fn node_write_cleanup(node: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM node_writes WHERE node_id=$1 AND operation_id IN (SELECT w.operation_id FROM node_writes w LEFT JOIN stream_leases l ON l.id=w.lease_id WHERE w.node_id=$1 AND COALESCE(w.completed_at,w.created_at)<clock_timestamp()-interval '1 day' AND (l.id IS NULL OR l.expires_at<=clock_timestamp()) ORDER BY w.created_at LIMIT 64)").bind(node)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn insert_node_write<'q>(
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

pub(super) fn node_copy_source(
    source: &crate::model::node_write::CopySource,
) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT l.id FROM stream_leases l JOIN streams s ON s.id=l.stream_id WHERE l.id=$1 AND l.stream_id=$2 AND l.lease_type='read' AND l.expires_at>clock_timestamp() AND s.payload_revision=$3 AND s.state IN ('ready','retired') FOR SHARE OF l,s")
        .bind(source.read).bind(source.stream).bind(source.revision)
}

pub(super) fn save_node_copy_batch<'q>(
    node: Uuid,
    input: &crate::model::node_write::CopyBatch,
    hash: &'q [u8],
    end: i64,
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_write_batches(node_id,operation_id,batch_id,request_hash,end_bytes,plan,ready,commit_hash) VALUES($1,$2,$3,$4,$5,'[]'::jsonb,true,$4)")
        .bind(node).bind(input.operation).bind(input.batch).bind(hash).bind(end)
}

pub(super) fn abort_node_writes<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    operations: &'q [Uuid],
    leases: &'q [Uuid],
) -> Rows<'q, (Option<Uuid>, Option<Uuid>)> {
    sqlx::query_as("UPDATE node_writes SET state='aborted',completed_at=clock_timestamp() WHERE node_id=$1 AND credential_generation=$2 AND boot_id=$3 AND fence=$4 AND state='active' AND (operation_id=ANY($5) OR lease_id=ANY($6)) RETURNING stream_id,lease_id")
        .bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(operations).bind(leases)
}

pub(super) fn abandon_node_streams(ids: &[Uuid]) -> Statement<'_> {
    sqlx::query("UPDATE streams SET state='abandoned',touched_at=clock_timestamp() WHERE id=ANY($1) AND state='writing'").bind(ids)
}

pub(super) fn release_node_write_leases(ids: &[Uuid]) -> Statement<'_> {
    sqlx::query("DELETE FROM stream_leases WHERE id=ANY($1) AND lease_type='write'").bind(ids)
}

#[derive(sqlx::FromRow)]
pub(super) struct NodeWriteBatch {
    pub packing: Option<sqlx::types::Json<crate::model::node_pack::Batch>>,
    pub request_hash: Vec<u8>,
    pub end_bytes: i64,
    pub plan: sqlx::types::Json<Vec<crate::model::node_chunk::Item>>,
    pub ready: bool,
    pub commit_hash: Option<Vec<u8>>,
}

#[derive(sqlx::FromRow)]
pub(super) struct NodeRawWrite {
    pub request_hash: Vec<u8>,
    pub source_id: Uuid,
    pub size: i64,
    pub digest: Vec<u8>,
    pub content_hash: Option<Vec<u8>>,
    pub committed: bool,
}

pub(super) fn node_raw_write(node: Uuid, operation: Uuid) -> Rows<'static, NodeRawWrite> {
    sqlx::query_as("SELECT request_hash,source_id,size,digest,content_hash,committed FROM node_write_raw WHERE node_id=$1 AND operation_id=$2 FOR UPDATE").bind(node).bind(operation)
}

pub(super) fn insert_node_raw_write<'q>(
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

pub(super) fn commit_node_raw_write(node: Uuid, operation: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE node_write_raw SET committed=true WHERE node_id=$1 AND operation_id=$2 AND NOT committed").bind(node).bind(operation)
}

pub(super) fn node_write_batch(
    node: Uuid,
    operation: Uuid,
    batch: i64,
) -> Rows<'static, NodeWriteBatch> {
    sqlx::query_as("SELECT request_hash,end_bytes,plan,packing,ready,commit_hash FROM node_write_batches WHERE node_id=$1 AND operation_id=$2 AND batch_id=$3 FOR UPDATE")
        .bind(node).bind(operation).bind(batch)
}

pub(super) fn node_write_pending(node: Uuid, operation: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT (SELECT count(*) FROM node_write_batches WHERE node_id=$1 AND operation_id=$2 AND commit_hash IS NULL)+(SELECT count(*) FROM node_write_raw WHERE node_id=$1 AND operation_id=$2 AND NOT committed)").bind(node).bind(operation)
}

pub(super) fn insert_node_write_batch<'q>(
    node: Uuid,
    input: &'q crate::model::node_chunk::Prepare,
    hash: &'q [u8],
    end: i64,
    plan: &'q [crate::model::node_chunk::Item],
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_write_batches(node_id,operation_id,batch_id,request_hash,end_bytes,plan) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(node).bind(input.operation).bind(input.batch).bind(hash).bind(end).bind(sqlx::types::Json(plan))
}

pub(super) fn advance_node_write(node: Uuid, operation: Uuid, end: i64) -> Statement<'static> {
    sqlx::query("UPDATE node_writes SET next_batch=next_batch+1,next_offset=$3 WHERE node_id=$1 AND operation_id=$2 AND state='active'").bind(node).bind(operation).bind(end)
}

pub(super) fn activate_node_write_batch<'q>(
    node: Uuid,
    operation: Uuid,
    batch: i64,
    plan: &'q [crate::model::node_chunk::Item],
) -> Statement<'q> {
    sqlx::query("UPDATE node_write_batches SET ready=true,plan=$4 WHERE node_id=$1 AND operation_id=$2 AND batch_id=$3 AND NOT ready").bind(node).bind(operation).bind(batch).bind(sqlx::types::Json(plan))
}

pub(super) fn commit_node_write_batch<'q>(
    node: Uuid,
    operation: Uuid,
    batch: i64,
    hash: &'q [u8],
) -> Statement<'q> {
    sqlx::query("UPDATE node_write_batches SET commit_hash=$4,plan='[]'::jsonb,packing=NULL WHERE node_id=$1 AND operation_id=$2 AND batch_id=$3 AND ready AND commit_hash IS NULL").bind(node).bind(operation).bind(batch).bind(hash)
}

pub(super) fn node_write_size(stream: Id) -> Rows<'static, (i64, DateTime<Utc>)> {
    sqlx::query_as("SELECT size,touched_at FROM streams WHERE id=$1 FOR UPDATE").bind(stream)
}

pub(super) fn publish_node_write<'q>(
    node: Uuid,
    operation: Uuid,
    hash: &'q [u8],
    result: &'q crate::model::node_write::Published,
) -> Statement<'q> {
    sqlx::query("UPDATE node_writes SET state='published',completed_at=clock_timestamp(),publish_hash=$3,result=$4 WHERE node_id=$1 AND operation_id=$2 AND state='active'")
        .bind(node).bind(operation).bind(hash).bind(sqlx::types::Json(result))
}

pub(super) fn node_write_permits(
    node: Uuid,
    operation: Uuid,
    batch: Option<i64>,
) -> Rows<'static, (Uuid, i64)> {
    sqlx::query_as("SELECT p.token,GREATEST(0,(extract(epoch FROM (p.expires_at-clock_timestamp()))*1000)::bigint) FROM storage_permits p JOIN node_write_permits w ON w.token=p.token WHERE w.node_id=$1 AND w.operation_id=$2 AND w.batch_id IS NOT DISTINCT FROM $3 AND p.direction='upload' AND p.expires_at>clock_timestamp() ORDER BY p.token")
        .bind(node).bind(operation).bind(batch)
}

pub(super) fn node_write_own_permit(
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

pub(super) fn release_write_upload_permits(
    node: Uuid,
    operation: Uuid,
    batch: Option<i64>,
) -> Statement<'static> {
    sqlx::query("DELETE FROM storage_permits p USING node_write_permits n WHERE p.token=n.token AND n.node_id=$1 AND n.operation_id=$2 AND n.batch_id IS NOT DISTINCT FROM $3").bind(node).bind(operation).bind(batch)
}

pub(super) fn node_write_release_permits<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    streams: &'q [Uuid],
    permits: &'q [Uuid],
) -> Statement<'q> {
    sqlx::query("DELETE FROM storage_permits p USING node_write_permits n,node_writes w WHERE p.token=n.token AND w.node_id=n.node_id AND w.operation_id=n.operation_id AND w.node_id=$1 AND w.credential_generation=$2 AND w.boot_id=$3 AND w.fence=$4 AND (w.stream_id=ANY($5) OR p.token=ANY($6))")
        .bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(streams).bind(permits)
}

pub(super) fn node_multipart_operation(
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

pub(super) fn save_node_multipart_operation<'q>(
    node: &crate::cluster::NodeState,
    operation: Uuid,
    hash: &'q [u8],
    outcome: &'q crate::model::node_multipart::Outcome,
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_multipart_operations(node_id,operation_id,credential_generation,request_hash,outcome) VALUES($1,$2,$3,$4,$5)").bind(node.node_id).bind(operation).bind(node.credential_generation).bind(hash).bind(sqlx::types::Json(outcome))
}

pub(super) fn expire_node_multipart_operations(node: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM node_multipart_operations WHERE node_id=$1 AND operation_id IN (SELECT operation_id FROM node_multipart_operations WHERE node_id=$1 AND created_at<clock_timestamp()-interval '1 day' ORDER BY created_at LIMIT 64)").bind(node)
}

pub(super) fn node_upload_page<'q>(
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
pub(super) struct NodeCompletion {
    pub manifest_hash: String,
    pub conditions: sqlx::types::Json<crate::model::node_complete::Conditions>,
    pub summary: sqlx::types::Json<crate::model::node_complete::Summary>,
}

pub(super) fn node_completion(upload: Uuid) -> Rows<'static, NodeCompletion> {
    sqlx::query_as(
        "SELECT manifest_hash,conditions,summary FROM node_completions WHERE upload_id=$1",
    )
    .bind(upload)
}

pub(super) fn node_completion_summary(
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

pub(super) fn node_selected_parts<'q>(
    upload: Uuid,
    numbers: &'q [i32],
) -> Rows<'q, crate::model::node_complete::Selected> {
    sqlx::query_as("SELECT p.part_number AS number,s.size,s.etag,s.checksums FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=$1 AND p.part_number=ANY($2) AND s.state='ready' ORDER BY p.part_number FOR SHARE OF p,s").bind(upload).bind(numbers)
}

pub(super) fn save_node_completion<'q>(
    upload: Uuid,
    hash: &'q str,
    conditions: &'q crate::model::node_complete::Conditions,
    summary: &'q crate::model::node_complete::Summary,
) -> Statement<'q> {
    sqlx::query("INSERT INTO node_completions(upload_id,manifest_hash,conditions,summary) VALUES($1,$2,$3,$4)").bind(upload).bind(hash).bind(sqlx::types::Json(conditions)).bind(sqlx::types::Json(summary))
}

pub(super) fn freeze_node_completion<'q>(upload: Uuid, numbers: &'q [i32]) -> Statement<'q> {
    sqlx::query("INSERT INTO node_completion_parts(upload_id,ordinal,part_number,stream_id,size,etag,checksums) SELECT $1,n.ordinal-1,p.part_number,s.id,s.size,s.etag,s.checksums FROM unnest($2::integer[]) WITH ORDINALITY AS n(number,ordinal) JOIN parts p ON p.upload_id=$1 AND p.part_number=n.number JOIN streams s ON s.id=p.stream_id").bind(upload).bind(numbers)
}

pub(super) fn node_completion_checksums(upload: Uuid) -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT checksums FROM node_completion_parts WHERE upload_id=$1 ORDER BY ordinal",
    )
    .bind(upload)
}

pub(super) fn node_completion_sources(
    upload: Uuid,
    first: i32,
    count: i64,
) -> Rows<'static, crate::app::StoredStream> {
    sqlx::query_as("SELECT s.* FROM node_completion_parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=$1 AND p.ordinal>=$2 AND p.ordinal<$2::integer+$3::bigint AND s.state IN ('ready','retired') ORDER BY p.ordinal LIMIT $3 FOR SHARE OF s").bind(upload).bind(first).bind(count)
}

pub(super) fn bind_node_completion(
    node: Uuid,
    operation: Uuid,
    upload: Uuid,
    size: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE node_writes SET completion_upload=$3,expected_size=$4 WHERE node_id=$1 AND operation_id=$2").bind(node).bind(operation).bind(upload).bind(size)
}

pub(super) fn renew_node_completions<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    leases: &'q [Uuid],
) -> Statement<'q> {
    sqlx::query("UPDATE uploads u SET completion_expires_at=clock_timestamp()+interval '120 seconds',touched_at=clock_timestamp() FROM node_writes w,stream_leases l WHERE w.completion_upload=u.id AND u.completion_token=w.operation_id AND u.state='completing' AND u.completion_expires_at>clock_timestamp() AND w.state='active' AND w.node_id=$1 AND w.credential_generation=$2 AND w.boot_id=$3 AND w.fence=$4 AND l.id=w.lease_id AND l.id=ANY($5) AND l.expires_at>clock_timestamp()").bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(leases)
}

pub(super) fn release_node_completions(streams: &[Uuid]) -> Statement<'_> {
    sqlx::query("UPDATE uploads u SET completion_expires_at=clock_timestamp() FROM node_writes w WHERE w.completion_upload=u.id AND u.completion_token=w.operation_id AND u.state='completing' AND w.stream_id=ANY($1) AND w.state='aborted'").bind(streams)
}

pub(super) fn raw_gc_candidates(
    storage: Uuid,
    grace: i64,
    minimum: i64,
    limit: i64,
) -> Rows<'static, RawSource> {
    sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.storage_id=$1 AND r.owner_stream IS NULL AND r.unreferenced_at<now()-$2*interval '1 second' AND r.created_at<now()-$3*interval '1 second' AND NOT EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.kind='raw' AND n.location_id=r.id AND l.expires_at>clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=r.id) AND NOT EXISTS(SELECT 1 FROM transfer_sources p JOIN transfer_runs t ON t.task_id=p.task_id WHERE p.source_type='raw' AND p.source_id=r.id AND t.phase NOT IN ('complete','cancelled')) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=r.id) AND NOT EXISTS(SELECT 1 FROM uploads WHERE raw_source_id=r.id) ORDER BY r.unreferenced_at,r.id LIMIT $4").bind(storage).bind(grace).bind(minimum).bind(limit)
}

pub(super) fn raw_gc_claim(
    source: Id,
    revision: i64,
    grace: i64,
    minimum: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET state='deleting',revision=revision+1 WHERE id=$1 AND revision=$2 AND owner_stream IS NULL AND unreferenced_at<now()-$3*interval '1 second' AND created_at<now()-$4*interval '1 second' AND NOT EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.kind='raw' AND n.location_id=raw_sources.id AND l.expires_at>clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.task_type='integrity' AND t.state='running' AND t.detail->>'operation' IN ('recheck','verify') AND t.detail->>'source_location'=raw_sources.id::text AND ((t.lease_node_id IS NULL AND t.lease_expires_at IS NULL) OR t.lease_expires_at>clock_timestamp())) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM transfer_sources p JOIN transfer_runs t ON t.task_id=p.task_id WHERE p.source_type='raw' AND p.source_id=raw_sources.id AND t.phase NOT IN ('complete','cancelled')) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM uploads WHERE raw_source_id=raw_sources.id)").bind(source).bind(revision).bind(grace).bind(minimum)
}

pub(super) fn raw_gc_finish(source: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_sources WHERE id=$1 AND state='deleting' AND NOT EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.kind='raw' AND n.location_id=raw_sources.id AND l.expires_at>clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.task_type='integrity' AND t.state='running' AND t.detail->>'operation' IN ('recheck','verify') AND t.detail->>'source_location'=raw_sources.id::text AND ((t.lease_node_id IS NULL AND t.lease_expires_at IS NULL) OR t.lease_expires_at>clock_timestamp())) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM transfer_sources p JOIN transfer_runs t ON t.task_id=p.task_id WHERE p.source_type='raw' AND p.source_id=raw_sources.id AND t.phase NOT IN ('complete','cancelled')) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM uploads WHERE raw_source_id=raw_sources.id)").bind(source)
}

pub(super) fn node_upload_backlog(
    node: Option<Uuid>,
) -> Rows<'static, (i64, i64, Option<DateTime<Utc>>)> {
    sqlx::query_as("SELECT count(*),COALESCE(sum(size),0)::bigint,min(created_at) FROM (SELECT staged_size AS size,created_at FROM pending_uploads WHERE ($1::uuid IS NULL OR node_id=$1) UNION ALL SELECT r.size,r.created_at FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE ($1::uuid IS NULL OR u.node_id=$1) UNION ALL SELECT s.size,s.created_at FROM parts p JOIN streams s ON s.id=p.stream_id JOIN uploads u ON u.id=p.upload_id WHERE p.remote_node_id IS NOT NULL AND p.remote_etag IS NULL AND ($1::uuid IS NULL OR p.remote_node_id=$1) AND u.state IN ('active','completing')) waiting").bind(node)
}
