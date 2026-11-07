use super::*;
use crate::model::journal::restore::{Job, Progress, Selection};
use crate::storage::marker::Journal;
use sqlx::types::Json;
pub(in super::super) fn read(b: &Backend) -> &sqlx::PgPool {
    &b.pool
}
pub(in super::super) fn write(b: &Backend) -> &sqlx::PgPool {
    &b.pool
}
pub(in super::super) async fn begin(
    b: &Backend,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>> {
    Ok(b.pool.begin().await?)
}
pub(in super::super) async fn admin<'a>(
    b: &'a Backend,
    p: &Principal,
) -> Result<sqlx::Transaction<'a, sqlx::Postgres>> {
    transactions::admin_transaction(&b.pool, p).await
}
pub(in super::super) fn category(_key: &str, _mime: &str) -> &'static str {
    "other"
}

pub(in super::super) fn create<'a>(
    id: Uuid,
    store: Uuid,
    source: &'a [u8],
    progress: &'a Progress,
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_recoveries(id,source_storage,source,progress,state) VALUES($1,$2,$3,$4,'preview')").bind(id).bind(store).bind(source).bind(Json(progress))
}

pub(in super::super) fn job(id: Uuid) -> Rows<'static, Job> {
    sqlx::query_as("SELECT id,state,source,selection,progress,error_detail,revision,source_storage,target_storage FROM journal_recoveries WHERE id=$1").bind(id)
}

pub(in super::super) fn pending() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM journal_recoveries WHERE state IN ('queued','verifying','importing','claiming') AND (lease_until IS NULL OR lease_until<=clock_timestamp()) ORDER BY created_at LIMIT 8",
    )
}

pub(in super::super) fn queue<'a>(
    selection: &'a Selection,
    source: &'a [u8],
    target: Uuid,
    id: Uuid,
    rev: i64,
) -> Statement<'a> {
    sqlx::query("UPDATE journal_recoveries SET selection=$1,source=$2,target_storage=$3,state='queued',revision=revision+1 WHERE id=$4 AND revision=$5 AND state='preview'").bind(Json(selection)).bind(source).bind(target).bind(id).bind(rev)
}

pub(in super::super) fn scope(target: Uuid, id: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO journal_restore_scopes(storage_id,job) VALUES($1,$2)")
        .bind(target)
        .bind(id)
}

pub(in super::super) fn retry(id: Uuid, rev: i64) -> Statement<'static> {
    sqlx::query("UPDATE journal_recoveries SET state='queued',error_detail=NULL,revision=revision+1 WHERE id=$1 AND revision=$2 AND state='failed'").bind(id).bind(rev)
}

pub(in super::super) fn cancel(id: Uuid, rev: i64) -> Statement<'static> {
    sqlx::query("UPDATE journal_recoveries SET state='cancelled',source='\\x'::bytea,lease_token=NULL,lease_until=NULL,revision=revision+1 WHERE id=$1 AND revision=$2 AND state IN ('preview','queued','verifying','importing','failed')").bind(id).bind(rev)
}

pub(in super::super) fn discard_items(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_restore_items WHERE job=$1").bind(id)
}

pub(in super::super) fn discard_buckets(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_restore_buckets WHERE job=$1 AND state<>'published'").bind(id)
}

pub(in super::super) fn claim(token: Uuid, id: Uuid, again: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_recoveries SET state=CASE WHEN state='queued' THEN 'verifying' ELSE state END,lease_token=$1,lease_until=clock_timestamp()+INTERVAL '60 seconds' WHERE id=$2 AND state IN ('queued','verifying','importing','claiming') AND (lease_until IS NULL OR lease_until<=clock_timestamp() OR lease_token=$3)").bind(token).bind(id).bind(again)
}

pub(in super::super) fn fence(id: Uuid, token: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM journal_recoveries WHERE id=$1 AND lease_token=$2 AND lease_until>clock_timestamp() FOR UPDATE").bind(id).bind(token)
}

pub(in super::super) fn save<'a>(
    state: &'a str,
    progress: &'a Progress,
    error: Option<&'a str>,
    id: Uuid,
) -> Statement<'a> {
    sqlx::query("UPDATE journal_recoveries SET state=$1,progress=$2,error_detail=$3,lease_token=NULL,lease_until=NULL,updated_at=clock_timestamp(),revision=revision+1 WHERE id=$4").bind(state).bind(Json(progress)).bind(error).bind(id)
}

pub(in super::super) fn conflict<'a>(
    id: Uuid,
    fp: &'a [u8],
    algorithm: &'a str,
    again: Uuid,
    material: &'a [u8],
) -> Scalar<'a, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM managed_keys WHERE (id=$1 OR fingerprint=$2) AND (key_algorithm<>$3 OR id<>$4 OR fingerprint<>$5 OR NOT recovery_read_only OR revoked_at IS NOT NULL OR secret_encrypted IS NULL))").bind(id).bind(fp).bind(algorithm).bind(again).bind(material)
}

pub(in super::super) fn has_key(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM managed_keys WHERE id=$1").bind(id)
}

pub(in super::super) fn insert_key<'a>(
    id: Uuid,
    name: &'a str,
    alg: &'a str,
    protected: &'a [u8],
    fp: &'a [u8],
) -> Statement<'a> {
    sqlx::query("INSERT INTO managed_keys(id,name,purpose,key_algorithm,secret_encrypted,fingerprint,recovery_read_only) VALUES($1,$2,'block',$3,$4,$5,TRUE)").bind(id).bind(name).bind(alg).bind(protected).bind(fp)
}

pub(in super::super) fn mapping<'a>(job: Uuid, hash: &'a [u8]) -> Rows<'a, (Id, Vec<u8>)> {
    sqlx::query_as("SELECT native_id,content_hash FROM journal_restore_materials WHERE job=$1 AND identity_hash=$2").bind(job).bind(hash)
}

pub(in super::super) fn record_mapping<'a>(
    job: Uuid,
    hash: &'a [u8],
    id: Id,
    fp: &'a [u8],
    address: Uuid,
    unit: &'a str,
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_restore_materials(job,identity_hash,native_id,content_hash,address,unit) VALUES($1,$2,$3,$4,$5,$6)").bind(job).bind(hash).bind(id).bind(fp).bind(address).bind(unit)
}

pub(in super::super) fn chunk_by_hash<'a>(
    storage: Uuid,
    hash: &'a [u8],
    size: i32,
) -> Scalar<'a, Id> {
    sqlx::query_scalar("SELECT id FROM chunks WHERE storage_id=$1 AND hash=$2 AND raw_size=$3 AND state='ready' FOR UPDATE").bind(storage).bind(hash).bind(size)
}

pub(in super::super) async fn location_conflict(
    b: &Backend,
    target: Uuid,
    kind: crate::model::journal::record::Kind,
    ids: &[Uuid],
) -> Result<bool> {
    use crate::model::journal::record::Kind;
    let (table, address, storage) = match kind {
        Kind::Member => ("chunks", "id", "storage_id"),
        Kind::Chunk => ("chunk_encodings", "encoding_id", "storage_id"),
        Kind::Pack => ("packs", "id", "storage_id"),
        Kind::Raw => ("raw_sources", "id", "storage_id"),
        Kind::Fragment => ("fragments", "id", "staging_storage_id"),
    };
    let mut sql = sqlx::QueryBuilder::<sqlx::Postgres>::new(format!(
        "SELECT EXISTS(SELECT 1 FROM {table} WHERE ({storage} IS NULL OR {storage}<>"
    ));
    sql.push_bind(target).push(format!(") AND {address} IN ("));
    let mut list = sql.separated(",");
    for id in ids {
        list.push_bind(*id);
    }
    sql.push("))");
    Ok(sql.build_query_scalar().fetch_one(read(b)).await?)
}

pub(in super::super) fn chunk_id(uuid: Uuid, target: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM chunks WHERE id=$1 AND storage_id=$2")
        .bind(uuid)
        .bind(target)
}

pub(in super::super) fn encoding_id(uuid: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT encoding_id FROM chunk_encodings WHERE encoding_id=$1").bind(uuid)
}

pub(in super::super) fn pack_id(uuid: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM packs WHERE id=$1").bind(uuid)
}

pub(in super::super) fn raw_id(uuid: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM raw_sources WHERE id=$1").bind(uuid)
}

pub(in super::super) fn fragment_id(uuid: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM fragments WHERE id=$1").bind(uuid)
}

pub(in super::super) fn insert_chunk<'a>(
    uid: Uuid,
    target: Uuid,
    hash: &'a [u8],
    size: i32,
    alg: &'a str,
    block: Option<Uuid>,
    pack: Option<Id>,
    created: chrono::DateTime<chrono::Utc>,
) -> Statement<'a> {
    sqlx::query("INSERT INTO chunks(id,storage_id,hash,raw_size,encryption_algorithm,block_key_id,pack_id,created_at,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'preparing')").bind(uid).bind(target).bind(hash).bind(size).bind(alg).bind(block).bind(pack).bind(created)
}

pub(in super::super) fn member(
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

pub(in super::super) fn insert_encoding<'a>(
    uid: Uuid,
    chunk: Id,
    storage: Uuid,
    size: Option<i32>,
    compressed: bool,
    alg: &'a str,
    block: Option<Uuid>,
    nonce: Option<&'a [u8]>,
    counter: Option<i64>,
    created: chrono::DateTime<chrono::Utc>,
) -> Statement<'a> {
    sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,storage_id,standalone_size,standalone_compressed,encryption_algorithm,block_key_id,nonce,nonce_id,created_at,encoding_fingerprint,state,stored_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,decode(repeat('00',32),'hex'),'ready',clock_timestamp())").bind(uid).bind(chunk).bind(storage).bind(size).bind(compressed).bind(alg).bind(block).bind(nonce).bind(counter).bind(created)
}

pub(in super::super) fn ready_chunk<'a>(
    encoding: Id,
    size: Option<i32>,
    compressed: bool,
    nonce: Option<&'a [u8]>,
    id: Id,
) -> Statement<'a> {
    sqlx::query("UPDATE chunks SET state='ready',current_encoding_id=$1,standalone_size=$2,standalone_compressed=$3,nonce=$4 WHERE id=$5").bind(encoding).bind(size).bind(compressed).bind(nonce).bind(id)
}

pub(in super::super) fn insert_pack<'a>(
    uid: Uuid,
    storage: Uuid,
    alg: &'a str,
    block: Option<Uuid>,
    raw: i64,
    size: Option<i64>,
    compressed: bool,
    nonce: Option<&'a [u8]>,
    counter: Option<i64>,
    digest: Option<&'a [u8]>,
    members: i32,
    created: chrono::DateTime<chrono::Utc>,
) -> Statement<'a> {
    sqlx::query("INSERT INTO packs(id,storage_id,encryption_algorithm,block_key_id,raw_size,stored_size,pack_compressed,nonce,nonce_id,digest,member_count,created_at,pack_fingerprint,state,stored_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,decode(repeat('00',32),'hex'),'ready',clock_timestamp())").bind(uid).bind(storage).bind(alg).bind(block).bind(raw).bind(size).bind(compressed).bind(nonce).bind(counter).bind(digest).bind(members).bind(created)
}

pub(in super::super) fn insert_raw<'a>(
    uid: Uuid,
    storage: Uuid,
    size: i64,
    digest: Option<&'a [u8]>,
    profile: Uuid,
) -> Statement<'a> {
    sqlx::query("INSERT INTO raw_sources(id,storage_id,size,digest,profile_revision,medium,state) SELECT $1,$2,$3,$4,p.revision,CASE WHEN s.type='local' THEN 'local' ELSE 'shared_s3' END,'ready' FROM storage_profiles p JOIN storages s ON s.id=p.storage_id WHERE p.storage_id=$5").bind(uid).bind(storage).bind(size).bind(digest).bind(profile)
}

pub(in super::super) fn insert_fragment<'a>(
    uid: Uuid,
    size: i32,
    hash: &'a [u8],
    target: Uuid,
    path: &'a str,
) -> Statement<'a> {
    sqlx::query("INSERT INTO fragments(id,size,hash,staging_storage_id,staging_object_key,sealed) VALUES($1,$2,$3,$4,$5,TRUE)").bind(uid).bind(size).bind(hash).bind(target).bind(path)
}

pub(in super::super) fn planned(
    job: Uuid,
    bucket: Uuid,
) -> Rows<'static, (Uuid, bool, Option<String>)> {
    sqlx::query_as("SELECT target_bucket,state='published',CAST(next_key AS text) FROM journal_restore_buckets WHERE job=$1 AND source_bucket=$2 FOR UPDATE").bind(job).bind(bucket)
}

pub(in super::super) fn plan<'a>(
    job: Uuid,
    source: Uuid,
    target: Uuid,
    name: &'a str,
    generation: i64,
    water: i64,
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_restore_buckets(job,source_bucket,target_bucket,target_name,generation,watermark,state) VALUES($1,$2,$3,$4,$5,$6,'building')").bind(job).bind(source).bind(target).bind(name).bind(generation).bind(water)
}

pub(in super::super) fn stage<'a>(
    job: Uuid,
    bucket: Uuid,
    key: &'a str,
    size: i64,
    etag: &'a str,
    metadata: &'a serde_json::Value,
    sums: &'a serde_json::Value,
    payload: &'a str,
    modified: i64,
    mime: &'a str,
    category: &'a str,
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_restore_items(job,source_bucket,object_key,size,etag,metadata,checksums,payload,modified_us,content_type,category) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)").bind(job).bind(bucket).bind(key).bind(size).bind(etag).bind(Json(metadata)).bind(Json(sums)).bind(payload).bind(modified).bind(mime).bind(category)
}

pub(in super::super) fn range<'a>(
    job: Uuid,
    bucket: Uuid,
    key: &'a str,
    offset: i64,
    len: i64,
    start: i64,
    kind: &'a str,
    native: Id,
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_restore_ranges(job,source_bucket,object_key,offset_bytes,length,source_offset,kind,native_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8)").bind(job).bind(bucket).bind(key).bind(offset).bind(len).bind(start).bind(kind).bind(native)
}

pub(in super::super) fn advance<'a>(
    next: &'a str,
    size: i64,
    job: Uuid,
    source: Uuid,
) -> Statement<'a> {
    sqlx::query("UPDATE journal_restore_buckets SET next_key=$1,restored_objects=restored_objects+1,restored_bytes=restored_bytes+$2 WHERE job=$3 AND source_bucket=$4").bind(next).bind(size).bind(job).bind(source)
}

pub(in super::super) fn totals(job: Uuid, source: Uuid) -> Rows<'static, (String, i64, i64)> {
    sqlx::query_as("SELECT target_name,restored_bytes,restored_objects FROM journal_restore_buckets WHERE job=$1 AND source_bucket=$2").bind(job).bind(source)
}

pub(in super::super) fn publish_bucket<'a>(
    bucket: Uuid,
    name: &'a str,
    project: Uuid,
    storage: Uuid,
) -> Statement<'a> {
    sqlx::query("INSERT INTO buckets(id,name,project_id,storage_id) VALUES($1,$2,$3,$4)")
        .bind(bucket)
        .bind(name)
        .bind(project)
        .bind(storage)
}

pub(in super::super) fn streams(bucket: Uuid, job: Uuid, source: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,etag,metadata,checksums,payload_type,created_at,touched_at,write_authorization) SELECT $1,i.object_key,'object','ready',i.size,i.etag,i.metadata,i.checksums,i.payload,TIMESTAMPTZ 'epoch'+i.modified_us*INTERVAL '1 microsecond',TIMESTAMPTZ 'epoch'+i.modified_us*INTERVAL '1 microsecond','{}' FROM journal_restore_items i WHERE i.job=$2 AND i.source_bucket=$3").bind(bucket).bind(job).bind(source)
}

pub(in super::super) fn extents(bucket: Uuid, job: Uuid, source: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,source_offset,chunk_id,fragment_id) SELECT s.id,r.offset_bytes,r.length,r.source_offset,CASE WHEN r.kind='chunk' THEN r.native_id END,CASE WHEN r.kind='fragment' THEN r.native_id END FROM journal_restore_ranges r JOIN streams s ON s.bucket_id=$1 AND s.object_key=r.object_key WHERE r.job=$2 AND r.source_bucket=$3 AND r.kind<>'raw'").bind(bucket).bind(job).bind(source)
}

pub(in super::super) fn raw_ranges(bucket: Uuid, job: Uuid, source: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_offset,source_id) SELECT s.id,r.offset_bytes,r.length,r.source_offset,r.native_id FROM journal_restore_ranges r JOIN streams s ON s.bucket_id=$1 AND s.object_key=r.object_key WHERE r.job=$2 AND r.source_bucket=$3 AND r.kind='raw'").bind(bucket).bind(job).bind(source)
}

pub(in super::super) fn objects(
    bucket: Uuid,
    owner: Uuid,
    job: Uuid,
    source: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token,catalog_size,catalog_modified,catalog_type,catalog_category,catalog_public) SELECT $1,i.object_key,s.id,s.id,i.size,s.created_at,i.content_type,i.category,FALSE FROM journal_restore_items i JOIN streams s ON s.bucket_id=$2 AND s.object_key=i.object_key WHERE i.job=$3 AND i.source_bucket=$4").bind(bucket).bind(owner).bind(job).bind(source)
}

pub(in super::super) fn complete_bucket(job: Uuid, source: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE journal_restore_buckets SET state='published' WHERE job=$1 AND source_bucket=$2",
    )
    .bind(job)
    .bind(source)
}

pub(in super::super) fn remove_items(job: Uuid, source: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_restore_items WHERE job=$1 AND source_bucket=$2")
        .bind(job)
        .bind(source)
}

pub(in super::super) fn orphan<'a>(
    job: Uuid,
    addr: Uuid,
    path: &'a str,
    size: i64,
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_restore_orphans(job,address,file_path,byte_count) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING").bind(job).bind(addr).bind(path).bind(size)
}

pub(in super::super) fn adopt<'a>(
    desc: &'a Journal,
    target: Uuid,
    expected: &'a Journal,
) -> Statement<'a> {
    sqlx::query("UPDATE journal_stores SET descriptor=$1,marker_installed=TRUE WHERE storage_id=$2 AND (descriptor IS NULL OR descriptor=$3)").bind(Json(desc)).bind(target).bind(Json(expected))
}
pub(in super::super) fn protecting(storage: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM journal_restore_scopes WHERE storage_id=$1 AND protect_unknown)").bind(storage)
}
pub(in super::super) fn intent<'a>(source: &'a [u8], id: Uuid) -> Statement<'a> {
    sqlx::query(
        "UPDATE journal_recoveries SET source=$1,state='claiming',revision=revision+1 WHERE id=$2",
    )
    .bind(source)
    .bind(id)
}
pub(in super::super) fn release_source(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_recoveries SET source=decode('','hex') WHERE id=$1").bind(id)
}
pub(in super::super) fn release_scope(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_restore_scopes WHERE job=$1 AND storage_id<>(SELECT source_storage FROM journal_recoveries WHERE id=$2)").bind(id).bind(id)
}
pub(in super::super) async fn create_bucket_with_quota(
    db: &mut sqlx::PgConnection,
    bucket: Uuid,
    name: &str,
    project: Uuid,
    storage: Uuid,
    _bytes: i64,
    _objects: i64,
) -> Result<()> {
    // Native quota triggers reserve and account for the new bucket and its objects.
    publish_bucket(bucket, name, project, storage)
        .execute(&mut *db)
        .await?;

    Ok(())
}

pub(in super::super) fn packed_encoding(uid: Uuid, chunk: Id, storage: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,storage_id,state,encryption_algorithm,encoding_fingerprint) VALUES($1,$2,$3,'deleted','none',decode(repeat('00',32),'hex'))").bind(uid).bind(chunk).bind(storage)
}
pub(in super::super) fn seed_recovered(
    storage: Uuid,
    job: Uuid,
    source: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO journal_buckets(bucket_id,storage_id,bucket_name,generation,last_seq,published_seq,scanning,dirty,prepared_generation) SELECT source_bucket,$1,target_name,generation,watermark,watermark,FALSE,TRUE,generation FROM journal_restore_buckets WHERE job=$2 AND source_bucket=$3").bind(storage).bind(job).bind(source)
}
pub(in super::super) fn expire() -> Statement<'static> {
    sqlx::query(
        "UPDATE journal_recoveries SET source=decode('','hex'),state='cancelled',revision=revision+1 WHERE state='preview' AND created_at<clock_timestamp()-INTERVAL '24 hours'",
    )
}
pub(in super::super) fn recent() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM journal_recoveries WHERE state NOT IN ('preview','locked','cancelled') ORDER BY updated_at DESC LIMIT 20",
    )
}
