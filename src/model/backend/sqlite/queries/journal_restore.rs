use super::*;
use crate::model::journal::restore::{Job, Progress, Selection};
use crate::storage::marker::Journal;
use sqlx::types::Json;
pub(in super::super) fn read(b: &Backend) -> &sqlx::SqlitePool {
    &b.read
}
pub(in super::super) fn write(b: &Backend) -> &sqlx::SqlitePool {
    &b.write
}
pub(in super::super) async fn begin(
    b: &Backend,
) -> Result<sqlx::Transaction<'static, sqlx::Sqlite>> {
    Ok(b.write.begin_with(BEGIN_WRITE).await?)
}
pub(in super::super) async fn admin<'a>(
    b: &'a Backend,
    p: &Principal,
) -> Result<sqlx::Transaction<'a, sqlx::Sqlite>> {
    transactions::admin_transaction(&b.write, p).await
}
pub(in super::super) use super::quota::catalog_category as category;

pub(in super::super) fn create<'a>(
    id: Uuid,
    store: Uuid,
    source: &'a [u8],
    progress: &'a Progress,
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_recoveries(id,source_storage,source,progress,state) VALUES(?,?,?,?,'preview')").bind(id).bind(store).bind(source).bind(Json(progress))
}

pub(in super::super) fn job(id: Uuid) -> Rows<'static, Job> {
    sqlx::query_as("SELECT id,state,source,selection,progress,error_detail,revision,source_storage,target_storage FROM journal_recoveries WHERE id=?").bind(id)
}

pub(in super::super) fn pending() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM journal_recoveries WHERE state IN ('queued','verifying','importing','claiming') AND (lease_until IS NULL OR lease_until<=CAST(unixepoch('subsec')*1000000 AS INTEGER)) ORDER BY created_at LIMIT 8",
    )
}

pub(in super::super) fn queue<'a>(
    selection: &'a Selection,
    source: &'a [u8],
    target: Uuid,
    id: Uuid,
    rev: i64,
) -> Statement<'a> {
    sqlx::query("UPDATE journal_recoveries SET selection=?,source=?,target_storage=?,state='queued',revision=revision+1 WHERE id=? AND revision=? AND state='preview'").bind(Json(selection)).bind(source).bind(target).bind(id).bind(rev)
}

pub(in super::super) fn scope(target: Uuid, id: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO journal_restore_scopes(storage_id,job) VALUES(?,?)")
        .bind(target)
        .bind(id)
}

pub(in super::super) fn retry(id: Uuid, rev: i64) -> Statement<'static> {
    sqlx::query("UPDATE journal_recoveries SET state='queued',error_detail=NULL,revision=revision+1 WHERE id=? AND revision=? AND state='failed'").bind(id).bind(rev)
}

pub(in super::super) fn cancel(id: Uuid, rev: i64) -> Statement<'static> {
    sqlx::query("UPDATE journal_recoveries SET state='cancelled',source=X'',lease_token=NULL,lease_until=NULL,revision=revision+1 WHERE id=? AND revision=? AND state IN ('preview','queued','verifying','importing','failed')").bind(id).bind(rev)
}

pub(in super::super) fn discard_items(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_restore_items WHERE job=?").bind(id)
}

pub(in super::super) fn discard_buckets(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_restore_buckets WHERE job=? AND state<>'published'").bind(id)
}

pub(in super::super) fn claim(token: Uuid, id: Uuid, again: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_recoveries SET state=CASE WHEN state='queued' THEN 'verifying' ELSE state END,lease_token=?,lease_until=CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000 WHERE id=? AND state IN ('queued','verifying','importing','claiming') AND (lease_until IS NULL OR lease_until<=CAST(unixepoch('subsec')*1000000 AS INTEGER) OR lease_token=?)").bind(token).bind(id).bind(again)
}

pub(in super::super) fn fence(id: Uuid, token: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM journal_recoveries WHERE id=? AND lease_token=? AND lease_until>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(id).bind(token)
}

pub(in super::super) fn save<'a>(
    state: &'a str,
    progress: &'a Progress,
    error: Option<&'a str>,
    id: Uuid,
) -> Statement<'a> {
    sqlx::query("UPDATE journal_recoveries SET state=?,progress=?,error_detail=?,lease_token=NULL,lease_until=NULL,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),revision=revision+1 WHERE id=?").bind(state).bind(Json(progress)).bind(error).bind(id)
}

pub(in super::super) fn conflict<'a>(
    id: Uuid,
    fp: &'a [u8],
    algorithm: &'a str,
    again: Uuid,
    material: &'a [u8],
) -> Scalar<'a, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM managed_keys WHERE (id=? OR fingerprint=?) AND (key_algorithm<>? OR id<>? OR fingerprint<>? OR NOT recovery_read_only OR revoked_at IS NOT NULL OR secret_encrypted IS NULL))").bind(id).bind(fp).bind(algorithm).bind(again).bind(material)
}

pub(in super::super) fn has_key(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM managed_keys WHERE id=?").bind(id)
}

pub(in super::super) fn insert_key<'a>(
    id: Uuid,
    name: &'a str,
    alg: &'a str,
    protected: &'a [u8],
    fp: &'a [u8],
) -> Statement<'a> {
    sqlx::query("INSERT INTO managed_keys(id,name,purpose,key_algorithm,secret_encrypted,fingerprint,recovery_read_only) VALUES(?,?,'block',?,?,?,TRUE)").bind(id).bind(name).bind(alg).bind(protected).bind(fp)
}

pub(in super::super) fn mapping<'a>(job: Uuid, hash: &'a [u8]) -> Rows<'a, (Id, Vec<u8>)> {
    sqlx::query_as("SELECT native_id,content_hash FROM journal_restore_materials WHERE job=? AND identity_hash=?").bind(job).bind(hash)
}

pub(in super::super) fn record_mapping<'a>(
    job: Uuid,
    hash: &'a [u8],
    id: Id,
    fp: &'a [u8],
    address: Uuid,
    unit: &'a str,
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_restore_materials(job,identity_hash,native_id,content_hash,address,unit) VALUES(?,?,?,?,?,?)").bind(job).bind(hash).bind(id).bind(fp).bind(address).bind(unit)
}

pub(in super::super) fn chunk_by_hash<'a>(
    storage: Uuid,
    hash: &'a [u8],
    size: i32,
) -> Scalar<'a, Id> {
    sqlx::query_scalar(
        "SELECT id FROM chunks WHERE storage_id=? AND hash=? AND raw_size=? AND state='ready'",
    )
    .bind(storage)
    .bind(hash)
    .bind(size)
}

pub(in super::super) async fn location_conflict(
    b: &Backend,
    target: Uuid,
    kind: crate::model::journal::record::Kind,
    ids: &[Uuid],
) -> Result<bool> {
    use crate::model::journal::record::Kind;
    let (table, address, storage) = match kind {
        Kind::Member => return Ok(false),
        Kind::Chunk => ("chunk_encodings", "location_id", "storage_id"),
        Kind::Pack => ("packs", "location_id", "storage_id"),
        Kind::Raw => ("raw_sources", "location_id", "storage_id"),
        Kind::Fragment => ("fragments", "location_id", "staging_storage_id"),
    };
    let mut sql = sqlx::QueryBuilder::<sqlx::Sqlite>::new(format!(
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
    sqlx::query_scalar("SELECT id FROM chunks WHERE uuid=? AND storage_id=?")
        .bind(uuid)
        .bind(target)
}

pub(in super::super) fn encoding_id(uuid: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT encoding_id FROM chunk_encodings WHERE location_id=?").bind(uuid)
}

pub(in super::super) fn pack_id(uuid: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM packs WHERE location_id=?").bind(uuid)
}

pub(in super::super) fn raw_id(uuid: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM raw_sources WHERE location_id=?").bind(uuid)
}

pub(in super::super) fn fragment_id(uuid: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM fragments WHERE location_id=?").bind(uuid)
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
    sqlx::query("INSERT INTO chunks(uuid,storage_id,hash,raw_size,encryption_algorithm,block_key_id,pack_id,created_at,state) VALUES(?,?,?,?,?,?,?,?,'preparing')").bind(uid).bind(target).bind(hash).bind(size).bind(alg).bind(block).bind(pack).bind(created.timestamp_micros())
}

pub(in super::super) fn member(
    pack: Id,
    ordinal: i32,
    chunk: Id,
    offset: i64,
    encoding: &crate::content::codec::Chunk,
) -> Statement<'_> {
    sqlx::query("INSERT INTO pack_members(pack_id,ordinal,chunk_id,offset_bytes,encoding_uuid,encoded_size,compressed,encryption_algorithm,block_key_id,nonce) VALUES(?,?,?,?,?,?,?,?,?,?)")
        .bind(pack)
        .bind(ordinal)
        .bind(chunk)
        .bind(offset)
        .bind(encoding.location_id)
        .bind(encoding.stored_size)
        .bind(encoding.compressed)
        .bind(&encoding.algorithm)
        .bind(encoding.block_key_id)
        .bind(&encoding.nonce)
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
    sqlx::query("INSERT INTO chunk_encodings(location_id,chunk_id,storage_id,standalone_size,standalone_compressed,encryption_algorithm,block_key_id,nonce,nonce_id,created_at,encoding_fingerprint,state,stored_at) VALUES(?,?,?,?,?,?,?,?,?,?,'[]','ready',CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(uid).bind(chunk).bind(storage).bind(size).bind(compressed).bind(alg).bind(block).bind(nonce).bind(counter).bind(created.timestamp_micros())
}

pub(in super::super) fn ready_chunk<'a>(
    encoding: Id,
    size: Option<i32>,
    compressed: bool,
    nonce: Option<&'a [u8]>,
    id: Id,
) -> Statement<'a> {
    sqlx::query("UPDATE chunks SET state='ready',current_encoding_id=?,standalone_size=?,standalone_compressed=?,nonce=? WHERE id=?").bind(encoding).bind(size).bind(compressed).bind(nonce).bind(id)
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
    sqlx::query("INSERT INTO packs(location_id,storage_id,encryption_algorithm,block_key_id,raw_size,stored_size,pack_compressed,nonce,nonce_id,digest,member_count,created_at,pack_fingerprint,state,stored_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'[]','ready',CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(uid).bind(storage).bind(alg).bind(block).bind(raw).bind(size).bind(compressed).bind(nonce).bind(counter).bind(digest).bind(members).bind(created.timestamp_micros())
}

pub(in super::super) fn insert_raw<'a>(
    uid: Uuid,
    storage: Uuid,
    size: i64,
    digest: Option<&'a [u8]>,
    profile: Uuid,
) -> Statement<'a> {
    sqlx::query("INSERT INTO raw_sources(location_id,storage_id,size,digest,profile_revision,medium,state) SELECT ?,?,?,?,p.revision,CASE WHEN s.type='local' THEN 'local' ELSE 'shared_s3' END,'ready' FROM storage_profiles p JOIN storages s ON s.id=p.storage_id WHERE p.storage_id=?").bind(uid).bind(storage).bind(size).bind(digest).bind(profile)
}

pub(in super::super) fn insert_fragment<'a>(
    uid: Uuid,
    size: i32,
    hash: &'a [u8],
    target: Uuid,
    path: &'a str,
) -> Statement<'a> {
    sqlx::query("INSERT INTO fragments(location_id,size,hash,staging_storage_id,staging_object_key,sealed) VALUES(?,?,?,?,?,TRUE)").bind(uid).bind(size).bind(hash).bind(target).bind(path)
}

pub(in super::super) fn planned(
    job: Uuid,
    bucket: Uuid,
) -> Rows<'static, (Uuid, bool, Option<String>)> {
    sqlx::query_as("SELECT target_bucket,state='published',CAST(next_key AS TEXT) FROM journal_restore_buckets WHERE job=? AND source_bucket=?").bind(job).bind(bucket)
}

pub(in super::super) fn plan<'a>(
    job: Uuid,
    source: Uuid,
    target: Uuid,
    name: &'a str,
    generation: i64,
    water: i64,
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_restore_buckets(job,source_bucket,target_bucket,target_name,generation,watermark,state) VALUES(?,?,?,?,?,?,'building')").bind(job).bind(source).bind(target).bind(name).bind(generation).bind(water)
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
    sqlx::query("INSERT INTO journal_restore_items(job,source_bucket,object_key,size,etag,metadata,checksums,payload,modified_us,content_type,category) VALUES(?,?,?,?,?,?,?,?,?,?,?)").bind(job).bind(bucket).bind(key.as_bytes()).bind(size).bind(etag).bind(Json(metadata)).bind(Json(sums)).bind(payload).bind(modified).bind(mime).bind(category)
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
    sqlx::query("INSERT INTO journal_restore_ranges(job,source_bucket,object_key,offset_bytes,length,source_offset,kind,native_id) VALUES(?,?,?,?,?,?,?,?)").bind(job).bind(bucket).bind(key.as_bytes()).bind(offset).bind(len).bind(start).bind(kind).bind(native)
}

pub(in super::super) fn advance<'a>(
    next: &'a str,
    size: i64,
    job: Uuid,
    source: Uuid,
) -> Statement<'a> {
    sqlx::query("UPDATE journal_restore_buckets SET next_key=?,restored_objects=restored_objects+1,restored_bytes=restored_bytes+? WHERE job=? AND source_bucket=?").bind(next.as_bytes()).bind(size).bind(job).bind(source)
}

pub(in super::super) fn totals(job: Uuid, source: Uuid) -> Rows<'static, (String, i64, i64)> {
    sqlx::query_as("SELECT target_name,restored_bytes,restored_objects FROM journal_restore_buckets WHERE job=? AND source_bucket=?").bind(job).bind(source)
}

pub(in super::super) fn quota(
    bytes: i64,
    objects: i64,
    project: Uuid,
    allowance: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE quota_scopes SET bucket_count=bucket_count+1,used_bytes=used_bytes+?,object_count=object_count+? WHERE scope='project' AND scope_id=? AND (bucket_count_limit IS NULL OR bucket_count<bucket_count_limit) AND (logical_bytes_limit IS NULL OR used_bytes+reserved_bytes+?<=logical_bytes_limit)").bind(bytes).bind(objects).bind(project).bind(allowance)
}

pub(in super::super) fn publish_bucket<'a>(
    bucket: Uuid,
    name: &'a str,
    project: Uuid,
    storage: Uuid,
) -> Statement<'a> {
    sqlx::query("INSERT INTO buckets(id,name,project_id,storage_id) VALUES(?,?,?,?)")
        .bind(bucket)
        .bind(name)
        .bind(project)
        .bind(storage)
}

pub(in super::super) fn bucket_quota(bucket: Uuid, bytes: i64, items: i64) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO quota_scopes(scope,scope_id,used_bytes,object_count) VALUES('bucket',?,?,?)",
    )
    .bind(bucket)
    .bind(bytes)
    .bind(items)
}

pub(in super::super) fn streams(bucket: Uuid, job: Uuid, source: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,etag,metadata,checksums,payload_type,created_at,touched_at,write_authorization) SELECT ?,i.object_key,'object','ready',i.size,i.etag,i.metadata,i.checksums,i.payload,i.modified_us,i.modified_us,'{}' FROM journal_restore_items i WHERE i.job=? AND i.source_bucket=?").bind(bucket).bind(job).bind(source)
}

pub(in super::super) fn extents(bucket: Uuid, job: Uuid, source: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,source_offset,chunk_id,fragment_id) SELECT s.id,r.offset_bytes,r.length,r.source_offset,CASE WHEN r.kind='chunk' THEN r.native_id END,CASE WHEN r.kind='fragment' THEN r.native_id END FROM journal_restore_ranges r JOIN streams s ON s.bucket_id=? AND s.object_key=r.object_key WHERE r.job=? AND r.source_bucket=? AND r.kind<>'raw'").bind(bucket).bind(job).bind(source)
}

pub(in super::super) fn raw_ranges(bucket: Uuid, job: Uuid, source: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_offset,source_id) SELECT s.id,r.offset_bytes,r.length,r.source_offset,r.native_id FROM journal_restore_ranges r JOIN streams s ON s.bucket_id=? AND s.object_key=r.object_key WHERE r.job=? AND r.source_bucket=? AND r.kind='raw'").bind(bucket).bind(job).bind(source)
}

pub(in super::super) fn objects(
    bucket: Uuid,
    owner: Uuid,
    job: Uuid,
    source: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token,catalog_size,catalog_modified,catalog_type,catalog_category,catalog_public) SELECT ?,i.object_key,s.id,s.id,i.size,s.created_at,i.content_type,i.category,FALSE FROM journal_restore_items i JOIN streams s ON s.bucket_id=? AND s.object_key=i.object_key WHERE i.job=? AND i.source_bucket=?").bind(bucket).bind(owner).bind(job).bind(source)
}

pub(in super::super) fn complete_bucket(job: Uuid, source: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE journal_restore_buckets SET state='published' WHERE job=? AND source_bucket=?",
    )
    .bind(job)
    .bind(source)
}

pub(in super::super) fn remove_items(job: Uuid, source: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_restore_items WHERE job=? AND source_bucket=?")
        .bind(job)
        .bind(source)
}

pub(in super::super) fn orphan<'a>(
    job: Uuid,
    addr: Uuid,
    path: &'a str,
    size: i64,
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_restore_orphans(job,address,file_path,byte_count) VALUES(?,?,?,?) ON CONFLICT DO NOTHING").bind(job).bind(addr).bind(path).bind(size)
}

pub(in super::super) fn adopt<'a>(
    desc: &'a Journal,
    target: Uuid,
    expected: &'a Journal,
) -> Statement<'a> {
    sqlx::query("UPDATE journal_stores SET descriptor=?,marker_installed=TRUE WHERE storage_id=? AND (descriptor IS NULL OR descriptor=?)").bind(Json(desc)).bind(target).bind(Json(expected))
}
pub(in super::super) fn protecting(storage: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM journal_restore_scopes WHERE storage_id=? AND protect_unknown)").bind(storage)
}
pub(in super::super) fn intent<'a>(source: &'a [u8], id: Uuid) -> Statement<'a> {
    sqlx::query(
        "UPDATE journal_recoveries SET source=?,state='claiming',revision=revision+1 WHERE id=?",
    )
    .bind(source)
    .bind(id)
}
pub(in super::super) fn release_source(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_recoveries SET source=X'' WHERE id=?").bind(id)
}
pub(in super::super) fn release_scope(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_restore_scopes WHERE job=? AND storage_id<>(SELECT source_storage FROM journal_recoveries WHERE id=?)").bind(id).bind(id)
}
pub(in super::super) async fn create_bucket_with_quota(
    db: &mut sqlx::SqliteConnection,
    bucket: Uuid,
    name: &str,
    project: Uuid,
    storage: Uuid,
    bytes: i64,
    objects: i64,
) -> Result<()> {
    ensure!(
        quota(bytes, objects, project, bytes)
            .execute(&mut *db)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::QuotaExceeded
    );
    publish_bucket(bucket, name, project, storage)
        .execute(&mut *db)
        .await?;
    bucket_quota(bucket, bytes, objects)
        .execute(&mut *db)
        .await?;
    Ok(())
}

pub(in super::super) fn packed_encoding(uid: Uuid, chunk: Id, storage: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO chunk_encodings(location_id,chunk_id,storage_id,state,encryption_algorithm,encoding_fingerprint) VALUES(?,?,?,'deleted','none','[]')").bind(uid).bind(chunk).bind(storage)
}
pub(in super::super) fn seed_recovered(
    storage: Uuid,
    job: Uuid,
    source: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO journal_buckets(bucket_id,storage_id,bucket_name,generation,last_seq,published_seq,scanning,dirty,prepared_generation) SELECT source_bucket,?,target_name,generation,watermark,watermark,FALSE,TRUE,generation FROM journal_restore_buckets WHERE job=? AND source_bucket=?").bind(storage).bind(job).bind(source)
}
pub(in super::super) fn expire() -> Statement<'static> {
    sqlx::query(
        "UPDATE journal_recoveries SET source=X'',state='cancelled',revision=revision+1 WHERE state='preview' AND created_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-86400000000",
    )
}
pub(in super::super) fn recent() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM journal_recoveries WHERE state NOT IN ('preview','locked','cancelled') ORDER BY updated_at DESC LIMIT 20",
    )
}
