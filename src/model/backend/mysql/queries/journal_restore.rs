use super::*;
use crate::model::journal::restore::{Job, Progress, Selection};
use crate::storage::marker::Journal;
use sqlx::types::Json;
pub(in super::super) fn read(b: &Backend) -> &sqlx::MySqlPool {
    &b.pool
}
pub(in super::super) fn write(b: &Backend) -> &sqlx::MySqlPool {
    &b.pool
}
pub(in super::super) async fn begin(
    b: &Backend,
) -> Result<sqlx::Transaction<'static, sqlx::MySql>> {
    Ok(b.pool.begin().await?)
}
pub(in super::super) async fn admin<'a>(
    b: &'a Backend,
    p: &Principal,
) -> Result<sqlx::Transaction<'a, sqlx::MySql>> {
    transactions::admin_transaction(&b.pool, p).await
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
        "SELECT id FROM journal_recoveries WHERE state IN ('queued','verifying','importing','claiming') AND (lease_until IS NULL OR lease_until<=UTC_TIMESTAMP(6)) ORDER BY created_at LIMIT 8",
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
    sqlx::query("UPDATE journal_recoveries SET state=CASE WHEN state='queued' THEN 'verifying' ELSE state END,lease_token=?,lease_until=TIMESTAMPADD(SECOND,60,UTC_TIMESTAMP(6)) WHERE id=? AND state IN ('queued','verifying','importing','claiming') AND (lease_until IS NULL OR lease_until<=UTC_TIMESTAMP(6) OR lease_token=?)").bind(token).bind(id).bind(again)
}

pub(in super::super) fn fence(id: Uuid, token: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM journal_recoveries WHERE id=? AND lease_token=? AND lease_until>UTC_TIMESTAMP(6) FOR UPDATE").bind(id).bind(token)
}

pub(in super::super) fn save<'a>(
    state: &'a str,
    progress: &'a Progress,
    error: Option<&'a str>,
    id: Uuid,
) -> Statement<'a> {
    sqlx::query("UPDATE journal_recoveries SET state=?,progress=?,error_detail=?,lease_token=NULL,lease_until=NULL,updated_at=UTC_TIMESTAMP(6),revision=revision+1 WHERE id=?").bind(state).bind(Json(progress)).bind(error).bind(id)
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
    sqlx::query_scalar("SELECT id FROM chunks WHERE storage_id=? AND hash=? AND raw_size=? AND state='ready' FOR UPDATE").bind(storage).bind(hash).bind(size)
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
    let mut sql = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
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
    sqlx::query("INSERT INTO chunks(uuid,storage_id,hash,raw_size,encryption_algorithm,block_key_id,pack_id,created_at,state) VALUES(?,?,?,?,?,?,?,?,'preparing')").bind(uid).bind(target).bind(hash).bind(size).bind(alg).bind(block).bind(pack).bind(created)
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
    sqlx::query("INSERT INTO chunk_encodings(location_id,chunk_id,storage_id,standalone_size,standalone_compressed,encryption_algorithm,block_key_id,nonce,nonce_id,created_at,encoding_fingerprint,state,stored_at) VALUES(?,?,?,?,?,?,?,?,?,?,UNHEX(REPEAT('00',32)),'ready',UTC_TIMESTAMP(6))").bind(uid).bind(chunk).bind(storage).bind(size).bind(compressed).bind(alg).bind(block).bind(nonce).bind(counter).bind(created)
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
    sqlx::query("INSERT INTO packs(location_id,storage_id,encryption_algorithm,block_key_id,raw_size,stored_size,pack_compressed,nonce,nonce_id,digest,member_count,created_at,pack_fingerprint,state,stored_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,UNHEX(REPEAT('00',32)),'ready',UTC_TIMESTAMP(6))").bind(uid).bind(storage).bind(alg).bind(block).bind(raw).bind(size).bind(compressed).bind(nonce).bind(counter).bind(digest).bind(members).bind(created)
}

pub(in super::super) fn insert_raw<'a>(
    uid: Uuid,
    storage: Uuid,
    size: i64,
    digest: Option<&'a [u8]>,
    profile: Uuid,
) -> Statement<'a> {
    sqlx::query("INSERT INTO raw_sources(location_id,storage_id,size,digest,profile_revision,medium,state) SELECT ?,?,?,?,p.revision,CASE WHEN s.type='local' THEN 'local' ELSE 'shared' END,'ready' FROM storage_profiles p JOIN storages s ON s.id=p.storage_id WHERE p.storage_id=?").bind(uid).bind(storage).bind(size).bind(digest).bind(profile)
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
    sqlx::query_as("SELECT target_bucket,state='published',CAST(next_key AS CHAR) FROM journal_restore_buckets WHERE job=? AND source_bucket=? FOR UPDATE").bind(job).bind(bucket)
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
    identity: &'a str,
    logical: &'a str,
    history_id: Option<&'a str>,
    order: Option<i64>,
    when: Option<i64>,
    marker: bool,
    size: i64,
    etag: &'a str,
    metadata: &'a serde_json::Value,
    sums: &'a serde_json::Value,
    payload: &'a str,
    modified: i64,
    mime: &'a str,
    category: &'a str,
    origin: Option<Uuid>,
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_restore_items(job,source_bucket,object_key,size,etag,metadata,checksums,payload,modified_us,content_type,category,stage_key,logical_key,history_id,history_seq,history_time,history_marker,origin_stream) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)").bind(job).bind(bucket).bind(key.as_bytes()).bind(size).bind(etag).bind(Json(metadata)).bind(Json(sums)).bind(payload).bind(modified).bind(mime).bind(category).bind(identity.as_bytes()).bind(logical.as_bytes()).bind(history_id).bind(order).bind(when).bind(marker).bind(origin)
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
    count: i64,
    job: Uuid,
    source: Uuid,
) -> Statement<'a> {
    sqlx::query("UPDATE journal_restore_buckets SET next_key=?,restored_objects=restored_objects+?,restored_bytes=restored_bytes+? WHERE job=? AND source_bucket=?").bind(next.as_bytes()).bind(count).bind(size).bind(job).bind(source)
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
    sqlx::query("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,etag,metadata,checksums,payload_type,created_at,touched_at,write_authorization) SELECT ?,i.stage_key,'object',CASE WHEN i.history_id IS NULL THEN 'ready' ELSE 'retired' END,i.size,i.etag,i.metadata,i.checksums,i.payload,TIMESTAMPADD(MICROSECOND,i.modified_us,CAST('1970-01-01' AS DATETIME(6))),TIMESTAMPADD(MICROSECOND,i.modified_us,CAST('1970-01-01' AS DATETIME(6))),'{}' FROM journal_restore_items i WHERE NOT i.history_marker AND i.job=? AND i.source_bucket=?").bind(bucket).bind(job).bind(source)
}

pub(in super::super) fn extents(bucket: Uuid, job: Uuid, source: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,source_offset,chunk_id,fragment_id) SELECT s.id,r.offset_bytes,r.length,r.source_offset,CASE WHEN r.kind='chunk' THEN r.native_id END,CASE WHEN r.kind='fragment' THEN r.native_id END FROM journal_restore_ranges r JOIN journal_restore_items i ON i.job=r.job AND i.source_bucket=r.source_bucket AND i.object_key=r.object_key JOIN streams s ON s.bucket_id=? AND s.object_key=i.stage_key WHERE r.job=? AND r.source_bucket=? AND r.kind<>'raw'").bind(bucket).bind(job).bind(source)
}

pub(in super::super) fn raw_ranges(bucket: Uuid, job: Uuid, source: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_offset,source_id) SELECT s.id,r.offset_bytes,r.length,r.source_offset,r.native_id FROM journal_restore_ranges r JOIN journal_restore_items i ON i.job=r.job AND i.source_bucket=r.source_bucket AND i.object_key=r.object_key JOIN streams s ON s.bucket_id=? AND s.object_key=i.stage_key WHERE r.job=? AND r.source_bucket=? AND r.kind='raw'").bind(bucket).bind(job).bind(source)
}

pub(in super::super) fn objects(
    bucket: Uuid,
    owner: Uuid,
    job: Uuid,
    source: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token,catalog_size,catalog_modified,catalog_type,catalog_category,catalog_public) SELECT ?,i.logical_key,s.id,s.id,i.size,s.created_at,i.content_type,i.category,FALSE FROM journal_restore_items i JOIN streams s ON s.bucket_id=? AND s.object_key=i.stage_key WHERE i.history_id IS NULL AND NOT i.history_marker AND i.job=? AND i.source_bucket=?").bind(bucket).bind(owner).bind(job).bind(source)
}

pub(in super::super) fn restore_mode(job: Uuid, source: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT COALESCE(MAX(history_seq),0) FROM journal_restore_items WHERE job=? AND source_bucket=? AND history_id='mode'").bind(job).bind(source)
}
pub(in super::super) fn version_rows(
    bucket: Uuid,
    owner: Uuid,
    job: Uuid,
    source: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO object_versions(bucket_id,object_key,seq,version_id,stream_id,modified_us) SELECT ?,i.logical_key,i.history_seq,i.history_id,s.id,i.history_time FROM journal_restore_items i LEFT JOIN streams s ON s.bucket_id=? AND s.object_key=i.stage_key WHERE i.job=? AND i.source_bucket=? AND i.history_id IS NOT NULL AND i.history_id NOT IN ('mode','point','window')").bind(bucket).bind(owner).bind(job).bind(source)
}
pub(in super::super) fn version_clock(bucket: Uuid, owner: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO object_version_clocks(bucket_id,version_seq) SELECT ?,COALESCE(MAX(seq),0) FROM object_versions WHERE bucket_id=?").bind(bucket).bind(owner)
}
pub(in super::super) fn missing_heads(bucket: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO objects(bucket_id,object_key,write_token) SELECT v.bucket_id,v.object_key,0 FROM object_versions v LEFT JOIN objects o ON o.bucket_id=v.bucket_id AND o.object_key=v.object_key WHERE v.bucket_id=? AND o.object_key IS NULL GROUP BY v.bucket_id,v.object_key").bind(bucket)
}
pub(in super::super) async fn charge_history(
    tx: &mut super::history::Tx<'_>,
    bucket: Uuid,
) -> Result<()> {
    let bytes:i64=sqlx::query_scalar("SELECT CAST(COALESCE(SUM(s.size),0) AS SIGNED) FROM object_versions v JOIN streams s ON s.id=v.stream_id WHERE v.bucket_id=? AND NOT EXISTS(SELECT 1 FROM objects o WHERE o.bucket_id=v.bucket_id AND o.object_key=v.object_key AND o.stream_id=v.stream_id)").bind(bucket).fetch_one(&mut **tx).await?;
    if bytes > 0 {
        let mut accounts = super::super::transactions::lock_quota(tx, bucket).await?;
        super::super::transactions::adjust_quota(tx, &mut accounts, bytes, 0, 0, 0).await?;
    }
    Ok(())
}
pub(in super::super) fn retire_heads(bucket: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='retired' WHERE bucket_id=? AND state='ready' AND EXISTS(SELECT 1 FROM objects o JOIN object_versions v ON v.bucket_id=o.bucket_id AND v.object_key=o.object_key WHERE o.stream_id=streams.id)").bind(bucket)
}
pub(in super::super) fn version_heads(bucket: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE objects SET stream_id=(SELECT v.stream_id FROM object_versions v WHERE v.bucket_id=objects.bucket_id AND v.object_key=objects.object_key ORDER BY v.seq DESC LIMIT 1) WHERE bucket_id=? AND EXISTS(SELECT 1 FROM object_versions v WHERE v.bucket_id=objects.bucket_id AND v.object_key=objects.object_key)").bind(bucket)
}
pub(in super::super) fn ready_heads(bucket: Uuid, owner: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='ready' WHERE bucket_id=? AND id IN (SELECT stream_id FROM objects WHERE bucket_id=? AND stream_id IS NOT NULL)").bind(bucket).bind(owner)
}
pub(in super::super) fn stream_names(job: Uuid, source: Uuid, bucket: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE streams SET object_key=(SELECT i.logical_key FROM journal_restore_items i WHERE i.job=? AND i.source_bucket=? AND i.stage_key=streams.object_key) WHERE bucket_id=?").bind(job).bind(source).bind(bucket)
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
    sqlx::query("INSERT INTO journal_restore_orphans(job,address,file_path,byte_count) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE address=VALUES(address)").bind(job).bind(addr).bind(path).bind(size)
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
    db: &mut sqlx::MySqlConnection,
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
    sqlx::query("INSERT INTO chunk_encodings(location_id,chunk_id,storage_id,state,encryption_algorithm,encoding_fingerprint) VALUES(?,?,?,'deleted','none',UNHEX(REPEAT('00',32)))").bind(uid).bind(chunk).bind(storage)
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
        "UPDATE journal_recoveries SET source=X'',state='cancelled',revision=revision+1 WHERE state='preview' AND created_at<TIMESTAMPADD(HOUR,-24,UTC_TIMESTAMP(6))",
    )
}
pub(in super::super) fn recent() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM journal_recoveries WHERE state NOT IN ('preview','locked','cancelled') ORDER BY updated_at DESC LIMIT 20",
    )
}
pub(in super::super) fn native_pending(storage: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT j.id FROM journal_restore_scopes p JOIN journal_recoveries j ON j.id=p.job JOIN storages s ON s.id=p.storage_id WHERE p.storage_id=? AND s.type='mokyu' AND j.source_storage=j.target_storage AND j.state<>'completed'").bind(storage)
}

pub(in super::super) fn native_read_only(bucket: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM buckets b JOIN storages s ON s.id=b.storage_id JOIN journal_restore_scopes p ON p.storage_id=s.id JOIN journal_recoveries j ON j.id=p.job WHERE b.id=? AND s.type='mokyu' AND j.source_storage=j.target_storage AND j.state<>'completed')").bind(bucket)
}

pub(in super::super) fn time_window_record(
    job: Uuid,
    b: Uuid,
) -> Rows<'static, (i64, i64, String)> {
    sqlx::query_as("SELECT history_seq,history_time,CONVERT(metadata USING utf8mb4) FROM journal_restore_items WHERE job=? AND source_bucket=? AND history_id='window'").bind(job).bind(b)
}
pub(in super::super) fn time_rows(
    bucket: Uuid,
    owner: Uuid,
    job: Uuid,
    source: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO object_time_points(bucket_id,object_key,seq,seen_us,stream_id) SELECT ?,i.logical_key,i.history_seq,i.history_time,CASE WHEN i.history_marker THEN NULL ELSE COALESCE((SELECT ss.id FROM journal_restore_items alt JOIN streams ss ON ss.object_key=alt.stage_key WHERE ss.bucket_id=s.bucket_id AND alt.job=i.job AND alt.source_bucket=i.source_bucket AND alt.origin_stream=i.origin_stream AND alt.logical_key=i.logical_key AND NOT alt.history_marker ORDER BY CASE WHEN alt.history_id='point' THEN 2 WHEN alt.history_id IS NULL THEN 1 ELSE 0 END,alt.object_key LIMIT 1),s.id) END FROM journal_restore_items i LEFT JOIN streams s ON s.bucket_id=? AND s.object_key=i.stage_key WHERE i.job=? AND i.source_bucket=? AND i.history_id='point'").bind(bucket).bind(owner).bind(job).bind(source)
}
pub(in super::super) fn time_clock(b: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE bucket_time_windows SET seq=COALESCE((SELECT MAX(t.seq) FROM object_time_points t WHERE t.bucket_id=bucket_time_windows.bucket_id),0),last_us=COALESCE((SELECT MAX(t.seen_us) FROM object_time_points t WHERE t.bucket_id=bucket_time_windows.bucket_id),floor_us) WHERE bucket_id=?").bind(b)
}

pub(in super::super) fn retained_time_points(b: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO time_retained_streams(bucket_id,stream_id,bytes) SELECT DISTINCT t.bucket_id,t.stream_id,s.size FROM object_time_points t JOIN streams s ON s.id=t.stream_id WHERE t.bucket_id=? AND NOT EXISTS(SELECT 1 FROM objects o WHERE o.bucket_id=t.bucket_id AND o.stream_id=t.stream_id) AND NOT EXISTS(SELECT 1 FROM object_versions v WHERE v.bucket_id=t.bucket_id AND v.stream_id=t.stream_id)").bind(b)
}
