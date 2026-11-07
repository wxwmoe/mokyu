pub(in super::super) use super::journal_restore::{admin, begin, read, write};
use super::*;
use crate::model::journal::snapshots::{Garbage, Item, Job, Part, Scope};
use crate::storage::journal::snapshot::Page;
use sqlx::types::Json;

pub(in super::super) fn count_log(
    bytes: i64,
    file: Uuid,
    inc: i64,
    threshold: i64,
    storage: Uuid,
) -> Statement<'static> {
    sqlx::query("UPDATE journal_stores SET snapshot_log_bytes=snapshot_log_bytes+?,snapshot_auto_due=snapshot_auto_due OR (EXISTS(SELECT 1 FROM journal_file_entries m JOIN journal_entries e ON e.bucket_id=m.bucket_id AND e.seq=m.seq WHERE m.file_id=? AND e.operation IN ('put','delete')) AND snapshot_log_bytes+?>=?) WHERE storage_id=?").bind(bytes).bind(file).bind(inc).bind(threshold).bind(storage)
}

pub(in super::super) fn sources() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT s.storage_id FROM journal_stores s WHERE s.marker_installed AND ((s.snapshot_auto_due AND NOT EXISTS(SELECT 1 FROM journal_snapshots f WHERE f.storage_id=s.storage_id AND f.state='failed')) OR EXISTS(SELECT 1 FROM journal_snapshots j WHERE j.storage_id=s.storage_id AND (j.state IN ('queued','building','prepared') OR (j.state='failed' AND j.retry_after<=CAST(unixepoch('subsec')*1000000 AS INTEGER)) OR (j.state='retained' AND j.gc_after<=CAST(unixepoch('subsec')*1000000 AS INTEGER)))) OR (s.snapshot_gc_error IS NULL AND NOT EXISTS(SELECT 1 FROM journal_snapshots f WHERE f.storage_id=s.storage_id AND f.state='failed') AND EXISTS(SELECT 1 FROM journal_snapshot_gc g WHERE g.storage_id=s.storage_id AND g.reclaim_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER)))) ORDER BY s.storage_id",
    )
}

pub(in super::super) fn claim(token: Uuid, storage: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_stores SET snapshot_token=?,snapshot_lease_until=CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000 WHERE storage_id=? AND (snapshot_token IS NULL OR snapshot_lease_until<=CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(token).bind(storage)
}

pub(in super::super) fn renew(storage: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_stores SET snapshot_lease_until=CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000 WHERE storage_id=? AND snapshot_token=? AND snapshot_lease_until>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(storage).bind(token)
}

pub(in super::super) fn release(storage: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_stores SET snapshot_token=NULL,snapshot_lease_until=NULL WHERE storage_id=? AND snapshot_token=?").bind(storage).bind(token)
}

pub(in super::super) fn fence(storage: Uuid, token: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT storage_id FROM journal_stores WHERE storage_id=? AND snapshot_token=? AND snapshot_lease_until>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(storage).bind(token)
}

pub(in super::super) fn installed(storage: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT storage_id FROM journal_stores WHERE storage_id=? AND marker_installed",
    )
    .bind(storage)
}

pub(in super::super) fn job(id: Uuid) -> Rows<'static, Job> {
    sqlx::query_as("SELECT encrypt_frames,id,storage_id,kind,state,parent,revision,sequence_no,digest,epoch,page_limit,covered_bytes,manifest_etag,index_etag FROM journal_snapshots WHERE id=?").bind(id)
}

pub(in super::super) fn next(storage: Uuid) -> Rows<'static, Job> {
    sqlx::query_as("SELECT encrypt_frames,id,storage_id,kind,state,parent,revision,sequence_no,digest,epoch,page_limit,covered_bytes,manifest_etag,index_etag FROM journal_snapshots WHERE storage_id=? AND state IN ('building','prepared','queued','failed') ORDER BY planned_at,id LIMIT 1").bind(storage)
}

pub(in super::super) fn first(
    storage: Uuid,
) -> Rows<'static, (Uuid, String, String, Option<String>)> {
    sqlx::query_as("SELECT id,kind,state,error_detail FROM journal_snapshots WHERE storage_id=? AND state IN ('building','prepared','queued','failed') ORDER BY planned_at,id LIMIT 1").bind(storage)
}

pub(in super::super) fn request<'a>(id: Uuid, storage: Uuid, kind: &'a str) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_snapshots(id,storage_id,kind,state,encrypt_frames) VALUES(?,?,?,'queued',(SELECT block_key_id IS NOT NULL FROM storages WHERE id=?))")
        .bind(id)
        .bind(storage)
        .bind(kind)
        .bind(storage)
}

pub(in super::super) fn auto(storage: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT snapshot_auto_due FROM journal_stores WHERE storage_id=?")
        .bind(storage)
}

pub(in super::super) fn retry(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_snapshots SET state=CASE WHEN root_index IS NULL THEN 'building' ELSE 'prepared' END,error_detail=NULL WHERE id=? AND state='failed'").bind(id)
}

pub(in super::super) fn sync(storage: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_stores SET flush_requested=TRUE WHERE storage_id=? AND open_file IS NOT NULL").bind(storage)
}

pub(in super::super) fn ready(storage: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT allocated_seq=emitted_seq AND open_file IS NULL AND NOT EXISTS(SELECT 1 FROM journal_files f WHERE f.storage_id=journal_stores.storage_id AND f.state='sealed') AND (lease_token IS NULL OR lease_until<=CAST(unixepoch('subsec')*1000000 AS INTEGER)) AND NOT EXISTS(SELECT 1 FROM journal_buckets b JOIN bucket_change_gates g ON g.bucket_id=b.bucket_id WHERE b.storage_id=journal_stores.storage_id AND g.journal_mode=1 AND (b.scanning OR b.prepared_generation<>b.generation)) AND NOT EXISTS(SELECT 1 FROM journal_file_entries m JOIN journal_files f ON f.id=m.file_id JOIN journal_buckets j ON j.bucket_id=m.bucket_id WHERE f.storage_id=journal_stores.storage_id AND f.state='published' AND m.seq>j.published_seq) FROM journal_stores WHERE storage_id=?").bind(storage)
}

pub(in super::super) fn point(
    storage: Uuid,
) -> Rows<
    'static,
    (
        Option<Uuid>,
        i64,
        i64,
        Option<Vec<u8>>,
        i64,
        Json<crate::model::journal::policy::Policy>,
        Json<crate::storage::marker::Journal>,
    ),
> {
    sqlx::query_as("SELECT snapshot_id,snapshot_revision,emitted_seq,seal_digest,snapshot_log_bytes,policy,descriptor FROM journal_stores WHERE storage_id=?").bind(storage)
}

pub(in super::super) fn freeze<'a>(
    parent: Option<Uuid>,
    revision: i64,
    seq: i64,
    tail: &'a [u8],
    epoch: Uuid,
    page: i64,
    bytes: i64,
    id: Uuid,
) -> Statement<'a> {
    sqlx::query("UPDATE journal_snapshots SET state='building',parent=?,revision=?,sequence_no=?,digest=?,epoch=?,page_limit=?,covered_bytes=?,error_detail=NULL WHERE id=?").bind(parent).bind(revision).bind(seq).bind(tail).bind(epoch).bind(page).bind(bytes).bind(id)
}

pub(in super::super) fn reset_dictionary(storage: Uuid, epoch: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_dictionary WHERE storage_id=? AND epoch=?")
        .bind(storage)
        .bind(epoch)
}

pub(in super::super) fn clear_work(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_snapshot_work WHERE snapshot_id=?").bind(id)
}

pub(in super::super) fn clear_refs(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_snapshot_refs WHERE snapshot_id=?").bind(id)
}

pub(in super::super) fn clear_scopes(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_snapshot_buckets WHERE snapshot_id=?").bind(id)
}

pub(in super::super) fn clear_members(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_snapshot_members WHERE owner=?").bind(id)
}

pub(in super::super) fn clear_pages(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_snapshot_pages WHERE owner=?").bind(id)
}

pub(in super::super) fn scope(id: Uuid, storage: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO journal_snapshot_buckets(snapshot_id,bucket_id,bucket_name,generation,watermark,included,mode_revision) SELECT ?,j.bucket_id,j.bucket_name,j.generation,j.published_seq,(COALESCE(g.journal_mode,0)=1 AND j.prepared_generation=j.generation),COALESCE(b.settings_revision,0) FROM journal_buckets j LEFT JOIN bucket_change_gates g ON g.bucket_id=j.bucket_id LEFT JOIN buckets b ON b.id=j.bucket_id WHERE j.storage_id=? AND j.published_seq>0").bind(id).bind(storage)
}

pub(in super::super) fn scopes(id: Uuid) -> Rows<'static, Scope> {
    sqlx::query_as("SELECT bucket_id,bucket_name,generation,watermark,included,mode_revision FROM journal_snapshot_buckets WHERE snapshot_id=? ORDER BY bucket_id").bind(id)
}

pub(in super::super) fn changed(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM journal_snapshot_buckets sc LEFT JOIN buckets b ON b.id=sc.bucket_id LEFT JOIN bucket_change_gates g ON g.bucket_id=sc.bucket_id WHERE sc.snapshot_id=? AND (sc.mode_revision<>COALESCE(b.settings_revision,0) OR sc.included<>(COALESCE(g.journal_mode,0)=1)))").bind(id)
}

pub(in super::super) fn deltas(job: Uuid, cut: Uuid, parent: Option<Uuid>) -> Statement<'static> {
    sqlx::query("INSERT INTO journal_snapshot_work(snapshot_id,bucket_id,object_key,generation,seq,deleted) SELECT ?,e.bucket_id,e.object_key,e.generation,e.seq,e.operation='delete' FROM journal_entries e JOIN (SELECT d.bucket_id,d.object_key,MAX(d.seq) AS seq FROM journal_entries d JOIN journal_snapshot_buckets b ON b.snapshot_id=? AND b.bucket_id=d.bucket_id AND b.included AND d.generation=b.generation AND d.seq<=b.watermark LEFT JOIN journal_snapshot_buckets p ON p.snapshot_id=? AND p.bucket_id=d.bucket_id WHERE d.operation IN ('put','delete') AND d.seq>COALESCE(p.watermark,0) GROUP BY d.bucket_id,d.object_key) latest ON latest.bucket_id=e.bucket_id AND latest.seq=e.seq").bind(job).bind(cut).bind(parent)
}

pub(in super::super) fn current(
    job: Uuid,
    cut: Uuid,
    storage: Uuid,
    again: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO journal_snapshot_work(snapshot_id,bucket_id,object_key,generation,seq,deleted) SELECT ?,c.bucket_id,c.object_key,c.generation,c.seq,c.deleted FROM journal_snapshot_current c JOIN journal_snapshot_buckets b ON b.snapshot_id=? AND b.bucket_id=c.bucket_id AND b.included AND b.generation=c.generation WHERE c.storage_id=? AND NOT c.deleted AND NOT EXISTS(SELECT 1 FROM journal_snapshot_work w WHERE w.snapshot_id=? AND w.bucket_id=c.bucket_id AND w.object_key=c.object_key)").bind(job).bind(cut).bind(storage).bind(again)
}

pub(in super::super) fn hot_items(
    job: Uuid,
    cut: Uuid,
    owner: Uuid,
    family: i64,
    storage: Uuid,
    again: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO journal_snapshot_work(snapshot_id,bucket_id,object_key,generation,seq,deleted) SELECT ?,c.bucket_id,c.object_key,c.generation,c.seq,c.deleted FROM journal_snapshot_current c JOIN journal_snapshot_buckets b ON b.snapshot_id=? AND b.bucket_id=c.bucket_id AND b.included AND b.generation=c.generation JOIN journal_snapshot_members m ON m.owner=? AND m.family=? AND m.bucket_id=c.bucket_id AND m.seq=c.seq WHERE c.storage_id=? AND NOT EXISTS(SELECT 1 FROM journal_snapshot_work w WHERE w.snapshot_id=? AND w.bucket_id=c.bucket_id AND w.object_key=c.object_key)").bind(job).bind(cut).bind(owner).bind(family).bind(storage).bind(again)
}

pub(in super::super) fn scores(
    cut: Uuid,
    job: Uuid,
    storage: Uuid,
    parent: Uuid,
) -> Rows<'static, (Uuid, i64, i64, i64)> {
    sqlx::query_as("SELECT m.owner,m.family,CAST(SUM(m.encoded_bytes) AS INTEGER),CAST(SUM(CASE WHEN b.included AND m.generation=b.generation AND m.seq=COALESCE(w.seq,c.seq) THEN 0 ELSE m.encoded_bytes END) AS INTEGER) FROM journal_snapshot_refs r JOIN journal_snapshot_members m ON m.owner=r.owner AND m.family=r.page_no LEFT JOIN journal_snapshot_buckets b ON b.snapshot_id=? AND b.bucket_id=m.bucket_id LEFT JOIN journal_snapshot_work w ON w.snapshot_id=? AND w.bucket_id=m.bucket_id AND w.object_key=m.object_key LEFT JOIN journal_snapshot_current c ON c.storage_id=? AND c.bucket_id=m.bucket_id AND c.object_key=m.object_key WHERE r.snapshot_id=? GROUP BY m.owner,m.family").bind(cut).bind(job).bind(storage).bind(parent)
}

pub(in super::super) fn omit_deletes(job: Uuid, against: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_snapshot_work SET output_row=FALSE WHERE snapshot_id=? AND deleted AND NOT EXISTS(SELECT 1 FROM journal_snapshot_refs r JOIN journal_snapshot_members m ON m.owner=r.owner AND m.family=r.page_no WHERE r.snapshot_id=? AND m.bucket_id=journal_snapshot_work.bucket_id AND m.object_key=journal_snapshot_work.object_key AND m.generation=journal_snapshot_work.generation AND m.seq<journal_snapshot_work.seq AND NOT m.deleted)").bind(job).bind(against)
}

pub(in super::super) fn pages(id: Uuid) -> Rows<'static, Part> {
    sqlx::query_as("SELECT p.owner,p.page_no,p.family,p.bucket_id,p.description,p.etag FROM journal_snapshot_refs r JOIN journal_snapshot_pages p ON p.owner=r.owner AND p.page_no=r.page_no WHERE r.snapshot_id=? ORDER BY r.position_no").bind(id)
}

pub(in super::super) fn reference(
    id: Uuid,
    pos: i64,
    owner: Uuid,
    page: i64,
) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO journal_snapshot_refs(snapshot_id,position_no,owner,page_no) VALUES(?,?,?,?)",
    )
    .bind(id)
    .bind(pos)
    .bind(owner)
    .bind(page)
}

pub(in super::super) fn items<'a>(
    job: Uuid,
    bucket: Uuid,
    after: Option<&'a str>,
    again: Option<&'a str>,
    lim: i64,
) -> Rows<'a, Item> {
    sqlx::query_as("SELECT bucket_id,generation,seq,CAST(object_key AS TEXT) AS object_key,deleted FROM journal_snapshot_work WHERE snapshot_id=? AND bucket_id=? AND (? IS NULL OR object_key>?) AND output_row ORDER BY object_key LIMIT ?").bind(job).bind(bucket).bind(after.map(str::as_bytes)).bind(again.map(str::as_bytes)).bind(lim)
}

pub(in super::super) fn inputs(
    bucket: Uuid,
    seq: i64,
    page: i32,
    bucket2: Uuid,
    seq2: i64,
    page2: i32,
) -> Rows<'static, (i32, Vec<u8>)> {
    sqlx::query_as("SELECT page,encrypted FROM journal_drafts WHERE bucket_id=? AND seq=? AND page>? UNION ALL SELECT i.page,i.encrypted FROM journal_snapshot_inputs i WHERE i.bucket_id=? AND i.seq=? AND i.page>? AND NOT EXISTS(SELECT 1 FROM journal_drafts d WHERE d.bucket_id=i.bucket_id AND d.seq=i.seq AND d.page=i.page) ORDER BY page LIMIT 8").bind(bucket).bind(seq).bind(page).bind(bucket2).bind(seq2).bind(page2)
}

pub(in super::super) fn page<'a>(
    owner: Uuid,
    number: i64,
    family: i64,
    bucket: Uuid,
    desc: &'a Page,
    bytes: &'a [u8],
    size: i64,
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_snapshot_pages(owner,page_no,family,bucket_id,description,encrypted,byte_count) VALUES(?,?,?,?,?,?,?)").bind(owner).bind(number).bind(family).bind(bucket).bind(Json(desc)).bind(bytes).bind(size)
}

pub(in super::super) fn member<'a>(
    owner: Uuid,
    family: i64,
    bucket: Uuid,
    generation: i64,
    seq: i64,
    key: &'a str,
    del: bool,
    size: i64,
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_snapshot_members(owner,family,bucket_id,generation,seq,object_key,deleted,encoded_bytes) VALUES(?,?,?,?,?,?,?,?)").bind(owner).bind(family).bind(bucket).bind(generation).bind(seq).bind(key.as_bytes()).bind(del).bind(size)
}

pub(in super::super) fn archive(bucket: Uuid, seq: i64) -> Statement<'static> {
    sqlx::query("INSERT INTO journal_snapshot_inputs(bucket_id,seq,page,encrypted) SELECT bucket_id,seq,page,encrypted FROM journal_drafts WHERE bucket_id=? AND seq=? ON CONFLICT DO NOTHING").bind(bucket).bind(seq)
}

pub(in super::super) fn hold(bucket: Uuid, seq: i64) -> Statement<'static> {
    sqlx::query("INSERT INTO journal_snapshot_holds(bucket_id,seq,storage_id,unit,address,block_key) SELECT bucket_id,seq,storage_id,unit,address,block_key FROM journal_holds WHERE bucket_id=? AND seq=? ON CONFLICT DO NOTHING").bind(bucket).bind(seq)
}

pub(in super::super) fn seal<'a>(manifest: &'a [u8], root: &'a [u8], job: Uuid) -> Statement<'a> {
    sqlx::query("UPDATE journal_snapshots SET manifest=?,root_index=?,manifest_size=?,index_size=?,state='prepared' WHERE id=? AND state='building'").bind(manifest).bind(root).bind(manifest.len() as i64).bind(root.len() as i64).bind(job)
}

pub(in super::super) fn blobs(job: Uuid) -> Rows<'static, (Vec<u8>, Vec<u8>)> {
    sqlx::query_as("SELECT manifest,root_index FROM journal_snapshots WHERE id=?").bind(job)
}

pub(in super::super) fn bytes(owner: Uuid, number: i64) -> Scalar<'static, Vec<u8>> {
    sqlx::query_scalar("SELECT encrypted FROM journal_snapshot_pages WHERE owner=? AND page_no=?")
        .bind(owner)
        .bind(number)
}

pub(in super::super) fn receipt<'a>(
    etag: &'a str,
    version: Option<&'a str>,
    job: Uuid,
    page: i64,
) -> Statement<'a> {
    sqlx::query("UPDATE journal_snapshot_pages SET etag=?,remote_version=?,encrypted=NULL WHERE owner=? AND page_no=?").bind(etag).bind(version).bind(job).bind(page)
}

pub(in super::super) fn manifest<'a>(
    etag: &'a str,
    version: Option<&'a str>,
    job: Uuid,
) -> Statement<'a> {
    sqlx::query("UPDATE journal_snapshots SET manifest_etag=?,manifest_version=? WHERE id=?")
        .bind(etag)
        .bind(version)
        .bind(job)
}

pub(in super::super) fn parent_etag(id: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT index_etag FROM journal_snapshots WHERE id=?").bind(id)
}

pub(in super::super) fn retire(grace: i64, id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_snapshots SET state='retained',gc_after=CAST(unixepoch('subsec')*1000000 AS INTEGER)+1000000*? WHERE id=? AND state='current'").bind(grace).bind(id)
}

pub(in super::super) fn activate<'a>(
    etag: &'a str,
    checkpoint: &'a str,
    version: Option<&'a str>,
    job: Uuid,
) -> Statement<'a> {
    sqlx::query("UPDATE journal_snapshots SET state='current',index_etag=?,checkpoint_etag=?,checkpoint_version=?,manifest=NULL,root_index=NULL,confirmed_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='prepared'").bind(etag).bind(checkpoint).bind(version).bind(job)
}

pub(in super::super) fn remove_values(storage: Uuid, cut: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_snapshot_current WHERE storage_id=? AND NOT EXISTS(SELECT 1 FROM journal_snapshot_buckets b WHERE b.snapshot_id=? AND b.bucket_id=journal_snapshot_current.bucket_id AND b.included AND b.generation=journal_snapshot_current.generation)").bind(storage).bind(cut)
}

pub(in super::super) fn remove_changes(storage: Uuid, cut: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_snapshot_current WHERE storage_id=? AND EXISTS(SELECT 1 FROM journal_snapshot_work w WHERE w.snapshot_id=? AND w.bucket_id=journal_snapshot_current.bucket_id AND w.object_key=journal_snapshot_current.object_key)").bind(storage).bind(cut)
}

pub(in super::super) fn values(storage: Uuid, cut: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO journal_snapshot_current(storage_id,bucket_id,object_key,generation,seq,deleted) SELECT ?,bucket_id,object_key,generation,seq,deleted FROM journal_snapshot_work WHERE snapshot_id=? AND output_row").bind(storage).bind(cut)
}

pub(in super::super) fn dirty(job: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_buckets SET dirty=TRUE WHERE EXISTS(SELECT 1 FROM journal_snapshot_buckets b WHERE b.snapshot_id=? AND b.bucket_id=journal_buckets.bucket_id AND NOT b.included AND b.generation=journal_buckets.generation)").bind(job)
}

pub(in super::super) fn stats(storage: Uuid, job: Uuid) -> Rows<'static, (i64, i64)> {
    sqlx::query_as("SELECT CAST(COALESCE(SUM(CASE WHEN m.deleted AND c.seq=m.seq THEN m.encoded_bytes ELSE 0 END),0) AS INTEGER),CAST(COALESCE(SUM(CASE WHEN c.seq=m.seq THEN 0 ELSE m.encoded_bytes END),0) AS INTEGER) FROM journal_snapshot_refs r JOIN journal_snapshot_members m ON m.owner=r.owner AND m.family=r.page_no LEFT JOIN journal_snapshot_current c ON c.bucket_id=m.bucket_id AND c.object_key=m.object_key AND c.generation=m.generation AND c.storage_id=? WHERE r.snapshot_id=?").bind(storage).bind(job)
}

pub(in super::super) fn settle(
    job: Uuid,
    rev: i64,
    deleted: i64,
    invalid: i64,
    threshold: i64,
    covered: i64,
    bytes: i64,
    storage: Uuid,
) -> Statement<'static> {
    sqlx::query("UPDATE journal_stores SET snapshot_gc_error=NULL,snapshot_id=?,snapshot_revision=?,snapshot_delete_bytes=?,snapshot_invalid_bytes=?,snapshot_auto_due=snapshot_log_bytes>=?+?,snapshot_log_bytes=snapshot_log_bytes-? WHERE storage_id=?").bind(job).bind(rev).bind(deleted).bind(invalid).bind(threshold).bind(covered).bind(bytes).bind(storage)
}

pub(in super::super) fn failed<'a>(retryable: bool, error: &'a str, id: Uuid) -> Statement<'a> {
    sqlx::query("UPDATE journal_snapshots SET attempts=attempts+1,retry_after=CASE WHEN ? THEN CAST(unixepoch('subsec')*1000000 AS INTEGER)+15000000*MIN(attempts+1,20) END,state='failed',error_detail=? WHERE id=?").bind(retryable).bind(error).bind(id)
}

pub(in super::super) fn status(
    storage: Uuid,
) -> Rows<'static, (Option<Uuid>, i64, i64, i64, i64, i64, i64, i64, i64)> {
    sqlx::query_as("SELECT s.snapshot_id,s.snapshot_revision,s.snapshot_log_bytes,s.snapshot_delete_bytes,s.snapshot_invalid_bytes,CAST(COALESCE((SELECT SUM(g.byte_count) FROM journal_snapshot_gc g WHERE g.storage_id=s.storage_id),0) AS INTEGER),CAST((SELECT COUNT(*) FROM journal_snapshot_refs r WHERE r.snapshot_id=s.snapshot_id) AS INTEGER),CAST((SELECT COUNT(*) FROM journal_entries e JOIN journal_buckets b ON b.bucket_id=e.bucket_id WHERE b.storage_id=s.storage_id) AS INTEGER),CAST((SELECT COUNT(*) FROM journal_snapshot_refs r JOIN journal_snapshot_members m ON m.owner=r.owner AND m.family=r.page_no WHERE r.snapshot_id=s.snapshot_id AND m.deleted) AS INTEGER) FROM journal_stores s WHERE s.storage_id=?").bind(storage)
}

pub(in super::super) fn segments(
    storage: Uuid,
    epoch: Uuid,
    h: i64,
) -> Rows<'static, (Uuid, i64, String, Option<String>, i64)> {
    sqlx::query_as("SELECT id,first_seq,etag,object_version,byte_count FROM journal_files WHERE storage_id=? AND epoch=? AND last_seq<=? AND state='published' ORDER BY first_seq").bind(storage).bind(epoch).bind(h)
}

pub(in super::super) fn log_gc(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_files SET state='gc' WHERE id=? AND state='published'").bind(id)
}

pub(in super::super) fn queue_gc<'a>(
    storage: Uuid,
    id: Uuid,
    page: i64,
    kind: &'a str,
    path: &'a str,
    etag: &'a str,
    version: Option<&'a str>,
    size: i64,
    grace: i64,
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_snapshot_gc(storage_id,object_id,page_no,kind,file_path,etag,remote_version,byte_count,reclaim_at) VALUES(?,?,?,?,?,?,?,?,CAST(unixepoch('subsec')*1000000 AS INTEGER)+1000000*?) ON CONFLICT DO NOTHING").bind(storage).bind(id).bind(page).bind(kind).bind(path).bind(etag).bind(version).bind(size).bind(grace)
}

pub(in super::super) fn pinned(storage: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM journal_recoveries WHERE source_storage=? AND state IN ('preview','queued','verifying','claiming','importing','failed'))").bind(storage)
}

pub(in super::super) fn retained(
    storage: Uuid,
) -> Rows<
    'static,
    (
        Uuid,
        String,
        Option<String>,
        i64,
        Uuid,
        i64,
        String,
        Option<String>,
        i64,
    ),
> {
    sqlx::query_as("SELECT id,manifest_etag,manifest_version,manifest_size,epoch,revision,checkpoint_etag,checkpoint_version,index_size FROM journal_snapshots WHERE storage_id=? AND state='retained' AND gc_after<=CAST(unixepoch('subsec')*1000000 AS INTEGER) ORDER BY revision LIMIT 8").bind(storage)
}

pub(in super::super) fn gone(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_snapshots SET state='gone' WHERE id=? AND state='retained'")
        .bind(id)
}

pub(in super::super) fn unreferenced(
    storage: Uuid,
) -> Rows<'static, (Uuid, i64, String, Option<String>, i64)> {
    sqlx::query_as("SELECT p.owner,p.page_no,p.etag,p.remote_version,p.byte_count FROM journal_snapshot_pages p JOIN journal_snapshots j ON j.id=p.owner WHERE j.storage_id=? AND p.gc_state='live' AND p.etag IS NOT NULL AND NOT EXISTS(SELECT 1 FROM journal_snapshot_refs r WHERE r.owner=p.owner AND r.page_no=p.page_no) ORDER BY p.owner,p.page_no LIMIT 64").bind(storage)
}

pub(in super::super) fn page_pending(id: Uuid, page: i64) -> Statement<'static> {
    sqlx::query("UPDATE journal_snapshot_pages SET gc_state='pending' WHERE owner=? AND page_no=? AND gc_state='live'").bind(id).bind(page)
}

pub(in super::super) fn gc(storage: Uuid) -> Rows<'static, Garbage> {
    sqlx::query_as("SELECT storage_id,object_id,page_no,kind,file_path,etag,remote_version,byte_count FROM journal_snapshot_gc WHERE storage_id=? AND reclaim_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER) ORDER BY reclaim_at,object_id,page_no LIMIT 32").bind(storage)
}

pub(in super::super) fn log_gone(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_files SET state='gone' WHERE id=? AND state='gc'").bind(id)
}

pub(in super::super) fn remove_parts(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_parts WHERE file_id=?").bind(id)
}

pub(in super::super) fn remove_file_entries(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_file_entries WHERE file_id=?").bind(id)
}

pub(in super::super) fn drop_drafts(storage: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_drafts WHERE EXISTS(SELECT 1 FROM journal_snapshot_buckets b JOIN journal_stores s ON s.snapshot_id=b.snapshot_id WHERE s.storage_id=? AND b.bucket_id=journal_drafts.bucket_id AND b.watermark>=journal_drafts.seq) AND NOT EXISTS(SELECT 1 FROM journal_file_entries m WHERE m.bucket_id=journal_drafts.bucket_id AND m.seq=journal_drafts.seq)").bind(storage)
}

pub(in super::super) fn drop_holds(storage: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_holds WHERE EXISTS(SELECT 1 FROM journal_snapshot_buckets b JOIN journal_stores s ON s.snapshot_id=b.snapshot_id WHERE s.storage_id=? AND b.bucket_id=journal_holds.bucket_id AND b.watermark>=journal_holds.seq) AND NOT EXISTS(SELECT 1 FROM journal_file_entries m WHERE m.bucket_id=journal_holds.bucket_id AND m.seq=journal_holds.seq)").bind(storage)
}

pub(in super::super) fn drop_entries(storage: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_entries WHERE EXISTS(SELECT 1 FROM journal_snapshot_buckets b JOIN journal_stores s ON s.snapshot_id=b.snapshot_id WHERE s.storage_id=? AND b.bucket_id=journal_entries.bucket_id AND b.watermark>=journal_entries.seq) AND NOT EXISTS(SELECT 1 FROM journal_file_entries m WHERE m.bucket_id=journal_entries.bucket_id AND m.seq=journal_entries.seq)").bind(storage)
}

pub(in super::super) fn page_gone(id: Uuid, page: i64) -> Statement<'static> {
    sqlx::query("UPDATE journal_snapshot_pages SET gc_state='gone' WHERE owner=? AND page_no=?")
        .bind(id)
        .bind(page)
}

pub(in super::super) fn drop_members(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_snapshot_members WHERE owner=? AND NOT EXISTS(SELECT 1 FROM journal_snapshot_pages p WHERE p.owner=journal_snapshot_members.owner AND p.family=journal_snapshot_members.family AND p.gc_state<>'gone')").bind(id)
}

pub(in super::super) fn drop_inputs(storage: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_snapshot_inputs WHERE EXISTS(SELECT 1 FROM journal_buckets b WHERE b.storage_id=? AND b.bucket_id=journal_snapshot_inputs.bucket_id) AND NOT EXISTS(SELECT 1 FROM journal_snapshot_members m WHERE m.bucket_id=journal_snapshot_inputs.bucket_id AND m.seq=journal_snapshot_inputs.seq)").bind(storage)
}

pub(in super::super) fn drop_snap_holds(storage: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_snapshot_holds WHERE EXISTS(SELECT 1 FROM journal_buckets b WHERE b.storage_id=? AND b.bucket_id=journal_snapshot_holds.bucket_id) AND NOT EXISTS(SELECT 1 FROM journal_snapshot_members m WHERE m.bucket_id=journal_snapshot_holds.bucket_id AND m.seq=journal_snapshot_holds.seq)").bind(storage)
}

pub(in super::super) fn dequeue<'a>(s: Uuid, id: Uuid, k: &'a str, p: i64) -> Statement<'a> {
    sqlx::query("DELETE FROM journal_snapshot_gc WHERE storage_id=? AND object_id=? AND kind=? AND page_no=?").bind(s).bind(id).bind(k).bind(p)
}
pub(in super::super) fn bucket_log(
    id: Uuid,
    again: Uuid,
    sign: i64,
    start: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE journal_buckets SET confirmed_log_bytes=confirmed_log_bytes+COALESCE((SELECT SUM(m.encoded_bytes) FROM journal_file_entries m WHERE m.bucket_id=journal_buckets.bucket_id AND m.file_id=? AND m.byte_offset>=?),0)*? WHERE bucket_id IN(SELECT bucket_id FROM journal_file_entries WHERE file_id=?)").bind(id).bind(start).bind(sign).bind(again)
}
pub(in super::super) fn clear_usage(storage: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_snapshot_usage WHERE storage_id=?").bind(storage)
}
pub(in super::super) fn usage(storage: Uuid, again: Uuid, job: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO journal_snapshot_usage(storage_id,bucket_id,page_bytes,invalid_bytes) SELECT ?,m.bucket_id,SUM(m.encoded_bytes),SUM(CASE WHEN c.seq=m.seq THEN 0 ELSE m.encoded_bytes END) FROM journal_snapshot_refs r JOIN journal_snapshot_members m ON m.owner=r.owner AND m.family=r.page_no LEFT JOIN journal_snapshot_current c ON c.storage_id=? AND c.bucket_id=m.bucket_id AND c.object_key=m.object_key AND c.generation=m.generation WHERE r.snapshot_id=? GROUP BY m.bucket_id").bind(storage).bind(again).bind(job)
}
pub(in super::super) fn waste(storage: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT CAST(COALESCE(SUM(CASE WHEN COALESCE(g.journal_mode,0)=1 THEN COALESCE(u.invalid_bytes,0) ELSE b.confirmed_log_bytes+COALESCE(u.page_bytes,0) END),0) AS INTEGER) FROM journal_buckets b LEFT JOIN journal_snapshot_usage u ON u.storage_id=b.storage_id AND u.bucket_id=b.bucket_id LEFT JOIN bucket_change_gates g ON g.bucket_id=b.bucket_id WHERE b.storage_id=?").bind(storage)
}
pub(in super::super) fn retry_due(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT retry_after IS NOT NULL AND retry_after<=CAST(unixepoch('subsec')*1000000 AS INTEGER) FROM journal_snapshots WHERE id=?").bind(id)
}
pub(in super::super) fn gc_failure<'a>(
    error: &'a str,
    storage: Uuid,
    token: Uuid,
) -> Statement<'a> {
    sqlx::query(
        "UPDATE journal_stores SET snapshot_gc_error=? WHERE storage_id=? AND snapshot_token=?",
    )
    .bind(error)
    .bind(storage)
    .bind(token)
}
pub(in super::super) fn gc_error(storage: Uuid) -> Scalar<'static, Option<String>> {
    sqlx::query_scalar("SELECT snapshot_gc_error FROM journal_stores WHERE storage_id=?")
        .bind(storage)
}
pub(in super::super) fn abandoned(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_snapshots SET state='gone',manifest=NULL,root_index=NULL WHERE id=? AND state='prepared'").bind(id)
}
pub(in super::super) fn abandoned_manifest(
    id: Uuid,
) -> Rows<'static, (String, Option<String>, i64)> {
    sqlx::query_as("SELECT manifest_etag,manifest_version,manifest_size FROM journal_snapshots WHERE id=? AND manifest_etag IS NOT NULL").bind(id)
}
pub(in super::super) fn reopened(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT b.bucket_id FROM journal_snapshot_buckets b JOIN bucket_change_gates g ON g.bucket_id=b.bucket_id JOIN journal_buckets j ON j.bucket_id=b.bucket_id WHERE b.snapshot_id=? AND NOT b.included AND b.generation=j.generation AND g.journal_mode=1 ORDER BY b.bucket_id").bind(id)
}
pub(in super::super) fn prune_pages(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_snapshot_pages WHERE owner=? AND gc_state='gone' AND NOT EXISTS(SELECT 1 FROM journal_snapshot_members m WHERE m.owner=journal_snapshot_pages.owner AND m.family=journal_snapshot_pages.page_no)").bind(id)
}
pub(in super::super) fn prune_log(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_files WHERE id=? AND state='gone' AND NOT EXISTS(SELECT 1 FROM journal_dictionary d WHERE d.first_file=journal_files.id)").bind(id)
}
pub(in super::super) fn prune_job(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_snapshots WHERE id=? AND state='gone' AND NOT EXISTS(SELECT 1 FROM journal_snapshot_pages p WHERE p.owner=journal_snapshots.id) AND NOT EXISTS(SELECT 1 FROM journal_snapshot_gc g WHERE g.object_id=journal_snapshots.id) AND NOT EXISTS(SELECT 1 FROM journal_snapshot_buckets b WHERE b.snapshot_id=journal_snapshots.id)").bind(id)
}
