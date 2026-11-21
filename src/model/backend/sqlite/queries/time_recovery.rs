pub(in super::super) use super::history::Tx;
use super::*;
use crate::model::time_recovery::Item;

pub(in super::super) fn now() -> Scalar<'static, i64> {
    sqlx::query_scalar(r#"SELECT CAST(unixepoch('subsec')*1000000 AS INTEGER)"#)
}

pub(in super::super) fn window(b: Uuid) -> Rows<'static, (i64, i64, i64, i64)> {
    sqlx::query_as(
        r#"SELECT seconds,floor_us,last_us,seq FROM bucket_time_windows WHERE bucket_id=?"#,
    )
    .bind(b)
}

pub(in super::super) fn lock_window(b: Uuid) -> Rows<'static, (i64, i64, i64, i64)> {
    sqlx::query_as(
        r#"SELECT seconds,floor_us,last_us,seq FROM bucket_time_windows WHERE bucket_id=?"#,
    )
    .bind(b)
}

pub(in super::super) fn start(b: Uuid, n: i64, t: i64, t2: i64) -> Statement<'static> {
    sqlx::query(r#"INSERT INTO bucket_time_windows(bucket_id,seconds,floor_us,last_us,gaps,seq) VALUES(?,?,?,?,'[]',0)"#).bind(b).bind(n).bind(t).bind(t2)
}

pub(in super::super) fn baseline(t: i64, b: Uuid) -> Statement<'static> {
    sqlx::query(r#"INSERT INTO object_time_points(bucket_id,object_key,seq,seen_us,stream_id) SELECT bucket_id,object_key,0,?,stream_id FROM objects WHERE bucket_id=? AND stream_id IS NOT NULL"#).bind(t).bind(b)
}

pub(in super::super) fn resize(n: i64, t: i64, b: Uuid) -> Statement<'static> {
    sqlx::query(r#"UPDATE bucket_time_windows SET seconds=?,floor_us=? WHERE bucket_id=?"#)
        .bind(n)
        .bind(t)
        .bind(b)
}

pub(in super::super) fn close(b: Uuid) -> Statement<'static> {
    sqlx::query(r#"DELETE FROM bucket_time_windows WHERE bucket_id=?"#).bind(b)
}

pub(in super::super) fn retained(b: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar(r#"SELECT CAST(COALESCE(SUM(bytes),0) AS BIGINT) FROM time_retained_streams WHERE bucket_id=?"#).bind(b)
}

pub(in super::super) fn free_retained(b: Uuid) -> Statement<'static> {
    sqlx::query(r#"DELETE FROM time_retained_streams WHERE bucket_id=? AND NOT EXISTS(SELECT 1 FROM object_time_points t WHERE t.bucket_id=time_retained_streams.bucket_id AND t.stream_id=time_retained_streams.stream_id) AND NOT EXISTS(SELECT 1 FROM time_restore_items i JOIN time_restore_plans p ON p.id=i.plan_id WHERE p.source_bucket=time_retained_streams.bucket_id AND i.source_stream=time_retained_streams.stream_id)"#).bind(b)
}

pub(in super::super) fn cut(b: Uuid, t: i64) -> Scalar<'static, i64> {
    sqlx::query_scalar(
        r#"SELECT COALESCE(MAX(seq),0) FROM object_time_points WHERE bucket_id=? AND seen_us<=?"#,
    )
    .bind(b)
    .bind(t)
}

pub(in super::super) fn due_window() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        r#"SELECT bucket_id FROM bucket_time_windows WHERE floor_us < CAST(unixepoch('subsec')*1000000 AS INTEGER)-seconds*1000000-1000000 ORDER BY floor_us LIMIT 1"#,
    )
}

pub(in super::super) fn stale_points(b: Uuid, t: i64, t2: i64) -> Rows<'static, (String, i64)> {
    sqlx::query_as(r#"SELECT CAST(p.object_key AS TEXT),p.seq FROM object_time_points p WHERE p.bucket_id=? AND p.seen_us<=? AND EXISTS(SELECT 1 FROM object_time_points z WHERE z.bucket_id=p.bucket_id AND z.object_key=p.object_key AND z.seq>p.seq AND z.seen_us<=?) ORDER BY p.seen_us,p.seq LIMIT 256"#).bind(b).bind(t).bind(t2)
}

pub(in super::super) fn erase<'a>(b: Uuid, k: &'a str, n: i64) -> Statement<'a> {
    sqlx::query(r#"DELETE FROM object_time_points WHERE bucket_id=? AND object_key=? AND seq=?"#)
        .bind(b)
        .bind(k.as_bytes())
        .bind(n)
}

pub(in super::super) fn release_plan(id: Uuid) -> Statement<'static> {
    sqlx::query(r#"UPDATE time_restore_items SET source_stream=NULL WHERE plan_id=?"#).bind(id)
}

pub(in super::super) fn expire(id: Uuid) -> Statement<'static> {
    sqlx::query(r#"UPDATE time_restore_plans SET phase='expired' WHERE id=? AND phase IN ('preview','running')"#).bind(id)
}

pub(in super::super) fn expired() -> Rows<'static, (Uuid, Uuid)> {
    sqlx::query_as(
        r#"SELECT id,source_bucket FROM time_restore_plans WHERE phase='preview' AND expires_us<CAST(unixepoch('subsec')*1000000 AS INTEGER) ORDER BY expires_us LIMIT 32"#,
    )
}

pub(in super::super) fn plan(
    id: Uuid,
) -> Rows<'static, (Uuid, Uuid, String, String, String, i64, i64, String)> {
    sqlx::query_as(r#"SELECT source_bucket,target_bucket,request_json,owner_json,digest,cut_seq,expires_us,phase FROM time_restore_plans WHERE id=?"#).bind(id)
}

pub(in super::super) fn insert_plan<'a>(
    id: Uuid,
    b: Uuid,
    target: Uuid,
    request: &'a str,
    owner: &'a str,
    cut: i64,
    t: i64,
    expires: i64,
) -> Statement<'a> {
    sqlx::query(r#"INSERT INTO time_restore_plans(id,source_bucket,target_bucket,request_json,owner_json,digest,cut_seq,at_us,expires_us,phase) VALUES(?,?,?,?,?,'',?,?,?,'preview')"#).bind(id).bind(b).bind(target).bind(request).bind(owner).bind(cut).bind(t).bind(expires)
}

pub(in super::super) fn digest<'a>(v: &'a str, id: Uuid) -> Statement<'a> {
    sqlx::query(r#"UPDATE time_restore_plans SET digest=? WHERE id=?"#)
        .bind(v)
        .bind(id)
}

pub(in super::super) fn run(id: Uuid) -> Statement<'static> {
    sqlx::query(r#"UPDATE time_restore_plans SET phase='running' WHERE id=? AND phase='preview'"#)
        .bind(id)
}

pub(in super::super) fn complete(id: Uuid) -> Statement<'static> {
    sqlx::query(r#"UPDATE time_restore_plans SET phase='done' WHERE id=? AND phase='running'"#)
        .bind(id)
}

pub(in super::super) fn active() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        r#"SELECT id FROM time_restore_plans WHERE phase='running' AND lease_until_us<=CAST(unixepoch('subsec')*1000000 AS INTEGER) ORDER BY expires_us LIMIT 1"#,
    )
}

pub(in super::super) fn counts(id: Uuid) -> Rows<'static, (String, i64)> {
    sqlx::query_as(r#"SELECT item_state,CAST(COUNT(*) AS BIGINT) FROM time_restore_items WHERE plan_id=? GROUP BY item_state"#).bind(id)
}

pub(in super::super) fn items<'a>(id: Uuid, after: &'a str) -> Rows<'a, Item> {
    sqlx::query_as(r#"SELECT CAST(object_key AS TEXT) AS "key",CAST(target_key AS TEXT) AS target,item_state AS state,source_stream AS source,expected_stream AS expected,target_changed FROM time_restore_items WHERE plan_id=? AND object_key>? ORDER BY object_key LIMIT 51"#).bind(id).bind(after.as_bytes())
}

pub(in super::super) fn item<'a>(id: Uuid, k: &'a str) -> Rows<'a, Item> {
    sqlx::query_as(r#"SELECT CAST(object_key AS TEXT) AS "key",CAST(target_key AS TEXT) AS target,item_state AS state,source_stream AS source,expected_stream AS expected,target_changed FROM time_restore_items WHERE plan_id=? AND object_key=?"#).bind(id).bind(k.as_bytes())
}

pub(in super::super) fn next(id: Uuid) -> Rows<'static, Item> {
    sqlx::query_as(r#"SELECT CAST(object_key AS TEXT) AS "key",CAST(target_key AS TEXT) AS target,item_state AS state,source_stream AS source,expected_stream AS expected,target_changed FROM time_restore_items WHERE plan_id=? AND item_state IN ('add','replace','remove') ORDER BY object_key LIMIT 1"#).bind(id)
}

pub(in super::super) fn insert_item<'a>(
    id: Uuid,
    k: &'a str,
    target: &'a str,
    source: Option<Id>,
    expected: Option<Id>,
    state: &'a str,
) -> Statement<'a> {
    sqlx::query(r#"INSERT INTO time_restore_items(plan_id,object_key,target_key,source_stream,expected_stream,item_state) VALUES(?,?,?,?,?,?)"#).bind(id).bind(k.as_bytes()).bind(target.as_bytes()).bind(source).bind(expected).bind(state)
}

pub(in super::super) fn finish_item<'a>(state: &'a str, id: Uuid, k: &'a str) -> Statement<'a> {
    sqlx::query(r#"UPDATE time_restore_items SET item_state=? WHERE plan_id=? AND object_key=? AND item_state IN ('add','replace','remove')"#).bind(state).bind(id).bind(k.as_bytes())
}

pub(in super::super) fn target<'a>(b: Uuid, k: &'a str) -> Scalar<'a, Option<Id>> {
    sqlx::query_scalar(r#"SELECT stream_id FROM objects WHERE bucket_id=? AND object_key=?"#)
        .bind(b)
        .bind(k.as_bytes())
}

pub(in super::super) fn quota_ok(b: Uuid, b2: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar(r#"SELECT NOT EXISTS(SELECT 1 FROM quota_scopes WHERE ((scope='bucket' AND scope_id=?) OR (scope='project' AND scope_id=(SELECT project_id FROM published_buckets WHERE id=?))) AND logical_bytes_limit IS NOT NULL AND used_bytes+reserved_bytes>logical_bytes_limit)"#).bind(b).bind(b2)
}

pub(in super::super) fn scan<'a>(
    cut: i64,
    b: Uuid,
    after: &'a str,
    prefix: &'a str,
    prefix2: &'a str,
) -> Rows<'a, (String, Option<Id>)> {
    sqlx::query_as(r#"SELECT CAST(p.object_key AS TEXT),(SELECT v.stream_id FROM object_time_points v WHERE v.bucket_id=p.bucket_id AND v.object_key=p.object_key AND v.seq<=? ORDER BY v.seq DESC LIMIT 1) FROM object_time_points p WHERE p.bucket_id=? AND p.object_key>? AND substr(p.object_key,1,length(?))=? GROUP BY p.bucket_id,p.object_key ORDER BY p.object_key LIMIT 128"#).bind(cut).bind(b).bind(after.as_bytes()).bind(prefix.as_bytes()).bind(prefix2.as_bytes())
}

pub(in super::super) async fn admin_check(tx: &mut Tx<'_>, p: &Principal) -> Result<()> {
    anyhow::ensure!(
        super::validate_identity(p, tx).await?,
        s3s::s3_error!(AccessDenied)
    );
    Ok(())
}

pub(in super::super) fn journal_window(b: Uuid, closed: bool) -> Statement<'static> {
    sqlx::query(if closed { "INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq+1,j.generation,'delete',CAST('~w' AS BLOB),NULL,NULL,NULL,TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id WHERE j.bucket_id=? AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=j.bucket_id)=1" } else { "INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq+1,j.generation,'put',CAST('~w' AS BLOB),NULL,NULL,json_object('size',0,'etag','','metadata',json_object(),'checksums',json_object(),'payload','chunks','modified_us',0,'history',json_object('seconds',b.seconds,'floor',b.floor_us,'gaps',json(b.gaps))),TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id JOIN bucket_time_windows b ON b.bucket_id=j.bucket_id WHERE b.bucket_id=? AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=j.bucket_id)=1" }).bind(b)
}

pub(in super::super) fn dirty_window(b: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=? AND EXISTS(SELECT 1 FROM bucket_change_gates g WHERE g.bucket_id=journal_buckets.bucket_id AND g.journal_mode=2)").bind(b)
}

pub(in super::super) fn clear_points(b: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM object_time_points WHERE bucket_id=?").bind(b)
}

pub(in super::super) fn missing(stream: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar(r#"SELECT NOT EXISTS (
 SELECT 1 FROM streams s WHERE s.id=? AND s.state IN ('ready','retired') AND
 CASE WHEN s.payload_type='raw' THEN
   (SELECT COALESCE(SUM(r.length),0) FROM raw_ranges r JOIN raw_sources src ON src.id=r.source_id
    WHERE r.stream_id=s.id AND src.state='ready' AND src.quarantined_at IS NULL)=s.size
 ELSE
   (SELECT COALESCE(SUM(e.length),0) FROM extents e WHERE e.stream_id=s.id AND (
    EXISTS(SELECT 1 FROM fragments f WHERE f.id=e.fragment_id AND f.sealed) OR
    EXISTS(SELECT 1 FROM chunks c WHERE c.id=e.chunk_id AND c.state='ready' AND (
      EXISTS(SELECT 1 FROM chunk_encodings enc WHERE enc.chunk_id=c.id AND enc.state='ready' AND enc.quarantined_at IS NULL) OR
      EXISTS(SELECT 1 FROM packs p JOIN pack_members m ON m.pack_id=p.id WHERE p.id=c.pack_id AND m.chunk_id=c.id AND p.state='ready' AND p.quarantined_at IS NULL) OR
      EXISTS(SELECT 1 FROM pending_uploads u WHERE u.chunk_id=c.id)))))=s.size
 END)"#).bind(stream)
}

pub(in super::super) fn seal_read(t: i64, b: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE bucket_time_windows SET last_us=? WHERE bucket_id=?")
        .bind(t)
        .bind(b)
}

pub(in super::super) fn gaps(b: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT gaps FROM bucket_time_windows WHERE bucket_id=?").bind(b)
}

pub(in super::super) fn store_gaps(v: &str, t: i64, b: Uuid) -> Statement<'_> {
    sqlx::query("UPDATE bucket_time_windows SET gaps=?,last_us=? WHERE bucket_id=?")
        .bind(v)
        .bind(t)
        .bind(b)
}

pub(in super::super) fn lease(token: Uuid, id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE time_restore_plans SET worker_token=?,lease_until_us=CAST(unixepoch('subsec')*1000000 AS INTEGER)+30000000 WHERE id=? AND phase='running' AND lease_until_us<=CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(token).bind(id)
}

pub(in super::super) fn renew(id: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE time_restore_plans SET lease_until_us=CAST(unixepoch('subsec')*1000000 AS INTEGER)+30000000 WHERE id=? AND worker_token=? AND phase='running'").bind(id).bind(token)
}

pub(in super::super) fn release(id: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE time_restore_plans SET worker_token=NULL,lease_until_us=0 WHERE id=? AND worker_token=?").bind(id).bind(token)
}

pub(in super::super) fn owns(id: Uuid, token: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM time_restore_plans WHERE id=? AND worker_token=? AND phase='running')").bind(id).bind(token)
}
