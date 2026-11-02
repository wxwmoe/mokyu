use super::packing::*;
use super::*;
pub(in super::super) async fn ready_encoding(
    db: impl sqlx::SqliteExecutor<'_>,
    chunk: Id,
) -> Result<Option<(Id, Uuid, i32)>> {
    Ok(sqlx::query_as("SELECT encoding_id,location_id,standalone_size FROM chunk_encodings WHERE chunk_id=? AND state='ready'").bind(chunk).fetch_optional(db).await?)
}
pub(in super::super) async fn pack_has_pending(
    db: impl sqlx::SqliteExecutor<'_>,
    pack: Id,
) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE source_pack_id=?)")
            .bind(pack)
            .fetch_one(db)
            .await?,
    )
}
pub(in super::super) async fn pack_layout(
    db: impl sqlx::SqliteExecutor<'_>,
    pack: Id,
) -> Result<Vec<Mapping>> {
    Ok(super::packing::pack_layout(pack, false)
        .build_query_as()
        .fetch_all(db)
        .await?)
}
pub(in super::super) async fn pack_has_writers(
    db: impl sqlx::SqliteExecutor<'_>,
    pack: Id,
) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunks c JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id WHERE c.pack_id=? AND s.state='writing')").bind(pack).fetch_one(db).await?)
}
pub(in super::super) async fn pack_raw_size(
    db: impl sqlx::SqliteExecutor<'_>,
    pack: Id,
) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT raw_size FROM packs WHERE id=? AND state='ready'")
            .bind(pack)
            .fetch_one(db)
            .await?,
    )
}
pub(in super::super) async fn chunk_has_references(
    db: impl sqlx::SqliteExecutor<'_>,
    chunk: Id,
) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents WHERE chunk_id=?)")
            .bind(chunk)
            .fetch_one(db)
            .await?,
    )
}
pub(in super::super) async fn pack_stored_size(
    db: impl sqlx::SqliteExecutor<'_>,
    pack: Id,
) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT stored_size FROM packs WHERE id=?")
            .bind(pack)
            .fetch_one(db)
            .await?,
    )
}
pub(in super::super) async fn pack_candidates(
    db: impl sqlx::SqliteExecutor<'_>,
    stream: Id,
    offset: i64,
    cooldown: f64,
) -> Result<Vec<PackCandidate>> {
    let mut query = sqlx::QueryBuilder::<Sqlite>::new("SELECT ");
    query.push(CHUNK_COLUMNS).push(",e.offset_bytes,c.pack_id,(NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings q WHERE q.chunk_id=c.id AND q.state='ready' AND q.quarantined_at IS NOT NULL) AND NOT EXISTS(SELECT 1 FROM packs q WHERE q.id=c.pack_id AND q.quarantined_at IS NOT NULL) AND e.source_offset=0 AND e.length=c.raw_size AND (SELECT COUNT(*) FROM extents WHERE chunk_id=c.id)=1 AND COALESCE(c.repack_after<=CAST(unixepoch('subsec')*1000000 AS INTEGER),TRUE) AND (c.split_at IS NULL OR c.reference_changed_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER)-").push_bind((cooldown * 1_000_000.) as i64).push(")) AS eligible,CAST(IIF(c.pack_id IS NULL,1,(SELECT COUNT(*) FROM chunks WHERE pack_id=c.pack_id AND state='ready')) AS INTEGER) AS unit_count FROM extents e JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' JOIN streams s ON s.id=e.stream_id WHERE e.stream_id=").push_bind(stream).push(" AND e.offset_bytes>=").push_bind(offset).push(" AND s.state='ready' AND c.state='ready' AND EXISTS(SELECT 1 FROM objects WHERE stream_id=s.id) ORDER BY e.offset_bytes LIMIT 1026");
    Ok(query
        .build_query_as::<PackCandidateRow>()
        .fetch_all(db)
        .await?
        .into_iter()
        .map(|r| PackCandidate {
            row: r.row,
            eligible: r.eligible,
            unit_count: r.unit_count,
        })
        .collect())
}
pub(in super::super) async fn pack_member_count(
    db: impl sqlx::SqliteExecutor<'_>,
    pack: Id,
) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE pack_id=? AND state='ready'")
            .bind(pack)
            .fetch_one(db)
            .await?,
    )
}
pub(in super::super) async fn physical_bytes(
    db: impl sqlx::SqliteExecutor<'_>,
    ids: &[Id],
) -> Result<i64> {
    Ok(pack_layout_bytes(ids)
        .build_query_scalar()
        .fetch_one(db)
        .await?)
}
pub(in super::super) async fn next_pack(
    db: impl sqlx::SqliteExecutor<'_>,
    after: Id,
    pack: Option<Id>,
    upper: Option<DateTime<Utc>>,
    storage: Option<Uuid>,
) -> Result<Option<Id>> {
    Ok(sqlx::query_scalar("SELECT id FROM packs WHERE state='ready' AND id>? AND (? IS NULL OR id=?) AND (? IS NULL OR storage_id=?) AND created_at<=COALESCE(?,CAST(unixepoch('subsec')*1000000 AS INTEGER)) ORDER BY id LIMIT 1").bind(after).bind(pack).bind(pack).bind(storage).bind(storage).bind(upper.map(|time|time.timestamp_micros())).fetch_optional(db).await?)
}
pub(in super::super) async fn next_pack_stream(
    db: impl sqlx::SqliteExecutor<'_>,
    after: Id,
    offset: i64,
    upper: DateTime<Utc>,
    storage: Option<Uuid>,
) -> Result<Option<Id>> {
    Ok(sqlx::query_scalar("SELECT s.id FROM streams s JOIN objects o ON o.stream_id=s.id JOIN buckets b ON b.id=o.bucket_id WHERE s.state='ready' AND (s.id>? OR(s.id=? AND ?>=0)) AND (? IS NULL OR b.storage_id=?) AND s.created_at<=? ORDER BY s.id LIMIT 1").bind(after).bind(after).bind(offset).bind(storage).bind(storage).bind(upper.timestamp_micros()).fetch_optional(db).await?)
}
pub(in super::super) async fn next_pack_change(
    db: impl sqlx::SqliteExecutor<'_>,
    cutoff: DateTime<Utc>,
    policy: &str,
    storage: Option<Uuid>,
) -> Result<Option<(Id, i64)>> {
    Ok(sqlx::query_as("SELECT pc.pack_id,pc.generation FROM pack_changes pc JOIN packs p ON p.id=pc.pack_id WHERE pc.next_check_at<=? AND pc.reason=? AND (? IS NULL OR p.storage_id=?) ORDER BY pc.next_check_at,pc.pack_id LIMIT 1").bind(cutoff.timestamp_micros()).bind(policy).bind(storage).bind(storage).fetch_optional(db).await?)
}
pub(crate) async fn finish_pack_change(
    db: &mut sqlx::SqliteConnection,
    task: Execution,
    pack: Id,
    generation: i64,
) -> Result<()> {
    transactions::lock_execution(db, task).await?;
    super::finish_pack_change(pack, generation)
        .execute(db)
        .await?;
    Ok(())
}
pub(in super::super) async fn next_pack_queue(
    db: impl sqlx::SqliteExecutor<'_>,
    cutoff: DateTime<Utc>,
    policy: &str,
    storage: Option<Uuid>,
) -> Result<Option<(Id, i64, i64)>> {
    Ok(sqlx::query_as("SELECT pm.stream_id,pm.progress_cursor,pm.generation FROM pack_maintenance pm JOIN streams s ON s.id=pm.stream_id JOIN objects o ON o.stream_id=s.id JOIN buckets b ON b.id=o.bucket_id WHERE pm.next_check_at<=? AND pm.reason=? AND (? IS NULL OR b.storage_id=?) ORDER BY pm.next_check_at,pm.stream_id LIMIT 1").bind(cutoff.timestamp_micros()).bind(policy).bind(storage).bind(storage).fetch_optional(db).await?)
}
pub(in super::super) async fn stream_repack_after(
    db: impl sqlx::SqliteExecutor<'_>,
    stream: Id,
    cooldown: f64,
) -> Result<Option<DateTime<Utc>>> {
    let micros:Option<i64>=sqlx::query_scalar("SELECT MAX(later) FROM (SELECT MAX(COALESCE(c.repack_after,c.reference_changed_at),c.reference_changed_at+?) AS later FROM extents e JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=? AND c.split_at IS NOT NULL AND (SELECT COUNT(*) FROM extents WHERE chunk_id=c.id)=1) deferred WHERE later>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind((cooldown*1_000_000.) as i64).bind(stream).fetch_one(db).await?;
    Ok(micros.map(datetime).transpose()?)
}
pub(crate) async fn defer_pack_queue(
    db: &mut sqlx::SqliteConnection,
    task: Execution,
    stream: Id,
    generation: i64,
    later: DateTime<Utc>,
) -> Result<()> {
    transactions::lock_execution(db, task).await?;
    super::defer_pack_queue(stream, generation, later)
        .execute(db)
        .await?;
    Ok(())
}
pub(crate) async fn finish_pack_queue(
    db: &mut sqlx::SqliteConnection,
    task: Execution,
    stream: Id,
    generation: i64,
) -> Result<()> {
    transactions::lock_execution(db, task).await?;
    super::finish_pack_queue(stream, generation)
        .execute(db)
        .await?;
    Ok(())
}
pub(crate) async fn advance_pack_queue(
    db: &mut sqlx::SqliteConnection,
    task: Execution,
    stream: Id,
    generation: i64,
    cursor: i64,
) -> Result<()> {
    transactions::lock_execution(db, task).await?;
    super::advance_pack_queue(stream, generation, cursor)
        .execute(db)
        .await?;
    Ok(())
}
pub(in super::super) async fn next_range_pack(
    db: impl sqlx::SqliteExecutor<'_>,
    storage: Uuid,
    cutoff: DateTime<Utc>,
    window: f64,
    minimum: i64,
) -> Result<Option<Id>> {
    Ok(sqlx::query_scalar("SELECT p.id FROM packs p JOIN pack_access_windows w ON w.pack_id=p.id WHERE p.storage_id=? AND p.state='ready' AND p.created_at<=? AND (p.range_checked_at IS NULL OR p.range_checked_at<?) AND w.window_start>=CAST((CAST(unixepoch('subsec')*1000000 AS INTEGER)-?)/3600000000 AS INTEGER)*3600000000 GROUP BY p.id,p.range_checked_at HAVING SUM(w.partial_downloads)>=? AND (p.range_checked_at IS NULL OR MAX(w.updated_at)>p.range_checked_at) ORDER BY SUM(w.partial_bytes) DESC,p.id LIMIT 1").bind(storage).bind(cutoff.timestamp_micros()).bind(cutoff.timestamp_micros()).bind((window * 1_000_000.) as i64).bind(minimum).fetch_optional(db).await?)
}
pub(in super::super) async fn range_pack_blocked(
    db: impl sqlx::SqliteExecutor<'_>,
    pack: Id,
) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE source_pack_id=?) OR EXISTS(SELECT 1 FROM chunks c JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id WHERE c.pack_id=? AND s.state='writing')").bind(pack).bind(pack).fetch_one(db).await?)
}
pub(in super::super) async fn pack_range_totals(
    db: impl sqlx::SqliteExecutor<'_>,
    pack: Id,
    window: f64,
) -> Result<(i64, i64, i64)> {
    Ok(sqlx::query_as("SELECT CAST(COALESCE(SUM(partial_downloads),0) AS INTEGER),CAST(COALESCE(SUM(partial_bytes),0) AS INTEGER),CAST(COALESCE(SUM(downloads-partial_downloads),0) AS INTEGER) FROM pack_access_windows WHERE pack_id=? AND window_start>=CAST((CAST(unixepoch('subsec')*1000000 AS INTEGER)-?)/3600000000 AS INTEGER)*3600000000").bind(pack).bind((window * 1_000_000.) as i64).fetch_one(db).await?)
}
pub(in super::super) async fn pack_member_hits(
    db: impl sqlx::SqliteExecutor<'_>,
    pack: Id,
    window: f64,
) -> Result<Vec<(Id, i64)>> {
    Ok(sqlx::query_as("SELECT chunk_id,CAST(SUM(downloads) AS INTEGER) FROM pack_member_access_windows WHERE pack_id=? AND window_start>=CAST((CAST(unixepoch('subsec')*1000000 AS INTEGER)-?)/3600000000 AS INTEGER)*3600000000 GROUP BY chunk_id").bind(pack).bind((window * 1_000_000.) as i64).fetch_all(db).await?)
}
pub(in super::super) async fn pack_dimensions(
    db: impl sqlx::SqliteExecutor<'_>,
    pack: Id,
) -> Result<(i64, i64, i32)> {
    Ok(
        sqlx::query_as("SELECT stored_size,raw_size,member_count FROM packs WHERE id=?")
            .bind(pack)
            .fetch_one(db)
            .await?,
    )
}
pub(in super::super) async fn range_pack_layout(
    db: impl sqlx::SqliteExecutor<'_>,
    pack: Id,
) -> Result<Vec<Mapping>> {
    Ok(super::packing::pack_layout(pack, true)
        .build_query_as()
        .fetch_all(db)
        .await?)
}
pub(in super::super) async fn defer_range_repack(
    db: impl sqlx::SqliteExecutor<'_>,
    stream: Id,
    window: f64,
    flush: f64,
    retry: f64,
    offset: i64,
) -> Result<()> {
    sqlx::query("UPDATE chunks AS c SET repack_after=CAST(unixepoch('subsec')*1000000 AS INTEGER)+? FROM access_state g WHERE c.id IN (SELECT chunk_id FROM extents WHERE stream_id=? AND offset_bytes>=? ORDER BY offset_bytes LIMIT 1026) AND c.range_split_at IS NOT NULL AND c.repack_after<=CAST(unixepoch('subsec')*1000000 AS INTEGER) AND (g.coverage_since IS NULL OR g.coverage_since>CAST((CAST(unixepoch('subsec')*1000000 AS INTEGER)-?)/3600000000 AS INTEGER)*3600000000 OR g.flushed_at IS NULL OR g.flushed_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-? OR EXISTS(SELECT 1 FROM chunk_access_windows w WHERE w.chunk_id=c.id AND w.range_read_count>0 AND w.window_start>=CAST((CAST(unixepoch('subsec')*1000000 AS INTEGER)-?)/3600000000 AS INTEGER)*3600000000))").bind((retry*1_000_000.) as i64).bind(stream).bind(offset).bind((window*1_000_000.) as i64).bind((flush*1_000_000.) as i64).bind((window*1_000_000.) as i64).execute(db).await?;
    Ok(())
}

pub(in super::super) type Connection = sqlx::SqliteConnection;
pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, sqlx::Sqlite>;
pub(in super::super) use super::packing::pack_record as source_pack;
pub(in super::super) use super::read_sources::node_read_encodings as encodings;
#[derive(sqlx::FromRow)]
pub(in super::super) struct Block {
    pub(in super::super) id: Uuid,
    pub(in super::super) algorithm: String,
}
pub(in super::super) fn storage_key(storage: Uuid) -> Rows<'static, Block> {
    sqlx::query_as("SELECT k.id,k.key_algorithm AS algorithm FROM storages s JOIN managed_keys k ON k.id=s.block_key_id WHERE s.id=? AND k.purpose='block' AND k.revoked_at IS NULL").bind(storage)
}
pub(in super::super) async fn clock(db: &mut Connection) -> Result<DateTime<Utc>> {
    let n: i64 = sqlx::query_scalar("SELECT CAST(unixepoch('subsec')*1000000 AS INTEGER)")
        .fetch_one(db)
        .await?;
    DateTime::from_timestamp_micros(n).context("invalid database timestamp")
}
pub(in super::super) async fn keys(
    db: &mut Connection,
    ids: &[Uuid],
    key: &crate::content::codec::Key,
) -> Result<Vec<crate::model::node_read::BlockKey>> {
    super::read_sources::node_read_keys(ids)
        .fetch_all(db)
        .await?
        .into_iter()
        .map(|r| crate::model::node_read::BlockKey::unprotect(r, key))
        .collect()
}
pub(in super::super) async fn physical_members(
    db: &mut Connection,
    pack: Id,
) -> Result<Vec<crate::model::node_work::Member>> {
    let rows:Vec<(Uuid,i64,i32,Vec<u8>)>=sqlx::query_as("SELECT c.uuid,m.offset_bytes,c.raw_size,c.hash FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=? ORDER BY m.ordinal LIMIT 4097").bind(pack).fetch_all(db).await?;
    Ok(rows
        .into_iter()
        .map(|(uuid, offset, size, hash)| (uuid.into(), offset, size, hash))
        .collect())
}
pub(in super::super) async fn creation_allowed(db: &mut Connection) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT NOT pack_creation_paused FROM mokyu_meta WHERE id=1")
            .fetch_one(db)
            .await?,
    )
}
pub(in super::super) async fn cursor(db: &mut Connection, task: Uuid) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT progress_cursor FROM tasks WHERE id=?")
            .bind(task)
            .fetch_one(db)
            .await?,
    )
}
pub(in super::super) async fn advance_cursor(
    db: &mut Connection,
    task: Execution,
    next: Option<String>,
    n: i64,
) -> Result<()> {
    if next.is_none() {
        let mut detail = super::lock_task_detail(task).fetch_one(&mut *db).await?;
        if detail["storages"].is_array() {
            let done = super::super::transactions::advance_storage_scope(&mut detail)?;
            super::advance_storage_task(task.id, detail, n, done)
                .execute(&mut *db)
                .await?;
            return Ok(());
        }
    }
    sqlx::query("UPDATE tasks SET progress_cursor=?,processed=processed+?,state=CASE WHEN ? IS NULL THEN 'completed' ELSE state END,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND execution_token=? AND state='running'").bind(&next).bind(n).bind(&next).bind(task.id).bind(task.token).execute(db).await?;
    Ok(())
}
pub(in super::super) async fn save_snapshot(
    db: &mut Connection,
    task: Execution,
    value: &Value,
) -> Result<()> {
    sqlx::query("UPDATE tasks SET detail=json_set(detail,'$.node_pack',json(?)) WHERE id=? AND execution_token=? AND state='running'")
        .bind(value).bind(task.id).bind(task.token).execute(db).await?;
    Ok(())
}
pub(in super::super) async fn clear_snapshot(db: &mut Connection, task: Uuid) -> Result<()> {
    sqlx::query("UPDATE tasks SET detail=json_remove(detail,'$.node_pack') WHERE id=?")
        .bind(task)
        .execute(db)
        .await?;
    Ok(())
}
pub(in super::super) async fn live(db: &mut Connection, ids: &[Id]) -> Result<Vec<Id>> {
    Ok(sqlx::query_scalar("SELECT DISTINCT chunk_id FROM extents WHERE chunk_id IN (SELECT CAST(value AS INTEGER) FROM json_each(?))").bind(sqlx::types::Json(ids)).fetch_all(db).await?)
}
pub(in super::super) async fn pack_counts(
    db: &mut Connection,
    ids: &[Id],
) -> Result<Vec<(Id, i64)>> {
    Ok(sqlx::query_as("SELECT pack_id,count(*) FROM chunks WHERE pack_id IN (SELECT CAST(value AS INTEGER) FROM json_each(?)) AND state='ready' GROUP BY pack_id").bind(sqlx::types::Json(ids)).fetch_all(db).await?)
}
pub(in super::super) async fn reference_boundaries(
    db: &mut Connection,
    rows: &[&Mapping],
) -> Result<Vec<bool>> {
    if rows.len() < 2 {
        return Ok(Vec::new());
    }
    let pairs = rows
        .windows(2)
        .map(|r| {
            (
                r[0].chunk.id,
                r[1].chunk.id,
                r[1].offset_bytes - r[0].offset_bytes,
            )
        })
        .collect::<Vec<_>>();
    let rows=sqlx::query_scalar("WITH pairs AS (SELECT CAST(json_extract(value,'$[0]') AS INTEGER) AS a,CAST(json_extract(value,'$[1]') AS INTEGER) AS b,CAST(json_extract(value,'$[2]') AS INTEGER) AS distance,key AS position FROM json_each(?)) SELECT NOT EXISTS(SELECT 1 FROM extents e JOIN chunks c ON c.id=e.chunk_id WHERE e.chunk_id=v.a AND (e.source_offset<>0 OR e.length<>c.raw_size OR NOT EXISTS(SELECT 1 FROM extents f JOIN chunks d ON d.id=f.chunk_id WHERE f.chunk_id=v.b AND f.stream_id=e.stream_id AND f.offset_bytes=e.offset_bytes+v.distance AND f.source_offset=0 AND f.length=d.raw_size))) AND NOT EXISTS(SELECT 1 FROM extents f JOIN chunks d ON d.id=f.chunk_id WHERE f.chunk_id=v.b AND (f.source_offset<>0 OR f.length<>d.raw_size OR NOT EXISTS(SELECT 1 FROM extents e JOIN chunks c ON c.id=e.chunk_id WHERE e.chunk_id=v.a AND e.stream_id=f.stream_id AND e.offset_bytes=f.offset_bytes-v.distance AND e.source_offset=0 AND e.length=c.raw_size))) FROM pairs v ORDER BY v.position").bind(sqlx::types::Json(&pairs)).fetch_all(db).await?;
    Ok(rows)
}
pub(in super::super) async fn prepare_outputs(
    db: &mut Connection,
    task: Uuid,
    storage: Uuid,
    batch: &mut crate::model::node_pack::Batch,
) -> Result<()> {
    for c in &mut batch.chunks {
        if c.ready.is_some() {
            continue;
        }
        let result=sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,state,created_at,unreferenced_at,owner_task,encryption_algorithm,block_key_id,nonce,nonce_id,encoding_fingerprint) VALUES(?,?,?,'uploading',?,CAST(unixepoch('subsec')*1000000 AS INTEGER),?,?,?,?,?,'[]')").bind(c.id).bind(c.location).bind(storage).bind(batch.created.timestamp_micros()).bind(task).bind(&batch.algorithm).bind(batch.key).bind(&c.nonce).bind(crate::content::nonce::id(c.nonce.as_deref())).execute(&mut *db).await?;
        let id = (result.last_insert_rowid()).into();
        super::refresh_encoding_fingerprint(&mut *db, id).await?;
        c.encoding = Some(id);
    }
    for a in &mut batch.packs {
        let size: i64 = batch.chunks[a.start..a.end]
            .iter()
            .map(|c| i64::from(c.size))
            .sum();
        let result=sqlx::query("INSERT INTO packs(location_id,storage_id,encryption_algorithm,block_key_id,raw_size,member_count,state,created_at,unreferenced_at,owner_task,nonce,nonce_id,pack_fingerprint) VALUES(?,?,?,?,?,?,'preparing',?,CAST(unixepoch('subsec')*1000000 AS INTEGER),?,?,?,'[]')").bind(a.location).bind(storage).bind(&batch.algorithm).bind(batch.key).bind(size).bind(i32::try_from(a.end-a.start)?).bind(batch.created.timestamp_micros()).bind(task).bind(&a.nonce).bind(crate::content::nonce::id(a.nonce.as_deref())).execute(&mut *db).await?;
        let id = (result.last_insert_rowid()).into();
        super::refresh_pack_fingerprint(&mut *db, id).await?;
        a.id = Some(id);
    }
    Ok(())
}
pub(in super::super) async fn record_outputs(
    db: &mut Connection,
    task: Uuid,
    rows: &[Mapping],
    outputs: &[PackOutput],
) -> Result<()> {
    let mut chunks = std::collections::BTreeMap::new();
    for row in rows {
        chunks.insert(row.chunk.id, &row.chunk);
    }
    for out in outputs {
        if let Some(pack) = &out.pack {
            ensure!(
                super::pack_metadata(pack)
                    .execute(&mut *db)
                    .await?
                    .rows_affected()
                    == 1,
                crate::model::Error::OperationAborted
            );
            super::refresh_pack_fingerprint(&mut *db, pack.id).await?;
            let members = out
                .chunks
                .iter()
                .map(|id| chunks.get(id).copied().context("pack member is missing"))
                .collect::<Result<Vec<_>>>()?;
            super::insert_pack_members(pack.id, members.into_iter())
                .build()
                .execute(&mut *db)
                .await?;
        } else if let Some(chunk) = &out.independent {
            sqlx::query("UPDATE chunk_encodings SET standalone_size=?,standalone_compressed=? WHERE encoding_id=? AND chunk_id=? AND location_id=? AND owner_task=? AND state='uploading'").bind(chunk.stored_size).bind(chunk.compressed).bind(chunk.encoding_id).bind(chunk.id).bind(chunk.location_id).bind(task).execute(&mut *db).await?;
            super::refresh_encoding_fingerprint(&mut *db, chunk.encoding_id).await?;
        }
    }
    Ok(())
}
