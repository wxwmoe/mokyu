use super::packing::*;
use super::*;
pub(in super::super) async fn ready_encoding(
    db: impl sqlx::SqliteExecutor<'_>,
    chunk: Id,
) -> Result<Option<(Id, Uuid, i32)>> {
    Ok(sqlx::query_as("SELECT encoding_id,location_id,standalone_size FROM chunk_encodings WHERE chunk_id=? AND state='ready' AND quarantined_at IS NULL ORDER BY encoding_id LIMIT 1").bind(chunk).fetch_optional(db).await?)
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
    _cooldown: f64,
) -> Result<Vec<PackCandidate>> {
    let mut query = sqlx::QueryBuilder::<Sqlite>::new("SELECT ");
    query.push(CHUNK_COLUMNS).push(",e.offset_bytes,c.pack_id,(NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings q WHERE q.chunk_id=c.id AND q.state='ready' AND q.quarantined_at IS NOT NULL) AND NOT EXISTS(SELECT 1 FROM packs q WHERE q.id=c.pack_id AND q.quarantined_at IS NOT NULL) AND e.source_offset=0 AND e.length=c.raw_size AND (c.repack_after IS NULL OR c.repack_after<=CAST(unixepoch('subsec')*1000000 AS INTEGER))) AS eligible,EXISTS(SELECT 1 FROM extents other WHERE other.chunk_id=c.id AND other.stream_id<>e.stream_id) AS shared FROM extents e JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' JOIN streams s ON s.id=e.stream_id WHERE e.stream_id=").push_bind(stream).push(" AND e.offset_bytes>=").push_bind(offset).push(" AND s.state='ready' AND c.state='ready' AND EXISTS(SELECT 1 FROM objects WHERE stream_id=s.id) ORDER BY e.offset_bytes LIMIT 1026");
    Ok(query
        .build_query_as::<PackCandidateRow>()
        .fetch_all(db)
        .await?
        .into_iter()
        .map(|r| PackCandidate {
            row: r.row,
            eligible: r.eligible,
            shared: r.shared,
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
pub(in super::super) async fn reclaim_estimate(
    db: &mut sqlx::SqliteConnection,
    pack: Id,
    groups: &[Vec<Mapping>],
) -> Result<i64> {
    let before: Option<i64> = sqlx::query_scalar("SELECT stored_size FROM packs p WHERE id=? AND NOT EXISTS(SELECT 1 FROM journal_protected_addresses h WHERE h.address=p.location_id)").bind(pack).fetch_optional(&mut *db).await?.flatten();
    let Some(before) = before else { return Ok(0) };
    let index:std::collections::BTreeMap<_,_>=sqlx::query_as::<_,(Id,Option<i32>,bool)>("SELECT m.chunk_id,m.encoded_size,EXISTS(SELECT 1 FROM chunk_encodings e WHERE e.chunk_id=m.chunk_id AND e.storage_id=p.storage_id AND e.state='ready' AND e.quarantined_at IS NULL) FROM pack_members m JOIN packs p ON p.id=m.pack_id WHERE m.pack_id=?").bind(pack).fetch_all(db).await?.into_iter().map(|(id,size,ready)|(id,(size,ready))).collect();
    let mut after = 0i64;
    for group in groups {
        after += 8;
        for member in group {
            let &(size, ready) = index.get(&member.chunk.id).context("missing pack member")?;
            if group.len() == 1 && ready {
                after -= 8;
                continue;
            }
            after += i64::from(size.context("missing pack member length")?);
        }
    }
    Ok(before - after)
}

pub(in super::super) async fn repack_benefit(
    db: &mut sqlx::SqliteConnection,
    stream: Id,
    selected: &[Mapping],
) -> Result<i64> {
    let (Some(first), Some(last)) = (selected.first(), selected.last()) else {
        return Ok(0);
    };
    const PLACEMENT: &str = "SELECT e.offset_bytes,e.length,c.id AS chunk_id,CASE WHEN l.encoding_id IS NULL THEN c.pack_id END AS pack_id,m.offset_bytes AS physical_offset,COALESCE(m.encoded_size,l.standalone_size,c.standalone_size) AS encoded_size FROM extents e JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' LEFT JOIN pack_members m ON l.encoding_id IS NULL AND m.pack_id=c.pack_id AND m.chunk_id=c.id WHERE e.stream_id=? AND e.offset_bytes+e.length>? AND e.offset_bytes<? ORDER BY e.offset_bytes";
    let rows = sqlx::query_as::<_, crate::model::packing::ReadPlacement>(PLACEMENT)
        .bind(stream)
        .bind(first.offset_bytes)
        .bind(last.offset_bytes + i64::from(last.chunk.raw_size))
        .fetch_all(&mut *db)
        .await?;
    let Some(proposed) = crate::model::packing::proposed_ranges(&rows, selected) else {
        return Ok(0);
    };
    // Include cold reads of every object whose members would move, including previously contiguous reads.
    let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
        "SELECT r.stream_id,r.start_bytes,r.end_bytes,CAST(SUM(r.requests) AS INTEGER) FROM pack_read_shapes r WHERE EXISTS(SELECT 1 FROM objects o WHERE o.stream_id=r.stream_id) AND EXISTS(SELECT 1 FROM extents e WHERE e.stream_id=r.stream_id AND e.offset_bytes<r.end_bytes AND e.offset_bytes+e.length>r.start_bytes AND e.chunk_id IN (",
    );
    {
        let mut ids = query.separated(",");
        for member in selected {
            ids.push_bind(member.chunk.id);
        }
    }
    query.push(")) GROUP BY r.stream_id,r.start_bytes,r.end_bytes LIMIT 257");
    let shapes = query
        .build_query_as::<(Id, i64, i64, i64)>()
        .fetch_all(&mut *db)
        .await?;
    if shapes.len() > 256 {
        return Ok(0);
    }
    let mut gain = 0i64;
    for (id, start, end, requests) in shapes {
        let rows = sqlx::query_as::<_, crate::model::packing::ReadPlacement>(PLACEMENT)
            .bind(id)
            .bind(start)
            .bind(end)
            .fetch_all(&mut *db)
            .await?;
        let Some(delta) = crate::model::packing::read_savings(&rows, &proposed) else {
            return Ok(0);
        };
        gain = gain.saturating_add(delta.saturating_mul(requests));
    }
    Ok(gain)
}

pub(in super::super) async fn next_repack_stream(
    db: &mut sqlx::SqliteConnection,
    task: Uuid,
    after: Id,
    offset: i64,
    upper: DateTime<Utc>,
    storage: Uuid,
) -> Result<Option<Id>> {
    if after == Id::default() {
        sqlx::query("INSERT INTO pack_repack_queue(task_id,stream_id,saved_gets) SELECT ?,r.stream_id,SUM(r.saved_gets) FROM pack_read_shapes r JOIN streams s ON s.id=r.stream_id JOIN buckets b ON b.id=s.bucket_id WHERE b.storage_id=? AND s.state='ready' AND r.observed_hour<=? AND EXISTS(SELECT 1 FROM objects o WHERE o.stream_id=s.id) GROUP BY r.stream_id HAVING SUM(r.saved_gets)>0 ON CONFLICT(task_id,stream_id) DO NOTHING").bind(task).bind(storage).bind(upper.timestamp()).execute(&mut *db).await?;
    }
    Ok(sqlx::query_scalar("SELECT q.stream_id FROM pack_repack_queue q LEFT JOIN pack_repack_queue previous ON previous.task_id=q.task_id AND previous.stream_id=? WHERE q.task_id=? AND (previous.stream_id IS NULL OR q.saved_gets<previous.saved_gets OR (q.saved_gets=previous.saved_gets AND (q.stream_id>previous.stream_id OR (q.stream_id=previous.stream_id AND ?>=0)))) AND EXISTS(SELECT 1 FROM streams s JOIN buckets b ON b.id=s.bucket_id WHERE s.id=q.stream_id AND b.storage_id=?) ORDER BY q.saved_gets DESC,q.stream_id LIMIT 1").bind(after).bind(task).bind(offset).bind(storage).fetch_optional(db).await?)
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
    sqlx::query_as("SELECT k.id,k.key_algorithm AS algorithm FROM storages s JOIN managed_keys k ON k.id=s.block_key_id WHERE s.id=? AND k.purpose='block' AND NOT k.recovery_read_only AND k.revoked_at IS NULL").bind(storage)
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
    pack: &crate::content::pack::Pack,
) -> Result<Vec<crate::model::node_work::Member>> {
    super::physical::pack_index_rows(db, pack.id, None)
        .await?
        .into_iter()
        .map(|(_, mut entry)| {
            pack.resolve_entry(&mut entry)?;
            Ok(entry)
        })
        .collect()
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
pub(in super::super) async fn prepare_outputs(
    db: &mut Connection,
    task: Uuid,
    storage: Uuid,
    batch: &mut crate::model::node_pack::Batch,
) -> Result<()> {
    let packed = batch.packed_chunks()?;
    for (index, grouped) in packed.into_iter().enumerate() {
        if grouped || batch.chunks[index].ready.is_some() {
            continue;
        }
        if batch.chunks[index].original.is_some() {
            let c = batch.chunk(storage, index)?;
            batch.chunks[index].encoding =
                Some(super::reserve_member_encoding(db, &c, task).await?);
            batch.chunks[index].location = c.location_id;
            continue;
        }
        let c = &mut batch.chunks[index];
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
        let result=sqlx::query("INSERT INTO packs(location_id,storage_id,encryption_algorithm,block_key_id,raw_size,member_count,state,created_at,unreferenced_at,owner_task,nonce,nonce_id,pack_fingerprint) VALUES(?,?,?,?,?,?,'preparing',?,CAST(unixepoch('subsec')*1000000 AS INTEGER),?,?,?,'[]')").bind(a.location).bind(storage).bind("none").bind(Option::<Uuid>::None).bind(size).bind(i32::try_from(a.end-a.start)?).bind(batch.created.timestamp_micros()).bind(task).bind(&a.nonce).bind(crate::content::nonce::id(a.nonce.as_deref())).execute(&mut *db).await?;
        let id = (result.last_insert_rowid()).into();
        super::refresh_pack_fingerprint(&mut *db, id).await?;
        a.id = Some(id);
    }
    Ok(())
}
pub(in super::super) async fn record_outputs(
    db: &mut Connection,
    task: Uuid,
    outputs: &[PackOutput],
) -> Result<()> {
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
            super::write_pack_index(&mut *db, pack.id, &out.entries).await?;
        } else if let Some(chunk) = &out.independent {
            sqlx::query("UPDATE chunk_encodings SET standalone_size=?,standalone_compressed=? WHERE encoding_id=? AND chunk_id=? AND location_id=? AND owner_task=? AND state='uploading'").bind(chunk.stored_size).bind(chunk.compressed).bind(chunk.encoding_id).bind(chunk.id).bind(chunk.location_id).bind(task).execute(&mut *db).await?;
            super::refresh_encoding_fingerprint(&mut *db, chunk.encoding_id).await?;
        }
    }
    Ok(())
}
