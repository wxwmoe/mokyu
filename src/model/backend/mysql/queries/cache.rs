use super::*;

pub(in super::super) fn retire_rewrite_pack(pack: Id) -> Statement<'static> {
    sqlx::query("UPDATE packs SET state='retired',unreferenced_at=CURRENT_TIMESTAMP(6),owner_task=NULL WHERE id=? AND state='ready'").bind(pack)
}

pub(in super::super) fn rewrite_encoding_owner(encoding: Id, task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT state='uploading' AND owner_task=? FROM chunk_encodings WHERE encoding_id=? FOR UPDATE").bind(task).bind(encoding)
}

pub(in super::super) fn lock_cache_stream(stream: Id, storage: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT s.id FROM streams s WHERE s.id=? AND s.state='writing' AND EXISTS(SELECT 1 FROM buckets b WHERE b.id=s.bucket_id AND b.storage_id=?) FOR UPDATE").bind(stream).bind(storage)
}

pub(in super::super) fn lock_cache_source(
    chunk: Id,
) -> Rows<'static, (String, Option<Id>, Uuid, Id)> {
    sqlx::query_as(
        "SELECT state,pack_id,storage_id,current_encoding_id FROM chunks WHERE id=? FOR UPDATE",
    )
    .bind(chunk)
}

pub(in super::super) fn insert_pending_upload<'q>(
    input: &'q CacheStage<'q>,
    source: Option<Id>,
    timeout: i64,
) -> Statement<'q> {
    sqlx::query("INSERT INTO pending_uploads(chunk_id,stream_id,offset_bytes,staged_size,cache_key,source_pack_id,next_retry_at,encoding_policy) VALUES(?,?,?,?,?,?,TIMESTAMPADD(SECOND,?,CURRENT_TIMESTAMP(6)),?)")
        .bind(input.chunk.id).bind(input.stream).bind(input.offset).bind(input.size).bind(input.key).bind(source).bind(timeout).bind(input.encoding)
}

pub(in super::super) fn ready_cached_chunk(chunk: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET state='ready',unreferenced_at=NULL,owner_stream=NULL,reference_changed_at=CURRENT_TIMESTAMP(6) WHERE id=?").bind(chunk)
}

pub(in super::super) fn seal_stream_uploads(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE pending_uploads SET next_retry_at=LEAST(next_retry_at,CURRENT_TIMESTAMP(6)) WHERE owner_task IS NULL AND chunk_id IN (SELECT chunk_id FROM extents WHERE stream_id=?)").bind(stream)
}

pub(in super::super) fn unsealed_uploads(stream: Id) -> Rows<'static, (Id, i64, i32, String)> {
    sqlx::query_as("SELECT p.chunk_id,p.offset_bytes,c.raw_size,COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(p.encoding_policy,'$.pack_max_size')) AS CHAR CHARACTER SET ascii) COLLATE ascii_bin,sp.pack_max_size) FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id JOIN storage_profiles sp ON sp.storage_id=c.storage_id WHERE p.stream_id=? AND p.owner_task IS NULL ORDER BY p.offset_bytes LIMIT 1026").bind(stream)
}

pub(in super::super) fn seal_upload_chunks(ids: &[Id]) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "UPDATE pending_uploads SET next_retry_at=LEAST(next_retry_at,CURRENT_TIMESTAMP(6)) WHERE owner_task IS NULL AND chunk_id IN ",
    );
    chunk_ids(&mut query, ids);
    query
}

pub(in super::super) fn retryable_upload() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT t.id FROM tasks t WHERE t.task_type='upload' AND t.state='failed' AND EXISTS(SELECT 1 FROM pending_uploads p WHERE p.owner_task=t.id AND p.next_retry_at<=CURRENT_TIMESTAMP(6)) ORDER BY (SELECT p.next_retry_at FROM pending_uploads p WHERE p.owner_task=t.id ORDER BY p.next_retry_at,p.chunk_id LIMIT 1),(SELECT p.chunk_id FROM pending_uploads p WHERE p.owner_task=t.id ORDER BY p.next_retry_at,p.chunk_id LIMIT 1) LIMIT 1 FOR UPDATE SKIP LOCKED",
    )
}

pub(in super::super) fn queue_upload_retry(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='queued',error=NULL,updated_at=CURRENT_TIMESTAMP(6) WHERE id=? AND state='failed'").bind(id)
}

pub(super) fn write_delay(
    query: &mut sqlx::QueryBuilder<MySql>,
    created: &str,
    storage: &str,
    urgent: &[Uuid],
) {
    query.push(format!(" AND (sp.write_flush_interval=0 OR TIMESTAMPADD(SECOND,LEAST(sp.write_flush_interval,sp.write_backlog_max_age),{created})<=CURRENT_TIMESTAMP(6)"));
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

pub(in super::super) fn due_upload(urgent: &[Uuid]) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT p.chunk_id,p.stream_id,p.source_pack_id FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id JOIN storage_profiles sp ON sp.storage_id=c.storage_id WHERE p.owner_task IS NULL AND p.next_retry_at<=CURRENT_TIMESTAMP(6)",
    );
    write_delay(&mut query, "p.created_at", "c.storage_id", urgent);
    query.push(" ORDER BY CASE WHEN TIMESTAMPADD(SECOND,sp.write_backlog_max_age,p.created_at)<=CURRENT_TIMESTAMP(6) THEN 0 ELSE 1 END,p.next_retry_at,p.chunk_id LIMIT 1 FOR UPDATE SKIP LOCKED");
    query
}

pub(in super::super) fn pending_upload_segment(
    first: Id,
    stream: Option<Id>,
    source: Option<Id>,
) -> Rows<'static, (Id, i64, i32)> {
    sqlx::query_as("SELECT p.chunk_id,p.offset_bytes,c.raw_size FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE p.encoding_policy <=> (SELECT encoding_policy FROM pending_uploads WHERE chunk_id=?) AND p.owner_task IS NULL AND (p.chunk_id=? OR (p.stream_id=? AND p.source_pack_id<=>?)) ORDER BY p.offset_bytes LIMIT 1026")
        .bind(first).bind(first).bind(stream).bind(source)
}

pub(in super::super) fn claim_pending_uploads(ids: &[Id], task: Uuid) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new("UPDATE pending_uploads SET owner_task=");
    query.push_bind(task).push(" WHERE chunk_id IN ");
    chunk_ids(&mut query, ids);
    query
}

pub(in super::super) fn flush_candidate(upper: DateTime<Utc>, task: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT chunk_id FROM pending_uploads u WHERE created_at<=? AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=u.chunk_id) AND (owner_task IS NULL OR owner_task=? OR owner_task IN (SELECT id FROM tasks WHERE state IN ('paused','failed'))) ORDER BY created_at,chunk_id LIMIT 1")
        .bind(upper).bind(task)
}

pub(in super::super) fn flush_pending(upper: DateTime<Utc>) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE created_at<=?)")
        .bind(upper)
}

pub(in super::super) fn upload_is_durable(id: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE chunk_id=? AND (EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=c.id AND state='ready') OR (c.pack_id IS NOT NULL AND NOT(c.pack_id<=>u.source_pack_id))))").bind(id)
}

pub(in super::super) fn upload_is_unused(id: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=?)").bind(id)
}

pub(in super::super) fn pending_upload(
    id: Id,
    timeout: i64,
) -> Rows<'static, (Option<Id>, i64, Option<Id>, bool)> {
    sqlx::query_as("SELECT stream_id,offset_bytes,source_pack_id,created_at<=TIMESTAMPADD(SECOND,-?,CURRENT_TIMESTAMP(6)) FROM pending_uploads WHERE chunk_id=?").bind(timeout).bind(id)
}

pub(in super::super) fn pending_pack_uploads(pack: Id, task: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT chunk_id FROM pending_uploads WHERE source_pack_id=? AND (owner_task IS NULL OR owner_task=?) ORDER BY chunk_id").bind(pack).bind(task)
}

pub(in super::super) fn pending_upload_mappings(
    first: Id,
    stream: Option<Id>,
    offset: i64,
    task: Uuid,
    flush: bool,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new("SELECT ");
    query.push(CHUNK_COLUMNS).push(",p.offset_bytes,c.pack_id FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE p.encoding_policy <=> (SELECT encoding_policy FROM pending_uploads WHERE chunk_id=").push_bind(first).push(") AND (p.chunk_id=").push_bind(first)
        .push(" OR (p.stream_id=").push_bind(stream).push(" AND p.offset_bytes>").push_bind(offset).push(" AND p.source_pack_id IS NULL)) AND (p.owner_task IS NULL OR p.owner_task=").push_bind(task)
        .push(" OR (").push_bind(flush).push(" AND p.owner_task IN (SELECT id FROM tasks WHERE state IN ('paused','failed')))) ORDER BY p.offset_bytes LIMIT 1026");
    query
}

pub(in super::super) fn same_references(a: Id, b: Id, distance: i64) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM extents a JOIN chunks c ON c.id=a.chunk_id WHERE a.chunk_id=? AND (a.source_offset<>0 OR a.length<>c.raw_size OR NOT EXISTS(SELECT 1 FROM extents b JOIN chunks d ON d.id=b.chunk_id WHERE b.chunk_id=? AND b.stream_id=a.stream_id AND b.offset_bytes=a.offset_bytes+? AND b.source_offset=0 AND b.length=d.raw_size))) AND NOT EXISTS(SELECT 1 FROM extents b JOIN chunks c ON c.id=b.chunk_id WHERE b.chunk_id=? AND (b.source_offset<>0 OR b.length<>c.raw_size OR NOT EXISTS(SELECT 1 FROM extents a JOIN chunks d ON d.id=a.chunk_id WHERE a.chunk_id=? AND a.stream_id=b.stream_id AND a.offset_bytes=b.offset_bytes-? AND a.source_offset=0 AND a.length=d.raw_size)))")
        .bind(a).bind(b).bind(distance).bind(b).bind(a).bind(distance)
}

pub(in super::super) fn unclaim_uploads(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE pending_uploads SET owner_task=NULL WHERE owner_task=?").bind(task)
}

pub(in super::super) fn retire_task_packs(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE packs SET state='retired',unreferenced_at=CURRENT_TIMESTAMP(6) WHERE owner_task=? AND state='preparing'").bind(task)
}

pub(in super::super) fn retire_task_encodings(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=CURRENT_TIMESTAMP(6) WHERE owner_task=? AND state='uploading'").bind(task)
}

pub(in super::super) fn release_task_inputs(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM pack_inputs WHERE task_id=?").bind(task)
}

pub(in super::super) fn lock_pending_chunks(ids: &[Id]) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT c.id,(SELECT u.cache_key FROM pending_uploads u WHERE u.chunk_id=c.id) AS cache_key,CASE WHEN c.state='ready' THEN COALESCE((SELECT e.location_id FROM chunk_encodings e WHERE e.encoding_id=c.current_encoding_id AND e.state='ready'),CASE WHEN EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.state='ready') THEN c.uuid END) END FROM chunks c WHERE c.id IN ",
    );
    chunk_ids(&mut query, ids);
    query.push(" ORDER BY c.id FOR UPDATE");
    query
}

pub(in super::super) fn can_release_pending(id: Id, existing_pack: bool) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=? AND state='ready') OR EXISTS(SELECT 1 FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE c.id=? AND p.state='ready' AND (? OR NOT(c.pack_id<=>(SELECT source_pack_id FROM pending_uploads WHERE chunk_id=c.id)))) OR NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=?)")
        .bind(id).bind(id).bind(existing_pack).bind(id)
}

pub(in super::super) fn schedule_uploaded_pack(id: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO pack_maintenance(stream_id,reason) SELECT DISTINCT e.stream_id,'pack' FROM extents e JOIN streams s ON s.id=e.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE e.chunk_id=? AND s.state='ready' AND c.pack_id IS NULL ON DUPLICATE KEY UPDATE progress_cursor=0,generation=pack_maintenance.generation+1,updated_at=CURRENT_TIMESTAMP(6)").bind(id)
}

pub(in super::super) fn delete_upload_pins(id: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM cache_pins WHERE chunk_id=?").bind(id)
}

pub(in super::super) fn delete_pending_upload(id: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM pending_uploads WHERE chunk_id=?").bind(id)
}

pub(in super::super) fn owned_upload_pins(
    names: &[String],
    storage: Option<Uuid>,
) -> sqlx::QueryBuilder<MySql> {
    let mut query =
        sqlx::QueryBuilder::new("SELECT u.cache_key FROM pending_uploads u WHERE u.cache_key IN (");
    let mut list = query.separated(",");
    if names.is_empty() {
        list.push("NULL");
    }
    for name in names {
        list.push_bind(name.clone());
    }
    query.push(")");
    if let Some(storage) = storage {
        query
            .push(" AND EXISTS(SELECT 1 FROM chunks c WHERE c.id=u.chunk_id AND c.storage_id=")
            .push_bind(storage)
            .push(")");
    }
    query.push(" ORDER BY u.chunk_id FOR UPDATE");
    query
}

#[async_trait::async_trait]
impl UploadCacheStore for Backend {
    async fn bypass(&self, stream: Id) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT upload_cache_bypass FROM streams WHERE id=?")
                .bind(stream)
                .fetch_one(&self.pool)
                .await?,
        )
    }
    async fn disable(&self, stream: Id) -> Result<()> {
        sqlx::query("UPDATE streams SET upload_cache_bypass=TRUE WHERE id=?")
            .bind(stream)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn stage(&self, input: CacheStage<'_>) -> Result<bool> {
        transactions::cache_stage(&self.pool, input).await
    }
    async fn seal(&self, stream: Id, eof: bool) -> Result<bool> {
        transactions::cache_seal(&self.pool, stream, eof).await
    }
    async fn status(&self) -> Result<(Value, Vec<Value>)> {
        let (entries,bytes,oldest,failed): (i64,i64,Option<DateTime<Utc>>,i64)=sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(staged_size),0) AS SIGNED),MIN(created_at),CAST(COALESCE(SUM(last_error IS NOT NULL),0) AS SIGNED) FROM pending_uploads").fetch_one(&self.pool).await?;
        let rows=sqlx::query("SELECT p.chunk_id,p.pin_type,p.owner_id,p.created_at,u.last_error,u.attempts,u.next_retry_at FROM cache_pins p JOIN pending_uploads u USING(chunk_id) ORDER BY p.created_at,p.chunk_id,p.owner_id LIMIT 100").fetch_all(&self.pool).await?;
        let pins=rows.into_iter().map(|r| Ok(serde_json::json!({
            "chunk_id":r.try_get::<i64,_>("chunk_id")?.to_string(),"pin_type":r.try_get::<String,_>("pin_type")?,
            "owner_id":r.try_get::<Id,_>("owner_id")?,"created_at":r.try_get::<DateTime<Utc>,_>("created_at")?,
            "last_error":r.try_get::<Option<String>,_>("last_error")?,"attempts":r.try_get::<i32,_>("attempts")?,
            "next_retry_at":r.try_get::<DateTime<Utc>,_>("next_retry_at")?
        }))).collect::<sqlx::Result<Vec<_>>>()?;
        Ok((
            serde_json::json!({"entries":entries,"bytes":bytes,"oldest_at":oldest,"failed_entries":failed}),
            pins,
        ))
    }
    async fn flush(&self, node: Option<Uuid>) -> Result<Uuid> {
        transactions::cache_flush(&self.pool, node).await
    }
    async fn backlog(&self, _node: Option<Uuid>) -> Result<Value> {
        let (entries, bytes, oldest): (i64, i64, Option<DateTime<Utc>>) = sqlx::query_as("SELECT count(*),CAST(COALESCE(sum(size),0) AS SIGNED),min(created_at) FROM (SELECT staged_size AS size,created_at FROM pending_uploads UNION ALL SELECT r.size,r.created_at FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id) waiting").fetch_one(&self.pool).await?;
        Ok(serde_json::json!({"entries": entries, "bytes": bytes, "oldest_at": oldest}))
    }
    async fn enqueue(&self, urgent: &[Uuid]) -> Result<bool> {
        transactions::cache_enqueue(&self.pool, urgent).await
    }
    async fn next_attempt(&self) -> Result<Option<DateTime<Utc>>> {
        Ok(sqlx::query_scalar("SELECT MIN(p.next_retry_at) FROM pending_uploads p LEFT JOIN tasks t ON t.id=p.owner_task WHERE p.owner_task IS NULL OR (t.task_type='upload' AND t.state='failed')").fetch_one(&self.pool).await?)
    }
    async fn pause(&self, task: Execution) -> Result<()> {
        TaskControlStore::pause(self, task).await
    }
    async fn batch(&self, task: Execution, detail: &Value, flush: bool) -> Result<CacheBatch> {
        transactions::cache_batch(&self.pool, task, detail, flush).await
    }
    async fn metadata(&self, stream: Id) -> Result<Option<(Value, String)>> {
        sqlx::query("SELECT metadata,object_key FROM streams WHERE id=?")
            .bind(stream)
            .fetch_optional(&self.pool)
            .await?
            .map(|r| Ok((r.try_get("metadata")?, object_key(&r)?)))
            .transpose()
    }
    async fn processed(&self, task: Execution) -> Result<()> {
        sqlx::query("UPDATE tasks SET processed=processed+1,updated_at=CURRENT_TIMESTAMP(6) WHERE id=? AND state='running' AND execution_token=?").bind(task.id).bind(task.token).execute(&self.pool).await?;
        Ok(())
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
    async fn unused(&self, storage: Option<Uuid>) -> Result<Vec<Id>> {
        Ok(sqlx::query_scalar("SELECT u.chunk_id FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE (? IS NULL OR c.storage_id=?) AND u.owner_task IS NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=u.chunk_id) ORDER BY u.chunk_id LIMIT 128").bind(storage).bind(storage).fetch_all(&self.pool).await?)
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for Mapping {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        Ok(Self {
            chunk: sqlx::FromRow::from_row(row)?,
            offset_bytes: row.try_get("offset_bytes")?,
            pack_id: row.try_get("pack_id")?,
        })
    }
}

pub(in super::super) fn claim_task_candidate(
    class: i32,
    active: &[Uuid],
    draining: bool,
) -> sqlx::QueryBuilder<MySql> {
    // Keep the state prefix: retries span states and a filesort locks every candidate.
    let mut query = sqlx::QueryBuilder::new(
        "SELECT t.id FROM tasks t FORCE INDEX(tasks_work) WHERE t.source_node IS NULL AND (t.state='queued' OR (t.state='paused' AND EXISTS(SELECT 1 FROM redundancy_links rl WHERE rl.id=t.id AND rl.state='idle' AND (rl.next_reconcile_at<=UTC_TIMESTAMP(6) OR EXISTS(SELECT 1 FROM transfer_changes rc WHERE rc.task_id=rl.id)))) OR (t.state='failed' AND (CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation')) AS BINARY)='cdc' OR EXISTS(SELECT 1 FROM redundancy_links rl WHERE rl.id=t.id AND rl.state<>'paused')) AND t.updated_at<TIMESTAMPADD(SECOND,-30,CURRENT_TIMESTAMP(6)))) AND NOT EXISTS(SELECT 1 FROM raw_sources r WHERE r.processing_task=t.id AND (r.owner_stream IS NOT NULL OR EXISTS(SELECT 1 FROM raw_ranges e JOIN streams s ON s.id=e.stream_id WHERE e.source_id=r.id AND s.state='writing'))) AND CASE WHEN t.task_type IN ('pack','unpack') THEN 1 WHEN t.task_type IN ('upload','cache_flush','storage_migration','rewrite') THEN 2 ELSE 0 END=",
    );
    query.push_bind(class);
    query.push(" AND NOT EXISTS(SELECT 1 FROM mokyu_meta m WHERE m.remote_work AND m.deployment_mode='single_control' AND t.task_type IN ('integrity','pack','unpack','storage_migration','rewrite','sweep'))");
    if draining {
        query.push(" AND t.task_type IN ('upload','cache_flush') AND COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation')) AS BINARY),'')<>'cdc'");
    }
    if !active.is_empty() {
        query.push(" AND t.id NOT IN (");
        let mut ids = query.separated(",");
        for id in active {
            ids.push_bind(*id);
        }
        query.push(")");
    }
    query.push(" AND NOT EXISTS(SELECT 1 FROM mokyu_meta m WHERE (m.maintenance AND t.task_type IN ('pack','unpack','purge','gc','storage_migration','rewrite','usage')) OR EXISTS(SELECT 1 FROM maintenance_controls c WHERE c.policy=CASE WHEN t.task_type='pack' THEN CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.policy')) AS BINARY) ELSE CAST(t.task_type AS BINARY) END AND IF(c.policy='gc',m.gc_paused,c.paused)) OR (t.task_type='pack' AND CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.policy')) AS BINARY) IN ('pack','repack') AND m.pack_creation_paused)) ORDER BY t.state,t.updated_at,t.id LIMIT 1 FOR UPDATE SKIP LOCKED");
    query
}
