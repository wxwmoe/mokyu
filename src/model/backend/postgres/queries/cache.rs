use super::*;

pub(in super::super) fn cache_bypass(stream: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT upload_cache_bypass FROM streams WHERE id=$1").bind(stream)
}

pub(in super::super) fn bypass_upload_cache(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET upload_cache_bypass=true WHERE id=$1").bind(stream)
}

pub(in super::super) fn lock_cache_source(chunk: Id) -> Rows<'static, (String, Option<Id>)> {
    sqlx::query_as("SELECT state,pack_id FROM chunks WHERE id=$1 FOR UPDATE").bind(chunk)
}

pub(in super::super) fn insert_pending_upload<'q>(
    input: &'q CacheStage<'q>,
    source: Option<Id>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO pending_uploads(chunk_id,stream_id,offset_bytes,staged_size,source_pack_id,staging_storage_id,staging_object_key,next_retry_at,node_id,encoding_policy) VALUES($1,$2,$3,$4,$5,$6,$7,now()+$8*interval '1 second',$9,$10)").bind(input.chunk.id).bind(input.stream).bind(input.offset).bind(input.size).bind(source).bind(input.storage).bind(input.key).bind(input.timeout as f64).bind(input.node.map(|n| n.0.node_id)).bind(input.encoding)
}

pub(in super::super) fn insert_upload_pin(
    chunk: Id,
    source: Option<Id>,
    stream: Id,
) -> Statement<'static> {
    sqlx::query("INSERT INTO cache_pins(chunk_id,pin_type,owner_id) VALUES($1,$2,$3)")
        .bind(chunk)
        .bind(if source.is_some() { "pack" } else { "upload" })
        .bind(stream)
}

pub(in super::super) fn ready_cached_chunk(chunk: Id) -> Statement<'static> {
    sqlx::query(
        "UPDATE chunks SET state='ready',unreferenced_at=NULL,owner_stream=NULL WHERE id=$1",
    )
    .bind(chunk)
}

pub(in super::super) fn insert_cached_extent<'q>(input: &'q CacheStage<'q>) -> Statement<'q> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES($1,$2,$3,$4)")
        .bind(input.stream)
        .bind(input.offset)
        .bind(input.chunk.raw_size)
        .bind(input.chunk.id)
}

pub(in super::super) fn resize_cached_stream(stream: Id, size: i64) -> Statement<'static> {
    sqlx::query("UPDATE streams SET size=GREATEST(size,$2),touched_at=now() WHERE id=$1 AND state='writing'").bind(stream).bind(size)
}

pub(in super::super) fn seal_stream_uploads(stream: Id) -> sqlx::RawSql {
    // A typed UUID allows a fresh plan in one roundtrip as the queue grows.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "UPDATE pending_uploads SET next_retry_at=LEAST(next_retry_at,now()) WHERE owner_task IS NULL AND chunk_id IN (SELECT chunk_id FROM extents WHERE stream_id='{stream}'::uuid)"
    )))
}

pub(in super::super) fn unsealed_uploads(stream: Id) -> Rows<'static, (Id, i64, i32, String)> {
    sqlx::query_as("SELECT p.chunk_id,p.offset_bytes,c.raw_size,COALESCE(p.encoding_policy->>'pack_max_size',sp.pack_max_size) FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id JOIN storage_profiles sp ON sp.storage_id=c.storage_id WHERE p.stream_id=$1 AND p.owner_task IS NULL ORDER BY p.offset_bytes LIMIT 1026").bind(stream)
}

pub(in super::super) fn seal_upload_chunks<'q>(ids: &'q [Id]) -> Statement<'q> {
    sqlx::query("UPDATE pending_uploads SET next_retry_at=LEAST(next_retry_at,now()) WHERE chunk_id=ANY($1) AND owner_task IS NULL").bind(ids)
}

pub(in super::super) fn upload_cache_summary() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT jsonb_build_object('entries',count(*),'bytes',COALESCE(sum(staged_size),0),'oldest_at',min(created_at),'failed_entries',count(*) FILTER(WHERE last_error IS NOT NULL)) FROM pending_uploads",
    )
}

pub(in super::super) fn upload_cache_pins() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT jsonb_build_object('chunk_id',p.chunk_id::text,'pin_type',p.pin_type,'owner_id',p.owner_id,'created_at',p.created_at,'last_error',u.last_error,'attempts',u.attempts,'next_retry_at',u.next_retry_at) FROM cache_pins p JOIN pending_uploads u USING(chunk_id) ORDER BY p.created_at,p.chunk_id LIMIT 100",
    )
}

pub(in super::super) fn retryable_upload() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT p.owner_task FROM pending_uploads p JOIN tasks t ON t.id=p.owner_task WHERE p.next_retry_at<=now() AND t.task_type='upload' AND t.state='failed' ORDER BY p.next_retry_at,p.chunk_id LIMIT 1 FOR UPDATE OF t SKIP LOCKED",
    )
}

pub(in super::super) fn queue_upload_retry(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='queued',error=NULL,updated_at=now() WHERE id=$1 AND state='failed'").bind(task)
}

pub(super) fn write_delay(
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

pub(in super::super) fn due_upload(urgent: &[Uuid]) -> sqlx::QueryBuilder<sqlx::Postgres> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT p.chunk_id,p.stream_id,p.source_pack_id,p.node_id FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id JOIN storage_profiles sp ON sp.storage_id=c.storage_id WHERE p.owner_task IS NULL AND p.next_retry_at<=clock_timestamp()",
    );
    write_delay(&mut query, "p.created_at", "c.storage_id", urgent);
    query.push(" ORDER BY CASE WHEN p.created_at+(sp.write_backlog_max_age)*interval '1 second'<=clock_timestamp() THEN 0 ELSE 1 END,p.next_retry_at,p.chunk_id LIMIT 1 FOR UPDATE OF p SKIP LOCKED");
    query
}

pub(in super::super) fn pending_upload_segment(
    first: Id,
    stream: Option<Id>,
    source: Option<Id>,
) -> Rows<'static, (Id, i64, i32)> {
    sqlx::query_as("SELECT p.chunk_id,p.offset_bytes,c.raw_size FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE p.encoding_policy IS NOT DISTINCT FROM (SELECT encoding_policy FROM pending_uploads WHERE chunk_id=$1) AND p.owner_task IS NULL AND p.node_id IS NOT DISTINCT FROM (SELECT node_id FROM pending_uploads WHERE chunk_id=$1) AND (p.chunk_id=$1 OR ($2::uuid IS NOT NULL AND p.stream_id=$2 AND p.source_pack_id IS NOT DISTINCT FROM $3)) ORDER BY p.offset_bytes LIMIT 1026").bind(first).bind(stream).bind(source)
}

pub(in super::super) fn claim_pending_uploads<'q>(ids: &'q [Id], task: Uuid) -> Statement<'q> {
    sqlx::query("UPDATE pending_uploads SET owner_task=$2 WHERE chunk_id=ANY($1)")
        .bind(ids)
        .bind(task)
}

pub(in super::super) fn claim_upload_segment(ids: &[Id], first: Id, task: Uuid) -> Statement<'_> {
    if ids.len() == 1 {
        return claim_pending_uploads(ids, task);
    }
    // Each scheduler already owns its first row. Keep just that row when another owns part of the batch.
    sqlx::query(
        "WITH available AS MATERIALIZED (
            SELECT chunk_id FROM pending_uploads WHERE chunk_id=ANY($1) AND owner_task IS NULL
            ORDER BY chunk_id FOR UPDATE SKIP LOCKED
        )
        UPDATE pending_uploads p SET owner_task=$2 FROM available a
        WHERE p.chunk_id=a.chunk_id AND (a.chunk_id=$3 OR (SELECT count(*) FROM available)=cardinality($1))",
    )
    .bind(ids)
    .bind(task)
    .bind(first)
}

pub(in super::super) fn pause_upload_task(task: Execution) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='paused',updated_at=now() WHERE id=$1 AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())").bind(task.id)
        .bind(task.token)
}

pub(in super::super) fn flush_candidate<'q>(detail: &'q Value, task: Uuid) -> Scalar<'q, Id> {
    sqlx::query_scalar("SELECT chunk_id FROM pending_uploads u WHERE u.node_id IS NOT DISTINCT FROM ($1->>'node_id')::uuid AND created_at<=($1->>'upper_time')::timestamptz AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=u.chunk_id) AND (owner_task IS NULL OR owner_task=$2 OR owner_task IN (SELECT id FROM tasks WHERE state IN ('paused','failed'))) ORDER BY created_at,chunk_id LIMIT 1").bind(detail).bind(task)
}

pub(in super::super) fn flush_pending<'q>(detail: &'q Value) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE (($1->>'coordinator')::boolean IS TRUE OR node_id IS NOT DISTINCT FROM ($1->>'node_id')::uuid) AND created_at<=($1->>'upper_time')::timestamptz)").bind(detail)
}

pub(in super::super) fn upload_is_durable(chunk: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE chunk_id=$1 AND (EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=$1 AND state='ready') OR (c.pack_id IS NOT NULL AND c.pack_id IS DISTINCT FROM u.source_pack_id)))").bind(chunk)
}

pub(in super::super) fn upload_is_unused(chunk: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=$1)").bind(chunk)
}

pub(in super::super) fn pending_upload(
    chunk: Id,
    timeout: u64,
) -> Rows<'static, (Option<Id>, i64, Option<Id>, bool)> {
    sqlx::query_as("SELECT stream_id,offset_bytes,source_pack_id,created_at<=now()-$2*interval '1 second' FROM pending_uploads WHERE chunk_id=$1").bind(chunk).bind(timeout as f64)
}

pub(in super::super) fn pending_pack_uploads(pack: Id, task: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT chunk_id FROM pending_uploads WHERE source_pack_id=$1 AND (owner_task IS NULL OR owner_task=$2)").bind(pack).bind(task)
}

pub(in super::super) fn pending_upload_mappings(
    first: Id,
    stream: Option<Id>,
    offset: i64,
    task: Uuid,
    flush: bool,
) -> Rows<'static, crate::model::Mapping> {
    sqlx::query_as("SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at,p.offset_bytes,c.pack_id FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE p.encoding_policy IS NOT DISTINCT FROM (SELECT encoding_policy FROM pending_uploads WHERE chunk_id=$1) AND p.node_id IS NOT DISTINCT FROM (SELECT node_id FROM pending_uploads WHERE chunk_id=$1) AND (p.chunk_id=$1 OR ($2::uuid IS NOT NULL AND p.stream_id=$2 AND p.offset_bytes>$3 AND p.source_pack_id IS NULL)) AND (p.owner_task IS NULL OR p.owner_task=$4 OR ($5 AND p.owner_task IN (SELECT id FROM tasks WHERE state IN ('paused','failed')))) ORDER BY p.offset_bytes LIMIT 1026").bind(first).bind(stream).bind(offset).bind(task).bind(flush)
}

pub(in super::super) fn upload_stream_metadata(stream: Id) -> Rows<'static, (Value, String)> {
    sqlx::query_as("SELECT metadata,object_key FROM streams WHERE id=$1").bind(stream)
}

pub(in super::super) fn lock_cache_inputs<'q>(
    ids: &'q [Id],
    task: Uuid,
    flush: bool,
) -> Rows<'q, (Id, Id, Option<Id>, Option<Id>, i64, Option<Id>)> {
    sqlx::query_as("SELECT c.id,c.current_encoding_id,c.pack_id,p.stream_id,p.offset_bytes,p.source_pack_id FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE p.chunk_id=ANY($1) AND (p.owner_task IS NULL OR p.owner_task=$2 OR ($3 AND p.owner_task IN (SELECT id FROM tasks WHERE state IN ('paused','failed')))) ORDER BY c.id FOR UPDATE OF p")
        .bind(ids).bind(task).bind(flush)
}

pub(in super::super) fn count_upload_batch(task: Execution) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET processed=processed+1,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())").bind(task.id)
        .bind(task.token)
}

pub(in super::super) fn unclaim_uploads(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE pending_uploads SET owner_task=NULL WHERE owner_task=$1").bind(task)
}

pub(in super::super) fn complete_upload_task(task: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET state='completed',updated_at=now() WHERE id=$1 AND state='running'",
    )
    .bind(task)
}

pub(in super::super) fn lock_pending_chunks<'q>(
    ids: &'q [Id],
) -> Rows<'q, (Id, Option<String>, Option<Uuid>)> {
    sqlx::query_as("SELECT c.id,u.staging_object_key,CASE WHEN c.state='ready' THEN COALESCE((SELECT e.encoding_id FROM chunk_encodings e WHERE e.encoding_id=c.current_encoding_id AND e.state='ready'),CASE WHEN EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.state='ready') THEN c.id END) END FROM chunks c LEFT JOIN pending_uploads u ON u.chunk_id=c.id WHERE c.id=ANY($1) ORDER BY c.id FOR UPDATE OF c").bind(ids)
}

pub(in super::super) fn pending_staging_objects<'q>(ids: &'q [Id]) -> Rows<'q, (Id, Uuid, String)> {
    sqlx::query_as("SELECT chunk_id,staging_storage_id,staging_object_key FROM pending_uploads WHERE chunk_id=ANY($1) AND staging_storage_id IS NOT NULL AND staging_object_key LIKE '__mokyu/multipart/%'").bind(ids)
}

pub(in super::super) fn can_release_pending(
    chunk: Id,
    existing_pack: bool,
) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=$1 AND state='ready') OR EXISTS(SELECT 1 FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE c.id=$1 AND p.state='ready' AND ($2 OR c.pack_id IS DISTINCT FROM (SELECT source_pack_id FROM pending_uploads WHERE chunk_id=$1))) OR NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=$1)").bind(chunk).bind(existing_pack)
}

pub(in super::super) fn schedule_uploaded_pack(chunk: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO pack_maintenance(stream_id,reason) SELECT DISTINCT e.stream_id,'pack' FROM extents e JOIN streams s ON s.id=e.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE e.chunk_id=$1 AND s.state='ready' AND c.pack_id IS NULL ON CONFLICT(stream_id) DO UPDATE SET cursor=0,generation=pack_maintenance.generation+1,updated_at=now()").bind(chunk)
}

pub(in super::super) fn delete_pending_upload(chunk: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM pending_uploads WHERE chunk_id=$1").bind(chunk)
}

pub(in super::super) fn backoff_pending_uploads<'q>(task: Uuid, error: &'q str) -> Statement<'q> {
    sqlx::query("UPDATE pending_uploads SET owner_task=CASE WHEN (SELECT task_type FROM tasks WHERE id=$1)='upload' THEN owner_task ELSE NULL END,attempts=LEAST(attempts+1,30),last_error=$2,next_retry_at=now()+LEAST(300,power(2,LEAST(attempts,8))) * interval '1 second' WHERE owner_task=$1").bind(task).bind(error)
}

pub(in super::super) fn lock_upload_pins<'q>(
    names: &'q [String],
    storage: Option<Uuid>,
) -> Statement<'q> {
    sqlx::query("SELECT u.chunk_id FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE u.staging_object_key=ANY($1) AND ($2::uuid IS NULL OR c.storage_id=$2) ORDER BY u.chunk_id FOR UPDATE").bind(names).bind(storage)
}

pub(in super::super) fn owned_upload_pins<'q>(
    names: &'q [String],
    storage: Option<Uuid>,
) -> Scalar<'q, String> {
    sqlx::query_scalar("SELECT u.staging_object_key FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE u.staging_object_key=ANY($1) AND ($2::uuid IS NULL OR c.storage_id=$2)").bind(names).bind(storage)
}

pub(in super::super) fn unused_pending_uploads(storage: Option<Uuid>) -> Scalar<'static, Id> {
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

pub(in super::super) fn retire_task_packs(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE packs SET state='retired',unreferenced_at=now() WHERE owner_task=$1 AND state='preparing'").bind(task)
}

pub(in super::super) fn retire_task_encodings(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=now() WHERE owner_task=$1 AND state='uploading'").bind(task)
}

pub(in super::super) fn release_task_inputs(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM pack_inputs WHERE task_id=$1").bind(task)
}
