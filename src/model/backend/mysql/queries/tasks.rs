use super::*;

pub(in super::super) fn claim_task(id: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='running',started_at=COALESCE(started_at,CURRENT_TIMESTAMP(6)),execution_token=?,updated_at=CURRENT_TIMESTAMP(6) WHERE id=? AND (state='queued' OR (state='paused' AND EXISTS(SELECT 1 FROM redundancy_links rl WHERE rl.id=tasks.id AND rl.state='idle' AND (rl.next_reconcile_at<=UTC_TIMESTAMP(6) OR EXISTS(SELECT 1 FROM transfer_changes rc WHERE rc.task_id=rl.id)))) OR (state='failed' AND (CAST(JSON_UNQUOTE(JSON_EXTRACT(detail,'$.operation')) AS BINARY)='cdc' OR EXISTS(SELECT 1 FROM redundancy_links rl WHERE rl.id=tasks.id AND rl.state<>'paused')) AND updated_at<TIMESTAMPADD(SECOND,-30,CURRENT_TIMESTAMP(6))))").bind(token).bind(id)
}

pub(in super::super) fn task_record(id: Uuid) -> Rows<'static, TaskRecord> {
    sqlx::query_as("SELECT task_type,state,bucket_id,progress_cursor,detail,execution_token FROM tasks WHERE id=?").bind(id)
}

pub(in super::super) fn pause_owned_task(task: Execution, error: Option<&str>) -> Statement<'_> {
    sqlx::query("UPDATE tasks SET state='paused',error=COALESCE(?,error),updated_at=CURRENT_TIMESTAMP(6) WHERE id=? AND state='running' AND execution_token=?").bind(error).bind(task.id).bind(task.token)
}

pub(in super::super) fn storage_profile_revision(id: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT revision FROM storage_profiles WHERE storage_id=?").bind(id)
}

pub(in super::super) fn lock_task_finish(task: Execution) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT task_type,state FROM tasks WHERE id=? AND execution_token=? FOR UPDATE")
        .bind(task.id)
        .bind(task.token)
}

pub(in super::super) fn defer_pack_changes(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE pack_changes SET next_check_at=(SELECT TIMESTAMPADD(SECOND,(CAST(LEFT(sp.pack_reuse_interval,LENGTH(sp.pack_reuse_interval)-1) AS UNSIGNED)*CASE RIGHT(sp.pack_reuse_interval,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END),UTC_TIMESTAMP(6)) FROM packs p JOIN storage_profiles sp ON sp.storage_id=p.storage_id WHERE p.id=pack_changes.pack_id) WHERE pack_id IN (SELECT c.pack_id FROM pack_inputs i JOIN chunks c ON c.id=i.chunk_id WHERE i.task_id=?)").bind(id)
}

pub(in super::super) fn defer_pack_maintenance(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE pack_maintenance SET next_check_at=(SELECT TIMESTAMPADD(SECOND,(CAST(LEFT(sp.pack_reuse_interval,LENGTH(sp.pack_reuse_interval)-1) AS UNSIGNED)*CASE RIGHT(sp.pack_reuse_interval,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END),UTC_TIMESTAMP(6)) FROM streams s JOIN buckets b ON b.id=s.bucket_id JOIN storage_profiles sp ON sp.storage_id=b.storage_id WHERE s.id=pack_maintenance.stream_id) WHERE stream_id IN (SELECT e.stream_id FROM extents e JOIN pack_inputs i ON i.chunk_id=e.chunk_id WHERE i.task_id=?)").bind(id)
}

pub(in super::super) fn backoff_pending_uploads(
    task: Uuid,
    error: &str,
    retain: bool,
) -> Statement<'_> {
    // MySQL assigns left to right; retry delay uses the previous attempt count.
    sqlx::query("UPDATE pending_uploads SET next_retry_at=TIMESTAMPADD(SECOND,LEAST(300,POW(2,LEAST(attempts,8))),CURRENT_TIMESTAMP(6)),attempts=LEAST(attempts+1,30),last_error=?,owner_task=IF(?,owner_task,NULL) WHERE owner_task=?").bind(error).bind(retain).bind(task)
}

pub(in super::super) fn finish_task<'q>(
    task: Execution,
    state: &'q str,
    error: Option<&'q str>,
) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET state=?,error=COALESCE(?,error),execution_token=NULL,updated_at=CURRENT_TIMESTAMP(6) WHERE id=? AND execution_token=?").bind(state).bind(error).bind(task.id).bind(task.token)
}

#[async_trait::async_trait]
impl TaskExecutionStore for Backend {
    async fn claim(&self, input: TaskClaim<'_>) -> Result<Option<Uuid>> {
        transactions::claim_task(&self.pool, input).await
    }
    async fn load(&self, id: Uuid) -> Result<TaskRecord> {
        Ok(task_record(id).fetch_one(&self.pool).await?)
    }
    async fn fence(&self, expected_mode: &str, id: Uuid, token: Uuid) -> Result<bool> {
        let Some(row) = task_record(id).fetch_optional(&self.pool).await? else {
            return Ok(false);
        };
        if row.state != "running" || row.execution_token != Some(token) {
            return Ok(false);
        }
        let execution = Execution { id, token };
        if expected_mode != super::replication::mode().fetch_one(&self.pool).await? {
            pause_owned_task(execution, None)
                .execute(&self.pool)
                .await?;
            return Ok(false);
        }
        if let (Some(storage), Some(revision)) = (
            row.detail["storage_id"]
                .as_str()
                .and_then(|s| s.parse::<Uuid>().ok()),
            row.detail["profile_revision"].as_i64(),
        ) && storage_profile_revision(storage)
            .fetch_optional(&self.pool)
            .await?
            != Some(revision)
        {
            pause_owned_task(
                execution,
                Some("storage profile changed; resume to refresh"),
            )
            .execute(&self.pool)
            .await?;
            return Ok(false);
        }
        Ok(true)
    }
    async fn confirm(&self, id: Uuid, token: Uuid) -> Result<bool> {
        TaskControlStore::running(self, Execution { id, token }).await
    }
    async fn finish(&self, input: TaskFinish) -> Result<()> {
        transactions::finish_task(&self.pool, input).await
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for TaskRecord {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        Ok(Self {
            task_type: row.try_get("task_type")?,
            state: row.try_get("state")?,
            bucket_id: row.try_get("bucket_id")?,
            cursor: row.try_get("progress_cursor")?,
            detail: row.try_get("detail")?,
            execution_token: row.try_get("execution_token")?,
        })
    }
}

pub(in super::super) fn task_value(row: sqlx::mysql::MySqlRow, detailed: bool) -> Result<Value> {
    let mut value = serde_json::json!({
        "id": row.try_get::<Uuid,_>("id")?,
        "task_type": row.try_get::<String,_>("task_type")?,
        "bucket_id": row.try_get::<Option<Uuid>,_>("bucket_id")?,
        "state": row.try_get::<String,_>("state")?,
        "cursor": row.try_get::<Option<String>,_>("progress_cursor")?,
        "processed": row.try_get::<i64,_>("processed")?,
        "detail": row.try_get::<Value,_>("detail")?,
        "error": row.try_get::<Option<String>,_>("error")?,
        "created_by_label": row.try_get::<String,_>("created_by_label")?,
        "source": row.try_get::<String,_>("source")?,
        "created_at": row.try_get::<DateTime<Utc>,_>("created_at")?,
        "updated_at": row.try_get::<DateTime<Utc>,_>("updated_at")?,
        "started_at": row.try_get::<Option<DateTime<Utc>>,_>("started_at")?,
    });
    if detailed {
        value["bucket_name"] = row.try_get::<Option<String>, _>("bucket_name")?.into();
        value["blocked"] = row.try_get::<Option<String>, _>("blocked")?.into();
    }
    Ok(value)
}

const JOB_SQL: &str = "/*M!100603 SET STATEMENT max_statement_time=5 FOR */ SELECT /*+ MAX_EXECUTION_TIME(5000) */ t.*,b.name AS bucket_name,CASE WHEN c.paused OR (c.policy='gc' AND m.gc_paused) THEN 'PolicyPaused' WHEN m.maintenance AND t.task_type IN ('pack','unpack','purge','gc','upload','storage_migration','rewrite','usage') THEN 'MaintenanceMode' WHEN m.pack_creation_paused AND t.task_type='pack' AND CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.policy')) AS BINARY) IN ('pack','repack') THEN 'PackCreationStopped' END AS blocked FROM tasks t LEFT JOIN buckets b ON b.id=t.bucket_id CROSS JOIN mokyu_meta m LEFT JOIN maintenance_controls c ON c.policy=CASE WHEN t.task_type='pack' THEN CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.policy')) AS BINARY) ELSE CAST(t.task_type AS BINARY) END";

pub(in super::super) fn task_page(
    q: JobQuery,
    cursor: Option<(DateTime<Utc>, Uuid)>,
    limit: i64,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(JOB_SQL);
    query.push(" WHERE TRUE");
    if let Some(state) = q.state {
        match state.as_str() {
            "running" => query
                .push(" AND (t.state='running' OR t.state='paused' AND t.error='replica_pending')"),
            "paused" => {
                query.push(" AND t.state='paused' AND COALESCE(t.error,'')<>'replica_pending'")
            }
            _ => query.push(" AND t.state=").push_bind(state),
        };
    }
    if let Some(policy) = q.policy {
        query.push(" AND t.policy=").push_bind(policy);
    }
    if let Some(bucket) = q.bucket {
        query.push(" AND t.bucket_id=").push_bind(bucket);
    }
    if let Some(storage) = q.storage {
        query
            .push(" AND (b.storage_id=")
            .push_bind(storage)
            .push(" OR CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.storage_id')) AS BINARY)=")
            .push_bind(storage.to_string())
            .push(" OR JSON_CONTAINS(t.detail,")
            .push_bind(serde_json::json!([{"storage_id":storage}]))
            .push(",'$.storages'))");
    }
    if let Some(actor) = q.actor {
        query
            .push(" AND INSTR(LOWER(t.created_by_label),LOWER(")
            .push_bind(actor)
            .push("))>0");
    }
    if let Some((time, id)) = cursor {
        query
            .push(" AND (t.created_at<")
            .push_bind(time)
            .push(" OR (t.created_at=")
            .push_bind(time)
            .push(" AND t.id<")
            .push_bind(id)
            .push("))");
    }
    query
        .push(" ORDER BY t.created_at DESC,t.id DESC LIMIT ")
        .push_bind(limit);
    query
}

pub(in super::super) fn task_view(id: Uuid) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(JOB_SQL);
    query.push(" WHERE t.id=").push_bind(id);
    query
}

#[async_trait::async_trait]
impl TaskControlStore for Backend {
    async fn unpack_status(&self, pack: Id) -> Result<Option<Value>> {
        sqlx::query("SELECT * FROM tasks WHERE task_type='unpack' AND CAST(JSON_UNQUOTE(JSON_EXTRACT(detail,'$.pack_id')) AS BINARY)=? ORDER BY created_at DESC,id DESC LIMIT 1")
            .bind(pack.to_string()).fetch_optional(&self.pool).await?.map(|row| task_value(row,false)).transpose()
    }
    async fn rewrite_status(&self, storage: Uuid) -> Result<Option<Value>> {
        sqlx::query("SELECT * FROM tasks WHERE task_type='rewrite' AND CAST(JSON_UNQUOTE(JSON_EXTRACT(detail,'$.storage_id')) AS BINARY)=? ORDER BY created_at DESC,id DESC LIMIT 1")
            .bind(storage.to_string()).fetch_optional(&self.pool).await?.map(|row| task_value(row,false)).transpose()
    }
    async fn rewrite_task(&self, storage: Uuid) -> Result<Option<Uuid>> {
        Ok(sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='rewrite' AND CAST(JSON_UNQUOTE(JSON_EXTRACT(detail,'$.storage_id')) AS BINARY)=? ORDER BY created_at DESC,id DESC LIMIT 1")
            .bind(storage.to_string()).fetch_optional(&self.pool).await?)
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
    async fn view(&self, principal: &Principal, id: Uuid) -> Result<Value> {
        transactions::task_view(&self.pool, principal, id).await
    }
    async fn all(&self) -> Result<Vec<Value>> {
        sqlx::query("SELECT * FROM tasks ORDER BY created_at DESC,id DESC LIMIT 100")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| task_value(row, false))
            .collect()
    }
    async fn raw(&self, id: Uuid) -> Result<Option<Value>> {
        sqlx::query("SELECT * FROM tasks WHERE id=?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| task_value(row, false))
            .transpose()
    }
    async fn other_integrity(&self, id: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='integrity' AND JSON_EXTRACT(detail,'$.source_operation') IS NULL AND COALESCE(JSON_UNQUOTE(JSON_EXTRACT(detail,'$.operation')),'')<>'external_test' AND state IN ('queued','running') AND id<>?)").bind(id).fetch_one(&self.pool).await?)
    }
    async fn enqueue(&self, task_type: &str, bucket: Option<Uuid>, detail: Value) -> Result<Uuid> {
        transactions::enqueue_task(&mut *self.pool.acquire().await?, task_type, bucket, detail)
            .await
    }
    async fn sweep_preview(&self, id: Uuid) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT detail FROM tasks WHERE id=? AND task_type='sweep' AND state='completed'",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?)
    }
    async fn pause(&self, task: Execution) -> Result<()> {
        sqlx::query("UPDATE tasks SET state='paused',updated_at=CURRENT_TIMESTAMP(6) WHERE id=? AND execution_token=? AND state IN ('queued','running')").bind(task.id).bind(task.token).execute(&self.pool).await?;
        Ok(())
    }
    async fn running(&self, task: Execution) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=? AND state='running' AND execution_token=?)").bind(task.id).bind(task.token).fetch_one(&self.pool).await?)
    }
    async fn complete(&self, task: Execution) -> Result<()> {
        sqlx::query("UPDATE tasks SET state='completed',updated_at=CURRENT_TIMESTAMP(6) WHERE id=? AND execution_token=? AND state='running'").bind(task.id).bind(task.token).execute(&self.pool).await?;
        Ok(())
    }
    async fn progress(
        &self,
        task: Execution,
        cursor: &str,
        processed: i64,
        detail: Value,
    ) -> Result<()> {
        sqlx::query("UPDATE tasks SET progress_cursor=?,processed=processed+?,detail=?,updated_at=CURRENT_TIMESTAMP(6) WHERE id=? AND execution_token=? AND state='running'").bind(cursor).bind(processed).bind(detail).bind(task.id).bind(task.token).execute(&self.pool).await?;
        Ok(())
    }
    async fn load(&self, id: Uuid) -> Result<Option<(bool, String, Value)>> {
        Ok(
            sqlx::query_as::<_, (String, Value)>("SELECT task_type,detail FROM tasks WHERE id=?")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?
                .map(|(task_type, detail)| (false, task_type, detail)),
        )
    }
    async fn change_state(&self, id: Uuid, parent: bool, resume: bool) -> Result<bool> {
        ensure!(!parent, "MySQL does not support child tasks");
        let query = if resume {
            "UPDATE tasks t LEFT JOIN storage_profiles p ON p.storage_id=UNHEX(REPLACE(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.storage_id')),'-','')) SET t.state='queued',t.error=NULL,t.updated_at=CURRENT_TIMESTAMP(6),t.detail=IF(p.storage_id IS NOT NULL AND JSON_CONTAINS_PATH(t.detail,'one','$.profile_revision'),JSON_SET(t.detail,'$.profile_revision',p.revision),t.detail) WHERE t.id=? AND t.state IN ('paused','failed') AND COALESCE(t.error,'') NOT IN ('replica_pending','replica_drain') AND COALESCE(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.test_stopped')),'false')<>'true'"
        } else {
            "UPDATE tasks SET state='paused',updated_at=CURRENT_TIMESTAMP(6) WHERE id=? AND state IN ('queued','running')"
        };
        Ok(sqlx::query(query)
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected()
            == 1)
    }
}
