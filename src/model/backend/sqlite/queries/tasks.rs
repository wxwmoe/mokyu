use super::*;

const JOB_SQL: &str = "SELECT t.*,b.name AS bucket_name,CASE WHEN c.paused OR (c.policy='gc' AND m.gc_paused) THEN 'PolicyPaused' WHEN m.maintenance AND t.task_type IN ('pack','unpack','purge','gc','upload','storage_migration','rewrite','usage') THEN 'MaintenanceMode' WHEN m.pack_creation_paused AND t.task_type='pack' AND json_extract(t.detail,'$.policy') IN ('pack','repack') THEN 'PackCreationStopped' END AS blocked FROM tasks t LEFT JOIN buckets b ON b.id=t.bucket_id CROSS JOIN mokyu_meta m LEFT JOIN maintenance_controls c ON c.policy=CASE WHEN t.task_type='pack' THEN json_extract(t.detail,'$.policy') ELSE t.task_type END";

pub(in super::super) fn task_page(
    q: &JobQuery,
    cursor: Option<(DateTime<Utc>, Uuid)>,
    limit: i64,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(JOB_SQL);
    query.push(" WHERE TRUE");
    if let Some(state) = &q.state {
        match state.as_str() {
            "running" => query
                .push(" AND (t.state='running' OR t.state='paused' AND t.error='replica_pending')"),
            "paused" => {
                query.push(" AND t.state='paused' AND COALESCE(t.error,'')<>'replica_pending'")
            }
            _ => query.push(" AND t.state=").push_bind(state),
        };
    }
    if let Some(policy) = &q.policy {
        query.push(" AND t.policy=").push_bind(policy);
    }
    if let Some(bucket) = q.bucket {
        query.push(" AND t.bucket_id=").push_bind(bucket);
    }
    if let Some(storage) = q.storage {
        query.push(" AND (b.storage_id=").push_bind(storage)
            .push(" OR json_extract(t.detail,'$.storage_id')=").push_bind(storage.to_string())
            .push(" OR EXISTS(SELECT 1 FROM json_each(t.detail,'$.storages') item WHERE json_extract(IIF(item.type='object',item.value,'{}'),'$.storage_id')=").push_bind(storage.to_string()).push("))");
    }
    if let Some((time, id)) = cursor {
        query
            .push(" AND (t.created_at<")
            .push_bind(time.timestamp_micros())
            .push(" OR (t.created_at=")
            .push_bind(time.timestamp_micros())
            .push(" AND t.id<")
            .push_bind(id)
            .push("))");
    }
    query
        .push(" ORDER BY t.created_at DESC,t.id DESC LIMIT ")
        .push_bind(limit);
    query
}

pub(in super::super) fn task_view(id: Uuid) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(JOB_SQL);
    query.push(" WHERE t.id=").push_bind(id);
    query
}

#[async_trait::async_trait]
impl TaskControlStore for Backend {
    async fn unpack_status(&self, pack: Id) -> Result<Option<Value>> {
        sqlx::query("SELECT * FROM tasks WHERE task_type='unpack' AND detail->>'$.pack_id'=? ORDER BY created_at DESC,id DESC LIMIT 1")
            .bind(pack).fetch_optional(&self.read).await?.map(|row| task_value(row,false)).transpose()
    }
    async fn rewrite_status(&self, storage: Uuid) -> Result<Option<Value>> {
        sqlx::query("SELECT * FROM tasks WHERE task_type='rewrite' AND detail->>'$.storage_id'=? ORDER BY created_at DESC,id DESC LIMIT 1")
            .bind(storage.to_string()).fetch_optional(&self.read).await?.map(|row| task_value(row,false)).transpose()
    }
    async fn rewrite_task(&self, storage: Uuid) -> Result<Option<Uuid>> {
        Ok(sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='rewrite' AND detail->>'$.storage_id'=? ORDER BY created_at DESC,id DESC LIMIT 1")
            .bind(storage.to_string()).fetch_optional(&self.read).await?)
    }
    async fn page(
        &self,
        principal: &Principal,
        filter: JobQuery,
        cursor: Option<(DateTime<Utc>, Uuid)>,
        limit: i64,
    ) -> Result<Vec<Value>> {
        transactions::task_page(&self.read, principal, filter, cursor, limit).await
    }
    async fn view(&self, principal: &Principal, id: Uuid) -> Result<Value> {
        transactions::task_view(&self.read, principal, id).await
    }
    async fn all(&self) -> Result<Vec<Value>> {
        sqlx::query("SELECT * FROM tasks ORDER BY created_at DESC,id DESC LIMIT 100")
            .fetch_all(&self.read)
            .await?
            .into_iter()
            .map(|row| task_value(row, false))
            .collect()
    }
    async fn raw(&self, id: Uuid) -> Result<Option<Value>> {
        sqlx::query("SELECT * FROM tasks WHERE id=?")
            .bind(id)
            .fetch_optional(&self.read)
            .await?
            .map(|row| task_value(row, false))
            .transpose()
    }
    async fn other_integrity(&self, id: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='integrity' AND json_extract(detail,'$.source_operation') IS NULL AND COALESCE(json_extract(detail,'$.operation'),'')<>'external_test' AND state IN ('queued','running') AND id<>?)").bind(id).fetch_one(&self.read).await?)
    }
    async fn enqueue(&self, task_type: &str, bucket: Option<Uuid>, detail: Value) -> Result<Uuid> {
        transactions::enqueue_task(&mut *self.write.acquire().await?, task_type, bucket, detail)
            .await
    }
    async fn sweep_preview(&self, id: Uuid) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT detail FROM tasks WHERE id=? AND task_type='sweep' AND state='completed'",
        )
        .bind(id)
        .fetch_optional(&self.read)
        .await?)
    }
    async fn pause(&self, task: Execution) -> Result<()> {
        sqlx::query("UPDATE tasks SET state='paused',updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND execution_token=? AND state IN ('queued','running')").bind(task.id).bind(task.token).execute(&self.write).await?;
        Ok(())
    }
    async fn running(&self, task: Execution) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=? AND state='running' AND execution_token=?)").bind(task.id).bind(task.token).fetch_one(&self.read).await?)
    }
    async fn complete(&self, task: Execution) -> Result<()> {
        sqlx::query("UPDATE tasks SET state='completed',updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND execution_token=? AND state='running'").bind(task.id).bind(task.token).execute(&self.write).await?;
        Ok(())
    }
    async fn progress(
        &self,
        task: Execution,
        cursor: &str,
        processed: i64,
        detail: Value,
    ) -> Result<()> {
        super::work_sweep::task_progress(task, cursor, processed, detail)
            .execute(&self.write)
            .await?;
        Ok(())
    }
    async fn load(&self, id: Uuid) -> Result<Option<(bool, String, Value)>> {
        Ok(
            sqlx::query_as::<_, (String, Value)>("SELECT task_type,detail FROM tasks WHERE id=?")
                .bind(id)
                .fetch_optional(&self.read)
                .await?
                .map(|(task_type, detail)| (false, task_type, detail)),
        )
    }
    async fn change_state(&self, id: Uuid, parent: bool, resume: bool) -> Result<bool> {
        ensure!(!parent, "SQLite does not support child tasks");
        let query = if resume {
            "UPDATE tasks SET state='queued',error=NULL,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),detail=CASE WHEN json_type(detail,'$.profile_revision') IS NOT NULL THEN COALESCE((SELECT json_set(tasks.detail,'$.profile_revision',p.revision) FROM storage_profiles p WHERE p.storage_id=unhex(replace(json_extract(tasks.detail,'$.storage_id'),'-',''))),detail) ELSE detail END WHERE id=? AND state IN ('paused','failed') AND COALESCE(error,'') NOT IN ('replica_pending','replica_drain') AND coalesce(json_extract(detail,'$.test_stopped'),0)=0"
        } else {
            "UPDATE tasks SET state='paused',updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state IN ('queued','running')"
        };
        Ok(sqlx::query(query)
            .bind(id)
            .execute(&self.write)
            .await?
            .rows_affected()
            == 1)
    }
}

pub(in super::super) fn claim_task(id: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='running',started_at=COALESCE(started_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)),execution_token=?,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND (state='queued' OR (state='paused' AND EXISTS(SELECT 1 FROM redundancy_links rl WHERE rl.id=tasks.id AND rl.state='idle' AND (rl.next_reconcile_at<=strftime('%Y-%m-%dT%H:%M:%fZ','now') OR EXISTS(SELECT 1 FROM transfer_changes rc WHERE rc.task_id=rl.id)))) OR (state='failed' AND (json_extract(detail,'$.operation')='cdc' OR EXISTS(SELECT 1 FROM redundancy_links rl WHERE rl.id=tasks.id AND rl.state<>'paused')) AND updated_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-30000000))").bind(token).bind(id)
}

pub(in super::super) fn task_record(id: Uuid) -> Rows<'static, TaskRecord> {
    sqlx::query_as("SELECT task_type,state,bucket_id,progress_cursor,detail,execution_token FROM tasks WHERE id=?").bind(id)
}

pub(in super::super) fn pause_owned_task(task: Execution, error: Option<&str>) -> Statement<'_> {
    sqlx::query("UPDATE tasks SET state='paused',error=COALESCE(?,error),updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='running' AND execution_token=?").bind(error).bind(task.id).bind(task.token)
}

pub(in super::super) fn storage_profile_revision(id: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT revision FROM storage_profiles WHERE storage_id=?").bind(id)
}

pub(in super::super) fn lock_task_finish(task: Execution) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT task_type,state FROM tasks WHERE id=? AND execution_token=?")
        .bind(task.id)
        .bind(task.token)
}

pub(in super::super) fn defer_pack_changes(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE pack_changes SET next_check_at=(SELECT CAST(unixepoch('subsec')*1000000 AS INTEGER)+(CAST(substr(sp.pack_reuse_interval,1,length(sp.pack_reuse_interval)-1) AS INTEGER)*CASE substr(sp.pack_reuse_interval,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END)*1000000 FROM packs p JOIN storage_profiles sp ON sp.storage_id=p.storage_id WHERE p.id=pack_changes.pack_id) WHERE pack_id IN (SELECT c.pack_id FROM pack_inputs i JOIN chunks c ON c.id=i.chunk_id WHERE i.task_id=?)").bind(id)
}

pub(in super::super) fn defer_pack_maintenance(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE pack_maintenance SET next_check_at=(SELECT CAST(unixepoch('subsec')*1000000 AS INTEGER)+(CAST(substr(sp.pack_reuse_interval,1,length(sp.pack_reuse_interval)-1) AS INTEGER)*CASE substr(sp.pack_reuse_interval,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END)*1000000 FROM streams s JOIN buckets b ON b.id=s.bucket_id JOIN storage_profiles sp ON sp.storage_id=b.storage_id WHERE s.id=pack_maintenance.stream_id) WHERE stream_id IN (SELECT e.stream_id FROM extents e JOIN pack_inputs i ON i.chunk_id=e.chunk_id WHERE i.task_id=?)").bind(id)
}

pub(in super::super) fn backoff_pending_uploads(
    task: Uuid,
    error: &str,
    retain: bool,
) -> Statement<'_> {
    sqlx::query("UPDATE pending_uploads SET next_retry_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+MIN(300,1 << MIN(attempts,8))*1000000,attempts=MIN(attempts+1,30),last_error=?,owner_task=IIF(?,owner_task,NULL) WHERE owner_task=?").bind(error).bind(retain).bind(task)
}

pub(in super::super) fn finish_task<'q>(
    task: Execution,
    state: &'q str,
    error: Option<&'q str>,
) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET state=?,error=COALESCE(?,error),execution_token=NULL,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND execution_token=?").bind(state).bind(error).bind(task.id).bind(task.token)
}

#[async_trait::async_trait]
impl TaskExecutionStore for Backend {
    async fn claim(&self, input: TaskClaim<'_>) -> Result<Option<Uuid>> {
        if claim_task_candidate(input.class, input.active, input.draining)
            .build_query_scalar::<Uuid>()
            .fetch_optional(&self.read)
            .await?
            .is_none()
        {
            return Ok(None);
        }
        transactions::claim_task(self, input).await
    }
    async fn load(&self, id: Uuid) -> Result<TaskRecord> {
        Ok(task_record(id).fetch_one(&self.read).await?)
    }
    async fn fence(&self, expected_mode: &str, id: Uuid, token: Uuid) -> Result<bool> {
        let Some(row) = task_record(id).fetch_optional(&self.read).await? else {
            return Ok(false);
        };
        if row.state != "running" || row.execution_token != Some(token) {
            return Ok(false);
        }
        let execution = Execution { id, token };
        if expected_mode != super::replication::mode().fetch_one(&self.read).await? {
            pause_owned_task(execution, None)
                .execute(&self.write)
                .await?;
            return Ok(false);
        }
        if let (Some(storage), Some(revision)) = (
            row.detail["storage_id"]
                .as_str()
                .and_then(|s| s.parse::<Uuid>().ok()),
            row.detail["profile_revision"].as_i64(),
        ) && storage_profile_revision(storage)
            .fetch_optional(&self.read)
            .await?
            != Some(revision)
        {
            pause_owned_task(
                execution,
                Some("storage profile changed; resume to refresh"),
            )
            .execute(&self.write)
            .await?;
            return Ok(false);
        }
        Ok(true)
    }
    async fn confirm(&self, id: Uuid, token: Uuid) -> Result<bool> {
        TaskControlStore::running(self, Execution { id, token }).await
    }
    async fn finish(&self, input: TaskFinish) -> Result<()> {
        transactions::finish_task(&self.write, input).await
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for TaskRecord {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
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
