use super::*;

pub(in super::super) fn lock_bucket_state(id: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT state FROM buckets WHERE id=$1 FOR SHARE").bind(id)
}

pub(in super::super) fn lock_object_version<'q>(
    bucket: Uuid,
    key: &'q str,
) -> Rows<'q, (Option<Id>, Id)> {
    sqlx::query_as(
        "SELECT stream_id,write_token FROM objects WHERE bucket_id=$1 AND object_key=$2 FOR UPDATE",
    )
    .bind(bucket)
    .bind(key)
}

pub(in super::super) fn web_upload_expectation<'q>(id: &'q Uuid) -> Rows<'q, (Option<Id>, i64)> {
    sqlx::query_as("SELECT expected_stream_id,expected_size FROM web_uploads WHERE upload_id=$1")
        .bind(id)
}

pub(in super::super) fn stream_etag(id: Id) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT etag FROM streams WHERE id=$1").bind(id)
}

pub(in super::super) fn incomplete_extents(stream: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=$1 AND (e.fragment_id IS NOT NULL OR c.state<>'ready'))")
        .bind(stream)
}

pub(in super::super) fn extent_continuity(stream: Id) -> Rows<'static, (i64, bool)> {
    sqlx::query_as("SELECT COALESCE(max(offset_bytes+length),0),COALESCE(bool_and(offset_bytes=previous),true) FROM (SELECT offset_bytes,length,lag(offset_bytes+length,1,0::bigint) OVER (ORDER BY offset_bytes) AS previous FROM extents WHERE stream_id=$1) e")
        .bind(stream)
}

pub(in super::super) fn task_fence(id: Uuid) -> Rows<'static, TaskFenceRow> {
    sqlx::query_as("SELECT t.state,t.lease_token,t.parent_task_id,m.deployment_mode,t.detail FROM tasks t CROSS JOIN mokyu_meta m WHERE t.id=$1")
        .bind(id)
}

pub(in super::super) fn pause_owned_task(id: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='paused',lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2")
        .bind(id)
        .bind(token)
}

pub(in super::super) fn storage_profile_revision(id: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT revision FROM storage_profiles WHERE storage_id=$1").bind(id)
}

pub(in super::super) fn pause_stale_profile_task(id: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='paused',error='storage profile changed; resume to refresh',lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2")
        .bind(id)
        .bind(token)
}

pub(in super::super) fn task_state(id: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT state FROM tasks WHERE id=$1").bind(id)
}

pub(in super::super) fn task_record(id: Uuid) -> Rows<'static, TaskRecord> {
    sqlx::query_as(
        "SELECT task_type,state,bucket_id,cursor,detail,lease_token FROM tasks WHERE id=$1",
    )
    .bind(id)
}

pub(in super::super) fn defer_pack_changes(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE pack_changes SET next_check_at=(SELECT now()+(left(sp.pack_reuse_interval,-1)::bigint*CASE right(sp.pack_reuse_interval,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END)*interval '1 second' FROM packs p JOIN storage_profiles sp ON sp.storage_id=p.storage_id WHERE p.id=pack_changes.pack_id) WHERE pack_id IN (SELECT c.pack_id FROM pack_inputs i JOIN chunks c ON c.id=i.chunk_id WHERE i.task_id=$1)").bind(id)
}

pub(in super::super) fn defer_pack_maintenance(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE pack_maintenance SET next_check_at=(SELECT now()+(left(sp.pack_reuse_interval,-1)::bigint*CASE right(sp.pack_reuse_interval,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END)*interval '1 second' FROM streams s JOIN buckets b ON b.id=s.bucket_id JOIN storage_profiles sp ON sp.storage_id=b.storage_id WHERE s.id=pack_maintenance.stream_id) WHERE stream_id IN (SELECT e.stream_id FROM extents e JOIN pack_inputs i ON i.chunk_id=e.chunk_id WHERE i.task_id=$1)").bind(id)
}

pub(in super::super) fn lock_stale_tasks() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM tasks WHERE state='running' AND lease_node_id IS NOT NULL AND (lease_expires_at IS NULL OR lease_expires_at<now()) FOR UPDATE",
    )
}

pub(in super::super) fn abandon_task_streams<'q>(ids: &'q [Uuid]) -> Statement<'q> {
    sqlx::query("UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_lease_token=NULL,touched_at=now() WHERE state='writing' AND owner_task_id=ANY($1) AND NOT EXISTS(SELECT 1 FROM transfer_sources t WHERE t.candidate=streams.id) AND NOT EXISTS(SELECT 1 FROM transfer_imports i JOIN transfer_items t ON t.id=i.item_id WHERE i.part_stream=streams.id OR t.candidate=streams.id) AND NOT EXISTS(SELECT 1 FROM replica_publications rp WHERE rp.source_pin=streams.id AND rp.phase='preparing') AND NOT EXISTS(SELECT 1 FROM replica_targets rt WHERE rt.candidate=streams.id AND rt.phase IN ('preparing','prepared'))")
        .bind(ids)
}

pub(in super::super) fn requeue_stale_tasks<'q>(ids: &'q [Uuid]) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET state='queued',lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=ANY($1) AND state='running'")
        .bind(ids)
}

pub(in super::super) fn extend_task_lease(id: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET lease_expires_at=now()+interval '5 minutes' WHERE id=$1 AND state='running' AND lease_token=$2 AND lease_expires_at>now()")
        .bind(id)
        .bind(token)
}

pub(in super::super) fn confirm_claimed_task(id: Uuid, token: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("UPDATE tasks SET lease_expires_at=now()+interval '5 minutes' WHERE id=$1 AND state='running' AND lease_token=$2 AND lease_expires_at>clock_timestamp() RETURNING true")
        .bind(id)
        .bind(token)
}

pub(in super::super) fn release_task_lease(id: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND lease_token=$2").bind(id).bind(token)
}

pub(in super::super) fn requeue_owned_task(id: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='queued',lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2")
        .bind(id)
        .bind(token)
}

pub(in super::super) fn fail_owned_task<'q>(
    id: Uuid,
    error: &'q str,
    token: Uuid,
) -> Statement<'q> {
    sqlx::query("WITH failed AS (UPDATE tasks SET state='failed',error=$2,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$3 RETURNING parent_task_id) UPDATE tasks SET state='failed',error=$2,updated_at=now() WHERE id=(SELECT parent_task_id FROM failed) AND state IN ('queued','running')")
        .bind(id)
        .bind(error)
        .bind(token)
}

pub(in super::super) fn update_parent_progress(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks p SET batch_completed=(SELECT count(*) FROM tasks c WHERE c.parent_task_id=p.id AND c.state='completed'),processed=(SELECT count(*) FROM tasks c WHERE c.parent_task_id=p.id AND c.state='completed'),state=CASE WHEN (SELECT count(*) FROM tasks c WHERE c.parent_task_id=p.id AND c.state='completed')>=coalesce(p.batch_total,0) THEN 'completed' ELSE p.state END,updated_at=now() WHERE p.id=$1 AND p.state IN ('queued','running')")
        .bind(id)
}

pub(in super::super) fn claim_task<'q>(
    class: i32,
    active: &'q [Uuid],
    node: Option<Uuid>,
    token: Uuid,
    draining: bool,
    allowed: &'q [String],
    metadata_only: Option<bool>,
    cache_storage: Option<Uuid>,
) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("UPDATE tasks SET state='running',started_at=coalesce(started_at,now()),lease_node_id=$3,lease_token=$4,lease_expires_at=CASE WHEN $3::uuid IS NOT NULL THEN now()+interval '5 minutes' END,updated_at=now() WHERE id=(SELECT t.id FROM tasks t CROSS JOIN mokyu_meta m WHERE (t.state='queued' OR (t.state='paused' AND EXISTS(SELECT 1 FROM redundancy_links rl WHERE rl.id=t.id AND rl.state='idle' AND (rl.next_reconcile_at<=now() OR EXISTS(SELECT 1 FROM transfer_changes rc WHERE rc.task_id=rl.id)))) OR (t.state='failed' AND (t.detail->>'operation' IN ('cdc','deduplicate','cache') OR EXISTS(SELECT 1 FROM redundancy_links rl WHERE rl.id=t.id AND rl.state<>'paused')) AND t.updated_at<now()-interval '30 seconds') OR ($3::uuid IS NOT NULL AND t.state='running' AND t.lease_expires_at<now())) AND NOT EXISTS(SELECT 1 FROM raw_sources r WHERE r.processing_task=t.id AND (r.owner_stream IS NOT NULL OR EXISTS(SELECT 1 FROM raw_ranges e JOIN streams s ON s.id=e.stream_id WHERE e.source_id=r.id AND s.state='writing'))) AND CASE WHEN t.task_type='storage_migration' AND t.detail->>'operation'='replication' AND t.detail->>'phase'<>'verify' AND NOT coalesce((t.detail->>'external')::boolean,false) THEN 0 WHEN t.task_type IN ('pack','unpack') THEN 1 WHEN t.task_type IN ('upload','cache_flush','storage_migration','rewrite') THEN 2 ELSE 0 END=$1 AND (NOT $5 OR (t.task_type IN ('upload','cache_flush') AND coalesce(t.detail->>'operation' NOT IN ('cdc','deduplicate'),true))) AND ((t.detail->>'operation' IS DISTINCT FROM 'deduplicate' AND t.detail->>'operation' IS DISTINCT FROM 'cdc') OR (NOT m.maintenance AND (NOT EXISTS(SELECT 1 FROM raw_processing w WHERE w.token=t.id) OR EXISTS(SELECT 1 FROM raw_processing w WHERE w.token=t.id AND w.next_attempt_at<=clock_timestamp())))) AND (t.detail->>'operation' IS DISTINCT FROM 'cdc' OR t.state<>'queued' OR NOT t.detail ? 'node_conversion' OR t.updated_at<clock_timestamp()-interval '1 second') AND (t.detail->>'operation' IS DISTINCT FROM 'cache' OR NOT EXISTS(SELECT 1 FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id JOIN storage_profiles p ON p.storage_id=r.storage_id WHERE r.id::text=t.detail->>'source_id' AND (u.next_attempt_at>clock_timestamp() OR u.token IS NOT NULL AND u.lease_expires_at>clock_timestamp() OR r.owner_stream IS NOT NULL OR EXISTS(SELECT 1 FROM raw_ranges e JOIN streams s ON s.id=e.stream_id WHERE e.source_id=r.id AND s.state='writing') OR NOT $5 AND NOT m.maintenance AND r.created_at+p.write_backlog_max_age*interval '1 second'>clock_timestamp() AND EXISTS(SELECT 1 FROM tasks c WHERE c.id=r.processing_task AND c.state<>'completed')))) AND ($8::uuid IS NULL OR (t.task_type='upload' AND t.detail->>'operation'='cache' AND t.detail->>'storage_id'=$8::uuid::text)) AND (m.deployment_mode<>'single_control' OR t.execution_scope='node' OR NOT (t.task_type IN ('integrity','pack','unpack','storage_migration','rewrite','sweep')) OR (m.remote_work=($3::uuid IS NOT NULL))) AND NOT(t.id=ANY($2)) AND (($3::uuid IS NULL AND (t.execution_scope<>'node' OR t.target_node_id IS NULL)) OR (t.execution_scope='node' AND t.target_node_id=$3) OR (t.execution_scope<>'node' AND EXISTS(SELECT 1 FROM nodes n WHERE n.id=$3 AND n.enabled AND n.role IN ('control','worker','node')))) AND (cardinality($6::text[])=0 OR (t.task_type=ANY($6) AND ($7 IS DISTINCT FROM false OR t.task_type<>'upload' OR t.detail->>'operation' IN ('cdc','deduplicate','cache')) AND (t.task_type='sweep' OR NOT EXISTS(SELECT 1 FROM tasks sweep WHERE sweep.task_type='sweep' AND sweep.detail->>'dry_run'='false' AND sweep.state IN ('queued','running'))) AND (t.task_type<>'integrity' OR t.detail->>'operation'='external_test' OR t.detail ? 'source_operation' OR (t.detail->>'mode' IN ('head','full') AND t.detail->>'phase' IN ('raw','chunks'))))) AND ($7::bool IS NULL OR $7=((t.task_type='storage_migration' AND t.detail->>'operation'='replication' AND t.detail->>'phase'<>'verify' AND NOT coalesce((t.detail->>'external')::boolean,false)) OR t.task_type IN ('usage','purge') OR (t.task_type='cleanup' AND t.detail->>'storage_id' IS NULL) OR coalesce(t.task_type='integrity' AND NOT t.detail ? 'source_operation' AND (t.detail->>'mode'='metadata' OR t.detail->>'phase'='metadata'),false))) AND t.detail->>'role' IS DISTINCT FROM 'parent' AND NOT (m.maintenance AND t.task_type IN ('pack','unpack','purge','gc','storage_migration','rewrite','usage')) AND NOT EXISTS(SELECT 1 FROM maintenance_controls c WHERE c.policy=CASE WHEN t.task_type='pack' THEN t.detail->>'policy' ELSE t.task_type END AND CASE WHEN c.policy='gc' THEN m.gc_paused ELSE c.paused END) AND NOT (t.task_type='pack' AND t.detail->>'policy' IN ('pack','repack') AND m.pack_creation_paused) ORDER BY t.updated_at,t.id LIMIT 1) AND (state='queued' OR (state='paused' AND EXISTS(SELECT 1 FROM redundancy_links rl WHERE rl.id=tasks.id AND rl.state='idle' AND (rl.next_reconcile_at<=now() OR EXISTS(SELECT 1 FROM transfer_changes rc WHERE rc.task_id=rl.id)))) OR (state='failed' AND (detail->>'operation' IN ('cdc','deduplicate','cache') OR EXISTS(SELECT 1 FROM redundancy_links rl WHERE rl.id=tasks.id AND rl.state<>'paused')) AND updated_at<now()-interval '30 seconds') OR ($3::uuid IS NOT NULL AND state='running' AND lease_expires_at<now())) RETURNING id")
        .bind(class)
        .bind(active)
        .bind(node)
        .bind(token)
        .bind(draining)
        .bind(allowed)
        .bind(metadata_only)
        .bind(cache_storage)
}

pub(in super::super) fn lock_storage_permits(key: String) -> Statement<'static> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))").bind(key)
}

pub(in super::super) fn publish_object<'q>(
    bucket: Uuid,
    key: &'q str,
    stream: Id,
) -> Statement<'q> {
    sqlx::query("UPDATE objects SET stream_id=$3 WHERE bucket_id=$1 AND object_key=$2")
        .bind(bucket)
        .bind(key)
        .bind(stream)
}

pub(in super::super) fn publish_stream(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams s SET state='ready',touched_at=now(),http_compression=CASE WHEN stream_type='object' THEN (SELECT CASE WHEN COALESCE(sum(e.length),0)=s.size THEN jsonb_build_object('estimated_size',COALESCE(sum(CASE WHEN e.source_offset=0 AND e.length=c.raw_size AND c.pack_id IS NULL AND COALESCE(l.standalone_size,c.standalone_size) IS NOT NULL THEN COALESCE(l.standalone_size,c.standalone_size) ELSE e.length END),0)) ELSE '{}'::jsonb END FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE e.stream_id=s.id) ELSE '{}'::jsonb END WHERE id=$1").bind(id)
}

pub(in super::super) fn learn_compression(
    observation: crate::model::CompressionObservation,
    node: Option<Uuid>,
) -> Statement<'static> {
    sqlx::query("WITH allowed AS MATERIALIZED (SELECT 1 FROM mokyu_meta WHERE singleton AND NOT maintenance AND (deployment_mode='single_control' OR deployment_mode=CASE WHEN $5::uuid IS NULL THEN 'single' ELSE 'cluster' END) FOR SHARE), active_node AS MATERIALIZED (SELECT 1 FROM nodes WHERE id=$5 AND enabled AND role IN ('data','node') FOR SHARE) UPDATE streams SET http_compression=jsonb_set(CASE WHEN COALESCE((http_compression->>'version')::bigint,0)<$2 THEN jsonb_strip_nulls(jsonb_build_object('version',$2::bigint,'estimated_size',CASE WHEN jsonb_typeof(http_compression->'estimated_size')='number' AND (http_compression->>'estimated_size') ~ '^[0-9]+$' THEN CASE WHEN (http_compression->>'estimated_size')::numeric<=9223372036854775807 THEN http_compression->'estimated_size' END END)) ELSE http_compression END,ARRAY[$3::text],to_jsonb($4::bigint)) WHERE id=$1 AND state='ready' AND COALESCE((http_compression->>'version')::bigint,0)<=$2 AND (COALESCE((http_compression->>'version')::bigint,0)<$2 OR NOT http_compression ? $3) AND EXISTS(SELECT 1 FROM allowed) AND ($5::uuid IS NULL OR EXISTS(SELECT 1 FROM active_node))")
        .bind(observation.stream).bind(i64::from(observation.version)).bind(observation.encoding).bind(observation.size).bind(node)
}

pub(in super::super) fn touch_credentials(accesses: &[String]) -> Statement<'_> {
    sqlx::query("UPDATE s3_credentials SET last_used_at=now() WHERE access_key_id=ANY($1) AND (last_used_at IS NULL OR last_used_at<now()-interval '5 minutes')").bind(accesses)
}

pub(in super::super) fn retire_stream(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1").bind(id)
}

pub(in super::super) fn complete_upload<'q>(
    id: &'q Uuid,
    hash: &'q str,
    result: &'q Value,
    stream: Id,
    token: Uuid,
) -> Statement<'q> {
    sqlx::query("UPDATE uploads SET state='completed',result=$3,output_stream=$4,touched_at=now() WHERE id=$1 AND state='completing' AND manifest_hash=$2 AND completion_token=$5 AND (completion_expires_at>clock_timestamp() OR output_stream=$4)")
        .bind(id)
        .bind(hash)
        .bind(result)
        .bind(stream)
        .bind(token)
}

pub(in super::super) fn retire_upload_parts<'q>(id: &'q Uuid) -> Statement<'q> {
    sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id IN (SELECT stream_id FROM parts WHERE upload_id=$1)")
        .bind(id)
}

pub(in super::super) fn delete_upload_parts<'q>(id: &'q Uuid) -> Statement<'q> {
    sqlx::query("DELETE FROM parts WHERE upload_id=$1").bind(id)
}

pub(in super::super) fn task_control(id: Uuid) -> Rows<'static, (bool, String, Value)> {
    sqlx::query_as(
        "SELECT coalesce(detail->>'role'='parent',false),task_type,detail FROM tasks WHERE id=$1",
    )
    .bind(id)
}

pub(in super::super) fn change_task_state(
    id: Uuid,
    parent: bool,
    resume: bool,
) -> Statement<'static> {
    let query = if resume {
        if parent {
            "UPDATE tasks SET state='queued',error=NULL,updated_at=now() WHERE (id=$1 OR parent_task_id=$1) AND state IN ('paused','failed') AND COALESCE(error,'') NOT IN ('replica_pending','replica_drain') AND coalesce(detail->>'test_stopped','false')<>'true'"
        } else {
            "UPDATE tasks SET state='queued',error=NULL,updated_at=now() WHERE id=$1 AND state IN ('paused','failed') AND COALESCE(error,'') NOT IN ('replica_pending','replica_drain') AND coalesce(detail->>'test_stopped','false')<>'true'"
        }
    } else if parent {
        "UPDATE tasks SET state='paused',updated_at=now() WHERE (id=$1 OR parent_task_id=$1) AND state IN ('queued','running')"
    } else {
        "UPDATE tasks SET state='paused',updated_at=now() WHERE id=$1 AND state IN ('queued','running')"
    };
    sqlx::query(query).bind(id)
}

pub(in super::super) fn refresh_task_profiles(id: Uuid, parent: bool) -> Statement<'static> {
    sqlx::query("UPDATE tasks t SET detail=jsonb_set(t.detail,'{profile_revision}',to_jsonb(p.revision)) FROM storage_profiles p WHERE (t.id=$1 OR ($2 AND t.parent_task_id=$1)) AND t.state IN ('paused','failed') AND t.detail ? 'profile_revision' AND p.storage_id=NULLIF(t.detail->>'storage_id','')::uuid")
        .bind(id)
        .bind(parent)
}

pub(in super::super) fn set_stream_public(id: Id, public: bool) -> Statement<'static> {
    sqlx::query("UPDATE streams SET public_read=$2 WHERE id=$1")
        .bind(id)
        .bind(public)
}

pub(in super::super) fn remove_object<'q>(bucket: Uuid, key: &'q str, token: Id) -> Statement<'q> {
    sqlx::query("INSERT INTO objects(bucket_id,object_key,write_token) VALUES($1,$2,$3) ON CONFLICT(bucket_id,object_key) DO UPDATE SET stream_id=NULL,write_token=excluded.write_token")
        .bind(bucket)
        .bind(key)
        .bind(token)
}

pub(super) fn task_value(mut value: Value, single: bool) -> Value {
    if let Some(task) = value.as_object_mut() {
        task.remove("lease_token");
        if single {
            for field in [
                "parent_task_id",
                "execution_scope",
                "target_node_id",
                "lease_node_id",
                "lease_expires_at",
                "batch_cursor",
                "batch_total",
                "batch_completed",
            ] {
                task.remove(field);
            }
        }
    }
    value
}

#[async_trait::async_trait]
impl TaskControlStore for Backend {
    async fn unpack_status(&self, pack: Id) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar("SELECT to_jsonb(t) FROM tasks t WHERE task_type='unpack' AND detail->>'pack_id'=$1 ORDER BY created_at DESC LIMIT 1").bind(pack.to_string()).fetch_optional(&self.pool).await?.map(|value| task_value(value, self.single)))
    }
    async fn rewrite_status(&self, storage: Uuid) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar("SELECT to_jsonb(t) FROM tasks t WHERE task_type='rewrite' AND parent_task_id IS NULL AND detail->>'storage_id'=$1 ORDER BY created_at DESC LIMIT 1").bind(storage.to_string()).fetch_optional(&self.pool).await?.map(|value| task_value(value, self.single)))
    }
    async fn rewrite_task(&self, storage: Uuid) -> Result<Option<Uuid>> {
        Ok(sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='rewrite' AND parent_task_id IS NULL AND detail->>'storage_id'=$1 ORDER BY created_at DESC LIMIT 1").bind(storage.to_string()).fetch_optional(&self.pool).await?)
    }

    async fn all(&self) -> Result<Vec<Value>> {
        let tasks: Vec<Value> = sqlx::query_scalar(
            "SELECT to_jsonb(t) FROM tasks t ORDER BY created_at DESC LIMIT 100",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(tasks
            .into_iter()
            .map(|value| task_value(value, self.single))
            .collect())
    }
    async fn raw(&self, task: Uuid) -> Result<Option<Value>> {
        Ok(
            sqlx::query_scalar("SELECT to_jsonb(t) FROM tasks t WHERE id=$1")
                .bind(task)
                .fetch_optional(&self.pool)
                .await?
                .map(|value| task_value(value, self.single)),
        )
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
    async fn view(&self, principal: &Principal, task: Uuid) -> Result<Value> {
        transactions::task_view(&self.pool, principal, task).await
    }

    async fn other_integrity(&self, task: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='integrity' AND NOT (detail ? 'source_operation') AND detail->>'operation' IS DISTINCT FROM 'external_test' AND state IN ('queued','running') AND id<>$1)").bind(task).fetch_one(&self.pool).await?)
    }

    async fn enqueue(&self, task_type: &str, bucket: Option<Uuid>, detail: Value) -> Result<Uuid> {
        transactions::enqueue_task(
            &mut *self.pool.acquire().await?,
            task_type,
            bucket,
            detail,
            false,
        )
        .await
    }
    async fn sweep_preview(&self, task: Uuid) -> Result<Option<Value>> {
        Ok(sweep_preview(task).fetch_optional(&self.pool).await?)
    }
    async fn pause(&self, task: Execution) -> Result<()> {
        pause_active_task(task).execute(&self.pool).await?;
        Ok(())
    }
    async fn running(&self, task: Execution) -> Result<bool> {
        Ok(task_running(task).fetch_one(&self.pool).await?)
    }
    async fn complete(&self, task: Execution) -> Result<()> {
        complete_running_task(task).execute(&self.pool).await?;
        Ok(())
    }
    async fn progress(
        &self,
        task: Execution,
        cursor: &str,
        processed: i64,
        detail: Value,
    ) -> Result<()> {
        task_progress(task, cursor, processed, detail)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn load(&self, id: uuid::Uuid) -> Result<Option<(bool, String, Value)>> {
        Ok(task_control(id).fetch_optional(&self.pool).await?)
    }

    async fn change_state(&self, id: uuid::Uuid, parent: bool, resume: bool) -> Result<bool> {
        transactions::change_task_state(&self.pool, id, parent, resume).await
    }
}

#[async_trait::async_trait]
impl TaskExecutionStore for Backend {
    async fn claim(&self, input: TaskClaim<'_>) -> Result<Option<Uuid>> {
        transactions::claim_task(self, input, None).await
    }

    async fn load(&self, id: Uuid) -> Result<TaskRecord> {
        Ok(task_record(id).fetch_one(&self.pool).await?)
    }

    async fn fence(&self, expected_mode: &str, id: uuid::Uuid, token: uuid::Uuid) -> Result<bool> {
        fence_task(&self.pool, expected_mode, id, token).await
    }

    async fn confirm(&self, id: uuid::Uuid, token: uuid::Uuid) -> Result<bool> {
        if self.single {
            Ok(task_running(Execution { id, token })
                .fetch_one(&self.pool)
                .await?)
        } else {
            confirm_task_lease(&self.pool, id, token).await
        }
    }

    async fn finish(&self, input: TaskFinish) -> Result<()> {
        transactions::finish_task(&self.pool, input).await
    }
}

async fn fence_task(
    pool: &PgPool,
    expected_mode: &str,
    id: uuid::Uuid,
    token: uuid::Uuid,
) -> Result<bool> {
    let row = task_fence(id).fetch_optional(pool).await?;
    let Some((state, lease, parent, mode, detail)) = row else {
        return Ok(false);
    };
    if mode != expected_mode {
        pause_owned_task(id, token).execute(pool).await?;
        return Ok(false);
    }
    if let (Some(storage), Some(revision)) = (
        detail["storage_id"]
            .as_str()
            .and_then(|value| value.parse::<uuid::Uuid>().ok()),
        detail["profile_revision"].as_i64(),
    ) {
        let current: Option<i64> = storage_profile_revision(storage)
            .fetch_optional(pool)
            .await?;
        if current != Some(revision) {
            pause_stale_profile_task(id, token).execute(pool).await?;
            return Ok(false);
        }
    }
    if state != "running" || lease != Some(token) {
        return Ok(false);
    }
    if let Some(parent) = parent {
        let parent_state: Option<String> = task_state(parent).fetch_optional(pool).await?;
        if !matches!(parent_state.as_deref(), Some("queued" | "running")) {
            pause_owned_task(id, token).execute(pool).await?;
            return Ok(false);
        }
    }
    Ok(true)
}

pub(in super::super) async fn renew_task_lease(
    pool: &PgPool,
    id: uuid::Uuid,
    token: uuid::Uuid,
) -> Result<bool> {
    Ok(extend_task_lease(id, token)
        .execute(pool)
        .await?
        .rows_affected()
        == 1)
}

pub(in super::super) async fn confirm_task_lease(
    pool: &PgPool,
    id: uuid::Uuid,
    token: uuid::Uuid,
) -> Result<bool> {
    Ok(confirm_claimed_task(id, token)
        .fetch_optional(pool)
        .await?
        .is_some_and(|value| value))
}

pub(in super::super) fn task_page(
    q: JobQuery,
    cursor: Option<(DateTime<Utc>, Uuid)>,
    limit: i64,
) -> sqlx::QueryBuilder<Postgres> {
    let mut sql = sqlx::QueryBuilder::new(JOB_SQL);
    sql.push(" WHERE true");
    if let Some(value) = q.state {
        match value.as_str() {
            "running" => sql
                .push(" AND (t.state='running' OR t.state='paused' AND t.error='replica_pending')"),
            "paused" => {
                sql.push(" AND t.state='paused' AND COALESCE(t.error,'')<>'replica_pending'")
            }
            _ => sql.push(" AND t.state=").push_bind(value),
        };
    }
    if let Some(value) = q.policy {
        sql.push(" AND coalesce(t.detail->>'policy',t.task_type)=")
            .push_bind(value);
    }
    if let Some(value) = q.bucket {
        sql.push(" AND t.bucket_id=").push_bind(value);
    }
    if let Some(value) = q.storage {
        sql.push(" AND (b.storage_id=")
            .push_bind(value)
            .push(" OR t.detail->>'storage_id'=")
            .push_bind(value.to_string())
            .push(" OR t.detail->'storages' @> ")
            .push_bind(serde_json::json!([{"storage_id":value}]))
            .push(')');
    }
    if let Some(value) = q.actor {
        sql.push(" AND position(lower(")
            .push_bind(value)
            .push(") in lower(t.created_by_label))>0");
    }
    if let Some(cursor) = cursor {
        sql.push(" AND (t.created_at,t.id)<(")
            .push_bind(cursor.0)
            .push(",")
            .push_bind(cursor.1)
            .push(")");
    }
    sql.push(" ORDER BY t.created_at DESC,t.id DESC LIMIT ")
        .push_bind(limit);
    sql
}

pub(in super::super) fn task_view(id: Uuid) -> sqlx::QueryBuilder<Postgres> {
    let mut query = sqlx::QueryBuilder::new(JOB_SQL);
    query.push(" WHERE t.id=").push_bind(id);
    query
}

pub(in super::super) fn task_page_limits() -> sqlx::RawSql {
    sqlx::raw_sql("SET LOCAL statement_timeout='5s';SET LOCAL work_mem='4MB'")
}

pub(in super::super) fn set_maintenance_mode(enabled: bool) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET maintenance=$1").bind(enabled)
}

pub(in super::super) fn pause_maintenance_tasks() -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET state='paused',updated_at=now() WHERE task_type IN ('purge','pack','unpack','storage_migration','rewrite') AND state IN ('queued','running')",
    )
}
