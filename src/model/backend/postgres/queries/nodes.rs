use super::*;

const NODE_OBSERVATIONS: &str = r#"
WITH reports AS (
    SELECT n.id,n.name,n.role,n.enabled,n.last_seen_at,n.created_at,r.metadata,r.status,r.sampled_at,r.received_at,
        CASE WHEN NOT n.enabled THEN 'released' WHEN n.last_seen_at IS NULL THEN 'not_reported'
            WHEN n.last_seen_at < now()-$2*interval '1 second' THEN 'unresponsive' ELSE 'online' END AS contact,
        COALESCE(n.enabled AND n.last_seen_at >= now()-$2*interval '1 second'
            AND r.sampled_at IS NOT NULL AND r.received_at >= now()-$2*interval '1 second',false) AS report_fresh
    FROM nodes n LEFT JOIN node_reports r ON r.node_id=n.id
), observed AS (
    SELECT r.id,r.name,r.role,r.enabled,r.last_seen_at,r.created_at,r.sampled_at,r.received_at,r.contact,r.report_fresh,
        CASE WHEN NOT r.enabled THEN 'unknown'
            WHEN r.status->>'work'='starting' AND r.received_at>=now()-$2*interval '1 second' THEN 'starting'
            WHEN r.report_fresh AND r.metadata->>'format'='1' AND r.status->>'work' IN ('normal','maintenance','restricted','recovery') THEN r.status->>'work'
            ELSE 'unknown' END AS work,
        COALESCE(s.settings,'{}'::jsonb) AS settings
    FROM reports r LEFT JOIN LATERAL (
        SELECT jsonb_object_agg(section,CASE
            WHEN desired->'value'='{}'::jsonb THEN 'not_applicable'
            WHEN NOT r.report_fresh OR r.metadata->>'format' IS DISTINCT FROM '1' THEN 'unknown'
            WHEN NOT COALESCE((r.metadata->'loaded'->section) ?& ARRAY(SELECT jsonb_object_keys(desired->'value')),false) THEN 'unknown'
            WHEN NOT ((r.metadata->'loaded'->section) @> (desired->'value')) THEN 'restart_required'
            WHEN r.metadata->'revisions'->section=desired->'revision' THEN 'applied'
            ELSE 'equivalent' END) AS settings
        FROM jsonb_each($1::jsonb->r.role) AS setting(section,desired)
    ) s ON true
), states AS (
    SELECT *,CASE WHEN settings='{}'::jsonb OR EXISTS(SELECT 1 FROM jsonb_each_text(settings) v WHERE v.value='unknown') THEN 'unknown'
        WHEN EXISTS(SELECT 1 FROM jsonb_each_text(settings) v WHERE v.value='restart_required') THEN 'restart_required'
        WHEN EXISTS(SELECT 1 FROM jsonb_each_text(settings) v WHERE v.value='equivalent') THEN 'equivalent'
        ELSE 'applied' END AS configuration FROM observed
)
"#;

pub(in super::super) fn begin_node_report(
    id: Uuid,
    boot: Uuid,
    started: DateTime<Utc>,
) -> Statement<'static> {
    sqlx::query("INSERT INTO node_reports(node_id,boot_id,started_at,status) VALUES($1,$2,$3,'{\"work\":\"starting\"}') ON CONFLICT(node_id) DO UPDATE SET boot_id=$2,sequence=0,started_at=$3,sampled_at=NULL,received_at=now(),metadata='{}',status=EXCLUDED.status")
        .bind(id).bind(boot).bind(started)
}

pub(in super::super) fn save_node_report(id: Uuid, report: &NodeReport) -> Statement<'_> {
    sqlx::query("UPDATE node_reports SET sequence=$3,sampled_at=$4,received_at=now(),metadata=COALESCE($5,metadata),status=$6 WHERE node_id=$1 AND boot_id=$2 AND sequence<$3")
        .bind(id).bind(report.boot_id).bind(report.sequence).bind(report.sampled_at).bind(&report.metadata).bind(&report.status)
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for NodeView {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            role: row.try_get("role")?,
            enabled: row.try_get("enabled")?,
            last_seen_at: row.try_get("last_seen_at")?,
            created_at: row.try_get("created_at")?,
            contact: row.try_get("contact")?,
            work: row.try_get("work")?,
            configuration: row.try_get("configuration")?,
            settings: row.try_get("settings")?,
            sampled_at: row.try_get("sampled_at")?,
            received_at: row.try_get("received_at")?,
            report_fresh: row.try_get("report_fresh")?,
        })
    }
}

#[async_trait::async_trait]
impl crate::model::cluster::NodeNotices for Backend {
    async fn control_consumer(
        &self,
        c: crate::model::notification::Consumer,
        activate: bool,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        if activate {
            transactions::notifications::activate(&mut tx, c, "control").await?;
        } else {
            notifications::renew(c.node).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    async fn claim_notifications(
        &self,
        consumers: &[crate::model::notification::Consumer],
    ) -> Result<Vec<crate::model::notification::Delivery>> {
        transactions::notifications::claim(&self.pool, consumers).await
    }
    async fn acknowledge_notifications(
        &self,
        consumer: crate::model::notification::Consumer,
        ids: &[Uuid],
    ) -> Result<u64> {
        transactions::notifications::acknowledge(&self.pool, consumer, ids).await
    }
    async fn maintain_notifications(&self) -> Result<bool> {
        transactions::notifications::maintain(&self.pool).await
    }
}

#[async_trait::async_trait]
impl ClusterStore for Backend {
    async fn task_targets(&self, tasks: &[Uuid]) -> Result<Vec<Option<Uuid>>> {
        anyhow::ensure!(tasks.len() <= 256, "too many task hints");
        Ok(sqlx::query_scalar("SELECT DISTINCT CASE WHEN t.execution_scope='node' THEN t.target_node_id END FROM tasks t WHERE t.id=ANY($1) AND (t.execution_scope<>'node' OR t.target_node_id IS NOT NULL) AND NOT (t.task_type IN ('usage','purge') OR (t.task_type='cleanup' AND t.detail->>'storage_id' IS NULL) OR coalesce(t.task_type='integrity' AND NOT t.detail ? 'source_operation' AND (t.detail->>'mode'='metadata' OR t.detail->>'phase'='metadata'),false))").bind(tasks).fetch_all(&self.pool).await?)
    }
    async fn notification_changes(
        &self,
    ) -> Result<Box<dyn crate::model::cluster::NotificationChanges>> {
        connection::notification_changes(&self.pool).await
    }

    async fn pending_read(&self, streams: &[Id], node: Uuid) -> Result<bool> {
        Ok(super::pending_sources(streams, Some(node))
            .fetch_one(&self.pool)
            .await?)
    }
    async fn prioritize_uploads(&self, streams: &[Id]) -> Result<()> {
        sqlx::query("UPDATE pending_uploads SET next_retry_at=LEAST(next_retry_at,clock_timestamp()) WHERE owner_task IS NULL AND chunk_id IN (SELECT chunk_id FROM extents WHERE stream_id=ANY($1) AND chunk_id IS NOT NULL)").bind(streams).execute(&self.pool).await?;
        Ok(())
    }
    async fn read_page(
        &self,
        stream: Id,
        lease: Uuid,
        from: i64,
        until: i64,
    ) -> Result<Vec<ReadExtent>> {
        if let Some(replica) = &self.replica
            && let Some(page) = replica.read(stream, lease, from, until).await
        {
            return Ok(page);
        }
        Ok(read_extents(stream, from, until)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn claim_task(
        &self,
        input: TaskClaim<'_>,
        node: &crate::cluster::NodeState,
        role: &str,
        key: &crate::content::codec::Key,
    ) -> Result<Option<Uuid>> {
        transactions::claim_task(self, input, Some((node, role, key))).await
    }
    async fn recover_tasks(&self) -> Result<u64> {
        transactions::recover_stale(&self.pool).await
    }
    async fn renew_task(&self, id: Uuid, token: Uuid) -> Result<bool> {
        renew_task_lease(&self.pool, id, token).await
    }

    async fn start_usage(&self, node: Option<Uuid>) -> Result<Value> {
        transactions::start_usage(&self.pool, node).await
    }
    async fn complete_usage(&self, task: Execution) -> Result<bool> {
        complete_usage(&self.pool, task).await
    }

    async fn acquire_integrity_rate(
        &self,
        storage: Uuid,
        rates: &[(&str, f64, f64)],
    ) -> Result<bool> {
        transactions::acquire_integrity_rate(&self.pool, storage, rates).await
    }
    async fn current_stream(
        &self,
        bucket: Uuid,
        key: &str,
        ttl_seconds: i64,
    ) -> Result<(crate::app::StoredStream, Uuid)> {
        transactions::current_stream(&self.pool, bucket, key, ttl_seconds).await
    }
    async fn create_stream(
        &self,
        input: crate::model::StreamCreate,
        ttl_seconds: i64,
    ) -> Result<Id> {
        transactions::create_stream(&self.pool, input, Some(ttl_seconds)).await
    }
    async fn renew_read(&self, id: uuid::Uuid, ttl_seconds: i64) -> Result<()> {
        renew_read_lease(&self.pool, id, ttl_seconds).await
    }

    async fn release_read(&self, id: uuid::Uuid) -> Result<()> {
        release_read_lease(&self.pool, id).await
    }

    async fn renew_write(&self, stream: Id, ttl_seconds: i64) -> Result<()> {
        renew_write_lease(&self.pool, stream, ttl_seconds).await
    }
    async fn expire_stream_leases(&self) -> Result<()> {
        expire_stream_leases().execute(&self.pool).await?;
        Ok(())
    }
    async fn mode(&self) -> Result<String> {
        Ok(deployment_mode().fetch_one(&self.pool).await?)
    }
    async fn credential(&self, id: Uuid) -> Result<Option<(String, Vec<u8>)>> {
        Ok(sqlx::query_as("SELECT role,credential_encrypted FROM nodes WHERE id=$1 AND role='control' AND enabled AND credential_encrypted IS NOT NULL").bind(id).fetch_optional(&self.pool).await?)
    }
    async fn recover(&self, name: &str, role: &str) -> Result<()> {
        transactions::recover_node(&self.pool, name, role).await
    }
    async fn leader(&self, key: i64) -> Result<Option<Box<dyn Leader>>> {
        transactions::cluster_leader(&self.pool, key).await
    }
    async fn maintenance_state(&self) -> Result<(Value, DateTime<Utc>)> {
        Ok(maintenance_state().fetch_one(&self.pool).await?)
    }
}

#[async_trait::async_trait]
impl crate::model::cluster::NodeSessions for Backend {
    async fn provision_node(
        &self,
        principal: &Principal,
        name: &str,
        role: &str,
        key: &crate::content::codec::Key,
    ) -> Result<crate::cluster::NodeState> {
        transactions::provision_node(self, principal, name, Some(role), key).await
    }
    async fn rotate_node(
        &self,
        principal: &Principal,
        name: &str,
        key: &crate::content::codec::Key,
    ) -> Result<crate::cluster::NodeState> {
        transactions::provision_node(self, principal, name, None, key).await
    }
    async fn close_session(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: crate::model::session::Run,
    ) -> Result<()> {
        transactions::close_node_session(self, node, key, run).await
    }
    async fn report_session(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: crate::model::session::Run,
        report: &NodeReport,
        sample_interval: u64,
    ) -> Result<bool> {
        transactions::report_node_session(self, node, key, run, report, sample_interval).await
    }
    async fn open_session(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::session::Open,
    ) -> Result<crate::model::session::Snapshot> {
        transactions::open_node_session(self, node, key, input).await
    }
    async fn sync_node(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: crate::model::session::Run,
    ) -> Result<crate::model::session::Snapshot> {
        transactions::sync_node(self, node, key, run).await
    }
    async fn pulse_session(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: crate::model::session::Run,
        epoch: i64,
        sequence: i64,
        draining: bool,
        resources: crate::model::node_read::KeepAlive,
    ) -> Result<crate::model::session::Snapshot> {
        transactions::pulse_node_session(self, node, key, run, epoch, sequence, draining, resources)
            .await
    }
    async fn maintain_session(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: crate::model::session::Run,
        epoch: i64,
        sequence: i64,
        resources: crate::model::node_read::KeepAlive,
    ) -> Result<(i64, Vec<crate::model::node_read::GrantResult>)> {
        transactions::maintain_node_session(self, node, key, run, epoch, sequence, resources).await
    }
    async fn release(&self, name: &str) -> Result<()> {
        transactions::release_node(self, name).await
    }
    async fn register(
        &self,
        name: &str,
        role: &str,
        credential: String,
        key: &crate::content::codec::Key,
    ) -> Result<Option<crate::cluster::NodeState>> {
        transactions::register_node(self, name, role, credential, key).await
    }
    async fn heartbeat(
        &self,
        state: &crate::cluster::NodeState,
        role: &str,
        key: &crate::content::codec::Key,
    ) -> Result<()> {
        transactions::heartbeat(&self.pool, state, role, key).await
    }
}

#[async_trait::async_trait]
impl crate::model::cluster::NodeWork for Backend {
    async fn read_transfer(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_work::ReadTransfer,
    ) -> Result<(
        crate::model::node_read::Reply,
        Option<crate::model::replication::external::RawRead>,
    )> {
        transactions::work::export::read(self, node, key, input).await
    }
    async fn checkpoint_transfer(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        run: crate::model::session::Run,
        operation: Uuid,
        progress: &crate::model::transfer::RawProgress,
    ) -> Result<()> {
        transactions::work::checkpoint_transfer(self, node, key, run, operation, progress).await
    }
    async fn checkpoint_raw_pages(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_work::RawPages,
    ) -> Result<()> {
        transactions::work::checkpoint_raw_pages(self, node, key, input).await
    }
    async fn read_migration(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_work::ReadMigration,
    ) -> Result<crate::model::node_read::Reply> {
        transactions::work::migration::read(self, node, key, input).await
    }
    async fn recover_work(&self) -> Result<u64> {
        if self.control_epoch.is_some() {
            let mut tx = self.pool.begin().await?;
            self.fence_control(&mut tx).await?;
            expire_stream_leases().execute(&mut *tx).await?;
            tx.commit().await?;
        }
        transactions::recover_stale(&self.pool).await
    }
    async fn exchange_work(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_work::Exchange,
    ) -> Result<crate::model::node_work::Reply> {
        transactions::work::exchange(self, node, key, input).await
    }
}

#[async_trait::async_trait]
impl crate::model::cluster::StoragePermits for Backend {
    async fn acquire_storage_permit(
        &self,
        storage_id: uuid::Uuid,
        direction: &str,
        limit: i32,
        holder: uuid::Uuid,
    ) -> Result<Option<uuid::Uuid>> {
        transactions::try_acquire_storage_permit(&self.pool, storage_id, direction, limit, holder)
            .await
    }

    async fn renew_storage_permit(&self, token: uuid::Uuid) -> Result<bool> {
        transactions::renew_storage_permit(&self.pool, token).await
    }

    async fn release_storage_permit(&self, token: uuid::Uuid) -> Result<()> {
        transactions::release_storage_permit(&self.pool, token).await
    }
}

#[async_trait::async_trait]
impl crate::model::cluster::NodeWrites for Backend {
    async fn check_raw_files(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_native::Inventory,
    ) -> Result<Vec<crate::model::node_raw::File>> {
        transactions::raw_cache::inventory(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn prepare_tail(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_tail::Prepare,
    ) -> Result<crate::model::node_tail::Prepared> {
        transactions::prepare_node_tail(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn commit_tail(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_write::Lookup,
    ) -> Result<crate::model::node_tail::Prepared> {
        transactions::commit_node_tail(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn completion_status(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_complete::Lookup,
    ) -> Result<(
        crate::model::node_write::Phase,
        crate::model::node_complete::Summary,
    )> {
        transactions::node_completion_status(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn complete_sources(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_complete::Sources,
    ) -> Result<crate::model::node_complete::SourceBatch> {
        transactions::node_complete_sources(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn prepare_chunks(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_chunk::Prepare,
    ) -> Result<crate::model::node_chunk::Assembly> {
        super::super::transactions::prepare_node_chunks(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn activate_chunks(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_chunk::Prepare,
        nonces: Vec<crate::model::node_chunk::Nonce>,
    ) -> Result<crate::model::node_chunk::Prepared> {
        super::super::transactions::activate_node_chunks(self, node, key, input, nonces)
            .await
            .map_err(normalize_error)
    }
    async fn commit_chunk_batch(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: &mut crate::model::node_chunk::Commit,
        verified: &[crate::model::Id],
    ) -> Result<crate::model::node_chunk::CommitStep> {
        super::super::transactions::commit_node_chunks(self, node, key, input, verified)
            .await
            .map_err(normalize_error)
    }
    async fn copy_write_references(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_write::CopyBatch,
    ) -> Result<i64> {
        transactions::copy_node_references(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn begin_write(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_write::Begin,
    ) -> Result<crate::model::node_write::Reply> {
        transactions::begin_node_write(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn write_status(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_write::Lookup,
        abort: bool,
    ) -> Result<crate::model::node_write::Reply> {
        transactions::node_write_status(self, node, key, input, abort)
            .await
            .map_err(normalize_error)
    }
    async fn publish_write(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_write::Publish,
    ) -> Result<crate::model::node_write::Reply> {
        super::super::transactions::publish_node_write(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn prune_write_receipts(&self, limit: i64) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        self.fence_control(&mut tx).await?;
        let count = expired_node_writes(limit)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(count)
    }
    async fn prepare_raw(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_raw::Prepare,
    ) -> Result<crate::model::node_raw::Prepared> {
        transactions::prepare_node_raw(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn commit_raw(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_raw::Commit,
    ) -> Result<crate::model::node_raw::Prepared> {
        transactions::commit_node_raw(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
}

#[async_trait::async_trait]
impl crate::model::cluster::StagingStore for Backend {
    async fn release_staging(&self, storage: Uuid, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM staging_objects WHERE storage_id=$1 AND object_key=$2")
            .bind(storage)
            .bind(key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn stale_staging(&self, storage: Option<Uuid>) -> Result<Vec<(Uuid, String)>> {
        Ok(sqlx::query_as("SELECT s.storage_id,s.object_key FROM staging_objects s WHERE ($1::uuid IS NULL OR s.storage_id=$1) AND s.created_at < now()-interval '5 minutes' AND NOT EXISTS (SELECT 1 FROM pending_uploads p WHERE p.staging_storage_id=s.storage_id AND p.staging_object_key=s.object_key) AND NOT EXISTS (SELECT 1 FROM fragments f WHERE f.staging_storage_id=s.storage_id AND f.staging_object_key=s.object_key) AND NOT EXISTS (SELECT 1 FROM uploads u WHERE u.staging_storage_id=s.storage_id AND u.staging_object_key=s.object_key) ORDER BY s.created_at LIMIT 128").bind(storage).fetch_all(&self.pool).await?)
    }

    async fn claim_staging(&self, storage: Uuid, key: &str, bytes: u64, limit: u64) -> Result<()> {
        transactions::claim_staging(&self.pool, storage, key, bytes, limit).await
    }
}

#[async_trait::async_trait]
impl crate::model::cluster::NativeParts for Backend {
    async fn prepare_local_part(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_native::LocalPart,
    ) -> Result<crate::model::node_write::Reply> {
        transactions::native::prepare_local_part(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn claim_native(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_native::Claim,
    ) -> Result<crate::model::node_native::Reply> {
        transactions::native::claim(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn finish_native(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_native::Finish,
    ) -> Result<()> {
        transactions::native::finish(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn check_local_files(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_native::Inventory,
    ) -> Result<Vec<crate::model::node_raw::File>> {
        transactions::native::inventory(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn prepare_native_complete(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_native::Complete,
    ) -> Result<crate::model::node_native::PreparedCompletion> {
        transactions::native::complete(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn stage_native_pages(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_native::Pages,
    ) -> Result<()> {
        transactions::native::stage_pages(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
    async fn adopt_native_complete(
        &self,
        node: &NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_native::Adopt,
    ) -> Result<crate::model::node_write::Reply> {
        transactions::native::adopt(self, node, key, input)
            .await
            .map_err(normalize_error)
    }
}

#[async_trait::async_trait]
impl crate::model::cluster::RemoteParts for Backend {
    async fn remote_source_id(&self, upload: Uuid, _stream: Id, token: Uuid) -> Result<Id> {
        let mut tx = self.pool.begin().await?;
        self.fence_control(&mut tx).await?;
        let source = remote_output_lock(upload, token)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(crate::model::Error::OperationAborted)?;
        tx.commit().await?;
        Ok(source.into())
    }
    async fn remote_upload(&self, upload: Uuid) -> Result<Option<RemoteUpload>> {
        Ok(remote_upload(upload).fetch_optional(&self.pool).await?)
    }
    async fn begin_remote(
        &self,
        upload: Uuid,
        storage: Uuid,
        revision: i64,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<RemoteUpload> {
        transactions::begin_remote(self, upload, storage, revision, node).await
    }
    async fn claim_remote(
        &self,
        upload: Uuid,
        token: Uuid,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::claim_remote(self, upload, token, node).await
    }
    async fn initialized_remote(
        &self,
        upload: Uuid,
        token: Uuid,
        upstream: Option<&str>,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::initialized_remote(self, upload, token, upstream, node).await
    }
    async fn commit_remote_part(&self, input: RemoteCommit<'_>) -> Result<()> {
        transactions::commit_upload_part(
            self,
            input.authority,
            input.bucket,
            input.upload,
            input.number,
            input.stream,
            Some(&input),
        )
        .await
    }
    async fn claim_remote_part(
        &self,
        token: Uuid,
        node: crate::model::raw::Processor<'_>,
        stream: Option<Id>,
    ) -> Result<Option<RemotePart>> {
        transactions::claim_remote_part(self, token, node, stream).await
    }
    async fn renew_remote_part(
        &self,
        work: &RemotePart,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::finish_remote_part(self, work, None, node).await
    }
    async fn finish_remote_part(
        &self,
        work: &RemotePart,
        etag: &str,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        anyhow::ensure!(
            !etag.is_empty() && etag.len() <= 1024,
            "invalid remote part ETag"
        );
        transactions::finish_remote_part(self, work, Some(etag), node).await
    }
    async fn retry_remote_part(&self, work: &RemotePart, error: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        self.fence_control(&mut tx).await?;
        sqlx::query("UPDATE parts SET remote_token=NULL,remote_expires_at=NULL,remote_retry_at=clock_timestamp()+interval '2 seconds',remote_error=left($5,1024) WHERE upload_id=$1 AND part_number=$2 AND stream_id=$3 AND remote_token=$4")
            .bind(work.upload.upload).bind(work.part_number).bind(work.stream.id).bind(work.remote_token).bind(error).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn remote_parts(
        &self,
        upload: Uuid,
        streams: &[Id],
    ) -> Result<Vec<(Id, i32, Option<String>)>> {
        Ok(sqlx::query_as("SELECT stream_id,part_number,remote_etag FROM parts WHERE upload_id=$1 AND stream_id=ANY($2) ORDER BY part_number").bind(upload).bind(streams).fetch_all(&self.pool).await?)
    }
    async fn freeze_remote(&self, upload: Uuid, token: Uuid, hash: &str) -> Result<RemoteUpload> {
        transactions::freeze_remote(self, upload, token, hash).await
    }
    async fn resend_remote(&self, upload: Uuid, token: Uuid, streams: &[Id]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        self.fence_control(&mut tx).await?;
        sqlx::query("UPDATE parts p SET remote_etag=NULL,remote_retry_at=clock_timestamp() FROM uploads u WHERE u.id=$1 AND u.state='completing' AND u.completion_token=$2 AND u.completion_expires_at>clock_timestamp() AND p.upload_id=u.id AND p.stream_id=ANY($3)").bind(upload).bind(token).bind(streams).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn adopt_remote(&self, input: RemoteOutput<'_>) -> Result<Id> {
        transactions::adopt_remote(self, input).await.map(Id::from)
    }
    async fn remote_charges(&self, node: Uuid, after: Id) -> Result<Vec<(Id, Uuid, i64)>> {
        Ok(sqlx::query_as("SELECT s.id,r.storage_id,s.size FROM parts p JOIN streams s ON s.id=p.stream_id JOIN uploads u ON u.id=p.upload_id JOIN raw_sources r ON r.id=u.raw_source_id WHERE p.remote_node_id=$1 AND p.remote_cached AND p.remote_etag IS NULL AND s.id>$2 AND u.state IN ('active','completing') ORDER BY s.id LIMIT 128").bind(node).bind(after).fetch_all(&self.pool).await?)
    }
    async fn claim_remote_cleanup(
        &self,
        token: Uuid,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<Option<RemoteUpload>> {
        transactions::claim_remote_cleanup(self, token, node).await
    }
    async fn clean_remote(&self, upload: Uuid, token: Uuid) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        self.fence_control(&mut tx).await?;
        sqlx::query("UPDATE uploads SET raw_source_id=NULL,remote_token=NULL,remote_expires_at=NULL WHERE id=$1 AND remote_token=$2 AND remote_expires_at>clock_timestamp() AND state IN ('aborted','completed')").bind(upload).bind(token).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::model::cluster::NodeInventory for Backend {
    async fn begin_report(
        &self,
        state: &crate::cluster::NodeState,
        role: &str,
        key: &crate::content::codec::Key,
        boot: Uuid,
        started: DateTime<Utc>,
    ) -> Result<()> {
        transactions::begin_report(&self.pool, state, role, key, boot, started).await
    }
    async fn status(&self) -> Result<ClusterStatus> {
        let (mode, mode_revision,remote_work,local_storages):(String,i64,bool,i64)=sqlx::query_as("SELECT deployment_mode,deployment_mode_revision,remote_work,(SELECT count(*) FROM storages WHERE type='local') FROM mokyu_meta").fetch_one(&self.pool).await?;
        let (enabled_nodes,remote_nodes,ready_data,ready_workers):(i64,i64,i64,i64)=sqlx::query_as("SELECT count(*),count(*) FILTER(WHERE n.role<>'control'),count(*) FILTER(WHERE n.role IN ('data','node') AND ss.expires_at>now() AND NOT ss.draining AND ss.boot_id=r.boot_id AND ss.credential_generation=n.credential_generation AND r.status->>'ready'='true' AND r.status->>'draining'='false' AND r.received_at>=now()-interval '30 seconds' AND n.last_seen_at>=now()-interval '30 seconds' AND r.status->>'deployment_mode_revision'=$1),count(*) FILTER(WHERE n.role IN ('worker','node') AND ss.expires_at>now() AND NOT ss.draining AND ss.boot_id=r.boot_id AND ss.credential_generation=n.credential_generation AND r.status->>'ready'='true' AND r.status->>'draining'='false' AND r.received_at>=now()-interval '30 seconds' AND n.last_seen_at>=now()-interval '30 seconds' AND r.status->>'deployment_mode_revision'=$1) FROM nodes n LEFT JOIN node_reports r ON r.node_id=n.id LEFT JOIN node_sessions ss ON ss.node_id=n.id WHERE n.enabled").bind(mode_revision.to_string()).fetch_one(&self.pool).await?;
        Ok(ClusterStatus {
            mode,
            mode_revision,
            enabled_nodes,
            remote_nodes,
            ready_data,
            ready_workers,
            remote_work,
            local_storages,
        })
    }
    async fn nodes(&self, filter: &NodeFilter, desired: &Value) -> Result<NodePage> {
        let cursor = filter.cursor()?;
        let mut items: Vec<NodeView> = sqlx::query_as(sqlx::AssertSqlSafe(format!("{NODE_OBSERVATIONS} SELECT * FROM states WHERE ($3::text IS NULL OR role=$3) AND (($4::text IS NULL AND enabled) OR contact=$4) AND ($5::text IS NULL OR work=$5) AND ($6::text IS NULL OR CASE WHEN $7::text IS NULL THEN configuration ELSE settings->>$7 END=$6) AND ($8::uuid IS NULL OR id>$8) AND ($10::text IS NULL OR strpos(lower(name),lower($10))>0) ORDER BY id LIMIT $9")))
            .bind(desired).bind(CONTACT_SECONDS).bind(&filter.role).bind(&filter.contact).bind(&filter.work)
            .bind(&filter.configuration).bind(&filter.section).bind(cursor).bind(filter.limit+1).bind(&filter.name)
            .fetch_all(&self.pool).await?;
        let more = items.len() > filter.limit as usize;
        items.truncate(filter.limit as usize);
        let next_cursor = if more {
            items.last().map(|node| filter.next(node.id)).transpose()?
        } else {
            None
        };
        Ok(NodePage { items, next_cursor })
    }
    async fn node(&self, id: Uuid, desired: &Value) -> Result<Option<NodeView>> {
        Ok(sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "{NODE_OBSERVATIONS} SELECT * FROM states WHERE id=$3"
        )))
        .bind(desired)
        .bind(CONTACT_SECONDS)
        .bind(id)
        .fetch_optional(&self.pool)
        .await?)
    }
    async fn node_counts(&self, desired: &Value) -> Result<Vec<NodeCount>> {
        let rows: Vec<(String,String,String,String,String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!("{NODE_OBSERVATIONS} SELECT role,contact,work,configuration,count(*)::text FROM states GROUP BY role,contact,work,configuration ORDER BY role,contact,work,configuration")))
            .bind(desired).bind(CONTACT_SECONDS).fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|(role, contact, work, configuration, count)| NodeCount {
                role,
                contact,
                work,
                configuration,
                count,
            })
            .collect())
    }
    async fn setting_counts(&self, desired: &Value) -> Result<Vec<SettingCount>> {
        let rows: Vec<(String,String,String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!("{NODE_OBSERVATIONS} SELECT s.key,s.value,count(*)::text FROM states CROSS JOIN LATERAL jsonb_each_text(settings) s WHERE enabled GROUP BY s.key,s.value ORDER BY s.key,s.value")))
            .bind(desired).bind(CONTACT_SECONDS).fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|(section, state, count)| SettingCount {
                section,
                state,
                count,
            })
            .collect())
    }
    async fn node_report(&self, id: Uuid) -> Result<Option<SavedReport>> {
        let row: Option<(Uuid, DateTime<Utc>, Value, Value)> = sqlx::query_as(
            "SELECT boot_id,started_at,metadata,status FROM node_reports WHERE node_id=$1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(
            row.map(|(boot_id, started_at, metadata, status)| SavedReport {
                boot_id,
                started_at,
                metadata,
                status,
            }),
        )
    }
    async fn node_tasks(&self, id: Uuid) -> Result<Vec<crate::model::metrics::WorkSummary>> {
        Ok(sqlx::query_as("SELECT id,task_type,state,processed::text,updated_at FROM tasks WHERE lease_node_id=$1 AND state='running' ORDER BY updated_at DESC,id LIMIT 21").bind(id).fetch_all(&self.pool).await?)
    }
    async fn sample_node_runtime(
        &self,
        state: &crate::cluster::NodeState,
        role: &str,
        key: &crate::content::codec::Key,
        sample: &crate::model::metrics::NodeSample,
    ) -> Result<bool> {
        transactions::sample_node_runtime(&self.pool, state, role, key, sample)
            .await
            .map_err(normalize_error)
    }
    async fn aggregate_runtime(&self, interval: u64, retention: u64) -> Result<()> {
        transactions::aggregate_runtime(&self.pool, interval, retention)
            .await
            .map_err(normalize_error)
    }
    async fn cluster_runtime(
        &self,
        query: &crate::model::metrics::RuntimeQuery,
    ) -> Result<Vec<crate::model::metrics::ClusterRuntimePoint>> {
        query.validate()?;
        let mut tx = self.pool.begin().await?;
        runtime_diagnostic_limits().execute(&mut *tx).await?;
        let rows = cluster_runtime_history(query)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| normalize_error(e.into()))?;
        tx.commit().await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }
    async fn node_runtime(
        &self,
        id: Uuid,
        query: &crate::model::metrics::RuntimeQuery,
    ) -> Result<Vec<crate::model::metrics::NodeRuntimePoint>> {
        query.validate()?;
        let mut tx = self.pool.begin().await?;
        runtime_diagnostic_limits().execute(&mut *tx).await?;
        let rows = node_runtime_history(id, query)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| normalize_error(e.into()))?;
        tx.commit().await?;
        Ok(rows
            .into_iter()
            .map(
                |(node_id, received_at, sample)| crate::model::metrics::NodeRuntimePoint {
                    node_id,
                    received_at,
                    sample: sample.0,
                },
            )
            .collect())
    }
    async fn report(
        &self,
        state: &crate::cluster::NodeState,
        role: &str,
        key: &crate::content::codec::Key,
        report: &NodeReport,
    ) -> Result<bool> {
        transactions::report_node(&self.pool, state, role, key, report).await
    }
}
