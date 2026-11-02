use super::*;

pub(in super::super) const MIGRATION_SETUP: &str =
    "SET synchronous_commit = on; SET lock_timeout = '30s'";

pub(in super::super) fn enabled_storage(id: Uuid) -> Rows<'static, StorageRecord> {
    sqlx::query_as("SELECT s.id,s.type AS store_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.access_key_encrypted AS access,s.secret_key_encrypted AS secret,s.identity_marker,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=$1 AND s.enabled")
        .bind(id)
}

pub(in super::super) fn storage_key(id: Uuid) -> Rows<'static, ManagedKeyRecord> {
    sqlx::query_as("SELECT k.id,k.name,k.key_algorithm AS algorithm FROM storages s JOIN managed_keys k ON k.id=s.block_key_id WHERE s.id=$1 AND k.purpose='block' AND k.revoked_at IS NULL")
        .bind(id)
}

pub(in super::super) fn managed_key<'q>(name: &'q str) -> Rows<'q, ManagedKeyMaterial> {
    sqlx::query_as("SELECT id,name,purpose,key_algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE name=$1 AND secret_encrypted IS NOT NULL")
        .bind(name)
}

pub(in super::super) fn ensure_runtime_setting<'q>(
    section: &'q str,
    value: &'q Value,
) -> Statement<'q> {
    sqlx::query("INSERT INTO runtime_settings(section,value) VALUES($1,$2) ON CONFLICT (section) DO NOTHING")
        .bind(section)
        .bind(value)
}

pub(in super::super) fn runtime_settings() -> Rows<'static, (String, Value, i64)> {
    sqlx::query_as("SELECT section,value,revision FROM runtime_settings ORDER BY section")
}

pub(in super::super) fn runtime_setting<'q>(section: &'q str) -> Scalar<'q, Value> {
    sqlx::query_scalar("SELECT value FROM runtime_settings WHERE section=$1").bind(section)
}

pub(in super::super) fn managed_keys() -> Rows<'static, ManagedKeyMaterial> {
    sqlx::query_as(
        "SELECT id,name,purpose,key_algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE secret_encrypted IS NOT NULL ORDER BY id",
    )
}

pub(in super::super) fn create_read_lease(stream: Id, ttl: i64) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("INSERT INTO stream_leases(stream_id,lease_type,expires_at) VALUES($1,'read',now()+$2*interval '1 second') RETURNING id")

        .bind(stream)
        .bind(ttl)
}

pub(in super::super) fn create_write_lease(stream: Id, ttl: i64) -> Statement<'static> {
    sqlx::query("INSERT INTO stream_leases(stream_id,lease_type,expires_at) VALUES($1,'write',now()+$2*interval '1 second')")

        .bind(stream)
        .bind(ttl)
}

pub(in super::super) fn server_version()
-> sqlx::query::QueryAs<'static, Postgres, (i32, String), PgArguments> {
    sqlx::query_as(
        "SELECT current_setting('server_version_num')::integer, current_setting('server_version')",
    )
}

pub(in super::super) fn synchronous_commit() -> Statement<'static> {
    sqlx::query("SET synchronous_commit = on")
}

pub(in super::super) fn statement_timeout() -> Statement<'static> {
    sqlx::query("SET statement_timeout = '30s'")
}

pub(in super::super) fn fsync() -> Scalar<'static, String> {
    sqlx::query_scalar("SHOW fsync")
}

pub(in super::super) const SCHEMA: &str = include_str!("../schema.sql");

pub(in super::super) fn flush_time() -> Scalar<'static, DateTime<Utc>> {
    sqlx::query_scalar("SELECT clock_timestamp()")
}

pub(in super::super) fn schema_empty() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT NOT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname NOT IN ('public','information_schema')) AND NOT EXISTS(SELECT 1 FROM pg_depend d JOIN pg_namespace n ON d.refclassid='pg_namespace'::regclass AND d.refobjid=n.oid WHERE n.nspname='public') AND NOT EXISTS(SELECT 1 FROM pg_extension WHERE extname<>'plpgsql') AND NOT EXISTS(SELECT 1 FROM pg_event_trigger)",
    )
}

pub(in super::super) fn initialize_meta(version: i64) -> Statement<'static> {
    sqlx::query("INSERT INTO mokyu_meta(schema_version,deployment_id) VALUES($1,gen_random_uuid())")
        .bind(version)
}

#[cfg(test)]
pub(in super::super) fn metadata_table() -> Scalar<'static, Option<String>> {
    sqlx::query_scalar("SELECT to_regclass('public.mokyu_meta')::text")
}

pub(in super::super) fn schema_version() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT schema_version::bigint FROM mokyu_meta WHERE singleton")
}

pub(in super::super) fn claim_runtime(single: bool) -> Scalar<'static, bool> {
    sqlx::query_scalar(if single {
        "SELECT pg_try_advisory_lock(734922709851001)"
    } else {
        "SELECT pg_try_advisory_lock_shared(734922709851001)"
    })
}

pub(in super::super) fn release_runtime() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT pg_advisory_unlock(734922709851001)")
}

#[cfg(test)]
pub(in super::super) fn migration_identity<'q>(value: &'q str) -> Statement<'q> {
    sqlx::query("SELECT set_config('media_gateway.backend_identity',$1,false)").bind(value)
}

pub(in super::super) fn deployment_fingerprint() -> Scalar<'static, Option<Vec<u8>>> {
    sqlx::query_scalar("SELECT deployment_key_fingerprint FROM mokyu_meta WHERE singleton")
}

pub(in super::super) fn set_deployment_fingerprint<'q>(value: &'q [u8]) -> Statement<'q> {
    sqlx::query("UPDATE mokyu_meta SET deployment_key_fingerprint=$1 WHERE singleton").bind(value)
}

pub(in super::super) fn register_key_fingerprint<'q>(
    id: &'q str,
    algorithm: &'q str,
    hash: &'q [u8],
) -> Statement<'q> {
    sqlx::query("INSERT INTO key_fingerprints(key_id,algorithm,fingerprint) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
        .bind(id)
        .bind(algorithm)
        .bind(hash)
}

pub(in super::super) fn key_fingerprint<'q>(id: &'q str) -> Rows<'q, (String, Vec<u8>)> {
    sqlx::query_as("SELECT algorithm,fingerprint FROM key_fingerprints WHERE key_id=$1").bind(id)
}

pub(in super::super) fn required_keys() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT DISTINCT block_key_id FROM (SELECT block_key_id FROM chunk_encodings WHERE encryption_algorithm<>'none' AND state<>'deleted' UNION SELECT block_key_id FROM packs WHERE encryption_algorithm<>'none' AND state<>'deleted' UNION SELECT block_key_id FROM chunks WHERE encryption_algorithm<>'none' AND EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=chunks.id)) keys WHERE block_key_id IS NOT NULL",
    )
}

#[async_trait::async_trait]
impl RuntimeSettingsStore for Backend {
    async fn revisions(&self) -> Result<Vec<(String, i64)>> {
        let revisions: Vec<(String, i64)> =
            sqlx::query_as("SELECT section,revision FROM runtime_settings")
                .fetch_all(&self.pool)
                .await?;
        Ok(revisions)
    }
    async fn inspect(&self) -> Result<Vec<(String, Value, i64, bool)>> {
        let runtime: Vec<(String, serde_json::Value, i64, bool)> = sqlx::query_as(
        "SELECT section,value,revision,updated_by_user_id IS NULL FROM runtime_settings ORDER BY section",
    )
            .fetch_all(&self.pool)
            .await?;
        Ok(runtime)
    }

    async fn row(&self, section: &str) -> Result<RuntimeSettingsRow> {
        Ok(runtime_settings_row(section).fetch_one(&self.pool).await?)
    }
    async fn sections(&self, principal: &Principal) -> Result<Vec<(String, Value, i64)>> {
        transactions::runtime_settings_sections(&self.pool, principal).await
    }
    async fn section(&self, principal: &Principal, section: &str) -> Result<RuntimeSettingsRow> {
        transactions::runtime_settings_section(&self.pool, principal, section).await
    }
    async fn save(
        &self,
        principal: &Principal,
        section: &str,
        revision: Option<i64>,
        value: Value,
    ) -> Result<RuntimeSettingsRow> {
        transactions::save_runtime_settings(&self.pool, principal, section, revision, value).await
    }
    async fn reset(
        &self,
        principal: &Principal,
        section: &str,
        value: Value,
    ) -> Result<RuntimeSettingsRow> {
        transactions::reset_runtime_settings(&self.pool, principal, section, value).await
    }
    async fn update_web(
        &self,
        principal: &Principal,
        input: ControlSettingsPatch,
    ) -> Result<(crate::config::WebSettings, i64)> {
        transactions::update_web_settings(&self.pool, principal, input).await
    }

    async fn ensure(&self, defaults: &[(&str, Value)]) -> Result<()> {
        for (section, value) in defaults {
            ensure_runtime_setting(section, value)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn load(&self) -> Result<Vec<(String, Value, i64)>> {
        Ok(runtime_settings().fetch_all(&self.pool).await?)
    }

    async fn value(&self, section: &str) -> Result<Value> {
        Ok(runtime_setting(section).fetch_one(&self.pool).await?)
    }
}

pub(in super::super) fn runtime_settings_row(section: &str) -> Rows<'_, RuntimeSettingsRow> {
    sqlx::query_as("SELECT value,revision FROM runtime_settings WHERE section=$1").bind(section)
}

pub(in super::super) fn lock_runtime_settings(section: &str) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT revision FROM runtime_settings WHERE section=$1 FOR UPDATE")
        .bind(section)
}

pub(in super::super) fn save_runtime_settings<'q>(
    section: &'q str,
    value: Value,
    user: Option<Uuid>,
    revision: i64,
) -> Rows<'q, RuntimeSettingsRow> {
    sqlx::query_as("UPDATE runtime_settings SET value=$2,revision=revision+1,updated_at=now(),updated_by_user_id=$3 WHERE section=$1 AND revision=$4 RETURNING value,revision").bind(section).bind(value).bind(user).bind(revision)
}

pub(in super::super) fn reset_runtime_settings<'q>(
    section: &'q str,
    value: Value,
    user: Option<Uuid>,
) -> Rows<'q, RuntimeSettingsRow> {
    sqlx::query_as("UPDATE runtime_settings SET value=$2,revision=revision+1,updated_at=now(),updated_by_user_id=$3 WHERE section=$1 RETURNING value,revision").bind(section).bind(value).bind(user)
}

pub(in super::super) fn lock_web_settings() -> Rows<'static, RuntimeSettingsRow> {
    sqlx::query_as("SELECT value,revision FROM runtime_settings WHERE section='web' FOR UPDATE")
}

pub(in super::super) fn save_web_settings(
    value: Value,
    next: i64,
    user: Option<Uuid>,
    revision: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE runtime_settings SET value=$1,revision=$2,updated_at=now(),updated_by_user_id=$3 WHERE section='web' AND revision=$4").bind(value).bind(next).bind(user).bind(revision)
}

pub(in super::super) type ManagedKeyViewRow = (
    Uuid,
    String,
    String,
    String,
    bool,
    Vec<u8>,
    DateTime<Utc>,
    Option<DateTime<Utc>>,
);

pub(in super::super) fn managed_key_views() -> Rows<'static, ManagedKeyViewRow> {
    sqlx::query_as(
        "SELECT k.id,k.name,k.purpose,k.key_algorithm AS algorithm,EXISTS(SELECT 1 FROM storages s WHERE s.block_key_id=k.id) OR EXISTS(SELECT 1 FROM tier_locations h WHERE h.block_key_id=k.id) OR EXISTS(SELECT 1 FROM tier_moves m WHERE m.source_key=k.id OR m.target_key=k.id) OR EXISTS(SELECT 1 FROM chunk_encodings e WHERE e.block_key_id=k.id AND e.state<>'deleted' AND NOT EXISTS(SELECT 1 FROM tier_locations h WHERE h.unit=0 AND h.physical_id=e.encoding_id)) OR EXISTS(SELECT 1 FROM packs p WHERE p.block_key_id=k.id AND p.state<>'deleted' AND NOT EXISTS(SELECT 1 FROM tier_locations h WHERE h.unit=1 AND h.physical_id=p.id)) OR EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.block_key_id=k.id) OR EXISTS(SELECT 1 FROM node_writes w JOIN stream_leases l ON l.id=w.lease_id WHERE w.block_key_id=k.id AND w.state='active' AND l.expires_at>clock_timestamp()) OR EXISTS(SELECT 1 FROM tasks t WHERE t.detail->>'target_key_id'=k.id::text AND t.state IN ('queued','running','paused','failed')),k.fingerprint,k.created_at,k.revoked_at FROM managed_keys k ORDER BY k.name",
    )
}

pub(in super::super) fn managed_key_in_use(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storages WHERE block_key_id=$1) OR EXISTS(SELECT 1 FROM tier_locations h WHERE h.block_key_id=$1) OR EXISTS(SELECT 1 FROM tier_moves m WHERE m.source_key=$1 OR m.target_key=$1) OR EXISTS(SELECT 1 FROM chunk_encodings e WHERE e.block_key_id=$1 AND e.state<>'deleted' AND NOT EXISTS(SELECT 1 FROM tier_locations h WHERE h.unit=0 AND h.physical_id=e.encoding_id)) OR EXISTS(SELECT 1 FROM packs p WHERE p.block_key_id=$1 AND p.state<>'deleted' AND NOT EXISTS(SELECT 1 FROM tier_locations h WHERE h.unit=1 AND h.physical_id=p.id)) OR EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.block_key_id=$1) OR EXISTS(SELECT 1 FROM node_writes w JOIN stream_leases l ON l.id=w.lease_id WHERE w.block_key_id=$1 AND w.state='active' AND l.expires_at>clock_timestamp()) OR EXISTS(SELECT 1 FROM tasks WHERE detail->>'target_key_id'=$1::text AND state IN ('queued','running','paused','failed')) FROM managed_keys WHERE id=$1").bind(id)
}

pub(in super::super) fn lock_managed_key(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT true FROM managed_keys WHERE id=$1 FOR UPDATE").bind(id)
}

pub(in super::super) fn insert_managed_key(
    input: &ManagedKeyCreate,
) -> Rows<'_, ManagedKeyViewRow> {
    sqlx::query_as("INSERT INTO managed_keys(id,name,purpose,key_algorithm,secret_encrypted,fingerprint) VALUES($1,$2,$3,$4,$5,$6) RETURNING id,name,purpose,key_algorithm AS algorithm,false,fingerprint,created_at,revoked_at").bind(input.id).bind(input.name.trim()).bind(&input.purpose).bind(&input.algorithm).bind(&input.protected).bind(input.fingerprint.as_slice())
}

pub(in super::super) fn revoke_managed_key(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE managed_keys SET revoked_at=coalesce(revoked_at,now()),secret_encrypted=NULL WHERE id=$1").bind(id)
}

pub(in super::super) fn managed_key_view(
    (id, name, purpose, algorithm, active, fingerprint, created_at, revoked_at): ManagedKeyViewRow,
) -> ManagedKeyView {
    ManagedKeyView {
        nonce_capacity: None,
        id,
        name,
        purpose,
        algorithm,
        in_use: active,
        fingerprint: hex::encode(fingerprint),
        created_at,
        revoked_at,
    }
}

pub(in super::super) fn deployment_mode() -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton")
}

pub(in super::super) fn deployment_mode_lock() -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton FOR UPDATE")
}

pub(in super::super) fn cluster_local_storages() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storages WHERE type='local')")
}

pub(in super::super) fn cluster_staging_id() -> Scalar<'static, Option<Uuid>> {
    sqlx::query_scalar(
        "SELECT NULLIF(value->>'staging_storage_id','')::uuid FROM runtime_settings WHERE section='uploads'",
    )
}

pub(in super::super) fn cluster_staging_valid(id: Option<Uuid>) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM storages WHERE id=$1 AND enabled AND type='s3')",
    )
    .bind(id)
}

pub(in super::super) fn cluster_other_nodes(
    id: Option<Uuid>,
    controls_only: bool,
) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM nodes WHERE enabled AND ($1::uuid IS NULL OR id<>$1) AND (NOT $2 OR role='control'))",
    )
    .bind(id).bind(controls_only)
}

pub(in super::super) fn mode_transition_state() -> Rows<'static, (bool, bool, bool, bool)> {
    sqlx::query_as(
        "SELECT maintenance,EXISTS(SELECT 1 FROM tasks WHERE state IN ('queued','running','paused') OR lease_token IS NOT NULL),EXISTS(SELECT 1 FROM uploads WHERE state IN ('active','completing')) OR EXISTS(SELECT 1 FROM streams WHERE state='writing') OR EXISTS(SELECT 1 FROM stream_leases WHERE expires_at>clock_timestamp()),EXISTS(SELECT 1 FROM pending_uploads) OR EXISTS(SELECT 1 FROM raw_uploads) OR EXISTS(SELECT 1 FROM extents e JOIN streams s ON s.id=e.stream_id WHERE e.fragment_id IS NOT NULL AND s.state IN ('ready','writing')) FROM mokyu_meta WHERE singleton",
    )
}

pub(in super::super) fn preserve_failed_task_mode(mode: &str) -> Statement<'_> {
    sqlx::query("UPDATE tasks SET detail=jsonb_set(detail,'{execution_mode}',to_jsonb($1::text)) WHERE state='failed' AND NOT detail ? 'execution_mode'").bind(mode)
}

pub(in super::super) fn restore_parked_control() -> Statement<'static> {
    sqlx::query(
        "UPDATE nodes SET enabled=true,status='registered',updated_at=now() WHERE role='control' AND NOT enabled AND status='standby'",
    )
}

pub(in super::super) fn park_control(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE nodes SET enabled=false,status='standby',last_seen_at=NULL,updated_at=now() WHERE id=$1 AND enabled AND role='control'").bind(id)
}

pub(in super::super) fn release_node_id<'q>(id: &'q Uuid) -> Statement<'q> {
    sqlx::query("UPDATE nodes SET enabled=false,credential_encrypted=NULL,last_seen_at=NULL,status='released',recovery_required=false,updated_at=now() WHERE id=$1").bind(id)
}

pub(in super::super) fn release_node_tasks<'q>(id: &'q Uuid) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET state=CASE WHEN execution_scope='node' THEN 'paused' ELSE 'queued' END,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE lease_node_id=$1 AND state='running'").bind(id)
}

pub(in super::super) fn release_node_permits<'q>(id: &'q Uuid) -> Statement<'q> {
    sqlx::query("DELETE FROM storage_permits WHERE holder_id=$1").bind(id)
}

pub(in super::super) fn invalidate_mode_sessions() -> Statement<'static> {
    sqlx::query(
        "UPDATE node_sessions SET expires_at=clock_timestamp(),draining=true WHERE expires_at>clock_timestamp()",
    )
}

pub(in super::super) fn update_deployment_mode<'q>(mode: &'q str) -> Statement<'q> {
    sqlx::query("UPDATE mokyu_meta SET deployment_mode=$1,deployment_mode_revision=deployment_mode_revision+1 WHERE singleton").bind(mode)
}

pub(in super::super) fn retirement(id: Uuid) -> Rows<'static, (String, i64, i64, i64, i64)> {
    sqlx::query_as("SELECT role,(SELECT count(*) FROM pending_uploads WHERE node_id=$1 AND staging_storage_id IS NULL),(SELECT count(*) FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE u.node_id=$1 AND r.pending_upload AND r.medium IN ('local','memory')),(SELECT count(*) FROM parts p JOIN uploads u ON u.id=p.upload_id WHERE p.remote_node_id=$1 AND p.remote_etag IS NULL AND u.state IN ('active','completing')),(SELECT count(*) FROM node_writes WHERE node_id=$1 AND state='active') FROM nodes WHERE id=$1").bind(id)
}

pub(in super::super) fn release_node_name<'q>(name: &'q str) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("UPDATE nodes SET enabled=false,credential_encrypted=NULL,last_seen_at=NULL,status='released',recovery_required=false,updated_at=now() WHERE name=$1 AND enabled RETURNING id").bind(name)
}

pub(in super::super) fn node_seen(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE nodes SET last_seen_at=now(),updated_at=now() WHERE id=$1").bind(id)
}

pub(in super::super) fn recover_node_lock<'q>(name: &'q str) -> Rows<'q, (Uuid, String)> {
    sqlx::query_as("SELECT id,role FROM nodes WHERE name=$1 FOR UPDATE").bind(name)
}

pub(in super::super) fn try_cluster_leader(key: i64) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)").bind(key)
}

pub(in super::super) fn maintenance_state() -> Rows<'static, (Value, bool, DateTime<Utc>)> {
    sqlx::query_as(
        "SELECT jsonb_build_array(m.maintenance,m.gc_paused,m.pack_creation_paused,(SELECT jsonb_agg(jsonb_build_array(c.policy,c.paused,c.last_scheduled_at) ORDER BY c.policy) FROM maintenance_controls c),(SELECT jsonb_agg(jsonb_build_array(s.id,s.enabled,p.revision) ORDER BY s.id) FROM storages s JOIN storage_profiles p ON p.storage_id=s.id)),NOT m.maintenance AND EXISTS(SELECT 1 FROM maintenance_controls WHERE policy='reuse' AND NOT paused) AND EXISTS(SELECT 1 FROM storages WHERE enabled) AND EXISTS(SELECT 1 FROM pack_changes WHERE reason='reuse' AND next_check_at<=clock_timestamp()),clock_timestamp() FROM mokyu_meta m WHERE singleton",
    )
}

pub(in super::super) fn claim_control_epoch() -> Rows<'static, (i64, i32)> {
    sqlx::query_as(
        "WITH claimed AS (UPDATE mokyu_meta SET control_epoch=control_epoch+1 WHERE singleton AND deployment_mode='single_control' RETURNING control_epoch), expired AS (UPDATE node_sessions SET expires_at=clock_timestamp() WHERE expires_at>clock_timestamp() AND EXISTS (SELECT FROM claimed)) SELECT control_epoch,pg_backend_pid() FROM claimed",
    )
}

impl Backend {
    pub(in super::super) async fn fence_control(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<()> {
        if let Some((epoch, pid)) = self.control_epoch {
            let current: bool = sqlx::query_scalar("SELECT control_epoch=$1 AND deployment_mode='single_control' AND EXISTS(SELECT 1 FROM pg_locks WHERE pid=$2 AND locktype='advisory' AND classid=(734922709851001::bigint >> 32)::oid AND objid=(734922709851001::bigint & 4294967295)::oid AND objsubid=1 AND mode='ExclusiveLock' AND granted) FROM mokyu_meta WHERE singleton FOR SHARE")
                .bind(epoch).bind(pid).fetch_one(&mut **tx).await?;
            anyhow::ensure!(current, "control ownership was lost");
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl RuntimeStore for Backend {
    fn database_info(&self) -> Option<&crate::model::runtime::DatabaseInfo> {
        self.database_info.as_ref()
    }
    fn read_replica(&self) -> Option<Value> {
        self.replica.as_ref().map(connection::Replica::snapshot)
    }
    async fn initialize(&self, maintenance: bool) -> Result<RuntimeState> {
        if maintenance {
            set_maintenance_mode(true).execute(&self.pool).await?;
        }
        RuntimeStore::state(self).await
    }

    async fn state(&self) -> Result<RuntimeState> {
        let (gc_paused, maintenance, pack_creation_paused, mode, mode_revision) =
            runtime_state().fetch_one(&self.pool).await?;
        Ok(RuntimeState {
            gc_paused,
            maintenance,
            pack_creation_paused,
            mode,
            mode_revision,
        })
    }

    async fn set_mode(&self, principal: &Principal, mode: &str) -> Result<()> {
        connection::set_mode(self, principal, mode).await
    }
    async fn pending_uploads(&self) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads)")
                .fetch_one(&self.pool)
                .await?,
        )
    }

    async fn health(&self) -> Result<()> {
        health().execute(&self.pool).await?;
        Ok(())
    }

    fn pool(&self) -> (u32, usize) {
        (self.pool.size(), self.pool.num_idle())
    }
}

pub(in super::super) fn health() -> Statement<'static> {
    sqlx::query("SELECT 1")
}

fn runtime_state() -> Rows<'static, (bool, bool, bool, String, i64)> {
    sqlx::query_as(
        "SELECT gc_paused,maintenance,pack_creation_paused,deployment_mode,deployment_mode_revision FROM mokyu_meta",
    )
}
