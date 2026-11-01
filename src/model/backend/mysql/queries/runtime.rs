use super::*;

pub(in super::super) const SCHEMA: &str = include_str!("../schema.sql");

pub(in super::super) fn flush_time() -> Scalar<'static, DateTime<Utc>> {
    sqlx::query_scalar("SELECT CURRENT_TIMESTAMP(6)")
}

pub(in super::super) fn schema_empty() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT NOT EXISTS(SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()) AND NOT EXISTS(SELECT 1 FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA=DATABASE()) AND NOT EXISTS(SELECT 1 FROM information_schema.EVENTS WHERE EVENT_SCHEMA=DATABASE())",
    )
}

pub(in super::super) fn initialize_meta(version: i64) -> Statement<'static> {
    sqlx::query("INSERT INTO mokyu_meta(id,schema_version,deployment_id) VALUES(1,?,UNHEX(REPLACE(UUID(),'-','')))").bind(version)
}

#[async_trait::async_trait]
impl RuntimeStore for Backend {
    fn database_info(&self) -> Option<&crate::model::runtime::DatabaseInfo> {
        Some(&self.database_info)
    }
    async fn initialize(&self, maintenance: bool) -> Result<RuntimeState> {
        if maintenance {
            sqlx::query("UPDATE mokyu_meta SET maintenance=TRUE WHERE id=1")
                .execute(&self.pool)
                .await?;
        }
        self.state().await
    }
    async fn state(&self) -> Result<RuntimeState> {
        let (gc_paused, maintenance, pack_creation_paused) = sqlx::query_as(
            "SELECT gc_paused,maintenance,pack_creation_paused FROM mokyu_meta WHERE id=1",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(RuntimeState {
            gc_paused,
            maintenance,
            pack_creation_paused,
            mode: "single".into(),
            mode_revision: 0,
        })
    }
    async fn pending_uploads(&self) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads)")
                .fetch_one(&self.pool)
                .await?,
        )
    }
    async fn health(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }
    fn pool(&self) -> (u32, usize) {
        (self.pool.size(), self.pool.num_idle())
    }
}

const KEY_REFERENCES: &str = "EXISTS(SELECT 1 FROM storages s WHERE s.block_key_id=k.id) OR EXISTS(SELECT 1 FROM tier_locations h WHERE h.block_key_id=k.id) OR EXISTS(SELECT 1 FROM tier_moves m WHERE m.source_key=k.id OR m.target_key=k.id) OR EXISTS(SELECT 1 FROM chunk_encodings e WHERE e.block_key_id=k.id AND e.state<>'deleted' AND NOT EXISTS(SELECT 1 FROM tier_locations h WHERE h.unit=0 AND h.physical_id=e.location_id)) OR EXISTS(SELECT 1 FROM packs p WHERE p.block_key_id=k.id AND p.state<>'deleted' AND NOT EXISTS(SELECT 1 FROM tier_locations h WHERE h.unit=1 AND h.physical_id=p.location_id)) OR EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.block_key_id=k.id) OR EXISTS(SELECT 1 FROM tasks t WHERE UNHEX(REPLACE(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.target_key_id')),'-',''))=k.id AND t.state IN ('queued','running','paused','failed'))";

pub(in super::super) async fn managed_key_views(
    connection: &mut sqlx::MySqlConnection,
) -> Result<Vec<ManagedKeyView>> {
    let mut query = managed_key_query();
    query.push(" ORDER BY k.name");
    Ok(query.build_query_as().fetch_all(connection).await?)
}

fn managed_key_query() -> sqlx::QueryBuilder<MySql> {
    let mut query =
        sqlx::QueryBuilder::new("SELECT k.id,k.name,k.purpose,k.key_algorithm AS algorithm,");
    query.push(KEY_REFERENCES).push(" AS in_use,LOWER(HEX(k.fingerprint)) AS fingerprint,k.created_at,k.revoked_at FROM managed_keys k");
    query
}

pub(in super::super) async fn managed_key_view(
    connection: &mut sqlx::MySqlConnection,
    id: Uuid,
) -> Result<ManagedKeyView> {
    let mut query = managed_key_query();
    query.push(" WHERE k.id=").push_bind(id);
    Ok(query.build_query_as().fetch_one(connection).await?)
}

pub(in super::super) async fn managed_key_in_use(
    connection: &mut sqlx::MySqlConnection,
    id: Uuid,
) -> Result<bool> {
    let mut query = sqlx::QueryBuilder::new("SELECT ");
    query
        .push(KEY_REFERENCES)
        .push(" FROM managed_keys k WHERE k.id=")
        .push_bind(id);
    Ok(query.build_query_scalar().fetch_one(connection).await?)
}

pub(in super::super) fn insert_managed_key(input: &ManagedKeyCreate) -> Statement<'_> {
    sqlx::query("INSERT INTO managed_keys(id,name,purpose,key_algorithm,secret_encrypted,fingerprint) VALUES(?,?,?,?,?,?)").bind(input.id).bind(input.name.trim()).bind(&input.purpose).bind(&input.algorithm).bind(&input.protected).bind(input.fingerprint.as_slice())
}

pub(in super::super) fn lock_managed_key(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM managed_keys WHERE id=? FOR UPDATE").bind(id)
}

pub(in super::super) fn revoke_managed_key(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE managed_keys SET revoked_at=COALESCE(revoked_at,CURRENT_TIMESTAMP(6)),secret_encrypted=NULL WHERE id=?").bind(id)
}

pub(in super::super) fn reset_runtime_settings(
    section: &str,
    value: Value,
    is_default: bool,
) -> Statement<'_> {
    sqlx::query("UPDATE runtime_settings SET value=?,revision=revision+1,updated_at=CURRENT_TIMESTAMP(6),is_default=? WHERE section=?").bind(value).bind(is_default).bind(section)
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
    sqlx::query("UPDATE runtime_settings SET value=?,revision=?,updated_at=CURRENT_TIMESTAMP(6),is_default=? WHERE section='web' AND revision=?").bind(value).bind(next).bind(user.is_none()).bind(revision)
}

pub(in super::super) const MYSQL_COLLATION: &str = "SET NAMES utf8mb4 COLLATE utf8mb4_0900_bin";

pub(in super::super) const MARIADB_COLLATION: &str = "SET NAMES utf8mb4 COLLATE utf8mb4_nopad_bin";

pub(in super::super) const SESSION_SETUP: &str = "SET SESSION time_zone='+00:00'; SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED; SET SESSION sql_mode='STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'; SET SESSION innodb_lock_wait_timeout=15";

pub(in super::super) fn server()
-> sqlx::query::QueryAs<'static, MySql, (String, i64), MySqlArguments> {
    sqlx::query_as("SELECT VERSION(),CAST(@@GLOBAL.innodb_flush_log_at_trx_commit AS SIGNED)")
}

pub(in super::super) fn claim_runtime() -> Scalar<'static, Option<i32>> {
    sqlx::query_scalar("SELECT GET_LOCK(SHA2(CONCAT('mokyu:runtime:',DATABASE()),256),0)")
}

pub(in super::super) fn owns_runtime() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT COALESCE(IS_USED_LOCK(SHA2(CONCAT('mokyu:runtime:',DATABASE()),256))=CONNECTION_ID(),FALSE)",
    )
}

#[cfg(test)]
pub(in super::super) fn tables() -> Scalar<'static, String> {
    sqlx::query_scalar(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME",
    )
}

pub(in super::super) fn migrations()
-> sqlx::query::QueryAs<'static, MySql, (i64, bool, Vec<u8>), MySqlArguments> {
    sqlx::query_as("SELECT version,success,checksum FROM _sqlx_migrations ORDER BY version")
}

pub(in super::super) fn schema_version() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT CAST(schema_version AS SIGNED) FROM mokyu_meta WHERE id=1")
}

pub(in super::super) fn fingerprint() -> Scalar<'static, Option<Vec<u8>>> {
    sqlx::query_scalar("SELECT deployment_key_fingerprint FROM mokyu_meta WHERE id=1 FOR UPDATE")
}

pub(in super::super) fn set_fingerprint(value: &[u8]) -> Statement<'_> {
    sqlx::query("UPDATE mokyu_meta SET deployment_key_fingerprint=? WHERE id=1 AND deployment_key_fingerprint IS NULL").bind(value)
}

pub(in super::super) fn lock_audit_user(id: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM web_users WHERE id=? FOR UPDATE").bind(id)
}

pub(in super::super) fn security_audit<'q>(
    user: Uuid,
    action: &'q str,
    success: bool,
    request: &'q str,
) -> Statement<'q> {
    sqlx::query("INSERT INTO audit_events(actor_id,actor_label,source,action,target,outcome,request_id,finished_at,detail) SELECT id,username,'web',?,LOWER(INSERT(INSERT(INSERT(INSERT(HEX(id),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-')),?,?,CURRENT_TIMESTAMP(6),? FROM web_users WHERE id=? AND (? OR NOT EXISTS(SELECT 1 FROM audit_events WHERE actor_id=? AND action=? AND outcome='failed' AND created_at>CURRENT_TIMESTAMP(6)-INTERVAL 1 MINUTE))")
        .bind(action).bind(if success { "succeeded" } else { "failed" }).bind(request)
        .bind(serde_json::json!({"identity_verified":success})).bind(user).bind(success).bind(user).bind(action)
}

pub(in super::super) fn checkpoint_audit<'q>(
    id: Id,
    action: &'q str,
    target: &'q str,
    project: Option<Uuid>,
    bucket: Option<Uuid>,
    detail: Value,
) -> Statement<'q> {
    sqlx::query("UPDATE audit_events SET action=?,target=?,project_id=?,bucket_id=?,detail=?,outcome='succeeded',finished_at=CURRENT_TIMESTAMP(6) WHERE id=?")
        .bind(action).bind(target).bind(project).bind(bucket).bind(detail).bind(id)
}
