use super::*;

pub(in super::super) const BEGIN_WRITE: &str = "BEGIN IMMEDIATE";

pub(in super::super) const SCHEMA: &str = include_str!("../schema.sql");

pub(in super::super) fn flush_time() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT CAST(unixepoch('subsec')*1000000 AS INTEGER)")
}

pub(in super::super) fn schema_empty() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT NOT EXISTS(SELECT 1 FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*')",
    )
}

pub(in super::super) fn initialize_meta(version: i64) -> Statement<'static> {
    sqlx::query("INSERT INTO mokyu_meta(id,schema_version) VALUES(1,?)").bind(version)
}

pub(in super::super) fn version() -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT sqlite_version()")
}

pub(in super::super) fn journal_mode() -> Scalar<'static, String> {
    sqlx::query_scalar("PRAGMA journal_mode")
}

pub(in super::super) fn synchronous() -> Scalar<'static, i64> {
    sqlx::query_scalar("PRAGMA synchronous")
}

pub(in super::super) fn full_synchronous() -> Statement<'static> {
    sqlx::query("PRAGMA synchronous=FULL")
}

pub(in super::super) fn nonces_reserved() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nonce_counters WHERE reserved_id>0)")
}

pub(in super::super) fn nonce_sync_off() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM runtime_settings WHERE section='io' AND json_extract(value,'$.sqlite_synchronous')='off')",
    )
}

#[cfg(test)]
pub(in super::super) fn tables() -> Scalar<'static, String> {
    sqlx::query_scalar(
        "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name",
    )
}

pub(in super::super) fn migrations() -> Rows<'static, (i64, bool, Vec<u8>)> {
    sqlx::query_as("SELECT version,success,checksum FROM _sqlx_migrations ORDER BY version")
}

pub(in super::super) fn schema_version() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT schema_version FROM mokyu_meta WHERE id=1")
}

pub(in super::super) fn fingerprint() -> Scalar<'static, Option<Vec<u8>>> {
    sqlx::query_scalar("SELECT deployment_key_fingerprint FROM mokyu_meta WHERE id=1")
}

pub(in super::super) fn set_fingerprint(value: &[u8]) -> Statement<'_> {
    sqlx::query("UPDATE mokyu_meta SET deployment_key_fingerprint=? WHERE id=1 AND deployment_key_fingerprint IS NULL").bind(value)
}

pub(in super::super) fn datetime(micros: i64) -> sqlx::Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(micros)
        .ok_or_else(|| sqlx::Error::Decode("invalid SQLite timestamp".into()))
}

pub(super) fn date(row: &SqliteRow, name: &str) -> sqlx::Result<DateTime<Utc>> {
    datetime(row.try_get(name)?)
}

pub(in super::super) fn security_audit<'q>(
    user: Uuid,
    action: &'q str,
    success: bool,
    request: &'q str,
) -> Statement<'q> {
    sqlx::query("INSERT INTO audit_events(actor_id,actor_label,source,action,target,outcome,request_id,finished_at,detail) SELECT id,username,'web',?,?,?,?,CAST(unixepoch('subsec')*1000000 AS INTEGER),? FROM web_users WHERE id=? AND (? OR NOT EXISTS(SELECT 1 FROM audit_events WHERE actor_id=? AND action=? AND outcome='failed' AND created_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)-60000000))")
        .bind(action).bind(user.to_string()).bind(if success { "succeeded" } else { "failed" }).bind(request)
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
    sqlx::query("UPDATE audit_events SET action=?,target=?,project_id=?,bucket_id=?,detail=?,outcome='succeeded',finished_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?")
        .bind(action).bind(target).bind(project).bind(bucket).bind(detail).bind(id)
}

pub(in super::super) struct RefreshToken {
    pub(in super::super) id: Uuid,
    pub(in super::super) session_id: Uuid,
    pub(in super::super) family_id: Uuid,
    pub(in super::super) expires_at: DateTime<Utc>,
    pub(in super::super) revoked_at: Option<DateTime<Utc>>,
    pub(in super::super) replaced_by: Option<Uuid>,
}

pub(in super::super) fn insert_refresh<'q>(
    id: Uuid,
    session: Uuid,
    family: Uuid,
    hash: &'q [u8],
) -> Statement<'q> {
    sqlx::query("INSERT INTO session_refresh_tokens(id,session_id,family_id,token_hash,expires_at) VALUES(?,?,?,?,CAST(unixepoch('subsec')*1000000 AS INTEGER)+2592000000000)")
        .bind(id).bind(session).bind(family).bind(hash)
}

pub(in super::super) fn revoke_refreshes(session: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE session_refresh_tokens SET revoked_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE session_id=? AND revoked_at IS NULL").bind(session)
}

#[async_trait::async_trait]
impl RuntimeStore for Backend {
    fn database_info(&self) -> Option<&crate::model::runtime::DatabaseInfo> {
        Some(&self.database_info)
    }
    async fn initialize(&self, maintenance: bool) -> Result<RuntimeState> {
        if maintenance {
            sqlx::query("UPDATE mokyu_meta SET maintenance=TRUE WHERE id=1")
                .execute(&self.write)
                .await?;
        }
        self.state().await
    }
    async fn state(&self) -> Result<RuntimeState> {
        let (gc_paused, maintenance, pack_creation_paused) = sqlx::query_as(
            "SELECT gc_paused,maintenance,pack_creation_paused FROM mokyu_meta WHERE id=1",
        )
        .fetch_one(&self.read)
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
                .fetch_one(&self.read)
                .await?,
        )
    }
    async fn health(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.read).await?;
        Ok(())
    }
    fn pool(&self) -> (u32, usize) {
        let writer = (self.write.size(), self.write.num_idle());
        if self.shared_pool {
            writer
        } else {
            (writer.0 + self.read.size(), writer.1 + self.read.num_idle())
        }
    }
}
