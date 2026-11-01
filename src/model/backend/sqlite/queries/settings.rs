use super::*;

pub(in super::super) fn ensure_runtime_setting<'q>(
    section: &'q str,
    value: &'q Value,
) -> Statement<'q> {
    sqlx::query(
        "INSERT INTO runtime_settings(section,value) VALUES(?,?) ON CONFLICT(section) DO NOTHING",
    )
    .bind(section)
    .bind(value)
}

pub(in super::super) fn runtime_settings() -> Rows<'static, (String, Value, i64)> {
    sqlx::query_as("SELECT section,value,revision FROM runtime_settings ORDER BY section")
}

pub(in super::super) fn runtime_setting(section: &str) -> Scalar<'_, Value> {
    sqlx::query_scalar("SELECT value FROM runtime_settings WHERE section=?").bind(section)
}

pub(in super::super) fn runtime_settings_row(section: &str) -> Rows<'_, RuntimeSettingsRow> {
    sqlx::query_as("SELECT value,revision FROM runtime_settings WHERE section=?").bind(section)
}

pub(in super::super) fn lock_runtime_settings(section: &str) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT revision FROM runtime_settings WHERE section=?").bind(section)
}

pub(in super::super) fn save_runtime_settings(
    section: &str,
    value: Value,
    is_default: bool,
    revision: i64,
) -> Statement<'_> {
    sqlx::query("UPDATE runtime_settings SET value=?,revision=revision+1,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),is_default=? WHERE section=? AND revision=?").bind(value).bind(is_default).bind(section).bind(revision)
}

pub(in super::super) fn reset_runtime_settings(
    section: &str,
    value: Value,
    is_default: bool,
) -> Statement<'_> {
    sqlx::query("UPDATE runtime_settings SET value=?,revision=revision+1,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),is_default=? WHERE section=?").bind(value).bind(is_default).bind(section)
}

pub(in super::super) fn lock_web_settings() -> Rows<'static, RuntimeSettingsRow> {
    sqlx::query_as("SELECT value,revision FROM runtime_settings WHERE section='web'")
}

pub(in super::super) fn save_web_settings(
    value: Value,
    next: i64,
    user: Option<Uuid>,
    revision: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE runtime_settings SET value=?,revision=?,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),is_default=? WHERE section='web' AND revision=?").bind(value).bind(next).bind(user.is_none()).bind(revision)
}

#[async_trait::async_trait]
impl RuntimeSettingsStore for Backend {
    async fn revisions(&self) -> Result<Vec<(String, i64)>> {
        let revisions: Vec<(String, i64)> =
            sqlx::query_as("SELECT section,revision FROM runtime_settings")
                .fetch_all(&self.read)
                .await?;
        Ok(revisions)
    }
    async fn inspect(&self) -> Result<Vec<(String, Value, i64, bool)>> {
        let runtime: Vec<(String, serde_json::Value, i64, bool)> = sqlx::query_as(
            "SELECT section,value,revision,is_default FROM runtime_settings ORDER BY section",
        )
        .fetch_all(&self.read)
        .await?;
        Ok(runtime)
    }

    async fn row(&self, section: &str) -> Result<RuntimeSettingsRow> {
        Ok(runtime_settings_row(section).fetch_one(&self.read).await?)
    }
    async fn sections(&self, principal: &Principal) -> Result<Vec<(String, Value, i64)>> {
        transactions::runtime_settings_sections(&self.read, principal).await
    }
    async fn section(&self, principal: &Principal, section: &str) -> Result<RuntimeSettingsRow> {
        transactions::runtime_settings_section(&self.read, principal, section).await
    }
    async fn save(
        &self,
        principal: &Principal,
        section: &str,
        revision: Option<i64>,
        value: Value,
    ) -> Result<RuntimeSettingsRow> {
        transactions::save_runtime_settings(&self.write, principal, section, revision, value).await
    }
    async fn reset(
        &self,
        principal: &Principal,
        section: &str,
        value: Value,
    ) -> Result<RuntimeSettingsRow> {
        transactions::reset_runtime_settings(&self.write, principal, section, value).await
    }
    async fn update_web(
        &self,
        principal: &Principal,
        input: ControlSettingsPatch,
    ) -> Result<(crate::config::WebSettings, i64)> {
        transactions::update_web_settings(&self.write, principal, input).await
    }

    async fn ensure(&self, defaults: &[(&str, Value)]) -> Result<()> {
        transactions::ensure_runtime_settings(&self.write, defaults).await
    }

    async fn load(&self) -> Result<Vec<(String, Value, i64)>> {
        Ok(runtime_settings().fetch_all(&self.read).await?)
    }

    async fn value(&self, section: &str) -> Result<Value> {
        Ok(runtime_setting(section).fetch_one(&self.read).await?)
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for RuntimeSettingsRow {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            value: row.try_get("value")?,
            revision: row.try_get("revision")?,
        })
    }
}
