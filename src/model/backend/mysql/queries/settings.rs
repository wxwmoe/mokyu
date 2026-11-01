use super::*;

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
            "SELECT section,value,revision,is_default FROM runtime_settings ORDER BY section",
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
