use super::*;

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for RuntimeSettingsRow {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            value: row.try_get("value")?,
            revision: row.try_get("revision")?,
        })
    }
}

#[async_trait::async_trait]
impl AdministrationStore for Backend {
    async fn all_users(&self) -> Result<Vec<User>> {
        Ok(
            sqlx::query_as("SELECT * FROM web_users ORDER BY username LIMIT 1000")
                .fetch_all(&self.pool)
                .await?,
        )
    }

    async fn find_user(&self, name: &str) -> Result<Option<User>> {
        Ok(find_user(name).fetch_optional(&self.pool).await?)
    }
    async fn user(&self, id: Uuid) -> Result<Option<User>> {
        Ok(user(id).fetch_optional(&self.pool).await?)
    }
    async fn users(
        &self,
        search: &str,
        after: &str,
        limit: i64,
        role: Option<&str>,
    ) -> Result<Vec<User>> {
        Ok(list_users(search, after, limit, role)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn create_user(&self, principal: &Principal, input: UserCreate) -> Result<User> {
        transactions::create_user(&self.pool, principal, input).await
    }
    async fn update_user(&self, principal: &Principal, id: Uuid, input: UserPatch) -> Result<User> {
        transactions::update_user(&self.pool, principal, id, input).await
    }
    async fn delete_user(&self, principal: &Principal, id: Uuid) -> Result<()> {
        transactions::delete_user(&self.pool, principal, id).await
    }
    async fn reset_password(
        &self,
        principal: &Principal,
        id: Uuid,
        hash: &str,
        require_change: bool,
    ) -> Result<()> {
        transactions::reset_password(&self.pool, principal, id, hash, require_change).await
    }
    async fn save_member(
        &self,
        principal: &Principal,
        project: Uuid,
        user: Uuid,
        input: Option<Membership>,
    ) -> Result<()> {
        transactions::save_member(&self.pool, principal, project, user, input).await
    }
    async fn members(&self, project: Uuid) -> Result<Vec<Member>> {
        Ok(project_members(project).fetch_all(&self.pool).await?)
    }
    async fn projects(&self) -> Result<Vec<Project>> {
        Ok(projects().fetch_all(&self.pool).await?)
    }
    async fn visible_projects(
        &self,
        user: Uuid,
        admin: bool,
        token: Option<Uuid>,
    ) -> Result<Vec<Project>> {
        Ok(visible_projects(user, admin, token)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn save_project(
        &self,
        principal: &Principal,
        id: Option<Uuid>,
        input: ProjectInput,
    ) -> Result<Project> {
        transactions::save_project(&self.pool, principal, id, input).await
    }
    async fn delete_project(&self, principal: &Principal, id: Uuid) -> Result<()> {
        transactions::delete_project(&self.pool, principal, id).await
    }
    async fn project_mode(&self) -> Result<bool> {
        Ok(project_mode().fetch_one(&self.pool).await?)
    }
    async fn set_project_mode(&self, principal: &Principal, enabled: bool) -> Result<()> {
        transactions::set_project_mode(&self.pool, principal, enabled).await
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for crate::model::administration::User {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            username: row.try_get("username")?,
            display_name: row.try_get("display_name")?,
            role: row.try_get("role")?,
            enabled: row.try_get("enabled")?,
            must_change_password: row.try_get("must_change_password")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for crate::model::administration::Member {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            user_id: row.try_get("user_id")?,
            username: row.try_get("username")?,
            display_name: row.try_get("display_name")?,
            enabled: row.try_get("enabled")?,
            role: row.try_get("role")?,
            scope: row.try_get("scope")?,
            grants: grants(row, "grants")?,
            external_grants: grants(row, "external_grants")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for crate::model::administration::Project {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            description: row.try_get("description")?,
            builtin: row.try_get("builtin")?,
            allow_bucket_create: row.try_get("allow_bucket_create")?,
            created_at: row.try_get("created_at")?,
        })
    }
}
