use super::*;

pub(in super::super) fn find_user(name: &str) -> Rows<'_, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE username=?").bind(name)
}

pub(in super::super) fn enable_project_mode() -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET project_management=TRUE WHERE id=1")
}

pub(in super::super) fn has_admin() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM web_users WHERE enabled AND role='admin')")
}

pub(in super::super) fn insert_user(id: Uuid, input: &UserCreate) -> Statement<'_> {
    sqlx::query("INSERT INTO web_users(id,username,password_hash,role,must_change_password) VALUES(?,?,?,?,?) ON CONFLICT(username) DO NOTHING").bind(id).bind(&input.username).bind(&input.password_hash).bind(&input.role).bind(input.must_change_password)
}

pub(in super::super) fn admin_count() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT COUNT(*) FROM web_users WHERE enabled AND role='admin'")
}

pub(in super::super) fn lock_user(id: Uuid) -> Rows<'static, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE id=?").bind(id)
}

pub(in super::super) fn delete_user_memberships(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM project_members WHERE user_id=?").bind(id)
}

pub(in super::super) fn update_user(id: Uuid, role: &str, enabled: bool) -> Statement<'_> {
    sqlx::query("UPDATE web_users SET role=?,enabled=?,auth_revision=auth_revision+1,authorization_revision=authorization_revision+1 WHERE id=?").bind(role).bind(enabled).bind(id)
}

pub(in super::super) fn delete_user_sessions(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM sessions WHERE user_id=?").bind(id)
}

pub(in super::super) fn delete_user(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM web_users WHERE id=?").bind(id)
}

pub(in super::super) fn reset_user_password(
    id: Uuid,
    hash: &str,
    require_change: bool,
) -> Statement<'_> {
    sqlx::query("UPDATE web_users SET password_hash=?,auth_revision=auth_revision+1,authorization_revision=authorization_revision+1,must_change_password=? WHERE id=?").bind(hash).bind(require_change).bind(id)
}

pub(in super::super) fn list_users<'q>(
    after: &'q str,
    limit: i64,
    role: Option<&'q str>,
) -> Rows<'q, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE username>? AND (? IS NULL OR role=?) ORDER BY username LIMIT ?").bind(after).bind(role).bind(role).bind(limit)
}

pub(in super::super) fn user(id: Uuid) -> Rows<'static, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE id=?").bind(id)
}

pub(in super::super) fn lock_member(user: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT role FROM web_users WHERE id=?").bind(user)
}

pub(in super::super) fn project_exists(project: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM projects WHERE id=?)").bind(project)
}

pub(in super::super) async fn lock_project_buckets(
    connection: &mut sqlx::SqliteConnection,
    buckets: &[Uuid],
    project: Uuid,
) -> Result<Vec<Uuid>> {
    if buckets.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::<Sqlite>::new("SELECT id FROM buckets WHERE project_id=");
    query.push_bind(project).push(" AND id IN (");
    let mut ids = query.separated(",");
    for id in buckets {
        ids.push_bind(*id);
    }
    query.push(") ORDER BY id");
    Ok(query.build_query_scalar().fetch_all(connection).await?)
}

pub(in super::super) fn save_membership<'q>(
    user: Uuid,
    project: Uuid,
    role: &'q str,
    scope: &'q str,
) -> Statement<'q> {
    sqlx::query("INSERT INTO project_members(user_id,project_id,role,scope) VALUES(?,?,?,?) ON CONFLICT(user_id,project_id) DO UPDATE SET role=excluded.role,scope=excluded.scope").bind(user).bind(project).bind(role).bind(scope)
}

pub(in super::super) fn delete_member_grants(user: Uuid, project: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM member_bucket_grants WHERE user_id=? AND project_id=?")
        .bind(user)
        .bind(project)
}

pub(in super::super) fn insert_member_grant(
    user: Uuid,
    project: Uuid,
    grant: &BucketGrant,
) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO member_bucket_grants(user_id,project_id,bucket_id,actions) VALUES(?,?,?,?)",
    )
    .bind(user)
    .bind(project)
    .bind(grant.bucket_id)
    .bind(action_bits(&grant.actions))
}

pub(in super::super) fn delete_membership(user: Uuid, project: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM project_members WHERE user_id=? AND project_id=?")
        .bind(user)
        .bind(project)
}

pub(in super::super) fn advance_user_authorization(user: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE web_users SET authorization_revision=authorization_revision+1 WHERE id=?")
        .bind(user)
}

pub(super) fn grants(row: &SqliteRow, column: &str) -> sqlx::Result<Vec<BucketGrant>> {
    use sqlx::Row;
    let sqlx::types::Json(rows): sqlx::types::Json<Vec<(Uuid, i64)>> = row.try_get(column)?;
    let mut grants: Vec<_> = rows
        .into_iter()
        .map(|(bucket_id, bits)| BucketGrant {
            bucket_id,
            actions: actions(bits),
        })
        .collect();
    grants.sort_by_key(|grant| grant.bucket_id);
    Ok(grants)
}

pub(in super::super) fn project_members(project: Uuid) -> Rows<'static, Member> {
    sqlx::query_as("SELECT m.user_id,u.username,u.display_name,u.enabled,m.role,m.scope,COALESCE((SELECT JSON_GROUP_ARRAY(JSON_ARRAY(lower(hex(g.bucket_id)),g.actions)) FROM member_bucket_grants g WHERE g.user_id=m.user_id AND g.project_id=m.project_id),JSON_ARRAY()) AS grants,COALESCE((SELECT JSON_GROUP_ARRAY(JSON_ARRAY(lower(hex(g.bucket_id)),g.actions)) FROM member_external_grants g WHERE g.user_id=m.user_id AND g.project_id=m.project_id),JSON_ARRAY()) AS external_grants FROM project_members m JOIN web_users u ON u.id=m.user_id WHERE m.project_id=? ORDER BY u.username LIMIT 1000").bind(project)
}

pub(in super::super) fn projects() -> Rows<'static, Project> {
    sqlx::query_as("SELECT * FROM projects ORDER BY builtin DESC,name LIMIT 1000")
}

pub(in super::super) fn duplicate_project(name: &str, id: Option<Uuid>) -> Scalar<'_, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE name=? AND (? IS NULL OR id<>?))",
    )
    .bind(name)
    .bind(id)
    .bind(id)
}

pub(in super::super) fn update_project(id: Uuid, input: &ProjectInput) -> Statement<'_> {
    sqlx::query("UPDATE projects SET name=?,description=?,allow_bucket_create=? WHERE id=?")
        .bind(input.name.trim())
        .bind(&input.description)
        .bind(input.allow_bucket_create)
        .bind(id)
}

pub(in super::super) fn insert_project(id: Uuid, input: &ProjectInput) -> Statement<'_> {
    sqlx::query("INSERT INTO projects(id,name,description,allow_bucket_create) VALUES(?,?,?,?)")
        .bind(id)
        .bind(input.name.trim())
        .bind(&input.description)
        .bind(input.allow_bucket_create)
}

pub(in super::super) fn project(id: Uuid) -> Rows<'static, Project> {
    sqlx::query_as("SELECT * FROM projects WHERE id=?").bind(id)
}

pub(in super::super) fn lock_project(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM projects WHERE id=?").bind(id)
}

pub(in super::super) fn project_dependents(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM buckets WHERE project_id=?) OR EXISTS(SELECT 1 FROM s3_credentials WHERE project_id=?) OR EXISTS(SELECT 1 FROM project_members WHERE project_id=?) OR EXISTS(SELECT 1 FROM external_buckets WHERE project_id=?)").bind(id).bind(id).bind(id).bind(id)
}

pub(in super::super) fn delete_project(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM projects WHERE id=?").bind(id)
}

pub(in super::super) fn project_mode_dependents() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE NOT builtin) OR EXISTS(SELECT 1 FROM web_users WHERE role='member') OR EXISTS(SELECT 1 FROM project_members)",
    )
}

pub(in super::super) fn set_project_mode(enabled: bool) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET project_management=? WHERE id=1").bind(enabled)
}

pub(in super::super) fn visible_projects(
    user: Uuid,
    admin: bool,
    token: Option<Uuid>,
) -> Rows<'static, Project> {
    sqlx::query_as("SELECT p.* FROM projects p WHERE ? OR CASE WHEN ? IS NOT NULL THEN EXISTS(SELECT 1 FROM token_bucket_access t JOIN buckets b ON b.id=t.bucket_id WHERE t.token_id=? AND b.project_id=p.id AND (t.actions & 1)<>0) OR EXISTS(SELECT 1 FROM token_external_access t JOIN external_buckets b ON b.id=t.bucket_id WHERE t.token_id=? AND b.project_id=p.id AND (t.actions & 1)<>0) ELSE EXISTS(SELECT 1 FROM project_members m WHERE m.project_id=p.id AND m.user_id=?) END ORDER BY p.builtin DESC,p.name LIMIT 1000").bind(admin).bind(token).bind(token).bind(token).bind(user)
}

pub(in super::super) fn project_mode() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT project_management FROM mokyu_meta WHERE id=1")
}

#[async_trait::async_trait]
impl AdministrationStore for Backend {
    async fn all_users(&self) -> Result<Vec<User>> {
        Ok(
            sqlx::query_as("SELECT * FROM web_users ORDER BY username LIMIT 1000")
                .fetch_all(&self.read)
                .await?,
        )
    }

    async fn find_user(&self, name: &str) -> Result<Option<User>> {
        Ok(find_user(name).fetch_optional(&self.read).await?)
    }
    async fn user(&self, id: Uuid) -> Result<Option<User>> {
        Ok(user(id).fetch_optional(&self.read).await?)
    }
    async fn users(
        &self,
        search: &str,
        after: &str,
        limit: i64,
        role: Option<&str>,
    ) -> Result<Vec<User>> {
        ensure!(limit >= 0, "invalid user limit");
        if search.is_empty() {
            return Ok(list_users(after, limit, role).fetch_all(&self.read).await?);
        }
        let limit = usize::try_from(limit).context("invalid user limit")?;
        let search = search.to_lowercase();
        let mut after = after.to_owned();
        let mut result = Vec::new();
        // SQLite LOWER handles ASCII only; keep Unicode search with bounded native pages.
        while result.len() < limit {
            let batch = list_users(&after, 128, role).fetch_all(&self.read).await?;
            let more = batch.len() == 128;
            for user in batch {
                after.clone_from(&user.username);
                if user.username.to_lowercase().contains(&search)
                    || user.display_name.to_lowercase().contains(&search)
                {
                    result.push(user);
                    if result.len() == limit {
                        break;
                    }
                }
            }
            if !more {
                break;
            }
        }
        Ok(result)
    }
    async fn create_user(&self, principal: &Principal, input: UserCreate) -> Result<User> {
        transactions::create_user(&self.write, principal, input).await
    }
    async fn update_user(&self, principal: &Principal, id: Uuid, input: UserPatch) -> Result<User> {
        let _authorization = self.authorization.write().await;
        transactions::update_user(&self.write, principal, id, input).await
    }
    async fn delete_user(&self, principal: &Principal, id: Uuid) -> Result<()> {
        let _authorization = self.authorization.write().await;
        transactions::delete_user(&self.write, principal, id).await
    }
    async fn reset_password(
        &self,
        principal: &Principal,
        id: Uuid,
        hash: &str,
        require_change: bool,
    ) -> Result<()> {
        let _authorization = self.authorization.write().await;
        transactions::reset_password(&self.write, principal, id, hash, require_change).await
    }
    async fn save_member(
        &self,
        principal: &Principal,
        project: Uuid,
        user: Uuid,
        input: Option<Membership>,
    ) -> Result<()> {
        let _authorization = self.authorization.write().await;
        transactions::save_member(&self.write, principal, project, user, input).await
    }
    async fn members(&self, project: Uuid) -> Result<Vec<Member>> {
        Ok(project_members(project).fetch_all(&self.read).await?)
    }
    async fn projects(&self) -> Result<Vec<Project>> {
        Ok(projects().fetch_all(&self.read).await?)
    }
    async fn visible_projects(
        &self,
        user: Uuid,
        admin: bool,
        token: Option<Uuid>,
    ) -> Result<Vec<Project>> {
        Ok(visible_projects(user, admin, token)
            .fetch_all(&self.read)
            .await?)
    }
    async fn save_project(
        &self,
        principal: &Principal,
        id: Option<Uuid>,
        input: ProjectInput,
    ) -> Result<Project> {
        transactions::save_project(&self.write, principal, id, input).await
    }
    async fn delete_project(&self, principal: &Principal, id: Uuid) -> Result<()> {
        transactions::delete_project(&self.write, principal, id).await
    }
    async fn project_mode(&self) -> Result<bool> {
        Ok(project_mode().fetch_one(&self.read).await?)
    }
    async fn set_project_mode(&self, principal: &Principal, enabled: bool) -> Result<()> {
        transactions::set_project_mode(&self.write, principal, enabled).await
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for crate::model::administration::User {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            username: row.try_get("username")?,
            display_name: row.try_get("display_name")?,
            role: row.try_get("role")?,
            enabled: row.try_get("enabled")?,
            must_change_password: row.try_get("must_change_password")?,
            created_at: date(row, "created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for crate::model::administration::Member {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
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

impl<'r> sqlx::FromRow<'r, SqliteRow> for crate::model::administration::Project {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            description: row.try_get("description")?,
            builtin: row.try_get("builtin")?,
            allow_bucket_create: row.try_get("allow_bucket_create")?,
            created_at: date(row, "created_at")?,
        })
    }
}
