use super::*;

pub(in super::super) async fn bucket_actions(
    principal: &Principal,
    connection: &mut sqlx::MySqlConnection,
    bucket: Uuid,
    admin: bool,
) -> Result<Vec<Action>> {
    if admin {
        return Ok(Action::ALL.to_vec());
    }
    let bits: Option<u64> = match principal {
        Principal::User { id, .. } => sqlx::query_scalar("SELECT actions FROM user_bucket_access WHERE user_id=? AND bucket_id=?").bind(id).bind(bucket).fetch_optional(connection).await?,
        Principal::Token { id, .. } => sqlx::query_scalar("SELECT actions FROM token_bucket_access WHERE token_id=? AND bucket_id=?").bind(id).bind(bucket).fetch_optional(connection).await?,
        Principal::Service { access_key, .. } => sqlx::query_scalar("SELECT g.actions FROM s3_credential_grants g JOIN buckets b ON b.id=g.bucket_id JOIN s3_credentials c ON c.access_key_id=g.access_key AND c.project_id=b.project_id WHERE g.access_key=? AND g.bucket_id=?").bind(access_key).bind(bucket).fetch_optional(connection).await?,
        Principal::Node { .. } => return Err(s3s::s3_error!(AccessDenied).into()),
        Principal::System | Principal::Bootstrap | Principal::Local => Some(127),
    };
    Ok(actions(bits.unwrap_or(0)))
}

pub(in super::super) fn permission_bucket(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM buckets WHERE id=? LOCK IN SHARE MODE").bind(id)
}

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
    sqlx::query("INSERT INTO web_users(id,username,password_hash,role,must_change_password) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE id=web_users.id").bind(id).bind(&input.username).bind(&input.password_hash).bind(&input.role).bind(input.must_change_password)
}

pub(in super::super) fn admin_count() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT COUNT(*) FROM web_users WHERE enabled AND role='admin'")
}

pub(in super::super) fn lock_project_mode() -> Statement<'static> {
    lock_accounts()
}

pub(in super::super) fn lock_user(id: Uuid) -> Rows<'static, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE id=? FOR UPDATE").bind(id)
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
    search: &'q str,
    after: &'q str,
    limit: i64,
    role: Option<&'q str>,
) -> Rows<'q, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE (INSTR(LOWER(username),LOWER(?))>0 OR INSTR(LOWER(display_name),LOWER(?))>0) AND username>? AND (? IS NULL OR role=?) ORDER BY username LIMIT ?").bind(search).bind(search).bind(after).bind(role).bind(role).bind(limit)
}

pub(in super::super) fn user(id: Uuid) -> Rows<'static, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE id=?").bind(id)
}

pub(in super::super) fn lock_member(user: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT role FROM web_users WHERE id=? FOR UPDATE").bind(user)
}

pub(in super::super) fn project_exists(project: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM projects WHERE id=?)").bind(project)
}

pub(in super::super) async fn lock_project_buckets(
    connection: &mut sqlx::MySqlConnection,
    buckets: &[Uuid],
    project: Uuid,
) -> Result<Vec<Uuid>> {
    if buckets.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::<MySql>::new("SELECT id FROM buckets WHERE project_id=");
    query.push_bind(project).push(" AND id IN (");
    let mut ids = query.separated(",");
    for id in buckets {
        ids.push_bind(*id);
    }
    query.push(") ORDER BY id LOCK IN SHARE MODE");
    Ok(query.build_query_scalar().fetch_all(connection).await?)
}

pub(in super::super) fn save_membership<'q>(
    user: Uuid,
    project: Uuid,
    role: &'q str,
    scope: &'q str,
) -> Statement<'q> {
    sqlx::query("INSERT INTO project_members(user_id,project_id,role,scope) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE role=?,scope=?").bind(user).bind(project).bind(role).bind(scope).bind(role).bind(scope)
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

pub(super) fn grants(row: &sqlx::mysql::MySqlRow, column: &str) -> sqlx::Result<Vec<BucketGrant>> {
    use sqlx::Row;
    let sqlx::types::Json(rows): sqlx::types::Json<Vec<(Uuid, u64)>> = row.try_get(column)?;
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
    sqlx::query_as("SELECT m.user_id,u.username,u.display_name,u.enabled,m.role,m.scope,COALESCE((SELECT JSON_ARRAYAGG(JSON_ARRAY(HEX(g.bucket_id),g.actions)) FROM member_bucket_grants g WHERE g.user_id=m.user_id AND g.project_id=m.project_id),JSON_ARRAY()) AS grants,COALESCE((SELECT JSON_ARRAYAGG(JSON_ARRAY(HEX(g.bucket_id),g.actions)) FROM member_external_grants g WHERE g.user_id=m.user_id AND g.project_id=m.project_id),JSON_ARRAY()) AS external_grants FROM project_members m JOIN web_users u ON u.id=m.user_id WHERE m.project_id=? ORDER BY u.username LIMIT 1000").bind(project)
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
    sqlx::query_scalar("SELECT id FROM projects WHERE id=? FOR UPDATE").bind(id)
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
impl crate::model::AuthorizationStore for Backend {
    async fn admin(&self, principal: &Principal) -> Result<Box<dyn crate::model::AdminGuard>> {
        transactions::hold_admin(&self.pool, principal).await
    }

    async fn service(&self, access_key: &str) -> Result<Principal> {
        let revision = service_identity(access_key, false)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
        Ok(Principal::Service {
            access_key: access_key.into(),
            revision,
        })
    }

    async fn actions(&self, principal: &Principal, bucket: Uuid) -> Result<Vec<Action>> {
        let mut connection = self.pool.acquire().await?;
        let admin = validate_identity(principal, &mut connection, false).await?;
        bucket_actions(principal, &mut connection, bucket, admin).await
    }
}

pub(in super::super) fn ensure_runtime_setting<'q>(
    section: &'q str,
    value: &'q Value,
) -> Statement<'q> {
    sqlx::query("INSERT INTO runtime_settings(section,value) VALUES(?,?) ON DUPLICATE KEY UPDATE section=runtime_settings.section").bind(section).bind(value)
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
    sqlx::query_scalar("SELECT revision FROM runtime_settings WHERE section=? FOR UPDATE")
        .bind(section)
}

pub(in super::super) fn save_runtime_settings(
    section: &str,
    value: Value,
    is_default: bool,
    revision: i64,
) -> Statement<'_> {
    sqlx::query("UPDATE runtime_settings SET value=?,revision=revision+1,updated_at=CURRENT_TIMESTAMP(6),is_default=? WHERE section=? AND revision=?").bind(value).bind(is_default).bind(section).bind(revision)
}
