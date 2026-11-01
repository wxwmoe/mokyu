use super::*;

pub(in super::super) fn find_user<'q>(name: &'q str) -> Rows<'q, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE username=$1").bind(name)
}

pub(in super::super) fn enable_project_mode() -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET project_management=true")
}

pub(in super::super) fn has_admin() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM web_users WHERE enabled AND role='admin')")
}

pub(in super::super) fn insert_user<'q>(input: &'q UserCreate) -> Rows<'q, User> {
    sqlx::query_as("INSERT INTO web_users(username,password_hash,role,must_change_password) VALUES($1,$2,$3,$4) ON CONFLICT(username) DO NOTHING RETURNING *").bind(&input.username).bind(&input.password_hash).bind(&input.role).bind(input.must_change_password)
}

pub(in super::super) fn admin_count() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM web_users WHERE enabled AND role='admin'")
}

pub(in super::super) fn lock_project_mode() -> Statement<'static> {
    sqlx::query("SELECT singleton FROM mokyu_meta FOR UPDATE")
}

pub(in super::super) fn lock_user(id: Uuid) -> Rows<'static, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE id=$1 FOR UPDATE").bind(id)
}

pub(in super::super) fn delete_user_memberships(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM project_members WHERE user_id=$1").bind(id)
}

pub(in super::super) fn update_user<'q>(id: Uuid, role: &'q str, enabled: bool) -> Rows<'q, User> {
    sqlx::query_as("UPDATE web_users SET role=$2,enabled=$3,auth_revision=auth_revision+1 WHERE id=$1 RETURNING *").bind(id).bind(role).bind(enabled)
}

pub(in super::super) fn delete_user_sessions(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM sessions WHERE user_id=$1").bind(id)
}

pub(in super::super) fn delete_user(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM web_users WHERE id=$1").bind(id)
}

pub(in super::super) fn reset_user_password<'q>(
    id: Uuid,
    hash: &'q str,
    require_change: bool,
) -> Statement<'q> {
    sqlx::query("UPDATE web_users SET password_hash=$2,auth_revision=auth_revision+1,must_change_password=$3 WHERE id=$1").bind(id).bind(hash).bind(require_change)
}

pub(in super::super) fn list_users<'q>(
    search: &'q str,
    after: &'q str,
    limit: i64,
    role: Option<&'q str>,
) -> Rows<'q, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE (position(lower($1) in lower(username))>0 OR position(lower($1) in lower(display_name))>0) AND username COLLATE \"C\">$2 AND ($4::text IS NULL OR role=$4) ORDER BY username COLLATE \"C\" LIMIT $3").bind(search).bind(after).bind(limit).bind(role)
}

pub(in super::super) fn user(id: Uuid) -> Rows<'static, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE id=$1").bind(id)
}

pub(in super::super) fn lock_member(user: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT role FROM web_users WHERE id=$1 FOR UPDATE").bind(user)
}

pub(in super::super) fn project_exists(project: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM projects WHERE id=$1)").bind(project)
}

pub(in super::super) fn lock_project_buckets<'q>(
    buckets: &'q [Uuid],
    project: Uuid,
) -> Scalar<'q, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM buckets WHERE id=ANY($1) AND project_id=$2 ORDER BY id FOR SHARE",
    )
    .bind(buckets)
    .bind(project)
}

pub(in super::super) fn save_membership<'q>(
    user: Uuid,
    project: Uuid,
    role: &'q str,
    scope: &'q str,
) -> Statement<'q> {
    sqlx::query("INSERT INTO project_members(user_id,project_id,role,scope) VALUES($1,$2,$3,$4) ON CONFLICT(user_id,project_id) DO UPDATE SET role=excluded.role,scope=excluded.scope").bind(user).bind(project).bind(role).bind(scope)
}

pub(in super::super) fn delete_member_grants(user: Uuid, project: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM member_bucket_grants WHERE user_id=$1 AND project_id=$2")
        .bind(user)
        .bind(project)
}

pub(in super::super) fn insert_member_grant<'q>(
    user: Uuid,
    project: Uuid,
    grant: &'q BucketGrant,
) -> Statement<'q> {
    sqlx::query("INSERT INTO member_bucket_grants(user_id,project_id,bucket_id,actions) VALUES($1,$2,$3,$4)").bind(user).bind(project).bind(grant.bucket_id).bind(grant.actions.iter().map(|a| a.name()).collect::<Vec<_>>())
}

pub(in super::super) fn delete_membership(user: Uuid, project: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM project_members WHERE user_id=$1 AND project_id=$2")
        .bind(user)
        .bind(project)
}

pub(in super::super) fn project_members(id: Uuid) -> Rows<'static, Member> {
    sqlx::query_as("SELECT m.user_id,u.username,u.display_name,u.enabled,m.role,m.scope,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM member_bucket_grants g WHERE g.user_id=m.user_id AND g.project_id=m.project_id),'[]'::jsonb) AS grants,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM member_external_grants g WHERE g.user_id=m.user_id AND g.project_id=m.project_id),'[]'::jsonb) AS external_grants FROM project_members m JOIN web_users u ON u.id=m.user_id WHERE m.project_id=$1 ORDER BY u.username LIMIT 1000").bind(id)
}

pub(in super::super) fn projects() -> Rows<'static, Project> {
    sqlx::query_as("SELECT * FROM projects ORDER BY builtin DESC,name LIMIT 1000")
}

pub(in super::super) fn duplicate_project<'q>(name: &'q str, id: Option<Uuid>) -> Scalar<'q, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE name=$1 AND ($2::uuid IS NULL OR id<>$2))",
    )
    .bind(name)
    .bind(id)
}

pub(in super::super) fn update_project<'q>(id: Uuid, input: &'q ProjectInput) -> Rows<'q, Project> {
    sqlx::query_as(
        "UPDATE projects SET name=$2,description=$3,allow_bucket_create=$4 WHERE id=$1 RETURNING *",
    )
    .bind(id)
    .bind(input.name.trim())
    .bind(&input.description)
    .bind(input.allow_bucket_create)
}

pub(in super::super) fn insert_project<'q>(input: &'q ProjectInput) -> Rows<'q, Project> {
    sqlx::query_as(
        "INSERT INTO projects(name,description,allow_bucket_create) VALUES($1,$2,$3) RETURNING *",
    )
    .bind(input.name.trim())
    .bind(&input.description)
    .bind(input.allow_bucket_create)
}

pub(in super::super) fn lock_project(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM projects WHERE id=$1 FOR UPDATE").bind(id)
}

pub(in super::super) fn project_dependents(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM buckets WHERE project_id=$1) OR EXISTS(SELECT 1 FROM s3_credentials WHERE project_id=$1) OR EXISTS(SELECT 1 FROM project_members WHERE project_id=$1) OR EXISTS(SELECT 1 FROM external_buckets WHERE project_id=$1)").bind(id)
}

pub(in super::super) fn delete_project(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM projects WHERE id=$1").bind(id)
}

pub(in super::super) fn project_mode_dependents() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE NOT builtin) OR EXISTS(SELECT 1 FROM web_users WHERE role='member') OR EXISTS(SELECT 1 FROM project_members)",
    )
}

pub(in super::super) fn set_project_mode(enabled: bool) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET project_management=$1").bind(enabled)
}

pub(in super::super) fn visible_projects(
    user: Uuid,
    admin: bool,
    token: Option<Uuid>,
) -> Rows<'static, Project> {
    sqlx::query_as("SELECT p.* FROM projects p WHERE $2 OR CASE WHEN $3::uuid IS NOT NULL THEN EXISTS(SELECT 1 FROM token_bucket_access t JOIN buckets b ON b.id=t.bucket_id WHERE t.token_id=$3 AND b.project_id=p.id AND 'bucket.list'=ANY(t.actions)) OR EXISTS(SELECT 1 FROM token_external_access t JOIN external_buckets b ON b.id=t.bucket_id WHERE t.token_id=$3 AND b.project_id=p.id AND 'bucket.list'=ANY(t.actions)) ELSE EXISTS(SELECT 1 FROM project_members m WHERE m.project_id=p.id AND m.user_id=$1) END ORDER BY p.builtin DESC,p.name LIMIT 1000").bind(user).bind(admin).bind(token)
}

pub(in super::super) fn project_mode() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT project_management FROM mokyu_meta")
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
