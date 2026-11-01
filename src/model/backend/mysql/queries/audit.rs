use super::*;

const AUDIT_COLUMNS: &str = "SELECT e.id,e.created_at,e.finished_at,e.actor_id,e.actor_label,e.token_id,e.source,e.action,e.target,e.project_id,e.bucket_id,e.outcome,e.request_id,e.http_status,";

fn audit_visibility(query: &mut sqlx::QueryBuilder<MySql>, admin: bool, user: Option<Uuid>) {
    if !admin {
        query.push(" AND e.actor_id=").push_bind(user)
            .push(" AND ((e.bucket_id IS NULL AND (LEFT(e.action,8)='account.' OR LEFT(e.action,8)='session.' OR LEFT(e.action,6)='token.' OR (e.outcome='failed' AND OCTET_LENGTH(e.target)=0 AND JSON_TYPE(e.detail)=CAST('OBJECT' AS BINARY) AND JSON_LENGTH(e.detail)=0))) OR EXISTS(SELECT 1 FROM user_bucket_access a WHERE a.user_id=")
            .push_bind(user).push(" AND a.bucket_id=e.bucket_id AND (a.actions & 1)<>0))");
    }
}

#[async_trait::async_trait]
impl crate::model::AuditStore for Backend {
    async fn begin(
        &self,
        principal: &Principal,
        action: &str,
        request_id: Option<&str>,
    ) -> Result<Id> {
        let actor = principal.user_id();
        let label = if let Some(id) = actor {
            sqlx::query_scalar("SELECT username FROM web_users WHERE id=?")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?
                .unwrap_or_else(|| "Deleted account".to_string())
        } else {
            "Local CLI".to_string()
        };
        let source = if principal.token_id().is_some() {
            "token"
        } else if matches!(
            principal,
            Principal::System | Principal::Bootstrap | Principal::Local | Principal::Node { .. }
        ) {
            "cli"
        } else {
            "web"
        };
        let result = sqlx::query("INSERT INTO audit_events(actor_id,actor_label,token_id,source,action,request_id) VALUES(?,?,?,?,?,?)")
            .bind(actor).bind(label).bind(principal.token_id()).bind(source).bind(action).bind(request_id)
            .execute(&self.pool).await?;
        Ok(Id::try_from(result.last_insert_id())?)
    }

    async fn finish(&self, id: Id, status: u16, partial: bool) -> Result<()> {
        let outcome = if partial {
            "partial"
        } else if status >= 400 {
            "failed"
        } else {
            "succeeded"
        };
        sqlx::query("UPDATE audit_events SET outcome=IF(outcome='unknown',?,outcome),finished_at=COALESCE(finished_at,CURRENT_TIMESTAMP(6)),http_status=? WHERE id=?")
            .bind(outcome).bind(i32::from(status)).bind(id).execute(&self.pool).await?;
        Ok(())
    }

    async fn events(
        &self,
        admin: bool,
        user: Option<Uuid>,
        before: Option<Id>,
        filter: crate::model::audit::Filter,
        limit: i64,
        export: bool,
    ) -> Result<Vec<crate::model::audit::Event>> {
        let mut query = sqlx::QueryBuilder::new(AUDIT_COLUMNS);
        query.push("IF(");
        query.push_bind(export).push(" OR OCTET_LENGTH(CAST(e.detail AS CHAR))<=8192,e.detail,JSON_OBJECT('details_available',TRUE)) AS visible_detail FROM audit_events e WHERE TRUE");
        audit_visibility(&mut query, admin, user);
        if let Some(before) = before {
            query.push(" AND e.id<").push_bind(before);
        }
        if let Some(actor) = filter.actor.filter(|s| !s.is_empty()) {
            query
                .push(" AND INSTR(LOWER(e.actor_label),LOWER(")
                .push_bind(actor)
                .push("))>0");
        }
        if let Some(action) = filter.action.filter(|s| !s.is_empty()) {
            query
                .push(" AND LEFT(e.action,CHAR_LENGTH(")
                .push_bind(action.clone())
                .push("))=CAST(")
                .push_bind(action)
                .push(" AS BINARY)");
        }
        if let Some(source) = filter.source {
            query.push(" AND e.source=").push_bind(source);
        }
        if let Some(outcome) = filter.outcome {
            query.push(" AND e.outcome=").push_bind(outcome);
        }
        if let Some(project) = filter.project {
            query.push(" AND e.project_id=").push_bind(project);
        }
        if let Some(bucket) = filter.bucket {
            query.push(" AND e.bucket_id=").push_bind(bucket);
        }
        if let Some(since) = filter.since {
            query.push(" AND e.created_at>=").push_bind(since);
        }
        if let Some(until) = filter.until {
            query.push(" AND e.created_at<=").push_bind(until);
        }
        query
            .push(" ORDER BY e.id DESC LIMIT ")
            .push_bind(limit + 1);
        Ok(query.build_query_as().fetch_all(&self.pool).await?)
    }

    async fn event(
        &self,
        id: Id,
        admin: bool,
        user: Uuid,
    ) -> Result<Option<crate::model::audit::Event>> {
        let mut query = sqlx::QueryBuilder::new(AUDIT_COLUMNS);
        query.push("e.detail AS visible_detail FROM audit_events e WHERE e.id=");
        query.push_bind(id);
        audit_visibility(&mut query, admin, Some(user));
        Ok(query.build_query_as().fetch_optional(&self.pool).await?)
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for crate::model::audit::Event {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get::<i64, _>("id")?.to_string(),
            created_at: row.try_get("created_at")?,
            finished_at: row.try_get("finished_at")?,
            actor_id: row.try_get("actor_id")?,
            actor_label: row.try_get("actor_label")?,
            token_id: row.try_get("token_id")?,
            source: row.try_get("source")?,
            action: row.try_get("action")?,
            target: row.try_get("target")?,
            project_id: row.try_get("project_id")?,
            bucket_id: row.try_get("bucket_id")?,
            outcome: row.try_get("outcome")?,
            request_id: row.try_get("request_id")?,
            status: row.try_get("http_status")?,
            detail: row.try_get("visible_detail")?,
        })
    }
}

pub(in super::super) fn credential_record(access: &str) -> Rows<'_, Credential> {
    sqlx::query_as("SELECT c.*,COALESCE((SELECT JSON_ARRAYAGG(JSON_ARRAY(HEX(g.bucket_id),g.actions)) FROM s3_credential_grants g WHERE g.access_key=c.access_key_id),JSON_ARRAY()) AS grants FROM s3_credentials c WHERE c.access_key_id=?").bind(access)
}

pub(in super::super) fn credential_records(
    project: Option<Uuid>,
    after: &str,
    limit: i64,
) -> Rows<'_, Credential> {
    sqlx::query_as("SELECT c.*,COALESCE((SELECT JSON_ARRAYAGG(JSON_ARRAY(HEX(g.bucket_id),g.actions)) FROM s3_credential_grants g WHERE g.access_key=c.access_key_id),JSON_ARRAY()) AS grants FROM s3_credentials c WHERE (? IS NULL OR c.project_id=?) AND c.access_key_id>? ORDER BY c.access_key_id LIMIT ?").bind(project).bind(project).bind(after).bind(limit)
}

pub(in super::super) fn delete_credential_grants(access: &str) -> Statement<'_> {
    sqlx::query("DELETE FROM s3_credential_grants WHERE access_key=?").bind(access)
}

pub(in super::super) fn insert_credential_grant<'q>(
    access: &'q str,
    grant: &BucketGrant,
) -> Statement<'q> {
    sqlx::query("INSERT INTO s3_credential_grants(access_key,bucket_id,actions) VALUES(?,?,?)")
        .bind(access)
        .bind(grant.bucket_id)
        .bind(action_bits(&grant.actions))
}

pub(in super::super) fn advance_credential_authorization(access: &str) -> Statement<'_> {
    sqlx::query("UPDATE s3_credentials SET authorization_revision=authorization_revision+1 WHERE access_key_id=?").bind(access)
}

pub(in super::super) fn credential_project(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM projects WHERE id=? LOCK IN SHARE MODE").bind(id)
}

pub(in super::super) fn insert_credential<'q>(
    material: &'q CredentialMaterial,
    input: &'q CredentialInput,
    expires: Option<DateTime<Utc>>,
    user: Option<Uuid>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO s3_credentials(access_key_id,secret_encrypted,project_id,label,expires_at,created_by) VALUES(?,?,?,?,?,?)").bind(&material.access).bind(&material.encrypted).bind(input.project_id).bind(input.label.trim()).bind(expires).bind(user)
}
