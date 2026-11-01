use super::*;

const AUDIT_COLUMNS: &str = "SELECT e.id,e.created_at,e.finished_at,e.actor_id,e.actor_label,e.token_id,e.source,e.action,e.target,e.project_id,e.bucket_id,e.outcome,e.request_id,e.http_status,";

fn audit_visibility(query: &mut sqlx::QueryBuilder<Sqlite>, admin: bool, user: Option<Uuid>) {
    if !admin {
        query.push(" AND e.actor_id=").push_bind(user)
            .push(" AND ((e.bucket_id IS NULL AND (substr(e.action,1,8)='account.' OR substr(e.action,1,8)='session.' OR substr(e.action,1,6)='token.' OR (e.outcome='failed' AND e.target='' AND json_type(e.detail)='object' AND NOT EXISTS(SELECT 1 FROM json_each(e.detail))))) OR EXISTS(SELECT 1 FROM user_bucket_access a WHERE a.user_id=")
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
                .fetch_optional(&self.read)
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
            .execute(&self.write).await?;
        Ok(result.last_insert_rowid().into())
    }

    async fn finish(&self, id: Id, status: u16, partial: bool) -> Result<()> {
        let outcome = if partial {
            "partial"
        } else if status >= 400 {
            "failed"
        } else {
            "succeeded"
        };
        sqlx::query("UPDATE audit_events SET outcome=IIF(outcome='unknown',?,outcome),finished_at=COALESCE(finished_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)),http_status=? WHERE id=?")
            .bind(outcome).bind(i32::from(status)).bind(id).execute(&self.write).await?;
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
        ensure!(limit >= 0, "invalid audit limit");
        let limit = limit.checked_add(1).context("audit limit overflow")?;
        let Some(actor) = filter.actor.as_deref().filter(|actor| !actor.is_empty()) else {
            return Ok(audit_page(admin, user, before, &filter, limit, export)
                .build_query_as()
                .fetch_all(&self.read)
                .await?);
        };
        let actor = actor.to_lowercase();
        let wanted = usize::try_from(limit).context("invalid audit limit")?;
        let mut before = before;
        let mut result = Vec::new();
        // Native pages keep Unicode actor filtering bounded without SQLite extensions.
        while result.len() < wanted {
            let batch: Vec<crate::model::audit::Event> =
                audit_page(admin, user, before, &filter, 128, export)
                    .build_query_as()
                    .fetch_all(&self.read)
                    .await?;
            let more = batch.len() == 128;
            for event in batch {
                before = Some(event.id.parse()?);
                if event.actor_label.to_lowercase().contains(&actor) {
                    result.push(event);
                    if result.len() == wanted {
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
        Ok(query.build_query_as().fetch_optional(&self.read).await?)
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for crate::model::audit::Event {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get::<i64, _>("id")?.to_string(),
            created_at: date(row, "created_at")?,
            finished_at: optional_date(row, "finished_at")?,
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

fn audit_page(
    admin: bool,
    user: Option<Uuid>,
    before: Option<Id>,
    filter: &crate::model::audit::Filter,
    limit: i64,
    export: bool,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(AUDIT_COLUMNS);
    query.push("IIF(");
    query.push_bind(export).push(" OR length(CAST(e.detail AS BLOB))<=8192,e.detail,JSON_OBJECT('details_available',json('true'))) AS visible_detail FROM audit_events e WHERE TRUE");
    audit_visibility(&mut query, admin, user);
    if let Some(before) = before {
        query.push(" AND e.id<").push_bind(before);
    }
    if let Some(action) = filter.action.as_ref().filter(|s| !s.is_empty()) {
        query
            .push(" AND substr(e.action,1,length(")
            .push_bind(action.clone())
            .push("))=")
            .push_bind(action.clone());
    }
    if let Some(source) = &filter.source {
        query.push(" AND e.source=").push_bind(source.clone());
    }
    if let Some(outcome) = &filter.outcome {
        query.push(" AND e.outcome=").push_bind(outcome.clone());
    }
    if let Some(project) = filter.project {
        query.push(" AND e.project_id=").push_bind(project);
    }
    if let Some(bucket) = filter.bucket {
        query.push(" AND e.bucket_id=").push_bind(bucket);
    }
    if let Some(since) = filter.since {
        query
            .push(" AND e.created_at>=")
            .push_bind(since.timestamp_micros());
    }
    if let Some(until) = filter.until {
        query
            .push(" AND e.created_at<=")
            .push_bind(until.timestamp_micros());
    }
    query.push(" ORDER BY e.id DESC LIMIT ").push_bind(limit);
    query
}
