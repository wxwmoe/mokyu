use super::*;

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
            sqlx::query_scalar("SELECT username FROM web_users WHERE id=$1")
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
        Ok(sqlx::query_scalar("INSERT INTO audit_events(actor_id,actor_label,token_id,source,action,request_id) VALUES($1,$2,$3,$4,$5,$6) RETURNING id")
            .bind(actor).bind(label).bind(principal.token_id()).bind(source).bind(action).bind(request_id).fetch_one(&self.pool).await?)
    }

    async fn finish(&self, id: Id, status: u16, partial: bool) -> Result<()> {
        let outcome = if partial {
            "partial"
        } else if status >= 400 {
            "failed"
        } else {
            "succeeded"
        };
        sqlx::query("UPDATE audit_events SET outcome=CASE WHEN outcome='unknown' THEN $2 ELSE outcome END,finished_at=COALESCE(finished_at,now()),http_status=$3 WHERE id=$1")
            .bind(id).bind(outcome).bind(i32::from(status)).execute(&self.pool).await?;
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
        Ok(sqlx::query_as("SELECT e.id::text,e.created_at,e.finished_at,e.actor_id,e.actor_label,e.token_id,e.source,e.action,e.target,e.project_id,e.bucket_id,e.outcome,e.request_id,e.http_status,CASE WHEN $13 OR octet_length(e.detail::text)<=8192 THEN e.detail ELSE '{\"details_available\":true}'::jsonb END AS detail FROM audit_events e WHERE ($1 OR (actor_id=$2 AND ((e.bucket_id IS NULL AND (starts_with(e.action,'account.') OR starts_with(e.action,'session.') OR starts_with(e.action,'token.') OR (e.outcome='failed' AND e.target='' AND e.detail='{}'::jsonb))) OR EXISTS(SELECT 1 FROM user_bucket_access a WHERE a.user_id=$2 AND a.bucket_id=e.bucket_id AND 'bucket.list'=ANY(a.actions))))) AND ($3::uuid IS NULL OR e.id<$3) AND position(lower($4) in lower(e.actor_label))>0 AND starts_with(e.action,$5) AND ($6::text IS NULL OR e.source=$6) AND ($7::text IS NULL OR e.outcome=$7) AND ($8::uuid IS NULL OR e.project_id=$8) AND ($9::uuid IS NULL OR e.bucket_id=$9) AND ($10::timestamptz IS NULL OR e.created_at>=$10) AND ($11::timestamptz IS NULL OR e.created_at<=$11) ORDER BY e.id DESC LIMIT $12")
        .bind(admin).bind(user).bind(before).bind(filter.actor.unwrap_or_default()).bind(filter.action.unwrap_or_default()).bind(filter.source).bind(filter.outcome).bind(filter.project).bind(filter.bucket).bind(filter.since).bind(filter.until).bind(limit+1).bind(export).fetch_all(&self.pool).await?)
    }

    async fn event(
        &self,
        id: Id,
        admin: bool,
        user: Uuid,
    ) -> Result<Option<crate::model::audit::Event>> {
        Ok(sqlx::query_as("SELECT e.id::text,e.created_at,e.finished_at,e.actor_id,e.actor_label,e.token_id,e.source,e.action,e.target,e.project_id,e.bucket_id,e.outcome,e.request_id,e.http_status,e.detail FROM audit_events e WHERE e.id=$1 AND ($2 OR (e.actor_id=$3 AND ((e.bucket_id IS NULL AND (starts_with(e.action,'account.') OR starts_with(e.action,'session.') OR starts_with(e.action,'token.') OR (e.outcome='failed' AND e.target='' AND e.detail='{}'::jsonb))) OR EXISTS(SELECT 1 FROM user_bucket_access a WHERE a.user_id=$3 AND a.bucket_id=e.bucket_id AND 'bucket.list'=ANY(a.actions)))))")
        .bind(id).bind(admin).bind(user).fetch_optional(&self.pool).await?)
    }
}

pub(in super::super) fn checkpoint_audit<'q>(
    id: Id,
    action: &'q str,
    target: &'q str,
    project: Option<Uuid>,
    bucket: Option<Uuid>,
    detail: Value,
) -> Statement<'q> {
    sqlx::query("UPDATE audit_events SET action=$2,target=$3,project_id=$4,bucket_id=$5,detail=$6,outcome='succeeded',finished_at=now() WHERE id=$1")
        .bind(id).bind(action).bind(target).bind(project).bind(bucket).bind(detail)
}

pub(in super::super) fn partial_audit(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE audit_events SET outcome='partial' WHERE id=$1").bind(id)
}

pub(in super::super) fn lock_audit_user(id: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM web_users WHERE id=$1 FOR UPDATE").bind(id)
}

pub(in super::super) fn security_audit<'q>(
    user: Uuid,
    action: &'q str,
    success: bool,
    request: &'q str,
) -> Statement<'q> {
    sqlx::query("INSERT INTO audit_events(actor_id,actor_label,source,action,target,outcome,request_id,finished_at,detail) SELECT id,username,'web',$2,id::text,$3,$4,now(),jsonb_build_object('identity_verified',$5::boolean) FROM web_users WHERE id=$1 AND ($5 OR NOT EXISTS(SELECT 1 FROM audit_events WHERE actor_id=$1 AND action=$2 AND outcome='failed' AND created_at>now()-interval '1 minute'))")
        .bind(user).bind(action).bind(if success {"succeeded"} else {"failed"}).bind(request).bind(success)
}
