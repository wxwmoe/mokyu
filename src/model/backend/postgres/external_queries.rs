use crate::{
    authorization::{Action, Principal},
    model::external::{self, ExternalBucket, Record, Save},
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{Postgres, types::Json};
use uuid::Uuid;

pub(super) fn test_node(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM nodes WHERE id=$1 AND enabled AND role IN ('worker','node'))",
    )
    .bind(id)
}

#[derive(sqlx::FromRow)]
pub(super) struct TestRow {
    pub id: Uuid,
    pub external_id: Option<Uuid>,
    pub revision: i64,
    pub connection_encrypted: Option<Vec<u8>>,
    pub node_id: Option<Uuid>,
    pub attempts: i32,
    pub report: Json<crate::model::external_test::State>,
    pub cleanup_required: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: String,
    pub stopped: bool,
}
impl TestRow {
    pub(super) fn view(self) -> Result<crate::model::external_test::Test> {
        Ok(crate::model::external_test::Test {
            id: self.id,
            external_id: self.external_id,
            revision: self.revision,
            node_id: self.node_id,
            attempts: self.attempts,
            state: self.state,
            stopped: self.stopped,
            report: self.report.0.report,
            cleanup_required: self.cleanup_required,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}
pub(super) fn test(id: Uuid) -> Rows<'static, TestRow> {
    sqlx::query_as("SELECT e.id,e.external_id,e.revision,e.connection_encrypted,e.node_id,e.attempts,e.report,e.cleanup_required,e.created_at,e.updated_at,t.state,coalesce(t.detail->>'test_stopped','false')='true' AS stopped FROM external_tests e JOIN tasks t ON t.id=e.id WHERE e.id=$1 FOR UPDATE OF e").bind(id)
}
pub(super) fn latest_test(id: Uuid) -> Rows<'static, TestRow> {
    sqlx::query_as("SELECT e.id,e.external_id,e.revision,e.connection_encrypted,e.node_id,e.attempts,e.report,e.cleanup_required,e.created_at,e.updated_at,t.state,coalesce(t.detail->>'test_stopped','false')='true' AS stopped FROM external_tests e JOIN tasks t ON t.id=e.id WHERE e.external_id=$1 ORDER BY e.created_at DESC,e.id DESC LIMIT 1").bind(id)
}
pub(super) fn insert_test(id: Uuid, input: &crate::model::external_test::Start) -> Statement<'_> {
    sqlx::query("INSERT INTO external_tests(id,external_id,revision,connection_encrypted,report) VALUES($1,$2,$3,$4,$5)")
        .bind(id).bind(input.external_id).bind(input.revision).bind(&input.connection_encrypted)
        .bind(Json(crate::model::external_test::State { report: input.report.clone(), started: false }))
}
pub(super) fn test_owned(task: crate::model::task::Execution) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE id=$1 AND lease_token=$2 AND state='running' AND task_type='integrity' AND detail->>'operation'='external_test' AND (lease_node_id IS NULL OR lease_expires_at>clock_timestamp()) FOR UPDATE").bind(task.id).bind(task.token)
}
pub(super) fn test_dispatched(
    id: Uuid,
    node: Option<Uuid>,
    state: &crate::model::external_test::State,
) -> Statement<'_> {
    sqlx::query("UPDATE external_tests SET node_id=$1,attempts=attempts+1,report=$2,updated_at=clock_timestamp() WHERE id=$3")
        .bind(node).bind(Json(state)).bind(id)
}
pub(super) fn test_result(id: Uuid, state: &crate::model::external_test::State) -> Statement<'_> {
    let done = state.report.phase == crate::model::external_test::Phase::Done;
    sqlx::query("UPDATE external_tests SET report=$1,cleanup_required=$2,connection_encrypted=CASE WHEN $3 THEN NULL ELSE connection_encrypted END,updated_at=clock_timestamp() WHERE id=$4")
        .bind(Json(state)).bind(!done).bind(done).bind(id)
}
pub(super) fn test_complete(task: crate::model::task::Execution) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='completed',updated_at=clock_timestamp() WHERE id=$1 AND lease_token=$2 AND state='running'").bind(task.id).bind(task.token)
}

type Rows<'q, T> = sqlx::query::QueryAs<'q, Postgres, T, sqlx::postgres::PgArguments>;
type Statement<'q> = sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>;
type Scalar<'q, T> = sqlx::query::QueryScalar<'q, Postgres, T, sqlx::postgres::PgArguments>;

#[derive(sqlx::FromRow)]
pub(super) struct Row {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub connection: Json<crate::storage::external::Connection>,
    pub revision: i64,
    pub access_encrypted: Vec<u8>,
    pub secret_encrypted: Vec<u8>,
    pub enabled: bool,
    pub created_at: i64,
    pub updated_at: i64,
}
impl Row {
    pub(super) fn record(self, actions: Vec<Action>) -> Result<Record> {
        Ok(Record {
            bucket: ExternalBucket {
                id: self.id,
                project_id: self.project_id,
                name: self.name,
                connection: self.connection.0,
                revision: self.revision,
                enabled: self.enabled,
                actions,
                created_at: DateTime::<Utc>::from_timestamp_micros(self.created_at)
                    .context("invalid external creation time")?,
                updated_at: DateTime::<Utc>::from_timestamp_micros(self.updated_at)
                    .context("invalid external update time")?,
            },
            access_encrypted: self.access_encrypted,
            secret_encrypted: self.secret_encrypted,
        })
    }
}
#[derive(sqlx::FromRow)]
pub(super) struct Visible {
    #[sqlx(flatten)]
    row: Row,
    actions: Vec<String>,
}
impl Visible {
    pub(super) fn view(self) -> Result<ExternalBucket> {
        let row = self;
        let actions = external::ACTIONS
            .into_iter()
            .filter(|a| row.actions.iter().any(|v| v == a.name()))
            .collect();
        Ok(row.row.record(actions)?.bucket)
    }
}
pub(super) fn page(
    principal: &Principal,
    admin: bool,
    project: Option<Uuid>,
    after: Option<Uuid>,
    id: Option<Uuid>,
    limit: usize,
) -> Rows<'static, Visible> {
    sqlx::query_as("WITH request AS (SELECT $1::boolean AS admin,$2::uuid AS actor,$3::uuid AS token,$4::uuid AS project,$5::uuid AS after_id,$6::uuid AS id) SELECT b.id,b.project_id,b.name,b.connection,b.revision,b.access_encrypted,b.secret_encrypted,b.enabled,(extract(epoch FROM b.created_at)*1000000)::bigint AS created_at,(extract(epoch FROM b.updated_at)*1000000)::bigint AS updated_at,CASE WHEN r.admin THEN ARRAY['bucket.list','object.read','object.write','object.delete','object.acl']::text[] ELSE COALESCE(CASE WHEN r.token IS NULL THEN ua.actions ELSE ta.actions END,ARRAY[]::text[]) END AS actions FROM external_buckets b CROSS JOIN request r LEFT JOIN user_external_access ua ON ua.bucket_id=b.id AND ua.user_id=r.actor LEFT JOIN token_external_access ta ON ta.bucket_id=b.id AND ta.token_id=r.token WHERE (r.project IS NULL OR b.project_id=r.project) AND (r.after_id IS NULL OR b.id>r.after_id) AND (r.id IS NULL OR b.id=r.id) AND (r.admin OR cardinality(COALESCE(CASE WHEN r.token IS NULL THEN ua.actions ELSE ta.actions END,ARRAY[]::text[]))>0) ORDER BY b.id LIMIT $7")
        .bind(admin).bind(principal.user_id()).bind(principal.token_id()).bind(project).bind(after).bind(id).bind(limit as i64)
}
pub(super) fn record(id: Uuid, lock: bool) -> Rows<'static, Row> {
    sqlx::query_as(if lock { "SELECT b.id,b.project_id,b.name,b.connection,b.revision,b.access_encrypted,b.secret_encrypted,b.enabled,(extract(epoch FROM b.created_at)*1000000)::bigint AS created_at,(extract(epoch FROM b.updated_at)*1000000)::bigint AS updated_at FROM external_buckets b WHERE b.id=$1 FOR UPDATE" } else { "SELECT b.id,b.project_id,b.name,b.connection,b.revision,b.access_encrypted,b.secret_encrypted,b.enabled,(extract(epoch FROM b.created_at)*1000000)::bigint AS created_at,(extract(epoch FROM b.updated_at)*1000000)::bigint AS updated_at FROM external_buckets b WHERE b.id=$1" }).bind(id)
}
pub(super) fn duplicate(project: Uuid, name: &str, id: Uuid) -> Scalar<'_, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM external_buckets WHERE project_id=$1 AND name=$2 AND id<>$3)",
    )
    .bind(project)
    .bind(name)
    .bind(id)
}
pub(super) fn insert(input: &Save) -> Statement<'_> {
    sqlx::query("INSERT INTO external_buckets(id,project_id,name,connection,access_encrypted,secret_encrypted,enabled) VALUES($1,$2,$3,$4,$5,$6,$7)")
        .bind(input.id).bind(input.project_id).bind(&input.name).bind(Json(&input.connection)).bind(&input.access_encrypted).bind(&input.secret_encrypted).bind(input.enabled)
}
pub(super) fn update(input: &Save) -> Statement<'_> {
    sqlx::query("UPDATE external_buckets SET name=$1,connection=$2,access_encrypted=$3,secret_encrypted=$4,enabled=$5,revision=revision+1,updated_at=clock_timestamp() WHERE id=$6 AND project_id=$7 AND revision=$8")
        .bind(&input.name).bind(Json(&input.connection)).bind(&input.access_encrypted).bind(&input.secret_encrypted).bind(input.enabled).bind(input.id).bind(input.project_id).bind(input.expected)
}
pub(super) fn referenced(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM external_tests e JOIN tasks t ON t.id=e.id WHERE e.external_id=$1 AND (e.cleanup_required OR t.state<>'completed') AND coalesce(t.detail->>'test_stopped','false')<>'true')").bind(id)
}
pub(super) fn delete(id: Uuid, revision: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM external_buckets WHERE id=$1 AND revision=$2")
        .bind(id)
        .bind(revision)
}

pub(super) fn scope(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT project_id FROM external_buckets WHERE id=$1 FOR SHARE").bind(id)
}
pub(super) fn owner_actions(user: Uuid, id: Uuid) -> Scalar<'static, Vec<String>> {
    sqlx::query_scalar("SELECT actions FROM user_external_access WHERE user_id=$1 AND bucket_id=$2")
        .bind(user)
        .bind(id)
}
pub(super) fn clear_member(user: Uuid, project: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM member_external_grants WHERE user_id=$1 AND project_id=$2")
        .bind(user)
        .bind(project)
}
pub(super) fn member_grant(
    user: Uuid,
    project: Uuid,
    grant: &crate::model::administration::BucketGrant,
) -> Statement<'static> {
    sqlx::query("INSERT INTO member_external_grants(user_id,project_id,bucket_id,actions) VALUES($1,$2,$3,$4)").bind(user).bind(project).bind(grant.bucket_id).bind(grant.actions.iter().map(|a| a.name()).collect::<Vec<_>>())
}
pub(super) fn clear_token(token: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM token_external_grants WHERE token_id=$1").bind(token)
}
pub(super) fn token_grant(
    token: Uuid,
    grant: &crate::model::administration::BucketGrant,
) -> Statement<'static> {
    sqlx::query("INSERT INTO token_external_grants(token_id,bucket_id,actions) VALUES($1,$2,$3)")
        .bind(token)
        .bind(grant.bucket_id)
        .bind(grant.actions.iter().map(|a| a.name()).collect::<Vec<_>>())
}

pub(super) fn test_stop(id: Uuid, external: Uuid) -> Statement<'static> {
    sqlx::query(r#"UPDATE tasks t SET state='failed',error='external test recovery was stopped',lease_token=NULL,lease_node_id=NULL,lease_expires_at=NULL,detail=detail||'{"test_stopped":true}'::jsonb,updated_at=clock_timestamp() WHERE t.id=$1 AND t.state IN ('queued','paused','failed') AND t.detail->>'operation'='external_test' AND EXISTS(SELECT 1 FROM external_tests e WHERE e.id=t.id AND e.external_id=$2) AND NOT EXISTS(SELECT 1 FROM node_work w WHERE w.task_id=t.id AND w.result_hash IS NULL AND w.expires_at>clock_timestamp())"#).bind(id).bind(external)
}

pub(super) fn test_forget_secret(id: Uuid) -> Statement<'static> {
    sqlx::query("WITH cleared AS (UPDATE node_work SET plan=NULL,storage_encrypted=NULL WHERE task_id=$1) UPDATE external_tests SET connection_encrypted=NULL,updated_at=clock_timestamp() WHERE id=$1").bind(id)
}
