use super::queries;
use crate::{
    authorization::{Action, Principal},
    model::external::{ExternalBucket, Record, Save},
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{MySql, types::Json};
use uuid::Uuid;

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
    pub created_at: i64,
    pub updated_at: i64,
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
            created_at: DateTime::<Utc>::from_timestamp_micros(self.created_at)
                .context("invalid external test creation time")?,
            updated_at: DateTime::<Utc>::from_timestamp_micros(self.updated_at)
                .context("invalid external test update time")?,
        })
    }
}
pub(super) fn test(id: Uuid) -> Rows<'static, TestRow> {
    sqlx::query_as("SELECT e.id,e.external_id,e.revision,e.connection_encrypted,e.node_id,e.attempts,e.report,e.cleanup_required,CAST(UNIX_TIMESTAMP(e.created_at)*1000000 AS SIGNED) AS created_at,CAST(UNIX_TIMESTAMP(e.updated_at)*1000000 AS SIGNED) AS updated_at,t.state,COALESCE(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.test_stopped')),'false')='true' AS stopped FROM external_tests e JOIN tasks t ON t.id=e.id WHERE e.id=? FOR UPDATE").bind(id)
}
pub(super) fn latest_test(id: Uuid) -> Rows<'static, TestRow> {
    sqlx::query_as("SELECT e.id,e.external_id,e.revision,e.connection_encrypted,e.node_id,e.attempts,e.report,e.cleanup_required,CAST(UNIX_TIMESTAMP(e.created_at)*1000000 AS SIGNED) AS created_at,CAST(UNIX_TIMESTAMP(e.updated_at)*1000000 AS SIGNED) AS updated_at,t.state,COALESCE(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.test_stopped')),'false')='true' AS stopped FROM external_tests e JOIN tasks t ON t.id=e.id WHERE e.external_id=? ORDER BY e.created_at DESC,e.id DESC LIMIT 1").bind(id)
}
pub(super) fn insert_test(id: Uuid, input: &crate::model::external_test::Start) -> Statement<'_> {
    sqlx::query("INSERT INTO external_tests(id,external_id,revision,connection_encrypted,report) VALUES(?,?,?,?,?)")
        .bind(id).bind(input.external_id).bind(input.revision).bind(&input.connection_encrypted)
        .bind(Json(crate::model::external_test::State { report: input.report.clone(), started: false }))
}
pub(super) fn test_owned(task: crate::model::task::Execution) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE id=? AND execution_token=? AND state='running' AND task_type='integrity' AND JSON_UNQUOTE(JSON_EXTRACT(detail,'$.operation'))='external_test' FOR UPDATE").bind(task.id).bind(task.token)
}
pub(super) fn test_dispatched(
    id: Uuid,
    node: Option<Uuid>,
    state: &crate::model::external_test::State,
) -> Statement<'_> {
    sqlx::query("UPDATE external_tests SET node_id=?,attempts=attempts+1,report=?,updated_at=CURRENT_TIMESTAMP(6) WHERE id=?")
        .bind(node).bind(Json(state)).bind(id)
}
pub(super) fn test_result(id: Uuid, state: &crate::model::external_test::State) -> Statement<'_> {
    let done = state.report.phase == crate::model::external_test::Phase::Done;
    sqlx::query("UPDATE external_tests SET report=?,cleanup_required=?,connection_encrypted=CASE WHEN ? THEN NULL ELSE connection_encrypted END,updated_at=CURRENT_TIMESTAMP(6) WHERE id=?")
        .bind(Json(state)).bind(!done).bind(done).bind(id)
}
pub(super) fn test_complete(task: crate::model::task::Execution) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='completed',updated_at=CURRENT_TIMESTAMP(6) WHERE id=? AND execution_token=? AND state='running'").bind(task.id).bind(task.token)
}

type Rows<'q, T> = sqlx::query::QueryAs<'q, MySql, T, sqlx::mysql::MySqlArguments>;
type Statement<'q> = sqlx::query::Query<'q, MySql, sqlx::mysql::MySqlArguments>;
type Scalar<'q, T> = sqlx::query::QueryScalar<'q, MySql, T, sqlx::mysql::MySqlArguments>;

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
    actions: u64,
}
impl Visible {
    pub(super) fn view(self) -> Result<ExternalBucket> {
        Ok(self.record()?.bucket)
    }
    pub(super) fn record(self) -> Result<Record> {
        let row = self;
        let actions = queries::actions(row.actions);
        row.row.record(actions)
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
    sqlx::query_as("WITH request AS (SELECT ? AS admin,? AS actor,? AS token,? AS project,? AS after_id,? AS id) SELECT b.id,b.project_id,b.name,b.connection,b.revision,b.access_encrypted,b.secret_encrypted,b.enabled,CAST(UNIX_TIMESTAMP(b.created_at)*1000000 AS SIGNED) AS created_at,CAST(UNIX_TIMESTAMP(b.updated_at)*1000000 AS SIGNED) AS updated_at,CAST(CASE WHEN r.admin THEN 31 ELSE COALESCE(CASE WHEN r.token IS NULL THEN ua.actions ELSE ta.actions END,0) END AS UNSIGNED) AS actions FROM external_buckets b CROSS JOIN request r LEFT JOIN user_external_access ua ON ua.bucket_id=b.id AND ua.user_id=r.actor LEFT JOIN token_external_access ta ON ta.bucket_id=b.id AND ta.token_id=r.token WHERE (r.project IS NULL OR b.project_id=r.project) AND (r.after_id IS NULL OR b.id>r.after_id) AND (r.id IS NULL OR b.id=r.id) AND (r.admin OR COALESCE(CASE WHEN r.token IS NULL THEN ua.actions ELSE ta.actions END,0)>0) ORDER BY b.id LIMIT ?")
        .bind(admin).bind(principal.user_id()).bind(principal.token_id()).bind(project).bind(after).bind(id).bind(limit as i64)
}
pub(super) fn record(id: Uuid, lock: bool) -> Rows<'static, Row> {
    sqlx::query_as(if lock { "SELECT b.id,b.project_id,b.name,b.connection,b.revision,b.access_encrypted,b.secret_encrypted,b.enabled,CAST(UNIX_TIMESTAMP(b.created_at)*1000000 AS SIGNED) AS created_at,CAST(UNIX_TIMESTAMP(b.updated_at)*1000000 AS SIGNED) AS updated_at FROM external_buckets b WHERE b.id=? FOR UPDATE" } else { "SELECT b.id,b.project_id,b.name,b.connection,b.revision,b.access_encrypted,b.secret_encrypted,b.enabled,CAST(UNIX_TIMESTAMP(b.created_at)*1000000 AS SIGNED) AS created_at,CAST(UNIX_TIMESTAMP(b.updated_at)*1000000 AS SIGNED) AS updated_at FROM external_buckets b WHERE b.id=?" }).bind(id)
}
pub(super) fn duplicate(project: Uuid, name: &str, id: Uuid) -> Scalar<'_, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM external_buckets WHERE project_id=? AND name=? AND id<>?)",
    )
    .bind(project)
    .bind(name)
    .bind(id)
}
pub(super) fn insert(input: &Save) -> Statement<'_> {
    sqlx::query("INSERT INTO external_buckets(id,project_id,name,connection,access_encrypted,secret_encrypted,enabled) VALUES(?,?,?,?,?,?,?)")
        .bind(input.id).bind(input.project_id).bind(&input.name).bind(Json(&input.connection)).bind(&input.access_encrypted).bind(&input.secret_encrypted).bind(input.enabled)
}
pub(super) fn update(input: &Save) -> Statement<'_> {
    sqlx::query("UPDATE external_buckets SET name=?,connection=?,access_encrypted=?,secret_encrypted=?,enabled=?,revision=revision+1,updated_at=CURRENT_TIMESTAMP(6) WHERE id=? AND project_id=? AND revision=?")
        .bind(&input.name).bind(Json(&input.connection)).bind(&input.access_encrypted).bind(&input.secret_encrypted).bind(input.enabled).bind(input.id).bind(input.project_id).bind(input.expected)
}
pub(super) fn referenced(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM transfer_connections WHERE external_id=?) OR EXISTS(SELECT 1 FROM external_tests e JOIN tasks t ON t.id=e.id WHERE e.external_id=? AND (e.cleanup_required OR t.state<>'completed') AND COALESCE(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.test_stopped')),'false')<>'true')").bind(id).bind(id)
}
pub(super) fn delete(id: Uuid, revision: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM external_buckets WHERE id=? AND revision=?")
        .bind(id)
        .bind(revision)
}

pub(super) fn scope(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT project_id FROM external_buckets WHERE id=? LOCK IN SHARE MODE")
        .bind(id)
}
pub(super) fn owner_actions(user: Uuid, id: Uuid) -> Scalar<'static, u64> {
    sqlx::query_scalar("SELECT actions FROM user_external_access WHERE user_id=? AND bucket_id=?")
        .bind(user)
        .bind(id)
}
pub(super) fn clear_member(user: Uuid, project: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM member_external_grants WHERE user_id=? AND project_id=?")
        .bind(user)
        .bind(project)
}
pub(super) fn member_grant(
    user: Uuid,
    project: Uuid,
    grant: &crate::model::administration::BucketGrant,
) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO member_external_grants(user_id,project_id,bucket_id,actions) VALUES(?,?,?,?)",
    )
    .bind(user)
    .bind(project)
    .bind(grant.bucket_id)
    .bind(queries::action_bits(&grant.actions))
}
pub(super) fn clear_token(token: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM token_external_grants WHERE token_id=?").bind(token)
}
pub(super) fn token_grant(
    token: Uuid,
    grant: &crate::model::administration::BucketGrant,
) -> Statement<'static> {
    sqlx::query("INSERT INTO token_external_grants(token_id,bucket_id,actions) VALUES(?,?,?)")
        .bind(token)
        .bind(grant.bucket_id)
        .bind(queries::action_bits(&grant.actions))
}

pub(super) fn test_stop(id: Uuid, external: Uuid) -> Statement<'static> {
    sqlx::query(r#"UPDATE tasks t SET t.state='failed',t.error='external test recovery was stopped',t.execution_token=NULL,t.detail=JSON_SET(t.detail,'$.test_stopped',JSON_EXTRACT('true','$')),t.updated_at=CURRENT_TIMESTAMP(6) WHERE t.id=? AND t.state IN ('queued','paused','failed') AND JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation'))='external_test' AND EXISTS(SELECT 1 FROM external_tests e WHERE e.id=t.id AND e.external_id=?)"#).bind(id).bind(external)
}

pub(super) fn test_forget_secret(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE external_tests SET connection_encrypted=NULL,updated_at=CURRENT_TIMESTAMP(6) WHERE id=?").bind(id)
}
