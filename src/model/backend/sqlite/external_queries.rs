use super::queries;
use crate::{
    authorization::{Action, Principal},
    model::external::{ExternalBucket, Record, Save},
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{Sqlite, types::Json};
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
    sqlx::query_as("SELECT e.id,e.external_id,e.revision,e.connection_encrypted,e.node_id,e.attempts,e.report,e.cleanup_required,e.created_at,e.updated_at,t.state,coalesce(json_extract(t.detail,'$.test_stopped'),0)<>0 AS stopped FROM external_tests e JOIN tasks t ON t.id=e.id WHERE e.id=?1").bind(id)
}
pub(super) fn latest_test(id: Uuid) -> Rows<'static, TestRow> {
    sqlx::query_as("SELECT e.id,e.external_id,e.revision,e.connection_encrypted,e.node_id,e.attempts,e.report,e.cleanup_required,e.created_at,e.updated_at,t.state,coalesce(json_extract(t.detail,'$.test_stopped'),0)<>0 AS stopped FROM external_tests e JOIN tasks t ON t.id=e.id WHERE e.external_id=?1 ORDER BY e.created_at DESC,e.id DESC LIMIT 1").bind(id)
}
pub(super) fn insert_test(id: Uuid, input: &crate::model::external_test::Start) -> Statement<'_> {
    sqlx::query("INSERT INTO external_tests(id,external_id,revision,connection_encrypted,report) VALUES(?1,?2,?3,?4,?5)")
        .bind(id).bind(input.external_id).bind(input.revision).bind(&input.connection_encrypted)
        .bind(Json(crate::model::external_test::State { report: input.report.clone(), started: false }))
}
pub(super) fn test_owned(task: crate::model::task::Execution) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE id=?1 AND execution_token=?2 AND state='running' AND task_type='integrity' AND json_extract(detail,'$.operation')='external_test'").bind(task.id).bind(task.token)
}
pub(super) fn test_dispatched(
    id: Uuid,
    node: Option<Uuid>,
    state: &crate::model::external_test::State,
) -> Statement<'_> {
    sqlx::query("UPDATE external_tests SET node_id=?1,attempts=attempts+1,report=?2,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?3")
        .bind(node).bind(Json(state)).bind(id)
}
pub(super) fn test_result(id: Uuid, state: &crate::model::external_test::State) -> Statement<'_> {
    let done = state.report.phase == crate::model::external_test::Phase::Done;
    sqlx::query("UPDATE external_tests SET report=?1,cleanup_required=?2,connection_encrypted=CASE WHEN ?3 THEN NULL ELSE connection_encrypted END,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?4")
        .bind(Json(state)).bind(!done).bind(done).bind(id)
}
pub(super) fn test_complete(task: crate::model::task::Execution) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='completed',updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?1 AND execution_token=?2 AND state='running'").bind(task.id).bind(task.token)
}

type Rows<'q, T> = sqlx::query::QueryAs<'q, Sqlite, T, sqlx::sqlite::SqliteArguments>;
type Statement<'q> = sqlx::query::Query<'q, Sqlite, sqlx::sqlite::SqliteArguments>;
type Scalar<'q, T> = sqlx::query::QueryScalar<'q, Sqlite, T, sqlx::sqlite::SqliteArguments>;

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
    actions: i64,
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
    sqlx::query_as("WITH request AS (SELECT ?1 AS admin,?2 AS actor,?3 AS token,?4 AS project,?5 AS after_id,?6 AS id) SELECT b.id,b.project_id,b.name,b.connection,b.revision,b.access_encrypted,b.secret_encrypted,b.enabled,b.created_at AS created_at,b.updated_at AS updated_at,CASE WHEN r.admin THEN 31 ELSE COALESCE(CASE WHEN r.token IS NULL THEN ua.actions ELSE ta.actions END,0) END AS actions FROM external_buckets b CROSS JOIN request r LEFT JOIN user_external_access ua ON ua.bucket_id=b.id AND ua.user_id=r.actor LEFT JOIN token_external_access ta ON ta.bucket_id=b.id AND ta.token_id=r.token WHERE (r.project IS NULL OR b.project_id=r.project) AND (r.after_id IS NULL OR b.id>r.after_id) AND (r.id IS NULL OR b.id=r.id) AND (r.admin OR COALESCE(CASE WHEN r.token IS NULL THEN ua.actions ELSE ta.actions END,0)>0) ORDER BY b.id LIMIT ?7")
        .bind(admin).bind(principal.user_id()).bind(principal.token_id()).bind(project).bind(after).bind(id).bind(limit as i64)
}
pub(super) fn record(id: Uuid) -> Rows<'static, Row> {
    sqlx::query_as("SELECT b.id,b.project_id,b.name,b.connection,b.revision,b.access_encrypted,b.secret_encrypted,b.enabled,b.created_at,b.updated_at FROM external_buckets b WHERE b.id=?1").bind(id)
}
pub(super) fn duplicate(project: Uuid, name: &str, id: Uuid) -> Scalar<'_, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM external_buckets WHERE project_id=?1 AND name=?2 AND id<>?3)",
    )
    .bind(project)
    .bind(name)
    .bind(id)
}
pub(super) fn insert(input: &Save) -> Statement<'_> {
    sqlx::query("INSERT INTO external_buckets(id,project_id,name,connection,access_encrypted,secret_encrypted,enabled) VALUES(?1,?2,?3,?4,?5,?6,?7)")
        .bind(input.id).bind(input.project_id).bind(&input.name).bind(Json(&input.connection)).bind(&input.access_encrypted).bind(&input.secret_encrypted).bind(input.enabled)
}
pub(super) fn update(input: &Save) -> Statement<'_> {
    sqlx::query("UPDATE external_buckets SET name=?1,connection=?2,access_encrypted=?3,secret_encrypted=?4,enabled=?5,revision=revision+1,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?6 AND project_id=?7 AND revision=?8")
        .bind(&input.name).bind(Json(&input.connection)).bind(&input.access_encrypted).bind(&input.secret_encrypted).bind(input.enabled).bind(input.id).bind(input.project_id).bind(input.expected)
}
pub(super) fn referenced(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM transfer_connections WHERE external_id=?1) OR EXISTS(SELECT 1 FROM external_tests e JOIN tasks t ON t.id=e.id WHERE e.external_id=?1 AND (e.cleanup_required OR t.state<>'completed') AND coalesce(json_extract(t.detail,'$.test_stopped'),0)=0)").bind(id)
}
pub(super) fn delete(id: Uuid, revision: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM external_buckets WHERE id=?1 AND revision=?2")
        .bind(id)
        .bind(revision)
}

pub(super) fn scope(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT project_id FROM external_buckets WHERE id=?1").bind(id)
}
pub(super) fn owner_actions(user: Uuid, id: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT actions FROM user_external_access WHERE user_id=?1 AND bucket_id=?2")
        .bind(user)
        .bind(id)
}
pub(super) fn clear_member(user: Uuid, project: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM member_external_grants WHERE user_id=?1 AND project_id=?2")
        .bind(user)
        .bind(project)
}
pub(super) fn member_grant(
    user: Uuid,
    project: Uuid,
    grant: &crate::model::administration::BucketGrant,
) -> Statement<'static> {
    sqlx::query("INSERT INTO member_external_grants(user_id,project_id,bucket_id,actions) VALUES(?1,?2,?3,?4)").bind(user).bind(project).bind(grant.bucket_id).bind(queries::action_bits(&grant.actions))
}
pub(super) fn clear_token(token: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM token_external_grants WHERE token_id=?1").bind(token)
}
pub(super) fn token_grant(
    token: Uuid,
    grant: &crate::model::administration::BucketGrant,
) -> Statement<'static> {
    sqlx::query("INSERT INTO token_external_grants(token_id,bucket_id,actions) VALUES(?1,?2,?3)")
        .bind(token)
        .bind(grant.bucket_id)
        .bind(queries::action_bits(&grant.actions))
}

pub(super) fn test_stop(id: Uuid, external: Uuid) -> Statement<'static> {
    sqlx::query(r#"UPDATE tasks SET state='failed',error='external test recovery was stopped',execution_token=NULL,detail=json_set(detail,'$.test_stopped',json('true')),updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?1 AND state IN ('queued','paused','failed') AND json_extract(detail,'$.operation')='external_test' AND EXISTS(SELECT 1 FROM external_tests e WHERE e.id=tasks.id AND e.external_id=?2)"#).bind(id).bind(external)
}

pub(super) fn test_forget_secret(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE external_tests SET connection_encrypted=NULL,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?1").bind(id)
}
