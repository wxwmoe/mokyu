use super::{Backend, external_queries as q, queries, transactions};
use crate::{
    authorization::Principal,
    model::{
        Error,
        external::{self, ExternalBucket, ExternalStore, Page, Record, Save},
    },
};
use anyhow::{Result, ensure};
use uuid::Uuid;

use crate::model::{external_test as test, task::Execution};

pub(super) async fn prepare_test(
    db: &mut sqlx::MySqlConnection,
    task: Execution,
    node: Option<Uuid>,
) -> Result<test::Assignment> {
    ensure!(
        q::test_owned(task)
            .fetch_optional(&mut *db)
            .await?
            .is_some(),
        Error::OperationAborted
    );
    let row = q::test(task.id).fetch_one(&mut *db).await?;
    let mut state = row.report.0;
    state.report.validate()?;
    ensure!(
        state.report.phase != test::Phase::Done,
        Error::OperationAborted
    );
    let plan = test::Plan {
        id: task.id,
        report: state.report.clone(),
        recover: state.started,
    };
    state.started = true;
    q::test_dispatched(task.id, node, &state)
        .execute(&mut *db)
        .await?;
    Ok(test::Assignment {
        plan,
        external_id: row.external_id.ok_or(Error::OperationAborted)?,
        revision: row.revision,
        connection_encrypted: row.connection_encrypted.ok_or(Error::OperationAborted)?,
    })
}

pub(super) async fn finish_test(
    db: &mut sqlx::MySqlConnection,
    task: Execution,
    plan: &test::Plan,
    report: &test::Report,
) -> Result<()> {
    ensure!(
        plan.id == task.id
            && q::test_owned(task)
                .fetch_optional(&mut *db)
                .await?
                .is_some(),
        Error::OperationAborted
    );
    let row = q::test(task.id).fetch_one(&mut *db).await?;
    ensure!(
        row.report.0.started
            && serde_json::to_value(&row.report.0.report)? == serde_json::to_value(&plan.report)?,
        Error::OperationAborted
    );
    plan.report.advance(report)?;
    let state = test::State {
        report: report.clone(),
        started: report.blocked.is_some(),
    };
    q::test_result(task.id, &state).execute(&mut *db).await?;
    if report.phase == test::Phase::Done {
        q::test_complete(task).execute(&mut *db).await?;
    }
    Ok(())
}

pub(super) async fn replace_member(
    db: &mut sqlx::MySqlConnection,
    user: Uuid,
    project: Uuid,
    grants: &[crate::model::administration::BucketGrant],
) -> Result<()> {
    external::validate_grants(grants)?;
    let mut sorted: Vec<_> = grants.iter().collect();
    sorted.sort_by_key(|grant| grant.bucket_id);
    for grant in &sorted {
        ensure!(
            q::scope(grant.bucket_id).fetch_optional(&mut *db).await? == Some(project),
            s3s::s3_error!(InvalidArgument)
        );
    }
    q::clear_member(user, project).execute(&mut *db).await?;
    for grant in sorted {
        q::member_grant(user, project, grant)
            .execute(&mut *db)
            .await?;
    }
    Ok(())
}
pub(super) async fn token_scope(
    db: &mut sqlx::MySqlConnection,
    user: Uuid,
    role: &str,
    grants: &[crate::model::administration::BucketGrant],
) -> Result<()> {
    external::validate_grants(grants)?;
    let mut sorted: Vec<_> = grants.iter().collect();
    sorted.sort_by_key(|grant| grant.bucket_id);
    for grant in sorted {
        ensure!(
            q::scope(grant.bucket_id)
                .fetch_optional(&mut *db)
                .await?
                .is_some(),
            s3s::s3_error!(AccessDenied)
        );
        if role != "admin" {
            let allowed = q::owner_actions(user, grant.bucket_id)
                .fetch_optional(&mut *db)
                .await?
                .unwrap_or(0);
            let requested = queries::action_bits(&grant.actions);
            ensure!(
                allowed & requested == requested,
                s3s::s3_error!(AccessDenied)
            );
        }
    }
    Ok(())
}
pub(super) async fn replace_token(
    db: &mut sqlx::MySqlConnection,
    token: Uuid,
    grants: &[crate::model::administration::BucketGrant],
) -> Result<()> {
    q::clear_token(token).execute(&mut *db).await?;
    for grant in grants {
        q::token_grant(token, grant).execute(&mut *db).await?;
    }
    Ok(())
}

pub(super) async fn authorized(
    db: &mut sqlx::MySqlConnection,
    permit: &crate::authorization::Permit,
) -> Result<Vec<Record>> {
    ensure!(permit.resources.len() <= 2, s3s::s3_error!(InvalidArgument));
    let admin = queries::validate_identity(&permit.principal, &mut *db, true).await?;
    let mut result = Vec::new();
    for (id, actions) in &permit.resources {
        q::scope(*id)
            .fetch_optional(&mut *db)
            .await?
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
        let record = q::page(&permit.principal, admin, None, None, Some(*id), 1)
            .fetch_optional(&mut *db)
            .await?
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?
            .record()?;
        ensure!(
            record.bucket.enabled
                && actions
                    .iter()
                    .all(|action| record.bucket.actions.contains(action)),
            s3s::s3_error!(AccessDenied)
        );
        result.push(record);
    }
    Ok(result)
}

#[async_trait::async_trait]
impl ExternalStore for Backend {
    async fn authorized(&self, permit: &crate::authorization::Permit) -> Result<Vec<Record>> {
        ensure!(permit.resources.len() <= 2, s3s::s3_error!(InvalidArgument));
        let mut db = self.pool.begin().await?;
        let result = authorized(&mut db, permit).await?;
        db.commit().await?;
        Ok(result)
    }

    async fn start_test(&self, principal: &Principal, input: test::Start) -> Result<Uuid> {
        input.report.validate()?;
        ensure!(
            input.report.phase == test::Phase::Inspect
                && input.connection_encrypted.len() <= 16384
                && !input.connection_encrypted.is_empty(),
            s3s::s3_error!(InvalidArgument)
        );
        let mut tx = transactions::admin_transaction(&self.pool, principal).await?;
        let row = q::record(input.external_id, true)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        ensure!(
            row.revision == input.revision
                && row.enabled
                && !q::referenced(input.external_id).fetch_one(&mut *tx).await?,
            Error::OperationAborted
        );
        ensure!(
            serde_json::to_value(&row.connection.0)?
                == serde_json::to_value(&input.report.connection)?,
            Error::OperationAborted
        );
        ensure!(input.node.is_none(), s3s::s3_error!(InvalidArgument));
        let detail = serde_json::json!({"operation":"external_test","policy":"external_test","external_id":input.external_id,"revision":input.revision,"project_id":row.project_id,"source_node":input.node});
        let id = transactions::enqueue_task(&mut tx, "integrity", None, detail).await?;
        q::insert_test(id, &input).execute(&mut *tx).await?;
        transactions::checkpoint_event(&mut tx, "external.test", &input.external_id.to_string(), serde_json::json!({"external_id":input.external_id,"revision":input.revision,"task_id":id,"node_id":input.node})).await?;
        tx.commit().await?;
        Ok(id)
    }
    async fn latest_test(&self, principal: &Principal, id: Uuid) -> Result<Option<test::Test>> {
        let mut tx = transactions::admin_transaction(&self.pool, principal).await?;
        ensure!(
            q::record(id, false)
                .fetch_optional(&mut *tx)
                .await?
                .is_some(),
            s3s::s3_error!(NoSuchKey)
        );
        let result = q::latest_test(id)
            .fetch_optional(&mut *tx)
            .await?
            .map(|row| row.view())
            .transpose()?;
        tx.commit().await?;
        Ok(result)
    }
    async fn test(&self, principal: &Principal, id: Uuid) -> Result<test::Test> {
        let mut tx = transactions::admin_transaction(&self.pool, principal).await?;
        let value = q::test(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?
            .view()?;
        tx.commit().await?;
        Ok(value)
    }
    async fn stop_test(&self, principal: &Principal, external: Uuid, id: Uuid) -> Result<()> {
        let mut tx = transactions::admin_transaction(&self.pool, principal).await?;
        ensure!(
            q::test_stop(id, external)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        q::test_forget_secret(id).execute(&mut *tx).await?;
        transactions::checkpoint_event(
            &mut tx,
            "external.test.stop",
            &external.to_string(),
            serde_json::json!({"external_id":external,"task_id":id}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    async fn prepare_test(&self, task: Execution) -> Result<test::Assignment> {
        let mut tx = self.pool.begin().await?;
        let assignment = prepare_test(&mut tx, task, None).await?;
        tx.commit().await?;
        Ok(assignment)
    }
    async fn finish_test(
        &self,
        task: Execution,
        plan: &test::Plan,
        report: &test::Report,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        finish_test(&mut tx, task, plan, report).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn page(
        &self,
        principal: &Principal,
        project: Option<Uuid>,
        after: Option<Uuid>,
        limit: usize,
    ) -> Result<Page> {
        ensure!((1..=200).contains(&limit), "invalid external page limit");

        let mut tx = self.pool.begin().await?;
        let admin = queries::validate_identity(principal, &mut tx, false).await?;
        let rows = q::page(principal, admin, project, after, None, limit + 1)
            .fetch_all(&mut *tx)
            .await?;
        let mut items = rows
            .into_iter()
            .map(|row| row.view())
            .collect::<Result<Vec<_>>>()?;
        let next = if items.len() > limit {
            items.pop();
            items.last().map(|item| item.id)
        } else {
            None
        };
        tx.commit().await?;
        Ok(Page { items, next })
    }
    async fn view(&self, principal: &Principal, id: Uuid) -> Result<ExternalBucket> {
        let mut tx = self.pool.begin().await?;
        let admin = queries::validate_identity(principal, &mut tx, false).await?;
        let value = q::page(principal, admin, None, None, Some(id), 1)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?
            .view()?;
        tx.commit().await?;
        Ok(value)
    }
    async fn record(&self, principal: &Principal, id: Uuid) -> Result<Record> {
        let mut tx = transactions::admin_transaction(&self.pool, principal).await?;
        let row = q::record(id, false)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        tx.commit().await?;
        row.record(external::ACTIONS.to_vec())
    }
    async fn save(&self, principal: &Principal, input: Save) -> Result<ExternalBucket> {
        input.validate()?;
        let mut tx = transactions::admin_transaction(&self.pool, principal).await?;
        ensure!(
            queries::lock_project(input.project_id)
                .fetch_optional(&mut *tx)
                .await?
                .is_some(),
            s3s::s3_error!(NoSuchKey)
        );
        ensure!(
            !q::duplicate(input.project_id, &input.name, input.id)
                .fetch_one(&mut *tx)
                .await?,
            Error::OperationAborted
        );
        let action = if input.expected.is_some() {
            ensure!(
                q::update(&input).execute(&mut *tx).await?.rows_affected() == 1,
                Error::OperationAborted
            );
            "external.update"
        } else {
            q::insert(&input).execute(&mut *tx).await?;
            "external.create"
        };
        let bucket = q::record(input.id, false)
            .fetch_one(&mut *tx)
            .await?
            .record(external::ACTIONS.to_vec())?
            .bucket;
        transactions::checkpoint_event(&mut tx, action, &input.id.to_string(), serde_json::json!({"external_id": input.id, "project_id": input.project_id, "name": input.name, "revision": bucket.revision, "enabled": input.enabled})).await?;
        tx.commit().await?;
        Ok(bucket)
    }
    async fn delete(&self, principal: &Principal, id: Uuid, revision: i64) -> Result<()> {
        ensure!(revision > 0, "invalid external revision");
        let mut tx = transactions::admin_transaction(&self.pool, principal).await?;
        let row = q::record(id, true)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        ensure!(
            row.revision == revision && !q::referenced(id).fetch_one(&mut *tx).await?,
            Error::OperationAborted
        );
        ensure!(
            q::delete(id, revision)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        transactions::checkpoint_event(&mut tx, "external.delete", &id.to_string(), serde_json::json!({"external_id": id, "project_id": row.project_id, "name": row.name, "revision": revision})).await?;
        tx.commit().await?;
        Ok(())
    }
}
