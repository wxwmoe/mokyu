use super::{
    queries::{self, mirroring as q, replica::Tx},
    transactions,
};
use crate::{
    content::codec::Key,
    model::{Error, mirroring as mirror, task::Execution},
};
use anyhow::{Context, Result, ensure};

pub(super) async fn range(
    tx: &mut Tx<'_>,
    bucket: uuid::Uuid,
    object: &str,
    key: &Key,
) -> Result<Option<mirror::Ranged>> {
    let Some((id, token)) = q::reader(bucket, object).fetch_optional(&mut **tx).await? else {
        return Ok(None);
    };
    let task = Execution { id, token };
    let assignment = assignment(tx, task, key).await?;
    let row = queries::task_record(id).fetch_one(&mut **tx).await?;
    let Some(value) = row.detail.get("origin").filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let observed: mirror::Observed = serde_json::from_value(value.clone())?;
    observed.validate()?;
    Ok(Some(mirror::Ranged {
        task: id,
        token,
        assignment,
        observed,
    }))
}

pub(super) async fn range_result(
    tx: &mut Tx<'_>,
    task: Execution,
    bytes: u64,
    conflict: bool,
) -> Result<()> {
    ensure!(bytes <= 5 * 1024_u64.pow(4), Error::NodeOperationConflict);
    q::range_result(task, bytes, conflict)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(super) async fn progress(
    tx: &mut Tx<'_>,
    task: Execution,
    bytes: u64,
    publishing: bool,
) -> Result<()> {
    transactions::mirroring::guard(tx, task).await?;
    let mut row = queries::task_record(task.id).fetch_one(&mut **tx).await?;
    ensure!(
        bytes <= 5 * 1024_u64.pow(4)
            && row.detail["source_size"]
                .as_u64()
                .is_none_or(|size| bytes <= size),
        Error::NodeOperationConflict
    );
    row.detail["source_bytes"] = bytes
        .max(row.detail["source_bytes"].as_u64().unwrap_or(0))
        .into();
    row.detail["phase"] = if publishing { "publishing" } else { "reading" }.into();
    queries::work_sweep::task_progress(task, "", 0, row.detail)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(super) async fn assignment(
    tx: &mut Tx<'_>,
    task: Execution,
    key: &Key,
) -> Result<mirror::Assignment> {
    transactions::mirroring::guard(tx, task).await?;
    let row = queries::task_record(task.id).fetch_one(&mut **tx).await?;
    let bucket = row.bucket_id.context("origin bucket missing")?;
    let (revision, encrypted) = q::record(bucket).fetch_one(&mut **tx).await?;
    let view = mirror::Record {
        revision,
        encrypted,
    }
    .open(bucket, key)?;
    let object = row.detail["key"]
        .as_str()
        .context("origin key missing")?
        .to_owned();
    let rule = view
        .config
        .select(&object)
        .ok_or(Error::OperationAborted)?
        .clone();
    ensure!(
        row.detail["revision"].as_i64() == Some(revision)
            && row.detail["rule_id"].as_str() == Some(rule.id.to_string().as_str()),
        Error::OperationAborted
    );
    let mut connections = Vec::new();
    for source in std::iter::once(&rule.source).chain(rule.backup.iter()) {
        if let mirror::Source::S3 { external_id, .. } = source {
            let record = queries::external::page(
                &crate::authorization::Principal::System,
                true,
                None,
                None,
                Some(*external_id),
                1,
            )
            .fetch_one(&mut **tx)
            .await?
            .record()?;
            connections.push(crate::model::external::Snapshot::from_record(record, key)?);
        }
    }
    Ok(mirror::Assignment {
        bucket,
        key: object,
        config: view.config,
        rule,
        connections,
    })
}

pub(super) async fn finish(
    tx: &mut Tx<'_>,
    task: Execution,
    size: Option<u64>,
    missing: bool,
    key: &Key,
) -> Result<&'static str> {
    let mut row = queries::task_record(task.id).fetch_one(&mut **tx).await?;
    ensure!(row.execution_token == Some(task.token), Error::NodeExpired);
    if let Some(size) = size {
        ensure!(
            q::published(task).fetch_optional(&mut **tx).await? == Some(i64::try_from(size)?),
            Error::NodeOperationConflict
        );
        row.detail["published"] = true.into();
        row.detail["source_bytes"] = size.into();
        row.detail["phase"] = "saved".into();
        queries::work_sweep::task_progress(task, "", 1, row.detail)
            .execute(&mut **tx)
            .await?;
        return Ok("completed");
    }
    if transactions::mirroring::guard(tx, task).await.is_ok() {
        let ttl = if missing {
            assignment(tx, task, key).await?.config.negative_ttl_seconds
        } else {
            0
        };
        q::negative(
            task.id,
            if ttl > 0 {
                chrono::Utc::now().timestamp() + i64::from(ttl)
            } else {
                0
            },
            if missing {
                0
            } else {
                chrono::Utc::now().timestamp() + 30
            },
        )
        .execute(&mut **tx)
        .await?;
    }
    row.detail["origin_missing"] = missing.into();
    row.detail["phase"] = "failed".into();
    queries::work_sweep::task_progress(task, "", 0, row.detail)
        .execute(&mut **tx)
        .await?;
    Ok("failed")
}
