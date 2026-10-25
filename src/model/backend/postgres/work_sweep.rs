use super::*;
use crate::model::node_work::ListedObject;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
struct Page {
    cursor: String,
    scanned: usize,
    items: Vec<ListedObject>,
}

async fn progress(
    db: &mut PgConnection,
    task: Execution,
    cursor: &str,
    count: usize,
    detail: &Value,
) -> Result<()> {
    ensure!(
        queries::task_progress(task, cursor, count as i64, detail.clone())
            .execute(db)
            .await?
            .rows_affected()
            == 1,
        Error::NodeExpired
    );
    Ok(())
}

fn cutoff(
    detail: &Value,
    profile: &crate::storage::profile::StorageProfile,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<chrono::DateTime<chrono::Utc>> {
    let accepted = detail["cutoff"]
        .as_str()
        .context("sweep cutoff is missing")?
        .parse::<chrono::DateTime<chrono::Utc>>()?;
    let current = now
        .checked_sub_signed(chrono::Duration::seconds(i64::try_from(
            profile.min_storage_seconds()?,
        )?))
        .context("sweep retention is too large")?;
    Ok(accepted.min(current))
}

fn account(detail: &mut Value, row: &ListedObject) -> Result<()> {
    detail["candidates"] = json!(
        detail["candidates"]
            .as_u64()
            .unwrap_or(0)
            .checked_add(1)
            .context("sweep count overflow")?
    );
    detail["bytes"] = json!(
        detail["bytes"]
            .as_u64()
            .unwrap_or(0)
            .checked_add(row.size)
            .context("sweep bytes overflow")?
    );
    let samples = detail["samples"]
        .as_array_mut()
        .context("sweep samples are missing")?;
    if samples.len() < 20 {
        samples.push(json!(row.path));
    }
    Ok(())
}

async fn retained(
    db: &mut PgConnection,
    row: &ListedObject,
    before: chrono::DateTime<chrono::Utc>,
) -> Result<bool> {
    let Some(id) = crate::storage::Storage::parse_physical_path(&row.path) else {
        return Ok(true);
    };
    Ok(row.modified > before || queries::physical_indexed(id).fetch_one(db).await?)
}

pub(super) async fn prepare(
    tx: &mut Transaction<'_, Postgres>,
    task: Execution,
    mut detail: Value,
    profile: &crate::storage::profile::StorageProfile,
    prefix: &str,
    maximum: usize,
    maintenance: bool,
) -> Result<Option<work::Plan>> {
    let expected = if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    };
    let destructive = detail["dry_run"] == false;
    let error = if detail["prefix"] != expected {
        Some("sweep_scope_changed")
    } else if destructive && !maintenance {
        Some("sweep_requires_maintenance")
    } else {
        None
    };
    if let Some(error) = error {
        q::finish(task.id, task.token, 0, true, Some(error))
            .execute(&mut **tx)
            .await?;
        return Ok(None);
    }
    if destructive && !q::sweep_drained(task.id).fetch_one(&mut **tx).await? {
        return Ok(None);
    }
    let mut cursor: Option<String> = sqlx::query_scalar("SELECT cursor FROM tasks WHERE id=$1")
        .bind(task.id)
        .fetch_one(&mut **tx)
        .await?;
    let before = cutoff(
        &detail,
        profile,
        queries::node_clock().fetch_one(&mut **tx).await?,
    )?;
    let limit = maximum.min(work::SCAN_ITEMS);
    if let Some(value) = detail.get("sweep_page") {
        ensure!(destructive, Error::NodeOperationConflict);
        let mut page: Page = serde_json::from_value(value.clone())?;
        ensure!(
            page.items.len() <= work::SCAN_ITEMS && page.scanned <= work::SCAN_ITEMS,
            Error::NodeOperationConflict
        );
        let mut remaining = Vec::with_capacity(page.items.len());
        for row in page.items {
            if !retained(tx, &row, before).await? {
                remaining.push(row);
            }
        }
        page.items = remaining;
        if page.items.is_empty() {
            detail.as_object_mut().unwrap().remove("sweep_page");
            progress(tx, task, &page.cursor, page.scanned, &detail).await?;
            cursor = Some(page.cursor);
        } else {
            let items = page.items.iter().take(limit).cloned().collect();
            detail["sweep_page"] = serde_json::to_value(&page)?;
            progress(tx, task, cursor.as_deref().unwrap_or(""), 0, &detail).await?;
            return Ok(Some(work::Plan::SweepDelete {
                storage: profile.storage_id,
                before,
                items,
            }));
        }
    }
    Ok(Some(work::Plan::SweepScan {
        storage: profile.storage_id,
        after: cursor,
        limit,
        bytes: profile
            .maintenance_byte_budget
            .as_deref()
            .map(crate::config::bytes)
            .transpose()?,
    }))
}

pub(super) async fn listed(
    tx: &mut Transaction<'_, Postgres>,
    task: Execution,
    mut detail: Value,
    after: Option<String>,
    limit: usize,
    bytes: Option<u64>,
    outcomes: &[work::Outcome],
    maintenance: bool,
) -> Result<(bool, Option<&'static str>)> {
    let [outcome] = outcomes else {
        return Err(Error::NodeOperationConflict.into());
    };
    let items = match outcome {
        work::Outcome::Retry => return Ok((false, Some("backend_io_failed"))),
        work::Outcome::Listed { items } => items,
        _ => return Err(Error::NodeOperationConflict.into()),
    };
    ensure!(
        items.len() <= limit && limit <= work::SCAN_ITEMS,
        Error::NodeOperationConflict
    );
    let mut previous = after.as_deref().unwrap_or("");
    let mut size = 0u64;
    for row in items {
        ensure!(
            !row.path.is_empty()
                && row.path.len() <= 1024
                && row.path.as_str() > previous
                && row.size <= i64::MAX as u64,
            Error::NodeOperationConflict
        );
        size = size
            .checked_add(row.size)
            .ok_or(Error::NodeOperationConflict)?;
        previous = &row.path;
    }
    ensure!(
        items.len() <= 1 || bytes.is_none_or(|limit| size <= limit),
        Error::NodeOperationConflict
    );
    let destructive = detail["dry_run"] == false;
    if destructive && !maintenance {
        return Ok((false, Some("sweep_requires_maintenance")));
    }
    let Some(last) = items.last() else {
        return Ok((true, None));
    };
    let storage: Uuid = detail["storage_id"]
        .as_str()
        .context("sweep storage is missing")?
        .parse()?;
    let profile = queries::storage_profile(storage)
        .fetch_one(&mut **tx)
        .await?;
    let before = cutoff(
        &detail,
        &profile,
        queries::node_clock().fetch_one(&mut **tx).await?,
    )?;
    let mut candidates = vec![];
    for row in items {
        if row.modified > before {
            continue;
        }
        if crate::storage::Storage::parse_physical_path(&row.path).is_none() {
            detail["unrecognized"] = json!(
                detail["unrecognized"]
                    .as_u64()
                    .unwrap_or(0)
                    .checked_add(1)
                    .context("sweep count overflow")?
            );
        } else if !retained(tx, row, before).await? {
            if destructive {
                candidates.push(row.clone());
            } else {
                account(&mut detail, row)?;
            }
        }
    }
    if candidates.is_empty() {
        progress(tx, task, &last.path, items.len(), &detail).await?;
    } else {
        detail["sweep_page"] = serde_json::to_value(Page {
            cursor: last.path.clone(),
            scanned: items.len(),
            items: candidates,
        })?;
        progress(tx, task, after.as_deref().unwrap_or(""), 0, &detail).await?;
    }
    Ok((false, None))
}

pub(super) async fn deleted(
    tx: &mut Transaction<'_, Postgres>,
    task: Execution,
    mut detail: Value,
    before: chrono::DateTime<chrono::Utc>,
    items: Vec<ListedObject>,
    outcomes: &[work::Outcome],
    maintenance: bool,
) -> Result<Option<&'static str>> {
    ensure!(
        items.len() == outcomes.len() && detail["dry_run"] == false,
        Error::NodeOperationConflict
    );
    if !maintenance {
        return Ok(Some("sweep_requires_maintenance"));
    }
    let mut page: Page = serde_json::from_value(detail["sweep_page"].clone())?;
    let mut failed = false;
    for (item, outcome) in items.iter().zip(outcomes) {
        ensure!(
            page.items.iter().any(|row| row.path == item.path
                && row.size == item.size
                && row.modified == item.modified),
            Error::NodeOperationConflict
        );
        match outcome {
            work::Outcome::Deleted { stored_at } => {
                ensure!(
                    stored_at.is_none_or(|time| time <= before),
                    Error::NodeOperationConflict
                );
                account(&mut detail, item)?;
            }
            work::Outcome::Retained { stored_at } => {
                ensure!(*stored_at > before, Error::NodeOperationConflict);
            }
            work::Outcome::Retry => {
                failed = true;
                continue;
            }
            _ => return Err(Error::NodeOperationConflict.into()),
        }
        page.items.retain(|row| row.path != item.path);
    }
    if page.items.is_empty() {
        detail.as_object_mut().unwrap().remove("sweep_page");
        progress(tx, task, &page.cursor, page.scanned, &detail).await?;
    } else {
        detail["sweep_page"] = serde_json::to_value(&page)?;
        let cursor: Option<String> = sqlx::query_scalar("SELECT cursor FROM tasks WHERE id=$1")
            .bind(task.id)
            .fetch_one(&mut **tx)
            .await?;
        progress(tx, task, cursor.as_deref().unwrap_or(""), 0, &detail).await?;
    }
    Ok(failed.then_some("backend_io_failed"))
}
