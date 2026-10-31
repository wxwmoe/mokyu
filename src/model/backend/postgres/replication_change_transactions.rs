use super::{
    replication_change_queries as c, replication_queries as q, replication_remote_queries as r,
};
use crate::model::{
    Error, Id,
    replication::{Options, Permissions, Run, changes::Confirmation, external::State},
};
use anyhow::{Result, ensure};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

type Tx<'a> = Transaction<'a, Postgres>;

pub(super) async fn activate(db: &mut Tx<'_>, task: Uuid, options: &Options) -> Result<()> {
    if !options.source_external {
        c::gate(options.source_bucket).fetch_one(&mut **db).await?;
        c::subscribe(task, options.source_bucket, &options.scope)
            .execute(&mut **db)
            .await?;
        c::count(options.source_bucket, 1)
            .execute(&mut **db)
            .await?;
    }
    Ok(())
}

pub(super) async fn seal(db: &mut Tx<'_>, run: &mut Run) -> Result<()> {
    if run.captured && !run.sealed {
        c::gate(run.options.0.source_bucket)
            .fetch_one(&mut **db)
            .await?;
        if c::seal(run.task_id)
            .execute(&mut **db)
            .await?
            .rows_affected()
            == 1
        {
            c::count(run.options.0.source_bucket, -1)
                .execute(&mut **db)
                .await?;
        }
    }
    run.sealed = true;
    Ok(())
}

pub(super) async fn cancel(db: &mut Tx<'_>, run: &mut Run) -> Result<bool> {
    if !run.sealed {
        seal(db, run).await?;
        return Ok(false);
    }
    if c::discard(run.task_id)
        .execute(&mut **db)
        .await?
        .rows_affected()
        > 0
    {
        return Ok(false);
    }
    c::forget(run.task_id).execute(&mut **db).await?;
    Ok(true)
}

pub(super) async fn advance(db: &mut Tx<'_>, run: &mut Run) -> Result<bool> {
    if run.redundancy_id.is_some() {
        if !run.scanned
            || super::redundancy_queries::pending(run.task_id)
                .fetch_one(&mut **db)
                .await?
        {
            run.phase = "scan".into();
            return Ok(true);
        }
        return Ok(false);
    }
    if !run.scanned
        || !run.sealed
        || run.captured && c::subscribed(run.task_id).fetch_one(&mut **db).await?
    {
        run.phase = "scan".into();
        return Ok(true);
    }
    Ok(false)
}

pub(super) fn owned(item: &q::Entry, target: &q::Source) -> bool {
    item.confirmation
        .as_ref()
        .is_some_and(|value| value.0.matches(target.id, &target.snapshot()))
}

pub(super) async fn confirm(
    db: &mut Tx<'_>,
    item: &q::Entry,
    bucket: Uuid,
    output: Id,
) -> Result<()> {
    let current = q::source(bucket, &item.target_key)
        .fetch_one(&mut **db)
        .await?;
    ensure!(current.id == output, Error::OperationAborted);
    c::confirm(
        item.id,
        &Confirmation::Internal {
            version: output,
            snapshot: current.snapshot(),
        },
    )
    .execute(&mut **db)
    .await?;
    Ok(())
}

pub(super) fn forget_result(run: &mut Run, state: &str) {
    match state {
        "completed" => run.progress.0.completed -= 1,
        "skipped" => run.progress.0.skipped -= 1,
        "conflict" => run.progress.0.conflicts -= 1,
        _ => {}
    }
}

pub(super) async fn reset_remote(db: &mut Tx<'_>, item: &q::Entry) -> Result<()> {
    let previous_state = r::state(item.id)
        .fetch_optional(&mut **db)
        .await?
        .map(|state| state.0)
        .unwrap_or_default();
    let previous = item
        .confirmation
        .as_ref()
        .map(|value| value.0.target())
        .transpose()?;
    c::reset_parts(item.id).execute(&mut **db).await?;
    c::reset_external(item.id).execute(&mut **db).await?;
    r::insert(
        item.id,
        &State {
            previous,
            cleanup_pending: previous_state.cleanup_pending || previous_state.starts > 1,
            ..State::default()
        },
    )
    .execute(&mut **db)
    .await?;
    Ok(())
}

pub(super) async fn replay(db: &mut Tx<'_>, run: &mut Run) -> Result<()> {
    // Release scan row locks before closing the publication gate.
    if run.redundancy_id.is_none() && !run.sealed {
        return seal(db, run).await;
    }
    // Live publishers lock the source before the queue row, including same-version ACL updates.
    let mut frozen = None;
    let change = if run.redundancy_id.is_some() {
        if let Some(seen) = super::redundancy_queries::peek(run.task_id)
            .fetch_optional(&mut **db)
            .await?
        {
            if let Some(version) = seen.source_version {
                frozen = c::frozen(version).fetch_optional(&mut **db).await?;
            }
            let current = super::redundancy_queries::exact(run.task_id, &seen.object_key)
                .fetch_optional(&mut **db)
                .await?;
            if !current.as_ref().is_some_and(|row| {
                row.revision == seen.revision && row.source_version == seen.source_version
            }) {
                return Ok(());
            }
            current
        } else {
            None
        }
    } else {
        c::pending(run.task_id).fetch_optional(&mut **db).await?
    };
    let Some(change) = change else {
        if run.redundancy_id.is_none() {
            c::forget(run.task_id).execute(&mut **db).await?;
        }
        run.phase = if run.options.0.external() {
            "verify"
        } else {
            "publish"
        }
        .into();
        return Ok(());
    };
    let existing = match r::lookup(run.task_id, &change.object_key)
        .fetch_optional(&mut **db)
        .await?
    {
        Some(id) => Some(r::entry(run.task_id, id).fetch_one(&mut **db).await?),
        None => None,
    };
    let mut snapshot = change
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.0.clone())
        .unwrap_or_default();
    if let Some(version) = change.source_version {
        let source = if run.redundancy_id.is_some() {
            frozen.ok_or(Error::SourceUnavailable)?
        } else {
            c::frozen(version).fetch_one(&mut **db).await?
        };
        ensure!(
            source.size == snapshot.size && source.etag == snapshot.etag,
            Error::SourceUnavailable
        );
        snapshot.payload = source.payload_type;
    }
    if let Some(item) = existing.as_ref() {
        let mut previous = item.snapshot.0.clone();
        if matches!(item.state.as_str(), "completed" | "skipped") {
            previous.payload.clone_from(&snapshot.payload);
        }
        if run.options.0.permissions != Permissions::Inherit {
            previous.public = snapshot.public;
        }
        if !change.reconcile
            && change.source_version == item.source_version
            && previous == snapshot
            && matches!(item.state.as_str(), "completed" | "skipped" | "conflict")
        {
            c::consume(run.task_id, &change.object_key, change.revision)
                .execute(&mut **db)
                .await?;
            return Ok(());
        }
    }
    if change.source_version.is_none() {
        if let Some(item) = existing {
            if run.redundancy_id.is_some() {
                super::redundancy_queries::missing(item.id)
                    .execute(&mut **db)
                    .await?;
            }
            if run.options.0.propagate_deletes && item.confirmation.is_some() {
                let target = q::source(run.options.0.target_bucket, &item.target_key)
                    .fetch_optional(&mut **db)
                    .await?;
                if let Some(target) = target {
                    if owned(&item, &target) {
                        super::redundancy_transactions::writable(
                            db,
                            run.options.0.target_bucket,
                            &item.target_key,
                            run.redundancy_id,
                        )
                        .await?;
                        super::transactions::apply_object_change(
                            db,
                            run.options.0.target_bucket,
                            &item.target_key,
                            Some(target.id),
                            None,
                        )
                        .await?;
                    } else if item.confirmation.is_some() {
                        forget_result(run, &item.state);
                        q::finish_item(item.id, "conflict", None, Some("target_changed"))
                            .execute(&mut **db)
                            .await?;
                        run.progress.0.conflicts += 1;
                    }
                }
            } else if !matches!(item.state.as_str(), "completed" | "skipped") {
                forget_result(run, &item.state);
                q::finish_item(item.id, "skipped", None, Some("source_deleted"))
                    .execute(&mut **db)
                    .await?;
                run.progress.0.skipped += 1;
            }
        }
    } else {
        if change.reconcile {
            super::redundancy_queries::checking(run.task_id, &change.object_key, change.revision)
                .execute(&mut **db)
                .await?;
        }
        let target_key = run.options.0.target(&change.object_key).ok();
        let problem = if target_key.is_none() {
            Some("invalid_target_key")
        } else if run.redundancy_id.is_some()
            && crate::model::redundancy::marked(&snapshot.metadata)
        {
            Some("replica_origin")
        } else {
            None
        };
        if let Some(problem) = problem {
            if let Some(item) = existing {
                forget_result(run, &item.state);
                q::finish_item(item.id, "conflict", None, Some(problem))
                    .execute(&mut **db)
                    .await?;
            } else {
                q::insert_item(
                    run.task_id,
                    &change.object_key,
                    target_key.as_deref().unwrap_or_default(),
                    change.source_version,
                    None,
                    &snapshot,
                    "conflict",
                    Some(problem),
                )
                .execute(&mut **db)
                .await?;
                run.progress.0.discovered += 1;
            }
            run.progress.0.conflicts += 1;
            c::consume(run.task_id, &change.object_key, change.revision)
                .execute(&mut **db)
                .await?;
            return Ok(());
        }
        let target_key = target_key.ok_or(Error::OperationAborted)?;
        let target = if run.options.0.target_external {
            None
        } else {
            q::source(run.options.0.target_bucket, &target_key)
                .fetch_optional(&mut **db)
                .await?
                .map(|target| target.id)
        };
        if let Some(item) = existing {
            forget_result(run, &item.state);
            c::reopen(
                item.id,
                change.source_version,
                target,
                &snapshot,
                "planning",
                None,
            )
            .execute(&mut **db)
            .await?;
            if run.options.0.target_external {
                reset_remote(db, &item).await?;
            }
        } else {
            q::insert_item(
                run.task_id,
                &change.object_key,
                &target_key,
                change.source_version,
                target,
                &snapshot,
                "planning",
                None,
            )
            .execute(&mut **db)
            .await?;
            if run.options.0.target_external {
                let item = r::lookup(run.task_id, &change.object_key)
                    .fetch_one(&mut **db)
                    .await?;
                r::insert(item, &State::default())
                    .execute(&mut **db)
                    .await?;
            }
            run.progress.0.discovered += 1;
        }
        run.phase = if run.options.0.target_external {
            "verify"
        } else {
            "plan"
        }
        .into();
        return Ok(());
    }
    c::consume(run.task_id, &change.object_key, change.revision)
        .execute(&mut **db)
        .await?;
    Ok(())
}
