use super::{replication_queries as q, replication_remote_queries as r};
use crate::model::{
    Error,
    replication::{
        Run, Snapshot,
        external::{self, Part, Phase, Plan, State},
    },
};
use anyhow::{Result, ensure};
use futures_util::TryStreamExt;
use sqlx::{MySql, Transaction};

type Tx<'a> = Transaction<'a, MySql>;

async fn parts(db: &mut Tx<'_>, item: crate::model::Id, state: &State) -> Result<Vec<Part>> {
    let bounds = match state.phase {
        Phase::Transfer if state.position > 0 => {
            Some((i32::from(state.position), i32::from(state.position)))
        }
        Phase::Complete => Some((1, i32::from(state.parts()?))),
        Phase::Verify => Some((i32::from(state.position) + 1, i32::from(state.position) + 1)),
        _ => None,
    };
    let mut rows = Vec::new();
    if let Some((first, last)) = bounds {
        let mut input = r::parts(item, first, last).fetch(&mut **db);
        let mut size = 0;
        while let Some(part) = input.try_next().await? {
            size += serde_json::to_vec(&part)?.len();
            ensure!(
                size <= 3 * 1024 * 1024 - 384 * 1024,
                "transfer receipts exceed limit"
            );
            rows.push(part);
        }
    }
    Ok(rows)
}

async fn finished(
    db: &mut Tx<'_>,
    run: &mut Run,
    item: crate::model::Id,
    result: &external::Finish,
) -> Result<()> {
    q::finish_item(item, &result.status, None, result.error.as_deref())
        .execute(&mut **db)
        .await?;
    match result.status.as_str() {
        "completed" => run.progress.0.completed += 1,
        "skipped" => run.progress.0.skipped += 1,
        "conflict" => run.progress.0.conflicts += 1,
        _ => {}
    }
    Ok(())
}

pub(super) async fn prepare(db: &mut Tx<'_>, run: &mut Run) -> Result<Option<Plan>> {
    ensure!(
        run.options.0.source_external && run.options.0.target_external,
        Error::OperationAborted
    );
    let mut plan = Plan {
        task: run.task_id,
        generation: run.generation,
        options: run.options.0.clone(),
        cursor: run.cursor_key.clone(),
        item: None,
        key: None,
        state: State::default(),
        parts: Vec::new(),
    };
    if run.phase == "scan" {
        plan.validate()?;
        return Ok(Some(plan));
    }
    ensure!(
        matches!(run.phase.as_str(), "verify" | "cancel"),
        Error::OperationAborted
    );
    let Some(item) = r::pending(run.task_id).fetch_optional(&mut **db).await? else {
        run.phase = if run.phase == "cancel" {
            "cancelled"
        } else {
            "complete"
        }
        .into();
        let conflicts = run.phase == "complete" && run.progress.0.conflicts > 0;
        q::task_state(
            run.task_id,
            if conflicts { "failed" } else { "completed" },
            conflicts.then_some("replication_conflicts"),
        )
        .execute(&mut **db)
        .await?;
        r::forget(run.task_id).execute(&mut **db).await?;
        return Ok(None);
    };
    let mut state = r::state(item).fetch_one(&mut **db).await?.0;
    if run.phase == "cancel" {
        state.cancel();
        r::save(item, &state).execute(&mut **db).await?;
        if state.phase == Phase::Done {
            finished(
                db,
                run,
                item,
                state.finish.as_ref().ok_or(Error::OperationAborted)?,
            )
            .await?;
            return Ok(None);
        }
    }
    if state.phase == Phase::Start {
        state.starts = state.starts.checked_add(1).ok_or(Error::OperationAborted)?;
        r::save(item, &state).execute(&mut **db).await?;
    }
    plan.item = Some(item);
    plan.key = Some(r::key(run.task_id, item).fetch_one(&mut **db).await?);
    plan.options = run
        .options
        .0
        .for_item(plan.key.as_deref().ok_or(Error::OperationAborted)?)?;
    plan.parts = parts(db, item, &state).await?;
    plan.state = state;
    plan.validate()?;
    Ok(Some(plan))
}

pub(super) async fn complete(
    db: &mut Tx<'_>,
    run: &mut Run,
    plan: &Plan,
    outcome: &external::Outcome,
) -> Result<()> {
    plan.validate()?;
    outcome.validate()?;
    let expected = if let Some(key) = &plan.key {
        run.options.0.for_item(key)?
    } else {
        run.options.0.clone()
    };
    ensure!(
        run.task_id == plan.task
            && run.generation == plan.generation
            && serde_json::to_value(&expected)? == serde_json::to_value(&plan.options)?
            && run.cursor_key == plan.cursor,
        Error::OperationAborted
    );
    if let Some(item) = plan.item {
        ensure!(
            matches!(run.phase.as_str(), "verify" | "cancel")
                && r::pending(run.task_id).fetch_optional(&mut **db).await? == Some(item)
                && Some(r::key(run.task_id, item).fetch_one(&mut **db).await?) == plan.key,
            Error::OperationAborted
        );
        let state = r::state(item).fetch_one(&mut **db).await?.0;
        ensure!(
            state == plan.state && parts(db, item, &state).await? == plan.parts,
            Error::OperationAborted
        );
        let change = external::apply(plan, outcome)?;
        if let Some(part) = &change.part {
            r::part(item, part).execute(&mut **db).await?;
        }
        r::save(item, &change.state).execute(&mut **db).await?;
        if let Some(result) = &change.finish {
            finished(db, run, item, result).await?;
        }
        return Ok(());
    }
    ensure!(run.phase == "scan", Error::OperationAborted);
    let external::Outcome::Listed { keys, next } = outcome else {
        return Err(Error::OperationAborted.into());
    };
    plan.listing(keys, next.as_deref())?;
    let before = run.progress.0.discovered;
    for key in keys {
        if r::lookup(run.task_id, key)
            .fetch_optional(&mut **db)
            .await?
            .is_some()
        {
            continue;
        }
        let target = run.options.0.target(key)?;
        q::insert_item(
            run.task_id,
            key,
            &target,
            None,
            None,
            &Snapshot::default(),
            "planning",
            None,
        )
        .execute(&mut **db)
        .await?;
        let item = r::lookup(run.task_id, key).fetch_one(&mut **db).await?;
        r::insert(item, &State::default())
            .execute(&mut **db)
            .await?;
        run.progress.0.discovered += 1;
    }
    ensure!(
        next.is_none() || run.progress.0.discovered > before,
        "external listing made no progress"
    );
    run.cursor_key = next.clone();
    if next.is_none() {
        run.phase = "verify".into();
    }
    Ok(())
}
