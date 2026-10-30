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
    ensure!(run.options.0.target_external, Error::OperationAborted);
    let mut plan = Plan {
        task: run.task_id,
        generation: run.generation,
        options: run.options.0.clone(),
        cursor: run.cursor_key.clone(),
        item: None,
        key: None,
        state: State::default(),
        parts: Vec::new(),
        native: None,
    };
    if run.phase == "scan" && !run.options.0.source_external {
        scan_native(db, run).await?;
        return Ok(None);
    }
    if run.phase == "scan" {
        plan.validate()?;
        return Ok(Some(plan));
    }
    ensure!(
        matches!(run.phase.as_str(), "verify" | "cancel"),
        Error::OperationAborted
    );
    let Some(item) = r::pending(run.task_id).fetch_optional(&mut **db).await? else {
        if !super::transfer_transactions::cleanup(db, run.task_id).await? {
            return Ok(None);
        }
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
    plan.native = native(db, run, item).await?;
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
        let current = native(db, run, item).await?;
        let mut change = external::apply(plan, outcome)?;
        if current != plan.native && plan.state.phase != Phase::Cleanup {
            change.source_changed();
        }
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

pub(super) async fn native(
    db: &mut Tx<'_>,
    run: &Run,
    item: crate::model::Id,
) -> Result<Option<external::Native>> {
    if run.options.0.source_external || run.phase == "cancel" {
        return Ok(None);
    }
    let entry = r::entry(run.task_id, item).fetch_one(&mut **db).await?;
    let source = q::source(run.options.0.source_bucket, &entry.source_key)
        .fetch_optional(&mut **db)
        .await?;
    match source {
        Some(source)
            if Some(source.id) == entry.source_pin && source.snapshot() == entry.snapshot.0 =>
        {
            Ok(Some(external::Native::new(source.id, &entry.snapshot.0)?))
        }
        _ => Ok(None),
    }
}

async fn scan_native(db: &mut Tx<'_>, run: &mut Run) -> Result<()> {
    let rows = super::replication_transactions::source_page(db, run).await?;
    if rows.is_empty() {
        run.phase = "verify".into();
        return Ok(());
    }
    for (key, source) in rows {
        let snapshot = source.as_ref().map(q::Source::snapshot).unwrap_or_default();
        q::insert_item(
            run.task_id,
            &key,
            &run.options.0.target(&key)?,
            source.map(|source| source.id),
            None,
            &snapshot,
            "planning",
            None,
        )
        .execute(&mut **db)
        .await?;
        let item = r::lookup(run.task_id, &key).fetch_one(&mut **db).await?;
        r::insert(item, &State::default())
            .execute(&mut **db)
            .await?;
        run.cursor_key = Some(key);
        run.progress.0.discovered += 1;
    }
    Ok(())
}

pub(super) async fn preflight(
    db: &mut Tx<'_>,
    run: &Run,
) -> Result<Option<crate::model::replication::Step>> {
    use super::{transfer_queries as t, transfer_transactions as transfer};
    use crate::model::{replication::Step, transfer::Input};
    if run.options.0.source_external || run.phase != "verify" {
        return Ok(None);
    }
    let Some(item) = r::pending(run.task_id).fetch_optional(&mut **db).await? else {
        return Ok(None);
    };
    let state = r::state(item).fetch_one(&mut **db).await?.0;
    if !matches!(state.phase, Phase::Compare | Phase::Start) {
        return Ok(None);
    }
    let Some(native) = native(db, run, item).await? else {
        return Ok(None);
    };
    let entry = r::entry(run.task_id, item).fetch_one(&mut **db).await?;
    if entry.snapshot.0.payload != "raw" {
        return Ok(None);
    }
    if entry.position_bytes < entry.snapshot.0.size {
        let rows = q::ranges(native.stream, entry.position_bytes)
            .fetch_all(&mut **db)
            .await?;
        let mut end = entry.position_bytes;
        let mut ids = Vec::new();
        for row in rows {
            row.validate()?;
            ensure!(row.offset_bytes == end, Error::SourceUnavailable);
            end = end
                .checked_add(row.length)
                .ok_or(Error::SourceUnavailable)?;
            ids.push(row.source.id);
        }
        ensure!(
            end > entry.position_bytes && end <= entry.snapshot.0.size,
            Error::SourceUnavailable
        );
        q::units(
            run.task_id,
            &ids.iter().map(|id| ("raw", *id)).collect::<Vec<_>>(),
        )
        .build()
        .execute(&mut **db)
        .await?;
        transfer::capture(db, run, &[], &ids).await?;
        q::advance(item, &entry.state, end)
            .execute(&mut **db)
            .await?;
        return Ok(Some(Step::Continue));
    }
    if let Some(mut unit) = r::pending_raw(run.task_id, native.stream)
        .fetch_optional(&mut **db)
        .await?
    {
        if let Some(scan) = transfer::scan(db, run, &mut unit).await? {
            return Ok(Some(Step::Scan(scan)));
        }
        if unit.state == "copying" {
            let Some(sqlx::types::Json(Input::Raw { source })) = &unit.input else {
                return Err(Error::OperationAborted.into());
            };
            ensure!(unit.scanned_bytes == source.size, Error::OperationAborted);
            ensure!(
                t::empty(unit.id).execute(&mut **db).await?.rows_affected() == 1,
                Error::OperationAborted
            );
        }
        return Ok(Some(Step::Continue));
    }
    Ok(None)
}

pub(super) async fn raw_read(
    db: &mut Tx<'_>,
    run: &Run,
    stream: &crate::app::StoredStream,
    offset: u64,
    length: u32,
) -> Result<Option<external::RawRead>> {
    use crate::model::transfer::{Input, Proof};
    ensure!(
        length > 0
            && length as usize <= crate::storage::external::objects::WINDOW
            && offset <= stream.size as u64
            && u64::from(length) <= stream.size as u64 - offset,
        Error::OperationAborted
    );
    if stream.payload_type != "raw" {
        return Ok(None);
    }
    let row = r::range(stream.id, offset as i64)
        .fetch_optional(&mut **db)
        .await?
        .ok_or(Error::SourceUnavailable)?;
    row.validate()?;
    let unit = super::transfer_queries::selected(run.task_id, "raw", row.source.id)
        .fetch_one(&mut **db)
        .await?;
    let Some(sqlx::types::Json(Input::Raw { source })) = &unit.input else {
        return Err(Error::SourceUnavailable.into());
    };
    ensure!(
        unit.state == "ready"
            && unit.scanned_bytes == row.source.size
            && crate::model::replication::same_source(source, &row.source),
        Error::SourceUnavailable
    );
    let position = row.source_offset as u64 + offset - row.offset_bytes as u64;
    let (start, size, hash) = r::proof(unit.id, position as i64)
        .fetch_one(&mut **db)
        .await?;
    let until = (start as u64)
        .checked_add(size as u64)
        .ok_or(Error::SourceUnavailable)?;
    ensure!(position < until, Error::SourceUnavailable);
    let length = u64::from(length)
        .min(row.offset_bytes as u64 + row.length as u64 - offset)
        .min(until - position);
    let range = external::RawRead {
        source: row.source,
        proof: Proof {
            offset: start as u64,
            size: u32::try_from(size)?,
            hash,
        },
        offset: position,
        length: u32::try_from(length)?,
    };
    range.validate()?;
    Ok(Some(range))
}
