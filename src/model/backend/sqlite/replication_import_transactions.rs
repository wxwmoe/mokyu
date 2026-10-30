use super::{
    queries as base, replication_import_queries as i, replication_queries as q,
    replication_remote_queries as r, replication_remote_transactions as remote,
    transfer_queries as t,
};
use crate::model::{
    Error, Id, StreamCreate,
    replication::{
        Run, Snapshot,
        external::{self, Native, Outcome, Phase, Plan},
        import,
    },
    task::Execution,
};
use anyhow::{Context, Result, ensure};
use sqlx::{Sqlite, Transaction};

type Tx<'a> = Transaction<'a, Sqlite>;

async fn current(db: &mut Tx<'_>, run: &Run, item: &q::Entry) -> Result<bool> {
    let target = q::source(run.options.0.target_bucket, &item.target_key)
        .fetch_optional(&mut **db)
        .await?;
    Ok(match target {
        Some(target) => {
            Some(target.id) == item.target_version && target.snapshot() == item.snapshot.0
        }
        None => item.target_version.is_none(),
    })
}

async fn adopt(db: &mut Tx<'_>, task: Execution, stream: Id) -> Result<()> {
    ensure!(
        q::adopt(stream, task)
            .execute(&mut **db)
            .await?
            .rows_affected()
            == 1,
        Error::OperationAborted
    );
    Ok(())
}

async fn create(
    db: &mut Tx<'_>,
    task: Execution,
    run: &Run,
    plan: &Plan,
    _lease: bool,
) -> Result<Id> {
    let source = plan
        .state
        .source
        .as_ref()
        .context("import source missing")?;
    super::transactions::create_stream_in(
        db,
        StreamCreate {
            authority: run.options.0.authority(run.authority.0.clone()),
            bucket: run.options.0.target_bucket,
            key: plan.target()?,
            stream_type: "object".into(),
            metadata: import::metadata(source)?,
            public: false,
            claim_object: false,
            completion: None,
            owner: Some((task.id, task.token)),
        },
    )
    .await
}

async fn candidate(db: &mut Tx<'_>, task: Execution, run: &Run, plan: &Plan) -> Result<Id> {
    let item = r::entry(run.task_id, plan.item.ok_or(Error::OperationAborted)?)
        .fetch_one(&mut **db)
        .await?;
    if let Some(stream) = item.candidate {
        adopt(db, task, stream).await?;
        Ok(stream)
    } else {
        let stream = create(db, task, run, plan, false).await?;
        q::candidate(item.id, stream).execute(&mut **db).await?;
        Ok(stream)
    }
}

pub(super) async fn cleanup(
    db: &mut Tx<'_>,
    task: uuid::Uuid,
    item: Id,
    output: Option<Id>,
) -> Result<()> {
    if let Some(part) = i::current(item).fetch_optional(&mut **db).await? {
        if let Some(stream) = part.part_stream {
            q::abandon(stream).execute(&mut **db).await?;
        }

        let entry = r::entry(task, item).fetch_one(&mut **db).await?;
        if let Some(stream) = entry.candidate.filter(|id| Some(*id) != output) {
            q::abandon(stream).execute(&mut **db).await?;
        }
        i::forget(item).execute(&mut **db).await?;
    }
    Ok(())
}

async fn finish_candidate(
    db: &mut Tx<'_>,
    task: Execution,
    run: &Run,
    plan: &mut Plan,
    mut snapshot: Snapshot,
) -> Result<()> {
    let stream = candidate(db, task, run, plan).await?;
    let item_id = plan.item.ok_or(Error::OperationAborted)?;
    let item = r::entry(run.task_id, item_id).fetch_one(&mut **db).await?;
    ensure!(
        item.position_bytes == snapshot.size,
        Error::SourceUnavailable
    );
    snapshot.payload = i::stream(stream).fetch_one(&mut **db).await?.payload_type;
    base::set_stream_public(stream, snapshot.public)
        .execute(&mut **db)
        .await?;
    q::finish_stream(
        stream,
        snapshot.size,
        &snapshot.etag,
        snapshot.checksums.clone(),
    )
    .execute(&mut **db)
    .await?;
    plan.state.output = Some(Native::new(stream, &snapshot)?.object);
    plan.state.phase = Phase::Finalize;
    plan.parts.clear();
    r::save(item_id, &plan.state).execute(&mut **db).await?;
    Ok(())
}

pub(super) async fn prepare(
    db: &mut Tx<'_>,
    task: Execution,
    run: &mut Run,
    mut plan: Plan,
) -> Result<Option<Plan>> {
    ensure!(
        run.options.0.source_external && !run.options.0.target_external,
        Error::OperationAborted
    );
    ensure!(
        q::profile(run.options.0.target_bucket)
            .fetch_one(&mut **db)
            .await?
            == (run.storage()?, run.profile_revision()?),
        s3s::s3_error!(OperationAborted, "storage profile changed")
    );
    let item_id = plan.item.ok_or(Error::OperationAborted)?;
    let mut item = r::entry(run.task_id, item_id).fetch_one(&mut **db).await?;
    if i::current(item_id)
        .fetch_optional(&mut **db)
        .await?
        .is_none()
    {
        ensure!(plan.state.phase == Phase::Inspect, Error::OperationAborted);
        let target = q::source(run.options.0.target_bucket, &item.target_key)
            .fetch_optional(&mut **db)
            .await?;
        let snapshot = target.as_ref().map(q::Source::snapshot).unwrap_or_default();
        i::insert(item_id).execute(&mut **db).await?;
        i::target(item_id, target.map(|target| target.id), &snapshot)
            .execute(&mut **db)
            .await?;
        item = r::entry(run.task_id, item_id).fetch_one(&mut **db).await?;
    }
    if !current(db, run, &item).await? {
        let change = external::apply(&plan, &Outcome::Conflict)?;
        r::save(item_id, &change.state).execute(&mut **db).await?;
        remote::finished(
            db,
            run,
            item_id,
            change.finish.as_ref().ok_or(Error::OperationAborted)?,
            None,
        )
        .await?;
        return Ok(None);
    }
    plan.native = remote::native(db, run, item_id).await?;
    if plan.state.phase == Phase::Project {
        project(db, task, run, &mut plan, &item).await?;
        return Ok(None);
    }
    if plan.state.phase == Phase::Complete {
        let snapshot = import::snapshot(&plan)?;
        finish_candidate(db, task, run, &mut plan, snapshot).await?;
    }
    if plan.state.phase == Phase::Import {
        candidate(db, task, run, &plan).await?;
        let part = i::current(item_id).fetch_one(&mut **db).await?;
        if let Some(stream) = part.part_stream {
            let stored = i::stream(stream).fetch_optional(&mut **db).await?;
            if let Some(stored) = stored.filter(|stream| !stream.etag.is_empty()) {
                adopt(db, task, stream).await?;
                let receipt = import::receipt(i32::from(plan.state.position) + 1, &stored)?;
                receipt.validate(&plan.state)?;
                let change = external::apply(&plan, &Outcome::Transferred(receipt))?;
                r::part(
                    item_id,
                    change.part.as_ref().ok_or(Error::OperationAborted)?,
                )
                .execute(&mut **db)
                .await?;
                r::save(item_id, &change.state).execute(&mut **db).await?;
                return Ok(None);
            }
            q::abandon(stream).execute(&mut **db).await?;
        }
        let stream = create(db, task, run, &plan, true).await?;
        i::part(item_id, Some(stream), None, None)
            .execute(&mut **db)
            .await?;
        let (offset, size) = plan.state.range(i32::from(plan.state.position) + 1)?;
        plan.incoming = Some(import::Write {
            stream,
            storage: run.storage()?,
            offset,
            size,
            operation: None,
        });
    }
    plan.validate()?;
    Ok(Some(plan))
}

async fn project(
    db: &mut Tx<'_>,
    task: Execution,
    run: &Run,
    plan: &mut Plan,
    item: &q::Entry,
) -> Result<()> {
    let root = item.candidate.ok_or(Error::OperationAborted)?;
    adopt(db, task, root).await?;
    let (stream, payload, start, size) = if item.reuse_target {
        (
            item.target_version.ok_or(Error::SourceUnavailable)?,
            item.snapshot.0.payload.clone(),
            0,
            u64::try_from(item.snapshot.0.size)?,
        )
    } else {
        let part = i::current(item.id).fetch_one(&mut **db).await?;
        let stream = part.part_stream.ok_or(Error::OperationAborted)?;
        adopt(db, task, stream).await?;
        let stored = i::stream(stream).fetch_one(&mut **db).await?;
        let receipt = import::receipt(i32::from(plan.state.position), &stored)?;
        receipt.validate(&plan.state)?;
        let (start, size) = plan.state.range(receipt.number)?;
        (stream, stored.payload_type, start, size)
    };
    let start = i64::try_from(start)?;
    let mut position = item.position_bytes - start;
    ensure!(
        position >= 0 && (position < size as i64 || size == 0 && item.reuse_target),
        Error::SourceUnavailable
    );
    let previous = position;
    if size == 0 {
        ensure!(item.reuse_target, Error::SourceUnavailable);
    } else if payload == "raw" {
        if item.position_bytes == 0 {
            ensure!(
                base::raw_prepare(root)
                    .execute(&mut **db)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
        }
        let selected = q::ranges(stream, position).fetch_all(&mut **db).await?;
        let mut ids = std::collections::BTreeSet::new();
        let mut rows = Vec::new();
        for row in selected {
            row.validate()?;
            ensure!(
                row.offset_bytes == position && row.source.storage_id == run.storage()?,
                Error::SourceUnavailable
            );
            rows.push((
                start + position,
                row.length,
                row.source.id,
                row.source_offset,
            ));
            position += row.length;
            ids.insert(row.source.id);
        }
        let ids: Vec<_> = ids.into_iter().collect();
        let locked: Vec<Id> = base::raw_copy_sources(run.storage()?, &ids)
            .build_query_scalar()
            .fetch_all(&mut **db)
            .await?;
        ensure!(
            locked == ids && position > previous && position <= size as i64,
            Error::SourceUnavailable
        );
        t::project_raw(root, &rows)
            .build()
            .execute(&mut **db)
            .await?;
        base::raw_copy_reference(&ids)
            .build()
            .execute(&mut **db)
            .await?;
    } else {
        let selected = t::projected(stream, position, size as i64, 128)
            .fetch_all(&mut **db)
            .await?;
        let mut chunks = std::collections::BTreeSet::new();
        let mut rows = Vec::new();
        for (offset, length, chunk, within) in selected {
            ensure!(offset == position && length > 0, Error::SourceUnavailable);
            rows.push((start + position, length, chunk, within));
            chunks.insert(chunk);
            position += i64::from(length);
        }
        ensure!(
            position > previous && position <= size as i64,
            Error::SourceUnavailable
        );
        let chunks: Vec<_> = chunks.into_iter().collect();
        super::transactions::require_sources(db, None, &chunks).await?;
        t::project(root, &rows).build().execute(&mut **db).await?;
        base::changed_references(db, &chunks, true).await?;
    }
    base::resize_writing_stream(root, start + position)
        .execute(&mut **db)
        .await?;
    q::advance(item.id, "copying", start + position)
        .execute(&mut **db)
        .await?;
    if position == size as i64 {
        if item.reuse_target {
            let mut snapshot = item.snapshot.0.clone();
            snapshot.metadata =
                import::metadata(plan.state.source.as_ref().ok_or(Error::SourceUnavailable)?)?;
            snapshot.public = plan.options.permissions.public(
                plan.state.source_public.unwrap_or(false),
                plan.state.target_public,
            );
            finish_candidate(db, task, run, plan, snapshot).await?;
            return Ok(());
        }
        i::part(item.id, None, None, None)
            .execute(&mut **db)
            .await?;
        q::abandon(stream).execute(&mut **db).await?;
        plan.state.phase = if plan.state.position == plan.state.parts()? {
            Phase::Complete
        } else {
            Phase::Import
        };
        r::save(item.id, &plan.state).execute(&mut **db).await?;
    }
    Ok(())
}

pub(super) async fn complete(
    db: &mut Tx<'_>,
    task: Execution,
    run: &mut Run,
    plan: &Plan,
    outcome: &Outcome,
) -> Result<()> {
    let item_id = plan.item.ok_or(Error::OperationAborted)?;
    let item = r::entry(run.task_id, item_id).fetch_one(&mut **db).await?;
    let outcome = if !current(db, run, &item).await? {
        Outcome::Conflict
    } else if matches!(outcome, Outcome::Imported) {
        ensure!(plan.state.phase == Phase::Import, Error::OperationAborted);
        let incoming = plan.incoming.as_ref().ok_or(Error::OperationAborted)?;
        let part = i::current(item_id).fetch_one(&mut **db).await?;
        ensure!(
            part.part_stream == Some(incoming.stream) && part.operation_id == incoming.operation,
            Error::OperationAborted
        );
        adopt(db, task, incoming.stream).await?;
        let stored = i::stream(incoming.stream).fetch_one(&mut **db).await?;
        Outcome::Transferred(import::receipt(
            i32::from(plan.state.position) + 1,
            &stored,
        )?)
    } else {
        ensure!(
            !matches!(outcome, Outcome::Transferred(_)),
            Error::OperationAborted
        );
        if let Outcome::Inspected {
            target,
            target_public,
            ..
        } = outcome
        {
            ensure!(
                *target == plan.native.as_ref().map(|native| native.object.clone())
                    && *target_public == plan.native.as_ref().map(|native| native.public),
                Error::OperationAborted
            );
        }
        outcome.clone()
    };
    let change = external::apply(plan, &outcome)?;
    if change.state.phase == Phase::Project && change.part.is_none() {
        q::reuse_target(item_id).execute(&mut **db).await?;
        let mut reused = plan.clone();
        reused.state = change.state.clone();
        candidate(db, task, run, &reused).await?;
    }
    if let Some(part) = &change.part {
        r::part(item_id, part).execute(&mut **db).await?;
    }
    r::save(item_id, &change.state).execute(&mut **db).await?;
    if let Some(finish) = &change.finish {
        let output = if finish.status == "completed" {
            let candidate = item.candidate.ok_or(Error::OperationAborted)?;
            adopt(db, task, candidate).await?;
            super::transactions::publish_import_in(
                db,
                task,
                candidate,
                run.options.0.target_bucket,
                &item.target_key,
                item.target_version,
                plan.options.permissions.public(
                    plan.state.source_public.unwrap_or(false),
                    plan.state.target_public,
                ),
            )
            .await?;
            Some(candidate)
        } else if finish.status == "skipped" {
            item.target_version
        } else {
            None
        };
        remote::finished(db, run, item_id, finish, output).await?;
    }
    Ok(())
}
