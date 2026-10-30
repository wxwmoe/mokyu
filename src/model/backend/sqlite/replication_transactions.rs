use super::{Backend, queries, replication_queries as q, transactions};
use crate::{
    authorization::{Permit, Principal},
    model::{
        Error, Id, StreamCreate,
        object::StorageMigration,
        raw::RawSource,
        replication::{
            self as model, Command, Conflict, Item, Operation, Options, Permissions, Replication,
            ReplicationStore, Scope, Step,
        },
        task::Execution,
    },
};
use anyhow::{Result, ensure};
use serde_json::json;
use sqlx::{Sqlite, Transaction};
use uuid::Uuid;

type Tx<'a> = Transaction<'a, Sqlite>;

fn detail(run: &q::Run) -> serde_json::Value {
    json!({"operation":"replication","phase":run.phase,"storage_id":run.storage_id,
        "execution_mode":run.mode,"progress":run.progress.0,
        "external":run.options.0.external(),"source_bucket":run.options.0.source_bucket,"target_bucket":run.options.0.target_bucket})
}

pub(super) async fn save(db: &mut Tx<'_>, run: &q::Run) -> Result<()> {
    q::save(
        run.task_id,
        &run.phase,
        run.cursor_key.as_deref(),
        &run.progress.0,
    )
    .execute(&mut **db)
    .await?;
    q::task_progress(run.task_id, detail(run), run.progress.0.completed)
        .execute(&mut **db)
        .await?;
    Ok(())
}

async fn load(db: &mut Tx<'_>, id: Uuid) -> Result<q::Run> {
    q::run(id)
        .fetch_optional(&mut **db)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey).into())
}

async fn authorize(
    db: &mut Tx<'_>,
    run: &q::Run,
    principal: &Principal,
    write: bool,
) -> Result<()> {
    let options = &run.options.0;
    let authority = if write {
        options.authority(principal.clone())
    } else {
        options.visible(principal.clone(), false)
    };
    if write {
        transactions::lock_media_scope(db, &authority).await?;
    } else {
        transactions::lock_permit(db, &authority).await?;
    }
    if options.external() {
        let authority = if write {
            options.external_authority(principal.clone())
        } else {
            options.visible(principal.clone(), true)
        };
        let records = super::external_transactions::authorized(&mut **db, &authority).await?;
        if write {
            let connections = q::connections(run.task_id).fetch_all(&mut **db).await?;
            ensure!(connections.len() == records.len(), Error::OperationAborted);
            for record in records {
                ensure!(
                    connections
                        .iter()
                        .any(|connection| connection.external_id == record.bucket.id
                            && connection.revision == record.bucket.revision),
                    Error::OperationAborted
                );
            }
        }
    }
    Ok(())
}

pub(super) async fn owned(db: &mut Tx<'_>, task: Execution) -> Result<q::Run> {
    ensure!(
        q::owned(task).fetch_optional(&mut **db).await?.is_some(),
        Error::OperationAborted
    );
    let run = load(db, task.id).await?;
    if run.phase != "cancel" {
        ensure!(
            run.mode == q::mode().fetch_one(&mut **db).await?,
            Error::OperationAborted
        );
        authorize(db, &run, &run.authority.0, true).await?;
    }
    Ok(run)
}

#[async_trait::async_trait]
impl ReplicationStore for Backend {
    async fn create(
        &self,
        principal: &Principal,
        options: &Options,
        connections: &[model::external::Connection],
    ) -> Result<Uuid> {
        options.validate()?;
        ensure!(
            !options.external()
                || options.source_external
                    && options.target_external
                    && options.operation == Operation::Copy,
            s3s::s3_error!(NotImplemented, "external transfer route is unavailable")
        );
        let mut db = self.write.begin_with(queries::BEGIN_WRITE).await?;
        transactions::lock_media_scope(&mut db, &options.authority(principal.clone())).await?;
        if options.external() {
            let records = super::external_transactions::authorized(
                &mut db,
                &options.external_authority(principal.clone()),
            )
            .await?;
            ensure!(
                connections.len() == records.len()
                    && records.iter().all(|record| connections
                        .iter()
                        .any(|connection| connection.external_id == record.bucket.id
                            && connection.revision == record.bucket.revision)),
                Error::OperationAborted
            );
            let source = records
                .iter()
                .find(|record| record.bucket.id == options.source_bucket)
                .ok_or(Error::OperationAborted)?;
            let target = records
                .iter()
                .find(|record| record.bucket.id == options.target_bucket)
                .ok_or(Error::OperationAborted)?;
            options.external_scopes(&source.bucket.connection, &target.bucket.connection)?;
        } else {
            ensure!(connections.is_empty(), s3s::s3_error!(InvalidArgument));
        }
        let source = if options.source_external {
            None
        } else {
            Some(
                q::profile(options.source_bucket)
                    .fetch_one(&mut *db)
                    .await?,
            )
        };
        let target = if options.target_external {
            None
        } else {
            Some(
                q::profile(options.target_bucket)
                    .fetch_one(&mut *db)
                    .await?,
            )
        };
        if let Ok(id) = crate::model::REQUEST_ID.try_with(|id| *id) {
            if let Some(previous) = q::run(id).fetch_optional(&mut *db).await? {
                ensure!(
                    previous.created_by == principal.user_id()
                        && serde_json::to_value(&previous.options.0)?
                            == serde_json::to_value(options)?,
                    Error::OperationAborted
                );
                db.commit().await?;
                return Ok(id);
            }
        }
        let mode = q::mode().fetch_one(&mut *db).await?;
        let id = transactions::enqueue_task(&mut db, "storage_migration", (!options.target_external).then_some(options.target_bucket),
            json!({"operation":"replication","phase":"scan","external":options.external(),"storage_id":target.map(|value| value.0),"execution_mode":mode})).await?;
        q::create(
            id,
            principal,
            options,
            target.map(|value| value.0),
            target.map(|value| value.1),
            source.map(|value| value.0),
        )
        .execute(&mut *db)
        .await?;
        for connection in connections {
            connection.validate(id)?;
            q::connection(id, connection).execute(&mut *db).await?;
        }
        db.commit().await?;
        Ok(id)
    }

    async fn view(&self, principal: &Principal, id: Uuid) -> Result<Replication> {
        let mut db = self.write.begin_with(queries::BEGIN_WRITE).await?;
        let run = load(&mut db, id).await?;
        authorize(&mut db, &run, principal, false).await?;
        db.commit().await?;
        Ok(run.view())
    }

    async fn list(
        &self,
        principal: &Principal,
        after: Option<Uuid>,
        limit: i64,
    ) -> Result<(Vec<Replication>, Option<Uuid>)> {
        let mut db = self.write.begin_with(queries::BEGIN_WRITE).await?;
        let limit = limit.clamp(1, 100);
        let rows = q::list(principal.user_id(), after, limit + 1)
            .fetch_all(&mut *db)
            .await?;
        let next = (rows.len() > limit as usize).then(|| rows[limit as usize - 1].task_id);
        let mut visible = Vec::new();
        for row in rows.into_iter().take(limit as usize) {
            match authorize(&mut db, &row, principal, false).await {
                Ok(()) => visible.push(row.view()),
                Err(error)
                    if error
                        .downcast_ref::<s3s::S3Error>()
                        .is_some_and(|error| error.code() == &s3s::S3ErrorCode::AccessDenied) => {}
                Err(error) => return Err(error),
            }
        }
        db.commit().await?;
        Ok((visible, next))
    }

    async fn items(
        &self,
        principal: &Principal,
        id: Uuid,
        after: Option<Id>,
        limit: i64,
    ) -> Result<Vec<Item>> {
        let mut db = self.write.begin_with(queries::BEGIN_WRITE).await?;
        let run = load(&mut db, id).await?;
        authorize(&mut db, &run, principal, false).await?;
        let result = q::items(id, after, limit.clamp(1, 101))
            .fetch_all(&mut *db)
            .await?;
        db.commit().await?;
        Ok(result.into_iter().map(q::Entry::view).collect())
    }

    async fn command(&self, principal: &Principal, id: Uuid, command: Command) -> Result<()> {
        let mut db = self.write.begin_with(queries::BEGIN_WRITE).await?;
        q::task_lock(id).fetch_one(&mut *db).await?;
        let mut run = load(&mut db, id).await?;
        // Cancelling an owned task must still be possible after its bucket grant is revoked.
        if matches!(command, Command::Cancel) && run.created_by == principal.user_id() {
            transactions::lock_permit(
                &mut db,
                &Permit {
                    principal: principal.clone(),
                    resources: Default::default(),
                },
            )
            .await?;
        } else {
            authorize(&mut db, &run, principal, true).await?;
        }
        ensure!(
            !matches!(run.phase.as_str(), "complete" | "cancelled"),
            Error::OperationAborted
        );
        match command {
            Command::Pause => {
                q::task_state(id, "paused", None).execute(&mut *db).await?;
            }
            Command::Resume => {
                ensure!(
                    matches!(run.state.as_str(), "paused" | "failed"),
                    Error::OperationAborted
                );
                ensure!(
                    run.mode == q::mode().fetch_one(&mut *db).await?,
                    Error::OperationAborted
                );
                if !run.options.0.target_external {
                    let target = q::profile(run.options.0.target_bucket)
                        .fetch_one(&mut *db)
                        .await?;
                    ensure!(
                        target == (run.storage()?, run.profile_revision()?),
                        s3s::s3_error!(OperationAborted, "storage profile changed")
                    );
                }
                q::actor(id, principal).execute(&mut *db).await?;
                q::resume(id).execute(&mut *db).await?;
                run = load(&mut db, id).await?;
                run.progress.0.verified = 0;
                q::task_state(id, "queued", None).execute(&mut *db).await?;
            }
            Command::Cancel => {
                run.phase = "cancel".into();
                q::task_state(id, "queued", None).execute(&mut *db).await?;
            }
        }
        save(&mut db, &run).await?;
        db.commit().await?;
        Ok(())
    }

    async fn step(&self, task: Execution) -> Result<Step> {
        let mut db = self.write.begin_with(queries::BEGIN_WRITE).await?;
        let mut run = owned(&mut db, task).await?;
        if run.options.0.external() {
            let plan = super::replication_remote_transactions::prepare(&mut db, &mut run).await?;
            let connections = q::connections(task.id).fetch_all(&mut *db).await?;
            save(&mut db, &run).await?;
            db.commit().await?;
            return Ok(match plan {
                Some(plan) => Step::External {
                    plan: Box::new(plan),
                    connections,
                },
                None => Step::Continue,
            });
        }
        #[cfg(feature = "fault-injection")]
        let completed = run.progress.0.completed;
        let result = match run.phase.as_str() {
            "scan" => {
                scan(&mut db, &mut run).await?;
                Step::Continue
            }
            "plan" => {
                plan(&mut db, &mut run).await?;
                Step::Continue
            }
            "verify" => verification(&mut db, task, &mut run).await?,
            "publish" => {
                publish(&mut db, task, &mut run).await?;
                Step::Continue
            }
            "cancel" => {
                cancel(&mut db, &mut run).await?;
                Step::Continue
            }
            _ => return Err(Error::OperationAborted.into()),
        };
        save(&mut db, &run).await?;
        db.commit().await?;
        #[cfg(feature = "fault-injection")]
        if run.progress.0.completed > completed {
            crate::faults::point("replication-published").await;
        }
        Ok(result)
    }

    async fn external_complete(
        &self,
        task: Execution,
        plan: &model::external::Plan,
        outcome: &model::external::Outcome,
    ) -> Result<()> {
        let mut db = self.write.begin_with(queries::BEGIN_WRITE).await?;
        let mut run = owned(&mut db, task).await?;
        super::replication_remote_transactions::complete(&mut db, &mut run, plan, outcome).await?;
        save(&mut db, &run).await?;
        db.commit().await?;
        Ok(())
    }

    async fn scanned(
        &self,
        task: Execution,
        value: &crate::model::transfer::Scanned,
    ) -> Result<()> {
        let mut db = self.write.begin_with(queries::BEGIN_WRITE).await?;
        let run = owned(&mut db, task).await?;
        super::transfer_transactions::scanned(&mut db, &run, value).await?;
        db.commit().await?;
        Ok(())
    }

    async fn raw_progress(
        &self,
        task: Execution,
        plan: &crate::model::transfer::RawCopy,
        progress: &crate::model::transfer::RawProgress,
    ) -> Result<()> {
        let mut db = self.write.begin_with(queries::BEGIN_WRITE).await?;
        let run = owned(&mut db, task).await?;
        super::transfer_transactions::raw_progress(&mut db, task, &run, plan, progress).await?;
        db.commit().await?;
        Ok(())
    }

    async fn copied(
        &self,
        task: Execution,
        plan: &crate::model::transfer::Copy,
        outputs: &[crate::model::packing::PackOutput],
    ) -> Result<()> {
        let mut db = self.write.begin_with(queries::BEGIN_WRITE).await?;
        let run = owned(&mut db, task).await?;
        super::transfer_transactions::copied(&mut db, task, &run, plan, outputs).await?;
        db.commit().await?;
        Ok(())
    }

    async fn verified(&self, task: Execution, sources: &[RawSource]) -> Result<()> {
        let mut db = self.write.begin_with(queries::BEGIN_WRITE).await?;
        verified(&mut db, task, sources).await?;
        db.commit().await?;
        Ok(())
    }
}

async fn scan(db: &mut Tx<'_>, run: &mut q::Run) -> Result<()> {
    let options = run.options.0.clone();
    let rows: Vec<(String, Option<q::Source>)> = match &options.scope {
        Scope::Objects { keys } => {
            let mut keys = keys.clone();
            keys.sort();
            let selected: Vec<_> = keys
                .into_iter()
                .filter(|key| run.cursor_key.as_ref().is_none_or(|last| key > last))
                .take(64)
                .collect();
            if selected.is_empty() {
                Vec::new()
            } else {
                let mut found: std::collections::BTreeMap<_, _> =
                    q::selected(options.source_bucket, &selected)
                        .build_query_as::<q::Source>()
                        .fetch_all(&mut **db)
                        .await?
                        .into_iter()
                        .map(|row| (row.object_key.clone(), row))
                        .collect();
                selected
                    .into_iter()
                    .map(|key| {
                        let row = found.remove(&key);
                        (key, row)
                    })
                    .collect()
            }
        }
        scope => {
            let prefix = match scope {
                Scope::Prefix { prefix } => prefix.as_str(),
                _ => "",
            };
            q::scan(options.source_bucket, run.cursor_key.as_deref(), prefix)
                .fetch_all(&mut **db)
                .await?
                .into_iter()
                .map(|source| (source.object_key.clone(), Some(source)))
                .collect()
        }
    };
    if rows.is_empty() {
        run.phase = "plan".into();
        return Ok(());
    }
    let target_keys: Vec<_> = rows
        .iter()
        .filter_map(|(key, source)| source.as_ref().and_then(|_| options.target(key).ok()))
        .collect();
    let targets: std::collections::BTreeMap<_, _> = if target_keys.is_empty() {
        Default::default()
    } else {
        q::selected(options.target_bucket, &target_keys)
            .build_query_as::<q::Source>()
            .fetch_all(&mut **db)
            .await?
            .into_iter()
            .map(|row| (row.object_key, row.id))
            .collect()
    };
    for (key, source) in rows {
        let target = options.target(&key);
        let error = if target.is_err() {
            Some("invalid_target_key")
        } else if source.is_none() {
            Some("source_missing")
        } else {
            None
        };
        let target = target.unwrap_or_default();
        let expected = targets.get(&target).copied();
        let snapshot = source.as_ref().map(q::Source::snapshot).unwrap_or_default();
        q::insert_item(
            run.task_id,
            &key,
            &target,
            source.map(|s| s.id),
            expected,
            &snapshot,
            if error.is_some() {
                "conflict"
            } else {
                "planning"
            },
            error,
        )
        .execute(&mut **db)
        .await?;
        run.progress.0.discovered += 1;
        run.progress.0.conflicts += i64::from(error.is_some());
        run.cursor_key = Some(key);
    }
    Ok(())
}

async fn finish_item(
    db: &mut Tx<'_>,
    run: &mut q::Run,
    item: &q::Entry,
    state: &str,
    output: Option<Id>,
    error: Option<&str>,
) -> Result<()> {
    if let Some(id) = item.candidate {
        if output != Some(id) {
            q::abandon(id).execute(&mut **db).await?;
        }
    }
    q::finish_item(item.id, state, output, error)
        .execute(&mut **db)
        .await?;
    match state {
        "completed" => run.progress.0.completed += 1,
        "skipped" => run.progress.0.skipped += 1,
        "conflict" => run.progress.0.conflicts += 1,
        _ => {}
    }
    Ok(())
}

async fn current(db: &mut Tx<'_>, run: &q::Run, item: &q::Entry) -> Result<bool> {
    let source = q::source(run.options.0.source_bucket, &item.source_key)
        .fetch_optional(&mut **db)
        .await?;
    Ok(source.is_some_and(|source| {
        let mut snapshot = source.snapshot();
        if run.options.0.permissions != Permissions::Inherit {
            snapshot.public = item.snapshot.0.public;
        }
        Some(source.id) == item.source_version && snapshot == item.snapshot.0
    }))
}

async fn equivalent(db: &mut Tx<'_>, item: &q::Entry, target: &q::Source) -> Result<bool> {
    Ok(target.size == item.snapshot.0.size
        && (target.size == 0
            || q::same_data(
                item.source_pin.ok_or(Error::SourceUnavailable)?,
                target.id,
                &item.snapshot.0.payload,
            )
            .fetch_one(&mut **db)
            .await?))
}

async fn reusable(db: &mut Tx<'_>, item: &q::Entry, target: &q::Source) -> Result<bool> {
    use futures_util::TryStreamExt;
    let mut sources = [
        (
            item.source_pin.ok_or(Error::SourceUnavailable)?,
            item.snapshot.0.payload.as_str(),
        ),
        (target.id, target.payload_type.as_str()),
    ];
    sources.sort_by_key(|(id, _)| *id);
    for (id, payload) in sources {
        if !q::ready(id, payload).fetch_one(&mut **db).await? {
            return Ok(false);
        }
        if payload == "raw" {
            let mut rows = q::lock_raw(id).fetch(&mut **db);
            while let Some(ready) = rows.try_next().await? {
                if !ready {
                    return Ok(false);
                }
            }
        } else {
            transactions::require_sources(&mut **db, Some(id), &[]).await?;
            if !q::ready(id, payload).fetch_one(&mut **db).await? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

async fn plan(db: &mut Tx<'_>, run: &mut q::Run) -> Result<()> {
    let Some(item) = q::pending(run.task_id, "planning")
        .fetch_optional(&mut **db)
        .await?
    else {
        run.phase = "verify".into();
        return Ok(());
    };
    if !current(db, run, &item).await? {
        return finish_item(db, run, &item, "conflict", None, Some("source_changed")).await;
    }
    if item.position_bytes == 0 {
        let target = q::source(run.options.0.target_bucket, &item.target_key)
            .fetch_optional(&mut **db)
            .await?;
        if target.as_ref().map(|source| source.id) != item.target_version {
            return finish_item(db, run, &item, "conflict", None, Some("target_changed")).await;
        }
        if let Some(target) = target {
            let equal = equivalent(db, &item, &target).await?;
            if equal && reusable(db, &item, &target).await? {
                let mut expected = item.snapshot.0.clone();
                expected.public = run
                    .options
                    .0
                    .permissions
                    .public(expected.public, Some(target.public_read));
                expected.payload = target.payload_type.clone();
                if run.options.0.operation == Operation::Copy && target.snapshot() == expected {
                    return finish_item(db, run, &item, "skipped", Some(target.id), None).await;
                }
                let raw_allowed = target.payload_type != "raw"
                    || super::transfer_queries::policy(run.storage()?, run.profile_revision()?)
                        .fetch_one(&mut **db)
                        .await?
                        .0;
                if raw_allowed {
                    q::reuse_target(item.id).execute(&mut **db).await?;
                    return Ok(());
                }
            }
            if !equal && run.options.0.conflict != Conflict::Overwrite {
                return finish_item(
                    db,
                    run,
                    &item,
                    if run.options.0.conflict == Conflict::Skip {
                        "skipped"
                    } else {
                        "conflict"
                    },
                    None,
                    Some("target_exists"),
                )
                .await;
            }
        }
    }
    let source = item.source_pin.ok_or(Error::SourceUnavailable)?;
    let mut end = item.position_bytes;
    let mut units = std::collections::BTreeSet::new();
    let mut chunks = Vec::new();
    let mut raw = Vec::new();
    if item.snapshot.0.size > 0 {
        if item.snapshot.0.payload == "raw" {
            let rows = q::ranges(source, end).fetch_all(&mut **db).await?;
            ensure!(!rows.is_empty(), Error::SourceUnavailable);
            for row in rows {
                ensure!(
                    row.offset_bytes == end && row.length > 0,
                    Error::SourceUnavailable
                );
                end = end
                    .checked_add(row.length)
                    .ok_or(Error::SourceUnavailable)?;
                units.insert(("raw", row.source.id));
                raw.push(row.source.id);
            }
        } else {
            let rows = q::extents(source, end).fetch_all(&mut **db).await?;
            ensure!(!rows.is_empty(), Error::SourceUnavailable);
            for (offset, length, chunk, fragment, pack) in rows {
                ensure!(
                    offset == end && length > 0 && fragment.is_none(),
                    Error::SourceUnavailable
                );
                end = end
                    .checked_add(i64::from(length))
                    .ok_or(Error::SourceUnavailable)?;
                chunks.push((chunk.ok_or(Error::SourceUnavailable)?, pack));
                units.insert(if let Some(pack) = pack {
                    ("pack", pack)
                } else {
                    ("chunk", chunk.ok_or(Error::SourceUnavailable)?)
                });
            }
        }
    }
    ensure!(end <= item.snapshot.0.size, Error::SourceUnavailable);
    if run.source_storage()? != run.storage()? && !chunks.is_empty() {
        let ids: Vec<_> = chunks
            .iter()
            .map(|(id, _)| *id)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let layouts: Vec<(Id, Option<Id>, bool, bool)> = super::transfer_queries::layouts(&ids)
            .build_query_as()
            .fetch_all(&mut **db)
            .await?;
        ensure!(layouts.len() == ids.len(), Error::SourceUnavailable);
        if layouts
            .iter()
            .any(|(_, _, ready, pending)| !ready && *pending)
        {
            return Ok(());
        }
        ensure!(
            layouts.iter().all(|(_, _, ready, _)| *ready),
            Error::SourceUnavailable
        );
        chunks = layouts
            .into_iter()
            .map(|(id, pack, _, _)| (id, pack))
            .collect();
        units.retain(|(source_type, _)| *source_type == "raw");
        for (id, pack) in &chunks {
            units.insert(pack.map_or(("chunk", *id), |id| ("pack", id)));
        }
    }
    if !units.is_empty() {
        let units = units.into_iter().collect::<Vec<_>>();
        for (source_type, id) in &units {
            if *source_type == "raw" {
                q::raw(*id).fetch_one(&mut **db).await?.validate()?;
            }
        }
        let added = q::units(run.task_id, &units)
            .build()
            .execute(&mut **db)
            .await?
            .rows_affected() as i64;
        if added > 0 {
            for (source_type, id) in &units {
                if *source_type == "raw" {
                    q::protect_raw(*id).execute(&mut **db).await?;
                }
            }
        }
        run.progress.0.sources += added;
        if run.source_storage()? != run.storage()? {
            run.progress.0.sources -=
                super::transfer_transactions::capture(db, run, &chunks, &raw).await? as i64;
        }
    }
    q::advance(
        item.id,
        if end == item.snapshot.0.size {
            "planned"
        } else {
            "planning"
        },
        if end == item.snapshot.0.size { 0 } else { end },
    )
    .execute(&mut **db)
    .await?;
    Ok(())
}

pub(super) async fn verification(
    db: &mut Tx<'_>,
    task: Execution,
    run: &mut q::Run,
) -> Result<Step> {
    ensure!(run.phase == "verify", Error::OperationAborted);
    if run.source_storage()? != run.storage()? {
        if let Some(mut unit) = super::transfer_queries::pending(run.task_id)
            .fetch_optional(&mut **db)
            .await?
        {
            if let Some(scan) = super::transfer_transactions::scan(db, run, &mut unit).await? {
                return Ok(Step::Scan(scan));
            }
            if unit.state == "copying" || (unit.state == "selected" && unit.source_type != "raw") {
                return Ok(
                    super::transfer_transactions::prepare(db, task, run, &mut unit)
                        .await?
                        .unwrap_or(Step::Continue),
                );
            }
            return Ok(Step::Continue);
        }
        run.phase = "publish".into();
        save(db, run).await?;
        return Ok(Step::Continue);
    }
    let Some(id) = q::unverified(run.task_id, run.generation)
        .fetch_optional(&mut **db)
        .await?
    else {
        run.phase = "publish".into();
        save(db, run).await?;
        return Ok(Step::Continue);
    };
    let source = q::raw(id).fetch_one(&mut **db).await?;
    source.validate()?;
    if source.pending_upload {
        return Ok(Step::Continue);
    }
    ensure!(
        source.storage_id == run.storage()?
            && matches!(source.medium.as_str(), "local" | "shared_s3"),
        Error::SourceUnavailable
    );
    Ok(Step::Verify {
        storage: run.storage()?,
        sources: vec![source],
    })
}

pub(super) async fn verified(
    db: &mut Tx<'_>,
    task: Execution,
    sources: &[RawSource],
) -> Result<()> {
    let mut run = owned(db, task).await?;
    ensure!(
        run.phase == "verify" && sources.len() == 1,
        Error::OperationAborted
    );
    for source in sources {
        let current = q::raw(source.id).fetch_one(&mut **db).await?;
        current.validate()?;
        ensure!(
            !current.pending_upload && model::same_source(source, &current),
            Error::SourceUnavailable
        );
        run.progress.0.verified += q::verified(task.id, &current, run.generation)
            .execute(&mut **db)
            .await?
            .rows_affected() as i64;
    }
    save(db, &run).await
}

async fn publish(db: &mut Tx<'_>, task: Execution, run: &mut q::Run) -> Result<()> {
    let item = q::pending(task.id, "copying")
        .fetch_optional(&mut **db)
        .await?;
    let item = if item.is_some() {
        item
    } else {
        q::pending(task.id, "planned")
            .fetch_optional(&mut **db)
            .await?
    };
    let Some(item) = item else {
        if !super::transfer_transactions::cleanup(db, run.task_id).await? {
            return Ok(());
        }
        run.phase = "complete".into();
        q::task_state(
            task.id,
            if run.progress.0.conflicts == 0 {
                "completed"
            } else {
                "failed"
            },
            if run.progress.0.conflicts == 0 {
                None
            } else {
                Some("replication_conflicts")
            },
        )
        .execute(&mut **db)
        .await?;
        return Ok(());
    };
    if !current(db, run, &item).await? {
        return finish_item(db, run, &item, "conflict", None, Some("source_changed")).await;
    }
    let options = &run.options.0;
    let target = q::source(options.target_bucket, &item.target_key)
        .fetch_optional(&mut **db)
        .await?;
    if target.as_ref().map(|s| s.id) != item.target_version {
        return finish_item(db, run, &item, "conflict", None, Some("target_changed")).await;
    }
    if item.candidate.is_none() {
        if let Some(target) = &target {
            let equal = equivalent(db, &item, target).await?;
            let mut expected = item.snapshot.0.clone();
            expected.public = options
                .permissions
                .public(expected.public, Some(target.public_read));
            expected.payload = target.payload_type.clone();
            if equal
                && reusable(db, &item, target).await?
                && options.operation == Operation::Copy
                && target.snapshot() == expected
            {
                return finish_item(db, run, &item, "skipped", Some(target.id), None).await;
            }
            if !equal && options.conflict != Conflict::Overwrite {
                return finish_item(
                    db,
                    run,
                    &item,
                    if options.conflict == Conflict::Skip {
                        "skipped"
                    } else {
                        "conflict"
                    },
                    None,
                    Some("target_exists"),
                )
                .await;
            }
        }
    }
    let profile = q::profile(options.target_bucket)
        .fetch_one(&mut **db)
        .await?;
    ensure!(
        profile == (run.storage()?, run.profile_revision()?),
        s3s::s3_error!(OperationAborted, "storage profile changed")
    );
    let candidate = if let Some(id) = item.candidate {
        if q::adopt(id, task).execute(&mut **db).await?.rows_affected() != 1 {
            q::replan(item.id).execute(&mut **db).await?;
            run.phase = "plan".into();
            return Ok(());
        }

        id
    } else {
        let input = StreamCreate {
            authority: Permit::system(),
            bucket: options.target_bucket,
            key: item.target_key.clone(),
            stream_type: "object".into(),
            metadata: item.snapshot.0.metadata.clone(),
            public: false,
            claim_object: false,
            completion: None,
            owner: Some((task.id, task.token)),
        };
        let id = transactions::create_stream_in(db, input).await?;
        q::candidate(item.id, id).execute(&mut **db).await?;
        id
    };
    let source = item.source_pin.ok_or(Error::SourceUnavailable)?;
    let projection_source = if item.reuse_target {
        target.as_ref().ok_or(Error::OperationAborted)?.id
    } else {
        source
    };
    let raw = if item.reuse_target {
        target
            .as_ref()
            .is_some_and(|target| target.payload_type == "raw")
    } else {
        item.snapshot.0.payload == "raw"
    };
    let mut end = item.position_bytes;
    if item.snapshot.0.size > 0 {
        if run.source_storage()? != run.storage()? && !item.reuse_target {
            end = super::transfer_transactions::project(
                db,
                run,
                candidate,
                source,
                item.snapshot.0.payload == "raw",
                end,
                item.snapshot.0.size,
            )
            .await?;
        } else if raw {
            let rows = q::ranges(projection_source, end)
                .fetch_all(&mut **db)
                .await?;
            let (start, next, ids) = crate::model::raw::copy_window(run.storage()?, &rows)?;
            ensure!(
                start == end && next <= item.snapshot.0.size && next > end,
                Error::SourceUnavailable
            );
            for id in ids {
                if item.reuse_target {
                    continue;
                }
                let proof = q::proof(task.id, id, run.generation)
                    .fetch_optional(&mut **db)
                    .await?
                    .flatten();
                let current = q::raw(id).fetch_one(&mut **db).await?;
                if proof
                    .as_ref()
                    .is_none_or(|proof| !model::same_source(&proof.0, &current))
                {
                    q::invalidate(task.id, id).execute(&mut **db).await?;
                    q::abandon(candidate).execute(&mut **db).await?;
                    q::replan(item.id).execute(&mut **db).await?;
                    run.phase = "plan".into();
                    return Ok(());
                }
            }
            transactions::copy_raw_references_in(
                db,
                candidate,
                projection_source,
                run.storage()?,
                run.profile_revision()?,
                &rows,
            )
            .await?;
            end = next;
        } else {
            let rows = q::extents(projection_source, end)
                .fetch_all(&mut **db)
                .await?;
            ensure!(!rows.is_empty(), Error::SourceUnavailable);
            let mut chunks = std::collections::BTreeSet::new();
            for (offset, length, chunk, fragment, _) in rows {
                ensure!(
                    offset == end && length > 0 && fragment.is_none(),
                    Error::SourceUnavailable
                );
                end += i64::from(length);
                chunks.insert(chunk.ok_or(Error::SourceUnavailable)?);
            }
            ensure!(end <= item.snapshot.0.size, Error::SourceUnavailable);
            transactions::copy_chunk_references_in(
                db,
                candidate,
                projection_source,
                item.position_bytes,
                end,
                &chunks.into_iter().collect::<Vec<_>>(),
            )
            .await?;
        }
    }
    if end < item.snapshot.0.size {
        q::advance(item.id, "copying", end)
            .execute(&mut **db)
            .await?;
        return Ok(());
    }
    if item.reuse_target {
        ensure!(
            reusable(db, &item, target.as_ref().ok_or(Error::OperationAborted)?).await?,
            Error::SourceUnavailable
        );
    }
    let public = options.permissions.public(
        item.snapshot.0.public,
        target.as_ref().map(|s| s.public_read),
    );
    queries::set_stream_public(candidate, public)
        .execute(&mut **db)
        .await?;
    q::finish_stream(
        candidate,
        item.snapshot.0.size,
        &item.snapshot.0.etag,
        item.snapshot.0.checksums.clone(),
    )
    .execute(&mut **db)
    .await?;
    let job = StorageMigration {
        source_bucket: options.source_bucket,
        source_key: item.source_key.clone(),
        source_version: source,
        target_bucket: options.target_bucket,
        target_key: item.target_key.clone(),
        target_version: item.target_version,
        action: if options.operation == Operation::Move {
            "move"
        } else {
            "copy"
        }
        .into(),
        public_read: public,
    };
    transactions::publish_migration_in(db, task, candidate, &job).await?;
    finish_item(db, run, &item, "completed", Some(candidate), None).await
}

async fn cancel(db: &mut Tx<'_>, run: &mut q::Run) -> Result<()> {
    let rows = q::live(run.task_id).fetch_all(&mut **db).await?;
    if rows.is_empty() {
        if !super::transfer_transactions::cleanup(db, run.task_id).await? {
            return Ok(());
        }
        run.phase = "cancelled".into();
        q::task_state(run.task_id, "completed", None)
            .execute(&mut **db)
            .await?;
    } else {
        for item in rows {
            finish_item(db, run, &item, "cancelled", None, None).await?;
        }
    }
    Ok(())
}
