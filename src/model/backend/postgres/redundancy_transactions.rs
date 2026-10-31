use super::{
    Backend, queries, redundancy_queries as q, replication_change_queries as c,
    replication_change_transactions as changes, replication_queries as r,
    replication_transactions as transfer, transactions,
};
use crate::{
    authorization::{Action, Permit, Principal},
    model::{
        Error,
        redundancy::{
            Action as CommandAction, Command, Link, Location, Placement, Preview, RedundancyStore,
            Row, Settings,
        },
        replication::{Options, Run, external::Connection},
    },
};
use anyhow::{Result, ensure};
use futures_util::TryStreamExt;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

type Tx<'a> = Transaction<'a, Postgres>;

async fn access(db: &mut Tx<'_>, principal: &Principal, bucket: Uuid) -> Result<()> {
    transactions::lock_permit(
        db,
        &Permit::for_action(principal.clone(), bucket, Action::Settings),
    )
    .await
}

pub(super) fn authority(options: &Options, principal: Principal) -> Permit {
    let mut permit = options.authority(principal);
    permit.add(options.source_bucket, Action::Settings);
    if !options.target_external {
        permit.add(options.target_bucket, Action::Settings);
    }
    permit
}

type Locations = ((Uuid, Location), (Option<Uuid>, Location));

async fn locations(db: &mut Tx<'_>, principal: &Principal, options: &Options) -> Result<Locations> {
    transactions::lock_media_scope(db, &authority(options, principal.clone())).await?;
    async fn native(db: &mut Tx<'_>, bucket: Uuid) -> Result<(Uuid, Location)> {
        let (id, _) = r::profile(bucket)
            .fetch_optional(&mut **db)
            .await?
            .ok_or(Error::OperationAborted)?;
        let record = queries::enabled_storage(id).fetch_one(&mut **db).await?;
        Ok((id, Location::storage(&record)?))
    }
    let source = native(db, options.source_bucket).await?;
    let target = if options.target_external {
        let records = super::external_transactions::authorized(
            db,
            &options.external_authority(principal.clone()),
        )
        .await?;
        let record = records
            .into_iter()
            .find(|r| r.bucket.id == options.target_bucket)
            .ok_or(Error::OperationAborted)?;
        (
            None,
            Location::remote(&record.bucket.connection, &options.target_prefix)?,
        )
    } else {
        let (id, target) = native(db, options.target_bucket).await?;
        ensure!(
            id != source.0,
            s3s::s3_error!(
                InvalidArgument,
                "redundancy requires different storage backends"
            )
        );
        (Some(id), target)
    };
    Ok((source, target))
}

async fn check(
    db: &mut Tx<'_>,
    source: Uuid,
    settings: &Settings,
    placement: &Placement,
) -> Result<()> {
    placement.validate()?;
    let mut rows = q::candidates(source, settings, placement).fetch(&mut **db);
    while let Some(row) = rows.try_next().await? {
        placement.check(source, settings, &row)?;
    }
    Ok(())
}

impl Backend {
    async fn placement(
        &self,
        principal: &Principal,
        options: &Options,
    ) -> Result<(Locations, Placement)> {
        let mut db = self.pool.begin().await?;
        let input = locations(&mut db, principal, options).await?;
        db.commit().await?;
        let (source, target) = tokio::try_join!(input.0.1.resolve(), input.1.1.resolve())?;
        let placement = Placement { source, target };
        placement.validate()?;
        Ok((input, placement))
    }
}

async fn load(db: &mut Tx<'_>, bucket: Uuid, id: Uuid) -> Result<Row> {
    let row = q::link(id)
        .fetch_optional(&mut **db)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    ensure!(
        row.source_bucket == bucket
            || (!row.settings.0.target_external && row.settings.0.target_bucket == bucket),
        s3s::s3_error!(NoSuchKey)
    );
    Ok(row)
}

async fn activate(db: &mut Tx<'_>, id: Uuid, options: &Options) -> Result<()> {
    changes::activate(db, id, options).await?;
    q::transition(id, "active").execute(&mut **db).await?;
    Ok(())
}

// Callers supplying an owner have already locked its current execution token.
pub(super) async fn writable(
    db: &mut Tx<'_>,
    bucket: Uuid,
    key: &str,
    owner: Option<Uuid>,
) -> Result<()> {
    let managed = q::gate(bucket).fetch_one(&mut **db).await?;
    admitted(db, bucket, key, owner, managed).await
}

pub(super) async fn admitted(
    db: &mut Tx<'_>,
    bucket: Uuid,
    key: &str,
    owner: Option<Uuid>,
    managed: i32,
) -> Result<()> {
    if managed != 0 {
        if let Some((id, state)) = q::manager(bucket, key).fetch_optional(&mut **db).await? {
            ensure!(
                Some(id) == owner && matches!(state.as_str(), "active" | "catchup" | "reconciling"),
                s3s::s3_error!(
                    AccessDenied,
                    "object prefix is managed by a redundancy link"
                )
            );
        }
    }
    Ok(())
}

pub(super) async fn accept_origin(db: &mut Tx<'_>, bucket: Uuid, marked: bool) -> Result<()> {
    if marked {
        ensure!(
            !q::outgoing(bucket).fetch_one(&mut **db).await?,
            s3s::s3_error!(
                InvalidBucketState,
                "cannot publish a marked replica into a redundancy source"
            )
        );
    }
    Ok(())
}

pub(super) async fn removable(db: &mut Tx<'_>, bucket: Uuid) -> Result<()> {
    ensure!(
        !q::referenced(bucket).fetch_one(&mut **db).await?,
        s3s::s3_error!(OperationAborted, "remove the bucket redundancy link first")
    );
    Ok(())
}

#[async_trait::async_trait]
impl RedundancyStore for Backend {
    async fn preview(
        &self,
        principal: &Principal,
        bucket: Uuid,
        settings: &Settings,
    ) -> Result<Preview> {
        let options = settings.replication(bucket)?;
        let (input, placement) = self.placement(principal, &options).await?;
        let mut db = self.pool.begin().await?;
        ensure!(
            input == locations(&mut db, principal, &options).await?,
            Error::OperationAborted
        );
        check(&mut db, bucket, settings, &placement).await?;
        db.commit().await?;
        Ok(Preview {
            source_bucket: bucket,
            settings: settings.clone(),
            source_storage: input.0.0,
            target_storage: input.1.0,
        })
    }

    async fn create(
        &self,
        principal: &Principal,
        bucket: Uuid,
        settings: &Settings,
        connections: &[Connection],
    ) -> Result<Uuid> {
        let options = settings.replication(bucket)?;
        let (input, placement) = self.placement(principal, &options).await?;
        let mut db = self.pool.begin().await?;
        q::lock_catalog().execute(&mut *db).await?;
        ensure!(
            input == locations(&mut db, principal, &options).await?,
            Error::OperationAborted
        );
        if let Ok(id) = crate::model::REQUEST_ID.try_with(|id| *id) {
            if let Some(run) = r::run(id).fetch_optional(&mut *db).await? {
                ensure!(
                    run.redundancy_id == Some(id) && run.created_by == principal.user_id(),
                    Error::OperationAborted
                );
                let row = load(&mut db, bucket, id).await?;
                ensure!(
                    row.source_bucket == bucket && row.settings.0 == *settings,
                    Error::OperationAborted
                );
                db.commit().await?;
                return Ok(id);
            }
        }
        check(&mut db, bucket, settings, &placement).await?;
        if !settings.target_external {
            c::gate(settings.target_bucket).fetch_one(&mut *db).await?;
        }
        let id = transfer::create_in(&mut db, principal, &options, connections, true).await?;
        q::insert(id, bucket, settings, &placement)
            .execute(&mut *db)
            .await?;
        q::backlog(id, settings.max_pending_keys)
            .execute(&mut *db)
            .await?;
        if !settings.target_external {
            q::count(settings.target_bucket, 1)
                .execute(&mut *db)
                .await?;
        }
        db.commit().await?;
        let mut db = self.pool.begin().await?;
        r::task_lock(id).fetch_one(&mut *db).await?;
        if q::state(id).fetch_optional(&mut *db).await?.as_deref() == Some("preparing") {
            activate(&mut db, id, &options).await?;
        }
        db.commit().await?;
        Ok(id)
    }

    async fn get(&self, principal: &Principal, bucket: Uuid, id: Uuid) -> Result<Link> {
        let mut db = self.pool.begin().await?;
        access(&mut db, principal, bucket).await?;
        let row = load(&mut db, bucket, id).await?;
        db.commit().await?;
        Ok(row.view())
    }

    async fn list(
        &self,
        principal: &Principal,
        bucket: Uuid,
        after: Option<Uuid>,
        limit: i64,
    ) -> Result<(Vec<Link>, Option<Uuid>)> {
        ensure!((1..=100).contains(&limit), s3s::s3_error!(InvalidArgument));
        let mut db = self.pool.begin().await?;
        access(&mut db, principal, bucket).await?;
        let mut rows = q::page(bucket, after, limit + 1)
            .fetch_all(&mut *db)
            .await?;
        let next = (rows.len() > limit as usize).then(|| rows[limit as usize - 1].id);
        rows.truncate(limit as usize);
        db.commit().await?;
        Ok((rows.into_iter().map(Row::view).collect(), next))
    }

    async fn command(
        &self,
        principal: &Principal,
        bucket: Uuid,
        id: Uuid,
        command: &Command,
        connections: &[Connection],
    ) -> Result<()> {
        command.validate()?;
        let mut db = self.pool.begin().await?;
        access(&mut db, principal, bucket).await?;
        r::task_lock(id)
            .fetch_optional(&mut *db)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        let row = load(&mut db, bucket, id).await?;
        ensure!(
            row.revision == command.revision
                && (row.source_bucket == bucket || command.action == CommandAction::Remove),
            Error::OperationAborted
        );
        let mut run = r::run(id).fetch_one(&mut *db).await?;
        let mut state = row.state.as_str();
        let mut prefix = String::new();
        match command.action {
            CommandAction::Pause | CommandAction::Remove => {
                ensure!(
                    state != "removing"
                        && (command.action == CommandAction::Remove
                            || !matches!(state, "paused" | "pausing")),
                    Error::OperationAborted
                );
                changes::seal(&mut db, &mut run).await?;
                run.phase = "cancel".into();
                state = if command.action == CommandAction::Remove {
                    "removing"
                } else {
                    "pausing"
                };
            }
            CommandAction::Resume | CommandAction::Retry | CommandAction::Reconcile => {
                let options = &run.options.0;
                transactions::lock_media_scope(&mut db, &authority(options, principal.clone()))
                    .await?;
                ensure!(
                    run.mode == r::mode().fetch_one(&mut *db).await?,
                    Error::OperationAborted
                );
                if options.target_external {
                    let records = super::external_transactions::authorized(
                        &mut db,
                        &options.external_authority(principal.clone()),
                    )
                    .await?;
                    let record = records.first().ok_or(Error::OperationAborted)?;
                    ensure!(
                        connections.len() == 1
                            && connections[0].external_id == record.bucket.id
                            && connections[0].revision == record.bucket.revision,
                        Error::OperationAborted
                    );
                    let space =
                        Location::remote(&record.bucket.connection, &options.target_prefix)?;
                    let Location::Remote { path, .. } = space else {
                        unreachable!()
                    };
                    ensure!(
                        q::target_space(id).fetch_one(&mut *db).await? == path,
                        Error::OperationAborted
                    );
                    connections[0].validate(id)?;
                    super::replication_remote_queries::forget(id)
                        .execute(&mut *db)
                        .await?;
                    r::connection(id, &connections[0]).execute(&mut *db).await?;
                } else {
                    let (storage, revision) = r::profile(options.target_bucket)
                        .fetch_one(&mut *db)
                        .await?;
                    ensure!(
                        connections.is_empty() && storage == run.storage()?,
                        Error::OperationAborted
                    );
                    if revision != run.profile_revision()? {
                        ensure!(
                            state == "paused" && command.action == CommandAction::Resume,
                            Error::OperationAborted
                        );
                        q::profile(revision, id).execute(&mut *db).await?;
                        run.profile_revision = Some(revision);
                    }
                }
                r::actor(id, principal).execute(&mut *db).await?;
                match command.action {
                    CommandAction::Resume => {
                        ensure!(state == "paused", Error::OperationAborted);
                        changes::activate(&mut db, id, options).await?;
                        run.sealed = false;
                        state = "reconciling";
                        reset_scan(&mut run);
                    }
                    CommandAction::Reconcile => {
                        ensure!(state == "idle", Error::OperationAborted);
                        prefix = command.prefix.clone().unwrap_or_default();
                        state = "reconciling";
                        reset_scan(&mut run);
                    }
                    CommandAction::Retry => {
                        ensure!(
                            row.task_state == "failed"
                                || state == "idle" && run.progress.0.conflicts > 0,
                            Error::OperationAborted
                        );
                        if state == "idle" {
                            state = "reconciling";
                            reset_scan(&mut run);
                        } else {
                            prefix = q::scope(id).fetch_one(&mut *db).await?;
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
        ensure!(
            q::command(id, state, &prefix, command.revision)
                .execute(&mut *db)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        r::resume(id).execute(&mut *db).await?;
        r::task_state(id, "queued", None).execute(&mut *db).await?;
        transfer::save(&mut db, &run).await?;
        db.commit().await?;
        Ok(())
    }
}

fn reset_scan(run: &mut Run) {
    run.scanned = false;
    run.cursor_key = None;
    run.scan_end = None;
    run.phase = "scan".into();
}

pub(super) async fn prepare(db: &mut Tx<'_>, run: &mut Run) -> Result<bool> {
    let id = run.task_id;
    let (state, prefix, credit, queued, capacity) = q::work(id).fetch_one(&mut **db).await?;
    match state.as_str() {
        "preparing" => {
            activate(db, id, &run.options.0).await?;
            return Ok(true);
        }
        "idle" => {
            if q::due(id).fetch_one(&mut **db).await? {
                q::transition(id, "reconciling").execute(&mut **db).await?;
                reset_scan(run);
            } else if queued > 0 {
                q::transition(id, "catchup").execute(&mut **db).await?;
                run.phase = "scan".into();
            } else {
                r::task_state(id, "paused", None).execute(&mut **db).await?;
            }
            return Ok(true);
        }
        "paused" => return Err(Error::OperationAborted.into()),
        _ => {}
    }
    if run.phase != "scan" {
        return Ok(false);
    }
    let room = capacity - queued;
    if run.scanned || (queued > 0 && (credit > 0 || room == 0)) {
        if !run.scanned && credit > 0 {
            q::credit(id, credit - 1).execute(&mut **db).await?;
        }
        changes::replay(db, run).await?;
        return Ok(true);
    }
    if run.scan_end.is_none() {
        let limit = crate::model::replication::changes::prefix_limit(&prefix);
        let current = c::upper(run.options.0.source_bucket, &prefix, limit.as_deref())
            .fetch_optional(&mut **db)
            .await?;
        let previous = q::upper(id, &prefix, limit.as_deref())
            .fetch_optional(&mut **db)
            .await?;
        run.scan_end = Some(current.max(previous).unwrap_or_default());
    }
    let end = run.scan_end.as_deref().ok_or(Error::OperationAborted)?;
    let mut keys = std::collections::BTreeMap::new();
    for (key, present) in q::history(
        id,
        run.options.0.source_bucket,
        &prefix,
        run.cursor_key.as_deref(),
        end,
    )
    .fetch_all(&mut **db)
    .await?
    {
        keys.insert(key, (present, None));
    }
    for source in r::scan(
        run.options.0.source_bucket,
        run.cursor_key.as_deref(),
        &prefix,
        end,
    )
    .fetch_all(&mut **db)
    .await?
    {
        keys.insert(source.object_key.clone(), (true, Some(source)));
    }
    run.scanned = keys.is_empty();
    for (key, (present, source)) in keys.into_iter().take(room.min(64) as usize) {
        // A key outside the current source page is not proof of deletion.
        if source.is_some() || !present {
            let snapshot = source.as_ref().map(|value| value.snapshot());
            q::enqueue(
                id,
                &key,
                source.as_ref().map(|value| value.id),
                snapshot.as_ref(),
            )
            .execute(&mut **db)
            .await?;
        }
        run.cursor_key = Some(key);
    }
    q::credit(id, 64).execute(&mut **db).await?;
    Ok(true)
}

pub(super) async fn finish(db: &mut Tx<'_>, run: &mut Run) -> Result<()> {
    let id = run.task_id;
    if run.phase == "cancel" {
        let state = q::state(id).fetch_one(&mut **db).await?;
        super::replication_remote_queries::forget(id)
            .execute(&mut **db)
            .await?;
        if state == "removing" {
            if !run.options.0.target_external {
                let target = run.options.0.target_bucket;
                c::gate(target).fetch_one(&mut **db).await?;
                q::count(target, -1).execute(&mut **db).await?;
            }
            q::forget(id).execute(&mut **db).await?;
            r::task_state(id, "completed", None)
                .execute(&mut **db)
                .await?;
        } else {
            ensure!(state == "pausing", Error::OperationAborted);
            q::transition(id, "paused").execute(&mut **db).await?;
            r::task_state(id, "paused", None).execute(&mut **db).await?;
        }
        run.phase = "cancelled".into();
    } else {
        q::settled(id).execute(&mut **db).await?;
        run.phase = "scan".into();
        r::task_state(id, "paused", None).execute(&mut **db).await?;
    }
    Ok(())
}
