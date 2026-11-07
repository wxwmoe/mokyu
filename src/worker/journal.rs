use crate::{
    app::App,
    model::journal::{Ledger, publication},
    storage::journal::remote::Remote,
};
use anyhow::{Result, ensure};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use uuid::Uuid;

async fn scan(app: Arc<App>) -> Result<()> {
    loop {
        let wake = app.wake_maintenance.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        if app.writable().is_ok() {
            match app.journal.seed_page(128).await {
                Ok(true) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Ok(false) => {}
                Err(err) => tracing::warn!(error = %err, "metadata journal scan failed"),
            }
        }
        tokio::select! { _ = wake => (), _ = tokio::time::sleep(Duration::from_secs(5)) => () }
    }
}

async fn step(
    app: &App,
    ledger: &Ledger,
    token: Uuid,
    cache: &mut Option<Arc<Remote>>,
) -> Result<(bool, u64)> {
    let id = ledger.storage_id;
    if app.storage.store_backend(id).is_err() {
        app.storage.refresh_store(id).await?;
    }
    let target = app.storage.store_backend(id)?;
    if cache.as_ref().is_none_or(|r| {
        r.revision != target.revision
            || ledger
                .descriptor
                .as_ref()
                .is_some_and(|desc| r.journal != desc.0)
    }) {
        let remote = Remote::connect(
            app.journal.as_ref(),
            app.storage_records.as_ref(),
            app.storage_profiles.as_ref(),
            &app.secrets,
            ledger,
            token,
        )
        .await?;
        if target.revision != remote.revision {
            app.storage.refresh_store(id).await?;
        }
        *cache = Some(Arc::new(remote));
    }
    let remote = cache.as_ref().unwrap();
    let db = app.journal.as_ref();
    if let Some(file) = db.next_file(id).await? {
        let _permit = app.storage.profile_permit(id, "maintenance").await?;
        let _memory = app
            .storage
            .pack_memory(usize::try_from(file.byte_count)?.saturating_mul(2))
            .await?;
        return Ok((remote.publish(db, &app.storage, id, token, &file).await?, 0));
    }
    let open = match ledger.open_file {
        Some(file) => Some(db.open(id, file).await?),
        None => None,
    };
    if let Some(file) = &open
        && file.mode == "multipart"
        && file.part_cut > 0
    {
        let sent: i64 = db
            .parts(file.id)
            .await?
            .iter()
            .filter(|p| p.etag.is_some())
            .map(|p| p.byte_count)
            .sum();
        if sent < file.part_cut {
            let _permit = app.storage.profile_permit(id, "maintenance").await?;
            let _memory = app
                .storage
                .pack_memory(usize::try_from(file.byte_count)?.saturating_mul(2))
                .await?;
            return Ok((remote.publish(db, &app.storage, id, token, file).await?, 0));
        }
    }
    let mut prepared = false;
    let mut blocked = None;
    let buckets = db.due_buckets(id, ledger.last_bucket, 16).await?;
    let share = 16usize.div_ceil(buckets.len().max(1)) as i64;
    let mut drafts = Vec::new();
    for bucket in buckets {
        match db
            .prepare_page(
                id,
                bucket,
                ledger.local,
                token,
                &remote.format(),
                &app.secrets,
            )
            .await
        {
            Ok(progress) => prepared |= progress,
            Err(error) => {
                let message = error
                    .to_string()
                    .chars()
                    .filter(|c| !c.is_control())
                    .take(512)
                    .collect::<String>();
                db.prepare_error(id, token, bucket, &message).await?;
                blocked = Some(error);
            }
        }
        drafts.extend(db.drafts(bucket, share).await?);
    }
    let decoder = remote.clone();
    let (records, ids) =
        tokio::task::spawn_blocking(move || publication::unpack(&decoder.format(), drafts))
            .await??;
    let known = db.known(id, remote.journal.writer_epoch, &ids).await?;
    let writer = remote.clone();
    let state = ledger.clone();
    let wait = deadline(ledger, open.as_ref());
    let plan = tokio::task::spawn_blocking(move || {
        publication::allocate(&state, open, records, known, &writer.format())
    })
    .await??;
    if !plan.frames.is_empty() {
        db.allocate(id, token, &plan).await?;
        return Ok((true, 0));
    }
    if ledger.flush_requested {
        db.idle(id, token).await?;
    }
    if !prepared && let Some(error) = blocked {
        return Err(error);
    }
    Ok((prepared, wait))
}

fn deadline(ledger: &Ledger, file: Option<&publication::File>) -> u64 {
    let mut us = 5_000_000i64;
    if let Some(f) = file.filter(|_| !ledger.local) {
        let limits = if f.mode == "put" {
            [
                (Some(f.oldest_us), ledger.policy.put_max_wait_seconds),
                (None, None),
            ]
        } else {
            [
                (Some(f.opened_us), ledger.policy.multipart_max_open_seconds),
                (f.part_since_us, ledger.policy.part_max_wait_seconds),
            ]
        };
        for (start, limit) in limits {
            if let (Some(start), Some(limit)) = (start, limit) {
                us = us.min(
                    start
                        .saturating_add(i64::from(limit) * 1_000_000)
                        .saturating_sub(ledger.now_us),
                );
            }
        }
    }
    u64::try_from(us.max(10_000)).unwrap_or(5_000_000)
}

async fn store(
    app: Arc<App>,
    id: Uuid,
    slots: Arc<Semaphore>,
    cache: &mut Option<Arc<Remote>>,
) -> Result<()> {
    loop {
        if app.writable().is_err() {
            return Ok(());
        }
        let wake = app.wake_maintenance.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        let slot = slots.acquire().await?;
        let token = Uuid::new_v4();
        let Some(ledger) = app.journal.claim(id, token).await? else {
            return Ok(());
        };
        let changed = app.wake_local_journal.notified();
        tokio::pin!(changed);
        if ledger.local {
            changed.as_mut().enable();
        }
        let mut renew = tokio::time::interval(Duration::from_secs(15));
        renew.tick().await;
        let result = {
            let work = step(&app, &ledger, token, cache);
            tokio::pin!(work);
            loop {
                tokio::select! {
                    result=&mut work=>break result,
                    _=renew.tick()=>ensure!(app.journal.renew(id,token).await?, "journal lease lost"),
                }
            }
        };
        let message = result.as_ref().err().map(|e| {
            e.to_string()
                .chars()
                .filter(|c| !c.is_control())
                .take(512)
                .collect::<String>()
        });
        app.journal.release(id, token, message.as_deref()).await?;
        drop(slot);
        let (progress, wait) = result?;
        if progress {
            tokio::task::yield_now().await;
            continue;
        }
        if ledger.open_file.is_none() && !ledger.flush_requested {
            return Ok(());
        }
        tokio::select! { _=wake=>(), _=changed, if ledger.local => (), _=tokio::time::sleep(Duration::from_micros(wait))=>() }
    }
}

async fn publish(app: Arc<App>) -> Result<()> {
    let slots = Arc::new(Semaphore::new(app.budget.cpu_jobs.clamp(1, 4)));
    let mut active = HashMap::new();
    let mut jobs = tokio::task::JoinSet::new();
    let remotes = Arc::new(std::sync::Mutex::new(HashMap::new()));
    loop {
        let wake = app.wake_maintenance.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        while let Some(result) = jobs.try_join_next() {
            if let Err(error) = result {
                tracing::warn!(%error,"journal worker interrupted");
            }
        }
        active.retain(|_, job: &mut tokio::task::AbortHandle| !job.is_finished());
        if app.writable().is_ok() {
            match app.journal.sources().await {
                Ok(ids) => {
                    for id in ids {
                        active.entry(id).or_insert_with(|| {
                        let app=app.clone();
                        let slots=slots.clone();
                        let remotes=remotes.clone();
                        jobs.spawn(async move {
                            let mut remote=remotes.lock().unwrap().remove(&id);
                            match store(app,id,slots,&mut remote).await {
                                Ok(())=>if let Some(remote)=remote {remotes.lock().unwrap().insert(id,remote);},
                                Err(error)=>tracing::warn!(storage=%id,%error,"metadata journal export stopped"),
                            }
                        })
                    });
                    }
                }
                Err(error) => tracing::warn!(%error,"cannot list metadata journals"),
            }
        }
        tokio::select! { _=wake=>(), _=tokio::time::sleep(Duration::from_secs(5))=>() }
    }
}

pub async fn run(app: Arc<App>) -> Result<()> {
    tokio::try_join!(
        scan(app.clone()),
        super::journal_restore::run(app.clone()),
        super::journal_snapshots::run(app.clone()),
        publish(app)
    )?;
    Ok(())
}
