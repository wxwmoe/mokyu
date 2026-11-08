use crate::{
    app::App,
    authorization::Principal,
    content::codec,
    control::{managed_keys, recovery},
    model::journal::{
        record::Material,
        restore::{Progress, Selection, Unlocked},
    },
    storage::{
        journal::{
            import::{Import, path},
            source,
        },
        marker, model,
    },
};
use anyhow::{Context, Result, ensure};
use futures_util::TryStreamExt;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;

async fn fresh_key(app: &App, job: Uuid) -> Result<Uuid> {
    let name = format!("journal-write-{}", job.simple());
    if let Some(key) = app.managed_keys.resolve_block(&name).await? {
        return Ok(key.id);
    }
    Ok(managed_keys::create_record(
        app,
        &Principal::System,
        managed_keys::ManagedKeyInput {
            name,
            purpose: "block".into(),
            algorithm: Some("aes-256-gcm".into()),
        },
    )
    .await?
    .id)
}

async fn restore(app: &App, id: Uuid, token: Uuid, progress: &mut Progress) -> Result<()> {
    let job = app.journal.recovery(id).await?;
    let mut unlocked = Unlocked::open(&job.source, id, &app.secrets.credential_key)?;
    let Selection {
        project,
        buckets,
        destination,
        exclusive_takeover,
        complete_journal_upload,
    } = job.selection.context("missing recovery selection")?.0;
    let target = job.target_storage.context("missing recovery target")?;
    let origin = unlocked.source.connect()?;
    let source_storage = unlocked.source.connection.id;
    let local = crate::storage::journal::local::Local::new(&unlocked.source.connection)?;
    let mut pin = match &local {
        Some(l) => Some(l.lock(exclusive_takeover).await?),
        None => None,
    };
    let doc = marker::Document::probe(origin.as_ref())
        .await?
        .context("missing recovery marker")?;
    let existing = marker::journal(&doc.value)?.context("journal descriptor disappeared")?;
    ensure!(
        marker::identity(&doc.value)?.storage_id == source_storage
            && (existing == unlocked.source.journal
                || unlocked
                    .claim
                    .as_ref()
                    .is_some_and(|claim| claim.descriptor == existing)),
        "source ownership changed"
    );
    if exclusive_takeover
        && (unlocked.claim.is_none() || local.is_some() && existing == unlocked.source.journal)
    {
        if complete_journal_upload {
            unlocked.cut = unlocked
                .source
                .salvage(origin.as_ref(), &unlocked.cut)
                .await?;
        } else {
            let (_, latest) = unlocked
                .source
                .read_locked(origin.as_ref(), None, None, local.as_ref())
                .await?;
            ensure!(
                latest == unlocked.cut,
                "journal advanced after exclusive preview"
            );
        }
    }
    let selected = buckets.keys().copied().collect::<BTreeSet<_>>();
    let (catalog, cut) = unlocked
        .source
        .read_locked(
            origin.as_ref(),
            Some(&unlocked.cut),
            (!exclusive_takeover).then_some(&selected),
            local.as_ref(),
        )
        .await?;
    if !exclusive_takeover {
        drop(pin.take());
    }
    let mut required = BTreeSet::new();
    for bucket in buckets.keys() {
        required.extend(
            catalog.required_keys(
                catalog
                    .buckets
                    .get(bucket)
                    .context("bucket missing at selected recovery point")?,
            )?,
        );
    }
    // Native physical IDs are unique within a deployment. Check before taking ownership.
    if unlocked.claim.is_none() {
        use crate::model::journal::record::Kind;
        let mut ids: BTreeMap<Kind, BTreeSet<Uuid>> = BTreeMap::new();
        for bucket in &selected {
            for object in catalog.buckets[bucket].objects.values() {
                for span in &object.ranges {
                    match &catalog.materials[&span.material] {
                        Material::Chunk { chunk } => {
                            ids.entry(Kind::Member).or_default().insert(chunk.uuid);
                            ids.entry(Kind::Chunk)
                                .or_default()
                                .insert(chunk.location_id);
                        }
                        Material::Member { pack, chunk, .. } => {
                            ids.entry(Kind::Member).or_default().insert(chunk.uuid);
                            let reference = crate::model::journal::record::Reference {
                                kind: Kind::Pack,
                                id: *pack,
                                member: None,
                            };
                            if let Material::Pack { pack } = catalog
                                .materials
                                .get(&reference)
                                .context("missing recovery pack")?
                            {
                                ids.entry(Kind::Pack).or_default().insert(pack.location_id);
                            }
                        }
                        Material::Raw { source } => {
                            ids.entry(Kind::Raw).or_default().insert(source.location_id);
                        }
                        Material::Fragment { id, .. } => {
                            ids.entry(Kind::Fragment).or_default().insert(*id);
                        }
                        Material::Pack { .. } => anyhow::bail!("invalid standalone pack range"),
                    }
                }
            }
        }
        for (kind, ids) in ids {
            let ids: Vec<_> = ids.into_iter().collect();
            for page in ids.chunks(256) {
                ensure!(
                    !app.journal
                        .recovery_location_conflict(target, kind, page)
                        .await?,
                    "recovery data is already registered on another storage in this deployment"
                );
            }
        }
    }
    for key in &required {
        ensure!(
            !app.journal
                .recovery_key_conflict(&catalog.keys[key])
                .await?,
            "historical key is in use for new encryption"
        );
    }
    if exclusive_takeover {
        if unlocked.claim.is_none() {
            let key = codec::Key::new("aes-256-gcm", unlocked.source.material)?;
            unlocked.claim = Some(crate::storage::journal::history::Claim::freeze(
                &existing,
                source_storage,
                cut.point.clone(),
                &app.secrets.credential_key,
                &key,
                !required.is_empty(),
                doc.revision(),
            )?);
            app.journal
                .recovery_intent(id, token, &unlocked.seal(id, &app.secrets.credential_key)?)
                .await?;
        }
        let claim = unlocked.claim.as_ref().unwrap();
        let descriptor = &claim.descriptor;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("journal-takeover-intent").await;
        let value = if existing == *descriptor {
            claim.verify(origin.as_ref()).await?;
            doc.value
        } else {
            ensure!(
                existing == unlocked.source.journal && claim.revision == doc.revision(),
                "takeover source revision changed"
            );
            claim.publish(origin.as_ref(), local.as_ref()).await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("journal-takeover-history").await;
            let write = match &local {
                Some(local) => {
                    doc.set_local_locked(origin.as_ref(), local, descriptor)
                        .await
                }
                None => doc.set_journal(origin.as_ref(), descriptor).await,
            };
            match write {
                Ok(updated) => updated,
                Err(error) => {
                    let read = marker::Document::read(origin.as_ref())
                        .await?
                        .context("cannot verify takeover")?;
                    if marker::journal(&read.value)?.as_ref() != Some(descriptor) {
                        return Err(error);
                    }
                    read.value
                }
            }
        };
        drop(pin.take());
        let write_key = if required.is_empty() {
            None
        } else {
            Some(fresh_key(app, id).await?)
        };
        #[cfg(feature = "fault-injection")]
        crate::faults::point("journal-recovery-claimed").await;
        recovery::register(app, &unlocked, value, write_key).await?;
        app.journal
            .recovery_adopt(id, token, target, descriptor)
            .await?;
    }
    let row = app
        .storage_records
        .enabled_by_id(target)
        .await?
        .context("destination storage is disabled")?;
    let to = model::Connection::unprotect(&model::Record::from(row), &app.secrets.credential_key)?;
    let destination_store = model::Store::connect(&to, source::options())?.backend;
    marker::verify_store(destination_store.as_ref(), &to.identity_marker).await?;
    if !exclusive_takeover {
        ensure!(
            destination == Some(target) && source_storage != target,
            "invalid copy scope"
        );
        if !required.is_empty() && app.managed_keys.for_storage(target).await?.is_none() {
            let key = fresh_key(app, id).await?;
            let name = app.storage_records.view(target).await?.name;
            crate::control::storage::set_current_key(app, name, Some(key.to_string())).await?;
        }
    }
    let mut keys = BTreeMap::new();
    for kid in required {
        let k = &catalog.keys[&kid];
        let protected = codec::protect(
            &k.material,
            &app.secrets.credential_key,
            format!("managed-key-{kid}").as_bytes(),
        )?;
        app.journal.recovery_key(id, token, k, &protected).await?;
        keys.insert(kid, codec::Key::new(&k.algorithm, k.material)?);
        app.storage.ensure_key_loaded(Some(kid)).await?;
        ensure!(
            app.secrets.key_for_id(Some(kid)).is_some_and(
                |loaded| loaded.algorithm == k.algorithm && loaded.material == k.material
            ),
            "recovery key material differs"
        );
    }
    let source_layout = crate::storage::location::Layout::for_local(local.is_some());
    let mut import = Import {
        source_layout,
        target_layout: crate::storage::location::Layout::for_local(to.r#type == "local"),
        app,
        job: id,
        token,
        target,
        same: exclusive_takeover,
        origin: origin.clone(),
        destination: destination_store,
        client: (to.r#type == "s3")
            .then(|| source::client(&to))
            .transpose()?,
        keys,
        locations: BTreeMap::new(),
    };
    let mut errors = Vec::new();
    for (bucket, name) in &buckets {
        let src = &catalog.buckets[bucket];
        let (_, published, after) = app
            .journal
            .recovery_bucket(
                id,
                token,
                *bucket,
                name,
                src.generation,
                src.sequence,
                exclusive_takeover,
            )
            .await?;
        if !published {
            let result: Result<()> = async {
                for (key, change) in src.objects.range::<String, _>((
                    after
                        .as_ref()
                        .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                    std::ops::Bound::Unbounded,
                )) {
                    ensure!(
                        change.key.as_ref() == Some(key),
                        "recovery object identity changed"
                    );
                    let mut locations = Vec::with_capacity(change.ranges.len());
                    for span in &change.ranges {
                        locations.push(import.location(&catalog, span.material).await?);
                    }
                    app.journal
                        .recovery_item(id, token, change, &locations)
                        .await?;
                }
                app.journal
                    .recovery_publish(id, token, *bucket, project, target)
                    .await?;
                Ok(())
            }
            .await;
            if let Err(e) = result {
                errors.push(format!("{name}: {e}"));
                continue;
            }
        }
        if !progress.recovered_buckets.contains(name) {
            progress.recovered_buckets.push(name.clone());
        }
    }
    ensure!(errors.is_empty(), "{}", errors.join(", "));
    if exclusive_takeover {
        let known = catalog
            .materials
            .values()
            .filter(|m| !matches!(m, Material::Member { .. }))
            .map(|m| path(m, true, source_layout))
            .collect::<Result<BTreeSet<_>>>()?;
        let mut files = origin.list(None);
        let mut candidates = 0;
        while let Some(file) = files.try_next().await? {
            let key = file.location.as_ref();
            if known.contains(key) {
                continue;
            }
            let addr = source_layout.parse_chunk_or_pack(key).or_else(|| {
                key.strip_prefix("objects/")
                    .and_then(|name| source_layout.parse_name(name))
            });
            if let Some(addr) = addr {
                app.journal
                    .recovery_orphan(id, token, addr, key, i64::try_from(file.size)?)
                    .await?;
                candidates += 1;
            }
        }
        progress.candidate_files = candidates;
    }
    Ok(())
}

async fn process(app: &App, id: Uuid) -> Result<()> {
    let token = Uuid::new_v4();
    if !app.journal.recovery_claim(id, token).await? {
        return Ok(());
    }
    let mut progress = app.journal.recovery(id).await?.progress.0;
    let result = {
        let work = restore(app, id, token, &mut progress);
        tokio::pin!(work);
        let mut timer = tokio::time::interval(Duration::from_secs(15));
        timer.tick().await;
        loop {
            tokio::select! {
                result = &mut work => break result,
                _ = timer.tick() => {
                    if !app.journal.recovery_claim(id, token).await? { return Ok(()) }
                }
            }
        }
    };
    let error = result.err().map(|e| {
        e.to_string()
            .chars()
            .filter(|c| !c.is_control())
            .take(512)
            .collect::<String>()
    });
    app.journal
        .recovery_save(
            id,
            token,
            if error.is_some() {
                "failed"
            } else {
                "completed"
            },
            &progress,
            error.as_deref(),
        )
        .await
}

pub(super) async fn run(app: Arc<App>) -> Result<()> {
    let mut expiry = tokio::time::interval(Duration::from_secs(3600));
    loop {
        let wake = app.wake_maintenance.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        if app.writable().is_ok() {
            match app.journal.recovery_jobs().await {
                Ok(ids) => {
                    for id in ids {
                        if let Err(error) = process(&app, id).await {
                            tracing::warn!(job=%id,%error,"metadata recovery interrupted")
                        }
                    }
                }
                Err(error) => tracing::warn!(%error,"cannot list metadata recoveries"),
            }
        }
        tokio::select! {
            _ = wake => (),
            _ = tokio::time::sleep(Duration::from_secs(3)) => (),
            _ = expiry.tick() => {
                if app.writable().is_ok() && let Err(error) = app.journal.expire_recovery_previews().await {
                    tracing::warn!(%error, "cannot expire recovery previews");
                }
            }
        }
    }
}
