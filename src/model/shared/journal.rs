use super::queries::{journal as q, pack_work::Connection};
use anyhow::{Result, ensure};
use uuid::Uuid;

async fn control(db: &mut Connection, bucket: Uuid, operation: &str) -> Result<()> {
    ensure!(
        q::advance(bucket).execute(&mut *db).await?.rows_affected() == 1,
        "missing journal bucket"
    );
    ensure!(
        q::control(bucket, operation)
            .execute(&mut *db)
            .await?
            .rows_affected()
            == 1,
        "lost journal boundary"
    );
    Ok(())
}

pub(super) async fn configure(db: &mut Connection, bucket: Uuid, enabled: bool) -> Result<()> {
    // Publications take a shared gate until commit; the switch fences them before changing capture.
    let mode = q::lock_mode(bucket).fetch_one(&mut *db).await?;
    if (mode == 1) == enabled {
        return Ok(());
    }
    let operation = if enabled {
        ensure!(
            q::supported(bucket).fetch_one(&mut *db).await?,
            s3s::s3_error!(InvalidArgument, "metadata journal requires managed storage")
        );
        if mode == 0 {
            q::create(bucket).execute(&mut *db).await?;
            "start"
        } else if q::restart_needed(bucket).fetch_one(&mut *db).await? {
            q::restart(bucket).execute(&mut *db).await?;
            "start"
        } else {
            "resume"
        }
    } else {
        "pause"
    };
    control(db, bucket, operation).await?;
    q::set_mode(bucket, if enabled { 1 } else { 2 })
        .execute(&mut *db)
        .await?;
    Ok(())
}

pub(super) async fn seed(db: &mut Connection, bucket: Uuid, limit: i64) -> Result<bool> {
    ensure!((1..=1024).contains(&limit), "invalid journal scan limit");
    if q::lock_mode(bucket).fetch_optional(&mut *db).await? != Some(1)
        || !q::scanning(bucket).fetch_one(&mut *db).await?
    {
        return Ok(false);
    }
    let count = i64::try_from(
        q::seed(bucket, limit)
            .execute(&mut *db)
            .await?
            .rows_affected(),
    )?;
    if count > 0 {
        q::scanned(bucket, count).execute(&mut *db).await?;
    }
    if count < limit {
        q::finish(bucket).execute(&mut *db).await?;
        control(db, bucket, "ready").await?;
    }
    Ok(true)
}

pub(super) async fn allocate(
    db: &mut Connection,
    storage: Uuid,
    token: Uuid,
    plan: &crate::model::journal::publication::Allocation,
) -> Result<()> {
    use anyhow::Context;
    q::fence(storage, token).fetch_one(&mut *db).await?;
    let before = q::ledger(storage).fetch_one(&mut *db).await?;
    ensure!(
        before.revision == plan.revision
            && before.allocated_seq == plan.base
            && before.allocated_digest == plan.base_digest
            && before.open_file == plan.base_open,
        "journal allocation changed"
    );
    let epoch = before
        .descriptor
        .context("journal descriptor missing")?
        .writer_epoch;
    for file in &plan.files {
        ensure!(
            file.storage_id == storage && file.epoch == epoch,
            "journal file ownership changed"
        );
        if Some(file.id) == before.open_file {
            ensure!(
                q::update_file(file)
                    .execute(&mut *db)
                    .await?
                    .rows_affected()
                    == 1,
                "journal file was sealed"
            );
        } else {
            q::insert_file(file).execute(&mut *db).await?;
        }
    }
    for (file, offset, bytes) in &plan.frames {
        q::frame(*file, *offset, bytes).execute(&mut *db).await?;
    }
    for member in &plan.members {
        q::member(member).execute(&mut *db).await?;
    }
    for (id, hash, file) in &plan.dictionary {
        q::dictionary(storage, epoch, id, hash, *file)
            .execute(&mut *db)
            .await?;
    }
    for page in &plan.pages {
        ensure!(
            q::assign(*page).execute(&mut *db).await?.rows_affected() == 1,
            "journal page was already assigned"
        );
    }
    q::allocated(storage, plan).execute(&mut *db).await?;
    Ok(())
}

pub(super) async fn part(
    db: &mut Connection,
    storage: Uuid,
    token: Uuid,
    part: &crate::model::journal::publication::Part,
) -> Result<()> {
    q::fence(storage, token).fetch_one(&mut *db).await?;
    let file = q::file(storage, part.file_id).fetch_one(&mut *db).await?;
    ensure!(
        file.upload_id.is_some()
            && matches!(file.state.as_str(), "open" | "sealed")
            && (1..=10000).contains(&part.number)
            && part.digest.len() == 32
            && part.byte_offset >= 0
            && part.byte_count > 0
            && part
                .byte_offset
                .checked_add(part.byte_count)
                .is_some_and(|end| end <= file.byte_count),
        "invalid journal part receipt"
    );
    if let Some(old) = q::part_row(file.id, part.number)
        .fetch_optional(&mut *db)
        .await?
    {
        ensure!(
            old.byte_offset == part.byte_offset
                && old.byte_count == part.byte_count
                && old.digest == part.digest
                && old
                    .etag
                    .as_ref()
                    .is_none_or(|v| part.etag.as_ref() == Some(v)),
            "journal part changed"
        );
        if old.etag.is_none() && part.etag.is_some() {
            q::part_etag(part).execute(&mut *db).await?;
        }
    } else {
        q::insert_part(part).execute(&mut *db).await?;
    }
    if part.etag.is_some() {
        q::part_members(part).execute(&mut *db).await?;
    }
    Ok(())
}

pub(super) async fn acknowledge(
    db: &mut Connection,
    storage: Uuid,
    token: Uuid,
    file: &crate::model::journal::publication::File,
    etag: Option<&str>,
    version: Option<&str>,
) -> Result<()> {
    q::fence(storage, token).fetch_one(&mut *db).await?;
    let ledger = q::ledger(storage).fetch_one(&mut *db).await?;
    let actual = q::file(storage, file.id).fetch_one(&mut *db).await?;
    ensure!(
        actual.digest == file.digest && actual.last_seq == file.last_seq,
        "journal receipt disagrees"
    );
    if actual.state == "published" && ledger.emitted_seq >= actual.last_seq {
        return Ok(());
    }
    let start = q::pending_offset(file.id)
        .fetch_one(&mut *db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing journal frames"))?;
    let next = ledger.emitted_seq.checked_add(1);
    ensure!(
        if ledger.local && start > 0 {
            matches!(actual.state.as_str(), "open" | "sealed")
                && actual.first_seq <= ledger.emitted_seq
                && actual.last_seq >= ledger.emitted_seq
        } else {
            (actual.state == "sealed" || ledger.local && actual.state == "open")
                && next == Some(actual.first_seq)
        },
        "out-of-order journal receipt"
    );
    if actual.state == "sealed" {
        ensure!(
            q::published(file.id, etag, version)
                .execute(&mut *db)
                .await?
                .rows_affected()
                == 1,
            "journal publication changed"
        );
    }
    for (bucket, seq, generation, previous) in q::watermarks(file.id).fetch_all(&mut *db).await? {
        q::watermark(bucket, seq, generation)
            .execute(&mut *db)
            .await?;
        q::release_streams(bucket, previous, seq)
            .execute(&mut *db)
            .await?;
    }
    q::emitted(&actual).execute(&mut *db).await?;
    super::queries::journal_snapshots::bucket_log(actual.id, actual.id, 1, start)
        .execute(&mut *db)
        .await?;
    super::queries::journal_snapshots::count_log(
        actual.byte_count - start,
        actual.id,
        actual.byte_count - start,
        i64::try_from(ledger.policy.snapshot_trigger_bytes)?,
        storage,
    )
    .execute(&mut *db)
    .await?;
    q::drop_frames(file.id).execute(&mut *db).await?;
    q::apply_policy(storage).execute(&mut *db).await?;
    q::finish_flush(storage).execute(&mut *db).await?;
    Ok(())
}

pub(super) async fn prepare(
    db: &mut Connection,
    storage: Uuid,
    bucket: Uuid,
    local: bool,
    token: Uuid,
    format: &crate::storage::journal::Format<'_>,
    secrets: &crate::config::Secrets,
) -> Result<bool> {
    use super::queries;
    use crate::model::journal::{
        Cursor,
        record::{BlockKey, Change, Operation, WrappedKey, add_segment},
    };
    use crate::storage::journal::{
        FrameType, Sequence,
        batch::{Record, range_digest},
        draft_path,
    };
    use anyhow::Context;
    use std::collections::{BTreeMap, BTreeSet};

    q::fence(storage, token).fetch_one(&mut *db).await?;
    q::rotate(storage, bucket).execute(&mut *db).await?;
    let Some(event) = q::event(bucket).fetch_optional(&mut *db).await? else {
        return Ok(false);
    };
    let format = &crate::storage::journal::Format {
        encrypted: event.encrypt_frames,
        ..*format
    };
    let mut prior = q::cursor(bucket, event.seq)
        .fetch_optional(&mut *db)
        .await?;
    let stream = match event.stream_id {
        Some(id) => Some(q::stream(id).fetch_one(&mut *db).await?),
        None => None,
    };
    let revision = stream.as_ref().map(|s| s.payload_revision);
    if prior
        .as_ref()
        .is_some_and(|p| p.source_revision != revision)
    {
        // Incomplete pages have never been assigned to a published file.
        q::clear_drafts(bucket, event.seq).execute(&mut *db).await?;
        q::clear_holds(bucket, event.seq).execute(&mut *db).await?;
        prior = None;
    }
    let mut state = Cursor {
        page: prior.as_ref().map_or(Ok(0), |p| {
            p.page.checked_add(1).context("too many journal pages")
        })?,
        end_offset: prior.as_ref().map_or(0, |p| p.end_offset),
        range_count: prior.as_ref().map_or(0, |p| p.range_count),
        range_digest: prior.map_or_else(|| vec![0; 32], |p| p.range_digest),
        source_revision: revision,
        final_page: true,
    };
    let mut change = Change {
        bucket: event.bucket_id,
        name: event.bucket_name,
        generation: event.generation,
        seq: event.seq,
        operation: Operation::parse(&event.operation)?,
        key: event.object_key,
        attributes: event.attributes.map(|v| v.0),
        ranges: Vec::new(),
    };
    let mut batch = Vec::new();
    if let Some(stream) = stream {
        let attrs = change
            .attributes
            .as_mut()
            .context("missing journal object attributes")?;
        ensure!(
            change.operation == Operation::Put
                && stream.bucket_id == bucket
                && Some(&stream.object_key) == change.key.as_ref()
                && stream.size == attrs.size
                && stream.etag == attrs.etag,
            "journal object identity changed"
        );
        attrs.payload.clone_from(&stream.payload_type);
        let start = state.end_offset;
        if start < stream.size {
            let (raw, extents) = if stream.payload_type == "raw" {
                (
                    queries::node_read_raw(stream.id, start, stream.size, 128)
                        .fetch_all(&mut *db)
                        .await?
                        .into_iter()
                        .map(|r| (stream.id, r))
                        .collect(),
                    Vec::new(),
                )
            } else {
                (
                    Vec::new(),
                    queries::node_read_extents(stream.id, start, stream.size, 128)
                        .fetch_all(&mut *db)
                        .await?,
                )
            };
            let mapping = queries::sources::Sources::load(&mut *db, raw, extents)
                .await?
                .mapping(Uuid::nil(), &stream, start, stream.size, local)?;
            let mut materials = BTreeMap::new();
            let mut ranges = Vec::new();
            for segment in mapping.segments {
                add_segment(storage, segment, &mut ranges, &mut materials)?;
            }
            let mut keys = BTreeSet::new();
            for material in materials.into_values() {
                if let Some(hold) = material.hold()? {
                    if let Some(id) = hold.key {
                        keys.insert(id);
                    }
                    q::hold(bucket, event.seq, &hold).execute(&mut *db).await?;
                }
                batch.push(Record::Material(material));
            }
            for id in keys {
                let (name, algorithm, protected, fingerprint) =
                    q::key_material(id).fetch_one(&mut *db).await?;
                let clear = crate::content::codec::unprotect_bytes(
                    &protected,
                    &secrets.credential_key,
                    format!("managed-key-{id}").as_bytes(),
                )?;
                ensure!(
                    blake3::hash(&clear).as_bytes().as_slice() == fingerprint,
                    "journal key fingerprint mismatch"
                );
                let material = clear
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("invalid journal key length"))?;
                batch.push(Record::Key(WrappedKey::seal(
                    &BlockKey {
                        id,
                        name,
                        algorithm,
                        material,
                    },
                    format,
                )?));
            }
            state.range_digest =
                range_digest(state.range_digest.as_slice().try_into()?, &ranges)?.to_vec();
            state.range_count = state
                .range_count
                .checked_add(i64::try_from(ranges.len())?)
                .context("too many journal ranges")?;
            state.end_offset = mapping.end;
            batch.push(Record::Ranges {
                bucket,
                generation: change.generation,
                seq: event.seq,
                offset: start,
                ranges,
            });
        }
        state.final_page = state.end_offset == stream.size;
        if local && state.final_page && stream.size > 0 {
            ensure!(
                !q::unsynced(bucket, event.seq).fetch_one(&mut *db).await?,
                "journal payload is waiting for local disk sync"
            );
        }
    }
    if state.final_page {
        batch.push(Record::Change {
            value: change,
            range_count: u64::try_from(state.range_count)?,
            range_digest: state.range_digest.as_slice().try_into()?,
        });
    }
    let path = draft_path(bucket, event.seq, state.page);
    let page = u64::try_from(state.page)? + 1;
    let protected = format.encode(
        FrameType::Draft,
        &path,
        Sequence {
            first: page,
            last: page,
            previous: [0; 32],
        },
        &serde_json::to_vec(&batch)?,
    )?;
    q::save_draft(bucket, event.seq, &state, &protected)
        .execute(&mut *db)
        .await?;
    Ok(true)
}
