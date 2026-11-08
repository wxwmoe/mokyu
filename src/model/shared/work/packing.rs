use super::*;
use crate::content::pack;
use crate::model::node_pack as wire;
use queries::pack_work as p;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Serialize, Deserialize)]
enum Progress {
    Scan(Option<String>),
    Change {
        pack: Id,
        generation: i64,
    },
    Queue {
        stream: Id,
        generation: i64,
        cursor: i64,
    },
}

#[derive(Serialize, Deserialize)]
enum Rule {
    Combine { before: i64, physical: usize },
    Split { before: i64, minimum: Option<u64> },
}

#[derive(Serialize, Deserialize)]
struct Snapshot {
    revision: i64,
    rows: Vec<Mapping>,
    ready: Vec<Option<Chunk>>,
    progress: Progress,
    rule: Rule,
}

struct Selected {
    rows: Vec<Mapping>,
    groups: Vec<Vec<Mapping>>,
    progress: Progress,
    rule: Rule,
}

async fn advance(
    db: &mut p::Connection,
    task: Execution,
    progress: &Progress,
    processed: usize,
    settings: &pack::Config,
) -> Result<()> {
    let next = match progress {
        Progress::Scan(next) => next.clone(),
        Progress::Change { pack, generation } => {
            p::finish_pack_change(&mut *db, task, *pack, *generation).await?;
            Some(pack.to_string())
        }
        Progress::Queue {
            stream,
            generation,
            cursor,
        } => {
            if *cursor < 0 {
                if let Some(later) = p::stream_repack_after(
                    &mut *db,
                    *stream,
                    config::seconds(&settings.repack_cooldown)? as f64,
                )
                .await?
                {
                    p::defer_pack_queue(&mut *db, task, *stream, *generation, later).await?;
                } else {
                    p::finish_pack_queue(&mut *db, task, *stream, *generation).await?;
                }
            } else {
                p::advance_pack_queue(&mut *db, task, *stream, *generation, *cursor).await?;
            }
            Some(format!("{stream}:{cursor}"))
        }
    };
    p::advance_cursor(db, task, next, processed as i64).await?;
    Ok(())
}

async fn split(
    db: &mut p::Connection,
    id: Id,
    unpack: bool,
    settings: &pack::Config,
    progress: Progress,
) -> Result<Selected> {
    let mut selected = Selected {
        rows: vec![],
        groups: vec![],
        progress,
        rule: Rule::Split {
            before: 0,
            minimum: None,
        },
    };
    if !unpack
        && (p::pack_has_pending(&mut *db, id).await? || p::pack_has_writers(&mut *db, id).await?)
    {
        return Ok(selected);
    }
    selected.rows = p::pack_layout(&mut *db, id).await?;
    if selected.rows.is_empty() {
        return Ok(selected);
    }
    let total = p::pack_raw_size(&mut *db, id).await?;
    let mut dead = total.saturating_sub(
        selected
            .rows
            .iter()
            .map(|r| i64::from(r.chunk.raw_size))
            .sum(),
    ) as u64;
    let maximum = config::bytes(&settings.max_size)?;
    let ids: Vec<_> = selected.rows.iter().map(|r| r.chunk.id).collect();
    let live: BTreeSet<Id> = p::live(&mut *db, &ids).await?.into_iter().collect();
    let alive: Vec<_> = selected
        .rows
        .iter()
        .filter(|r| live.contains(&r.chunk.id))
        .collect();
    dead += selected
        .rows
        .iter()
        .filter(|r| !live.contains(&r.chunk.id))
        .map(|r| r.chunk.raw_size as u64)
        .sum::<u64>();
    let minimum = config::bytes(&settings.reclaim_min_savings_bytes)?;
    let reclaim = !unpack;
    let mut group_size = 0;
    if unpack || dead > 0 {
        for row in alive {
            let size = row.chunk.raw_size as u64;
            if !unpack
                && group_size + size <= maximum
                && let Some(group) = selected.groups.last_mut()
            {
                group.push(row.clone());
                group_size += size;
            } else {
                selected.groups.push(vec![row.clone()]);
                group_size = size;
            }
        }
    }
    if reclaim
        && !selected.groups.is_empty()
        && p::reclaim_estimate(&mut *db, id, &selected.groups).await? < minimum.max(1) as i64
    {
        selected.groups.clear();
    }
    selected.rule = Rule::Split {
        before: p::pack_stored_size(db, id).await?,
        minimum: reclaim.then_some(minimum),
    };
    Ok(selected)
}

async fn combine(
    db: &mut p::Connection,
    task: Execution,
    stream: Id,
    from: i64,
    repack: bool,
    settings: &pack::Config,
    byte_budget: Option<u64>,
) -> Result<(Vec<Mapping>, Vec<Mapping>, i64, Rule)> {
    let rows = p::pack_candidates(
        &mut *db,
        stream,
        from,
        config::seconds(&settings.repack_cooldown)? as f64,
    )
    .await?;
    let maximum = config::bytes(&settings.max_size)?.min(byte_budget.unwrap_or(u64::MAX));
    let (mut group, cursor) = crate::model::packing::select_members(rows, repack, maximum);
    let physical: BTreeSet<_> = group
        .iter()
        .map(|r| (r.pack_id.is_some(), r.pack_id.unwrap_or(r.chunk.id)))
        .collect();
    let mut valid = group.len() >= 2 && physical.len() >= 2;
    let pack_ids: Vec<_> = group
        .iter()
        .filter_map(|r| r.pack_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let members: BTreeSet<_> = group.iter().map(|row| row.chunk.id).collect();
    let mut inputs = group.clone();
    for pack in pack_ids {
        let extra: Vec<_> = p::pack_layout(&mut *db, pack)
            .await?
            .into_iter()
            .filter(|row| !members.contains(&row.chunk.id))
            .collect();
        let ids: Vec<_> = extra.iter().map(|row| row.chunk.id).collect();
        if inputs.len() + extra.len() > crate::content::pack::MAX_MEMBERS
            || (!ids.is_empty() && !p::live(&mut *db, &ids).await?.is_empty())
        {
            valid = false;
            break;
        }
        inputs.extend(extra);
    }
    let ids: Vec<_> = group.iter().map(|r| r.chunk.id).collect();
    if repack {
        let gain = if valid {
            p::repack_benefit(&mut *db, stream, &group).await?
        } else {
            0
        };
        let reason = if !valid {
            "shared_or_contiguous"
        } else if gain <= 0 {
            "no_read_savings"
        } else {
            "fewer_origin_reads"
        };
        queries::record_repack_decision(
            task,
            crate::model::packing::repack_decision(stream, gain, reason, &group),
        )
        .execute(&mut *db)
        .await?;
        valid &= gain > 0;
    }
    let before = if valid {
        p::physical_bytes(db, &ids).await?
    } else {
        0
    };
    if !valid {
        group.clear();
        inputs.clear();
    }
    Ok((
        inputs,
        group,
        cursor,
        Rule::Combine {
            before,
            physical: physical.len(),
        },
    ))
}

async fn flush(db: &mut p::Connection) -> Result<f64> {
    let metrics: config::MetricsSettings =
        serde_json::from_value(queries::runtime_setting("metrics").fetch_one(db).await?)?;
    Ok((2 * config::seconds(&metrics.access_flush_interval)?) as f64)
}

pub(in super::super) async fn reserve_outputs(
    db: &mut p::Connection,
    task: Uuid,
    storage: Uuid,
    batch: &mut wire::Batch,
) -> Result<()> {
    if let Some(key) = batch.key {
        let tag = crate::content::nonce::CHUNK;
        let slots: Vec<_> = batch
            .chunks
            .iter_mut()
            .filter(|c| c.ready.is_none() && c.original.is_none())
            .map(|c| &mut c.nonce)
            .collect();
        if !slots.is_empty() {
            let units = slots.len().div_ceil(crate::content::nonce::UNIT as usize) as u32;
            let (first, last) = reserve_nonce_in(db, key, tag, units).await?;
            ensure!(
                last - first + 1 >= slots.len() as i64,
                "encryption key cannot reserve enough nonce IDs"
            );
            for (offset, nonce) in slots.into_iter().enumerate() {
                *nonce =
                    Some(codec::typed_nonce(batch.created, first + offset as i64, tag)?.to_vec());
            }
        }
    }
    p::prepare_outputs(db, task, storage, batch).await?;
    Ok(())
}

pub(in super::super) async fn prepare_write(
    db: &mut p::Connection,
    task: Uuid,
    storage: Uuid,
    block_key: Option<Uuid>,
    profile: &crate::storage::profile::StorageProfile,
    items: &[crate::model::node_chunk::Item],
) -> Result<Option<wire::Batch>> {
    let created = p::clock(&mut *db).await?;
    let mut batch = wire::Batch {
        algorithm: "none".into(),
        key: block_key,
        created,
        sources: vec![],
        chunks: vec![],
        groups: vec![],
        packs: vec![],
    };
    let mut unique = BTreeSet::new();
    let maximum = config::bytes(&profile.pack_max_size)?;
    let mut start = 0;
    let mut bytes = 0u64;
    for item in items.iter().filter(|item| !item.reused) {
        ensure!(
            !item.chunk.compressed && item.payload_hash.as_slice() == item.chunk.hash,
            Error::NodeOperationConflict
        );
        if !unique.insert(item.chunk.id) {
            continue;
        }
        if bytes + item.chunk.raw_size as u64 > maximum && start < batch.chunks.len() {
            batch.groups.push((start, batch.chunks.len()));
            start = batch.chunks.len();
            bytes = 0;
        }
        bytes += item.chunk.raw_size as u64;
        batch.algorithm = item.chunk.algorithm.clone();
        batch.chunks.push(wire::Target {
            uuid: (item.chunk.id != Id::Uuid(item.chunk.uuid)).then_some(item.chunk.uuid),
            encoding: None,
            id: item.chunk.id,
            size: item.chunk.raw_size,
            hash: item.chunk.hash.clone(),
            previous_size: None,
            original: None,
            compress: true,
            location: Uuid::now_v7(),
            nonce: None,
            ready: None,
        });
    }
    if batch.chunks.is_empty() {
        return Ok(None);
    }
    batch.groups.push((start, batch.chunks.len()));
    let allowed = p::creation_allowed(&mut *db).await?;
    if allowed {
        for &(start, end) in &batch.groups {
            if end - start > 1 {
                batch.packs.push(wire::Attempt {
                    id: None,
                    start,
                    end,
                    location: Uuid::now_v7(),
                    nonce: None,
                });
            }
        }
    }
    reserve_outputs(db, task, storage, &mut batch).await?;
    Ok(Some(batch))
}

pub(super) async fn prepare(
    db: &mut p::Tx<'_>,
    task: Execution,
    detail: &Value,
    profile: &crate::storage::profile::StorageProfile,
    key: &Key,
) -> Result<Option<(work::Plan, Vec<read::BlockKey>)>> {
    let storage = profile.storage_id;
    let policy = detail["policy"].as_str().unwrap_or("unpack");
    ensure!(
        matches!(policy, "pack" | "repack" | "reclaim" | "unpack"),
        Error::NodeOperationConflict
    );
    let unpack = policy == "unpack";
    let settings = profile.pack_config();
    let allow: bool = p::creation_allowed(&mut **db).await?;
    if (matches!(policy, "pack" | "repack") && !settings.enabled) {
        advance(db, task, &Progress::Scan(None), 0, &settings).await?;
        return Ok(None);
    }
    ensure!(
        !unpack || detail["all"] != true || !allow,
        Error::OperationAborted
    );
    ensure!(
        !matches!(policy, "pack" | "repack") || allow,
        Error::OperationAborted
    );
    let cutoff = detail["upper_time"]
        .as_str()
        .map(str::parse::<chrono::DateTime<chrono::Utc>>)
        .transpose()?;
    let automatic = detail["automatic"] == true;
    let flush = flush(db).await?;
    for _ in 0..32 {
        let cursor: Option<String> = p::cursor(&mut **db, task.id).await?;
        let selected = if unpack || policy == "reclaim" {
            let next = if automatic {
                p::next_pack_change(
                    &mut **db,
                    cutoff.context("missing pack cutoff")?,
                    policy,
                    Some(storage),
                )
                .await?
                .map(|(id, generation)| {
                    (
                        id,
                        Progress::Change {
                            pack: id,
                            generation,
                        },
                    )
                })
            } else {
                p::next_pack(
                    &mut **db,
                    cursor.as_deref().unwrap_or("0").parse()?,
                    Id::from_json(&detail["pack_id"])?,
                    cutoff,
                    Some(storage),
                )
                .await?
                .map(|id| (id, Progress::Scan(Some(id.to_string()))))
            };
            if let Some((id, progress)) = next {
                Some(split(db, id, unpack, &settings, progress).await?)
            } else {
                None
            }
        } else {
            let mut generation = None;
            let next = if automatic {
                p::next_pack_queue(
                    &mut **db,
                    cutoff.context("missing pack cutoff")?,
                    policy,
                    Some(storage),
                )
                .await?
                .map(|(id, offset, g)| {
                    generation = Some(g);
                    (id, offset)
                })
            } else {
                let (after, offset) = if let Some(cursor) = &cursor {
                    let (id, offset) = cursor.split_once(':').context("invalid pack cursor")?;
                    (id.parse()?, offset.parse()?)
                } else {
                    (Id::default(), 0)
                };
                let upper = cutoff.context("missing pack snapshot")?;
                if policy == "repack" {
                    p::next_repack_stream(&mut **db, task.id, after, offset, upper, storage).await?
                } else {
                    p::next_pack_stream(&mut **db, after, offset, upper, Some(storage)).await?
                }
                .map(|id| (id, if id == after { offset } else { 0 }))
            };
            if let Some((stream, offset)) = next {
                let budget = profile
                    .pack_byte_budget
                    .as_deref()
                    .map(config::bytes)
                    .transpose()?;
                let (rows, group, cursor, rule) = combine(
                    db,
                    task,
                    stream,
                    offset,
                    policy == "repack",
                    &settings,
                    budget,
                )
                .await?;
                let progress = if let Some(generation) = generation {
                    Progress::Queue {
                        stream,
                        generation,
                        cursor,
                    }
                } else {
                    Progress::Scan(Some(format!("{stream}:{cursor}")))
                };
                Some(Selected {
                    groups: if group.is_empty() {
                        vec![]
                    } else {
                        vec![group]
                    },
                    rows,
                    progress,
                    rule,
                })
            } else {
                None
            }
        };
        let Some(selected) = selected else {
            advance(db, task, &Progress::Scan(None), 0, &settings).await?;
            return Ok(None);
        };
        if selected.groups.is_empty() {
            if unpack && !selected.rows.is_empty() {
                let mut ids: Vec<_> = selected.rows.iter().map(|r| r.chunk.id).collect();
                ids.sort_unstable();
                pin_inputs(db, task.id, &ids).await?;
                commit_pack_outputs_in(db, task, &selected.rows, &[], true, &settings, flush)
                    .await?;
                release_task_inputs(db, task.id).await?;
            }
            advance(db, task, &selected.progress, 0, &settings).await?;
            continue;
        }
        let target = p::storage_key(storage).fetch_optional(&mut **db).await?;
        let (algorithm, block_key) =
            target.map_or(("none".to_owned(), None), |k| (k.algorithm, Some(k.id)));
        let created = p::clock(&mut **db).await?;
        let mut batch = wire::Batch {
            algorithm,
            key: block_key,
            created,
            sources: vec![],
            chunks: vec![],
            groups: vec![],
            packs: vec![],
        };
        let mut ids: Vec<_> = selected.rows.iter().map(|r| r.chunk.id).collect();
        ids.sort_unstable();
        ensure!(
            ids.len() <= pack::MAX_MEMBERS && ids.windows(2).all(|pair| pair[0] != pair[1]),
            Error::NodeWorkLimit
        );
        pin_inputs(db, task.id, &ids).await?;
        let mut blocks = BTreeSet::new();
        blocks.extend(block_key);
        let mut sources = BTreeSet::new();
        let mut encodings: BTreeMap<_, _> = p::encodings(&ids)
            .fetch_all(&mut **db)
            .await?
            .into_iter()
            .map(|c| (c.id, c))
            .collect();
        let mut ready = Vec::new();
        let mut packed_encodings = BTreeMap::new();
        for group in &selected.groups {
            let start = batch.chunks.len();
            for row in group {
                if let Some(id) = row.pack_id {
                    if sources.insert((true, id)) {
                        let pack = p::source_pack(id).fetch_one(&mut **db).await?;
                        let members = p::physical_members(&mut **db, &pack).await?;
                        crate::model::source::manifest_digest(&pack, &members)?;
                        blocks.extend(
                            members
                                .iter()
                                .filter_map(|m| m.position.encoding.block_key_id),
                        );
                        packed_encodings.extend(
                            members
                                .iter()
                                .map(|m| (m.uuid, m.position.encoding.clone())),
                        );
                        batch.sources.push(wire::Source::Pack { pack, members });
                    }
                } else if sources.insert((false, row.chunk.encoding_id)) {
                    blocks.extend(row.chunk.block_key_id);
                    batch.sources.push(wire::Source::Chunk {
                        chunk: row.chunk.clone(),
                    });
                }
                let existing = encodings.remove(&row.chunk.id);
                if let Some(chunk) = &existing {
                    if row.pack_id.is_none() && sources.insert((false, chunk.encoding_id)) {
                        blocks.extend(chunk.block_key_id);
                        batch.sources.push(wire::Source::Chunk {
                            chunk: chunk.clone(),
                        });
                    }
                }
                let (location, nonce, encoded) = if let Some(chunk) = &existing {
                    (
                        chunk.location_id,
                        chunk.nonce.clone(),
                        Some(wire::encoded(
                            i64::from(chunk.stored_size.context("missing ready encoding size")?),
                            chunk.compressed,
                        )?),
                    )
                } else {
                    (Uuid::now_v7(), None, None)
                };
                batch.chunks.push(wire::Target {
                    uuid: matches!(row.chunk.id, Id::Integer(_)).then_some(row.chunk.uuid),
                    encoding: existing
                        .as_ref()
                        .filter(|_| matches!(row.chunk.id, Id::Integer(_)))
                        .map(|c| c.encoding_id),
                    id: row.chunk.id,
                    size: row.chunk.raw_size,
                    hash: row.chunk.hash.clone(),
                    previous_size: row.chunk.stored_size,
                    original: existing
                        .as_ref()
                        .map(pack::frame::Encoding::capture)
                        .transpose()?
                        .or_else(|| packed_encodings.get(&row.chunk.uuid).cloned()),
                    compress: row.chunk.stored_size.is_none() || row.chunk.compressed,
                    location,
                    nonce,
                    ready: encoded,
                });
                ready.push(existing);
            }
            let end = batch.chunks.len();
            batch.groups.push((start, end));
            if allow && !unpack && settings.enabled && end - start > 1 {
                let location = Uuid::now_v7();
                batch.packs.push(wire::Attempt {
                    id: None,
                    start,
                    end,
                    location,
                    nonce: None,
                });
            }
        }
        reserve_outputs(db, task.id, storage, &mut batch).await?;
        let snapshot = Snapshot {
            revision: profile.revision,
            rows: selected.rows,
            ready,
            progress: selected.progress,
            rule: selected.rule,
        };
        p::save_snapshot(&mut **db, task, &serde_json::to_value(snapshot)?).await?;
        let keys = rewrite::keys(db, &blocks.into_iter().collect::<Vec<_>>(), key).await?;
        let plan = work::Plan::Pack {
            source_permits: Vec::new(),
            storage,
            batch: Box::new(batch),
        };
        ensure!(
            serde_json::to_vec(&plan)?.len() <= 3 * 1024 * 1024,
            Error::NodeWorkLimit
        );
        return Ok(Some((plan, keys)));
    }
    Ok(None)
}

pub(in super::super) async fn outputs(
    db: &mut p::Connection,
    storage: Uuid,
    batch: &wire::Batch,
    result: &wire::Encoded,
    ready: &[Option<Chunk>],
    key: &Key,
) -> Result<Vec<PackOutput>> {
    use base64::{Engine, engine::general_purpose::STANDARD};
    ensure!(
        result.chunks.len() == batch.chunks.len()
            && ready.len() == batch.chunks.len()
            && result.packs.len() <= batch.chunks.len() / 2,
        Error::NodeOperationConflict
    );
    let mut key_ids = BTreeSet::new();
    key_ids.extend(batch.key);
    key_ids.extend(
        batch
            .chunks
            .iter()
            .filter_map(|c| c.original.as_ref()?.block_key_id),
    );
    key_ids.extend(ready.iter().flatten().filter_map(|c| c.block_key_id));
    let keys: BTreeMap<_, _> = rewrite::keys(db, &key_ids.into_iter().collect::<Vec<_>>(), key)
        .await?
        .into_iter()
        .map(|k| Ok((k.id, codec::Key::new(&k.algorithm, k.material)?)))
        .collect::<Result<_>>()?;
    let mut covered = BTreeSet::new();
    let mut outputs = Vec::new();
    for (index, value, digest) in &result.packs {
        ensure!(*index < batch.packs.len(), Error::NodeOperationConflict);
        let attempt = &batch.packs[*index];
        let mut pack = batch.pack(storage, *index)?;
        let (size, compressed) = wire::size(*value);
        pack.stored_size = Some(size);
        pack.compressed = compressed;
        pack.digest = Some(
            STANDARD
                .decode(digest)
                .map_err(|_| Error::NodeOperationConflict)?,
        );
        pack.validate(&keys)
            .map_err(|_| Error::NodeOperationConflict)?;
        let mut entries = Vec::with_capacity(attempt.end - attempt.start);
        let mut offset = pack::frame::HEADER.len() as u64;
        for index in attempt.start..attempt.end {
            ensure!(
                covered.insert(index) && result.chunks[index] != 0,
                Error::NodeOperationConflict
            );
            let (size, compressed) = wire::size(result.chunks[index]);
            let mut chunk = batch.chunk(storage, index)?;
            if let Some(original) = &batch.chunks[index].original {
                ensure!(
                    i64::from(original.stored_size) == size && original.compressed == compressed,
                    Error::NodeOperationConflict
                );
            }
            chunk.stored_size = Some(i32::try_from(size)?);
            chunk.compressed = compressed;
            codec::validate_metadata(&chunk, &keys)?;
            let position = pack::frame::Position {
                ordinal: u32::try_from(entries.len())?,
                offset,
                encoding: pack::frame::Encoding::capture(&chunk)?,
            };
            offset = position
                .range(
                    chunk.raw_size,
                    pack.stored_size.unwrap_or_default() as u64,
                    pack.member_count,
                )?
                .end;
            entries.push((chunk.id, pack::frame::Entry::new(&chunk, position)?));
        }
        ensure!(
            Some(i64::try_from(offset)?) == pack.stored_size,
            Error::NodeOperationConflict
        );
        outputs.push(PackOutput {
            entries,
            chunks: batch.chunks[attempt.start..attempt.end]
                .iter()
                .map(|c| c.id)
                .collect(),
            pack: Some(pack),
            independent: None,
        });
    }
    for (index, value) in result.chunks.iter().enumerate() {
        if covered.contains(&index) {
            continue;
        }
        ensure!(
            *value != 0 && covered.insert(index),
            Error::NodeOperationConflict
        );
        let chunk = if let Some(chunk) = &ready[index] {
            ensure!(
                batch.chunks[index].ready == Some(*value),
                Error::NodeOperationConflict
            );
            chunk.clone()
        } else {
            ensure!(
                batch.chunks[index].ready.is_none(),
                Error::NodeOperationConflict
            );
            let mut chunk = batch.chunk(storage, index)?;
            let (size, compressed) = wire::size(*value);
            if let Some(original) = &batch.chunks[index].original {
                ensure!(
                    i64::from(original.stored_size) == size && original.compressed == compressed,
                    Error::NodeOperationConflict
                );
            }
            chunk.stored_size =
                Some(i32::try_from(size).map_err(|_| Error::NodeOperationConflict)?);
            chunk.compressed = compressed;
            codec::validate_metadata(&chunk, &keys).map_err(|_| Error::NodeOperationConflict)?;
            chunk
        };
        outputs.push(PackOutput {
            entries: Vec::new(),
            chunks: vec![chunk.id],
            pack: None,
            independent: Some(chunk),
        });
    }
    ensure!(
        covered.len() == batch.chunks.len(),
        Error::NodeOperationConflict
    );
    Ok(outputs)
}

pub(super) async fn finish(
    db: &mut p::Tx<'_>,
    task: Execution,
    storage: Uuid,
    batch: wire::Batch,
    detail: &Value,
    outcomes: &[work::Outcome],
    key: &Key,
) -> Result<Option<&'static str>> {
    let [outcome] = outcomes else {
        return Err(Error::NodeOperationConflict.into());
    };
    if matches!(outcome, work::Outcome::Retry) {
        return Ok(Some("backend_io_failed"));
    }
    let work::Outcome::Packed(result) = outcome else {
        return Err(Error::NodeOperationConflict.into());
    };
    let snapshot: Snapshot = serde_json::from_value(detail["node_pack"].clone())?;
    ensure!(
        result.chunks.len() == batch.chunks.len()
            && snapshot.ready.len() == batch.chunks.len()
            && result.packs.len() <= pack::MAX_MEMBERS / 2,
        Error::NodeOperationConflict
    );
    let profile = queries::storage_profile(storage)
        .fetch_one(&mut **db)
        .await?;
    if profile.revision != snapshot.revision {
        return Ok(Some("pack_profile_changed"));
    }
    let settings = profile.pack_config();
    let target = p::storage_key(storage).fetch_optional(&mut **db).await?;
    if target.as_ref().map(|k| k.id) != batch.key
        || target.as_ref().map_or("none", |k| k.algorithm.as_str()) != batch.algorithm
    {
        return Ok(Some("pack_key_changed"));
    }
    let allowed: bool = p::creation_allowed(&mut **db).await?;
    if !allowed && !result.packs.is_empty() {
        return Ok(Some("pack_creation_stopped"));
    }
    let ids: Vec<_> = snapshot.rows.iter().map(|r| r.chunk.id).collect();
    if let Err(error) = check_source_inputs(db, task.id, &ids).await {
        if error
            .downcast_ref::<Error>()
            .is_some_and(|e| matches!(e, Error::OperationAborted | Error::SourceUnavailable))
        {
            return Ok(Some("source_changed"));
        }
        return Err(error);
    }
    let outputs = outputs(db, storage, &batch, result, &snapshot.ready, key).await?;
    let after: i64 = outputs.iter().map(PackOutput::size).sum();
    let publish = match &snapshot.rule {
        Rule::Combine { before, physical } => {
            outputs.len() < *physical
                && after <= before + 8 + pack::MAX_MEMBER_TAG as i64 * batch.chunks.len() as i64
        }
        Rule::Split { before, minimum } => {
            let ready: BTreeSet<_> = snapshot.ready.iter().flatten().map(|c| c.id).collect();
            let written: i64 = outputs
                .iter()
                .filter(|out| {
                    !out.independent
                        .as_ref()
                        .is_some_and(|c| ready.contains(&c.id))
                })
                .map(PackOutput::size)
                .sum();
            minimum.is_none_or(|n| before.saturating_sub(written) >= n as i64)
        }
    };
    if publish {
        let flush = flush(db).await?;
        let mut publication = sqlx::Acquire::begin(&mut **db).await?;
        p::record_outputs(&mut publication, task.id, &outputs).await?;
        if let Err(error) = commit_pack_outputs_in(
            &mut publication,
            task,
            &snapshot.rows,
            &outputs,
            !matches!(snapshot.rule, Rule::Combine { .. }),
            &settings,
            flush,
        )
        .await
        {
            publication.rollback().await?;
            let changed = error
                .downcast_ref::<Error>()
                .is_some_and(|e| matches!(e, Error::OperationAborted | Error::SourceUnavailable))
                || [
                    "pack layout changed; retry maintenance",
                    "pack references changed; retry maintenance",
                    "pack reference boundaries changed; retry maintenance",
                    "pack acquired a new reference; retry maintenance",
                ]
                .contains(&error.to_string().as_str());
            if changed {
                return Ok(Some("source_changed"));
            }
            return Err(error);
        }
        publication.commit().await?;
    }
    advance(
        db,
        task,
        &snapshot.progress,
        if publish { batch.chunks.len() } else { 0 },
        &settings,
    )
    .await?;
    p::clear_snapshot(&mut **db, task.id).await?;
    Ok(None)
}
