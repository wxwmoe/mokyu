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
    Range {
        pack: Id,
        report: Value,
    },
}

#[derive(Serialize, Deserialize)]
enum Rule {
    Combine {
        before: i64,
        physical: usize,
    },
    Split {
        before: i64,
        minimum: Option<u64>,
    },
    Range {
        partial: i64,
        observed: i64,
        full: i64,
        before: i64,
        hits: BTreeMap<Id, i64>,
    },
}

impl Rule {
    fn benefit(&self, outputs: &[(Vec<Id>, i64)]) -> Option<crate::worker::range::Benefit> {
        let Self::Range {
            partial,
            observed,
            full,
            before,
            hits,
        } = self
        else {
            return None;
        };
        Some(crate::worker::range::benefit(
            *partial, *observed, *full, *before, hits, outputs,
        ))
    }
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
    db: &mut PgConnection,
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
        Progress::Range { pack, report } => {
            queries::mark_range_checked(*pack).execute(&mut *db).await?;
            queries::record_range_result(
                task.id,
                pack.to_string(),
                processed as i64,
                report.clone(),
            )
            .execute(db)
            .await?;
            return Ok(());
        }
    };
    sqlx::query("UPDATE tasks SET cursor=$2,processed=processed+$3,state=CASE WHEN $2::text IS NULL THEN 'completed' ELSE state END,updated_at=clock_timestamp() WHERE id=$1 AND lease_token=$4 AND state='running'")
        .bind(task.id).bind(next).bind(processed as i64).bind(task.token).execute(db).await?;
    Ok(())
}

async fn split(
    db: &mut PgConnection,
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
    let live: BTreeSet<Id> =
        sqlx::query_scalar("SELECT DISTINCT chunk_id FROM extents WHERE chunk_id=ANY($1)")
            .bind(&ids)
            .fetch_all(&mut *db)
            .await?
            .into_iter()
            .collect();
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
    let boundaries = if unpack {
        vec![false; alive.len().saturating_sub(1)]
    } else {
        p::reference_boundaries(&mut *db, &alive).await?
    };
    let mut reference_split = false;
    let mut group_size = 0;
    for (index, row) in alive.into_iter().enumerate() {
        let join = if let Some(group) = selected.groups.last() {
            let last = group.last().unwrap();
            let same = boundaries[index - 1];
            reference_split |= !same;
            !unpack
                && same
                && last.offset_bytes + i64::from(last.chunk.raw_size) == row.offset_bytes
                && group_size + row.chunk.raw_size as u64 <= maximum
        } else {
            false
        };
        if join {
            selected.groups.last_mut().unwrap().push(row.clone());
            group_size += row.chunk.raw_size as u64;
        } else {
            selected.groups.push(vec![row.clone()]);
            group_size = row.chunk.raw_size as u64;
        }
    }
    let reclaim = !unpack && !reference_split && dead > 0;
    let minimum = config::bytes(&settings.reclaim_min_savings_bytes)?;
    if (!unpack
        && dead == 0
        && selected.groups.len() == 1
        && selected.groups[0].len() == selected.rows.len())
        || (reclaim && dead < minimum)
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
    db: &mut PgConnection,
    stream: Id,
    from: i64,
    repack: bool,
    settings: &pack::Config,
    byte_budget: Option<u64>,
    flush: f64,
) -> Result<(Vec<Mapping>, i64, Rule)> {
    p::defer_range_repack(
        &mut *db,
        stream,
        config::seconds(&settings.range_repack_window)? as f64,
        flush,
        config::seconds(&settings.range_repack_retry_interval)? as f64,
        from,
    )
    .await?;
    let rows = p::pack_candidates(
        &mut *db,
        stream,
        from,
        config::seconds(&settings.repack_cooldown)? as f64,
    )
    .await?;
    let mut cursor = rows.first().map_or(-1, |r| r.row.offset_bytes);
    let mut rows = rows.into_iter().peekable();
    let mut group = Vec::<Mapping>::new();
    let mut size = 0u64;
    let maximum = config::bytes(&settings.max_size)?.min(byte_budget.unwrap_or(u64::MAX));
    while let Some(first) = rows.next() {
        let mut unit = vec![first];
        if let Some(pack) = unit[0].row.pack_id {
            while rows.peek().is_some_and(|r| r.row.pack_id == Some(pack)) {
                unit.push(rows.next().unwrap());
            }
        }
        let head = &unit[0];
        let end = unit.last().unwrap().row.offset_bytes
            + i64::from(unit.last().unwrap().row.chunk.raw_size);
        let length = unit
            .iter()
            .map(|r| r.row.chunk.raw_size as u64)
            .sum::<u64>();
        let valid = unit.len() == head.unit_count as usize
            && unit.iter().all(|r| r.eligible)
            && (repack || head.row.pack_id.is_none())
            && unit.windows(2).all(|r| {
                r[0].row.offset_bytes + i64::from(r[0].row.chunk.raw_size) == r[1].row.offset_bytes
            });
        if !valid
            || head.row.offset_bytes != cursor
            || group.first().is_some_and(|r| {
                r.chunk.algorithm != head.row.chunk.algorithm
                    || r.chunk.block_key_id != head.row.chunk.block_key_id
            })
            || size + length > maximum
            || group.len() + unit.len() > pack::MAX_MEMBERS
        {
            if !group.is_empty() {
                break;
            }
            cursor = end;
            continue;
        }
        size += length;
        cursor = end;
        group.extend(unit.into_iter().map(|r| r.row));
    }
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
    let counts: Vec<(Id,i64)> = sqlx::query_as("SELECT pack_id,count(*) FROM chunks WHERE pack_id=ANY($1) AND state='ready' GROUP BY pack_id")
        .bind(&pack_ids).fetch_all(&mut *db).await?;
    valid &= counts.len() == pack_ids.len()
        && counts.into_iter().all(|(id, count)| {
            count as usize == group.iter().filter(|r| r.pack_id == Some(id)).count()
        });
    let ids: Vec<_> = group.iter().map(|r| r.chunk.id).collect();
    let before = if valid {
        p::physical_bytes(db, &ids).await?
    } else {
        0
    };
    if !valid {
        group.clear();
    }
    Ok((
        group,
        cursor,
        Rule::Combine {
            before,
            physical: physical.len(),
        },
    ))
}

async fn range(
    db: &mut PgConnection,
    id: Id,
    settings: &pack::Config,
    allow_pack: bool,
) -> Result<Selected> {
    let mut report = json!({"pack_id":id.to_string(),"applied":false});
    let mut selected = Selected {
        rows: vec![],
        groups: vec![],
        progress: Progress::Range {
            pack: id,
            report: Value::Null,
        },
        rule: Rule::Split {
            before: 0,
            minimum: None,
        },
    };
    if p::range_pack_blocked(&mut *db, id).await? {
        report["reason"] = json!("active_upload");
    } else {
        let window = config::seconds(&settings.range_window)? as f64;
        let (partial, observed, full) = p::pack_range_totals(&mut *db, id, window).await?;
        let hits: BTreeMap<_, _> = p::pack_member_hits(&mut *db, id, window)
            .await?
            .into_iter()
            .collect();
        let (before, raw, count) = p::pack_dimensions(&mut *db, id).await?;
        selected.rows = p::range_pack_layout(&mut *db, id).await?;
        if selected.rows.len() != count as usize
            || selected
                .rows
                .iter()
                .map(|r| i64::from(r.chunk.raw_size))
                .sum::<i64>()
                != raw
            || hits.values().sum::<i64>() < partial
            || partial < i64::from(settings.range_min_downloads)
            || observed == 0
        {
            report["reason"] = json!("incomplete_evidence_or_layout");
        } else {
            let references: Vec<_> = selected.rows.iter().collect();
            let boundaries = p::reference_boundaries(&mut *db, &references).await?;
            let mut group_size = 0;
            for (index, row) in selected.rows.iter().enumerate() {
                let join = if let Some(group) = selected.groups.last() {
                    let last = group.last().unwrap();
                    allow_pack
                        && settings.enabled
                        && hits.contains_key(&last.chunk.id) == hits.contains_key(&row.chunk.id)
                        && last.offset_bytes + i64::from(last.chunk.raw_size) == row.offset_bytes
                        && group_size + row.chunk.raw_size as u64
                            <= config::bytes(&settings.max_size)?
                        && boundaries[index - 1]
                } else {
                    false
                };
                if join {
                    selected.groups.last_mut().unwrap().push(row.clone());
                    group_size += row.chunk.raw_size as u64;
                } else {
                    selected.groups.push(vec![row.clone()]);
                    group_size = row.chunk.raw_size as u64;
                }
            }
            selected.rule = Rule::Range {
                partial,
                observed,
                full,
                before,
                hits,
            };
            if selected.groups.len() < 2 {
                selected.groups.clear();
                report["reason"] = json!("no_smaller_contiguous_layout");
            } else {
                let sources: Vec<_> = selected
                    .groups
                    .iter()
                    .map(|g| {
                        (
                            g.iter().map(|r| r.chunk.id).collect(),
                            g.iter()
                                .map(|r| {
                                    i64::from(r.chunk.stored_size.unwrap_or(r.chunk.raw_size + 16))
                                })
                                .sum::<i64>()
                                + if g.len() > 1 {
                                    12 + pack::MEMBER_SIZE as i64 * g.len() as i64
                                } else {
                                    0
                                },
                        )
                    })
                    .collect();
                let benefit = selected.rule.benefit(&sources).unwrap();
                report["partial_downloads"] = json!(partial);
                report["benefit"] = serde_json::to_value(&benefit)?;
                if !benefit.worthwhile(settings)? {
                    selected.groups.clear();
                    report["reason"] = json!("insufficient_net_savings");
                }
            }
        }
    }
    selected.progress = Progress::Range { pack: id, report };
    Ok(selected)
}

async fn flush(db: &mut PgConnection) -> Result<f64> {
    let metrics: config::MetricsSettings =
        serde_json::from_value(queries::runtime_setting("metrics").fetch_one(db).await?)?;
    Ok((2 * config::seconds(&metrics.access_flush_interval)?) as f64)
}

pub(super) async fn prepare(
    db: &mut Transaction<'_, Postgres>,
    task: Execution,
    detail: &Value,
    profile: &crate::storage::profile::StorageProfile,
    key: &Key,
) -> Result<Option<(work::Plan, Vec<read::BlockKey>)>> {
    let storage = profile.storage_id;
    let policy = detail["policy"].as_str().unwrap_or("unpack");
    ensure!(
        matches!(
            policy,
            "pack" | "repack" | "reuse" | "reclaim" | "range" | "unpack"
        ),
        Error::NodeOperationConflict
    );
    let unpack = policy == "unpack";
    let settings = profile.pack_config();
    let allow: bool =
        sqlx::query_scalar("SELECT NOT pack_creation_paused FROM mokyu_meta WHERE singleton")
            .fetch_one(&mut **db)
            .await?;
    if (matches!(policy, "pack" | "repack") && !settings.enabled)
        || (policy == "range" && !settings.range_optimization)
    {
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
        let cursor: Option<String> = sqlx::query_scalar("SELECT cursor FROM tasks WHERE id=$1")
            .bind(task.id)
            .fetch_one(&mut **db)
            .await?;
        let selected = if policy == "range" {
            if let Some(id) = p::next_range_pack(
                &mut **db,
                storage,
                cutoff.context("missing Range snapshot")?,
                config::seconds(&settings.range_window)? as f64,
                i64::from(settings.range_min_downloads),
            )
            .await?
            {
                Some(range(db, id, &settings, allow).await?)
            } else {
                None
            }
        } else if unpack || matches!(policy, "reuse" | "reclaim") {
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
                p::next_pack_stream(
                    &mut **db,
                    after,
                    offset,
                    cutoff.context("missing pack snapshot")?,
                    Some(storage),
                )
                .await?
                .map(|id| (id, if id == after { offset } else { 0 }))
            };
            if let Some((stream, offset)) = next {
                let budget = profile
                    .pack_byte_budget
                    .as_deref()
                    .map(config::bytes)
                    .transpose()?;
                let (rows, cursor, rule) = combine(
                    db,
                    stream,
                    offset,
                    policy == "repack",
                    &settings,
                    budget,
                    flush,
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
                    groups: if rows.is_empty() {
                        vec![]
                    } else {
                        vec![rows.clone()]
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
        let target = queries::storage_key(storage)
            .fetch_optional(&mut **db)
            .await?;
        let (algorithm, block_key) =
            target.map_or(("none".to_owned(), None), |k| (k.algorithm, Some(k.id)));
        let created = queries::node_clock().fetch_one(&mut **db).await?;
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
        let mut encodings: BTreeMap<_, _> = queries::node_read_encodings(&ids)
            .fetch_all(&mut **db)
            .await?
            .into_iter()
            .map(|c| (c.id, c))
            .collect();
        let mut ready = Vec::new();
        for group in &selected.groups {
            let start = batch.chunks.len();
            for row in group {
                if let Some(id) = row.pack_id {
                    if sources.insert((true, id)) {
                        let pack = queries::rewrite_pack(id).fetch_one(&mut **db).await?;
                        let members = q::members(id).fetch_all(&mut **db).await?;
                        crate::model::source::manifest_digest(&pack, &members)?;
                        blocks.extend(pack.block_key_id);
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
                    id: row.chunk.id,
                    size: row.chunk.raw_size,
                    hash: row.chunk.hash.clone(),
                    previous_size: row.chunk.stored_size,
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
                let middle = start + (end - start) / 2;
                for (start, end) in [(start, end), (start, middle), (middle, end)] {
                    if end - start < 2 {
                        continue;
                    }
                    let location = Uuid::now_v7();
                    batch.packs.push(wire::Attempt {
                        start,
                        end,
                        location,
                        nonce: None,
                    });
                }
            }
        }
        if let Some(key) = block_key {
            for tag in [crate::content::nonce::CHUNK, crate::content::nonce::PACK] {
                let slots: Vec<_> = if tag == crate::content::nonce::CHUNK {
                    batch
                        .chunks
                        .iter_mut()
                        .filter(|c| c.ready.is_none())
                        .map(|c| &mut c.nonce)
                        .collect()
                } else {
                    batch.packs.iter_mut().map(|p| &mut p.nonce).collect()
                };
                if !slots.is_empty() {
                    let units = slots.len().div_ceil(crate::content::nonce::UNIT as usize) as u32;
                    let (first, last) = reserve_nonce_in(db, key, tag, units).await?;
                    ensure!(
                        last - first + 1 >= slots.len() as i64,
                        "encryption key cannot reserve enough nonce IDs"
                    );
                    for (offset, nonce) in slots.into_iter().enumerate() {
                        *nonce =
                            Some(codec::typed_nonce(created, first + offset as i64, tag)?.to_vec());
                    }
                }
            }
        }
        p::prepare_outputs(db, task.id, storage, &batch).await?;
        let snapshot = Snapshot {
            revision: profile.revision,
            rows: selected.rows,
            ready,
            progress: selected.progress,
            rule: selected.rule,
        };
        sqlx::query("UPDATE tasks SET detail=jsonb_set(detail,'{node_pack}',$2) WHERE id=$1 AND lease_token=$3").bind(task.id).bind(serde_json::to_value(snapshot)?).bind(task.token).execute(&mut **db).await?;
        let keys = rewrite::keys(db, &blocks.into_iter().collect::<Vec<_>>(), key).await?;
        let plan = work::Plan::Pack {
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

pub(super) async fn finish(
    db: &mut Transaction<'_, Postgres>,
    task: Execution,
    storage: Uuid,
    batch: wire::Batch,
    detail: &Value,
    outcomes: &[work::Outcome],
    key: &Key,
) -> Result<Option<&'static str>> {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let [outcome] = outcomes else {
        return Err(Error::NodeOperationConflict.into());
    };
    if matches!(outcome, work::Outcome::Retry) {
        return Ok(Some("backend_io_failed"));
    }
    let work::Outcome::Packed(result) = outcome else {
        return Err(Error::NodeOperationConflict.into());
    };
    let mut snapshot: Snapshot = serde_json::from_value(detail["node_pack"].clone())?;
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
    let target = queries::storage_key(storage)
        .fetch_optional(&mut **db)
        .await?;
    if target.as_ref().map(|k| k.id) != batch.key
        || target.as_ref().map_or("none", |k| k.algorithm.as_str()) != batch.algorithm
    {
        return Ok(Some("pack_key_changed"));
    }
    let allowed: bool =
        sqlx::query_scalar("SELECT NOT pack_creation_paused FROM mokyu_meta WHERE singleton")
            .fetch_one(&mut **db)
            .await?;
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
    let mut key_ids = BTreeSet::new();
    key_ids.extend(batch.key);
    key_ids.extend(
        snapshot
            .ready
            .iter()
            .flatten()
            .filter_map(|c| c.block_key_id),
    );
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
        ensure!(
            (attempt.start..attempt.end).all(|i| covered.insert(i) && result.chunks[i] == 0),
            Error::NodeOperationConflict
        );
        outputs.push(PackOutput {
            chunks: batch.chunks[attempt.start..attempt.end]
                .iter()
                .map(|c| c.id)
                .collect(),
            pack: Some(pack),
            independent: None,
        });
    }
    for (index, value) in result.chunks.iter().enumerate() {
        if *value == 0 {
            continue;
        }
        ensure!(covered.insert(index), Error::NodeOperationConflict);
        let chunk = if let Some(chunk) = &snapshot.ready[index] {
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
            chunk.stored_size =
                Some(i32::try_from(size).map_err(|_| Error::NodeOperationConflict)?);
            chunk.compressed = compressed;
            codec::validate_metadata(&chunk, &keys).map_err(|_| Error::NodeOperationConflict)?;
            chunk
        };
        outputs.push(PackOutput {
            chunks: vec![chunk.id],
            pack: None,
            independent: Some(chunk),
        });
    }
    ensure!(
        covered.len() == batch.chunks.len(),
        Error::NodeOperationConflict
    );
    let after: i64 = outputs.iter().map(PackOutput::size).sum();
    let publish = match &snapshot.rule {
        Rule::Combine { before, physical } => {
            outputs.len() < *physical
                && after <= before + 12 + pack::MEMBER_SIZE as i64 * batch.chunks.len() as i64
        }
        Rule::Split { before, minimum } => {
            minimum.is_none_or(|n| before.saturating_sub(after) >= n as i64)
        }
        Rule::Range { .. } => {
            let sources = outputs
                .iter()
                .map(|o| (o.chunks.clone(), o.size()))
                .collect::<Vec<_>>();
            let benefit = snapshot.rule.benefit(&sources).unwrap();
            let worthwhile = benefit.worthwhile(&settings)?;
            if let Progress::Range { report, .. } = &mut snapshot.progress {
                report["benefit"] = serde_json::to_value(benefit)?;
                report["applied"] = json!(worthwhile);
                report["reason"] = json!(if worthwhile {
                    "repeated_partial_downloads"
                } else {
                    "encoded_layout_not_beneficial"
                });
            }
            worthwhile
        }
    };
    if publish {
        let flush = flush(db).await?;
        let mut publication = sqlx::Acquire::begin(&mut **db).await?;
        p::record_outputs(&mut publication, task.id, &snapshot.rows, &outputs).await?;
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
    sqlx::query("UPDATE tasks SET detail=detail-'node_pack' WHERE id=$1")
        .bind(task.id)
        .execute(&mut **db)
        .await?;
    Ok(None)
}
