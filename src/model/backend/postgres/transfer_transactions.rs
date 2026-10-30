use super::{replication_queries as r, transfer_queries as q};
use crate::model::{
    Error, Id, replication,
    transfer::{Input, Output, Scan, Scanned},
};
use anyhow::{Context, Result, ensure};
use sqlx::{Postgres, Transaction, types::Json};

type Tx<'a> = Transaction<'a, Postgres>;

pub(super) async fn capture(
    db: &mut Tx<'_>,
    run: &r::Run,
    chunks: &[(Id, Option<Id>)],
    raw: &[Id],
) -> Result<u64> {
    ensure!(
        run.options.0.target_external || run.source_storage()? != run.storage()?,
        Error::OperationAborted
    );
    for &id in raw {
        let unit = q::selected(run.task_id, "raw", id)
            .fetch_one(&mut **db)
            .await?;
        if unit.input.is_none() {
            let source = r::raw(id).fetch_one(&mut **db).await?;
            source.validate()?;
            ensure!(
                source.storage_id == run.source_storage()?,
                Error::SourceUnavailable
            );
            q::freeze(unit.id, &Input::Raw { source })
                .execute(&mut **db)
                .await?;
        }
    }
    for &(id, pack) in chunks {
        if q::member(run.task_id, id)
            .fetch_optional(&mut **db)
            .await?
            .is_some()
        {
            continue;
        }
        let (source_type, source) = pack.map_or(("chunk", id), |pack| ("pack", pack));
        let unit = q::selected(run.task_id, source_type, source)
            .fetch_one(&mut **db)
            .await?;
        if unit.input.is_none() {
            let input = if let Some(pack) = pack {
                Input::Pack {
                    pack: q::pack(pack).fetch_one(&mut **db).await?,
                    members: Vec::new(),
                }
            } else {
                Input::Chunk {
                    chunk: q::chunk(id).fetch_one(&mut **db).await?,
                }
            };
            ensure!(
                input.storage()? == run.source_storage()?,
                Error::SourceUnavailable
            );
            q::freeze(unit.id, &input).execute(&mut **db).await?;
        }
        q::capture_member(run.task_id, unit.id, id, pack)
            .execute(&mut **db)
            .await?;
        ensure!(
            q::member(run.task_id, id).fetch_optional(&mut **db).await? == Some(unit.id),
            Error::OperationAborted
        );
    }
    Ok(q::discard_unselected(run.task_id)
        .execute(&mut **db)
        .await?
        .rows_affected())
}

pub(super) async fn scan(
    db: &mut Tx<'_>,
    run: &r::Run,
    unit: &mut q::Unit,
) -> Result<Option<Scan>> {
    ensure!(
        run.phase == "verify"
            && (run.options.0.target_external || run.source_storage()? != run.storage()?),
        Error::OperationAborted
    );
    if !matches!(unit.state.as_str(), "selected" | "scanning") {
        return Ok(None);
    }
    if let Some(Json(Input::Pack { pack, members })) = unit.input.as_ref() {
        if unit.state == "selected" && members.is_empty() {
            let members = q::members(unit.id)
                .fetch_all(&mut **db)
                .await?
                .into_iter()
                .map(|(id, uuid, offset, size, hash)| {
                    Ok(crate::model::transfer::Member {
                        id,
                        uuid,
                        offset: u64::try_from(offset)?,
                        size: u32::try_from(size)?,
                        hash,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let input = Input::Pack {
                pack: pack.clone(),
                members,
            };
            input.validate()?;
            q::input(unit.id, &input).execute(&mut **db).await?;
            unit.input = Some(Json(input));
        }
    }
    let Some(Json(Input::Raw { source })) = unit.input.as_ref() else {
        return Ok(None);
    };
    ensure!(
        unit.source_type == "raw"
            && unit.source_id == source.id
            && source.storage_id == run.source_storage()?,
        Error::SourceUnavailable
    );
    let current = r::raw(source.id).fetch_one(&mut **db).await?;
    current.validate()?;
    if current.pending_upload {
        return Ok(None);
    }
    let mut expected = source.clone();
    if unit.state == "selected" {
        expected.pending_upload = current.pending_upload;
    }
    ensure!(
        replication::same_source(&expected, &current),
        Error::SourceUnavailable
    );
    let source = current;
    let (raw, cdc) = if run.options.0.target_external {
        (true, false)
    } else {
        q::policy(run.storage()?, run.profile_revision()?)
            .fetch_optional(&mut **db)
            .await?
            .ok_or(Error::OperationAborted)?
    };
    if unit.state == "selected" {
        let input = Input::Raw {
            source: source.clone(),
        };
        q::input(unit.id, &input).execute(&mut **db).await?;
        ensure!(
            q::begin_scan(unit.id)
                .execute(&mut **db)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        unit.input = Some(Json(input));
        unit.state = "scanning".into();
    }
    Ok(Some(Scan::next(
        unit.id,
        run.generation,
        source,
        u64::try_from(unit.scanned_bytes)?,
        unit.checksum
            .as_ref()
            .map(|value| value.0.clone())
            .unwrap_or_default(),
        (!raw).then_some(cdc),
    )?))
}

pub(super) async fn scanned(db: &mut Tx<'_>, run: &r::Run, value: &Scanned) -> Result<()> {
    let mut unit = q::unit(run.task_id, value.unit)
        .fetch_one(&mut **db)
        .await?;
    let plan = scan(db, run, &mut unit)
        .await?
        .context("transfer scan is no longer current")?;
    plan.validate(value)?;
    if !value.spans.is_empty() {
        q::spans(unit.id, &value.spans)
            .build()
            .execute(&mut **db)
            .await?;
    }
    if let Some(part) = &value.part {
        q::part(unit.id, part).execute(&mut **db).await?;
    }
    ensure!(
        q::scanned(value).execute(&mut **db).await?.rows_affected() == 1,
        Error::OperationAborted
    );
    Ok(())
}

pub(super) async fn cleanup(db: &mut Tx<'_>, task: uuid::Uuid) -> Result<bool> {
    if q::cleanup_spans(task)
        .execute(&mut **db)
        .await?
        .rows_affected()
        > 0
        || q::cleanup_parts(task)
            .execute(&mut **db)
            .await?
            .rows_affected()
            > 0
        || q::cleanup_members(task)
            .execute(&mut **db)
            .await?
            .rows_affected()
            > 0
    {
        return Ok(false);
    }
    let rows = q::cleanup(task).fetch_all(&mut **db).await?;
    if rows.is_empty() {
        return Ok(true);
    }
    for (unit, candidate) in rows {
        if let Some(candidate) = candidate {
            r::abandon(candidate).execute(&mut **db).await?;
        }
        ensure!(
            q::release(unit).execute(&mut **db).await?.rows_affected() == 1,
            Error::OperationAborted
        );
    }
    Ok(false)
}

pub(super) async fn prepare(
    db: &mut Tx<'_>,
    task: crate::model::task::Execution,
    run: &r::Run,
    unit: &mut q::Unit,
) -> Result<Option<replication::Step>> {
    use crate::authorization::Permit;
    use crate::model::{
        ChunkCreate, StreamCreate,
        cache::EncodingPolicy,
        transfer::{Copy, Prepared, Span, Target},
    };
    ensure!(
        run.phase == "verify"
            && run.source_storage()? != run.storage()?
            && matches!(unit.state.as_str(), "selected" | "copying"),
        Error::OperationAborted
    );
    let input = unit
        .input
        .as_ref()
        .context("transfer input missing")?
        .0
        .clone();
    input.validate()?;
    ensure!(
        input.storage()? == run.source_storage()?,
        Error::SourceUnavailable
    );
    ensure!(
        r::profile(run.options.0.target_bucket)
            .fetch_one(&mut **db)
            .await?
            == (run.storage()?, run.profile_revision()?),
        Error::OperationAborted
    );
    let profile = super::queries::storage_profile(run.storage()?)
        .fetch_one(&mut **db)
        .await?;
    ensure!(
        profile.revision == run.profile_revision()?,
        Error::OperationAborted
    );
    let (raw, _) = q::policy(run.storage()?, run.profile_revision()?)
        .fetch_one(&mut **db)
        .await?;
    let (algorithm, key) = q::encoding(run.storage()?).fetch_one(&mut **db).await?;
    let candidate = if let Some(candidate) = unit.candidate {
        ensure!(
            r::adopt(candidate, task)
                .execute(&mut **db)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        if run.mode == "cluster" {
            r::renew(candidate).execute(&mut **db).await?;
            super::transactions::create_write_lease(db, candidate, 120).await?;
        }
        candidate
    } else {
        let request = StreamCreate {
            authority: Permit::system(),
            bucket: run.options.0.target_bucket,
            key: format!("__transfer/{}", unit.id),
            stream_type: "object".into(),
            metadata: serde_json::json!({}),
            public: false,
            claim_object: false,
            completion: None,
            owner: Some((task.id, task.token)),
        };
        let candidate = super::transactions::create_stream_in(
            db,
            request,
            (run.mode == "cluster").then_some(120),
        )
        .await?;
        ensure!(
            q::candidate(unit.id, candidate)
                .execute(&mut **db)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        unit.candidate = Some(candidate);
        unit.state = "copying".into();
        candidate
    };
    ensure!(
        q::eligible(unit.id).fetch_one(&mut **db).await?,
        Error::SourceUnavailable
    );
    if raw {
        return Ok(Some(replication::Step::Raw(
            prepare_raw(db, run, unit, candidate, input).await?,
        )));
    }
    if input.size()? == 0 {
        ensure!(
            q::empty(unit.id).execute(&mut **db).await?.rows_affected() == 1,
            Error::OperationAborted
        );
        return Ok(None);
    }
    let output = if let Some(output) = &unit.output {
        output.0.chunks()?.clone()
    } else {
        let start = u64::try_from(unit.written_bytes)?;
        let mut spans = Vec::new();
        let mut selected = Vec::new();
        match &input {
            Input::Chunk { chunk } => {
                ensure!(start == 0, Error::OperationAborted);
                selected.push((chunk.id, chunk.hash.clone(), chunk.raw_size));
            }
            Input::Pack { members, .. } => {
                ensure!(start == 0, Error::OperationAborted);
                selected.extend(
                    members
                        .iter()
                        .map(|member| (member.id, member.hash.clone(), member.size as i32)),
                );
            }
            Input::Raw { source } => {
                ensure!(
                    unit.scanned_bytes == source.size && start < source.size as u64,
                    Error::OperationAborted
                );
                let mut offset = start;
                for (id, position, length, hash) in q::span_rows(unit.id, unit.written_bytes)
                    .fetch_all(&mut **db)
                    .await?
                {
                    ensure!(
                        u64::try_from(position)? == offset
                            && (1..=crate::content::codec::MAX as i32).contains(&length)
                            && hash.len() == 32,
                        Error::SourceUnavailable
                    );
                    let end = offset
                        .checked_add(length as u64)
                        .context("transfer offset overflow")?;
                    if end - start > crate::model::transfer::SCAN_BYTES {
                        break;
                    }
                    spans.push(Span {
                        id,
                        offset,
                        size: length as u32,
                        hash: hash.clone(),
                    });
                    selected.push((id, hash, length));
                    offset = end;
                }
            }
        }
        ensure!(!selected.is_empty(), Error::SourceUnavailable);
        let policy = EncodingPolicy {
            compression: profile.compression_config("0s"),
            pack_enabled: profile.pack_enabled,
            pack_max_size: profile.pack_max_size.clone(),
            pack_flush_timeout: profile.pack_upload_flush_timeout.clone(),
            algorithm,
            block_key_id: key,
        };
        policy.validate()?;
        let mut known: std::collections::BTreeMap<
            (Vec<u8>, i32),
            (crate::content::codec::Chunk, bool),
        > = std::collections::BTreeMap::new();
        let mut offset = start;
        let mut targets = Vec::with_capacity(selected.len());
        for (source, hash, size) in selected {
            let identity = (hash.clone(), size);
            let (chunk, reused) = if let Some(value) = known.get(&identity) {
                value.clone()
            } else {
                let create = ChunkCreate {
                    storage: run.storage()?,
                    stream: candidate,
                    hash: hash.as_slice().try_into()?,
                    raw_size: size,
                    algorithm: policy.algorithm.clone(),
                    key: policy.block_key_id,
                };
                let value =
                    super::transactions::allocate_chunk_in(db, create, run.mode == "single")
                        .await?;
                known.insert(identity, value.clone());
                value
            };
            targets.push(Target {
                source,
                offset,
                chunk,
                reused,
            });
            offset = offset
                .checked_add(size as u64)
                .context("transfer offset overflow")?;
        }
        ensure!(offset <= input.size()?, Error::SourceUnavailable);
        let output = Prepared {
            start,
            end: offset,
            policy,
            targets,
            spans,
        };
        ensure!(
            q::prepare(unit.id, &output)
                .execute(&mut **db)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        q::append(candidate, &output)
            .build()
            .execute(&mut **db)
            .await?;
        unit.output = Some(Json(Output::Chunks(output.clone())));
        output
    };
    ensure!(
        output.start == unit.written_bytes as u64,
        Error::OperationAborted
    );
    let plan = Copy {
        unit: unit.id,
        revision: run.generation,
        input,
        candidate,
        storage: run.storage()?,
        prepared: output,
    };
    plan.validate()?;
    Ok(Some(replication::Step::Copy(plan)))
}

pub(super) async fn copied(
    db: &mut Tx<'_>,
    task: crate::model::task::Execution,
    run: &r::Run,
    plan: &crate::model::transfer::Copy,
    outputs: &[crate::model::packing::PackOutput],
) -> Result<()> {
    use super::queries as base;
    let rows = plan.outputs(outputs)?;
    ensure!(
        run.phase == "verify"
            && run.generation == plan.revision
            && run.storage()? == plan.storage
            && run.source_storage()? == plan.input.storage()?,
        Error::OperationAborted
    );
    ensure!(
        r::profile(run.options.0.target_bucket)
            .fetch_one(&mut **db)
            .await?
            == (run.storage()?, run.profile_revision()?),
        Error::OperationAborted
    );
    let unit = q::unit(task.id, plan.unit).fetch_one(&mut **db).await?;
    ensure!(
        q::eligible(unit.id).fetch_one(&mut **db).await?,
        Error::SourceUnavailable
    );
    ensure!(
        unit.state == "copying"
            && unit.candidate == Some(plan.candidate)
            && unit.written_bytes == plan.prepared.start as i64
            && serde_json::to_value(unit.input.context("transfer input missing")?.0)?
                == serde_json::to_value(&plan.input)?
            && serde_json::to_value(unit.output.context("transfer output missing")?.0.chunks()?)?
                == serde_json::to_value(&plan.prepared)?,
        Error::OperationAborted
    );
    ensure!(
        base::conversion_owned(plan.candidate, task.id, task.token)
            .fetch_optional(&mut **db)
            .await?
            .is_some(),
        Error::OperationAborted
    );
    for row in &rows {
        ensure!(
            base::conversion_chunk(row.chunk.id, plan.candidate)
                .fetch_optional(&mut **db)
                .await?
                .is_some(),
            Error::OperationAborted
        );
        ensure!(
            base::conversion_ready(row.chunk.id, plan.candidate)
                .execute(&mut **db)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        base::conversion_retire_encoding(row.chunk.encoding_id)
            .execute(&mut **db)
            .await?;
    }
    for output in outputs {
        if let Some(pack) = &output.pack {
            ensure!(
                base::conversion_output_pack(pack.id, task.id)
                    .fetch_optional(&mut **db)
                    .await?
                    .is_some(),
                Error::OperationAborted
            );
        }
        if let Some(chunk) = &output.independent {
            ensure!(
                base::conversion_output_encoding(chunk.encoding_id, task.id, chunk.id)
                    .fetch_optional(&mut **db)
                    .await?
                    .is_some(),
                Error::OperationAborted
            );
        }
    }
    if !rows.is_empty() {
        super::queries::pack_work::publish_outputs(db, &rows, outputs, false, false, 0.).await?;
    }
    super::transactions::require_sources(db, Some(plan.candidate), &[]).await?;
    base::resize_writing_stream(plan.candidate, plan.prepared.end as i64)
        .execute(&mut **db)
        .await?;
    ensure!(
        q::copied(
            plan.unit,
            plan.prepared.start as i64,
            plan.prepared.end as i64,
            plan.prepared.end == plan.input.size()?
        )
        .execute(&mut **db)
        .await?
        .rows_affected()
            == 1,
        Error::OperationAborted
    );
    Ok(())
}

pub(super) async fn project(
    db: &mut Tx<'_>,
    run: &r::Run,
    candidate: Id,
    source: Id,
    raw: bool,
    position: i64,
    size: i64,
) -> Result<i64> {
    ensure!(position >= 0 && position < size, Error::SourceUnavailable);
    if q::policy(run.storage()?, run.profile_revision()?)
        .fetch_one(&mut **db)
        .await?
        .0
    {
        return project_raw(db, run, candidate, source, raw, position, size).await;
    }
    let mut end = position;
    let mut rows = Vec::new();
    let mut chunks = std::collections::BTreeSet::new();
    while rows.len() < 128 && end < size {
        let (source, offset, length) = q::source_piece(source, end, raw)
            .fetch_one(&mut **db)
            .await?;
        ensure!(
            offset >= 0 && length > 0 && length <= size - end,
            Error::SourceUnavailable
        );
        let (unit, base) = q::target_piece(run.task_id, source, raw)
            .fetch_one(&mut **db)
            .await?;
        let start = base
            .checked_add(offset)
            .context("transfer offset overflow")?;
        let until = start
            .checked_add(length)
            .context("transfer offset overflow")?;
        let selected = q::projected(unit, start, until, (128 - rows.len()) as i64)
            .fetch_all(&mut **db)
            .await?;
        ensure!(!selected.is_empty(), Error::SourceUnavailable);
        let mut cursor = start;
        for (at, length, chunk, within) in selected {
            let last = at
                .checked_add(i64::from(length))
                .context("transfer offset overflow")?;
            let first = at.max(start);
            ensure!(
                first == cursor && last > cursor && length > 0,
                Error::SourceUnavailable
            );
            let count = i32::try_from(last.min(until) - first)?;
            let offset = i32::try_from(i64::from(within) + first - at)?;
            rows.push((end, count, chunk, offset));
            chunks.insert(chunk);
            cursor += i64::from(count);
            end += i64::from(count);
        }
    }
    ensure!(end > position && end <= size, Error::SourceUnavailable);
    super::transactions::require_sources(db, None, &chunks.iter().copied().collect::<Vec<_>>())
        .await?;
    q::project(candidate, &rows)
        .build()
        .execute(&mut **db)
        .await?;

    super::queries::resize_writing_stream(candidate, end)
        .execute(&mut **db)
        .await?;
    Ok(end)
}

pub(super) async fn prepare_raw(
    db: &mut Tx<'_>,
    run: &r::Run,
    unit: &q::Unit,
    candidate: Id,
    input: Input,
) -> Result<crate::model::transfer::RawCopy> {
    use super::queries as base;
    use crate::model::transfer::{Part, RawCopy};
    let target = if let Some(value) = &unit.output {
        let Output::Raw(target) = &value.0 else {
            anyhow::bail!(Error::OperationAborted)
        };
        target.clone()
    } else {
        let (medium, unencoded, revision, cdc, _) = base::raw_target(candidate, run.storage()?)
            .fetch_one(&mut **db)
            .await?;
        ensure!(
            unencoded && !cdc && revision == run.profile_revision()?,
            Error::OperationAborted
        );
        ensure!(
            base::raw_prepare(candidate)
                .execute(&mut **db)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        let medium = if medium == "local" {
            "local"
        } else {
            "shared_s3"
        };
        let id: Id = base::raw_insert(
            candidate,
            run.storage()?,
            run.profile_revision()?,
            medium,
            false,
        )
        .fetch_one(&mut **db)
        .await?
        .into();
        let mut target = base::raw_source(id).fetch_one(&mut **db).await?;
        target.size = i64::try_from(input.size()?)?;
        target.digest = match &input {
            Input::Raw { source } => source.digest.clone(),
            _ => None,
        };
        ensure!(
            q::raw_output(unit.id, &target)
                .execute(&mut **db)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        target
    };
    let rows = q::raw_parts(unit.id).fetch_all(&mut **db).await?;
    let mut parts = Vec::with_capacity(rows.len());
    let mut receipts = Vec::with_capacity(rows.len());
    for (number, offset, size, digest, etag) in rows {
        parts.push(Part {
            number: u16::try_from(number)?,
            offset: u64::try_from(offset)?,
            size: u64::try_from(size)?,
            digest,
        });
        receipts.push(etag.map(|value| blake3::hash(value.as_bytes()).to_hex().to_string()));
    }
    let value = RawCopy {
        unit: unit.id,
        revision: run.generation,
        input,
        candidate,
        target,
        upload: unit.upload_id.clone(),
        parts,
        receipts,
    };
    value.validate()?;
    Ok(value)
}

pub(super) async fn raw_progress(
    db: &mut Tx<'_>,
    task: crate::model::task::Execution,
    run: &r::Run,
    plan: &crate::model::transfer::RawCopy,
    progress: &crate::model::transfer::RawProgress,
) -> Result<()> {
    use super::queries as base;
    use crate::model::transfer::RawProgress;
    plan.validate()?;
    progress.validate()?;
    ensure!(
        run.phase == "verify"
            && run.generation == plan.revision
            && run.source_storage()? == plan.input.storage()?
            && run.storage()? == plan.target.storage_id,
        Error::OperationAborted
    );
    ensure!(
        r::profile(run.options.0.target_bucket)
            .fetch_one(&mut **db)
            .await?
            == (run.storage()?, run.profile_revision()?),
        Error::OperationAborted
    );
    let unit = q::unit(task.id, plan.unit).fetch_one(&mut **db).await?;
    ensure!(
        unit.candidate == Some(plan.candidate)
            && serde_json::to_value(
                unit.input
                    .as_ref()
                    .context("transfer input missing")?
                    .0
                    .clone()
            )? == serde_json::to_value(&plan.input)?
            && serde_json::to_value(
                unit.output
                    .as_ref()
                    .context("transfer output missing")?
                    .0
                    .clone()
            )? == serde_json::to_value(Output::Raw(plan.target.clone()))?,
        Error::OperationAborted
    );
    ensure!(
        base::conversion_owned(plan.candidate, task.id, task.token)
            .fetch_optional(&mut **db)
            .await?
            .is_some(),
        Error::OperationAborted
    );
    ensure!(
        q::eligible(unit.id).fetch_one(&mut **db).await?,
        Error::SourceUnavailable
    );
    if unit.state == "ready" {
        let RawProgress::Finished { digest } = progress else {
            anyhow::bail!(Error::OperationAborted)
        };
        let current = base::raw_source(plan.target.id)
            .fetch_one(&mut **db)
            .await?;
        current.validate()?;
        ensure!(
            current.size == plan.target.size && current.digest.as_ref() == Some(digest),
            Error::OperationAborted
        );
        return Ok(());
    }
    ensure!(unit.state == "copying", Error::OperationAborted);
    match progress {
        RawProgress::Started { upload } => {
            ensure!(
                plan.target.medium == "shared_s3" && !plan.parts.is_empty(),
                Error::OperationAborted
            );
            if let Some(current) = unit.upload_id {
                ensure!(current == *upload, Error::OperationAborted);
            } else {
                ensure!(unit.written_bytes == 0, Error::OperationAborted);
                q::raw_upload(unit.id, Some(upload))
                    .execute(&mut **db)
                    .await?;
                ensure!(
                    q::target_upload(plan.target.id, plan.candidate, Some(upload))
                        .execute(&mut **db)
                        .await?
                        .rows_affected()
                        == 1,
                    Error::OperationAborted
                );
            }
        }
        RawProgress::Reset { upload } => {
            if unit.upload_id.is_none() && unit.written_bytes == 0 {
                return Ok(());
            }
            ensure!(
                unit.upload_id.as_ref() == Some(upload),
                Error::OperationAborted
            );
            q::reset_parts(unit.id).execute(&mut **db).await?;
            q::raw_upload(unit.id, None).execute(&mut **db).await?;
            q::target_upload(plan.target.id, plan.candidate, None)
                .execute(&mut **db)
                .await?;
            q::raw_written(unit.id, 0, false).execute(&mut **db).await?;
        }
        RawProgress::Part {
            upload,
            number,
            etag,
        } => {
            ensure!(
                unit.upload_id == *upload && (upload.is_some() || plan.target.medium == "local"),
                Error::OperationAborted
            );
            let (offset, size, previous) =
                q::raw_part(unit.id, *number).fetch_one(&mut **db).await?;
            if let Some(previous) = previous {
                ensure!(
                    previous == *etag && unit.written_bytes >= offset + size,
                    Error::OperationAborted
                );
            } else {
                ensure!(unit.written_bytes == offset, Error::OperationAborted);
                ensure!(
                    q::part_receipt(unit.id, *number, etag)
                        .execute(&mut **db)
                        .await?
                        .rows_affected()
                        == 1,
                    Error::OperationAborted
                );
                q::raw_written(unit.id, offset + size, false)
                    .execute(&mut **db)
                    .await?;
            }
        }
        RawProgress::Finished { digest } => {
            ensure!(
                plan.target
                    .digest
                    .as_ref()
                    .is_none_or(|expected| expected == digest),
                Error::SourceUnavailable
            );
            ensure!(
                plan.parts.is_empty() || unit.written_bytes == plan.target.size,
                Error::OperationAborted
            );
            ensure!(
                base::raw_lock_stream(plan.candidate)
                    .fetch_optional(&mut **db)
                    .await?
                    .is_some(),
                Error::OperationAborted
            );
            ensure!(
                base::raw_finish(
                    plan.candidate,
                    plan.target.id,
                    plan.target.size,
                    digest,
                    true
                )
                .execute(&mut **db)
                .await?
                .rows_affected()
                    == 1,
                Error::OperationAborted
            );
            base::raw_attach(plan.candidate, plan.target.id, plan.target.size)
                .execute(&mut **db)
                .await?;
            base::resize_writing_stream(plan.candidate, plan.target.size)
                .execute(&mut **db)
                .await?;
            ensure!(
                q::raw_written(unit.id, plan.target.size, true)
                    .execute(&mut **db)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
        }
    }
    Ok(())
}

pub(super) async fn project_raw(
    db: &mut Tx<'_>,
    run: &r::Run,
    candidate: Id,
    source: Id,
    source_raw: bool,
    position: i64,
    size: i64,
) -> Result<i64> {
    use super::queries as base;
    if position == 0 {
        ensure!(
            base::raw_prepare(candidate)
                .execute(&mut **db)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
    }
    let mut end = position;
    let mut rows = Vec::new();
    let mut ids = std::collections::BTreeSet::new();
    while rows.len() < 128 && end < size {
        let (source, offset, length) = q::source_piece(source, end, source_raw)
            .fetch_one(&mut **db)
            .await?;
        ensure!(
            offset >= 0 && length > 0 && length <= size - end,
            Error::SourceUnavailable
        );
        let (unit, base) = q::target_piece(run.task_id, source, source_raw)
            .fetch_one(&mut **db)
            .await?;
        let start = base
            .checked_add(offset)
            .context("transfer offset overflow")?;
        let until = start
            .checked_add(length)
            .context("transfer offset overflow")?;
        let selected = base::raw_page(unit, start, until)
            .fetch_all(&mut **db)
            .await?;
        ensure!(!selected.is_empty(), Error::SourceUnavailable);
        let mut cursor = start;
        for row in selected.into_iter().take(128 - rows.len()) {
            row.validate()?;
            ensure!(
                row.source.storage_id == run.storage()?,
                Error::SourceUnavailable
            );
            let first = row.offset_bytes.max(start);
            let last = (row.offset_bytes + row.length).min(until);
            ensure!(first == cursor && last > cursor, Error::SourceUnavailable);
            let count = last - first;
            rows.push((
                end,
                count,
                row.source.id,
                row.source_offset + first - row.offset_bytes,
            ));
            ids.insert(row.source.id);
            cursor += count;
            end += count;
        }
    }
    ensure!(end > position && end <= size, Error::SourceUnavailable);
    let ids: Vec<_> = ids.into_iter().collect();
    let locked: Vec<Id> = base::raw_copy_sources(run.storage()?, &ids)
        .build_query_scalar()
        .fetch_all(&mut **db)
        .await?;
    ensure!(locked == ids, Error::SourceUnavailable);
    q::project_raw(candidate, &rows)
        .build()
        .execute(&mut **db)
        .await?;
    base::raw_copy_reference(&ids)
        .build()
        .execute(&mut **db)
        .await?;
    base::resize_writing_stream(candidate, end)
        .execute(&mut **db)
        .await?;
    Ok(end)
}
