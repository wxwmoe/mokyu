use super::*;
use crate::{
    model::{
        PhysicalSource,
        integrity::{Cursor, Detail, Finding},
        node_work::{AuditTarget, Inspection},
        source::{SourceScope, SourceType},
    },
    worker::integrity::Mode,
};

fn unavailable(error: &anyhow::Error) -> Option<&'static str> {
    error
        .downcast_ref::<codec::IntegrityError>()
        .map(|e| e.code())
        .or_else(|| {
            matches!(
                error.downcast_ref::<Error>(),
                Some(Error::SourceUnavailable | Error::OperationAborted)
            )
            .then_some("source_unavailable")
        })
}

async fn checkpoint(
    db: &mut PgConnection,
    task: Uuid,
    cursor: String,
    detail: &Detail,
) -> Result<()> {
    queries::integrity_checkpoint(
        task,
        cursor,
        serde_json::to_value(detail)?,
        (detail.objects_checked + detail.chunks_checked) as i64,
        detail.phase == "done",
    )
    .execute(db)
    .await?;
    Ok(())
}

async fn candidates(
    db: &mut PgConnection,
    detail: &Detail,
    cursor: &Cursor,
    limit: usize,
) -> Result<Vec<(AuditTarget, Option<SourceScope>)>> {
    if detail.phase == "chunks" {
        let chunks: Vec<crate::content::codec::Chunk> =
            queries::integrity_chunks(detail, cursor.chunk, limit as i64)?
                .build_query_as()
                .fetch_all(&mut *db)
                .await?;
        let mut targets = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let source = match queries::physical_source(db, chunk.id, true).await {
                Ok(PhysicalSource::Chunk(c)) => Some((SourceType::Chunk, c.encoding_id)),
                Ok(PhysicalSource::Pack(p)) => Some((SourceType::Pack, p.id)),
                Ok(PhysicalSource::Raw(_)) => unreachable!(),
                Err(error) if unavailable(&error).is_some() => None,
                Err(error) => return Err(error),
            };
            targets.push((
                AuditTarget::Chunk { id: chunk.id },
                source.map(|(source, id)| SourceScope {
                    storage_id: detail.storage_id.unwrap(),
                    source,
                    id: id.to_string(),
                }),
            ));
        }
        return Ok(targets);
    }
    ensure!(detail.phase == "raw", Error::NodeOperationConflict);
    let mut query = sqlx::QueryBuilder::new(
        "SELECT o.bucket_id,o.object_key,o.stream_id,e.offset_bytes,e.source_id FROM objects o JOIN streams s ON s.id=o.stream_id JOIN raw_ranges e ON e.stream_id=s.id WHERE s.payload_type='raw'",
    );
    queries::integrity_scope(&mut query, detail);
    if let (Some(bucket), Some(key)) = (cursor.bucket, &cursor.key) {
        query
            .push(" AND (o.bucket_id,o.object_key,e.offset_bytes)>(")
            .push_bind(bucket)
            .push(",")
            .push_bind(key)
            .push(",")
            .push_bind(cursor.after_extent)
            .push(")");
    }
    query
        .push(" ORDER BY o.bucket_id,o.object_key,e.offset_bytes LIMIT ")
        .push_bind(limit as i64);
    let rows: Vec<(Uuid, String, Id, i64, Id)> = query.build_query_as().fetch_all(db).await?;
    Ok(rows
        .into_iter()
        .map(|(bucket, key, stream, offset, source)| {
            (
                AuditTarget::Raw {
                    bucket,
                    key,
                    stream,
                    offset,
                },
                Some(SourceScope {
                    storage_id: detail.storage_id.unwrap(),
                    source: SourceType::Raw,
                    id: source.to_string(),
                }),
            )
        })
        .collect())
}

fn advance(cursor: &mut Cursor, target: &AuditTarget) {
    match target {
        AuditTarget::Chunk { id } => cursor.chunk = *id,
        AuditTarget::Raw {
            bucket,
            key,
            offset,
            ..
        } => {
            cursor.bucket = Some(*bucket);
            cursor.key = Some(key.clone());
            cursor.after_extent = *offset;
        }
    }
}

pub(super) async fn prepare(
    tx: &mut Transaction<'_, Postgres>,
    execution: Execution,
    detail: &Value,
    profile: &crate::storage::profile::StorageProfile,
    max_items: usize,
    key: &Key,
) -> Result<Option<(work::Plan, Vec<read::BlockKey>)>> {
    let mut detail: Detail = serde_json::from_value(detail.clone())?;
    ensure!(
        detail.storage_id == Some(profile.storage_id) && detail.mode != Mode::Metadata,
        Error::NodeOperationConflict
    );
    let saved: Option<String> = sqlx::query_scalar("SELECT cursor FROM tasks WHERE id=$1")
        .bind(execution.id)
        .fetch_one(&mut **tx)
        .await?;
    let mut cursor: Cursor = saved
        .as_deref()
        .map(serde_json::from_str)
        .transpose()?
        .unwrap_or_default();
    let mut rows = candidates(tx, &detail, &cursor, max_items).await?;
    if rows.is_empty() && detail.phase == "raw" {
        detail.phase = "chunks".into();
        checkpoint(tx, execution.id, serde_json::to_string(&cursor)?, &detail).await?;
        rows = candidates(tx, &detail, &cursor, max_items).await?;
    }
    if rows.is_empty() {
        detail.phase = "done".into();
        detail.finished_at = Some(chrono::Utc::now());
        checkpoint(tx, execution.id, serde_json::to_string(&cursor)?, &detail).await?;
        q::finish(execution.id, execution.token, 0, true, None)
            .execute(&mut **tx)
            .await?;
        return Ok(None);
    }
    let byte_limit = profile
        .integrity_byte_budget
        .as_deref()
        .map(crate::config::bytes)
        .transpose()?
        .unwrap_or(64 * 1024 * 1024);
    let mut bytes = 0u64;
    let mut member_count = 0;
    let mut requests = 0;
    let mut items: Vec<Inspection> = vec![];
    let mut keys = std::collections::BTreeMap::new();
    for (target, scope) in rows {
        let mut item = Inspection {
            location: Uuid::nil(),
            snapshot: None,
            members: vec![],
            targets: vec![],
            previous: Some(work::Outcome::Inspected {
                code: Some("source_unavailable".into()),
            }),
        };
        if let Some(scope) = scope {
            if let Some(existing) = items.iter_mut().find(|i| {
                i.snapshot
                    .as_ref()
                    .is_some_and(|s| s.scope.source == scope.source && s.scope.id == scope.id)
            }) {
                advance(&mut cursor, &target);
                existing.targets.push(target);
                continue;
            }
            match queries::source_snapshot(tx, &scope, true).await {
                Ok(snapshot) => {
                    let (size, block) = match &snapshot.physical {
                        PhysicalSource::Chunk(c) => (
                            i64::from(c.stored_size.unwrap_or(c.raw_size)),
                            c.block_key_id,
                        ),
                        PhysicalSource::Pack(p) => {
                            (p.stored_size.unwrap_or(p.raw_size), p.block_key_id)
                        }
                        PhysicalSource::Raw(r) => (r.size, None),
                    };
                    item.previous = if let PhysicalSource::Pack(p) = &snapshot.physical {
                        sqlx::query_scalar::<_, Option<String>>("SELECT error_code FROM integrity_packs WHERE task_id=$1 AND pack_id=$2")
                            .bind(execution.id).bind(p.id).fetch_optional(&mut **tx).await?.map(|code| work::Outcome::Inspected { code })
                    } else {
                        None
                    };
                    let cost = if detail.mode == Mode::Full && item.previous.is_none() {
                        size.max(0) as u64
                    } else {
                        0
                    };
                    if !items.is_empty() && bytes.saturating_add(cost) > byte_limit {
                        break;
                    }
                    if item.previous.is_none() {
                        if let PhysicalSource::Pack(pack) = &snapshot.physical {
                            item.members = q::members(pack.id).fetch_all(&mut **tx).await?;
                            if member_count + item.members.len() > 4096 && !items.is_empty() {
                                break;
                            }
                            member_count += item.members.len();
                        }
                        requests += if detail.mode == Mode::Full
                            && matches!(snapshot.physical, PhysicalSource::Raw(_))
                        {
                            3
                        } else {
                            2
                        };
                        if let Some(id) = block
                            && !keys.contains_key(&id)
                        {
                            if let Some(record) = queries::node_read_keys(&[id])
                                .fetch_optional(&mut **tx)
                                .await?
                            {
                                keys.insert(id, read::BlockKey::unprotect(record, key)?);
                            }
                        }
                    }
                    bytes += cost;
                    item.location = snapshot.location();
                    item.snapshot = Some(Box::new(snapshot));
                }
                Err(error) if unavailable(&error).is_some() => {
                    item.previous = Some(work::Outcome::Inspected {
                        code: Some(unavailable(&error).unwrap().into()),
                    });
                }
                Err(error) => return Err(error),
            }
        }
        advance(&mut cursor, &target);
        item.targets.push(target);
        items.push(item);
    }
    let mut rates = vec![];
    if let Some(rate) = profile.integrity_rps {
        rates.push(("integrity_rps", rate as f64, requests as f64));
    }
    if let Some(rate) = profile
        .integrity_bandwidth
        .as_deref()
        .map(crate::config::bytes)
        .transpose()?
    {
        rates.push(("integrity_bandwidth", rate as f64, bytes as f64));
    }
    if !integrity_rate_in(tx, profile.storage_id, &rates).await? {
        return Ok(None);
    }
    Ok(Some((
        work::Plan::Audit {
            storage: profile.storage_id,
            mode: detail.mode,
            cursor: serde_json::to_string(&cursor)?,
            items,
        },
        keys.into_values().collect(),
    )))
}

pub(super) async fn finish(
    tx: &mut Transaction<'_, Postgres>,
    execution: Execution,
    detail: Value,
    cursor: String,
    items: Vec<Inspection>,
    outcomes: &[work::Outcome],
) -> Result<Option<&'static str>> {
    ensure!(items.len() == outcomes.len(), Error::NodeOperationConflict);
    let mut retry = false;
    for (item, outcome) in items.iter().zip(outcomes) {
        match outcome {
            work::Outcome::Inspected { code } => {
                ensure!(
                    code.as_deref()
                        .is_none_or(|c| codec::IntegrityError::from_code(c).is_some()
                            || c == "source_unavailable"),
                    Error::NodeOperationConflict
                );
                ensure!(item.previous.as_ref().is_none_or(|previous| matches!(previous, work::Outcome::Inspected { code: prior } if prior == code)), Error::NodeOperationConflict);
            }
            work::Outcome::Retry => {
                retry = true;
            }
            _ => return Err(Error::NodeOperationConflict.into()),
        }
    }
    if retry {
        return Ok(Some("backend_io_failed"));
    }
    let mut detail: Detail = serde_json::from_value(detail)?;
    for (item, outcome) in items.iter().zip(outcomes) {
        let work::Outcome::Inspected { code } = outcome else {
            unreachable!()
        };
        let unchanged = if let Some(snapshot) = &item.snapshot {
            match queries::source_snapshot(tx, &snapshot.scope, true).await {
                Ok(current) => current.fingerprint()? == snapshot.fingerprint()?,
                Err(error)
                    if unavailable(&error).is_some()
                        || matches!(
                            error.downcast_ref::<sqlx::Error>(),
                            Some(sqlx::Error::RowNotFound)
                        ) =>
                {
                    false
                }
                Err(error) => return Err(error),
            }
        } else {
            true
        };
        let mut current_targets = 0;
        for target in &item.targets {
            let (current, mut finding) = match target {
                AuditTarget::Chunk { id } => {
                    let current: Option<Id> = queries::integrity_chunk_stream(*id, &detail)
                        .build_query_scalar()
                        .fetch_optional(&mut **tx)
                        .await?;
                    let selected = match queries::physical_source(tx, *id, true).await {
                        Ok(PhysicalSource::Chunk(c)) => c.location_id == item.location,
                        Ok(PhysicalSource::Pack(p)) => p.location_id == item.location,
                        Ok(PhysicalSource::Raw(_)) => false,
                        Err(error) if unavailable(&error).is_some() => item.snapshot.is_none(),
                        Err(error) => return Err(error),
                    };
                    (
                        current.is_some() && selected,
                        Finding {
                            subject: format!("chunk:{id}"),
                            code: String::new(),
                            chunk_id: Some(*id),
                            location_id: None,
                            stream_id: None,
                            bucket_id: None,
                            key: None,
                            detail: json!({}),
                        },
                    )
                }
                AuditTarget::Raw {
                    bucket,
                    key,
                    stream,
                    offset,
                } => {
                    let current = queries::integrity_current(*bucket, key, *stream)
                        .fetch_one(&mut **tx)
                        .await?;
                    let mapped: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw_ranges WHERE stream_id=$1 AND offset_bytes=$2 AND source_id=$3)")
                        .bind(stream).bind(offset).bind(item.location).fetch_one(&mut **tx).await?;
                    (
                        current && mapped,
                        Finding {
                            subject: format!("object:{stream}:{offset}"),
                            code: String::new(),
                            chunk_id: None,
                            location_id: None,
                            stream_id: Some(*stream),
                            bucket_id: Some(*bucket),
                            key: Some(key.clone()),
                            detail: json!({}),
                        },
                    )
                }
            };
            if !unchanged || !current {
                detail.skipped += 1;
                continue;
            }
            current_targets += 1;
            if matches!(target, AuditTarget::Chunk { .. }) {
                detail.chunks_checked += 1;
            }
            if let Some(code) = code {
                finding.code = code.clone();
                finding.location_id = (!item.location.is_nil()).then_some(item.location);
                finding.detail =
                    json!({"storage_id":detail.storage_id,"location_id":finding.location_id});
                if let Some(snapshot) = &item.snapshot {
                    finding.detail["source"] = serde_json::to_value(snapshot.scope.source)?;
                    if snapshot.scope.source == SourceType::Pack {
                        finding.detail["pack_id"] = json!(snapshot.scope.id);
                    } else if snapshot.scope.source == SourceType::Chunk {
                        finding.detail["encoding_id"] = json!(snapshot.scope.id);
                    }
                }
                detail.issues += queries::insert_integrity_finding(execution.id, &finding)
                    .execute(&mut **tx)
                    .await?
                    .rows_affected();
            }
        }
        if unchanged && current_targets > 0 && item.previous.is_none() {
            if let Some(snapshot) = &item.snapshot {
                if code.is_none() && detail.mode == Mode::Full {
                    detail.bytes_checked += match &snapshot.physical {
                        PhysicalSource::Chunk(c) => i64::from(c.stored_size.unwrap_or(0)),
                        PhysicalSource::Pack(p) => p.stored_size.unwrap_or(0),
                        PhysicalSource::Raw(r) => r.size,
                    }
                    .max(0) as u64;
                }
                if let PhysicalSource::Pack(pack) = &snapshot.physical {
                    sqlx::query("INSERT INTO integrity_packs(task_id,pack_id,error_code) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
                        .bind(execution.id).bind(pack.id).bind(code).execute(&mut **tx).await?;
                }
            }
        }
    }
    checkpoint(tx, execution.id, cursor, &detail).await?;
    Ok(None)
}
