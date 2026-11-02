use super::*;

pub(in super::super) async fn acquire_integrity_rate(
    pool: &PgPool,
    storage_id: Uuid,
    rates: &[(&str, f64, f64)],
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let accepted = integrity_rate_in(&mut tx, storage_id, rates).await?;
    tx.commit().await?;
    Ok(accepted)
}

pub(in super::super) async fn integrity_rate_in(
    tx: &mut Transaction<'_, Postgres>,
    storage_id: Uuid,
    rates: &[(&str, f64, f64)],
) -> Result<bool> {
    let mut remaining = Vec::new();
    for &(metric, rate, amount) in rates {
        // Allow one request larger than the per-second rate to make progress.
        let capacity = rate.max(amount).max(1.0);
        queries::initialize_integrity_rate(storage_id, metric, format!("{capacity:.6}"))
            .execute(&mut **tx)
            .await?;
        let (tokens, replenished): (f64, chrono::DateTime<chrono::Utc>) =
            queries::lock_integrity_rate(storage_id, metric)
                .fetch_one(&mut **tx)
                .await?;
        let elapsed = (chrono::Utc::now() - replenished).num_milliseconds().max(0) as f64 / 1000.0;
        let available = (tokens + elapsed * rate).min(capacity);
        if available < amount {
            return Ok(false);
        }
        remaining.push((metric, available - amount));
    }
    for (metric, value) in remaining {
        queries::update_integrity_rate(storage_id, metric, format!("{value:.6}"))
            .execute(&mut **tx)
            .await?;
    }
    Ok(true)
}

pub(in super::super) async fn start_integrity(
    pool: &PgPool,
    single: bool,
    input: IntegrityRequest,
    bucket: Option<Bucket>,
    node: Option<Uuid>,
) -> Result<(Uuid, usize)> {
    let mut tx = pool.begin().await?;
    queries::lock_pack_policy("mokyu:integrity".to_owned())
        .execute(&mut *tx)
        .await?;
    // Limit queued work as well as running work; repeated clicks must not grow an unbounded queue.
    let busy: bool = queries::integrity_busy().fetch_one(&mut *tx).await?;
    if busy {
        return Err(
            s3s::s3_error!(OperationAborted, "an integrity check is already active").into(),
        );
    }
    let storage_rows: Vec<(Uuid, String)> =
        if let Some(storage_id) = bucket.as_ref().and_then(|value| value.storage_id) {
            queries::integrity_storage(storage_id)
                .fetch_all(&mut *tx)
                .await?
        } else if bucket.is_some() {
            Vec::new()
        } else {
            queries::integrity_storages().fetch_all(&mut *tx).await?
        };
    let storage_rows = if storage_rows.is_empty() {
        vec![(Uuid::nil(), "shared".to_owned())]
    } else {
        storage_rows
    };
    let mut details = Vec::with_capacity(storage_rows.len());
    for (storage_id, storage_type) in storage_rows {
        let storage_id = (storage_id != Uuid::nil()).then_some(storage_id);
        let upper: Id = if let Some(storage_id) = storage_id {
            queries::integrity_upper_chunk(storage_id)
                .fetch_one(&mut *tx)
                .await?
        } else {
            queries::integrity_upper_chunk_all()
                .fetch_one(&mut *tx)
                .await?
        };
        let mut detail = Detail {
            storage_id,
            mode: input.mode,
            phase: "metadata".into(),
            bucket_id: bucket.as_ref().map(|value| value.id),
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            upper_chunk_id: upper.to_string(),
            upper_object: None,
            objects_checked: 0,
            chunks_checked: 0,
            bytes_checked: 0,
            issues: 0,
            skipped: 0,
            finished_at: None,
            storages: Vec::new(),
            storage_index: 0,
        };
        detail.upper_object = queries::integrity_upper_object(&mut tx, &detail).await?;
        details.push((storage_type, detail));
    }
    if single {
        let storages = details
            .iter()
            .map(|(_, detail)| crate::model::integrity::StorageSnapshot {
                storage_id: detail.storage_id,
                upper_chunk_id: detail.upper_chunk_id.clone(),
                upper_object: detail.upper_object.clone(),
            })
            .collect();
        let mut detail = details.remove(0).1;
        detail.storages = storages;
        let id = enqueue_task(
            &mut tx,
            "integrity",
            detail.bucket_id,
            serde_json::to_value(detail)?,
            false,
        )
        .await?;
        tx.commit().await?;
        return Ok((id, 0));
    }
    let child_count = details.len();
    let parent_detail = json!({
        "role": "parent",
        "mode": input.mode,
        "bucket": input.bucket.clone(),
        "key": input.key.clone(),
        "children": details.len(),
    });
    let parent = crate::model::REQUEST_ID
        .try_with(|id| *id)
        .unwrap_or_else(|_| Uuid::new_v4());
    insert_task(
        &mut *tx,
        parent,
        "integrity",
        bucket.as_ref().map(|value| value.id),
        parent_detail,
        crate::model::audit::current(),
    )
    .await?;
    for (storage_type, detail) in details {
        let child = Uuid::new_v4();
        insert_task(
            &mut *tx,
            child,
            "integrity",
            detail.bucket_id,
            serde_json::to_value(detail)?,
            crate::model::audit::current(),
        )
        .await?;
        let (execution_scope, target_node_id) = if storage_type == "local" {
            ("node", node)
        } else {
            ("cluster", None)
        };
        queries::scope_integrity_task(child, parent, 1, execution_scope, target_node_id)
            .execute(&mut *tx)
            .await?;
    }
    queries::task_batch_total(parent, child_count as i64)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((parent, child_count))
}

pub(in super::super) async fn checkpoint_integrity(
    pool: &PgPool,
    id: Execution,
    mut cursor: String,
    mut d: Detail,
    issues: Vec<Finding>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, id).await?;
    for i in issues {
        let inserted = queries::insert_integrity_finding(id.id, &i)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        d.issues += inserted;
        if inserted > 0 {
            tracing::warn!(task_id=%id.id,chunk_id=?i.chunk_id,stream_id=?i.stream_id,code=%i.code,"integrity finding");
        }
    }
    let mut advanced = false;
    if d.phase == "done" && !d.storages.is_empty() {
        d.storage_index += 1;
        if let Some(next) = d.storages.get(d.storage_index) {
            d.storage_id = next.storage_id;
            d.upper_chunk_id = next.upper_chunk_id.clone();
            d.upper_object = next.upper_object.clone();
            d.phase = "metadata".into();
            d.finished_at = None;
            cursor = serde_json::to_string(&crate::model::integrity::Cursor::default())?;
            advanced = true;
        }
    }
    queries::integrity_checkpoint(
        id.id,
        cursor,
        serde_json::to_value(&d)?,
        (d.objects_checked + d.chunks_checked) as i64,
        d.phase == "done",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    if advanced {
        #[cfg(feature = "fault-injection")]
        crate::faults::point("integrity-storage-advanced").await;
    }
    Ok(())
}

pub(in super::super) async fn change_pack_creation(pool: &PgPool, paused: bool) -> Result<()> {
    if !paused {
        let unpacking: bool = queries::unpacking_all().fetch_one(pool).await?;
        if unpacking {
            return Err(s3s::s3_error!(
                OperationAborted,
                "finish all-pack unpacking before enabling new packs"
            )
            .into());
        }
    }
    let mut tx = pool.begin().await?;
    queries::pause_pack_creation(paused)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "maintenance.pack_creation",
        "pack",
        json!({"paused":paused}),
    )
    .await?;
    tx.commit().await?;

    Ok(())
}
