use super::*;

pub(in super::super) async fn start_rewrite(
    pool: &PgPool,
    input: &RewriteTask,
    node: Option<Uuid>,
    single: bool,
) -> Result<(Uuid, bool)> {
    let mut tx = pool.begin().await?;
    queries::lock_rewrite_storage(input.storage_id)
        .fetch_optional(&mut *tx)
        .await?
        .context("enabled store was not found")?;
    if let Some(id) = queries::active_rewrite(input.storage_id)
        .fetch_optional(&mut *tx)
        .await?
    {
        tx.commit().await?;
        return Ok((id, true));
    }
    let upper = queries::rewrite_upper(input.storage_id)
        .fetch_one(&mut *tx)
        .await?;
    if single {
        let mode = queries::replication::mode().fetch_one(&mut *tx).await?;
        let detail = json!({"execution_mode":mode,"storage_id":input.storage_id,"upper_chunk_id":upper,"profile_revision":input.profile_revision,"target_key_id":input.target_key_id,"target_key":input.target_key,"target_algorithm":input.target_algorithm});
        let id = enqueue_task(&mut tx, "rewrite", None, detail, false).await?;
        if input.shards == 0 {
            queries::complete_empty_rewrite(id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        return Ok((id, false));
    }
    let local = queries::rewrite_is_local(input.storage_id)
        .fetch_one(&mut *tx)
        .await?;
    let node = if local { node } else { None };
    let scope = if local { "node" } else { "cluster" };
    let detail = json!({"role":"parent","storage_id":input.storage_id,"upper_chunk_id":upper,"profile_revision":input.profile_revision,"target_key_id":input.target_key_id,"target_key":input.target_key,"target_algorithm":input.target_algorithm});
    let id = enqueue_task(&mut tx, "rewrite", None, detail.clone(), false).await?;
    queries::scope_rewrite(id, scope, node, input.shards)
        .execute(&mut *tx)
        .await?;
    for shard in 0..input.shards {
        let mut child_detail = detail.clone();
        child_detail["role"] = json!("child");
        child_detail["shard"] = json!(shard);
        child_detail["shards"] = json!(input.shards);
        let child = enqueue_task(&mut tx, "rewrite", None, child_detail, true).await?;
        queries::scope_rewrite_child(child, id, scope, node, input.shards)
            .execute(&mut *tx)
            .await?;
    }
    if input.shards == 0 {
        queries::complete_empty_rewrite(id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok((id, false))
}

async fn rewrite_lease(tx: &mut Transaction<'_, Postgres>, task: Uuid, token: Uuid) -> Result<()> {
    ensure!(
        queries::lock_task_execution(task, token)
            .fetch_optional(&mut **tx)
            .await?
            .is_some(),
        "rewrite task was paused or lost its lease"
    );
    Ok(())
}

pub(in super::super) async fn commit_rewrite_chunk(
    pool: &PgPool,
    task: Uuid,
    token: Uuid,
    source: &Chunk,
    physical: &Chunk,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    commit_rewrite_chunk_in(&mut tx, task, token, source, physical).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn commit_rewrite_chunk_in(
    tx: &mut Transaction<'_, Postgres>,
    task: Uuid,
    token: Uuid,
    source: &Chunk,
    physical: &Chunk,
) -> Result<()> {
    rewrite_lease(tx, task, token).await?;
    ensure!(
        super::super::queries::tier::rewrite_source(
            &mut **tx,
            crate::model::tiering::Unit::Chunk,
            source.encoding_id,
            source.address(),
            source.storage_id,
            physical.storage_id
        )
        .await?,
        crate::model::Error::OperationAborted
    );
    let current = queries::lock_rewrite_chunk(source.id)
        .fetch_optional(&mut **tx)
        .await?;
    ensure!(current == Some(None), "chunk source changed during rewrite");
    check_source_inputs(tx, task, &[source.id]).await?;
    let changed = queries::retire_rewrite_encoding(source.encoding_id)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    ensure!(changed == 1, "source encoding changed during rewrite");
    queries::publish_rewrite_encoding(physical)
        .execute(&mut **tx)
        .await?;
    queries::publish_rewrite_chunk(physical)
        .execute(&mut **tx)
        .await?;
    queries::advance_rewrite(task, source.id.to_string())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(in super::super) async fn commit_rewrite_pack(
    pool: &PgPool,
    task: Uuid,
    token: Uuid,
    old: &Pack,
    new: &Pack,
    current: &[Id],
    members: &[Chunk],
    first: Id,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let mut offset = 0;
    let members = members
        .iter()
        .map(|member| {
            let row = (member.id, offset);
            offset += i64::from(member.raw_size);
            row
        })
        .collect::<Vec<_>>();
    commit_rewrite_pack_in(&mut tx, task, token, old, new, current, &members, first).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn commit_rewrite_pack_in(
    tx: &mut Transaction<'_, Postgres>,
    task: Uuid,
    token: Uuid,
    old: &Pack,
    new: &Pack,
    current: &[Id],
    members: &[(Id, i64)],
    first: Id,
) -> Result<()> {
    rewrite_lease(tx, task, token).await?;
    ensure!(
        super::super::queries::tier::rewrite_source(
            &mut **tx,
            crate::model::tiering::Unit::Pack,
            old.id,
            old.address(),
            old.storage_id,
            new.storage_id
        )
        .await?,
        crate::model::Error::OperationAborted
    );
    let locked = queries::lock_rewrite_members(old.id)
        .fetch_all(&mut **tx)
        .await?;
    ensure!(locked == current, "pack membership changed during rewrite");
    check_source_inputs(tx, task, current).await?;
    let updated = queries::publish_rewrite_pack(new)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    ensure!(updated == 1, "new pack was changed during rewrite");
    for (ordinal, (member, offset)) in members.iter().enumerate() {
        queries::insert_pack_member(new.id, ordinal as i32, *member, *offset)
            .execute(&mut **tx)
            .await?;
    }
    let changed = queries::replace_rewrite_pack(old.id, new.id)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    ensure!(
        changed == current.len() as u64,
        "pack membership changed during rewrite"
    );
    let retired = queries::retire_rewrite_pack(old.id)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    ensure!(retired == 1, "source pack changed during rewrite");
    // Packs can interleave chunk IDs; advance only past this pack's scan key.
    queries::advance_rewrite(task, first.to_string())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(in super::super) async fn start_purge(
    pool: &PgPool,
    principal: &Principal,
    name: &str,
    id: Uuid,
    confirmation: Option<&str>,
) -> Result<Value> {
    let mut tx = admin_transaction(pool, principal).await?;
    let found: Option<Uuid> = queries::lock_purge_bucket(name, id)
        .fetch_optional(&mut *tx)
        .await?;
    super::super::transactions::redundancy::removable(&mut tx, id).await?;
    if found.is_none() {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    if let Some(task) = queries::existing_purge(id).fetch_optional(&mut *tx).await? {
        return Ok(json!({"task_id":task,"existing":true}));
    }
    if let Some(expected) = confirmation {
        let preview = bucket_impact(&mut tx, id).await?;
        if fingerprint(&preview, None)? != expected {
            return Err(s3s::s3_error!(PreconditionFailed).into());
        }
    }
    queries::seal_purge_bucket(id).execute(&mut *tx).await?;
    let task = enqueue_task(
        &mut tx,
        "purge",
        Some(id),
        json!({"bucket_id":id,"name":name}),
        false,
    )
    .await?;
    checkpoint_event(
        &mut tx,
        "bucket.purge",
        name,
        json!({"bucket_id":id,"task_id":task}),
    )
    .await?;
    tx.commit().await?;
    Ok(json!({"task_id":task}))
}
