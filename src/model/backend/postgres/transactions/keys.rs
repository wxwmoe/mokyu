use super::*;

pub(in super::super) async fn create_managed_key(
    pool: &PgPool,
    principal: &Principal,
    input: ManagedKeyCreate,
) -> Result<ManagedKeyView> {
    let mut tx = admin_transaction(pool, principal).await?;
    let result = queries::managed_key_view(
        queries::insert_managed_key(&input)
            .fetch_one(&mut *tx)
            .await?,
    );
    if input.purpose == "block" {
        queries::initialize_nonce_counters(input.id)
            .execute(&mut *tx)
            .await?;
    }
    checkpoint_event(&mut tx, "managed_key.create", &input.id.to_string(), json!({"name":result.name,"purpose":result.purpose,"algorithm":result.algorithm,"fingerprint":result.fingerprint})).await?;
    tx.commit().await?;
    Ok(result)
}

pub(in super::super) async fn revoke_managed_key(
    pool: &PgPool,
    principal: &Principal,
    id: Uuid,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
    lock_mode(&mut tx).await?;
    queries::lock_managed_key(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    if queries::managed_key_in_use(id).fetch_one(&mut *tx).await? {
        return Err(s3s::s3_error!(
            InvalidRequest,
            "key is still used by a storage, physical encoding, pending upload or task"
        )
        .into());
    }
    if queries::revoke_managed_key(id)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 0
    {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    checkpoint_event(&mut tx, "managed_key.revoke", &id.to_string(), json!({})).await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn checkpoint_event(
    tx: &mut Transaction<'_, Postgres>,
    action: &str,
    target: &str,
    detail: Value,
) -> Result<()> {
    let Some(id) = crate::model::audit::current() else {
        return Ok(());
    };
    let project = detail
        .get("project_id")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<Uuid>().ok());
    let bucket = detail
        .get("bucket_id")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<Uuid>().ok());
    ensure!(
        serde_json::to_vec(&detail)?.len() <= 262144,
        "audit detail exceeds its bound"
    );
    checkpoint_audit(tx, id, action, target, project, bucket, detail).await
}

pub(in super::super) async fn key_replacement_preview(
    pool: &PgPool,
    principal: &Principal,
    source: Uuid,
    input: KeyReplacementInput,
) -> Result<KeyReplacementPreview> {
    key_replacement::validate(&input, source)?;
    let actor = key_replacement::actor(principal)?;
    let mut tx = admin_transaction(pool, principal).await?;
    lock_mode(&mut tx).await?;
    let storages = queries::replacement_storages(source)
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        !storages.is_empty(),
        s3s::s3_error!(InvalidRequest, "key has no associated storages")
    );
    ensure!(
        serde_json::to_vec(&storages)?.len() <= 131072,
        s3s::s3_error!(InvalidRequest, "associated storage list is too large")
    );
    let (source_name, mut algorithm) = queries::replacement_key(source)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let id = Uuid::new_v4();
    let target = input.target.unwrap_or_else(Uuid::new_v4);
    let target_name = if input.target.is_some() {
        let (name, selected_algorithm) = queries::replacement_key(target)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(InvalidArgument, "target must be an active block key"))?;
        ensure!(
            queries::replacement_capacity(target)
                .fetch_one(&mut *tx)
                .await?,
            s3s::s3_error!(InvalidRequest, "target key cannot reserve more nonce IDs")
        );
        algorithm = selected_algorithm;
        name
    } else {
        input
            .name
            .unwrap_or_else(|| format!("key-{}", target.simple()))
            .trim()
            .to_owned()
    };
    for storage in &storages {
        queries::storage_profile_lock(storage.id)
            .fetch_one(&mut *tx)
            .await?
            .validate_key(Some(target))?;
    }
    let preview = KeyReplacementPreview {
        id,
        source,
        source_name,
        target,
        target_name,
        algorithm,
        create: input.target.is_none(),
        expires_at: Utc::now() + chrono::Duration::minutes(10),
        storages,
    };
    queries::insert_key_replacement(&actor, &preview)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(preview)
}

pub(in super::super) async fn key_replacement(
    pool: &PgPool,
    principal: &Principal,
    id: Uuid,
) -> Result<ReplacementRow> {
    let actor = key_replacement::actor(principal)?;
    let mut tx = admin_transaction(pool, principal).await?;
    let row = queries::key_replacement(id, &actor)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn replace_keys(
    pool: &PgPool,
    principal: &Principal,
    id: Uuid,
    input: Option<ManagedKeyCreate>,
) -> Result<KeyReplacementResult> {
    let actor = key_replacement::actor(principal)?;
    let mut tx = admin_transaction(pool, principal).await?;
    lock_mode(&mut tx).await?;
    let (preview, result) = queries::key_replacement(id, &actor)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    if let Some(result) = result {
        tx.commit().await?;
        return Ok(result.0);
    }
    let preview = preview.0;
    ensure!(
        preview.expires_at > Utc::now(),
        crate::model::Error::OperationAborted
    );
    let storages = queries::replacement_storages(preview.source)
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        storages == preview.storages,
        crate::model::Error::StorageProfileConflict
    );
    for storage in &storages {
        let profile = queries::storage_profile_lock(storage.id)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            profile.revision == storage.revision,
            crate::model::Error::StorageProfileConflict
        );
        profile.validate_key(Some(preview.target))?;
    }
    ensure!(
        queries::replacement_key(preview.source)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    if preview.create {
        let input = input.context("new key material is required")?;
        ensure!(
            input.id == preview.target
                && input.name == preview.target_name
                && input.algorithm == preview.algorithm
                && input.purpose == "block",
            s3s::s3_error!(InvalidArgument)
        );
        queries::insert_managed_key(&input)
            .fetch_one(&mut *tx)
            .await?;
        queries::initialize_nonce_counters(input.id)
            .execute(&mut *tx)
            .await?;
    } else {
        let target = queries::replacement_key(preview.target)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(crate::model::Error::OperationAborted)?;
        ensure!(
            target == (preview.target_name.clone(), preview.algorithm.clone())
                && queries::replacement_capacity(preview.target)
                    .fetch_one(&mut *tx)
                    .await?,
            crate::model::Error::OperationAborted
        );
    }
    for storage in &storages {
        queries::update_storage_key(storage.id, Some(preview.target))
            .execute(&mut *tx)
            .await?;
        queries::bump_storage_profile(storage.id)
            .execute(&mut *tx)
            .await?;
    }
    let result = KeyReplacementResult {
        preview,
        committed_at: Utc::now(),
    };
    queries::complete_key_replacement(&result)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(&mut tx, "managed_key.replace", &id.to_string(), serde_json::json!({"source":result.preview.source,"target":result.preview.target,"created":result.preview.create,"storages":result.preview.storages})).await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("key-replacement-before-commit").await;
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("key-replacement-after-commit").await;
    Ok(result)
}
