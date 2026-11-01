use super::*;

pub(in super::super) async fn revoke_token(
    pool: &PgPool,
    principal: &Principal,
    id: Uuid,
) -> Result<()> {
    let user: Uuid = queries::token_user(id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let mut tx = token_owner(pool, principal, user, true).await?;
    queries::revoke_token(id).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "token.revoke",
        &id.to_string(),
        serde_json::json!({"user_id":user}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn lock_bucket_settings(
    tx: &mut Transaction<'_, Postgres>,
    principal: &Principal,
    bucket: Uuid,
) -> Result<()> {
    principal.lock_identity(tx).await?;
    queries::lock_bucket_id(bucket).execute(&mut **tx).await?;
    Permit::for_action(principal.clone(), bucket, Action::Settings)
        .lock(tx)
        .await
}

pub(in super::super) async fn create_bucket(
    pool: &PgPool,
    principal: &Principal,
    input: CreateBucket,
) -> Result<Bucket> {
    if !s3s::path::check_bucket_name(&input.name) {
        return Err(s3s::s3_error!(InvalidBucketName).into());
    }
    let mut tx = pool.begin().await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    let admin = principal.lock_identity(&mut tx).await?;
    let project = input.project_id.unwrap_or(DEFAULT_PROJECT);
    let allowed: Option<bool> = queries::bucket_creation_policy(project)
        .fetch_optional(&mut *tx)
        .await?;
    let allowed = allowed.ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let mut selected = false;
    if !admin {
        if !allowed || !matches!(principal, Principal::User { .. }) {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
        let member: Option<(String, String)> =
            queries::bucket_creator_membership(project, principal.user_id())
                .fetch_optional(&mut *tx)
                .await?;
        let Some((role, scope)) = member else {
            return Err(s3s::s3_error!(AccessDenied).into());
        };
        if role != "maintainer" {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
        selected = scope == "selected";
    }
    let available: Vec<Uuid> = queries::bucket_available_storages()
        .fetch_all(&mut *tx)
        .await?;
    let storage_id = match input.storage_id {
        Some(id) if available.contains(&id) => id,
        Some(_) => return Err(anyhow::anyhow!("storage is unavailable").into()),
        None if available.len() == 1 => available[0],
        None if available.is_empty() => {
            return Err(anyhow::anyhow!("no storage configured").into());
        }
        None => return Err(anyhow::anyhow!("storage must be selected").into()),
    };
    let row: Option<Bucket> = queries::insert_bucket(&input.name, project, storage_id)
        .fetch_optional(&mut *tx)
        .await?;
    let row = row.ok_or(crate::model::Error::BucketAlreadyExists)?;
    if selected {
        queries::grant_bucket_creator(principal.user_id(), project, row.id)
            .execute(&mut *tx)
            .await?;
    }
    checkpoint_event(
        &mut tx,
        "bucket.create",
        &row.name,
        json!({"bucket_id":row.id,"project_id":project}),
    )
    .await?;
    tx.commit().await?;
    Ok(row)
}

pub(crate) async fn lock_bucket_dependents(
    tx: &mut Transaction<'_, Postgres>,
    bucket: Uuid,
) -> Result<()> {
    // Identity rows precede the bucket, matching admitted requests and grant mutations.
    queries::lock_bucket_members(bucket)
        .execute(&mut **tx)
        .await?;
    queries::lock_bucket_credentials(bucket)
        .execute(&mut **tx)
        .await?;
    queries::lock_bucket_tokens(bucket)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(crate) async fn bucket_impact(
    tx: &mut Transaction<'_, Postgres>,
    bucket: Uuid,
) -> Result<BucketImpact> {
    Ok(queries::bucket_impact(bucket)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?)
}

pub(in super::super) async fn delete_bucket(
    pool: &PgPool,
    principal: &Principal,
    bucket: Uuid,
    name: &str,
    confirmation: Option<&str>,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
    lock_bucket_dependents(&mut tx, bucket).await?;
    let row: Bucket = queries::lock_bucket(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    super::super::transactions::redundancy::removable(&mut tx, bucket).await?;
    if row.name != name {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    if let Some(expected) = confirmation
        && fingerprint(&bucket_impact(&mut tx, bucket).await?, None)? != expected
    {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    let busy: bool = queries::bucket_has_streams(bucket)
        .fetch_one(&mut *tx)
        .await?;
    if busy || row.state != "active" {
        return Err(crate::model::Error::BucketNotEmpty.into());
    }
    queries::delete_bucket_objects(bucket)
        .execute(&mut *tx)
        .await?;
    queries::delete_bucket(bucket).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "bucket.delete",
        name,
        json!({"bucket_id":bucket,"project_id":row.project_id}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn domain_change(
    pool: &PgPool,
    principal: &Principal,
    value: &str,
    bucket: Option<Uuid>,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
    let old: Option<Uuid> = queries::bucket_domain_owner(value)
        .fetch_optional(&mut *tx)
        .await?;
    if bucket.is_some() && old.is_some() && old != bucket {
        return Err(crate::model::Error::DomainInUse.into());
    }
    if let Some(id) = bucket.or(old) {
        let state: Option<String> = queries::bucket_domain_state(id)
            .fetch_optional(&mut *tx)
            .await?;
        if state.as_deref() != Some("active") {
            return Err(s3s::s3_error!(OperationAborted).into());
        }
    }
    if let Some(id) = bucket {
        queries::insert_bucket_domain(value, id)
            .execute(&mut *tx)
            .await?;
    } else {
        queries::delete_bucket_domain(value)
            .execute(&mut *tx)
            .await?;
    }
    checkpoint_event(
        &mut tx,
        "bucket.domain",
        value,
        json!({"bucket_id":bucket.or(old),"removed":bucket.is_none()}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn bucket_transfer_target(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<TransferProject> {
    Ok(queries::bucket_transfer_target(id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?)
}

pub(in super::super) async fn save_bucket_settings(
    pool: &PgPool,
    principal: &Principal,
    bucket: Uuid,
    mut input: BucketUpdate,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    if input.domains.is_some() {
        queries::lock_accounts().execute(&mut *tx).await?;
    }
    let admin = principal.lock_identity(&mut tx).await?;
    lock_bucket_settings(&mut tx, principal, bucket).await?;
    let row: Bucket = queries::lock_bucket(bucket).fetch_one(&mut *tx).await?;
    if row.settings_revision != input.revision {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    if row.state != "active" {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    if !admin
        && (input.domains.is_some()
            || row.uploads_paused != input.uploads_paused
            || row.public_base_url != input.public_base_url)
    {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    if let Some(domains) = input.domains.take() {
        let used: bool = queries::bucket_domains_in_use(&domains, bucket)
            .fetch_one(&mut *tx)
            .await?;
        if used {
            return Err(crate::model::Error::DomainInUse.into());
        }
        queries::remove_bucket_domains(bucket, &domains)
            .execute(&mut *tx)
            .await?;
        queries::insert_bucket_domains(&domains, bucket)
            .execute(&mut *tx)
            .await?;
    }
    queries::save_bucket_settings(bucket, &input)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(&mut tx,"bucket.settings",&row.name,json!({"bucket_id":bucket,"project_id":row.project_id,"uploads_paused":input.uploads_paused})).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn transfer_bucket(
    pool: &PgPool,
    principal: &Principal,
    bucket: Uuid,
    input: TransferInput,
) -> Result<Bucket> {
    let mut tx = admin_transaction(pool, principal).await?;
    let source: Uuid = queries::bucket_project_id(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    if source == input.target_project {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    queries::lock_bucket_transfer_users(source, input.target_project)
        .execute(&mut *tx)
        .await?;
    lock_bucket_dependents(&mut tx, bucket).await?;
    queries::lock_bucket_id(bucket).execute(&mut *tx).await?;
    let current = bucket_impact(&mut tx, bucket).await?;
    if current.name != input.confirm_name
        || fingerprint(&current, Some(input.target_project))? != input.confirmation
    {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    if current.state != "active"
        || !current.uploads_paused
        || [
            &current.reserved_bytes,
            &current.inflight_bytes,
            &current.active_uploads,
            &current.writing_streams,
        ]
        .iter()
        .any(|s| s.as_str() != "0")
    {
        return Err(crate::model::Error::BucketNotDrained.into());
    }
    bucket_transfer_target(&mut tx, input.target_project).await?;
    queries::revoke_bucket_members(bucket)
        .execute(&mut *tx)
        .await?;
    queries::revoke_bucket_credentials(bucket)
        .execute(&mut *tx)
        .await?;
    queries::revoke_bucket_tokens(bucket)
        .execute(&mut *tx)
        .await?;
    queries::revoke_bucket_transfer_users(source, input.target_project)
        .execute(&mut *tx)
        .await?;
    let row: Bucket = queries::transfer_bucket(bucket, input.target_project)
        .fetch_one(&mut *tx)
        .await?;
    checkpoint_event(&mut tx,"bucket.transfer",&row.name,json!({"bucket_id":bucket,"project_id":input.target_project,"previous_project":source,"revoked_member_bucket_grants":current.member_bucket_grants,"revoked_service_grants":current.service_grants,"revoked_token_bucket_grants":current.token_bucket_grants})).await?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn bucket_settings(
    pool: &PgPool,
    bucket: Uuid,
) -> Result<(Bucket, Vec<String>)> {
    let mut tx = pool.begin().await?;
    queries::bucket_snapshot().execute(&mut *tx).await?;
    let row = queries::bucket_by_id(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    let domains = queries::bucket_domain_names(bucket)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((row, domains))
}

pub(in super::super) async fn bucket_domains(
    pool: &PgPool,
    principal: &Principal,
    bucket: Option<Uuid>,
) -> Result<Vec<Domain>> {
    let tx = admin_transaction(pool, principal).await?;
    tx.commit().await?;
    Ok(queries::bucket_domains(bucket).fetch_all(pool).await?)
}

pub(in super::super) async fn preview_bucket(
    pool: &PgPool,
    principal: &Principal,
    bucket: Uuid,
) -> Result<BucketImpact> {
    let mut tx = pool.begin().await?;
    principal.lock_admin(&mut tx).await?;
    bucket_impact(&mut tx, bucket).await
}

pub(in super::super) async fn preview_bucket_transfer(
    pool: &PgPool,
    principal: &Principal,
    bucket: Uuid,
    target: Uuid,
) -> Result<(BucketImpact, TransferProject)> {
    let mut tx = pool.begin().await?;
    principal.lock_admin(&mut tx).await?;
    let bucket = bucket_impact(&mut tx, bucket).await?;
    let target = bucket_transfer_target(&mut tx, target).await?;
    if bucket.project_id == target.id {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    Ok((bucket, target))
}

pub(in super::super) async fn set_bucket_cors(
    pool: &PgPool,
    principal: &Principal,
    bucket: Uuid,
    rules: Value,
) -> Result<Value> {
    let mut tx = pool.begin().await?;
    lock_bucket_settings(&mut tx, principal, bucket).await?;
    let result = queries::set_bucket_cors(bucket, rules)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    checkpoint_event(
        &mut tx,
        "bucket.cors",
        &bucket.to_string(),
        serde_json::json!({"bucket_id":bucket,"cors":result}),
    )
    .await?;
    tx.commit().await?;
    Ok(result)
}

pub(in super::super) async fn set_bucket_website(
    pool: &PgPool,
    principal: &Principal,
    bucket: Uuid,
    input: Website,
) -> Result<Website> {
    let mut tx = pool.begin().await?;
    lock_bucket_settings(&mut tx, principal, bucket).await?;
    let row = queries::set_bucket_website(bucket, &input)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    checkpoint_event(&mut tx,"bucket.website",&bucket.to_string(),serde_json::json!({"bucket_id":bucket,"website_enabled":input.website_enabled,"index_document":input.index_document,"error_document":input.error_document})).await?;
    tx.commit().await?;
    Ok(row)
}
