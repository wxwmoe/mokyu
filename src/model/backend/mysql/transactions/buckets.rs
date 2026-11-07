use super::*;

pub(in super::super) async fn bucket_domains(
    pool: &MySqlPool,
    principal: &Principal,
    bucket: Option<Uuid>,
) -> Result<Vec<Domain>> {
    let mut tx = admin_transaction(pool, principal).await?;
    let rows = queries::bucket_domains(bucket).fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok(rows)
}

pub(in super::super) async fn preview_bucket(
    pool: &MySqlPool,
    principal: &Principal,
    bucket: Uuid,
) -> Result<BucketImpact> {
    let mut tx = admin_transaction(pool, principal).await?;
    let row = queries::bucket_impact(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn preview_bucket_transfer(
    pool: &MySqlPool,
    principal: &Principal,
    bucket: Uuid,
    target: Uuid,
) -> Result<(BucketImpact, TransferProject)> {
    let mut tx = admin_transaction(pool, principal).await?;
    let row = queries::bucket_impact(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    let target = queries::bucket_transfer_target(target)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    ensure!(row.project_id != target.id, s3s::s3_error!(InvalidArgument));
    tx.commit().await?;
    Ok((row, target))
}

pub(super) async fn lock_bucket_dependents(
    connection: &mut MySqlConnection,
    bucket: Uuid,
) -> Result<Vec<Uuid>> {
    let members = queries::bucket_member_ids(bucket)
        .fetch_all(&mut *connection)
        .await?;
    queries::bucket_credential_ids(bucket)
        .fetch_all(&mut *connection)
        .await?;
    queries::bucket_token_ids(bucket)
        .fetch_all(&mut *connection)
        .await?;
    Ok(members)
}

async fn revoke_bucket_access(
    connection: &mut MySqlConnection,
    bucket: Uuid,
    members: &[Uuid],
) -> Result<()> {
    for user in members {
        queries::advance_user_authorization(*user)
            .execute(&mut *connection)
            .await?;
    }
    queries::advance_bucket_credentials(bucket)
        .execute(&mut *connection)
        .await?;
    queries::advance_bucket_tokens(bucket)
        .execute(&mut *connection)
        .await?;
    queries::revoke_bucket_members(bucket)
        .execute(&mut *connection)
        .await?;
    queries::revoke_bucket_credentials(bucket)
        .execute(&mut *connection)
        .await?;
    queries::revoke_bucket_tokens(bucket)
        .execute(&mut *connection)
        .await?;
    Ok(())
}

pub(super) async fn remove_empty_bucket(
    connection: &mut MySqlConnection,
    row: &Bucket,
    members: &[Uuid],
) -> Result<()> {
    let accounts = lock_quota(connection, row.id).await?;
    ensure!(
        accounts[1].used_bytes == 0
            && accounts[1].reserved_bytes == 0
            && accounts[1].inflight_bytes == 0
            && accounts[1].object_count == 0,
        crate::model::Error::BucketNotEmpty
    );
    revoke_bucket_access(connection, row.id, members).await?;
    queries::delete_bucket_objects(row.id)
        .execute(&mut *connection)
        .await?;
    queries::change_project_bucket_quota(row.project_id, -1, 0, 0)
        .execute(&mut *connection)
        .await?;
    queries::delete_quota_scope("bucket", row.id)
        .execute(&mut *connection)
        .await?;
    queries::delete_bucket(row.id).execute(connection).await?;
    Ok(())
}

pub(in super::super) async fn delete_bucket(
    pool: &MySqlPool,
    principal: &Principal,
    bucket: Uuid,
    name: &str,
    confirmation: Option<&str>,
) -> Result<()> {
    let mut tx = administration_transaction(pool, principal).await?;
    let members = lock_bucket_dependents(&mut tx, bucket).await?;
    let row = queries::lock_bucket(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    super::super::transactions::redundancy::removable(&mut tx, bucket).await?;
    ensure!(row.name == name, s3s::s3_error!(PreconditionFailed));
    if let Some(expected) = confirmation {
        ensure!(
            fingerprint(
                &queries::bucket_impact(bucket).fetch_one(&mut *tx).await?,
                None
            )? == expected,
            s3s::s3_error!(PreconditionFailed)
        );
    }
    ensure!(
        row.state == "active"
            && !queries::bucket_has_streams(bucket)
                .fetch_one(&mut *tx)
                .await?,
        crate::model::Error::BucketNotEmpty
    );
    remove_empty_bucket(&mut tx, &row, &members).await?;
    checkpoint_event(
        &mut tx,
        "bucket.delete",
        name,
        serde_json::json!({"bucket_id":bucket,"project_id":row.project_id}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn domain_change(
    pool: &MySqlPool,
    principal: &Principal,
    host: &str,
    bucket: Option<Uuid>,
) -> Result<()> {
    let mut tx = administration_transaction(pool, principal).await?;
    let old = queries::bucket_domain_owner(host)
        .fetch_optional(&mut *tx)
        .await?;
    ensure!(
        bucket.is_none() || old.is_none() || old == bucket,
        crate::model::Error::DomainInUse
    );
    if let Some(id) = bucket.or(old) {
        let row = queries::lock_bucket(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(crate::model::Error::OperationAborted)?;
        ensure!(row.state == "active", crate::model::Error::OperationAborted);
        if old != bucket {
            if bucket.is_some() {
                queries::insert_bucket_domain(host, id)
                    .execute(&mut *tx)
                    .await?;
            } else {
                queries::delete_bucket_domain(host)
                    .execute(&mut *tx)
                    .await?;
            }
            queries::advance_bucket_settings(id)
                .execute(&mut *tx)
                .await?;
        }
    }
    checkpoint_event(
        &mut tx,
        "bucket.domain",
        host,
        serde_json::json!({"bucket_id":bucket.or(old),"removed":bucket.is_none()}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn lock_bucket_settings(
    connection: &mut MySqlConnection,
    principal: &Principal,
    bucket: Uuid,
) -> Result<(Bucket, bool)> {
    let admin = queries::validate_identity(principal, connection, true).await?;
    let row = queries::lock_bucket(bucket)
        .fetch_optional(&mut *connection)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    ensure!(
        queries::bucket_actions(principal, connection, bucket, admin)
            .await?
            .contains(&Action::Settings),
        s3s::s3_error!(AccessDenied)
    );
    ensure!(row.state == "active", crate::model::Error::OperationAborted);
    Ok((row, admin))
}

pub(in super::super) async fn save_bucket_settings(
    pool: &MySqlPool,
    principal: &Principal,
    bucket: Uuid,
    input: BucketUpdate,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    if input.domains.is_some() {
        queries::lock_accounts().execute(&mut *tx).await?;
    }
    let (row, admin) = lock_bucket_settings(&mut tx, principal, bucket).await?;
    ensure!(
        row.settings_revision == input.revision,
        s3s::s3_error!(PreconditionFailed)
    );
    ensure!(
        admin
            || (input.domains.is_none()
                && input.uploads_paused == row.uploads_paused
                && input.public_base_url == row.public_base_url),
        s3s::s3_error!(AccessDenied)
    );
    if let Some(domains) = &input.domains {
        for host in domains {
            ensure!(
                queries::bucket_domain_owner(host)
                    .fetch_optional(&mut *tx)
                    .await?
                    .is_none_or(|id| id == bucket),
                crate::model::Error::DomainInUse
            );
        }
        let old = queries::bucket_domain_names(bucket)
            .fetch_all(&mut *tx)
            .await?;
        for host in &old {
            if !domains.contains(host) {
                queries::delete_bucket_domain(host)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        let unique: std::collections::BTreeSet<_> = domains.iter().collect();
        for host in unique {
            if !old.contains(host) {
                queries::insert_bucket_domain(host, bucket)
                    .execute(&mut *tx)
                    .await?;
            }
        }
    }
    if let Some(enabled) = input.metadata_journal_enabled {
        super::super::journal::configure(&mut tx, bucket, enabled).await?;
    }
    queries::save_bucket_settings(bucket, &input)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(&mut tx,"bucket.settings",&row.name,serde_json::json!({"bucket_id":bucket,"project_id":row.project_id,"uploads_paused":input.uploads_paused})).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn bucket_settings(
    pool: &MySqlPool,
    bucket: Uuid,
) -> Result<(Bucket, Vec<String>, Option<bool>)> {
    let mut tx = pool.begin().await?;
    let row = queries::share_bucket(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    let domains = queries::bucket_domain_names(bucket)
        .fetch_all(&mut *tx)
        .await?;
    let journal = queries::journal::enabled(bucket)
        .fetch_optional(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((row, domains, journal))
}

pub(in super::super) async fn set_bucket_cors(
    pool: &MySqlPool,
    principal: &Principal,
    bucket: Uuid,
    rules: Value,
) -> Result<Value> {
    let mut tx = pool.begin().await?;
    lock_bucket_settings(&mut tx, principal, bucket).await?;
    queries::set_bucket_cors(bucket, &rules)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "bucket.cors",
        &bucket.to_string(),
        serde_json::json!({"bucket_id":bucket,"cors":rules}),
    )
    .await?;
    tx.commit().await?;
    Ok(rules)
}

pub(in super::super) async fn set_bucket_website(
    pool: &MySqlPool,
    principal: &Principal,
    bucket: Uuid,
    input: Website,
) -> Result<Website> {
    let mut tx = pool.begin().await?;
    lock_bucket_settings(&mut tx, principal, bucket).await?;
    queries::set_bucket_website(bucket, &input)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "bucket.website",
        &bucket.to_string(),
        serde_json::json!({"bucket_id":bucket,"website":input}),
    )
    .await?;
    tx.commit().await?;
    Ok(input)
}

pub(in super::super) async fn transfer_bucket(
    pool: &MySqlPool,
    principal: &Principal,
    bucket: Uuid,
    input: TransferInput,
) -> Result<Bucket> {
    let mut tx = administration_transaction(pool, principal).await?;
    let source = queries::quota_project(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    ensure!(
        source != input.target_project,
        s3s::s3_error!(InvalidArgument)
    );
    let users = queries::bucket_transfer_users(source, input.target_project)
        .fetch_all(&mut *tx)
        .await?;
    let members = lock_bucket_dependents(&mut tx, bucket).await?;
    queries::lock_bucket(bucket).fetch_one(&mut *tx).await?;
    let current = queries::bucket_impact(bucket).fetch_one(&mut *tx).await?;
    ensure!(
        current.name == input.confirm_name
            && fingerprint(&current, Some(input.target_project))? == input.confirmation,
        s3s::s3_error!(PreconditionFailed)
    );
    ensure!(
        current.state == "active"
            && current.uploads_paused
            && [
                &current.reserved_bytes,
                &current.inflight_bytes,
                &current.active_uploads,
                &current.writing_streams
            ]
            .iter()
            .all(|v| v.as_str() == "0"),
        crate::model::Error::BucketNotDrained
    );
    let mut projects = [source, input.target_project];
    projects.sort();
    for project in projects {
        queries::quota_account("project", project, true)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    }
    let total = queries::quota_account("bucket", bucket, true)
        .fetch_one(&mut *tx)
        .await?;
    let target = queries::quota_account("project", input.target_project, false)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        total.reserved_bytes == 0 && total.inflight_bytes == 0,
        crate::model::Error::BucketNotDrained
    );
    ensure!(
        target
            .bucket_count_limit
            .is_none_or(|limit| target.bucket_count < limit)
            && (total.used_bytes == 0
                || target
                    .logical_bytes_limit
                    .is_none_or(|limit| i128::from(target.used_bytes)
                        + i128::from(target.reserved_bytes)
                        + i128::from(total.used_bytes)
                        <= i128::from(limit))),
        crate::model::Error::QuotaExceeded
    );
    revoke_bucket_access(&mut tx, bucket, &members).await?;
    for user in users {
        if !members.contains(&user) {
            queries::advance_user_authorization(user)
                .execute(&mut *tx)
                .await?;
        }
    }
    queries::change_project_bucket_quota(source, -1, -total.used_bytes, -total.object_count)
        .execute(&mut *tx)
        .await?;
    queries::change_project_bucket_quota(
        input.target_project,
        1,
        total.used_bytes,
        total.object_count,
    )
    .execute(&mut *tx)
    .await?;
    queries::transfer_bucket(bucket, input.target_project)
        .execute(&mut *tx)
        .await?;
    let row = queries::bucket_by_id(bucket).fetch_one(&mut *tx).await?;
    checkpoint_event(&mut tx,"bucket.transfer",&row.name,serde_json::json!({"bucket_id":bucket,"project_id":input.target_project,"previous_project":source,"revoked_member_bucket_grants":current.member_bucket_grants,"revoked_service_grants":current.service_grants,"revoked_token_bucket_grants":current.token_bucket_grants})).await?;
    tx.commit().await?;
    Ok(row)
}
