use super::super::Backend;
use super::super::queries::mirroring as q;
use super::*;

pub(in super::super) async fn request(
    backend: &Backend,
    input: crate::model::mirroring::Start,
) -> Result<Uuid> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    let task = request_in(&mut tx, input).await?;
    tx.commit().await?;
    Ok(task)
}

pub(in super::super) async fn request_in(
    tx: &mut super::super::queries::replica::Tx<'_>,
    input: crate::model::mirroring::Start,
) -> Result<Uuid> {
    queries::lock_bucket_id(input.bucket)
        .execute(&mut **tx)
        .await?;
    ensure!(
        q::enabled(input.bucket, input.revision)
            .fetch_one(&mut **tx)
            .await?,
        s3s::s3_error!(OperationAborted)
    );
    authorize(tx, input.bucket).await?;
    ensure!(
        q::target(input.bucket).fetch_one(&mut **tx).await? == Some(input.storage),
        s3s::s3_error!(OperationAborted)
    );
    q::insert_key(input.bucket, &input.key)
        .execute(&mut **tx)
        .await?;
    let (blocked, task, state, revision, missing_until, retry_after) =
        q::key(input.bucket, &input.key)
            .fetch_one(&mut **tx)
            .await?;
    ensure!(
        !blocked && (revision != input.revision || missing_until <= chrono::Utc::now().timestamp()),
        s3s::s3_error!(NoSuchKey)
    );
    ensure!(
        revision != input.revision || retry_after <= chrono::Utc::now().timestamp(),
        s3s::s3_error!(SlowDown, "origin retry is delayed")
    );
    ensure!(
        !q::occupied(input.bucket, &input.key)
            .fetch_one(&mut **tx)
            .await?,
        s3s::s3_error!(OperationAborted)
    );
    if let Some(task) = task.filter(|_| {
        revision == input.revision
            && matches!(state.as_deref(), Some("queued" | "running" | "paused"))
    }) {
        return Ok(task);
    }
    ensure!(
        q::active(input.bucket).fetch_one(&mut **tx).await? < 128,
        s3s::s3_error!(SlowDown, "origin queue is full")
    );
    for key in q::expired(input.bucket, chrono::Utc::now().timestamp())
        .fetch_all(&mut **tx)
        .await?
    {
        if key != input.key {
            q::erase(input.bucket, &key).execute(&mut **tx).await?;
        }
    }
    q::enqueue(&input).execute(&mut **tx).await?;
    q::assign(input.bucket, &input.key, input.task, input.revision)
        .execute(&mut **tx)
        .await?;
    queries::notify_work(input.task).execute(&mut **tx).await?;
    Ok(input.task)
}

pub(in super::super) async fn save(
    backend: &Backend,
    principal: &Principal,
    bucket: Uuid,
    revision: i64,
    config: &crate::model::mirroring::Config,
    encrypted: Vec<u8>,
) -> Result<i64> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    lock_bucket_settings(&mut tx, principal, bucket).await?;
    let mut permit = Permit::for_action(principal.clone(), bucket, Action::Settings);
    if config.rules.iter().any(|rule| rule.public_read) {
        permit.add(bucket, Action::Acl);
    }
    permit.lock(&mut tx).await?;
    let admin = queries::validate_identity(principal, &mut tx, true).await?;
    let project: Uuid = sqlx::query_scalar("SELECT project_id FROM buckets WHERE id=$1")
        .bind(bucket)
        .fetch_one(&mut *tx)
        .await?;
    for rule in &config.rules {
        for source in std::iter::once(&rule.source).chain(rule.backup.iter()) {
            match source {
                crate::model::mirroring::Source::Http { allow_private, .. } => {
                    ensure!(!allow_private || admin, s3s::s3_error!(AccessDenied))
                }
                crate::model::mirroring::Source::S3 { external_id, .. } => {
                    let permit = Permit::for_action(principal.clone(), *external_id, Action::Read);
                    let records = super::external::authorized(&mut tx, &permit).await?;
                    ensure!(
                        records
                            .iter()
                            .all(|record| record.bucket.project_id == project),
                        s3s::s3_error!(AccessDenied)
                    );
                }
            }
        }
    }
    let (current, _) = q::record(bucket).fetch_one(&mut *tx).await?;
    ensure!(current == revision, s3s::s3_error!(PreconditionFailed));
    let next = current
        .checked_add(1)
        .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
    q::save(
        bucket,
        next,
        config.enabled,
        &encrypted,
        crate::model::mirroring::Authority::new(principal, config),
    )
    .execute(&mut *tx)
    .await?;
    q::invalidate(bucket).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "bucket.mirroring",
        &bucket.to_string(),
        serde_json::json!({"enabled":config.enabled,"revision":next}),
    )
    .await?;
    tx.commit().await?;
    Ok(next)
}

pub(in super::super) async fn guard(
    tx: &mut super::super::queries::replica::Tx<'_>,
    task: crate::model::task::Execution,
) -> Result<()> {
    let (source, revision, bucket) = q::grant(task)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    authorize(tx, bucket).await?;
    let record = queries::task_record(task.id).fetch_one(&mut **tx).await?;
    let target: Uuid = serde_json::from_value(record.detail["storage_id"].clone())?;
    ensure!(
        q::target(bucket).fetch_one(&mut **tx).await? == Some(target),
        crate::model::Error::OperationAborted
    );
    if let Some(source) = source {
        ensure!(
            q::external(source, revision.context("origin revision missing")?, bucket)
                .fetch_optional(&mut **tx)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
    }
    Ok(())
}

pub(in super::super) async fn publication(
    tx: &mut super::super::queries::replica::Tx<'_>,
    stream: Id,
) -> Result<()> {
    if let Some((id, token)) = q::origin_owner(stream).fetch_optional(&mut **tx).await? {
        guard(tx, crate::model::task::Execution { id, token }).await?;
    }
    Ok(())
}

pub(in super::super) async fn claim(
    tx: &mut super::super::queries::replica::Tx<'_>,
    input: &StreamCreate,
    stream: Id,
) -> Result<()> {
    let (id, token) = input.owner.context("mirroring task owner missing")?;
    guard(tx, crate::model::task::Execution { id, token }).await?;
    q::ensure_claim(input.bucket, &input.key, stream)
        .execute(&mut **tx)
        .await?;
    q::claim(input.bucket, &input.key, stream, id)
        .execute(&mut **tx)
        .await?;
    let (old, current) = queries::lock_object_version(input.bucket, &input.key)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        old.is_none() && current == stream,
        crate::model::Error::OperationAborted
    );
    Ok(())
}

pub(in super::super) async fn bind_source(
    backend: &Backend,
    task: crate::model::task::Execution,
    source: Option<(Uuid, i64)>,
) -> Result<()> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    guard(&mut tx, task).await?;
    q::source(task.id, source).execute(&mut *tx).await?;
    guard(&mut tx, task).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn failed(
    backend: &Backend,
    task: crate::model::task::Execution,
    missing_ttl: Option<u32>,
) -> Result<()> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    guard(&mut tx, task).await?;
    q::negative(
        task.id,
        missing_ttl
            .filter(|v| *v > 0)
            .map_or(0, |v| chrono::Utc::now().timestamp() + i64::from(v)),
        if missing_ttl.is_none() {
            chrono::Utc::now().timestamp() + 30
        } else {
            0
        },
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn authorize(
    tx: &mut super::super::queries::replica::Tx<'_>,
    bucket: Uuid,
) -> Result<()> {
    let authority = q::authority(bucket)
        .fetch_one(&mut **tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(AccessDenied))?
        .0;
    let admin = if let Principal::User { id, revision, .. } = &authority.principal {
        // Saved rules outlive login sessions, but not their owner's authorization.
        let (role, current) = q::saved_user(*id)
            .fetch_optional(&mut **tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
        ensure!(current == *revision, s3s::s3_error!(AccessDenied));
        role == "admin"
    } else {
        queries::validate_identity(&authority.principal, &mut **tx, true).await?
    };
    let actions = queries::bucket_actions(&authority.principal, &mut **tx, bucket, admin).await?;
    ensure!(
        actions.contains(&Action::Settings)
            && (!authority.public || actions.contains(&Action::Acl))
            && (!authority.private || admin),
        s3s::s3_error!(AccessDenied)
    );
    for source in authority.sources {
        let record =
            queries::external::page(&authority.principal, admin, None, None, Some(source), 1)
                .fetch_optional(&mut **tx)
                .await?
                .ok_or_else(|| s3s::s3_error!(AccessDenied))?
                .record()?;
        ensure!(
            record.bucket.enabled && record.bucket.actions.contains(&Action::Read),
            s3s::s3_error!(AccessDenied)
        );
        ensure!(
            q::external(source, record.bucket.revision, bucket)
                .fetch_optional(&mut **tx)
                .await?
                .is_some(),
            s3s::s3_error!(AccessDenied)
        );
    }
    Ok(())
}

pub(in super::super) async fn unblock(
    backend: &Backend,
    principal: &Principal,
    bucket: Uuid,
    key: &str,
) -> Result<()> {
    ensure!(
        !key.is_empty() && key.len() <= 1024 && !key.contains('\0'),
        s3s::s3_error!(InvalidArgument)
    );
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    lock_bucket_settings(&mut tx, principal, bucket).await?;
    q::erase(bucket, key).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "bucket.mirroring_unblock",
        &bucket.to_string(),
        serde_json::json!({"key":key}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}
