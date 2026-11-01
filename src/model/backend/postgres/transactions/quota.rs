use super::*;

pub(in super::super) async fn copy_chunk_references_in(
    db: &mut PgConnection,
    destination: Id,
    source: Id,
    offset: i64,
    end: i64,
    chunks: &[Id],
) -> Result<()> {
    require_writing_stream(db, destination).await?;
    let locked: Vec<Id> = queries::lock_ready_chunks(chunks)
        .fetch_all(&mut *db)
        .await?;
    ensure!(locked.len() == chunks.len(), "copy source is not ready");
    require_sources(db, None, chunks).await?;
    queries::copy_extents(destination, source, offset, end)
        .execute(&mut *db)
        .await?;
    queries::reference_chunks(chunks).execute(&mut *db).await?;
    queries::pin_chunks(chunks, destination)
        .execute(&mut *db)
        .await?;
    queries::resize_writing_stream(destination, end)
        .execute(&mut *db)
        .await?;
    Ok(())
}

pub(in super::super) async fn reserve_quota(
    pool: &PgPool,
    stream: Id,
    size: i64,
    padding: bool,
    upload: Option<(uuid::Uuid, i32)>,
) -> Result<i64> {
    Ok(queries::reserve_quota(
        stream,
        size,
        padding,
        upload.map(|u| u.0),
        upload.map(|u| u.1),
    )
    .fetch_one(pool)
    .await?)
}

pub(in super::super) async fn complete_quota(
    pool: &PgPool,
    upload: uuid::Uuid,
    stream: Id,
    size: i64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let bucket: uuid::Uuid = queries::lock_completing_upload(upload)
        .fetch_one(&mut *tx)
        .await?;
    queries::lock_stream(stream).execute(&mut *tx).await?;
    queries::lock_quota(bucket).execute(&mut *tx).await?;
    // Reuse the accepted parts' reservation, including omitted parts until publication succeeds.
    let changed = queries::complete_quota(upload, stream, size)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    anyhow::ensure!(changed == 1, "multipart quota reservation is missing");
    tx.commit().await?;
    Ok(())
}

#[async_trait::async_trait]
impl QuotaStore for Backend {
    async fn read(&self, principal: &Principal, scope: &str, id: Uuid) -> Result<Quota> {
        read_quota(&self.pool, principal, scope, id).await
    }
    async fn save(
        &self,
        principal: &Principal,
        scope: &str,
        id: Uuid,
        input: Limits,
    ) -> Result<Quota> {
        save_quota(&self.pool, principal, scope, id, input).await
    }

    async fn reserve(
        &self,
        stream: Id,
        size: i64,
        padding: bool,
        upload: Option<(uuid::Uuid, i32)>,
    ) -> Result<i64> {
        reserve_quota(&self.pool, stream, size, padding, upload)
            .await
            .map_err(normalize_error)
    }

    async fn complete(&self, upload: uuid::Uuid, stream: Id, size: i64) -> Result<()> {
        complete_quota(&self.pool, upload, stream, size)
            .await
            .map_err(normalize_error)
    }
}

async fn read_quota(pool: &PgPool, principal: &Principal, scope: &str, id: Uuid) -> Result<Quota> {
    validate_scope(scope)?;
    let mut tx = pool.begin().await?;
    let admin = principal.lock_identity(&mut tx).await?;
    let visible = if scope == "bucket" {
        Permit::for_action(principal.clone(), id, Action::Inspect)
            .lock(&mut tx)
            .await?;
        true
    } else if admin {
        true
    } else {
        if principal.token_id().is_some() {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
        let scope: Option<String> = queries::quota_membership(principal.user_id(), id)
            .fetch_optional(&mut *tx)
            .await?;
        scope.ok_or_else(|| s3s::s3_error!(AccessDenied))? == "all"
    };
    let result = queries::quota_scope(scope, id, visible)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    tx.commit().await?;
    Ok(result)
}

async fn save_quota(
    pool: &PgPool,
    principal: &Principal,
    scope: &str,
    id: Uuid,
    input: Limits,
) -> Result<Quota> {
    validate_scope(scope)?;
    let (bytes, inflight, buckets) = (
        number(&input.logical_bytes_limit)?,
        number(&input.inflight_bytes_limit)?,
        number(&input.bucket_count_limit)?,
    );
    if scope == "bucket" && (inflight.is_some() || buckets.is_some()) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = admin_transaction(pool, principal).await?;
    let changed = queries::update_quota(scope, id, bytes, inflight, buckets)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if changed != 1 {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    let mut detail = serde_json::to_value(input)?;
    detail[format!("{scope}_id")] = serde_json::json!(id);
    checkpoint_event(&mut tx, "quota.update", &id.to_string(), detail).await?;
    tx.commit().await?;
    read_quota(pool, principal, scope, id).await
}
