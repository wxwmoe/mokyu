use super::*;

pub(super) async fn security_audit(
    db: &mut SqliteConnection,
    user: Uuid,
    action: &str,
    success: bool,
    request: &str,
) -> Result<()> {
    queries::security_audit(user, action, success, request)
        .execute(db)
        .await?;
    Ok(())
}

pub(crate) async fn checkpoint_event(
    tx: &mut Transaction<'_, Sqlite>,
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

pub(in super::super) async fn admin_transaction(
    pool: &SqlitePool,
    principal: &Principal,
) -> Result<Transaction<'static, Sqlite>> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    if !queries::validate_identity(principal, &mut tx).await? {
        return Err(denied());
    }
    Ok(tx)
}

pub(in super::super) async fn ensure_runtime_settings(
    pool: &SqlitePool,
    defaults: &[(&str, Value)],
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    for (section, value) in defaults {
        queries::ensure_runtime_setting(section, value)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn runtime_settings_sections(
    pool: &SqlitePool,
    principal: &Principal,
) -> Result<Vec<(String, Value, i64)>> {
    let mut tx = pool.begin().await?;
    if !queries::validate_identity(principal, &mut tx).await? {
        return Err(denied());
    }
    let rows = queries::runtime_settings().fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok(rows)
}

pub(in super::super) async fn runtime_settings_section(
    pool: &SqlitePool,
    principal: &Principal,
    section: &str,
) -> Result<RuntimeSettingsRow> {
    let mut tx = pool.begin().await?;
    if !queries::validate_identity(principal, &mut tx).await? {
        return Err(denied());
    }
    let row = queries::runtime_settings_row(section)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn save_runtime_settings(
    pool: &SqlitePool,
    principal: &Principal,
    section: &str,
    revision: Option<i64>,
    value: Value,
) -> Result<RuntimeSettingsRow> {
    let mut tx = admin_transaction(pool, principal).await?;
    check_upload_stage(&mut tx, section, &value).await?;
    if section == "io" && value["sqlite_synchronous"] == "off" {
        ensure!(
            !queries::nonces_reserved().fetch_one(&mut *tx).await?,
            s3s::s3_error!(
                InvalidArgument,
                "SQLite OFF synchronization is unavailable after encryption nonce reservations"
            )
        );
    }
    let revision = match revision {
        Some(revision) => revision,
        None => {
            queries::lock_runtime_settings(section)
                .fetch_one(&mut *tx)
                .await?
        }
    };
    let changed =
        queries::save_runtime_settings(section, value, principal.user_id().is_none(), revision)
            .execute(&mut *tx)
            .await?
            .rows_affected();
    ensure!(
        changed == 1,
        s3s::s3_error!(
            OperationAborted,
            "settings revision conflict; reload before saving"
        )
    );
    let row = queries::runtime_settings_row(section)
        .fetch_one(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "runtime.settings",
        section,
        serde_json::json!({"revision":row.revision}),
    )
    .await?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn reset_runtime_settings(
    pool: &SqlitePool,
    principal: &Principal,
    section: &str,
    value: Value,
) -> Result<RuntimeSettingsRow> {
    let mut tx = admin_transaction(pool, principal).await?;
    check_upload_stage(&mut tx, section, &value).await?;
    let changed = queries::reset_runtime_settings(section, value, principal.user_id().is_none())
        .execute(&mut *tx)
        .await?
        .rows_affected();
    ensure!(changed == 1, "runtime settings section is missing");
    let row = queries::runtime_settings_row(section)
        .fetch_one(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "runtime.settings",
        section,
        serde_json::json!({"revision":row.revision}),
    )
    .await?;
    tx.commit().await?;
    Ok(row)
}

async fn check_upload_stage(
    tx: &mut Transaction<'_, Sqlite>,
    section: &str,
    value: &Value,
) -> Result<()> {
    if section == "uploads" && queries::guard_stage_mode().fetch_one(&mut **tx).await? != "single" {
        let stage = value
            .get("staging_storage_id")
            .and_then(Value::as_str)
            .and_then(|v| Uuid::parse_str(v).ok());
        ensure!(
            queries::valid_stage(stage).fetch_one(&mut **tx).await?,
            crate::model::runtime::ModeConflict(
                "cluster mode requires an enabled S3 uploads.staging_storage_id"
            )
        );
    }
    Ok(())
}
