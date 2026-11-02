use super::*;

pub(in super::super) async fn runtime_settings_sections(
    pool: &PgPool,
    principal: &Principal,
) -> Result<Vec<(String, Value, i64)>> {
    let mut tx = admin_transaction(pool, principal).await?;
    let rows = queries::runtime_settings().fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok(rows)
}

pub(in super::super) async fn runtime_settings_section(
    pool: &PgPool,
    principal: &Principal,
    section: &str,
) -> Result<RuntimeSettingsRow> {
    let mut tx = admin_transaction(pool, principal).await?;
    let row = queries::runtime_settings_row(section)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn save_runtime_settings(
    pool: &PgPool,
    principal: &Principal,
    section: &str,
    revision: Option<i64>,
    value: Value,
) -> Result<RuntimeSettingsRow> {
    let mut tx = admin_transaction(pool, principal).await?;
    check_upload_stage(&mut tx, section, &value).await?;
    let revision = match revision {
        Some(revision) => revision,
        None => {
            queries::lock_runtime_settings(section)
                .fetch_one(&mut *tx)
                .await?
        }
    };
    let row = queries::save_runtime_settings(section, value, principal.user_id(), revision)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| {
            s3s::s3_error!(
                OperationAborted,
                "settings revision conflict; reload before saving"
            )
        })?;
    checkpoint_event(
        &mut tx,
        "runtime.settings",
        section,
        json!({"revision":row.revision}),
    )
    .await?;
    queries::notify_task(format!("settings:{section}:{}", row.revision))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn reset_runtime_settings(
    pool: &PgPool,
    principal: &Principal,
    section: &str,
    value: Value,
) -> Result<RuntimeSettingsRow> {
    let mut tx = admin_transaction(pool, principal).await?;
    check_upload_stage(&mut tx, section, &value).await?;
    let row = queries::reset_runtime_settings(section, value, principal.user_id())
        .fetch_one(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "runtime.settings",
        section,
        json!({"revision":row.revision}),
    )
    .await?;
    queries::notify_task(format!("settings:{section}:{}", row.revision))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn update_web_settings(
    pool: &PgPool,
    principal: &Principal,
    input: ControlSettingsPatch,
) -> Result<(crate::config::WebSettings, i64)> {
    let mut tx = admin_transaction(pool, principal).await?;
    let current = queries::lock_web_settings().fetch_one(&mut *tx).await?;
    let mut control = current.control()?;
    if let Some(value) = input.origin {
        control.origin = value;
    }
    if let Some(value) = input.secure_cookie {
        control.secure_cookie = value;
    }
    if let Some(value) = input.session_lifetime {
        control.session_lifetime = value;
    }
    if let Some(value) = input.gravatar_base_url {
        control.gravatar_base_url = value;
    }
    control.validate()?;
    if let Some(revision) = input.revision {
        anyhow::ensure!(
            revision == current.revision,
            s3s::s3_error!(
                OperationAborted,
                "settings revision conflict; reload before saving"
            )
        );
    }
    let value = serde_json::json!({
        "origin": control.origin,
        "secure_cookie": control.secure_cookie,
        "session_lifetime": control.session_lifetime,
        "gravatar_base_url": control.gravatar_base_url,
    });
    let next_revision = current
        .revision
        .checked_add(1)
        .context("settings revision overflow")?;
    queries::save_web_settings(value, next_revision, principal.user_id(), current.revision)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "control.settings",
        "",
        serde_json::json!({
            "origin": control.origin,
            "secure_cookie": control.secure_cookie,
            "session_lifetime": control.session_lifetime,
            "gravatar_base_url": control.gravatar_base_url,
        }),
    )
    .await?;
    tx.commit().await?;
    Ok((control, next_revision))
}

pub(in super::super) async fn list_managed_keys(
    pool: &PgPool,
    principal: &Principal,
) -> Result<Vec<ManagedKeyView>> {
    let tx = admin_transaction(pool, principal).await?;
    tx.commit().await?;
    Ok(queries::managed_key_views()
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(queries::managed_key_view)
        .collect())
}

async fn check_upload_stage(
    tx: &mut Transaction<'_, Postgres>,
    section: &str,
    value: &Value,
) -> Result<()> {
    if section == "uploads"
        && queries::deployment_mode_lock().fetch_one(&mut **tx).await? != "single"
    {
        let stage = value
            .get("staging_storage_id")
            .and_then(Value::as_str)
            .and_then(|v| Uuid::parse_str(v).ok());
        ensure!(
            queries::cluster_staging_valid(stage)
                .fetch_one(&mut **tx)
                .await?,
            crate::model::runtime::ModeConflict(
                "cluster mode requires an enabled S3 uploads.staging_storage_id"
            )
        );
    }
    Ok(())
}
