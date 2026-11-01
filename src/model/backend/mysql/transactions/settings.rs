use super::*;

pub(in super::super) async fn runtime_settings_section(
    pool: &MySqlPool,
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
    pool: &MySqlPool,
    principal: &Principal,
    section: &str,
    revision: Option<i64>,
    value: Value,
) -> Result<RuntimeSettingsRow> {
    let mut tx = admin_transaction(pool, principal).await?;
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
    pool: &MySqlPool,
    principal: &Principal,
    section: &str,
    value: Value,
) -> Result<RuntimeSettingsRow> {
    let mut tx = admin_transaction(pool, principal).await?;
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

pub(in super::super) async fn update_web_settings(
    pool: &MySqlPool,
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
