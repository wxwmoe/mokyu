use super::*;

pub(in super::super) async fn update_web_settings(
    pool: &SqlitePool,
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
