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

pub(in super::super) async fn change_mode(
    tx: &mut Transaction<'_, Sqlite>,
    mode: &str,
) -> Result<()> {
    use crate::model::runtime::ModeConflict;
    ensure!(
        matches!(mode, "single" | "single_control"),
        ModeConflict("cluster mode requires PostgreSQL")
    );
    let (_, current) = queries::storage_deployment().fetch_one(&mut **tx).await?;
    if current == mode {
        return Ok(());
    }
    let (maintenance, tasks, uploads, sources) =
        queries::mode_blockers().fetch_one(&mut **tx).await?;
    crate::model::runtime::check_drain(maintenance, tasks, uploads, sources)?;
    ensure!(
        !queries::node_registry::active_grants()
            .fetch_one(&mut **tx)
            .await?,
        ModeConflict("drain active node reads and storage permits before changing deployment mode")
    );
    if mode == "single_control" {
        ensure!(
            !queries::local_storages().fetch_one(&mut **tx).await?,
            ModeConflict(
                "local storages exist; migrate or remove every local storage before changing deployment mode"
            )
        );
        ensure!(
            queries::active_staging()
                .fetch_optional(&mut **tx)
                .await?
                .is_some(),
            ModeConflict("cluster mode requires an enabled S3 uploads.staging_storage_id")
        );
    }
    if mode == "single" {
        ensure!(
            !queries::node_registry::has_registered_peers()
                .fetch_one(&mut **tx)
                .await?,
            ModeConflict("release all registered remote nodes before returning to single mode")
        );
    }
    queries::preserve_task_mode(&current)
        .execute(&mut **tx)
        .await?;
    queries::set_deployment_mode(mode)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
