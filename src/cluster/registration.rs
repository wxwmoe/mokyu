use super::identity::{
    NodeState, credential, credential_binding, node_state_path, read_state, role_name, write_state,
};
use crate::{app::App, codec};
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

pub async fn initialize(app: &App, role: &str) -> Result<()> {
    let mode: String = sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton")
        .fetch_one(&app.db)
        .await?;
    if role == "single" {
        let path = node_state_path(app);
        let state = if let Some(state) = read_state(&path).await? {
            state
        } else {
            let name = role_name(app, role)?;
            let existing: Option<(Uuid, bool, String)> =
                sqlx::query_as("SELECT id,enabled,role FROM nodes WHERE name=$1")
                    .bind(&name)
                    .fetch_optional(&app.db)
                    .await?;
            let credential = credential()?;
            let node_id = if let Some((node_id, enabled, role)) = existing {
                if enabled || role != "control" {
                    sqlx::query("UPDATE nodes SET status='recovery_required',recovery_required=true,updated_at=now() WHERE id=$1")
                        .bind(node_id)
                        .execute(&app.db)
                        .await?;
                    app.recovery_required
                        .store(true, std::sync::atomic::Ordering::Release);
                    return Ok(());
                }
                let encrypted = codec::protect(
                    credential.as_bytes(),
                    &app.secrets.credential_key,
                    credential_binding(node_id).as_bytes(),
                )?;
                sqlx::query("UPDATE nodes SET credential_encrypted=$2,enabled=true,status='registered',recovery_required=false,last_seen_at=now(),updated_at=now() WHERE id=$1")
                    .bind(node_id)
                    .bind(encrypted)
                    .execute(&app.db)
                    .await?;
                node_id
            } else {
                let node_id = Uuid::new_v4();
                let encrypted = codec::protect(
                    credential.as_bytes(),
                    &app.secrets.credential_key,
                    credential_binding(node_id).as_bytes(),
                )?;
                sqlx::query("INSERT INTO nodes(id,name,role,credential_encrypted,last_seen_at) VALUES($1,$2,'control',$3,now())")
                    .bind(node_id)
                    .bind(name)
                    .bind(encrypted)
                    .execute(&app.db)
                    .await?;
                node_id
            };
            let state = NodeState {
                node_id,
                credential,
            };
            write_state(&path, &state).await?;
            state
        };
        let mut tx = app.db.begin().await?;
        validate_node(app, &mut *tx, &state, "control").await?;
        sqlx::query("UPDATE nodes SET last_seen_at=now(),updated_at=now() WHERE id=$1")
            .bind(state.node_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        ensure!(
            app.node.set(state).is_ok(),
            "node identity was already initialized"
        );
        if mode != "single" {
            tracing::warn!("single service is waiting for deployment mode change");
        }
        return Ok(());
    }
    if mode != "cluster" {
        tracing::warn!(role, "service role is waiting for cluster deployment mode");
        return Ok(());
    }
    if matches!(role, "data" | "worker") {
        app.storage.shared_staging_ready().await?;
    }
    let path = node_state_path(app);
    let state = if let Some(state) = read_state(&path).await? {
        state
    } else {
        let name = role_name(app, role)?;
        let existing: Option<(Uuid, bool, String)> =
            sqlx::query_as("SELECT id,enabled,role FROM nodes WHERE name=$1")
                .bind(&name)
                .fetch_optional(&app.db)
                .await?;
        let credential = credential()?;
        let node_id = if let Some((node_id, enabled, registered_role)) = existing {
            if enabled || registered_role != role {
                sqlx::query("UPDATE nodes SET status='recovery_required',recovery_required=true,updated_at=now() WHERE id=$1")
                    .bind(node_id)
                    .execute(&app.db)
                    .await?;
                app.recovery_required
                    .store(true, std::sync::atomic::Ordering::Release);
                return Ok(());
            }
            let encrypted = codec::protect(
                credential.as_bytes(),
                &app.secrets.credential_key,
                credential_binding(node_id).as_bytes(),
            )?;
            sqlx::query("UPDATE nodes SET credential_encrypted=$2,enabled=true,status='registered',recovery_required=false,last_seen_at=now(),updated_at=now() WHERE id=$1")
                .bind(node_id)
                .bind(encrypted)
                .execute(&app.db)
                .await?;
            node_id
        } else {
            let node_id = Uuid::new_v4();
            let encrypted = codec::protect(
                credential.as_bytes(),
                &app.secrets.credential_key,
                credential_binding(node_id).as_bytes(),
            )?;
            sqlx::query("INSERT INTO nodes(id,name,role,credential_encrypted,last_seen_at) VALUES($1,$2,$3,$4,now())")
                .bind(node_id)
                .bind(name)
                .bind(role)
                .bind(encrypted)
                .execute(&app.db)
                .await?;
            node_id
        };
        let state = NodeState {
            node_id,
            credential,
        };
        write_state(&path, &state).await?;
        state
    };
    let mut tx = app.db.begin().await?;
    validate_node(app, &mut *tx, &state, role).await?;
    sqlx::query("UPDATE nodes SET last_seen_at=now(),updated_at=now() WHERE id=$1")
        .bind(state.node_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    ensure!(
        app.node.set(state).is_ok(),
        "node identity was already initialized"
    );
    Ok(())
}

pub async fn recover(app: &App) -> Result<()> {
    ensure!(
        matches!(app.role.as_str(), "single" | "control"),
        "only a local control node can recover"
    );
    let name = role_name(app, &app.role)?;
    let role = if app.role == "single" {
        "control"
    } else {
        "control"
    };
    let mut tx = app.db.begin().await?;
    let registered: Option<(Uuid, String)> =
        sqlx::query_as("SELECT id,role FROM nodes WHERE name=$1 FOR UPDATE")
            .bind(&name)
            .fetch_optional(&mut *tx)
            .await?;
    if let Some((id, registered_role)) = registered {
        ensure!(
            registered_role == role,
            "registered node role does not match"
        );
        sqlx::query("UPDATE nodes SET enabled=false,credential_encrypted=NULL,last_seen_at=NULL,status='released',recovery_required=false,updated_at=now() WHERE id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE tasks SET state=CASE WHEN execution_scope='node' THEN 'paused' ELSE 'queued' END,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE lease_node_id=$1 AND state='running'")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM storage_permits WHERE holder_id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    let path = node_state_path(app);
    let _ = tokio::fs::remove_file(path).await;
    app.recovery_required
        .store(false, std::sync::atomic::Ordering::Release);
    initialize(app, &app.role).await
}

pub(crate) async fn validate_node(
    app: &App,
    db: &mut sqlx::PgConnection,
    state: &NodeState,
    role: &str,
) -> Result<()> {
    let record: Option<(String, bool, Option<Vec<u8>>)> = sqlx::query_as(
        "SELECT role,enabled,credential_encrypted FROM nodes WHERE id=$1 FOR UPDATE",
    )
    .bind(state.node_id)
    .fetch_optional(&mut *db)
    .await?;
    let (registered_role, enabled, encrypted) =
        record.context("registered node no longer exists")?;
    ensure!(
        enabled && registered_role == role,
        "registered node is disabled or belongs to another role"
    );
    let expected = codec::unprotect(
        encrypted.as_deref().context("node has no credential")?,
        &app.secrets.credential_key,
        credential_binding(state.node_id).as_bytes(),
    )?;
    ensure!(
        aws_lc_rs::constant_time::verify_slices_are_equal(
            expected.as_bytes(),
            state.credential.as_bytes()
        )
        .is_ok(),
        "invalid node credential"
    );
    Ok(())
}
