use super::{contract, operations};
use crate::{
    app::App,
    authorization::Principal,
    http::{HttpError, unauthorized},
};
use axum::{Json, extract::State, http::HeaderMap};
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;

#[utoipa::path(
    post,
    operation_id = "cli",
    path = "/api/v1/cli",
    request_body = Value,
    responses((status = 200, body = Value), (status = 400, body = contract::ErrorBody))
)]
pub(super) async fn cli(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(command): Json<crate::cli::Command>,
) -> Result<Json<Value>, HttpError> {
    let principal = if app.role == "single" {
        deployment_identity(&app, &headers)?;
        if app.mode_mismatch().await? {
            Principal::Bootstrap
        } else {
            Principal::Local
        }
    } else {
        cluster_identity(&app, &headers).await?
    };
    if matches!(&principal, Principal::Bootstrap) && !command.allows_bootstrap() {
        return Err(
            anyhow::anyhow!("node recovery is required before this command can run").into(),
        );
    }
    let result = operations::execute_as(&app, &principal, command).await?;
    Ok(Json(json!({"ok": true, "result": result})))
}

fn deployment_identity(app: &App, headers: &HeaderMap) -> Result<(), HttpError> {
    let key = headers
        .get("x-mokyu-deployment-key")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(unauthorized)?;
    crate::cluster::verify_deployment_key(app, key).map_err(|_| unauthorized())
}

async fn cluster_identity(app: &App, headers: &HeaderMap) -> Result<Principal, HttpError> {
    let node_id = headers
        .get("x-mokyu-node-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| Uuid::parse_str(v).ok());
    let credential = headers
        .get("x-mokyu-node-credential")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let row: Option<(String, Vec<u8>)> = if let Some(node_id) = node_id {
        app.cluster()?.credential(node_id).await?
    } else {
        None
    };
    Ok(if let Some((role, encrypted)) = row {
        let expected = crate::content::codec::unprotect(
            &encrypted,
            &app.secrets.credential_key,
            crate::cluster::credential_binding(node_id.expect("node id present")).as_bytes(),
        )
        .map_err(|_| unauthorized())?;
        if aws_lc_rs::constant_time::verify_slices_are_equal(
            expected.as_bytes(),
            credential.as_deref().unwrap_or_default().as_bytes(),
        )
        .is_err()
        {
            return Err(anyhow::anyhow!("invalid node credential").into());
        }
        Principal::Node {
            id: node_id.expect("node id present"),
            role,
        }
    } else {
        deployment_identity(app, headers)?;
        Principal::Bootstrap
    })
}
