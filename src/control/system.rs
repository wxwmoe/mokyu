use super::{assets, auth::Identity};
use crate::http::{HttpError, unauthorized};
use axum::{
    Json,
    body::Body,
    extract::{Extension, State},
    http::{HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::time::Duration;
static ACTION_CONCURRENCY: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

#[utoipa::path(
    get,
    operation_id = "openapi",
    path = "/api/v1/openapi.json",
    params(),
    responses((status = 200, content_type = "application/json", body = Value))
)]
pub(super) async fn openapi(Extension(schema): Extension<Value>) -> Json<Value> {
    Json(schema)
}

pub(super) async fn limit_object_actions(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if request.extensions().get::<Identity>().is_none() {
        return unauthorized().into_response();
    }
    let Ok(_permit) = ACTION_CONCURRENCY.try_acquire() else {
        return HttpError(s3s::s3_error!(SlowDown).into()).into_response();
    };
    match tokio::time::timeout(Duration::from_secs(60), next.run(request)).await {
        Ok(response) => response,
        Err(_) => (
            StatusCode::REQUEST_TIMEOUT,
            Json(json!({"error":"Request Timeout"})),
        )
            .into_response(),
    }
}

pub(super) async fn web_headers(
    State(policy): State<HeaderValue>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let mut response = next.run(request).await;
    if response
        .headers()
        .get("content-security-policy")
        .is_some_and(|value| value == assets::CSP)
    {
        response
            .headers_mut()
            .insert("content-security-policy", policy);
    }
    if !response.headers().contains_key("cache-control") {
        response.headers_mut().insert(
            "cache-control",
            HeaderValue::from_static("private, no-store"),
        );
    }
    for (name, value) in [
        ("x-content-type-options", "nosniff"),
        ("referrer-policy", "no-referrer"),
        ("x-frame-options", "DENY"),
    ] {
        response
            .headers_mut()
            .insert(name, HeaderValue::from_static(value));
    }
    response
}
