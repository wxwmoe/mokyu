use crate::app::{App, Bucket};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};
use uuid::Uuid;

#[derive(Serialize, ToSchema)]
pub(super) struct ApiInfo {
    product: &'static str,
    version: &'static str,
    contract: &'static str,
}

#[derive(Serialize, ToSchema)]
pub(super) struct LoginReply {
    pub csrf_token: String,
}

#[derive(Serialize, ToSchema)]
pub(super) struct SessionView {
    #[serde(flatten)]
    pub account: super::account::Profile,
    pub csrf_token: String,
}

#[derive(Serialize, ToSchema)]
pub(super) struct BucketView {
    id: Uuid,
    name: String,
    state: String,
    cors: Value,
    website_enabled: bool,
    index_document: String,
    error_document: String,
    created_at: DateTime<Utc>,
}

impl From<Bucket> for BucketView {
    fn from(bucket: Bucket) -> Self {
        Self {
            id: bucket.id,
            name: bucket.name,
            state: bucket.state,
            cors: bucket.cors,
            website_enabled: bucket.website_enabled,
            index_document: bucket.index_document,
            error_document: bucket.error_document,
            created_at: bucket.created_at,
        }
    }
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ErrorBody {
    pub error: String,
    pub code: String,
    pub request_id: Option<String>,
}

#[utoipa::path(get, path = "/api/info", responses((status = 200, body = ApiInfo)))]
async fn info() -> Json<ApiInfo> {
    Json(ApiInfo {
        product: "Mokyu",
        version: env!("CARGO_PKG_VERSION"),
        contract: "mokyu-manage-1",
    })
}

pub(super) fn routes() -> (Router<Arc<App>>, utoipa::openapi::OpenApi) {
    let (router, mut api) = OpenApiRouter::new()
        .routes(routes!(info))
        .routes(routes!(super::login))
        .routes(routes!(super::logout))
        .routes(routes!(super::session))
        .routes(routes!(super::status))
        .routes(routes!(super::buckets))
        .routes(routes!(super::website, super::save_website))
        .routes(routes!(super::account::me, super::account::save))
        .routes(routes!(super::account::password))
        .routes(routes!(super::account::reauthenticate))
        .routes(routes!(
            super::account::sessions,
            super::account::revoke_others
        ))
        .routes(routes!(super::account::revoke_session))
        .routes(routes!(super::account::bootstrap))
        .routes(routes!(super::account::setup))
        .split_for_parts();
    api.info.title = "Mokyu management API".into();
    api.info.description = Some("Management endpoints use a session cookie. Mutations require the configured Origin and X-CSRF-Token. S3 and public reads use separate listeners.".into());
    (router, api)
}

pub(crate) async fn errors(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let request_id = request
        .extensions()
        .get::<crate::stats::RequestContext>()
        .map(|c| c.id.clone());
    let response = next.run(request).await;
    let status = response.status();
    if !status.is_client_error() && !status.is_server_error() {
        return response;
    }
    let code = response
        .extensions()
        .get::<crate::http::ErrorCode>()
        .map(|c| c.0.clone())
        .unwrap_or_else(|| {
            match status.as_u16() {
                400 | 422 => "InvalidArgument",
                401 => "Unauthorized",
                403 => "AccessDenied",
                404 => "NotFound",
                405 => "MethodNotAllowed",
                408 => "RequestTimeout",
                413 => "RequestTooLarge",
                429 | 503 => "SlowDown",
                _ => "RequestFailed",
            }
            .into()
        });
    let headers = response.headers().clone();
    let mut result = (
        status,
        Json(ErrorBody {
            error: status.canonical_reason().unwrap_or("Request failed").into(),
            code,
            request_id,
        }),
    )
        .into_response();
    for (name, value) in &headers {
        if !matches!(
            name.as_str(),
            "content-length" | "content-type" | "content-encoding"
        ) {
            result.headers_mut().insert(name.clone(), value.clone());
        }
    }
    result
}

#[cfg(test)]
mod tests {
    #[test]
    fn registered_routes_and_schema_share_the_contract() {
        let schema = serde_json::to_value(super::routes().1).unwrap();
        for (path, method) in [
            ("/api/info", "get"),
            ("/api/login", "post"),
            ("/api/logout", "post"),
            ("/api/session", "get"),
            ("/api/buckets", "get"),
            ("/api/buckets/{bucket}/website", "put"),
        ] {
            assert!(schema["paths"][path][method].is_object(), "{method} {path}");
        }
        let schemas = &schema["components"]["schemas"];
        assert_eq!(schemas["Profile"]["properties"]["id"]["format"], "uuid");
        assert!(
            schemas["BucketView"]["properties"]
                .get("secret_key")
                .is_none()
        );
        assert_eq!(schemas["Login"]["additionalProperties"], false);
    }
}
