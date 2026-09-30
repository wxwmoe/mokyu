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

#[derive(Serialize, ToSchema, sqlx::FromRow)]
pub(super) struct BucketView {
    id: Uuid,
    project_id: Uuid,
    #[schema(value_type = Vec<crate::authorization::Action>)]
    actions: Vec<String>,
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
            project_id: bucket.project_id,
            actions: crate::authorization::Action::ALL
                .iter()
                .map(|a| a.name().into())
                .collect(),
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
        .routes(routes!(super::projects::list, super::projects::create))
        .routes(routes!(super::projects::update, super::projects::delete))
        .routes(routes!(super::users::list, super::users::create))
        .routes(routes!(
            super::users::get,
            super::users::update,
            super::users::delete
        ))
        .routes(routes!(super::users::reset))
        .routes(routes!(super::users::members))
        .routes(routes!(super::audit::list))
        .routes(routes!(super::audit::export))
        .routes(routes!(super::audit::get))
        .routes(routes!(super::quotas::get, super::quotas::put))
        .routes(routes!(super::uploads::create))
        .routes(routes!(super::uploads::list))
        .routes(routes!(super::uploads::get, super::uploads::abort))
        .routes(routes!(super::uploads::parts))
        .merge(
            OpenApiRouter::new()
                .routes(routes!(super::uploads::put_part))
                .layer(axum::extract::DefaultBodyLimit::disable()),
        )
        .merge(
            OpenApiRouter::new()
                .routes(routes!(super::uploads::complete))
                .layer(axum::extract::DefaultBodyLimit::max(2 * 1024 * 1024)),
        )
        .merge(
            OpenApiRouter::new()
                .routes(routes!(super::keys::list, super::keys::create))
                .routes(routes!(super::keys::update, super::keys::revoke))
                .routes(routes!(super::keys::grants))
                .routes(routes!(super::keys::rotate))
                .routes(routes!(super::tokens::list, super::tokens::create))
                .routes(routes!(super::tokens::update, super::tokens::revoke))
                .layer(axum::extract::DefaultBodyLimit::max(512 * 1024)),
        )
        .merge(
            OpenApiRouter::new()
                .routes(routes!(
                    super::users::put_member,
                    super::users::remove_member
                ))
                .layer(axum::extract::DefaultBodyLimit::max(512 * 1024)),
        )
        .routes(routes!(
            super::projects::get_mode,
            super::projects::set_mode
        ))
        .split_for_parts();
    api.info.title = "Mokyu management API".into();
    api.info.description = Some("Management endpoints accept a session cookie or a scoped Bearer token. Cookie mutations require the configured Origin and X-CSRF-Token. Account and token management require a session. S3 and public reads use separate listeners.".into());
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
        let mut ids = std::collections::HashSet::new();
        for path in schema["paths"].as_object().unwrap().values() {
            for operation in path.as_object().unwrap().values() {
                if let Some(id) = operation["operationId"].as_str() {
                    assert!(ids.insert(id), "duplicate operation ID: {id}");
                }
            }
        }
    }
}
