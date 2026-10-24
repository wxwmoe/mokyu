pub(crate) mod account;
pub(crate) mod actions;
mod assets;
pub(crate) mod audit;
pub(crate) mod auth;
pub(crate) mod buckets;
pub(crate) mod catalog;
pub(crate) mod cli;
pub(crate) mod cluster;
mod contract;
pub(crate) mod insights;
pub(crate) mod keys;
mod maintenance;
pub(crate) mod managed_keys;
pub(crate) mod media;
pub(crate) mod objects;
pub(crate) mod operations;
pub(crate) mod projects;
pub(crate) mod quotas;
pub(crate) mod service;
pub(crate) mod settings;
pub(crate) mod storage;
mod storage_catalog;
pub(crate) mod storage_policy;
pub(crate) mod system;
pub(crate) mod tasks;
pub(crate) mod tokens;
mod uploads;
pub(crate) mod users;

pub(crate) use auth::{Identity, origin, token};
pub(crate) use objects::Website;

use crate::{app::App, http::observe};
use axum::{
    Router,
    extract::{DefaultBodyLimit, Extension},
    http::HeaderValue,
};
use std::sync::Arc;

pub fn schema() -> utoipa::openapi::OpenApi {
    contract::routes().1
}

pub fn router(app: Arc<App>) -> Router {
    let avatar_origin = app
        .config
        .web
        .gravatar_origin()
        .expect("validated Gravatar origin");
    let web_policy: HeaderValue = assets::CSP
        .replace(
            "img-src 'self' blob: data:;",
            &format!("img-src 'self' blob: data: {avatar_origin};"),
        )
        .parse()
        .expect("validated content security policy");
    let (api, schema) = contract::routes();
    let schema = serde_json::to_value(schema).expect("serializable API schema");
    let admin = Router::new()
        .merge(api)
        .fallback(assets::serve)
        .layer(DefaultBodyLimit::max(16 * 1024))
        .layer(Extension(schema))
        .layer(axum::middleware::from_fn_with_state(
            app.clone(),
            auth::access_gate,
        ))
        .layer(axum::middleware::from_fn_with_state(
            app.clone(),
            crate::http::compression::json,
        ))
        .layer(axum::middleware::from_fn_with_state(
            web_policy,
            system::web_headers,
        ))
        .layer(axum::middleware::from_fn(contract::errors))
        .with_state(app.clone())
        .layer(axum::middleware::from_fn_with_state(
            (app.clone(), 2usize),
            observe,
        ));
    if app.role != "control" {
        return admin;
    }
    let rpc = crate::rpc::server::router(app);
    Router::new().fallback_service(tower::service_fn(move |request: axum::extract::Request| {
        let is_rpc = request.uri().path().starts_with("/mokyu.node.")
            || request
                .headers()
                .get("content-type")
                .is_some_and(|value| value.as_bytes().starts_with(b"application/grpc"));
        tower::ServiceExt::oneshot(if is_rpc { rpc.clone() } else { admin.clone() }, request)
    }))
}
