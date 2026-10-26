use super::{
    args::Command,
    listeners::{build_s3, serve_router, serve_s3},
    shutdown::drain,
};
use crate::{
    app, cluster, control, http,
    status::{access, stats},
    worker::{lifecycle, maintenance},
};
use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::{net::TcpListener, sync::Semaphore};

#[derive(Clone, Copy)]
pub(super) enum Role {
    Single,
    Control,
    Worker,
    Data,
}
impl Role {
    pub(super) fn title(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Control => "control",
            Self::Worker => "worker",
            Self::Data => "data",
        }
    }
    pub(super) fn uses_storage(self) -> bool {
        !matches!(self, Self::Control)
    }
    pub(super) fn maintenance(self, command: Option<&Command>) -> bool {
        match command {
            Some(Command::Control { maintenance })
            | Some(Command::Worker { maintenance })
            | Some(Command::Data { maintenance }) => *maintenance,
            _ => false,
        }
    }
}

pub(super) async fn run_role(app: Arc<app::App>, role: Role) -> Result<()> {
    match role {
        Role::Single => run_single(app).await,
        Role::Control => run_control(app).await,
        Role::Worker | Role::Data => anyhow::bail!("node roles require the API runtime"),
    }
}

async fn run_single(app: Arc<app::App>) -> Result<()> {
    if app.mode_mismatch().await? || app.recovery_required() {
        return run_single_mismatch(app).await;
    }
    let s3_listener = TcpListener::bind(&app.config.instance.listen_s3)
        .await
        .context("S3 listener")?;
    let web_listener = TcpListener::bind(&app.config.instance.listen_web)
        .await
        .context("public listener")?;
    let control_listener = TcpListener::bind(&app.config.instance.listen_control)
        .await
        .context("control listener")?;
    let connections = Arc::new(Semaphore::new(app.budget.request_connections));
    let s3 = build_s3(app.clone())?;
    let background = super::shutdown::background(app.clone());
    tokio::pin!(background);
    tokio::select! {
        result = &mut background => { result?; anyhow::bail!("storage workers stopped"); },
        result = serve_s3(s3_listener, s3, connections.clone(), app.clone()) => result?,
        result = serve_router(web_listener, http::public_router(app.clone()), connections.clone(), app.clone()) => result?,
        result = serve_router(control_listener, control::router(app.clone()), connections.clone(), app.clone()) => result?,
        result = lifecycle::run(app.clone()) => result?,
        result = maintenance::run(app.clone()) => result?,
        result = stats::run(app.clone()) => result?,
        result = control::insights::runtime_loop(app.clone()) => result?,
        result = access::run(app.clone()) => result?,
        _ = shutdown_signal() => tracing::info!(role="single", "stopping listeners; draining active operations"),
    }
    drain(app, background).await
}

async fn run_control(app: Arc<app::App>) -> Result<()> {
    if app.mode_mismatch().await? || app.recovery_required() {
        return run_control_restricted(app).await;
    }
    let control_listener = TcpListener::bind(&app.config.instance.listen_control)
        .await
        .context("control listener")?;
    let connections = Arc::new(Semaphore::new(app.budget.request_connections));
    tokio::select! {
        result = cluster::report::run(app.clone(), false) => result?,
        result = cluster::control_loop(app.clone()) => result?,
        result = cluster::report::aggregate(app.clone()) => result?,
        result = access::run(app.clone()) => result?,
        result = crate::worker::scheduler::run(app.clone()) => result?,
        result = serve_router(control_listener, control::router(app.clone()), connections, app.clone()) => result?,
        result = maintenance::run(app.clone()) => result?,
        result = stats::run(app.clone()) => result?,
        _ = shutdown_signal() => tracing::info!(role="control", "stopping listeners; draining active operations"),
    }
    drain(app, std::future::pending()).await
}

async fn run_control_restricted(app: Arc<app::App>) -> Result<()> {
    let control_listener = TcpListener::bind(&app.config.instance.listen_control)
        .await
        .context("control listener")?;
    let connections = Arc::new(Semaphore::new(app.budget.request_connections));
    tokio::select! {
        result = cluster::report::run(app.clone(), true) => result?,
        result = cluster::status::restricted_loop(app.clone()) => result?,
        result = serve_router(control_listener, control::router(app.clone()), connections, app.clone()) => result?,
        _ = shutdown_signal() => tracing::info!(role="control", "stopping restricted listener"),
    }
    Ok(())
}

async fn run_single_mismatch(app: Arc<app::App>) -> Result<()> {
    let s3_listener = TcpListener::bind(&app.config.instance.listen_s3)
        .await
        .context("S3 listener")?;
    let web_listener = TcpListener::bind(&app.config.instance.listen_web)
        .await
        .context("public listener")?;
    let control_listener = TcpListener::bind(&app.config.instance.listen_control)
        .await
        .context("control listener")?;
    let connections = Arc::new(Semaphore::new(app.budget.request_connections));
    let s3 = build_s3(app.clone())?;
    tokio::select! {
        result = serve_s3(s3_listener, s3, connections.clone(), app.clone()) => result?,
        result = serve_router(web_listener, control::router(app.clone()), connections.clone(), app.clone()) => result?,
        result = serve_router(control_listener, control::router(app.clone()), connections, app.clone()) => result?,
        _ = shutdown_signal() => tracing::info!(role="single", "stopping mismatch listeners"),
    }
    Ok(())
}

pub(super) async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {_=term.recv()=>{},_=tokio::signal::ctrl_c()=>{}}
}
