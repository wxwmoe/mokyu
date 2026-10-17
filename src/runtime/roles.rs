use super::{
    args::Command,
    listeners::{build_s3, serve_router, serve_s3},
};
use crate::{
    app, cluster, control, http,
    status::{access, stats},
    worker::{self, lifecycle, maintenance},
};
use anyhow::{Context, Result};
use std::{sync::Arc, time::Duration};
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
        Role::Worker => run_worker(app).await,
        Role::Data => run_data(app).await,
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
    tokio::select! {
        result = serve_s3(s3_listener, s3, connections.clone(), app.clone()) => result?,
        result = serve_router(web_listener, http::public_router(app.clone()), connections.clone()) => result?,
        result = serve_router(control_listener, control::router(app.clone()), connections.clone()) => result?,
        result = lifecycle::run(app.clone()) => result?,
        result = worker::scheduler::run(app.clone()) => result?,
        result = maintenance::run(app.clone()) => result?,
        result = stats::run(app.clone()) => result?,
        result = control::insights::runtime_loop(app.clone()) => result?,
        result = access::run(app.clone()) => result?,
        result = app.storage.compression.run() => result?,
        _ = shutdown_signal() => tracing::info!(role="single", "stopping listeners; draining active operations"),
    }
    drain(app).await
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
        result = serve_router(control_listener, control::router(app.clone()), connections) => result?,
        result = maintenance::run(app.clone()) => result?,
        result = stats::run(app.clone()) => result?,
        _ = shutdown_signal() => tracing::info!(role="control", "stopping listeners; draining active operations"),
    }
    drain(app).await
}

async fn run_worker(app: Arc<app::App>) -> Result<()> {
    if app.mode_mismatch().await? || app.recovery_required() {
        return run_status(app).await;
    }
    let status_listener = TcpListener::bind(&app.config.instance.listen_control)
        .await
        .context("status listener")?;
    tokio::select! {
        result = cluster::report::run(app.clone(), false) => result?,
        result = cluster::node_loop(app.clone(), "worker") => result?,
        result = serve_router(status_listener, http::status_router(app.clone()), Arc::new(Semaphore::new(4))) => result?,
        result = worker::scheduler::run(app.clone()) => result?,
        result = app.storage.compression.run() => result?,
        _ = shutdown_signal() => tracing::info!(role="worker", "stopping listeners; draining active operations"),
    }
    drain(app).await
}

async fn run_data(app: Arc<app::App>) -> Result<()> {
    if app.mode_mismatch().await? || app.recovery_required() {
        return run_data_restricted(app).await;
    }
    let s3_listener = TcpListener::bind(&app.config.instance.listen_s3)
        .await
        .context("S3 listener")?;
    let web_listener = TcpListener::bind(&app.config.instance.listen_web)
        .await
        .context("public listener")?;
    let status_listener = TcpListener::bind(&app.config.instance.listen_control)
        .await
        .context("status listener")?;
    let connections = Arc::new(Semaphore::new(app.budget.request_connections));
    let s3 = build_s3(app.clone())?;
    tokio::select! {
        result = cluster::report::run(app.clone(), false) => result?,
        result = cluster::node_loop(app.clone(), "data") => result?,
        result = worker::scheduler::run(app.clone()) => result?,
        result = serve_s3(s3_listener, s3, connections.clone(), app.clone()) => result?,
        result = serve_router(web_listener, http::public_router(app.clone()), connections.clone()) => result?,
        result = serve_router(status_listener, http::status_router(app.clone()), Arc::new(Semaphore::new(4))) => result?,
        result = access::run(app.clone()) => result?,
        result = app.storage.compression.run() => result?,
        _ = shutdown_signal() => tracing::info!(role="data", "stopping listeners; draining active operations"),
    }
    drain(app).await
}

async fn run_control_restricted(app: Arc<app::App>) -> Result<()> {
    let control_listener = TcpListener::bind(&app.config.instance.listen_control)
        .await
        .context("control listener")?;
    let connections = Arc::new(Semaphore::new(app.budget.request_connections));
    tokio::select! {
        result = cluster::report::run(app.clone(), true) => result?,
        result = cluster::status::restricted_loop(app.clone()) => result?,
        result = serve_router(control_listener, control::router(app.clone()), connections) => result?,
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
        result = serve_router(web_listener, control::router(app.clone()), connections.clone()) => result?,
        result = serve_router(control_listener, control::router(app.clone()), connections) => result?,
        _ = shutdown_signal() => tracing::info!(role="single", "stopping mismatch listeners"),
    }
    Ok(())
}

async fn run_data_restricted(app: Arc<app::App>) -> Result<()> {
    let s3_listener = TcpListener::bind(&app.config.instance.listen_s3)
        .await
        .context("S3 listener")?;
    let web_listener = TcpListener::bind(&app.config.instance.listen_web)
        .await
        .context("public listener")?;
    let control_listener = TcpListener::bind(&app.config.instance.listen_control)
        .await
        .context("status listener")?;
    let connections = Arc::new(Semaphore::new(app.budget.request_connections));
    let s3 = build_s3(app.clone())?;
    tokio::select! {
        result = cluster::report::run(app.clone(), true) => result?,
        result = cluster::status::restricted_loop(app.clone()) => result?,
        result = serve_s3(s3_listener, s3, connections.clone(), app.clone()) => result?,
        result = serve_router(web_listener, http::public_router(app.clone()), connections.clone()) => result?,
        result = serve_router(control_listener, http::status_router(app.clone()), connections) => result?,
        _ = shutdown_signal() => tracing::info!(role="data", "stopping restricted listeners"),
    }
    Ok(())
}

async fn run_status(app: Arc<app::App>) -> Result<()> {
    let listener = TcpListener::bind(&app.config.instance.listen_control)
        .await
        .context("status listener")?;
    tokio::select! {
        result = cluster::report::run(app.clone(), true) => result,
        result = cluster::status::restricted_loop(app.clone()) => result,
        result = serve_router(listener, http::status_router(app.clone()), Arc::new(Semaphore::new(4))) => result,
    }
}

async fn drain(app: Arc<app::App>) -> Result<()> {
    {
        let _coord = app.coord.lock().await;
        app.draining
            .store(true, std::sync::atomic::Ordering::Release);
        app.maintenance
            .store(true, std::sync::atomic::Ordering::Release);
    }
    #[cfg(feature = "fault-injection")]
    crate::faults::point("runtime-draining").await;
    let drain = async {
        while !app.active.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(30), drain).await;
    Ok(())
}
async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {_=term.recv()=>{},_=tokio::signal::ctrl_c()=>{}}
}
