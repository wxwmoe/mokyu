use super::{
    listeners::serve_http,
    roles::{Role, shutdown_signal},
};
use crate::{
    config::node::NodeConfig,
    http::node::Gateway,
    node::{NodeContext, reads::Reads, work::Worker},
};
use anyhow::{Context, Result};
use axum::{Router, body::Body, extract::Request};
use futures_util::future::BoxFuture;
use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
};

async fn configured(node: &NodeContext) -> Result<()> {
    let mut state = node.subscribe();
    loop {
        if node.background_snapshot().is_ok() {
            return Ok(());
        }
        state.changed().await.context("node state stopped")?;
    }
}

async fn background(node: Arc<NodeContext>) -> Result<()> {
    configured(&node).await?;
    let reads = Reads::new(node.clone())?;
    let worker = Worker::new(node.clone())?;
    if node.client.identity.role == "worker" {
        node.serving
            .store(true, std::sync::atomic::Ordering::Release);
    }
    tokio::select! {
        result = worker.run() => result,
        result = reads.maintain() => result,
        result = node.report() => result,
        result = node.report_access() => result,
    }
}

async fn listeners(
    node: Arc<NodeContext>,
    role: Role,
    connections: Arc<Semaphore>,
    stopping: watch::Sender<bool>,
) -> Result<()> {
    if !matches!(role, Role::Data) {
        return std::future::pending().await;
    }
    configured(&node).await?;
    let budget = node.budget.get().context("node budget missing")?;
    connections.add_permits(budget.request_connections);
    let gateway = Gateway::new(Arc::new(Reads::new(node.clone())?))?;
    let service = gateway.service()?;
    let public = gateway.clone();
    let s3 = Router::new().fallback(move |request: Request| {
        let (gateway, service) = (gateway.clone(), service.clone());
        async move {
            gateway
                .s3(&service, request.map(s3s::Body::http_body_unsync))
                .await
                .map(Body::new)
        }
    });
    let web = Router::new().fallback(move |request: Request| {
        let public = public.clone();
        async move { public.public(request).await }
    });
    let s3_listener = TcpListener::bind(&node.config.instance.listen_s3)
        .await
        .context("S3 listener")?;
    let web_listener = TcpListener::bind(&node.config.instance.listen_web)
        .await
        .context("public listener")?;
    node.serving
        .store(true, std::sync::atomic::Ordering::Release);
    tokio::select! {
        result = serve_http(s3_listener, s3, connections.clone(), budget.data_slots, stopping.subscribe()) => result,
        result = serve_http(web_listener, web, connections, budget.data_slots, stopping.subscribe()) => result,
    }
}

async fn drained(node: &NodeContext, connections: &Semaphore, role: Role) -> Result<()> {
    if node.subscribe().borrow().snapshot.is_none() {
        return Ok(());
    }
    let limit = if matches!(role, Role::Data) {
        node.budget
            .get()
            .map_or(0, |budget| budget.request_connections)
    } else {
        0
    };
    while connections.available_permits() != limit {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut states = node.subscribe();
    states.borrow_and_update();
    node.refresh();
    loop {
        states.changed().await?;
        let state = states.borrow_and_update();
        if state.pending_uploads == Some(0) && node.resources.active() == 0 {
            node.background_snapshot()?;
            return Ok(());
        }
    }
}

pub(super) async fn run(config: NodeConfig, role: Role) -> Result<()> {
    let node = NodeContext::new(config, role.title()).await?;
    tracing::info!(version=env!("CARGO_PKG_VERSION"), role=role.title(), node_id=%node.client.identity.node_id, "starting Mokyu node");
    let status_listener = TcpListener::bind(&node.config.instance.listen_control)
        .await
        .context("status listener")?;
    let (stopping, _) = watch::channel(false);
    let (status_stopping, _) = watch::channel(false);
    let connections = Arc::new(Semaphore::new(0));
    let mut status: BoxFuture<'_, Result<()>> = Box::pin(serve_http(
        status_listener,
        crate::node::status::router(node.clone()),
        Arc::new(Semaphore::new(4)),
        4,
        status_stopping.subscribe(),
    ));
    let mut services = Box::pin(listeners(
        node.clone(),
        role,
        connections.clone(),
        stopping.clone(),
    ));
    let current = node.clone();
    let driver = async move {
        if let Err(error) = current.run().await {
            tracing::error!(%error, "node session stopped; status remains available");
        }
        std::future::pending::<()>().await;
    };
    tokio::pin!(driver);
    let mut workers: BoxFuture<'_, Result<()>> = Box::pin(background(node.clone()));
    let result = tokio::select! {
        result = &mut status => { status = Box::pin(std::future::pending()); result },
        result = &mut services => result,
        result = &mut workers => { workers = Box::pin(std::future::pending()); result.and_then(|_| anyhow::bail!("node workers stopped")) },
        _ = &mut driver => unreachable!(),
        _ = shutdown_signal() => Ok(()),
    };
    drop(services);
    node.serving
        .store(false, std::sync::atomic::Ordering::Release);
    stopping.send_replace(true);
    node.drain();
    tracing::info!(
        role = role.title(),
        "stopping listeners; draining active operations"
    );
    let timeout = node
        .background_snapshot()
        .ok()
        .map(|snapshot| snapshot.runtime().io.shutdown_seconds())
        .transpose()?
        .unwrap_or(30);
    let flush = tokio::time::timeout(Duration::from_secs(timeout), async {
        loop {
            tokio::select! {
                result = drained(&node, &connections, role) => return result,
                result = &mut status => { status = Box::pin(std::future::pending()); tracing::warn!(?result, "node status listener stopped"); },
                result = &mut workers => { workers = Box::pin(std::future::pending()); tracing::warn!(?result, "node workers stopped during drain"); },
                _ = &mut driver => unreachable!(),
            }
        }
    }).await;
    if matches!(flush, Ok(Ok(()))) {
        tracing::info!("shutdown flush completed");
    } else {
        tracing::warn!(?flush, pending_uploads=?node.subscribe().borrow().pending_uploads, active_grants=node.resources.active(), "shutdown flush incomplete; pending sources remain for recovery");
    }
    drop(workers);
    status_stopping.send_replace(true);
    if let Err(error) = node.close().await {
        tracing::warn!(code=?error.code(), "node close was not acknowledged; run expiry remains in effect");
    }
    result
}
