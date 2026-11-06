use crate::{app, http, status::stats};
use anyhow::Result;
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use tokio::{
    net::TcpListener,
    sync::{Notify, Semaphore},
};
use tracing::Instrument;

pub(super) fn build_s3(app: Arc<app::App>) -> Result<s3s::service::S3Service> {
    let mut builder = s3s::service::S3ServiceBuilder::new(http::gateway::Gateway(app.clone()));
    builder.set_auth(http::gateway::Gateway(app.clone()));
    builder.set_access(http::gateway::Gateway(app.clone()));
    http::s3::configure(&mut builder, &app.config.requests)?;
    Ok(builder.build())
}

pub(super) async fn serve_s3(
    listener: TcpListener,
    service: s3s::service::S3Service,
    connections: Arc<Semaphore>,
    app: Arc<app::App>,
) -> Result<()> {
    let controls = Arc::new(Semaphore::new(app.budget.data_slots));
    loop {
        let (socket, _) = listener.accept().await?;
        // Compressed frames must not wait for delayed ACKs between small writes.
        socket.set_nodelay(true)?;
        let permit = match connections.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => continue,
        };
        let s3 = service.clone();
        let app = app.clone();
        let controls = controls.clone();
        let mut stopping = app.stop_connections.subscribe();
        let streams = app.budget.data_slots;
        let (requests, activity) = http::transport::Requests::new();
        let service = hyper::service::service_fn(
            move |mut request: hyper::Request<hyper::body::Incoming>| {
                let active = requests.start(request.version());
                let app = app.clone();
                let s3 = s3.clone();
                let controls = controls.clone();
                async move {
                    let observation = stats::Request::new(
                        &app.statistics.http[0],
                        "s3",
                        request.method().clone(),
                    );
                    request.extensions_mut().insert(observation.context.clone());
                    let mut response = http::s3_http(app, s3, request, controls)
                        .instrument(observation.span.clone())
                        .await;
                    stats::s3_request_id(&mut response, &observation.context.id);
                    Ok::<_, std::convert::Infallible>(
                        observation
                            .response(response, true)
                            .map(|body| http::transport::ResponseBody::new(body, active)),
                    )
                }
            },
        );
        tokio::spawn(async move {
            let _permit = permit;
            let builder = http::transport::builder(streams);
            let connection = builder.serve_connection(TokioIo::new(socket), service);
            tokio::pin!(connection);
            let stopping_now = *stopping.borrow_and_update();
            let result = if stopping_now {
                connection.as_mut().graceful_shutdown();
                connection.await
            } else {
                tokio::select! {
                    result = &mut connection => result,
                    _ = http::transport::idle(activity) => return,
                    _ = stopping.changed() => {
                        connection.as_mut().graceful_shutdown();
                        connection.await
                    }
                }
            };
            if let Err(e) = result {
                tracing::debug!(error=%e,"S3 connection closed");
            }
        });
    }
}
pub(super) async fn serve_router(
    listener: TcpListener,
    router: axum::Router,
    connections: Arc<Semaphore>,
    app: Arc<app::App>,
) -> Result<()> {
    let router = router.layer(axum::middleware::from_fn(http::transport::router_request));
    serve_http(
        listener,
        router,
        connections,
        app.budget.data_slots,
        app.stop_connections.subscribe(),
    )
    .await
}

pub(super) async fn serve_http(
    listener: TcpListener,
    router: axum::Router,
    connections: Arc<Semaphore>,
    streams: usize,
    stopping: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    loop {
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let permit = match connections.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => continue,
        };
        let service = hyper_util::service::TowerToHyperService::new(router.clone());
        let (requests, activity) = http::transport::Requests::new();
        let drain = Arc::new(Notify::new());
        let close = drain.clone();
        let service = hyper::service::service_fn(
            move |mut request: hyper::Request<hyper::body::Incoming>| {
                let active = requests.start(request.version());
                request.extensions_mut().insert(close.clone());
                let response = hyper::service::Service::call(&service, request);
                async move {
                    response.await.map(|response| {
                        response.map(|body| http::transport::ResponseBody::new(body, active))
                    })
                }
            },
        );
        let mut stopping = stopping.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let builder = http::transport::builder(streams);
            let connection = builder.serve_connection(TokioIo::new(socket), service);
            tokio::pin!(connection);
            let stopping_now = *stopping.borrow_and_update();
            let result = if stopping_now {
                connection.as_mut().graceful_shutdown();
                connection.await
            } else {
                tokio::select! {
                    result = &mut connection => result,
                    _ = http::transport::idle(activity) => return,
                    _ = drain.notified() => {
                        connection.as_mut().graceful_shutdown();
                        // An unresponsive HTTP/2 peer may never acknowledge graceful shutdown.
                        match tokio::time::timeout(std::time::Duration::from_secs(2), connection).await {
                            Ok(result) => result,
                            Err(_) => return,
                        }
                    }
                    _ = stopping.changed() => {
                        connection.as_mut().graceful_shutdown();
                        connection.await
                    }
                }
            };
            if let Err(e) = result {
                tracing::debug!(error=%e,"HTTP connection closed");
            }
        });
    }
}
