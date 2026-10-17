use crate::{app, config, http, status::stats};
use anyhow::Result;
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::Semaphore};
use tracing::Instrument;

pub(super) fn build_s3(app: Arc<app::App>) -> Result<s3s::service::S3Service> {
    let mut builder = s3s::service::S3ServiceBuilder::new(http::gateway::Gateway(app.clone()));
    builder.set_auth(http::gateway::Gateway(app.clone()));
    builder.set_access(http::gateway::Gateway(app.clone()));
    if let Some(domain) = &app.config.requests.s3_domain {
        builder.set_host(s3s::host::SingleDomain::new(domain)?.with_cname_fallback(false));
    }
    let mut cfg = s3s::config::S3Config::default();
    cfg.xml_max_body_size = 2 * 1024 * 1024;
    cfg.aws_chunked_stream_max_chunk_size =
        config::bytes(&app.config.requests.s3_chunk_limit)? as usize;
    cfg.enable_sig_v2 = false;
    cfg.sig_v4_allowed_services = vec!["s3".into()];
    cfg.expected_region = Some(app.config.requests.s3_region.parse()?);
    builder.set_config(Arc::new(s3s::config::StaticConfigProvider::new(Arc::new(
        cfg,
    ))));
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
        let service = hyper::service::service_fn(
            move |mut request: hyper::Request<hyper::body::Incoming>| {
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
                    Ok::<_, std::convert::Infallible>(observation.response(response, true))
                }
            },
        );
        tokio::spawn(async move {
            let _permit = permit;
            let result = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(Duration::from_secs(15))
                .max_buf_size(32 * 1024)
                .serve_connection(TokioIo::new(socket), service)
                .await;
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
) -> Result<()> {
    loop {
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let permit = match connections.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => continue,
        };
        let service = hyper_util::service::TowerToHyperService::new(router.clone());
        tokio::spawn(async move {
            let _permit = permit;
            let result = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(Duration::from_secs(15))
                .max_buf_size(32 * 1024)
                .serve_connection(TokioIo::new(socket), service)
                .await;
            if let Err(e) = result {
                tracing::debug!(error=%e,"HTTP connection closed");
            }
        });
    }
}
