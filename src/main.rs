mod admin;
mod app;
mod codec;
mod config;
mod db;
#[cfg(feature = "fault-injection")]
mod faults;
mod http;
mod lifecycle;
mod listing;
mod multipart;
mod s3;
mod stats;
mod storage;
mod tasks;
mod upload;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::Semaphore};
use tracing::Instrument;

#[derive(Parser)]
#[command(version, about = "S3-compatible media gateway")]
struct Args {
    #[arg(long, global = true, default_value = "/config/config.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
}
#[derive(Subcommand)]
enum Command {
    Serve {
        /// Start with writes and remote GC disabled, including after a database restore.
        #[arg(long)]
        maintenance: bool,
    },
    /// Generate fresh 32-byte key material without accessing service state.
    Keygen,
    Cli {
        #[arg(long)]
        socket: Option<PathBuf>,
        #[command(subcommand)]
        command: admin::Command,
    },
}
fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "media_gateway=info,s3s=warn".into()),
        )
        .with_target(false)
        .init();
    if matches!(args.command, Some(Command::Keygen)) {
        println!("{}", admin::random_secret()?);
        return Ok(());
    }
    let start_maintenance = matches!(&args.command, Some(Command::Serve { maintenance: true }));
    if let Some(Command::Cli { socket, command }) = args.command {
        let socket = if let Some(socket) = socket {
            socket
        } else if args.config.exists() {
            let text = std::fs::read_to_string(&args.config)?;
            let config: config::Config =
                toml::from_str(&text).map_err(|_| anyhow::anyhow!("invalid TOML configuration"))?;
            config.listen.admin_socket
        } else {
            PathBuf::from("/run/media-gateway/admin.sock")
        };
        return tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(admin::client(&socket, command));
    }
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("TLS provider initialization failed"))?;
    let (config, secrets, budget) = config::Config::load(&args.config)?;
    tracing::info!(version=env!("CARGO_PKG_VERSION"),resources=%serde_json::to_string(&budget)?,"starting media gateway");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(budget.available_cpus)
        .max_blocking_threads((budget.cpu_jobs * 2).max(8))
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let(app,mut owner)=app::App::new(config,secrets,budget).await?;
        if start_maintenance{sqlx::query("UPDATE gateway_meta SET maintenance=true").execute(&app.db).await?;app.maintenance.store(true,std::sync::atomic::Ordering::Release);}
        tokio::spawn(async move{loop{tokio::time::sleep(Duration::from_secs(1)).await;let alive=tokio::time::timeout(Duration::from_secs(3),sqlx::query("SELECT 1").execute(&mut owner)).await;if !matches!(alive,Ok(Ok(_))){tracing::error!("database ownership connection lost; terminating");std::process::exit(1);}}});
        app.recover().await?;
        let s3_listener=TcpListener::bind(&app.config.listen.s3).await.context("S3 listener")?;
        let web_listener=TcpListener::bind(&app.config.listen.web).await.context("web listener")?;
        let manage_listener=TcpListener::bind(&app.config.listen.manage).await.context("manage listener")?;
        let connections=Arc::new(Semaphore::new(app.budget.connections));
        let mut builder=s3s::service::S3ServiceBuilder::new(s3::Gateway(app.clone()));
        builder.set_auth(s3::Gateway(app.clone()));builder.set_access(s3::Gateway(app.clone()));
        if let Some(domain)=&app.config.listen.s3_domain{builder.set_host(s3s::host::SingleDomain::new(domain)?.with_cname_fallback(false));}
        let mut cfg=s3s::config::S3Config::default();cfg.xml_max_body_size=2*1024*1024;cfg.aws_chunked_stream_max_chunk_size=config::bytes(&app.config.listen.aws_chunk_limit)? as usize;cfg.enable_sig_v2=false;cfg.sig_v4_allowed_services=vec!["s3".into()];cfg.expected_region=Some(app.config.listen.region.parse()?);
        builder.set_config(Arc::new(s3s::config::StaticConfigProvider::new(Arc::new(cfg))));
        let s3=builder.build();
        tokio::select! {
            result=serve_s3(s3_listener,s3,connections.clone(),app.clone())=>result?,
            result=serve_router(web_listener,http::public_router(app.clone()),connections.clone())=>result?,
            result=serve_router(manage_listener,http::manage_router(app.clone()),connections)=>result?,
            result=admin::serve(app.clone())=>result?,
            result=lifecycle::run(app.clone())=>result?,
            result=lifecycle::run_history(app.clone())=>result?,
            result=tasks::run(app.clone())=>result?,
            result=stats::run(app.clone())=>result?,
            _=shutdown_signal()=>{tracing::info!("stopping listeners; draining active data operations");}
        }
        app.maintenance.store(true,std::sync::atomic::Ordering::Release);
        let drain=async{while !app.active.lock().unwrap().is_empty(){tokio::time::sleep(Duration::from_millis(100)).await;}};
        let _=tokio::time::timeout(Duration::from_secs(30),drain).await;Ok(())
    })
}
async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {_=term.recv()=>{},_=tokio::signal::ctrl_c()=>{}}
}
async fn serve_s3(
    listener: TcpListener,
    service: s3s::service::S3Service,
    connections: Arc<Semaphore>,
    app: Arc<app::App>,
) -> Result<()> {
    let controls = Arc::new(Semaphore::new(app.budget.data_slots));
    loop {
        let (socket, _) = listener.accept().await?;
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
async fn serve_router(
    listener: TcpListener,
    router: axum::Router,
    connections: Arc<Semaphore>,
) -> Result<()> {
    loop {
        let (socket, _) = listener.accept().await?;
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
