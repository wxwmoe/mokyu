mod args;
mod listeners;
mod roles;

use self::{
    args::{Args, Command, ToolCommand},
    roles::{Role, run_role},
};
use crate::{app, cli, cluster, config, control, worker};
use anyhow::Result;
use clap::Parser;
use std::time::Duration;

#[cfg(target_os = "linux")]
fn set_process_title(role: &str) {
    let _ = std::fs::write("/proc/self/comm", format!("mokyu: {role}"));
}
#[cfg(not(target_os = "linux"))]
fn set_process_title(_role: &str) {}
pub(crate) fn run() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mokyu=info,s3s=warn".into()),
        )
        .with_target(false)
        .init();
    if let Some(Command::Tool { command }) = args.command.as_ref() {
        match command {
            ToolCommand::Keygen => println!("{}", cli::deployment_key()?),
            ToolCommand::Openapi => {
                println!("{}", serde_json::to_string_pretty(&control::schema())?)
            }
        }
        return Ok(());
    }
    let role = match args.command.as_ref() {
        Some(Command::Control { .. }) => Role::Control,
        Some(Command::Worker { .. }) => Role::Worker,
        Some(Command::Data { .. }) => Role::Data,
        _ => Role::Single,
    };
    set_process_title(role.title());
    let start_maintenance = args.maintenance || role.maintenance(args.command.as_ref());
    if let Some(Command::Cli { command }) = args.command {
        set_process_title("cli");
        return tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(cli::client(&args.config, command));
    }
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("TLS provider initialization failed"))?;
    let (config, secrets, budget) = config::Config::load(&args.config)?;
    config.validate_role(role.title())?;
    tracing::info!(version=env!("CARGO_PKG_VERSION"),resources=%serde_json::to_string(&budget)?,"starting Mokyu");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(budget.available_cpus)
        .max_blocking_threads((budget.cpu_jobs * 2).max(8))
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let (app, mut owner) = app::App::new(
            config,
            secrets,
            budget,
            start_maintenance,
            role.uses_storage(),
            role.title(),
        )
        .await?;
        cluster::initialize(&app, role.title()).await?;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let alive = tokio::time::timeout(Duration::from_secs(3), owner.check()).await;
                if !matches!(alive, Ok(Ok(_))) {
                    tracing::error!("database ownership connection lost; terminating");
                    std::process::exit(1);
                }
            }
        });
        let startup_blocked = app.mode_mismatch().await? || app.recovery_required();
        if matches!(role, Role::Single) && !startup_blocked {
            app.recover().await?;
        }
        if matches!(role, Role::Worker) && !startup_blocked {
            worker::scheduler::recover_stale(&app).await?;
        }
        if matches!(role, Role::Single | Role::Control) {
            control::account::initialize(&app).await?;
        }
        run_role(app.clone(), role).await?;
        Ok(())
    })
}
