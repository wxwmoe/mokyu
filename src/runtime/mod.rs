mod args;
mod listeners;
pub(crate) mod locks;
mod node;
mod roles;
mod shutdown;
pub(crate) mod testing;

use self::{
    args::{Args, Command, ToolCommand},
    roles::{Role, run_role},
};
use crate::{app, cli, cluster, config, control};
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
        Some(Command::Node { .. }) => Role::Node,
        _ => Role::Single,
    };
    let testing = matches!(args.command, Some(Command::Testing));
    let explicit = args.config.as_deref();
    let path = explicit.unwrap_or_else(|| std::path::Path::new("/config/config.toml"));
    set_process_title(if testing { "testing" } else { role.title() });
    let start_maintenance = args.maintenance || role.maintenance(args.command.as_ref());
    if let Some(Command::Cli { command }) = args.command {
        set_process_title("cli");
        return tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(cli::client(explicit, command));
    }
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("TLS provider initialization failed"))?;
    if matches!(role, Role::Data | Role::Worker | Role::Node) {
        anyhow::ensure!(
            !start_maintenance,
            "set deployment maintenance on control before starting nodes"
        );
        let (config, budget) = config::node::NodeConfig::load(path, role.title())?;
        return tokio::runtime::Builder::new_multi_thread()
            .worker_threads(budget.available_cpus)
            .max_blocking_threads((budget.cpu_jobs * 2).max(8))
            .enable_all()
            .build()?
            .block_on(node::run(config, role));
    }
    let (settings, installation) = if testing {
        anyhow::ensure!(
            !start_maintenance,
            "testing does not support maintenance startup"
        );
        let (settings, installation) = testing::prepare(explicit)?;
        (settings, Some(installation))
    } else {
        (config::Config::load(path)?, None)
    };
    let (config, secrets, budget) = settings;
    config.validate_role(role.title())?;
    tracing::info!(version=env!("CARGO_PKG_VERSION"),resources=%serde_json::to_string(&budget)?,"starting Mokyu");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(budget.available_cpus)
        .max_blocking_threads((budget.cpu_jobs * 2).max(8))
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let (lock, mut installation) = match installation {
            Some((lock, state)) => (lock, Some(state)),
            None => (lock_directory(&config.instance.data_path)?, None),
        };
        let (app, mut owner) = app::App::from_directory(
            config,
            secrets,
            budget,
            start_maintenance,
            role.uses_storage(),
            role.title(),
            lock,
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
        if let Some(state) = installation.as_mut() {
            anyhow::ensure!(
                !startup_blocked && !app.shared_storage_only(),
                "testing installation requires single mode"
            );
            state.seed(&app).await?;
        }
        if matches!(role, Role::Single) && !startup_blocked {
            app.recover().await?;
        }
        if installation.is_none() && matches!(role, Role::Single | Role::Control) {
            control::account::initialize(&app).await?;
        }
        run_role(app.clone(), role).await?;
        Ok(())
    })
}

pub(crate) fn lock_directory(path: &std::path::Path) -> anyhow::Result<std::fs::File> {
    let lock = claim_directory(path)?;
    anyhow::ensure!(
        !testing::marked(path)?,
        "this data directory belongs to a testing installation"
    );
    Ok(lock)
}

fn claim_directory(path: &std::path::Path) -> anyhow::Result<std::fs::File> {
    use anyhow::Context;
    let open = || {
        std::fs::create_dir_all(path)?;
        std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.join("gateway.lock"))
    };
    let lock = match open() {
        Err(e)
            if e.kind() == std::io::ErrorKind::PermissionDenied
                && path.starts_with("/data")
                && !path
                    .components()
                    .any(|c| c == std::path::Component::ParentDir)
                && std::process::Command::new("/opt/mokyu/bin/mokyu-init-data")
                    .status()
                    .is_ok_and(|status| status.success()) =>
        {
            open()
        }
        result => result,
    }
    .with_context(|| format!("cannot write data directory {}", path.display()))?;
    fs2::FileExt::try_lock_exclusive(&lock).context("another gateway owns this data directory")?;
    Ok(lock)
}
