use crate::cli;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "S3-compatible media gateway")]
pub(super) struct Args {
    #[arg(
        short = 'c',
        long,
        global = true,
        default_value = "/config/config.toml"
    )]
    pub(super) config: PathBuf,
    /// Start in maintenance mode before recovering uploads and tasks.
    #[arg(long)]
    pub(super) maintenance: bool,
    #[command(subcommand)]
    pub(super) command: Option<Command>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_startup() {
        let single = Args::try_parse_from(["mokyu", "--maintenance"]).unwrap();
        assert!(single.maintenance && single.command.is_none());
        let control = Args::try_parse_from(["mokyu", "control", "--maintenance"]).unwrap();
        assert!(matches!(
            control.command,
            Some(Command::Control { maintenance: true })
        ));
        assert!(!Args::try_parse_from(["mokyu"]).unwrap().maintenance);
    }
}
#[derive(Subcommand)]
pub(super) enum Command {
    /// Run the management/control role.
    Control {
        #[arg(long)]
        maintenance: bool,
    },
    /// Run the background worker role.
    Worker {
        #[arg(long)]
        maintenance: bool,
    },
    /// Run the S3 and public-read data role.
    Data {
        #[arg(long)]
        maintenance: bool,
    },
    /// Run an offline tool without accessing service state.
    #[command(name = "tool")]
    Tool {
        #[command(subcommand)]
        command: ToolCommand,
    },
    Cli {
        #[command(subcommand)]
        command: cli::Command,
    },
}
#[derive(Subcommand)]
pub(super) enum ToolCommand {
    /// Generate a deployment key without accessing service state.
    Keygen,
    /// Print the management API schema without accessing service state.
    Openapi,
}
