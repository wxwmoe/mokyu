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
    fn cache_flush_wait_keeps_wire_contract() {
        for args in [
            vec!["mokyu", "cli", "cache", "flush"],
            vec![
                "mokyu",
                "cli",
                "cache",
                "flush",
                "--wait",
                "--timeout",
                "2m",
            ],
        ] {
            let Some(Command::Cli { command }) = Args::try_parse_from(args).unwrap().command else {
                panic!("CLI command expected")
            };
            assert_eq!(
                serde_json::to_value(&command).unwrap(),
                serde_json::json!({"Cache":"Flush"})
            );
        }
        let restored: cli::Command =
            serde_json::from_value(serde_json::json!({"Cache":"Flush"})).unwrap();
        assert!(matches!(
            restored,
            cli::Command::Cache(cli::Cache::Flush { wait: false, .. })
        ));
        assert!(
            Args::try_parse_from(["mokyu", "cli", "cache", "flush", "--timeout", "1s"]).is_err()
        );
    }

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

    #[test]
    fn combined_node_has_one_role_and_provisioned_identity() {
        let args = Args::try_parse_from(["mokyu", "node"]).unwrap();
        assert!(matches!(
            args.command,
            Some(Command::Node { maintenance: false })
        ));
        let args = Args::try_parse_from(["mokyu", "cli", "cluster", "create", "combined", "node"])
            .unwrap();
        assert!(
            matches!(args.command, Some(Command::Cli { command: cli::Command::Cluster(cli::Cluster::Create { role, .. }) }) if role == "node")
        );
        for args in [
            vec!["mokyu", "data+worker"],
            vec!["mokyu", "data", "worker"],
        ] {
            assert!(Args::try_parse_from(args).is_err());
        }
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
    /// Run data services and shared background work in one node.
    Node {
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
