use super::{Budget, Instance, RuntimeSettings};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use std::path::Path;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NodeConfig {
    pub(crate) instance: Instance,
}

impl NodeConfig {
    pub(crate) fn load(path: &Path, role: &str) -> Result<(Self, Budget)> {
        let text = std::fs::read_to_string(path).context("cannot read configuration")?;
        let config = Self::parse(&text, role)?;
        let budget = Budget::for_runtime(&config.instance, &RuntimeSettings::default(), &[])?;
        Ok((config, budget))
    }

    pub(crate) fn parse(text: &str, role: &str) -> Result<Self> {
        ensure!(
            matches!(role, "data" | "worker"),
            "node configuration requires data or worker role"
        );
        let config: Self = toml::from_str(text).map_err(|_| {
            anyhow::anyhow!("invalid node configuration, unknown field or wrong type")
        })?;
        ensure!(
            config.instance.deployment_key.is_none()
                && config.instance.database_connections.is_none()
                && config.instance.database_read_connections.is_none(),
            "data and worker must not configure deployment_key or database budgets"
        );
        crate::rpc::client::control_uri(
            config
                .instance
                .control_url
                .as_deref()
                .context("instance.control_url is required")?,
        )?;
        super::limits::validate_runtime(&config.instance, &RuntimeSettings::default())?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_configuration_has_no_database_or_root_key() {
        let valid = "[instance]\ncontrol_url='https://control.example'\n";
        for role in ["data", "worker"] {
            let config = NodeConfig::parse(valid, role).unwrap();
            let budget =
                Budget::for_runtime(&config.instance, &RuntimeSettings::default(), &[]).unwrap();
            assert_eq!(budget.database_connections, 0);
            assert_eq!(budget.database_read_connections, 0);
            for forbidden in [
                "deployment_key='secret'",
                "database_connections=4",
                "database_read_connections=4",
                "[database]\ntype='postgres'",
                "[replica_database]\nhost='db'",
                "[requests]\ns3_region='remote'",
                "control_urll='https://wrong.example'",
            ] {
                assert!(NodeConfig::parse(&format!("{valid}{forbidden}"), role).is_err());
            }
        }
        assert!(NodeConfig::parse(valid, "control").is_err());
        assert!(NodeConfig::parse("[instance]", "data").is_err());
        assert!(
            NodeConfig::parse("[instance]\ncontrol_url='http://control.example'", "worker")
                .is_err()
        );
        let error = NodeConfig::parse(
            &format!("{valid}deployment_key='private-root-secret'\n"),
            "data",
        )
        .err()
        .unwrap();
        assert!(!error.to_string().contains("private-root-secret"));
    }
}
