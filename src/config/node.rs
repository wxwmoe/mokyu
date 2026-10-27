use super::{Budget, Instance, RuntimeSettings};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use std::path::Path;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NodeConfig {
    pub(crate) instance: Instance,
    #[serde(skip)]
    pub(crate) configured_fields: std::collections::BTreeSet<String>,
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
            matches!(role, "data" | "worker" | "node"),
            "node configuration requires data, worker or node role"
        );
        let mut config: Self = toml::from_str(text).map_err(|_| {
            anyhow::anyhow!("invalid node configuration, unknown field or wrong type")
        })?;
        ensure!(
            config.instance.deployment_key.is_none()
                && config.instance.database_connections.is_none()
                && config.instance.database_read_connections.is_none(),
            "data, worker and node must not configure deployment_key or database budgets"
        );
        crate::rpc::client::control_uri(
            config
                .instance
                .control_url
                .as_deref()
                .context("instance.control_url is required")?,
        )?;
        let value: toml::Value = toml::from_str(text).expect("validated configuration");
        config.configured_fields = value["instance"]
            .as_table()
            .expect("instance table")
            .keys()
            .map(|field| format!("instance.{field}"))
            .collect();
        if !config.configured_fields.contains("instance.listen_control") {
            config.instance.listen_control = "127.0.0.1:9002".into();
        }
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
        for role in ["data", "worker", "node"] {
            let config = NodeConfig::parse(valid, role).unwrap();
            assert_eq!(config.instance.listen_control, "127.0.0.1:9002");
            assert!(!config.configured_fields.contains("instance.listen_control"));
            for address in ["0.0.0.0:9002", "[::1]:19002"] {
                let explicit =
                    NodeConfig::parse(&format!("{valid}listen_control='{address}'\n"), role)
                        .unwrap();
                assert_eq!(explicit.instance.listen_control, address);
                assert!(
                    explicit
                        .configured_fields
                        .contains("instance.listen_control")
                );
            }
            assert!(config.configured_fields.contains("instance.control_url"));
            assert!(!config.configured_fields.contains("instance.cpu_jobs"));
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
        assert_eq!(Instance::default().listen_control, "0.0.0.0:9002");
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
