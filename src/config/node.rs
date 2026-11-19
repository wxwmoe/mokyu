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
        let budget = config.budget(role, &RuntimeSettings::default())?;
        Ok((config, budget))
    }

    pub(crate) fn budget(&self, role: &str, runtime: &RuntimeSettings) -> Result<Budget> {
        if role == "storage" {
            Budget::for_storage(&self.instance)
        } else {
            Budget::runtime(
                &self.instance,
                runtime,
                &[],
                matches!(role, "data" | "node"),
                Budget::environment(),
            )
        }
    }

    pub(crate) fn parse(text: &str, role: &str) -> Result<Self> {
        ensure!(
            matches!(role, "data" | "worker" | "node" | "storage"),
            "node configuration requires data, worker, node or storage role"
        );
        let mut config: Self = toml::from_str(text).map_err(|_| {
            anyhow::anyhow!("invalid node configuration, unknown field or wrong type")
        })?;
        ensure!(
            config.instance.deployment_key.is_none()
                && config.instance.database_connections.is_none()
                && config.instance.database_read_connections.is_none(),
            "API nodes must not configure deployment_key or database budgets"
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
        super::storage::validate(&config.instance, role)?;
        super::limits::validate_runtime(&config.instance, &RuntimeSettings::default())?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upload_budget_respects_pack_space_and_explicit_limits() {
        let plan = |instance: &Instance, memory_mib: u64, upload_node| {
            Budget::runtime(
                instance,
                &RuntimeSettings::default(),
                &[],
                upload_node,
                (Default::default(), 8, memory_mib << 20),
            )
        };
        let mut instance = Instance::default();
        let standard = plan(&instance, 1536, false).unwrap();
        let upload = plan(&instance, 1536, true).unwrap();
        assert_eq!(
            standard.data_slots as u64,
            (384 << 20) / standard.slot_bytes
        );
        assert_eq!(upload.data_slots as u64, (512 << 20) / upload.slot_bytes);
        assert!(upload.upload_concurrency > standard.upload_concurrency);
        assert_eq!(
            plan(&instance, 4096, true).unwrap().data_slots,
            plan(&instance, 4096, false).unwrap().data_slots
        );

        instance.response_cache_size = Some("208MiB".into());
        instance.write_memory_size = Some("208MiB".into());
        let tight = plan(&instance, 1024, true).unwrap();
        assert_eq!(
            tight.data_slots,
            plan(&instance, 1024, false).unwrap().data_slots
        );
        assert_eq!(
            (tight.write_memory_bytes, tight.response_cache_bytes),
            (208 << 20, 208 << 20)
        );

        instance.stream_memory_budget = Some("128MiB".into());
        assert_eq!(plan(&instance, 1024, true).unwrap().upload_concurrency, 1);
        instance.stream_memory_budget = Some("768MiB".into());
        assert!(plan(&instance, 1024, true).is_err());
    }

    #[test]
    fn data_limit_uses_the_same_validation_on_every_node_role() {
        for role in ["data", "worker", "node"] {
            let config = "[instance]\ncontrol_url='https://control.example'\n";
            assert!(NodeConfig::parse(&format!("{config}data_limit='1GiB'\n"), role).is_ok());
            for invalid in ["", "0B", "-1GiB", "1GB/s", "18446744073709551615TiB"] {
                assert!(
                    NodeConfig::parse(&format!("{config}data_limit='{invalid}'\n"), role).is_err()
                );
            }
        }
    }

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
            let budget = config.budget(role, &RuntimeSettings::default()).unwrap();
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
