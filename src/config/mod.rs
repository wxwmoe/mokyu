use serde::Deserialize;

pub(crate) mod file;
pub(crate) mod limits;
pub(crate) mod runtime;

pub use file::FileConfig;
pub use limits::{Budget, bytes, cache_bytes, seconds};
pub use runtime::{
    Database, DatabaseType, Instance, Maintenance, MetricsSettings, ReplicaDatabase,
    RequestSettings, RuntimeSettings, Secrets, UploadSettings, WebSettings,
};

#[derive(Clone, Deserialize)]
pub struct Config {
    #[serde(skip)]
    pub configured_fields: std::collections::BTreeSet<String>,
    pub database: Database,
    pub replica_database: Option<ReplicaDatabase>,
    #[serde(skip)]
    pub instance: Instance,
    #[serde(flatten)]
    pub runtime: RuntimeSettings,
}

impl std::ops::Deref for Config {
    type Target = RuntimeSettings;
    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl Config {
    pub fn validate_role(&self, role: &str) -> anyhow::Result<()> {
        self.database.backend.validate_role(role)?;
        anyhow::ensure!(
            self.replica_database.is_none() || role != "single",
            "replica_database requires cluster mode"
        );
        let minimum = match role {
            "data" | "worker" => 4,
            // LISTEN, two coordinators, an admin guard and its query.
            "control" => 5,
            _ => 1,
        };
        anyhow::ensure!(
            self.instance
                .database_connections
                .is_none_or(|value| value >= minimum),
            "instance.database_connections must be at least {minimum} for {role}"
        );
        Ok(())
    }
}

impl std::ops::DerefMut for Config {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.runtime
    }
}

#[cfg(test)]
mod tests {
    use super::file::secret_value;
    use super::*;
    use std::{fs, path::Path};

    #[test]
    fn sqlite_sync_is_explicit_and_database_scoped() {
        use serde_json::json;
        for backend in ["postgres", "mysql", "sqlite"] {
            let mut config =
                toml::from_str::<FileConfig>(&format!("[database]\ntype='{backend}'\n"))
                    .unwrap()
                    .into_config();
            assert_eq!(config.io.sqlite_synchronous, "full");
            assert!(
                crate::model::runtime_defaults(&config)
                    .iter()
                    .any(|(name, _)| *name == "io")
            );
            crate::model::apply_runtime_section(
                &mut config,
                "io",
                json!({"shutdown_flush_timeout":"0s"}),
            )
            .unwrap();
            assert_eq!(config.io.shutdown_seconds().unwrap(), 0);
            for value in ["30m", "1s"] {
                config.io.shutdown_flush_timeout = value.into();
                assert!(config.io.shutdown_seconds().is_ok());
            }
            for value in ["31m", "bad", "-1s"] {
                config.io.shutdown_flush_timeout = value.into();
                assert!(config.io.shutdown_seconds().is_err());
            }
            config.io.shutdown_flush_timeout = "30s".into();
            for mode in ["full", "normal", "off"] {
                let result = crate::model::apply_runtime_section(
                    &mut config,
                    "io",
                    json!({"sqlite_synchronous": mode}),
                );
                assert_eq!(result.is_ok(), backend == "sqlite");
                assert!(config.io.validate().is_ok());
            }
            if backend == "sqlite" {
                for mode in ["", "FULL", "extra", "none"] {
                    crate::model::apply_runtime_section(
                        &mut config,
                        "io",
                        json!({"sqlite_synchronous": mode}),
                    )
                    .unwrap();
                    assert!(config.io.validate().is_err());
                }
                assert!(
                    crate::model::apply_runtime_section(
                        &mut config,
                        "io",
                        json!({"journal_mode": "off"})
                    )
                    .is_err()
                );
            }
        }
    }

    #[test]
    fn cluster_connection_limits_leave_room_for_queries() {
        let mut config = toml::from_str::<FileConfig>("[database]\ntype='postgres'\n")
            .unwrap()
            .into_config();
        for (role, minimum) in [("single", 1), ("control", 5), ("data", 4), ("worker", 4)] {
            config.instance.database_connections = None;
            assert!(config.validate_role(role).is_ok());
            for connections in 1..=5 {
                config.instance.database_connections = Some(connections);
                assert_eq!(
                    config.validate_role(role).is_ok(),
                    connections >= minimum,
                    "{role}: {connections}"
                );
            }
        }
    }
    #[test]
    fn avatar_mirror_requires_a_safe_https_origin() {
        let mut control = WebSettings::default();
        assert_eq!(
            control.gravatar_origin().unwrap(),
            "https://www.gravatar.com"
        );
        control.gravatar_base_url = "https://avatar.example:8443/gravatar/".into();
        assert_eq!(
            control.gravatar_origin().unwrap(),
            "https://avatar.example:8443"
        );
        for invalid in [
            "http://avatar.example/avatar",
            "https://user@avatar.example/avatar",
            "https://avatar.example/avatar?size=1",
            "https://avatar.example/avatar#fragment",
            "https://*/avatar",
            "https://avatar.example;script-src/avatar",
        ] {
            control.gravatar_base_url = invalid.into();
            assert!(control.gravatar_origin().is_err(), "{invalid}");
        }
    }
    #[test]
    fn secret_sources_are_exclusive_and_preserve_literal_passwords() {
        let base = std::env::temp_dir().join(format!("mokyu-config-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&base).unwrap();
        fs::write(base.join("password"), "file-password\n").unwrap();
        let file = Some(Path::new("password"));
        assert_eq!(
            secret_value(None, file, &base, "password").unwrap(),
            "file-password"
        );
        assert_eq!(
            secret_value(Some(" @:/?# space "), None, &base, "password").unwrap(),
            " @:/?# space "
        );
        for (value, path) in [(None, None), (Some("do-not-leak"), file), (Some(""), None)] {
            let error = secret_value(value, path, &base, "password")
                .unwrap_err()
                .to_string();
            assert!(!error.contains("do-not-leak"));
        }
        fs::remove_dir_all(base).unwrap();
    }
    #[test]
    fn quantities_reject_ambiguity_and_overflow() {
        assert_eq!(bytes("2GB").unwrap(), 2_000_000_000);
        assert_eq!(bytes("2GiB").unwrap(), 2_147_483_648);
        assert_eq!(seconds("24h").unwrap(), 86400);
        for s in [
            "",
            "0GB",
            "-1GB",
            "auto",
            "1G",
            "1.5GB",
            "18446744073709551615GB",
        ] {
            assert!(bytes(s).is_err());
        }
    }

    #[test]
    fn database_type_defaults_to_postgres_and_resolves_ports() {
        let file: FileConfig = toml::from_str(
            r#"
                [database]
                host = "postgres"
                name = "mokyu"
                user = "mokyu"
            "#,
        )
        .unwrap();
        assert_eq!(file.database.backend, DatabaseType::Postgres);
        assert_eq!(file.database.effective_port(), Some(5432));

        let mut config = file.into_config();
        config.database.backend = DatabaseType::Mysql;
        assert_eq!(config.database.effective_port(), Some(3306));
        config.database.backend = DatabaseType::Sqlite;
        assert_eq!(
            config.database.sqlite_path(Path::new("/data")).unwrap(),
            Path::new("/data/mokyu.db")
        );
        assert!(DatabaseType::Mysql.validate_role("control").is_err());
        assert!(DatabaseType::Sqlite.validate_role("worker").is_err());
        assert!(DatabaseType::Postgres.validate_role("data").is_ok());
    }

    #[test]
    fn replica_configuration_preserves_primary_credentials_and_role_boundaries() {
        let primary = "[database]\nhost='primary'\nport=5433\nname='mokyu'\nuser='writer'\npassword='primary-secret'\nssl_mode='verify-full'\n";
        let load = |extra: &str| -> Config {
            toml::from_str::<FileConfig>(&format!("{primary}{extra}"))
                .unwrap()
                .into_config()
        };
        assert_eq!(load("").budget().unwrap().database_read_connections, 0);
        let config = load("[replica_database]\nhost='replica'\n");
        config.validate().unwrap();
        let resolved = config
            .replica_database
            .as_ref()
            .unwrap()
            .resolve(&config.database)
            .unwrap();
        assert_eq!(resolved.host, "replica");
        assert_eq!(resolved.effective_port(), Some(5433));
        assert_eq!(resolved.name, "mokyu");
        assert_eq!(resolved.user, "writer");
        assert_eq!(resolved.password.as_deref(), Some("primary-secret"));
        assert_eq!(resolved.ssl_mode, "verify-full");
        assert!(config.validate_role("single").is_err());
        for role in ["data", "control", "worker"] {
            config.validate_role(role).unwrap();
        }
        for extra in [
            "host=''",
            "host='replica'\nname='different'",
            "host='replica'\nuser='reader'",
            "host='replica'\nport=0",
            "host='replica'\npassword='x'\npassword_file='file'",
            "host='replica'\nssl_mode='invalid'",
        ] {
            assert!(
                load(&format!("[replica_database]\n{extra}\n"))
                    .validate()
                    .is_err()
            );
        }
        for extra in [
            "type='postgres'",
            "path='/data/mokyu.db'",
            "unknown='value'",
        ] {
            assert!(
                toml::from_str::<FileConfig>(&format!(
                    "{primary}[replica_database]\nhost='replica'\n{extra}\n"
                ))
                .is_err()
            );
        }
        let mut config =
            load("[replica_database]\nhost='replica'\nuser='reader'\npassword='replica-secret'\n");
        let resolved = config
            .replica_database
            .as_ref()
            .unwrap()
            .resolve(&config.database)
            .unwrap();
        assert_eq!(resolved.password.as_deref(), Some("replica-secret"));
        config.instance.database_connections = Some(12);
        assert_eq!(config.budget().unwrap().database_read_connections, 3);
        config.instance.database_read_connections = Some(2);
        assert_eq!(config.budget().unwrap().database_read_connections, 2);
        config.instance.database_read_connections = Some(0);
        assert!(config.validate().is_err());
        config.instance.database_read_connections = None;
        config.database.backend = DatabaseType::Mysql;
        assert!(config.validate().is_err());
        config.database.backend = DatabaseType::Sqlite;
        assert!(config.validate().is_err());
    }

    #[test]
    fn replica_password_files_resolve_relative_to_the_config() {
        let base =
            std::env::temp_dir().join(format!("mokyu-replica-config-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&base).unwrap();
        fs::write(base.join("primary"), "primary-secret\n").unwrap();
        fs::write(base.join("replica"), "replica-secret\n").unwrap();
        let text = format!(
            "[database]\nhost='primary'\nname='mokyu'\nuser='writer'\npassword_file='primary'\n[instance]\ndeployment_key='{}'\n",
            "A".repeat(64)
        );
        let path = base.join("config.toml");
        fs::write(&path, format!("{text}[replica_database]\nhost='replica'\n")).unwrap();
        let (_, secrets, _) = Config::load(&path).unwrap();
        assert_eq!(secrets.replica_password.as_deref(), Some("primary-secret"));
        fs::write(
            &path,
            format!(
                "{text}[replica_database]\nhost='replica'\nuser='reader'\npassword_file='replica'\n"
            ),
        )
        .unwrap();
        let (_, secrets, _) = Config::load(&path).unwrap();
        assert_eq!(secrets.database_password, "primary-secret");
        assert_eq!(secrets.replica_password.as_deref(), Some("replica-secret"));
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn response_cache_shares_the_existing_memory_budget() {
        let file: FileConfig = toml::from_str("[database]\ntype='sqlite'\n").unwrap();
        let mut config = file.into_config();
        assert_eq!(config.budget().unwrap().response_cache_bytes, 0);
        config.instance.response_cache_size = Some("4MiB".into());
        let cached = config.budget().unwrap();
        assert_eq!(cached.response_cache_bytes, 4 * 1024 * 1024);
        assert!(
            cached.data_slots as u64 * cached.slot_bytes
                + cached.memory_bytes / 4
                + u64::from(cached.pack_memory_units) * 2 * 1024 * 1024
                + cached.response_cache_bytes
                <= cached.memory_bytes
        );
        config.instance.response_cache_size = Some("0B".into());
        let disabled = config.budget().unwrap();
        assert_eq!(disabled.response_cache_bytes, 0);
        assert!(disabled.pack_memory_units > cached.pack_memory_units);
        config.instance.response_cache_size = Some("1MiB".into());
        assert!(config.budget().is_err());
    }

    #[test]
    fn compatible_backends_exclude_cluster_settings() {
        let file: FileConfig =
            toml::from_str("[database]\nhost='database'\nname='mokyu'\nuser='mokyu'").unwrap();
        let mut config = file.into_config();
        for backend in [
            DatabaseType::Postgres,
            DatabaseType::Mysql,
            DatabaseType::Sqlite,
        ] {
            config.database.backend = backend;
            let defaults = crate::model::runtime_defaults(&config);
            let uploads = &defaults
                .iter()
                .find(|(section, _)| *section == "uploads")
                .unwrap()
                .1;
            assert_eq!(
                uploads.get("staging_storage_id").is_some(),
                backend.supports_cluster()
            );
            assert_eq!(
                uploads.get("staging_bytes_limit").is_some(),
                backend.supports_cluster()
            );
            let mut restored = config.clone();
            for (section, value) in defaults {
                crate::model::apply_runtime_section(&mut restored, section, value).unwrap();
            }
            for key in ["staging_storage_id", "staging_bytes_limit"] {
                let result = crate::model::apply_runtime_section(
                    &mut restored,
                    "uploads",
                    serde_json::json!({key: null}),
                );
                assert_eq!(result.is_ok(), backend.supports_cluster());
            }
            restored.uploads.staging_bytes_limit = Some("1GiB".into());
            assert_eq!(restored.validate().is_ok(), backend.supports_cluster());
            for role in ["control", "worker", "data"] {
                assert_eq!(
                    backend.validate_role(role).is_ok(),
                    backend.supports_cluster()
                );
            }
            assert!(backend.validate_role("single").is_ok());
        }
    }
}
