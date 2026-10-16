use serde::Deserialize;

pub(crate) mod file;
pub(crate) mod limits;
pub(crate) mod runtime;

pub use file::FileConfig;
pub use limits::{Budget, bytes, cache_bytes, seconds};
pub use runtime::{
    BackendDefaults, Database, DatabaseType, Instance, LocalCache, Maintenance, MetricsSettings,
    RequestSettings, RuntimeSettings, Secrets, UploadSettings, WebSettings,
};

#[derive(Clone, Deserialize)]
pub struct Config {
    #[serde(skip)]
    pub configured_fields: std::collections::BTreeSet<String>,
    pub database: Database,
    #[serde(skip)]
    pub instance: Instance,
    #[serde(skip)]
    pub backend_defaults: BackendDefaults,
    #[serde(skip)]
    pub local_cache: LocalCache,
    #[serde(skip)]
    pub local_compression: crate::content::compression::Config,
    #[serde(skip)]
    pub local_integrity: crate::worker::integrity::Config,
    #[serde(skip)]
    pub local_pack: crate::content::pack::Config,
    #[serde(flatten)]
    pub runtime: RuntimeSettings,
}

impl std::ops::Deref for Config {
    type Target = RuntimeSettings;
    fn deref(&self) -> &Self::Target {
        &self.runtime
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
    fn storage_minimum_age_is_optional_and_allows_zero() {
        let mut storage: BackendDefaults = toml::from_str("").unwrap();
        assert_eq!(storage.min_storage_seconds().unwrap(), 0);
        storage.min_storage_duration = "30d".into();
        assert_eq!(storage.min_storage_seconds().unwrap(), 30 * 86400);
        for invalid in ["", "-1d", "30", "30days", "18446744073709551615d"] {
            storage.min_storage_duration = invalid.into();
            assert!(storage.min_storage_seconds().is_err());
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
