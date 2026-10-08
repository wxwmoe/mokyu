use serde::Deserialize;

pub(crate) mod file;
pub(crate) mod limits;
pub(crate) mod runtime;

pub use file::FileConfig;
pub use limits::{Budget, bytes, cache_bytes, seconds};
pub use runtime::{
    BackendDefaults, Database, Instance, LocalCache, Maintenance, MetricsSettings, RequestSettings,
    RuntimeSettings, Secrets, UploadSettings, WebSettings,
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
    pub local_compression: crate::compression::Config,
    #[serde(skip)]
    pub local_integrity: crate::integrity::Config,
    #[serde(skip)]
    pub local_pack: crate::pack::Config,
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
}
