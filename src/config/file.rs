use super::{
    BackendDefaults, Budget, Config, Database, DatabaseType, Instance, LocalCache, RuntimeSettings,
    Secrets,
};
use anyhow::{Context, Result, bail, ensure};
use base64::Engine;
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    fs,
    path::Path,
    sync::{Arc, RwLock},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    pub database: Database,
    pub replica_database: Option<super::ReplicaDatabase>,
    #[serde(default)]
    pub instance: Instance,
}

impl FileConfig {
    pub fn into_config(self) -> Config {
        Config {
            configured_fields: Default::default(),
            database: self.database,
            replica_database: self.replica_database,
            instance: self.instance.clone(),
            backend_defaults: BackendDefaults::default(),
            local_cache: LocalCache::default(),
            local_compression: crate::content::compression::Config {
                context_idle_timeout: self.instance.compression_idle_timeout.clone(),
                ..Default::default()
            },
            local_integrity: Default::default(),
            local_pack: Default::default(),
            runtime: RuntimeSettings::default(),
        }
    }
}
fn secret(path: &Path, base: &Path) -> Result<String> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    };
    Ok(fs::read_to_string(&path)
        .with_context(|| format!("cannot read secret file {}", path.display()))?
        .trim()
        .to_owned())
}
pub(super) fn secret_value(
    value: Option<&str>,
    file: Option<&Path>,
    base: &Path,
    name: &str,
) -> Result<String> {
    let value = match (value, file) {
        (Some(value), None) => value.to_owned(),
        (None, Some(file)) => secret(file, base)?,
        _ => bail!("configure exactly one of {name} and {name}_file"),
    };
    ensure!(!value.is_empty(), "{name} must not be empty");
    Ok(value)
}
fn deployment_key(value: &str) -> Result<[u8; 32]> {
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value.as_bytes())
        .context("deployment_key must be URL-safe base64")?;
    ensure!(raw.len() == 48, "deployment_key must encode 48 bytes");
    Ok(*blake3::hash(&raw).as_bytes())
}

impl Config {
    pub fn load(path: &Path) -> Result<(Self, Secrets, Budget)> {
        let text = fs::read_to_string(path).context("cannot read configuration")?;
        // TOML errors may include the offending line; never include configuration values in errors.
        let file: FileConfig = toml::from_str(&text).map_err(|_| {
            anyhow::anyhow!("invalid TOML configuration, unknown field or wrong type")
        })?;
        let mut c = file.into_config();
        let raw: toml::Value =
            toml::from_str(&text).map_err(|_| anyhow::anyhow!("invalid TOML configuration"))?;
        if let Some(sections) = raw.as_table() {
            for (section, value) in sections {
                if let Some(fields) = value.as_table() {
                    for field in fields.keys() {
                        c.configured_fields.insert(format!("{section}.{field}"));
                    }
                }
            }
        }
        let base = path.parent().unwrap_or(Path::new("."));
        c.validate()?;
        let credential_material = c
            .instance
            .deployment_key
            .as_deref()
            .map(deployment_key)
            .transpose()?
            .context("instance.deployment_key is required")?;
        let database_password = if c.database.backend == DatabaseType::Sqlite {
            String::new()
        } else {
            secret_value(
                c.database.password.as_deref(),
                c.database.password_file.as_deref(),
                base,
                "database.password",
            )?
        };
        let replica_password = c
            .replica_database
            .as_ref()
            .map(|replica| {
                let resolved = replica.resolve(&c.database)?;
                secret_value(
                    resolved.password.as_deref(),
                    resolved.password_file.as_deref(),
                    base,
                    "replica_database.password",
                )
            })
            .transpose()?;
        let secrets = Secrets {
            database_password,
            replica_password,
            credential_key: crate::content::codec::Key::new("aes-256-gcm", credential_material)?,
            keys: BTreeMap::new(),
            runtime_keys: Arc::new(RwLock::new(BTreeMap::new())),
            key_names: Arc::new(RwLock::new(BTreeMap::new())),
        };
        let budget = c.budget()?;
        Ok((c, secrets, budget))
    }
}
