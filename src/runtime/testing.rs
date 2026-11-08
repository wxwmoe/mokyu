use crate::{
    app::App,
    authorization::{Action, Principal},
    cli, config,
    control::storage::{StorageInput, create_installation_store, update_profile},
    model::{
        administration::BucketGrant,
        bucket::CreateBucket,
        credential::{CredentialInput, CredentialMaterial},
    },
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fs::File, io::Write, path::Path};
use uuid::Uuid;

const MARKER: &str = "testing.json";
const BUCKET: &str = "test";
const ACCESS: &str = "MOKYUTESTING00000001";
const SECRET: &[u8] = b"mokyu-testing-secret";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Installation {
    deployment_key: String,
    storage: Uuid,
    namespace: Option<Value>,
    ready: bool,
}

pub(super) fn marked(path: &Path) -> Result<bool> {
    match path.join(MARKER).symlink_metadata() {
        Ok(meta) => {
            ensure!(meta.is_file(), "invalid testing installation marker");
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn instance(text: &str) -> Result<config::Instance> {
    #[derive(Deserialize, Default)]
    #[serde(default, deny_unknown_fields)]
    struct Options {
        instance: config::Instance,
    }
    let mut value = toml::from_str::<Options>(text)
        .map_err(|_| anyhow::anyhow!("invalid testing configuration"))?
        .instance;
    ensure!(
        value.control_url.is_none()
            && value.deployment_key.is_none()
            && value.name.is_none()
            && value.listen_control == config::Instance::default().listen_control
            && value.listen_web == config::Instance::default().listen_web,
        "testing accepts local S3 and resource settings only"
    );
    value.listen_control = "127.0.0.1:9002".into();
    value.testing = true;
    Ok(value)
}

pub(crate) fn cli_instance(text: &str) -> Result<config::Instance> {
    let mut instance = instance(text)?;
    let state = Installation::read(&instance.data_path)?;
    instance.deployment_key = Some(state.deployment_key);
    Ok(instance)
}

pub(super) fn prepare(
    path: Option<&Path>,
) -> Result<(
    (config::Config, config::Secrets, config::Budget),
    (File, Installation),
)> {
    let text = path
        .map(std::fs::read_to_string)
        .transpose()
        .context("cannot read testing configuration")?
        .unwrap_or_default();
    let mut instance = instance(&text)?;
    let lock = super::claim_directory(&instance.data_path)?;
    instance.data_path = instance.data_path.canonicalize()?;
    let root = &instance.data_path;
    let state = if marked(root)? {
        Installation::read(root)?
    } else {
        for item in std::fs::read_dir(root)? {
            ensure!(
                item?.file_name() == "gateway.lock",
                "testing requires an empty data directory"
            );
        }
        let state = Installation {
            deployment_key: cli::deployment_key()?,
            storage: Uuid::new_v4(),
            namespace: None,
            ready: false,
        };
        state.write(root)?;
        state
    };
    for name in ["mokyu.db", "storage", "storage/mokyu.json"] {
        let required = state.ready || (name == "mokyu.db" && state.namespace.is_some());
        match root.join(name).symlink_metadata() {
            Ok(meta) => ensure!(
                if name == "mokyu.db" {
                    meta.is_file() && (!required || meta.len() > 0)
                } else if name == "storage" {
                    meta.is_dir()
                } else {
                    meta.is_file()
                },
                "invalid testing installation path: {name}"
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && !required => (),
            Err(error) => return Err(error).context("testing installation is incomplete"),
        }
    }
    instance.deployment_key = Some(state.deployment_key.clone());
    let file = config::FileConfig {
        database: config::Database {
            backend: config::DatabaseType::Sqlite,
            host: String::new(),
            port: None,
            name: String::new(),
            user: String::new(),
            password: None,
            password_file: None,
            ssl_mode: "prefer".into(),
            path: None,
        },
        replica_database: None,
        instance,
    };
    let settings = config::Config::resolve(file.into_config(), Path::new("."))?;
    Ok((settings, (lock, state)))
}

impl Installation {
    fn read(root: &Path) -> Result<Self> {
        ensure!(marked(root)?, "testing installation marker is missing");
        serde_json::from_reader(File::open(root.join(MARKER))?)
            .context("invalid testing installation marker")
    }

    fn write(&self, root: &Path) -> Result<()> {
        let mut file = tempfile::NamedTempFile::new_in(root)?;
        serde_json::to_writer(file.as_file_mut(), self)?;
        file.write_all(b"\n")?;
        file.as_file().sync_all()?;
        file.persist(root.join(MARKER))?;
        File::open(root)?.sync_all()?;
        Ok(())
    }

    pub(super) async fn seed(&mut self, app: &App) -> Result<()> {
        let root = &app.config.instance.data_path;
        if self.namespace.is_none() {
            ensure!(!self.ready, "testing namespace is missing");
            let deployment = app.storage_records.creation().await?.deployment;
            self.namespace = Some(crate::storage::model::identity_marker(
                deployment,
                self.storage,
            )?);
            self.write(root)?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("testing-namespace").await;
        }
        let marker = self
            .namespace
            .as_ref()
            .context("testing namespace is missing")?;
        let row = app.storage_records.record("local").await?;
        if let Some(row) = row {
            ensure!(
                row.id == self.storage
                    && crate::storage::marker::verify(&row.identity_marker, marker).is_ok()
                    && row.r#type == "local"
                    && row.enabled,
                "testing storage identity mismatch"
            );
            app.storage.verify_store_record((&row).into()).await?;
        } else {
            ensure!(!self.ready, "testing storage record is missing");
            let input = StorageInput {
                profile: None,
                name: "local".into(),
                storage_type: "local".into(),
                path: Some(
                    root.join("storage")
                        .to_str()
                        .context("testing storage path is not UTF-8")?
                        .to_owned(),
                ),
                endpoint: None,
                bucket: None,
                region: None,
                prefix: String::new(),
                block_key_id: None,
                initialize: true,
                read_concurrency: None,
                upload_concurrency: None,
                control_concurrency: None,
                access_key: None,
                secret_key: None,
            };
            create_installation_store(app, input, self.storage, Some(marker.clone())).await?;
        }
        if !self.ready {
            let mut p = app.storage_profiles.load(self.storage).await?;
            p.cdc_enabled = false;
            p.pack_enabled = false;
            p.compression_enabled = false;
            p.deduplication_enabled = false;
            p.read_integrity = "off".into();
            p.write_durability = Some("volatile".into());
            update_profile(app, "local".into(), p).await?;
        }
        let bucket = match app.buckets.by_name(BUCKET, false).await? {
            Some(bucket) => bucket,
            None if !self.ready => {
                app.buckets
                    .create(
                        &Principal::Local,
                        CreateBucket {
                            name: BUCKET.into(),
                            project_id: None,
                            storage_id: Some(self.storage),
                        },
                    )
                    .await?
            }
            None => anyhow::bail!("testing bucket is missing"),
        };
        ensure!(
            bucket.storage_id == Some(self.storage),
            "testing bucket storage mismatch"
        );
        #[cfg(feature = "fault-injection")]
        crate::faults::point("testing-bucket").await;
        let key = &app.secrets.credential_key;
        if let Some(secret) = app.credentials.encrypted_secret(ACCESS).await? {
            ensure!(
                crate::content::codec::unprotect(&secret, key, ACCESS.as_bytes())?.as_bytes()
                    == SECRET,
                "testing S3 credential mismatch"
            );
        } else {
            ensure!(!self.ready, "testing S3 credential is missing");
            let input = CredentialInput {
                project_id: bucket.project_id,
                label: "testing".into(),
                expires_in: None,
                grants: vec![BucketGrant {
                    bucket_id: bucket.id,
                    actions: Action::role("writer"),
                }],
            };
            app.credentials
                .create_credential(
                    &Principal::Local,
                    input,
                    CredentialMaterial {
                        access: ACCESS.into(),
                        encrypted: crate::content::codec::protect(SECRET, key, ACCESS.as_bytes())?,
                    },
                )
                .await?;
        }
        if !self.ready {
            #[cfg(feature = "fault-injection")]
            crate::faults::point("testing-credential").await;
            self.ready = true;
            self.write(root)?;
        }
        Ok(())
    }
}
