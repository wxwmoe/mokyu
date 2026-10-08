use crate::{
    config::{self, Secrets},
    content::codec,
    storage::profile::StorageProfile,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use object_store::{
    ClientOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, RetryConfig,
    aws::AmazonS3Builder, local::LocalFileSystem, path::Path, prefix::PrefixStore,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

#[derive(Clone, sqlx::FromRow)]
pub(crate) struct Record {
    pub id: Uuid,
    pub name: String,
    pub r#type: String,
    pub path: Option<String>,
    pub endpoint_url: Option<String>,
    pub remote_bucket_name: Option<String>,
    pub region: Option<String>,
    pub prefix: String,
    pub access_key_encrypted: Option<Vec<u8>>,
    pub secret_key_encrypted: Option<Vec<u8>>,
    pub identity_marker: Value,
    pub enabled: bool,
}

#[derive(Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct IdentityMarker {
    format_version: u32,
    deployment_id: Uuid,
    storage_id: Uuid,
    chunk_layout: String,
    pack_layout: String,
    created_at: DateTime<Utc>,
    created_by: String,
}

pub(crate) fn identity_marker(deployment: Uuid, id: Uuid) -> Result<Value> {
    Ok(serde_json::to_value(IdentityMarker {
        format_version: 3,
        deployment_id: deployment,
        storage_id: id,
        chunk_layout: "uuid-prefix2".into(),
        pack_layout: "uuid-prefix2-v1".into(),
        created_at: Utc::now(),
        created_by: format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")),
    })?)
}

#[derive(Clone, Copy)]
pub(crate) struct Options {
    connect_timeout: Duration,
    request_timeout: Duration,
    retry_timeout: Duration,
    max_retries: usize,
}

impl Options {
    pub fn new(config: &config::BackendDefaults) -> Result<Self> {
        Ok(Self {
            connect_timeout: Duration::from_secs(config::seconds(&config.connect_timeout)?),
            request_timeout: Duration::from_secs(config::seconds(&config.request_timeout)?),
            retry_timeout: Duration::from_secs(config::seconds(&config.retry_timeout)?),
            max_retries: config.max_retries,
        })
    }

    pub fn from_profile(profile: &StorageProfile) -> Result<Self> {
        Ok(Self {
            connect_timeout: Duration::from_secs(config::seconds(&profile.connect_timeout)?),
            request_timeout: Duration::from_secs(config::seconds(&profile.request_timeout)?),
            retry_timeout: Duration::from_secs(config::seconds(&profile.retry_timeout)?),
            max_retries: profile.max_retries as usize,
        })
    }
}

pub(crate) struct Store {
    pub backend: Arc<dyn ObjectStore>,
    identity_marker: Value,
    verified: tokio::sync::OnceCell<()>,
}

impl Store {
    pub fn build(row: &Record, options: Options, secrets: &Secrets) -> Result<Self> {
        let meta: IdentityMarker = serde_json::from_value(row.identity_marker.clone())
            .context("storage identity marker is missing or invalid")?;
        ensure!(
            meta.format_version == 3
                && meta.storage_id == row.id
                && meta.chunk_layout == "uuid-prefix2"
                && meta.pack_layout == "uuid-prefix2-v1",
            "unsupported storage identity marker"
        );
        let backend: Arc<dyn ObjectStore> = match row.r#type.as_str() {
            "local" => Arc::new(LocalFileSystem::new_with_prefix(
                row.path
                    .as_deref()
                    .context("local storage path is missing")?,
            )?),
            "s3" => {
                let access = codec::unprotect(
                    row.access_key_encrypted
                        .as_deref()
                        .context("S3 access key is missing")?,
                    &secrets.credential_key,
                    format!("storage-{}-access", row.id).as_bytes(),
                )?;
                let secret = codec::unprotect(
                    row.secret_key_encrypted
                        .as_deref()
                        .context("S3 secret key is missing")?,
                    &secrets.credential_key,
                    format!("storage-{}-secret", row.id).as_bytes(),
                )?;
                Arc::new(
                    AmazonS3Builder::new()
                        .with_endpoint(
                            row.endpoint_url
                                .as_deref()
                                .context("S3 endpoint is missing")?,
                        )
                        .with_region(row.region.as_deref().context("S3 region is missing")?)
                        .with_bucket_name(
                            row.remote_bucket_name
                                .as_deref()
                                .context("S3 bucket is missing")?,
                        )
                        .with_access_key_id(access)
                        .with_secret_access_key(secret)
                        .with_allow_http(true)
                        .with_virtual_hosted_style_request(false)
                        .with_client_options(
                            ClientOptions::new()
                                .with_allow_http(true)
                                .with_connect_timeout(options.connect_timeout)
                                .with_timeout(options.request_timeout),
                        )
                        .with_retry(RetryConfig {
                            max_retries: options.max_retries,
                            retry_timeout: options.retry_timeout,
                            ..Default::default()
                        })
                        .build()?,
                )
            }
            _ => anyhow::bail!("unknown storage type"),
        };
        Ok(Self {
            backend: Arc::new(PrefixStore::new(backend, Path::parse(&row.prefix)?)),
            identity_marker: row.identity_marker.clone(),
            verified: tokio::sync::OnceCell::new(),
        })
    }

    pub async fn verify(&self) -> Result<()> {
        let result = self
            .backend
            .get(&Path::from("meta.json"))
            .await
            .context("storage meta.json is required; restore it before using this store")?;
        ensure!(
            result.meta.size <= 16 * 1024,
            "storage meta.json exceeds limit"
        );
        let mut data = Vec::new();
        let mut stream = result.into_stream();
        while let Some(bytes) = stream.try_next().await? {
            ensure!(
                data.len() + bytes.len() <= 16 * 1024,
                "storage meta.json exceeds limit"
            );
            data.extend_from_slice(&bytes);
        }
        let actual: IdentityMarker =
            serde_json::from_slice(&data).context("invalid storage meta.json")?;
        let expected: IdentityMarker = serde_json::from_value(self.identity_marker.clone())
            .context("invalid configured storage identity marker")?;
        ensure!(
            actual == expected,
            "storage meta.json does not match this store"
        );
        Ok(())
    }

    pub async fn ensure_verified(&self) -> Result<()> {
        self.verified.get_or_try_init(|| self.verify()).await?;
        Ok(())
    }

    pub async fn initialize(&self) -> Result<()> {
        let mut objects = self.backend.list(None);
        ensure!(
            objects.try_next().await?.is_none(),
            "storage namespace is not empty; refusing initialization"
        );
        drop(objects);
        self.backend
            .put_opts(
                &Path::from("meta.json"),
                Bytes::from(serde_json::to_vec_pretty(&self.identity_marker)?).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .context("cannot initialize storage without overwriting its marker")?;
        self.ensure_verified().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn markers_bind_deployment_and_store_without_reinitializing() {
        let store = object_store::memory::InMemory::new();
        let id = Uuid::new_v4();
        let expected = identity_marker(Uuid::new_v4(), id).unwrap();
        let target = Store {
            backend: Arc::new(PrefixStore::new(store, "test/nested")),
            identity_marker: expected.clone(),
            verified: tokio::sync::OnceCell::new(),
        };
        assert!(target.verify().await.is_err());
        target.initialize().await.unwrap();
        assert!(target.initialize().await.is_err());
        let mut wrong = expected;
        wrong["storage_id"] = serde_json::json!(Uuid::new_v4());
        target
            .backend
            .put(
                &Path::from("meta.json"),
                Bytes::from(serde_json::to_vec(&wrong).unwrap()).into(),
            )
            .await
            .unwrap();
        assert!(target.verify().await.is_err());
        target
            .backend
            .delete(&Path::from("meta.json"))
            .await
            .unwrap();
        assert!(target.verify().await.is_err());
        assert!(target.backend.head(&Path::from("meta.json")).await.is_err());
    }
}
