use crate::{
    config::{self, Secrets},
    content::codec,
    storage::profile::StorageProfile,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::{
    ClientOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, RetryConfig,
    aws::AmazonS3Builder, local::LocalFileSystem, path::Path, prefix::PrefixStore,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct Record {
    pub id: Uuid,
    pub name: String,
    pub r#type: String,
    pub cluster_id: Option<Uuid>,
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

pub(crate) use super::marker::create as identity_marker;

impl Record {
    pub(crate) fn validate_mode(&self, mode: &str) -> Result<()> {
        ensure!(
            self.r#type != "local" || mode == "single",
            "local storage requires single mode"
        );
        if self.r#type == "mokyu" {
            ensure!(
                mode != "single" && self.cluster_id.is_some(),
                "mokyu storage requires a cluster in single_control or cluster mode"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Options {
    pub(super) connect_timeout: Duration,
    pub(super) request_timeout: Duration,
    pub(super) retry_timeout: Duration,
    pub(super) max_retries: usize,
}

impl Options {
    pub fn from_profile(profile: &StorageProfile) -> Result<Self> {
        Ok(Self {
            connect_timeout: Duration::from_secs(config::seconds(&profile.connect_timeout)?),
            request_timeout: Duration::from_secs(config::seconds(&profile.request_timeout)?),
            retry_timeout: Duration::from_secs(config::seconds(&profile.retry_timeout)?),
            max_retries: profile.max_retries as usize,
        })
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Connection {
    pub id: Uuid,
    pub r#type: String,
    pub cluster_id: Option<Uuid>,
    pub path: Option<String>,
    pub endpoint_url: Option<String>,
    pub remote_bucket_name: Option<String>,
    pub region: Option<String>,
    pub prefix: String,
    pub access: Option<String>,
    pub secret: Option<String>,
    pub identity_marker: Value,
}

impl Connection {
    pub(crate) fn unprotect(row: &Record, key: &codec::Key) -> Result<Self> {
        let credentials = if row.r#type == "s3" {
            Some((
                codec::unprotect(
                    row.access_key_encrypted
                        .as_deref()
                        .context("S3 access key is missing")?,
                    key,
                    format!("storage-{}-access", row.id).as_bytes(),
                )?,
                codec::unprotect(
                    row.secret_key_encrypted
                        .as_deref()
                        .context("S3 secret key is missing")?,
                    key,
                    format!("storage-{}-secret", row.id).as_bytes(),
                )?,
            ))
        } else {
            None
        };
        let (access, secret) = credentials.map_or((None, None), |(access, secret)| {
            (Some(access), Some(secret))
        });
        Ok(Self {
            id: row.id,
            r#type: row.r#type.clone(),
            cluster_id: row.cluster_id,
            path: row.path.clone(),
            endpoint_url: row.endpoint_url.clone(),
            remote_bucket_name: row.remote_bucket_name.clone(),
            region: row.region.clone(),
            prefix: row.prefix.clone(),
            access,
            secret,
            identity_marker: row.identity_marker.clone(),
        })
    }
}

pub(crate) struct Store {
    pub backend: Arc<dyn ObjectStore>,
    pub native: Option<Arc<super::native::store::Store>>,
    pub multipart: Option<Arc<dyn object_store::multipart::MultipartStore>>,
    pub streaming: Option<super::s3::Streaming>,
    identity_marker: Value,
    verified: Arc<tokio::sync::OnceCell<()>>,
}

impl Store {
    pub fn build_control(
        row: &Record,
        options: Options,
        secrets: &Secrets,
        control: Arc<super::native::control::Control>,
        region: Option<String>,
    ) -> Result<Self> {
        if row.r#type != "mokyu" {
            return Self::build(row, options, secrets);
        }
        Self::connect_native(
            &Connection::unprotect(row, &secrets.credential_key)?,
            options,
            super::native::store::Authority::Control(control),
            region,
        )
    }

    pub fn connect_native(
        row: &Connection,
        options: Options,
        authority: super::native::store::Authority,
        region: Option<String>,
    ) -> Result<Self> {
        ensure!(
            row.r#type == "mokyu"
                && super::marker::identity(&row.identity_marker)?.storage_id == row.id,
            "invalid native storage identity"
        );
        super::marker::journal(&row.identity_marker)?;
        let native = Arc::new(super::native::store::Store {
            authority,
            namespace: row.id,
            cluster: row
                .cluster_id
                .context("native storage cluster is missing")?,
            prefix: Path::parse(&row.prefix)?,
            region,
            pool: Arc::new(super::native::transport::Pool::new(options.request_timeout)),
        });
        Ok(Self::native(native, row.identity_marker.clone()))
    }

    pub fn native(native: Arc<super::native::store::Store>, identity_marker: Value) -> Self {
        Self {
            backend: native.clone(),
            native: Some(native),
            multipart: None,
            streaming: None,
            identity_marker,
            verified: Arc::default(),
        }
    }

    pub fn scope_native(&self, native: Arc<super::native::store::Store>) -> Result<Self> {
        ensure!(
            self.native
                .as_ref()
                .is_some_and(|previous| previous.namespace == native.namespace
                    && previous.cluster == native.cluster
                    && previous.prefix == native.prefix
                    && Arc::ptr_eq(&previous.pool, &native.pool)),
            "native scope changed its connection"
        );
        // The connection material fixes the namespace and marker; grants still scope every read.
        let mut scoped = Self::native(native, self.identity_marker.clone());
        scoped.verified = self.verified.clone();
        Ok(scoped)
    }

    pub fn build(row: &Record, options: Options, secrets: &Secrets) -> Result<Self> {
        Self::connect(
            &Connection::unprotect(row, &secrets.credential_key)?,
            options,
        )
    }

    pub(crate) fn connect(row: &Connection, options: Options) -> Result<Self> {
        let marker = super::marker::identity(&row.identity_marker)?;
        ensure!(
            marker.storage_id == row.id,
            "storage identity does not match its record"
        );
        super::marker::journal(&row.identity_marker)?;
        let mut multipart: Option<Arc<dyn object_store::multipart::MultipartStore>> = None;
        let mut streaming = None;
        let prefix = Path::parse(&row.prefix)?;
        let backend: Arc<dyn ObjectStore> = match row.r#type.as_str() {
            "local" => Arc::new(PrefixStore::new(
                LocalFileSystem::new_with_prefix(
                    row.path
                        .as_deref()
                        .context("local storage path is missing")?,
                )?
                .with_fsync(true),
                prefix.clone(),
            )),
            "s3" => {
                let access = row.access.as_deref().context("S3 access key is missing")?;
                let secret = row.secret.as_deref().context("S3 secret key is missing")?;
                let build_client = || {
                    reqwest::Client::builder()
                        .connect_timeout(options.connect_timeout)
                        .timeout(options.request_timeout)
                        .http1_only()
                        .redirect(reqwest::redirect::Policy::none())
                        .build()
                };
                let client = build_client()?;
                let remote = Arc::new(
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
                        .with_http_connector(super::s3::Connector {
                            main: client.clone(),
                            ahead: build_client()?,
                        })
                        .build()?,
                );
                let store = Arc::new(PrefixStore::new(remote.as_ref().clone(), prefix.clone()));
                multipart = Some(store.clone());
                streaming = Some(super::s3::Streaming {
                    statistics: None,
                    signer: store.clone(),
                    bucket_signer: remote,
                    prefix: prefix.clone(),
                    client,
                    raw_client: reqwest::Client::builder()
                        .connect_timeout(options.connect_timeout)
                        .http1_only()
                        .redirect(reqwest::redirect::Policy::none())
                        .build()?,
                    options,
                });
                store
            }
            _ => anyhow::bail!("unknown storage type"),
        };
        Ok(Self {
            backend,
            native: None,
            multipart,
            streaming,
            identity_marker: row.identity_marker.clone(),
            verified: Arc::default(),
        })
    }

    pub async fn verify(&self) -> Result<()> {
        super::marker::verify_store(self.backend.as_ref(), &self.identity_marker).await
    }

    pub(crate) async fn verify_relocation(
        &self,
        source: &Store,
        source_layout: super::location::Layout,
        target_layout: super::location::Layout,
    ) -> Result<()> {
        async fn digest(store: &dyn ObjectStore, path: &Path, size: u64) -> Result<blake3::Hash> {
            let object = store
                .get(path)
                .await
                .with_context(|| format!("missing relocation source {path}"))?;
            ensure!(
                object.meta.size == size,
                "relocation size mismatch at {path}"
            );
            let mut reader = object.into_stream();
            let mut hash = blake3::Hasher::new();
            let mut actual = 0u64;
            while let Some(bytes) = reader.try_next().await? {
                actual = actual
                    .checked_add(bytes.len() as u64)
                    .context("relocation length overflow")?;
                hash.update(&bytes);
            }
            ensure!(actual == size, "incomplete relocation source at {path}");
            Ok(hash.finalize())
        }

        source.verify().await?;
        self.verify().await?;
        let mut files = source.backend.list(None);
        while let Some(file) = files.try_next().await? {
            let key = file.location.as_ref();
            let Some((category, name)) = key.split_once('/') else {
                continue;
            };
            let target = match category {
                "objects" | "chunks" | "packs" => {
                    let Some(id) = source_layout.parse_name(name) else {
                        continue;
                    };
                    match category {
                        "objects" => target_layout.object(id),
                        "chunks" => target_layout.chunk(id),
                        _ => target_layout.pack(id),
                    }
                }
                "metadata" => file.location.clone(),
                _ => continue,
            };
            let (original, copy) = tokio::try_join!(
                digest(source.backend.as_ref(), &file.location, file.size),
                digest(self.backend.as_ref(), &target, file.size),
            )?;
            ensure!(original == copy, "relocation content mismatch at {target}");
        }
        Ok(())
    }

    pub async fn ensure_verified(&self) -> Result<()> {
        self.verified.get_or_try_init(|| self.verify()).await?;
        Ok(())
    }

    pub async fn initialize(&self) -> Result<()> {
        ensure!(
            super::marker::Document::probe(self.backend.as_ref())
                .await?
                .is_none(),
            crate::model::InputDetail::storage(
                "storage namespace is not empty",
                s3s::S3ErrorCode::OperationAborted
            )
        );
        let created = self
            .backend
            .put_opts(
                &Path::from("mokyu.json"),
                Bytes::from(serde_json::to_vec_pretty(&self.identity_marker)?).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await;
        match created {
            Ok(_)
            | Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => self.ensure_verified().await,
            Err(error) => {
                Err(error).context("cannot initialize storage without overwriting its marker")
            }
        }
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
            native: None,
            backend: Arc::new(PrefixStore::new(store, "test/nested")),
            multipart: None,
            streaming: None,
            identity_marker: expected.clone(),
            verified: Arc::default(),
        };
        assert!(target.verify().await.is_err());
        target.initialize().await.unwrap();
        assert!(target.initialize().await.is_err());
        let mut wrong = expected;
        wrong["storage_id"] = serde_json::json!(Uuid::new_v4());
        target
            .backend
            .put(
                &Path::from("mokyu.json"),
                Bytes::from(serde_json::to_vec(&wrong).unwrap()).into(),
            )
            .await
            .unwrap();
        assert!(target.verify().await.is_err());
        target
            .backend
            .delete(&Path::from("mokyu.json"))
            .await
            .unwrap();
        assert!(target.verify().await.is_err());
        assert!(
            target
                .backend
                .head(&Path::from("mokyu.json"))
                .await
                .is_err()
        );
    }
}
