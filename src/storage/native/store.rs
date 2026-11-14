use super::{
    control::{Control, WriteRequest},
    reader, remote,
    transport::{Pool, Renewal},
    writer,
};
use crate::{
    content::fragment::Area,
    model::{
        storage_native::{Condition, access},
        storage_payload::Intent,
    },
    rpc::{client::Client, wire},
};
use anyhow::{Context as _, Result, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    path::Path,
};
use std::{ops::Range, sync::Arc};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) enum Authority {
    Unscoped,
    Recovery(Arc<super::recovery::archive::Archive>),
    Control(Arc<Control>),
    Read {
        client: Arc<Client>,
        run: Arc<crate::node::RunLease>,
        grants: Vec<Arc<crate::node::resources::Grant>>,
    },
    Upload {
        client: Arc<Client>,
        run: Arc<crate::node::RunLease>,
        grants: Vec<Arc<crate::node::resources::Grant>>,
        operation: Uuid,
        batch: Option<i64>,
    },
    Work {
        client: Arc<Client>,
        run: Arc<crate::node::RunLease>,
        grants: Vec<Arc<crate::node::resources::Grant>>,
        operation: Uuid,
    },
}

#[derive(Clone)]
pub(crate) struct Publication {
    pub owner: Uuid,
    pub area: Area,
}

#[derive(Clone)]
pub(crate) struct Store {
    pub authority: Authority,
    pub pool: Arc<Pool>,
    pub namespace: Uuid,
    pub cluster: Uuid,
    pub prefix: Path,
    pub region: Option<String>,
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MokyuStorage({})", self.namespace)
    }
}
impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl Store {
    pub(crate) async fn probe(&self) -> Result<()> {
        let Authority::Control(control) = &self.authority else {
            anyhow::bail!("native capability probe requires control authority");
        };
        let owner = Uuid::now_v7();
        let path = crate::storage::location::Layout::S3.object(owner);
        let bytes = Bytes::from(owner.as_bytes().repeat(8192));
        let checked = async {
            let intent = self
                .write(
                    &path,
                    bytes.len() as u64,
                    futures_util::stream::iter([Ok(bytes.clone())]).boxed(),
                    PutMode::Create.into(),
                )
                .await?;
            control
                .publish(self.namespace, intent.payload, owner)
                .await?;
            let actual = self.get_range(&path, 17..bytes.len() as u64 - 19).await?;
            ensure!(
                actual == bytes.slice(17..bytes.len() - 19),
                "native capability probe returned different bytes"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let retired = control.remove(self.namespace, &self.path(&path)).await;
        checked?;
        retired
    }

    pub(crate) async fn reclaim_recovery(&self, operation: Uuid) -> Result<bool> {
        let Authority::Control(control) = &self.authority else {
            anyhow::bail!("recovery cleanup requires control authority");
        };
        control
            .reclaim(self.namespace, operation, Area::Recovery, &self.pool)
            .await
    }

    // The transfer owns this address; the user-visible reference switches only after readback.
    pub(crate) async fn stage_copy<'a>(
        &self,
        path: &Path,
        size: u64,
        body: impl Fn() -> BoxStream<'a, Result<Bytes>>,
    ) -> Result<()> {
        ensure!(
            matches!(
                self.authority,
                Authority::Control(_) | Authority::Work { .. }
            ),
            "copy requires work authority"
        );
        let result = self.write(path, size, body(), PutMode::Create.into()).await;
        let intent = if result.as_ref().err().and_then(crate::model::error)
            == Some(crate::model::Error::StorageForwardFailed)
        {
            let mut direct = self.clone();
            direct.region = None;
            direct
                .write(path, size, body(), PutMode::Create.into())
                .await?
        } else {
            result?
        };
        if let Authority::Control(control) = &self.authority {
            let owner = carrier_owner(path)?;
            control
                .publish(self.namespace, intent.payload, owner)
                .await?;
        } else {
            self.maintain(access::MaintenanceAction::StageCopy {
                path: self.path(path),
                size,
            })
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn reclaim_blocks(&self, operation: Uuid) -> Result<bool> {
        if let Authority::Control(control) = &self.authority {
            return control
                .reclaim(self.namespace, operation, Area::Blocks, &self.pool)
                .await;
        }
        self.maintain(access::MaintenanceAction::Collect { payload: operation })
            .await?;
        Ok(true)
    }

    pub(crate) async fn publish_evidence(
        &self,
        db: &dyn crate::model::journal::JournalStore,
        token: Uuid,
        proof: &crate::model::storage_recovery::Proof,
    ) -> Result<()> {
        ensure!(
            proof.publication.namespace == self.namespace && proof.publication.parts.len() == 1,
            "recovery publication does not belong to this backend"
        );
        let storage = self.namespace;
        let Authority::Control(control) = &self.authority else {
            anyhow::bail!("journal requires control authority");
        };
        for media in &proof.publication.parts[0].intent.members {
            let mut after = db.native_evidence(storage, *media).await?;
            while after < proof.publication.revision {
                let history = db
                    .native_history(storage, after, proof.publication.revision)
                    .await?;
                ensure!(
                    !history.is_empty(),
                    "native publication predecessor is missing"
                );
                for (proof, manifest) in history {
                    ensure!(
                        proof.publication.revision == after + 1,
                        "native publication history has a gap"
                    );
                    let target = control
                        .publication_target(&proof, &manifest, *media)
                        .await?;
                    self.pool.publish(&target).await?;
                    after = proof.publication.revision;
                    db.native_receipt(storage, token, *media, after).await?;
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn next_recovery_identity(
        &self,
        path: &Path,
    ) -> Result<crate::model::storage_native::RecoveryIdentity> {
        use crate::model::{Error, storage_native::RecoveryIdentity};
        let Authority::Control(control) = &self.authority else {
            anyhow::bail!("snapshot requires control authority");
        };
        match control.open(self.namespace, &self.path(path), None).await {
            Ok(object) => Ok(RecoveryIdentity {
                payload: object.payload.intent.payload,
                revision: object
                    .payload
                    .intent
                    .revision
                    .checked_add(1)
                    .context("snapshot revision exhausted")?,
            }),
            Err(error)
                if matches!(
                    error.downcast_ref::<Error>(),
                    Some(Error::StorageObjectMissing)
                ) =>
            {
                Ok(RecoveryIdentity {
                    payload: Uuid::now_v7(),
                    revision: 1,
                })
            }
            Err(error) => Err(error),
        }
    }

    fn path(&self, path: &Path) -> String {
        if self.prefix.as_ref().is_empty() {
            path.to_string()
        } else {
            format!("{}/{path}", self.prefix)
        }
    }

    async fn maintain(
        &self,
        action: access::MaintenanceAction,
    ) -> Result<Vec<crate::model::storage_native::Metadata>> {
        let Authority::Work {
            client,
            run,
            grants,
            operation,
        } = &self.authority
        else {
            anyhow::bail!("native maintenance authority is missing");
        };
        run.check()?;
        for grant in grants {
            grant.check()?;
        }
        let reply = client
            .storage_maintenance(wire::NativeMaintenanceRequest {
                run: Some(run.run.clone()),
                input_json: serde_json::to_vec(&access::Maintenance {
                    operation: *operation,
                    storage: self.namespace,
                    action,
                })?,
            })
            .await?;
        let values: Vec<crate::model::storage_native::Metadata> =
            serde_json::from_slice(&reply.result_json)?;
        ensure!(
            values.len() <= 256,
            "native maintenance reply exceeds page limit"
        );
        Ok(values)
    }

    async fn plan(
        &self,
        path: &str,
        range: Option<Range<u64>>,
        version: Option<Uuid>,
        lease: Option<&crate::model::storage_native::ReadLease>,
    ) -> Result<(reader::Plan, Arc<dyn Renewal>)> {
        let (plan, renewal): (_, Arc<dyn Renewal>) = match &self.authority {
            Authority::Control(control) => (
                control
                    .plan_read(
                        self.namespace,
                        path,
                        version,
                        lease,
                        range,
                        self.region.as_deref(),
                        i64::MAX,
                    )
                    .await?,
                control.clone(),
            ),
            Authority::Read {
                client,
                run,
                grants,
            } => {
                run.check()?;
                let mut windows = Vec::new();
                for grant in grants
                    .iter()
                    .filter(|grant| grant.reference.r#type == wire::GrantType::ReadWindow as i32)
                {
                    grant.check()?;
                    windows.push(Uuid::parse_str(&grant.reference.id)?);
                }
                let reader = Arc::new(remote::Reader {
                    client: client.clone(),
                    run: run.run.clone(),
                    region: self.region.clone(),
                    access: access::Read {
                        windows,
                        upload: None,
                        work: None,
                        storage: self.namespace,
                        path: path.into(),
                        range,
                    },
                });
                (reader.plan(version, lease.cloned()).await?, reader)
            }
            Authority::Upload {
                client,
                run,
                grants,
                operation,
                ..
            }
            | Authority::Work {
                client,
                run,
                grants,
                operation,
            } => {
                run.check()?;
                for grant in grants {
                    grant.check()?;
                }
                let work = matches!(&self.authority, Authority::Work { .. });
                let reader = Arc::new(remote::Reader {
                    client: client.clone(),
                    run: run.run.clone(),
                    region: self.region.clone(),
                    access: access::Read {
                        windows: Vec::new(),
                        upload: (!work).then_some(*operation),
                        work: work.then_some(*operation),
                        storage: self.namespace,
                        path: path.into(),
                        range,
                    },
                });
                (reader.plan(version, lease.cloned()).await?, reader)
            }
            _ => anyhow::bail!("native read authority is missing"),
        };
        plan.validate()?;
        Ok((plan, renewal))
    }

    pub async fn write(
        &self,
        path: &Path,
        size: u64,
        body: BoxStream<'_, Result<Bytes>>,
        options: PutOptions,
    ) -> Result<Intent> {
        ensure!(
            options.attributes.is_empty(),
            "native storage does not accept object attributes"
        );
        let name = self.path(path);
        let intent = match &self.authority {
            Authority::Control(control) => {
                let publication = match options.extensions.get::<Publication>() {
                    Some(value) => value.clone(),
                    None => {
                        let owner = carrier_owner(path)?;
                        Publication {
                            owner,
                            area: Area::Blocks,
                        }
                    }
                };
                let condition = match options.mode {
                    PutMode::Create => Condition::Create,
                    PutMode::Overwrite if options.extensions.get::<Publication>().is_none() => {
                        Condition::Create
                    }
                    PutMode::Overwrite => Condition::Overwrite,
                    PutMode::Update(version) => {
                        let id = version
                            .version
                            .as_deref()
                            .or(version.e_tag.as_deref())
                            .context("native conditional write is missing its version")?;
                        Condition::Match(Uuid::parse_str(id.trim_matches('"'))?)
                    }
                };
                let request = WriteRequest {
                    namespace: self.namespace,
                    cluster: self.cluster,
                    path: &name,
                    owner: publication.owner,
                    condition,
                    area: publication.area,
                    total: size,
                    region: self.region.as_deref(),
                };
                let object = if let Some(identity) = options
                    .extensions
                    .get::<crate::model::storage_native::RecoveryIdentity>(
                ) {
                    control.reserve_recovery(request, *identity).await?
                } else {
                    control.reserve(request).await?
                };
                writer::write(
                    control.as_ref(),
                    &self.pool,
                    &object.payload.intent,
                    body,
                    self.region.as_deref(),
                )
                .await?;
                object.payload.intent
            }
            Authority::Upload {
                client,
                run,
                grants,
                operation,
                ..
            }
            | Authority::Work {
                client,
                run,
                grants,
                operation,
            } => {
                run.check()?;
                for grant in grants {
                    grant.check()?;
                }
                ensure!(
                    !matches!(options.mode, PutMode::Update(_))
                        && options.extensions.get::<Publication>().is_none(),
                    "native upload cannot replace a published carrier"
                );
                let (work, batch) = match &self.authority {
                    Authority::Upload { batch, .. } => (false, *batch),
                    _ => (true, None),
                };
                let writer = remote::Writer {
                    client: client.clone(),
                    run: run.run.clone(),
                    input: access::Upload {
                        operation: *operation,
                        work,
                        batch,
                        storage: self.namespace,
                        path: name,
                        size,
                    },
                };
                let intent = writer.prepare(self.region.clone()).await?;
                writer::write(&writer, &self.pool, &intent, body, self.region.as_deref()).await?;
                intent
            }
            _ => anyhow::bail!("native upload authority is missing"),
        };
        Ok(intent)
    }

    async fn get(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let name = self.path(path);
        if let Authority::Recovery(source) = &self.authority {
            return source.get(path, &name, options).await;
        }
        let version = options
            .version
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .map_err(|error| failure(path, error.into()))?;
        let (plan, _) = self
            .plan(&name, None, version, None)
            .await
            .map_err(|error| failure(path, error))?;
        let meta = metadata(path.clone(), &plan.object).map_err(|error| failure(path, error))?;
        options.check_preconditions(&meta)?;
        let range = options
            .range
            .as_ref()
            .map(|range| range.as_range(meta.size))
            .transpose()
            .map_err(|error| failure(path, error.into()))?
            .unwrap_or(0..meta.size);
        let store = self.clone();
        let requested = range.clone();
        let version = plan.object.version;
        let lease = plan.lease;
        let path = path.clone();
        let head = options.head;
        let body = async_stream::try_stream! {
            let mut offset = requested.start;
            while !head && offset < requested.end {
                let end = requested.end.min(offset + reader::WINDOW);
                let (plan, renewal) = store.plan(&name, Some(offset..end), Some(version), Some(&lease)).await.map_err(|error| failure(&path, error))?;
                let mut body = reader::read(store.pool.clone(), renewal, plan, 2).map_err(|error| failure(&path, error))?;
                while let Some(bytes) = body.try_next().await.map_err(|error| failure(&path, error))? { yield bytes; }
                offset = end;
            }
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(body)),
            meta,
            range,
            attributes: Default::default(),
            extensions: Default::default(),
        })
    }
}

fn carrier_owner(path: &Path) -> Result<Uuid> {
    let value = path.as_ref();
    if crate::storage::is_fragment_key(value) {
        return Ok(Uuid::parse_str(value.rsplit('/').next().unwrap())?);
    }
    let (directory, name) = value
        .split_once('/')
        .context("missing native carrier address")?;
    ensure!(
        matches!(directory, "objects" | "chunks" | "packs"),
        "native publication context is missing"
    );
    crate::storage::location::Layout::S3
        .parse_name(name)
        .context("invalid native carrier path")
}

pub(super) fn metadata(path: Path, value: &reader::Metadata) -> Result<ObjectMeta> {
    Ok(ObjectMeta {
        location: path,
        last_modified: chrono::DateTime::from_timestamp_micros(value.modified)
            .context("invalid native object time")?,
        size: value.size,
        e_tag: Some(format!("\"{}\"", value.version)),
        version: Some(value.version.to_string()),
    })
}

pub(super) fn failure(path: &Path, error: anyhow::Error) -> object_store::Error {
    match error.downcast_ref::<crate::model::Error>() {
        Some(crate::model::Error::StorageObjectMissing) => object_store::Error::NotFound {
            path: path.to_string(),
            source: error.into(),
        },
        Some(crate::model::Error::ObjectAlreadyExists) => object_store::Error::AlreadyExists {
            path: path.to_string(),
            source: error.into(),
        },
        Some(crate::model::Error::StorageObjectChanged) => object_store::Error::Precondition {
            path: path.to_string(),
            source: error.into(),
        },
        _ => object_store::Error::Generic {
            store: "Mokyu",
            source: error.reallocate_into_boxed_dyn_error_without_backtrace(),
        },
    }
}

fn unsupported(operation: &str) -> object_store::Error {
    object_store::Error::NotSupported {
        source: format!("native storage requires an explicit {operation} plan").into(),
    }
}

#[cfg(test)]
#[test]
fn native_failures_preserve_business_and_transport_errors() {
    let mut status = tonic::Status::aborted("conflicting write");
    status
        .metadata_mut()
        .insert("mokyu-s3-code", "OperationAborted".parse().unwrap());
    for (error, code) in [
        (
            anyhow::Error::new(status),
            s3s::S3ErrorCode::OperationAborted,
        ),
        (
            anyhow::Error::new(tonic::Status::unavailable("storage disconnected")),
            s3s::S3ErrorCode::ServiceUnavailable,
        ),
        (
            anyhow::Error::new(crate::model::Error::StorageCopiesPending),
            s3s::S3ErrorCode::ServiceUnavailable,
        ),
    ] {
        let wrapped = failure(
            &Path::from("mokyu.json"),
            error.context("native request failed"),
        );
        assert_eq!(crate::app::internal(wrapped.into()).code(), &code);
    }
}

#[async_trait::async_trait]
impl ObjectStore for Store {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        mut options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let size = payload.content_length() as u64;
        let marker = path.as_ref() == "mokyu.json";
        if marker {
            let validate = || -> Result<Publication> {
                ensure!(
                    matches!(self.authority, Authority::Control(_))
                        && !matches!(options.mode, PutMode::Overwrite)
                        && size <= crate::storage::marker::MAX_BYTES as u64,
                    "invalid native marker publication"
                );
                let mut bytes = Vec::with_capacity(size as usize);
                for part in payload.iter() {
                    bytes.extend_from_slice(part);
                }
                let value = serde_json::from_slice(&bytes)?;
                ensure!(
                    crate::storage::marker::identity(&value)?.storage_id == self.namespace,
                    "native marker belongs to another backend"
                );
                crate::storage::marker::journal(&value)?;
                let mut hash = blake3::Hasher::new();
                hash.update(self.namespace.as_bytes());
                hash.update(&bytes);
                hash.update(format!("{:?}", options.mode).as_bytes());
                Ok(Publication {
                    owner: options
                        .extensions
                        .get::<Publication>()
                        .map(|p| p.owner)
                        .unwrap_or(Uuid::from_slice(&hash.finalize().as_bytes()[..16])?),
                    area: Area::Blocks,
                })
            };
            options
                .extensions
                .insert(validate().map_err(|error| failure(path, error))?);
        }
        let owner = options
            .extensions
            .get::<Publication>()
            .map(|publication| publication.owner);
        let body = futures_util::stream::iter(payload.clone().into_iter().map(Ok)).boxed();
        let result = self.write(path, size, body, options.clone()).await;
        let result = if result.as_ref().err().and_then(crate::model::error)
            == Some(crate::model::Error::StorageForwardFailed)
        {
            let mut direct = self.clone();
            direct.region = None;
            let body = futures_util::stream::iter(payload.into_iter().map(Ok)).boxed();
            direct.write(path, size, body, options).await
        } else {
            result
        };
        let intent = result.map_err(|error| failure(path, error))?;
        if marker {
            let Authority::Control(control) = &self.authority else {
                unreachable!()
            };
            control
                .publish(self.namespace, intent.payload, owner.unwrap())
                .await
                .map_err(|error| failure(path, error))?;
        }
        // The owning business transaction publishes this candidate with its references.
        Ok(PutResult {
            e_tag: Some(format!("\"{}\"", intent.operation)),
            version: Some(intent.operation.to_string()),
            extensions: Default::default(),
        })
    }

    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(unsupported("streaming upload"))
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.get(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let store = self.clone();
        paths
            .then(move |path| {
                let store = store.clone();
                async move {
                    let path = path?;
                    let name = store.path(&path);
                    match &store.authority {
                        Authority::Control(control) => control.remove(store.namespace, &name).await,
                        Authority::Work { .. } => store
                            .maintain(access::MaintenanceAction::Delete { path: name })
                            .await
                            .map(|_| ()),
                        _ => return Err(unsupported("deletion")),
                    }
                    .map_err(|error| failure(&path, error))?;
                    Ok(path)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        Box::pin(async_stream::try_stream! {
            let mut after: Option<String> = None;
            loop {
                let page = match &store.authority {
                    Authority::Control(control) => control.list(store.namespace, after.as_deref()).await,
                    Authority::Work { .. } => store.maintain(access::MaintenanceAction::List { after: after.clone() }).await,
                    _ => Err(unsupported("listing").into()),
                }.map_err(|error| failure(&Path::default(), error))?;
                if page.is_empty() { break; }
                for object in page {
                    if after.as_ref().is_some_and(|after| object.path <= *after) {
                        Err(failure(&Path::default(), anyhow::anyhow!("native listing did not advance")))?;
                    }
                    after = Some(object.path.clone());
                    let full = Path::parse(&object.path)?;
                    let Some(parts) = full.prefix_match(&store.prefix) else { continue; };
                    let path: Path = parts.collect();
                    if prefix.as_ref().is_some_and(|prefix| path.prefix_match(prefix).is_none()) { continue; }
                    yield metadata(path.clone(), &object).map_err(|error| failure(&path, error))?;
                }
            }
        })
    }

    async fn list_with_delimiter(&self, _: Option<&Path>) -> object_store::Result<ListResult> {
        Err(unsupported("directory listing"))
    }
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> object_store::Result<()> {
        Err(unsupported("copy"))
    }
}
