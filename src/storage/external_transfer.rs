use super::{
    Client,
    objects::{Condition, Object, Source as _},
};
use crate::model::{
    external::Snapshot,
    replication::{
        self,
        external::{Outcome, Part, Phase, Plan},
    },
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;

enum Source<'a> {
    External(Client),
    Native(&'a Plan, Option<&'a dyn super::objects::Source>),
}

impl Source<'_> {
    async fn head(&self, key: &str) -> Result<Option<Object>> {
        match self {
            Self::External(client) => client.head(key, None).await,
            Self::Native(plan, _) => Ok(plan.native.as_ref().map(|native| native.object.clone())),
        }
    }

    async fn acl(&self, key: &str, object: &Object) -> Result<bool> {
        match self {
            Self::External(client) => client.acl(key, object).await,
            Self::Native(plan, _) => {
                let native = plan.native.as_ref().context("native source missing")?;
                ensure!(&native.object == object, s3s::s3_error!(PreconditionFailed));
                Ok(native.public)
            }
        }
    }

    fn external(&self) -> Result<&Client> {
        let Self::External(client) = self else {
            anyhow::bail!("native listings require a catalog")
        };
        Ok(client)
    }
}

#[async_trait::async_trait]
impl super::objects::Source for Source<'_> {
    async fn read(&self, key: &str, object: &Object, start: u64, size: usize) -> Result<Bytes> {
        match self {
            Self::External(client) => client.read(key, object, start, size).await,
            Self::Native(plan, reader) => {
                ensure!(
                    plan.key.as_deref() == Some(key)
                        && plan
                            .native
                            .as_ref()
                            .is_some_and(|native| &native.object == object),
                    s3s::s3_error!(PreconditionFailed, "native source changed")
                );
                reader
                    .context("native source reader missing")?
                    .read(key, object, start, size)
                    .await
            }
        }
    }
}

fn uploaded(part: &Part) -> super::super::s3::UploadedPart {
    super::super::s3::UploadedPart {
        number: part.number as u16,
        size: part.size as u64,
        etag: part.etag.clone(),
    }
}

pub(crate) fn client(
    plan: &Plan,
    snapshots: &[Snapshot],
    source: bool,
    memory: &std::sync::Arc<tokio::sync::OwnedSemaphorePermit>,
    reads: &super::Reads,
) -> Result<Client> {
    let id = if source {
        plan.options.source_bucket
    } else {
        plan.options.target_bucket
    };
    let snapshot = snapshots
        .iter()
        .find(|snapshot| snapshot.external_id == id)
        .context("transfer connection missing")?;
    snapshot.validate()?;
    let mut client = snapshot.connection.connect(
        &snapshot.access_key,
        &snapshot.secret_key,
        super::super::model::Options {
            connect_timeout: std::time::Duration::from_secs(15),
            request_timeout: std::time::Duration::from_secs(60),
            retry_timeout: std::time::Duration::from_secs(60),
            max_retries: 0,
        },
    )?;
    client.memory = Some(memory.clone());
    client.reads = Some(reads.clone());
    Ok(client)
}

fn code(error: &anyhow::Error, code: s3s::S3ErrorCode) -> bool {
    error
        .downcast_ref::<s3s::S3Error>()
        .is_some_and(|error| error.code() == &code)
}

async fn execute_import(
    plan: &Plan,
    source: &Client,
    reader: Option<&dyn super::objects::Source>,
) -> Result<Outcome> {
    ensure!(
        plan.options.source_external && !plan.options.target_external,
        "invalid import endpoints"
    );
    let key = plan.key.as_deref().context("import key missing")?;
    let state = &plan.state;
    match state.phase {
        Phase::Inspect | Phase::Finalize => {
            #[cfg(feature = "fault-injection")]
            if state.phase == Phase::Finalize {
                crate::faults::point("import-before-final-check").await;
            }
            let original = source.head(key, None).await?;
            let source_public = if plan.options.permissions == replication::Permissions::Inherit {
                if let Some(object) = &original {
                    Some(source.acl(key, object).await?)
                } else {
                    None
                }
            } else {
                None
            };
            if state.phase == Phase::Inspect {
                Ok(Outcome::Inspected {
                    source: original,
                    source_public,
                    target: plan.native.as_ref().map(|native| native.object.clone()),
                    target_public: plan.native.as_ref().map(|native| native.public),
                })
            } else {
                Ok(Outcome::Finalized {
                    source: original,
                    source_public,
                    target: state.output.clone(),
                    public: plan.public(),
                })
            }
        }
        Phase::Compare => {
            let original = state.source.as_ref().context("import source missing")?;
            let target = state.target.as_ref().context("import target missing")?;
            let (start, size) = state.range(i32::from(state.position) + 1)?;
            let left = source.hash_range(key, original, start, size).await?;
            let right = reader
                .context("import comparison reader missing")?
                .hash_range(&plan.target()?, target, start, size)
                .await?;
            Ok(Outcome::Compared {
                equal: left == right,
            })
        }
        _ => anyhow::bail!("unexpected native import phase"),
    }
}

pub(crate) async fn import_source(
    plan: &Plan,
    snapshots: &[Snapshot],
    memory: &std::sync::Arc<tokio::sync::OwnedSemaphorePermit>,
    reads: &super::Reads,
) -> Result<std::sync::Arc<Client>> {
    plan.validate()?;
    plan.incoming
        .as_ref()
        .context("import write missing")?
        .validate(plan)?;
    let source = client(plan, snapshots, true, memory, reads)?;
    if let Some(previous) = plan.parts.first() {
        let (start, size) = plan.state.range(previous.number)?;
        let digest = source
            .hash_range(
                plan.key.as_deref().context("import key missing")?,
                plan.state
                    .source
                    .as_ref()
                    .context("import source missing")?,
                start,
                size,
            )
            .await?;
        ensure!(
            digest.as_slice() == previous.digest,
            s3s::s3_error!(PreconditionFailed, "import boundary changed")
        );
    }
    Ok(std::sync::Arc::new(source))
}

pub(crate) fn import_body(
    source: std::sync::Arc<Client>,
    key: String,
    object: Object,
    range: std::ops::Range<u64>,
) -> futures_util::stream::BoxStream<'static, Result<Bytes>> {
    use futures_util::StreamExt;
    async_stream::try_stream! {
        (range.start <= range.end && range.end <= object.size).then_some(()).context("invalid import body range")?;
        let mut offset = range.start;
        while offset < range.end {
            let length = (range.end - offset).min(super::objects::WINDOW as u64) as usize;
            let bytes = source.read(&key, &object, offset, length).await?;
            (bytes.len() == length).then_some(()).context("import range is incomplete")?;
            offset += length as u64;
            yield bytes;
        }
    }
    .boxed()
}

pub(crate) async fn execute(
    plan: &Plan,
    snapshots: &[Snapshot],
    memory: &std::sync::Arc<tokio::sync::OwnedSemaphorePermit>,
    reader: Option<&dyn super::objects::Source>,
    reads: &super::Reads,
) -> Result<Outcome> {
    plan.validate()?;
    let result = match execute_plan(plan, snapshots, memory, reader, reads).await {
        Err(error) if code(&error, s3s::S3ErrorCode::PreconditionFailed) => Ok(Outcome::Conflict),
        result => result,
    };
    #[cfg(feature = "fault-injection")]
    if let Ok(outcome) = &result {
        let point = match outcome {
            Outcome::Started { .. } => Some("external-upload-started"),
            Outcome::Transferred(_) => Some("external-part-uploaded"),
            Outcome::Written(_) => Some("external-upload-completed"),
            _ => None,
        };
        if let Some(point) = point {
            crate::faults::point(point).await;
        }
    }
    result
}

async fn execute_plan(
    plan: &Plan,
    snapshots: &[Snapshot],
    memory: &std::sync::Arc<tokio::sync::OwnedSemaphorePermit>,
    reader: Option<&dyn super::objects::Source>,
    reads: &super::Reads,
) -> Result<Outcome> {
    if !plan.options.source_external
        && plan.native.is_none()
        && plan.state.source.is_some()
        && plan.state.phase != Phase::Cleanup
    {
        return Ok(Outcome::Conflict);
    }
    let source = if plan.options.source_external {
        Source::External(client(plan, snapshots, true, memory, reads)?)
    } else {
        Source::Native(plan, reader)
    };
    let Some(key) = plan.key.as_deref() else {
        let page = match &plan.options.scope {
            replication::Scope::Objects { keys } => {
                let position = plan
                    .cursor
                    .as_deref()
                    .map(str::parse::<usize>)
                    .transpose()?
                    .unwrap_or(0);
                ensure!(position <= keys.len(), "transfer listing position differs");
                let end = (position + 64).min(keys.len());
                return Ok(Outcome::Listed {
                    keys: keys[position..end].to_vec(),
                    next: (end < keys.len()).then(|| end.to_string()),
                });
            }
            replication::Scope::All => {
                source
                    .external()?
                    .list("", plan.cursor.as_deref(), 64)
                    .await?
            }
            replication::Scope::Prefix { prefix } => {
                source
                    .external()?
                    .list(prefix, plan.cursor.as_deref(), 64)
                    .await?
            }
        };
        return Ok(Outcome::Listed {
            keys: page.objects.into_iter().map(|entry| entry.key).collect(),
            next: page.next,
        });
    };
    if !plan.options.target_external {
        return execute_import(plan, source.external()?, reader).await;
    }
    let target = client(plan, snapshots, false, memory, reads)?;
    let destination = plan.target()?;
    let state = &plan.state;
    if state.phase == Phase::Inspect {
        let (original, previous) =
            tokio::try_join!(source.head(key), target.head(&destination, None))?;
        let source_public = if plan.options.permissions == replication::Permissions::Inherit {
            match &original {
                Some(object) => Some(source.acl(key, object).await?),
                None => None,
            }
        } else {
            None
        };
        let target_public = if plan.options.permissions != replication::Permissions::Ignore {
            match &previous {
                Some(object) => Some(target.acl(&destination, object).await?),
                None => None,
            }
        } else {
            None
        };
        return Ok(Outcome::Inspected {
            source: original,
            target: previous,
            source_public,
            target_public,
        });
    }
    if state.phase == Phase::Cleanup {
        if let Some(upload) = &state.upload {
            target.abort_upload(&destination, upload).await?;
        }
        return Ok(Outcome::Aborted);
    }
    let original = state.source.as_ref().context("transfer source missing")?;
    let public = plan.public();
    let condition = state
        .target
        .as_ref()
        .map(|object| Condition::Match(object.etag.clone()))
        .unwrap_or(Condition::Absent);
    match state.phase {
        Phase::Compare => {
            let previous = state.target.as_ref().context("transfer target missing")?;
            ensure!(
                previous.size == original.size,
                "transfer comparison sizes differ"
            );
            let (start, size) = state.range(i32::from(state.position) + 1)?;
            let left = source.hash_range(key, original, start, size).await?;
            let right = target
                .hash_range(&destination, previous, start, size)
                .await?;
            Ok(Outcome::Compared {
                equal: left == right,
            })
        }
        Phase::Start => {
            let current = target.head(&destination, None).await?;
            ensure!(
                current == state.target,
                s3s::s3_error!(PreconditionFailed, "transfer target changed")
            );
            let upload = target
                .start_upload(&destination, &original.metadata, public)
                .await?;
            Ok(Outcome::Started { upload })
        }
        Phase::Transfer => {
            let upload = state.upload.as_deref().context("transfer upload missing")?;
            let page = target
                .parts(&destination, upload, state.position.saturating_sub(1))
                .await?
                .context("transfer upload disappeared")?;
            if let Some(previous) = plan.parts.first() {
                ensure!(
                    page.items.first() == Some(&uploaded(previous)),
                    "transfer boundary receipt changed"
                );
            }
            let number = state.position + 1;
            let (start, size) = state.range(i32::from(number))?;
            let (part, digest) = target
                .transfer_part(
                    &destination,
                    upload,
                    number,
                    &source,
                    key,
                    original,
                    start,
                    size,
                )
                .await?;
            Ok(Outcome::Transferred(Part {
                number: i32::from(number),
                size: size as i64,
                etag: part.etag,
                digest: digest.to_vec(),
            }))
        }
        Phase::Complete => {
            let written = if original.size == 0 {
                target
                    .put(
                        &destination,
                        &condition,
                        &original.metadata,
                        public,
                        Bytes::new(),
                    )
                    .await
            } else {
                let upload = state.upload.as_deref().context("transfer upload missing")?;
                let mut after = 0;
                let mut index = 0;
                let mut missing = false;
                loop {
                    let Some(page) = target.parts(&destination, upload, after).await? else {
                        missing = true;
                        break;
                    };
                    for part in page.items {
                        ensure!(
                            plan.parts
                                .get(index)
                                .is_some_and(|expected| uploaded(expected) == part),
                            "transfer completion receipt changed"
                        );
                        index += 1;
                    }
                    if let Some(next) = page.next {
                        after = next;
                    } else {
                        break;
                    }
                }
                if missing {
                    return Ok(Outcome::Located {
                        object: target.head(&destination, None).await?,
                    });
                }
                ensure!(
                    index == plan.parts.len(),
                    "transfer completion receipts missing"
                );
                target
                    .complete_upload(
                        &destination,
                        upload,
                        &plan.parts.iter().map(uploaded).collect::<Vec<_>>(),
                        &condition,
                    )
                    .await
            };
            match written {
                Ok(written) => Ok(Outcome::Written(written)),
                Err(error)
                    if code(&error, s3s::S3ErrorCode::NoSuchUpload)
                        || code(&error, s3s::S3ErrorCode::PreconditionFailed) =>
                {
                    Ok(Outcome::Located {
                        object: target.head(&destination, None).await?,
                    })
                }
                Err(error) => Err(error),
            }
        }
        Phase::Locate => Ok(Outcome::Located {
            object: target
                .head(
                    &destination,
                    state
                        .written
                        .as_ref()
                        .and_then(|written| written.version.as_deref()),
                )
                .await?,
        }),
        Phase::Verify => {
            let output = state.output.as_ref().context("transfer output missing")?;
            let part = plan
                .parts
                .first()
                .context("transfer verification receipt missing")?;
            let (start, size) = state.range(part.number)?;
            Ok(Outcome::Verified {
                digest: target
                    .hash_range(&destination, output, start, size)
                    .await?
                    .to_vec(),
            })
        }
        Phase::Permissions => {
            let output = state
                .output
                .as_ref()
                .or(state.target.as_ref())
                .context("transfer output missing")?;
            let public = public.context("transfer permission selection missing")?;
            if target.acl(&destination, output).await? != public {
                target.set_acl(&destination, output, public).await?;
            }
            Ok(Outcome::Permissions {
                public: target.acl(&destination, output).await?,
            })
        }
        Phase::Finalize => {
            let (source_object, target_object) =
                tokio::try_join!(source.head(key), target.head(&destination, None))?;
            let source_public = if plan.options.permissions == replication::Permissions::Inherit {
                match &source_object {
                    Some(object) => Some(source.acl(key, object).await?),
                    None => None,
                }
            } else {
                None
            };
            let public = if public.is_some() {
                match &target_object {
                    Some(object) => Some(target.acl(&destination, object).await?),
                    None => None,
                }
            } else {
                None
            };
            Ok(Outcome::Finalized {
                source: source_object,
                source_public,
                target: target_object,
                public,
            })
        }
        _ => anyhow::bail!("invalid external transfer execution phase"),
    }
}
