use super::{
    Node,
    fragments::{self, Check, Input},
    inventory::{EvidenceKind, EvidenceScan, Scan},
};
use crate::{
    content::fragment::UNIT,
    model::storage_transfer::{Action, GRANT_LIMIT, Grant, SignedGrant},
    rpc::wire,
};
use anyhow::{Context, Result, ensure};
use futures_util::StreamExt;
use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, mpsc};
use tonic::{Status, Streaming};

pub(super) type Output =
    Pin<Box<dyn futures_util::Stream<Item = Result<wire::StorageOutput, Status>> + Send>>;

fn monitored(mut output: Output, check: Check) -> Output {
    let (sender, mut receiver) = mpsc::channel(1);
    let (finished, completion) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let drive = async {
            while let Some(value) = output.next().await {
                if sender.send(value).await.is_err() {
                    break;
                }
            }
        };
        tokio::pin!(drive);
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        let failure = loop {
            tokio::select! {
                biased;
                _ = sender.closed() => break None,
                _ = interval.tick() => {
                    if let Err(failure) = check() { break Some(error(failure)); }
                },
                _ = &mut drive => break None,
            }
        };
        let _ = finished.send(failure);
    });
    Box::pin(async_stream::stream! {
        while let Some(value) = receiver.recv().await { yield value; }
        match completion.await {
            Ok(Some(failure)) => yield Err(failure),
            Err(_) => yield Err(Status::internal("storage stream stopped unexpectedly")),
            Ok(None) => (),
        }
    })
}

pub(super) struct Authorization {
    node: Arc<Node>,
    grant: Mutex<(Grant, tokio::time::Instant)>,
    key: [u8; 32],
    cancelled: AtomicBool,
    _permit: OwnedSemaphorePermit,
}

fn signed(bytes: &[u8]) -> Result<SignedGrant> {
    ensure!(
        !bytes.is_empty() && bytes.len() <= GRANT_LIMIT,
        Status::invalid_argument("invalid storage grant size")
    );
    serde_json::from_slice(bytes).context("invalid storage grant")
}

fn deadline(grant: &Grant) -> Result<tokio::time::Instant> {
    let now = chrono::Utc::now().timestamp();
    ensure!(
        grant.issued <= now + 5 && grant.expires > now && grant.expires - now <= 95,
        Status::permission_denied("storage grant expired or clock differs")
    );
    Ok(tokio::time::Instant::now() + Duration::from_secs((grant.expires - now) as u64))
}

impl Authorization {
    pub fn open(node: Arc<Node>, bytes: &[u8], permit: OwnedSemaphorePermit) -> Result<Arc<Self>> {
        let signed = signed(bytes)?;
        let manifest = node
            .files
            .manifest
            .read()
            .unwrap()
            .clone()
            .context("storage membership is missing")?;
        signed
            .verify(&manifest)
            .map_err(|_| Status::permission_denied("invalid storage grant authority"))?;
        let deadline = deadline(&signed.grant)?;
        let value = Arc::new(Self {
            node,
            grant: Mutex::new((signed.grant, deadline)),
            key: manifest.publication_key,
            cancelled: AtomicBool::new(false),
            _permit: permit,
        });
        value.check()?;
        Ok(value)
    }

    pub fn value(&self) -> Grant {
        self.grant.lock().unwrap().0.clone()
    }

    pub fn check(&self) -> Result<()> {
        let (grant, deadline) = &*self.grant.lock().unwrap();
        ensure!(
            !self.cancelled.load(Ordering::Acquire) && *deadline > tokio::time::Instant::now(),
            Status::permission_denied("storage grant is no longer active")
        );
        let state = self.node.connection.lock().unwrap();
        let run = state
            .run
            .as_ref()
            .context("storage has no control session")?;
        ensure!(
            state.phase == "registered"
                && state.deadline > tokio::time::Instant::now()
                && run.boot_id == grant.boot.to_string()
                && run.fence == grant.fence.to_string()
                && grant.node == self.node.client.identity.node_id
                && grant.deployment == self.node.client.identity.deployment_id
                && grant.media == self.node.files.media.media_id,
            Status::permission_denied("storage grant belongs to an inactive run")
        );
        let manifest = self.node.files.manifest.read().unwrap();
        ensure!(
            manifest
                .as_ref()
                .is_some_and(|m| m.cluster_id == grant.cluster
                    && m.deployment_id == grant.deployment
                    && m.takeover_epoch == grant.takeover_epoch
                    && m.revision as u64 >= grant.manifest_revision
                    && m.publication_key == self.key
                    && m.members.iter().any(
                        |member| member.media_id == grant.media && member.node_id == grant.node
                    )),
            Status::permission_denied("storage grant belongs to an inactive membership")
        );
        Ok(())
    }

    pub fn renew(&self, bytes: &[u8]) -> Result<()> {
        self.check()?;
        let signed = signed(bytes)?;
        let manifest = self
            .node
            .files
            .manifest
            .read()
            .unwrap()
            .clone()
            .context("storage membership is missing")?;
        signed
            .verify(&manifest)
            .map_err(|_| Status::permission_denied("invalid storage renewal authority"))?;
        let until = deadline(&signed.grant)?;
        let mut current = self.grant.lock().unwrap();
        current.0.renewal(&signed.grant)?;
        let until = if signed.grant.expires == current.0.expires {
            current.1
        } else {
            until
        };
        *current = (signed.grant, until);
        Ok(())
    }

    pub fn checking(self: &Arc<Self>) -> Check {
        let value = self.clone();
        Arc::new(move || value.check())
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

pub(super) struct Cancel(pub Arc<Authorization>);
impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) fn error(error: anyhow::Error) -> Status {
    if let Some(status) = error.downcast_ref::<Status>() {
        return status.clone();
    }
    tracing::debug!(%error, "storage transfer failed");
    if error
        .downcast_ref::<crate::content::codec::IntegrityError>()
        .is_some()
    {
        return Status::data_loss("stored fragment failed integrity verification");
    }
    if let Some(io) = error.downcast_ref::<std::io::Error>() {
        match io.kind() {
            std::io::ErrorKind::NotFound => return Status::not_found("stored fragment is missing"),
            std::io::ErrorKind::UnexpectedEof => {
                return Status::data_loss("stored fragment is truncated");
            }
            _ => (),
        }
    }
    Status::failed_precondition(error.to_string())
}

pub(super) async fn next(input: &mut Streaming<wire::StorageInput>) -> Result<wire::StorageInput> {
    tokio::time::timeout(Duration::from_secs(30), input.message())
        .await
        .map_err(|_| Status::deadline_exceeded("storage input is idle"))??
        .context("storage input ended before completion")
}

pub(super) async fn first(
    node: Arc<Node>,
    input: &mut Streaming<wire::StorageInput>,
    permit: OwnedSemaphorePermit,
) -> Result<Arc<Authorization>> {
    let message = next(input).await?;
    ensure!(
        message.data.is_empty() && !message.complete,
        Status::invalid_argument("storage stream must begin with a grant")
    );
    Authorization::open(node, &message.grant_json, permit)
}

pub(super) async fn feed(
    mut input: Streaming<wire::StorageInput>,
    auth: Arc<Authorization>,
    sender: mpsc::Sender<Input>,
) -> Result<()> {
    loop {
        let message = next(&mut input).await?;
        auth.check()?;
        if !message.grant_json.is_empty() {
            ensure!(
                message.data.is_empty() && !message.complete,
                "renewal must not contain fragment bytes"
            );
            auth.renew(&message.grant_json)?;
        } else if message.complete {
            ensure!(
                message.data.is_empty(),
                "completion must follow the last fragment byte"
            );
            sender
                .send(Input::Complete)
                .await
                .context("storage writer stopped")?;
            return Ok(());
        } else {
            ensure!(
                !message.data.is_empty() && message.data.len() <= UNIT,
                "storage data message exceeds its bound"
            );
            sender
                .send(Input::Data(message.data.into()))
                .await
                .context("storage writer stopped")?;
        }
    }
}

struct Renewal {
    task: tokio::task::JoinHandle<()>,
    auth: Arc<Authorization>,
}
impl Drop for Renewal {
    fn drop(&mut self) {
        self.auth.cancel();
        self.task.abort();
    }
}

fn renewing(mut input: Streaming<wire::StorageInput>, auth: Arc<Authorization>) -> Renewal {
    let current = auth.clone();
    let task = tokio::spawn(async move {
        let result = async {
            loop {
                let message = next(&mut input).await?;
                ensure!(
                    message.data.is_empty() && !message.complete,
                    "read and inventory streams accept only renewals"
                );
                current.renew(&message.grant_json)?;
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(error) = result {
            tracing::debug!(%error, "storage renewal stopped");
        }
        current.cancel();
    });
    Renewal { task, auth }
}

pub(super) async fn read(
    node: Arc<Node>,
    input: Streaming<wire::StorageInput>,
    auth: Arc<Authorization>,
) -> Result<Output> {
    let Action::Read {
        receipt,
        offset,
        length,
    } = auth.value().action
    else {
        return Err(Status::permission_denied("a read grant is required").into());
    };
    let cancellation = Cancel(auth.clone());
    let mut bytes = fragments::read(&node.files, receipt, offset, length, auth.checking()).await?;
    let renewal = renewing(input, auth.clone());
    let check = auth.checking();
    Ok(monitored(
        Box::pin(async_stream::try_stream! {
            let _cancel = cancellation;
            let _renewal = renewal;
            #[cfg(feature = "fault-injection")]
            crate::faults::point(&format!("storage-read-{}-{offset}", auth.value().media)).await;
            let mut delivered = 0u64;
            while let Some(bytes) = tokio::time::timeout(Duration::from_secs(30), bytes.recv()).await.map_err(|_| Status::deadline_exceeded("storage read is idle"))? {
                auth.check().map_err(error)?;
                let bytes = bytes.map_err(error)?;
                delivered = delivered.checked_add(bytes.len() as u64).filter(|n| *n <= length)
                    .ok_or_else(|| Status::data_loss("storage read exceeds the requested range"))?;
                yield wire::StorageOutput { data: bytes.to_vec(), page_json: vec![], complete: false };
            }
            auth.check().map_err(error)?;
            if delivered != length {
                Err(Status::data_loss("storage read ended before the requested range"))?;
            }
            yield wire::StorageOutput { data: vec![], page_json: vec![], complete: true };
        }),
        check,
    ))
}

enum Inventory {
    Fragments(Scan),
    Evidence(EvidenceScan),
}

pub(super) async fn layouts(
    node: Arc<Node>,
    permit: OwnedSemaphorePermit,
    run: wire::Run,
    digest: String,
) -> Result<Output> {
    let manifest = node
        .files
        .manifest
        .read()
        .unwrap()
        .clone()
        .context("storage membership is missing")?;
    ensure!(
        manifest.digest()? == digest,
        "storage layout changed before inspection"
    );
    let cluster = manifest.cluster_id;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let current = node.clone();
    let check: Check = Arc::new(move || {
        let state = current.connection.lock().unwrap();
        ensure!(
            state.phase == "registered"
                && state.deadline > tokio::time::Instant::now()
                && deadline > tokio::time::Instant::now()
                && state
                    .run
                    .as_ref()
                    .is_some_and(|r| r.boot_id == run.boot_id && r.fence == run.fence)
                && current.files.manifest.read().unwrap().as_ref() == Some(&manifest),
            Status::permission_denied("storage layout inspection is no longer active")
        );
        Ok(())
    });
    // The current node credential permits discovery before the old layout is adopted.
    let scan = EvidenceScan::open(&node.files, EvidenceKind::Layouts, cluster, check.clone())?;
    let checking = check.clone();
    Ok(monitored(
        Box::pin(async_stream::try_stream! {
            let _permit = permit;
            let mut scan = scan;
            let mut number = 0;
            loop {
                checking().map_err(error)?;
                let (next, page) = tokio::task::spawn_blocking(move || -> Result<_> {
                    let page = scan.page(number)?;
                    Ok((scan, page))
                }).await.map_err(|_| Status::internal("storage layout inspection failed"))?.map_err(error)?;
                scan = next;
                checking().map_err(error)?;
                let complete = page.complete;
                yield wire::StorageOutput { data: vec![], page_json: serde_json::to_vec(&page)
                    .map_err(|_| Status::internal("encode storage layouts"))?, complete };
                if complete { break; }
                number += 1;
            }
        }),
        check,
    ))
}

pub(super) async fn inventory(
    node: Arc<Node>,
    input: Streaming<wire::StorageInput>,
    auth: Arc<Authorization>,
) -> Result<Output> {
    let check = auth.checking();
    let scan = match auth.value().action {
        Action::Scan { area, prefix } => {
            Inventory::Fragments(Scan::open(&node.files, area, prefix, check)?)
        }
        Action::History { namespace } => Inventory::Evidence(EvidenceScan::open(
            &node.files,
            EvidenceKind::History,
            namespace,
            check,
        )?),
        Action::Layouts => Inventory::Evidence(EvidenceScan::open(
            &node.files,
            EvidenceKind::Layouts,
            auth.value().cluster,
            check,
        )?),
        Action::Namespaces => Inventory::Evidence(EvidenceScan::open(
            &node.files,
            EvidenceKind::Namespaces,
            auth.value().cluster,
            check,
        )?),
        _ => return Err(Status::permission_denied("an inventory grant is required").into()),
    };
    let renewal = renewing(input, auth.clone());
    let check = auth.checking();
    Ok(monitored(
        Box::pin(async_stream::try_stream! {
            let _renewal = renewal;
            let mut scan = scan;
            let mut number = 0;
            loop {
                auth.check().map_err(error)?;
                let (next, bytes, complete) = tokio::task::spawn_blocking(move || -> Result<_> {
                    let (bytes, complete) = match &mut scan {
                        Inventory::Fragments(value) => { let page = value.page(number)?; (serde_json::to_vec(&page)?, page.complete) }
                        Inventory::Evidence(value) => { let page = value.page(number)?; (serde_json::to_vec(&page)?, page.complete) }
                    };
                    Ok((scan, bytes, complete))
                }).await.map_err(|_| Status::internal("storage inventory failed"))?.map_err(error)?;
                scan = next;
                auth.check().map_err(error)?;
                yield wire::StorageOutput { data: vec![], page_json: bytes, complete };
                if complete { break; }
                number += 1;
            }
        }),
        check,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::fragment::Area;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn storage_failures_distinguish_damage_from_transient_errors() {
        assert_eq!(
            error(crate::content::codec::IntegrityError::Hash.into()).code(),
            tonic::Code::DataLoss
        );
        assert_eq!(
            error(std::io::Error::from(std::io::ErrorKind::NotFound).into()).code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            error(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into()).code(),
            tonic::Code::DataLoss
        );
        for kind in [
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::TimedOut,
        ] {
            assert_eq!(
                error(std::io::Error::from(kind).into()).code(),
                tonic::Code::FailedPrecondition
            );
        }
        assert_eq!(
            error(Status::unavailable("disconnected").into()).code(),
            tonic::Code::Unavailable
        );
    }

    #[tokio::test]
    async fn expired_inventory_releases_its_freeze_without_receiver_progress() -> Result<()> {
        let (_directory, _, _, files) = fragments::tests::fixture("8MiB").await?;
        for _ in 0..257 {
            fragments::write(
                &files,
                fragments::tests::header(&files, 0),
                fragments::tests::input(bytes::Bytes::new(), true),
                fragments::tests::allowed(),
            )
            .await?;
        }
        let active = Arc::new(AtomicBool::new(true));
        let running = active.clone();
        let check: Check = Arc::new(move || {
            ensure!(
                running.load(Ordering::Acquire),
                "expired test authorization"
            );
            Ok(())
        });
        let mut scan = Scan::open(&files, Area::Blocks, 0, check.clone())?;
        let output: Output = Box::pin(async_stream::try_stream! {
            let mut number = 0;
            loop {
                let page = scan.page(number).map_err(error)?;
                yield wire::StorageOutput {
                    data: vec![], page_json: serde_json::to_vec(&page).unwrap(), complete: page.complete,
                };
                if page.complete { break; }
                number += 1;
            }
        });
        let mut output = monitored(output, check);
        assert!(
            output
                .next()
                .await
                .context("missing inventory page")?
                .is_ok()
        );
        assert!(files.mutations.clone().try_write_owned().is_err());
        active.store(false, Ordering::Release);
        let stopped = tokio::time::timeout(Duration::from_secs(2), async {
            while files.mutations.clone().try_write_owned().is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        drop(output);
        stopped?;
        Ok(())
    }
}
