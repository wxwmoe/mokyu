use crate::{
    content::fragment::UNIT,
    model::storage_transfer::{
        Action, Completion, Destination, FragmentState, GRANT_LIMIT, SignedGrant, SignedReceipt,
    },
    rpc::wire::{self, storage_node_client::StorageNodeClient},
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, stream::BoxStream};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

const WRITE_DEADLINE: Duration = Duration::from_secs(15 * 60);

#[async_trait::async_trait]
pub(crate) trait Renewal: Send + Sync {
    async fn renew(&self, grant: &SignedGrant) -> Result<SignedGrant>;
}

pub(crate) struct Pool {
    clients: Mutex<HashMap<(String, bool), StorageNodeClient<Channel>>>,
    timeout: Duration,
}

pub(crate) enum Inventory {
    Fragments(crate::model::storage_recovery::InventoryPage),
    Evidence(crate::model::storage_recovery::EvidencePage),
}

struct ReadRequests(BoxStream<'static, wire::StorageInput>);

impl futures_util::Stream for ReadRequests {
    type Item = wire::StorageInput;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.as_mut().poll_next(cx)
    }
}

impl Pool {
    pub async fn inventory(
        &self,
        target: Destination,
        renewal: Arc<dyn Renewal>,
    ) -> Result<BoxStream<'static, Result<Inventory>>> {
        let fragments = matches!(target.authorization.grant.action, Action::Scan { .. });
        ensure!(
            fragments
                || matches!(
                    target.authorization.grant.action,
                    Action::History { .. } | Action::Layouts | Action::Namespaces
                ),
            "inventory plan required"
        );
        let (guard, stopped) = oneshot::channel();
        let input = read_requests(target.authorization, renewal, stopped)?;
        let mut output = self
            .client(&target.url)?
            .inventory(ReadRequests(input))
            .await?
            .into_inner();
        Ok(Box::pin(async_stream::try_stream! {
            let _guard = guard;
            let mut number = 0u64;
            let mut complete = false;
            let mut prefix = 0;
            while let Some(message) = output.message().await? {
                (!complete && message.data.is_empty() && message.page_json.len() <= 1024 * 1024)
                    .then_some(()).context("invalid storage inventory page")?;
                let page = if fragments {
                    let page: crate::model::storage_recovery::InventoryPage = serde_json::from_slice(&message.page_json)?;
                    (page.number == number && page.complete == message.complete && page.entries.len() <= 128
                        && page.restart_prefix >= prefix && page.restart_prefix < 4096)
                        .then_some(()).context("storage inventory page is out of sequence")?;
                    prefix = page.restart_prefix;
                    Inventory::Fragments(page)
                } else {
                    let page: crate::model::storage_recovery::EvidencePage = serde_json::from_slice(&message.page_json)?;
                    (page.number == number && page.complete == message.complete && page.entries.len() <= 4)
                        .then_some(()).context("storage evidence page is out of sequence")?;
                    Inventory::Evidence(page)
                };
                complete = message.complete;
                number = number.checked_add(1).context("storage inventory page overflow")?;
                yield page;
            }
            complete.then_some(()).context("storage inventory ended without completion")?;
        }))
    }

    pub async fn layouts(
        &self,
        url: &str,
        credential: &str,
        run: crate::model::session::Run,
        manifest: &crate::model::storage_cluster::Manifest,
    ) -> Result<BoxStream<'static, Result<crate::model::storage_recovery::EvidencePage>>> {
        let mut request = tonic::Request::new(wire::StorageInspect {
            run: Some(wire::Run {
                boot_id: run.boot.to_string(),
                fence: run.fence.to_string(),
            }),
            manifest: manifest.digest()?,
        });
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {credential}").parse()?);
        let mut output = self.client(url)?.layouts(request).await?.into_inner();
        Ok(Box::pin(async_stream::try_stream! {
            let mut number = 0;
            let mut complete = false;
            while let Some(message) = output.message().await? {
                (!complete && message.data.is_empty() && message.page_json.len() <= 1024 * 1024)
                    .then_some(()).context("invalid storage layout page")?;
                let page: crate::model::storage_recovery::EvidencePage = serde_json::from_slice(&message.page_json)?;
                (page.number == number && page.complete == message.complete && page.entries.len() <= 4)
                    .then_some(()).context("storage layout page is out of sequence")?;
                complete = page.complete;
                number = number.checked_add(1).context("storage layout page overflow")?;
                yield page;
            }
            complete.then_some(()).context("storage layout inspection ended without completion")?;
        }))
    }

    pub fn new(timeout: Duration) -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            timeout,
        }
    }

    fn client(&self, url: &str) -> Result<StorageNodeClient<Channel>> {
        self.channel(url, false)
    }

    fn channel(&self, url: &str, writing: bool) -> Result<StorageNodeClient<Channel>> {
        crate::config::storage::url(url)?;
        let key = (url.to_owned(), writing);
        let mut clients = self.clients.lock().unwrap();
        if let Some(client) = clients.get(&key) {
            return Ok(client.clone());
        }
        let endpoint = Endpoint::from_shared(url.to_owned())?
            .connect_timeout(self.timeout.min(Duration::from_secs(10)))
            .timeout(if writing {
                self.timeout.max(WRITE_DEADLINE)
            } else {
                self.timeout
            })
            .concurrency_limit(crate::model::storage_transfer::STREAMS_PER_CONNECTION)
            .buffer_size(crate::model::storage_transfer::STREAMS_PER_CONNECTION)
            .initial_stream_window_size(1024 * 1024)
            .initial_connection_window_size(2 * 1024 * 1024)
            .tcp_nodelay(true)
            .tls_config(ClientTlsConfig::new().with_native_roots())?;
        let client = StorageNodeClient::new(endpoint.connect_lazy())
            .max_encoding_message_size(GRANT_LIMIT)
            .max_decoding_message_size(1024 * 1024);
        if clients.len() >= 1024 {
            if let Some(old) = clients.keys().next().cloned() {
                clients.remove(&old);
            }
        }
        clients.insert(key, client.clone());
        Ok(client)
    }

    pub async fn write(
        &self,
        target: Destination,
        body: BoxStream<'_, Result<Bytes>>,
        renewal: &dyn Renewal,
    ) -> Result<Completion> {
        Ok(self
            .write_many(vec![target], body, renewal)
            .await?
            .remove(0))
    }

    pub async fn forward(
        &self,
        targets: Vec<Destination>,
        body: BoxStream<'_, Result<Bytes>>,
        renewal: &dyn Renewal,
    ) -> Result<Vec<Completion>> {
        ensure!(
            (2..=crate::model::storage_cluster::Layout::PLAN_MEMBERS).contains(&targets.len()),
            "invalid number of forwarding destinations"
        );
        self.write_many(targets, body, renewal).await
    }

    async fn write_many(
        &self,
        mut targets: Vec<Destination>,
        mut body: BoxStream<'_, Result<Bytes>>,
        renewal: &dyn Renewal,
    ) -> Result<Vec<Completion>> {
        let target = &targets[0];
        let Some(header) = target.authorization.grant.action.write_header() else {
            anyhow::bail!("write plan required");
        };
        let expected = header.clone();
        let forwarding = targets.len() > 1;
        let mut client = self.channel(&target.url, true)?;
        let (send, receive) = mpsc::channel(2);
        let progress = Arc::new(Mutex::new(Instant::now()));
        let last_frame = progress.clone();
        let input = futures_util::stream::unfold(receive, |mut receive| async move {
            receive.recv().await.map(|value| (value, receive))
        })
        .inspect(move |frame: &wire::StorageInput| {
            if !frame.data.is_empty() || frame.complete {
                *last_frame.lock().unwrap() = Instant::now();
            }
        });
        let request = async {
            let response = if forwarding {
                client.forward(input).await?
            } else {
                client.write(input).await?
            }
            .into_inner();
            Ok::<Vec<SignedReceipt>, anyhow::Error>(if forwarding {
                serde_json::from_slice(&response.result_json)?
            } else {
                vec![serde_json::from_slice(&response.result_json)?]
            })
        };
        let feed = async {
            send.send(write_message(&targets)?)
                .await
                .context("storage writer stopped")?;
            let mut renew_at = Instant::now() + Duration::from_secs(20);
            let mut digest = blake3::Hasher::new();
            let mut sent = 0u64;
            loop {
                let next = tokio::select! {
                    value = body.next() => value,
                    _ = tokio::time::sleep_until(renew_at) => {
                        refresh(&mut targets, renewal, &send).await?;
                        renew_at = Instant::now() + Duration::from_secs(20);
                        continue;
                    }
                };
                let Some(bytes) = next else {
                    break;
                };
                let bytes = bytes?;
                for bytes in bytes.chunks(UNIT) {
                    if Instant::now() >= renew_at {
                        refresh(&mut targets, renewal, &send).await?;
                        renew_at = Instant::now() + Duration::from_secs(20);
                    }
                    sent = sent
                        .checked_add(bytes.len() as u64)
                        .context("storage write length overflow")?;
                    ensure!(
                        sent <= expected.shard_bytes,
                        "storage write exceeds its plan"
                    );
                    digest.update(bytes);
                    send.send(wire::StorageInput {
                        grant_json: vec![],
                        data: bytes.to_vec(),
                        complete: false,
                    })
                    .await
                    .context("storage writer stopped")?;
                }
            }
            ensure!(
                sent == expected.shard_bytes,
                "storage write ended before its planned length"
            );
            send.send(wire::StorageInput {
                grant_json: vec![],
                data: vec![],
                complete: true,
            })
            .await
            .context("storage writer stopped")?;
            Ok::<_, anyhow::Error>(*digest.finalize().as_bytes())
        };
        let transfer = tokio::time::timeout(self.timeout.max(WRITE_DEADLINE), async {
            tokio::try_join!(request, feed)
        });
        let (receipts, digest) = tokio::select! {
            result = transfer => result.map_err(|_| tonic::Status::deadline_exceeded("storage write exceeded its deadline"))??,
            _ = write_stalled(&progress, self.timeout) => {
                return Err(tonic::Status::deadline_exceeded("storage write made no progress").into());
            }
        };
        ensure!(
            receipts.len() == targets.len(),
            "storage forwarding omitted a receipt"
        );
        targets
            .into_iter()
            .zip(receipts)
            .map(|(target, receipt)| {
                let signed = target.authorization;
                let Some(header) = signed.grant.action.write_header() else {
                    anyhow::bail!("write plan required");
                };
                let mut expected = header.clone();
                ensure!(
                    expected.digest == [0; 32] || expected.digest == digest,
                    "storage write checksum differs from its plan"
                );
                expected.digest = digest;
                receipt.receipt.validate()?;
                ensure!(
                    receipt.grant == signed.grant.id && receipt.receipt.header == expected,
                    "storage receipt differs from the submitted bytes"
                );
                Ok(Completion {
                    authorization: signed,
                    receipt,
                })
            })
            .collect()
    }

    pub fn read(
        &self,
        target: Destination,
        renewal: Arc<dyn Renewal>,
    ) -> futures_util::future::BoxFuture<'static, Result<BoxStream<'static, Result<Bytes>>>> {
        let client = self.client(&target.url);
        Box::pin(async move {
            let mut signed = target.authorization;
            let Some((_, _, length)) = signed.grant.action.read_range() else {
                anyhow::bail!("read plan required");
            };
            if signed.grant.expires <= chrono::Utc::now().timestamp() + 5 {
                let next = renewal.renew(&signed).await?;
                signed.grant.renewal(&next.grant)?;
                signed = next;
            }
            let (guard, stopped) = oneshot::channel();
            let input = read_requests(signed, renewal, stopped)?;
            let mut client = client?;
            let request = tonic::Request::new(ReadRequests(input));
            let output = client.read(request).await?.into_inner();
            Ok(read_body(output, length, guard))
        })
    }

    pub async fn state(&self, target: &Destination) -> Result<FragmentState> {
        let Action::Stat { header } = &target.authorization.grant.action else {
            anyhow::bail!("state plan required");
        };
        let reply = self
            .client(&target.url)?
            .state(wire::StorageRequest {
                grant_json: serde_json::to_vec(&target.authorization)?,
            })
            .await?
            .into_inner();
        let state: FragmentState = serde_json::from_slice(&reply.result_json)?;
        state.validate(header)?;
        Ok(state)
    }

    pub async fn check_backup(&self, target: &Destination) -> Result<Completion> {
        ensure!(
            matches!(
                target.authorization.grant.action,
                Action::BackupCheck { .. }
            ),
            "backup verification grant required"
        );
        let reply = self
            .client(&target.url)?
            .state(wire::StorageRequest {
                grant_json: serde_json::to_vec(&target.authorization)?,
            })
            .await?
            .into_inner();
        Ok(Completion {
            authorization: target.authorization.clone(),
            receipt: serde_json::from_slice(&reply.result_json)?,
        })
    }

    pub async fn publish(&self, target: &Destination) -> Result<()> {
        let Action::Publish { proof, .. } = &target.authorization.grant.action else {
            anyhow::bail!("publication plan required");
        };
        let reply = self
            .client(&target.url)?
            .publish(wire::StorageRequest {
                grant_json: serde_json::to_vec(&target.authorization)?,
            })
            .await?
            .into_inner();
        let link: crate::model::storage_recovery::Link =
            serde_json::from_slice(&reply.result_json)?;
        ensure!(
            link == proof.link()?,
            "storage acknowledged another publication"
        );
        Ok(())
    }

    pub async fn remove(&self, target: &Destination) -> Result<()> {
        ensure!(
            matches!(
                target.authorization.grant.action,
                Action::Remove { .. } | Action::BackupRemove { .. } | Action::Discard { .. }
            ),
            "remove plan required"
        );
        self.client(&target.url)?
            .remove(wire::StorageRequest {
                grant_json: serde_json::to_vec(&target.authorization)?,
            })
            .await?;
        Ok(())
    }
}

async fn write_stalled(progress: &Mutex<Instant>, idle: Duration) {
    loop {
        let deadline = *progress.lock().unwrap() + idle;
        if deadline <= Instant::now() {
            return;
        }
        tokio::time::sleep_until(deadline).await;
    }
}

fn message(grant: &SignedGrant) -> Result<wire::StorageInput> {
    Ok(wire::StorageInput {
        grant_json: serde_json::to_vec(grant)?,
        data: vec![],
        complete: false,
    })
}

async fn refresh(
    targets: &mut [Destination],
    renewal: &dyn Renewal,
    sender: &mpsc::Sender<wire::StorageInput>,
) -> Result<()> {
    for target in targets.iter_mut() {
        let next = renewal.renew(&target.authorization).await?;
        target.authorization.grant.renewal(&next.grant)?;
        target.authorization = next;
    }
    sender
        .send(write_message(targets)?)
        .await
        .context("storage writer stopped")?;
    Ok(())
}

fn write_message(targets: &[Destination]) -> Result<wire::StorageInput> {
    if targets.len() == 1 {
        return message(&targets[0].authorization);
    }
    let grant_json = serde_json::to_vec(targets)?;
    ensure!(
        grant_json.len() <= GRANT_LIMIT,
        "forwarding plan exceeds its bound"
    );
    Ok(wire::StorageInput {
        grant_json,
        data: vec![],
        complete: false,
    })
}

fn read_requests(
    mut signed: SignedGrant,
    renewal: Arc<dyn Renewal>,
    mut stopped: oneshot::Receiver<()>,
) -> Result<BoxStream<'static, wire::StorageInput>> {
    let first = message(&signed)?;
    Ok(Box::pin(async_stream::stream! {
        yield first;
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                _ = tokio::time::sleep(Duration::from_secs(20)) => (),
            }
            let next = tokio::select! {
                _ = &mut stopped => break,
                next = renewal.renew(&signed) => next,
            };
            let Ok(next) = next else { break; };
            if signed.grant.renewal(&next.grant).is_err() { break; }
            signed = next;
            let Ok(next) = message(&signed) else { break; };
            yield next;
        }
    }))
}

fn read_body(
    mut output: tonic::Streaming<wire::StorageOutput>,
    length: u64,
    guard: oneshot::Sender<()>,
) -> BoxStream<'static, Result<Bytes>> {
    Box::pin(async_stream::try_stream! {
        let _guard = guard;
        let mut received = 0u64;
        let mut complete = false;
        while let Some(message) = output.message().await? {
            (!complete && message.page_json.is_empty() && message.data.len() <= UNIT)
                .then_some(()).context("invalid storage read frame")?;
            if message.complete {
                (message.data.is_empty() && received == length).then_some(())
                    .context("storage read ended before its planned length")?;
                complete = true;
            } else {
                (!message.data.is_empty()).then_some(()).context("empty storage read frame")?;
                received = received.checked_add(message.data.len() as u64).context("storage read length overflow")?;
                (received <= length).then_some(()).context("storage read exceeds its plan")?;
                yield Bytes::from(message.data);
            }
        }
        complete.then_some(()).context("storage read has no completion frame")?;
    })
}
