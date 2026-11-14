use super::{
    Node, forward, fragments,
    transfer::{self, Authorization, Cancel},
};
use crate::model::storage_transfer::{Action, SignedReceipt};
use crate::rpc::wire::{
    self,
    storage_node_server::{StorageNode, StorageNodeServer},
};
use anyhow::{Context, Result};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch},
};
use tonic::{
    Request, Response, Status,
    transport::{Identity, Server, ServerTlsConfig},
};

#[cfg(test)]
#[path = "../../../tests/storage/native_transport.rs"]
pub(super) mod tests;

struct Connection {
    socket: TcpStream,
    _permit: OwnedSemaphorePermit,
}

impl tonic::transport::server::Connected for Connection {
    type ConnectInfo = ();
    fn connect_info(&self) {}
}
impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.socket).poll_read(cx, bytes)
    }
}
impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.socket).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

struct Service {
    node: Arc<Node>,
    requests: Arc<Semaphore>,
    reads: Arc<Semaphore>,
    forwarding: crate::storage::native::transport::Pool,
}

struct Writing(Arc<Node>, uuid::Uuid);

impl Drop for Writing {
    fn drop(&mut self) {
        let mut active = self.0.active_writes.lock().unwrap();
        if let Some(index) = active.iter().position(|id| *id == self.1) {
            active.swap_remove(index);
        }
    }
}

#[tonic::async_trait]
impl StorageNode for Service {
    async fn write(
        &self,
        request: Request<tonic::Streaming<wire::StorageInput>>,
    ) -> Result<Response<wire::StorageResult>, Status> {
        let permit = self
            .node
            .writes
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("storage write capacity is full"))?;
        let mut input = request.into_inner();
        let auth = transfer::first(self.node.clone(), &mut input, permit)
            .await
            .map_err(transfer::error)?;
        let _cancel = Cancel(auth.clone());
        let grant = auth.value();
        let (Action::Write { header } | Action::Repair { header }) = grant.action else {
            return Err(Status::permission_denied("a write grant is required"));
        };
        self.node
            .active_writes
            .lock()
            .unwrap()
            .push(header.fragment);
        let _writing = Writing(self.node.clone(), header.fragment);
        let (sender, receiver) = mpsc::channel(2);
        let receipt = async {
            let writing = fragments::write(&self.node.files, header, receiver, auth.checking());
            let feeding = transfer::feed(input, auth.clone(), sender);
            tokio::pin!(writing, feeding);
            // Keep the disk error when a failed writer closes the input channel first.
            tokio::select! {
                written = &mut writing => {
                    let receipt = written?;
                    feeding.await?;
                    Ok::<_, anyhow::Error>(receipt)
                }
                fed = &mut feeding => {
                    let receipt = writing.await?;
                    fed?;
                    Ok(receipt)
                }
            }
        }
        .await
        .map_err(transfer::error)?;
        result_json(&self.receipt(grant.id, receipt).map_err(transfer::error)?)
    }

    async fn forward(
        &self,
        request: Request<tonic::Streaming<wire::StorageInput>>,
    ) -> Result<Response<wire::StorageResult>, Status> {
        let permit = self
            .node
            .writes
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("storage write capacity is full"))?;
        let mut input = request.into_inner();
        let first = transfer::next(&mut input).await.map_err(transfer::error)?;
        if !first.data.is_empty() || first.complete {
            return Err(Status::invalid_argument(
                "forwarding must begin with a plan",
            ));
        }
        let targets = forward::plan(&self.node, &first.grant_json).map_err(transfer::error)?;
        let auth = Authorization::open(
            self.node.clone(),
            &serde_json::to_vec(&targets[0].authorization)
                .map_err(|_| Status::internal("cannot encode forwarding grant"))?,
            permit,
        )
        .map_err(transfer::error)?;
        let _cancel = Cancel(auth.clone());
        let grant = auth.value();
        let Action::Write { header } = &grant.action else {
            return Err(Status::permission_denied(
                "forwarding requires a write grant",
            ));
        };
        self.node
            .active_writes
            .lock()
            .unwrap()
            .push(header.fragment);
        let _writing = Writing(self.node.clone(), header.fragment);
        let (local, remote) = forward::write(&self.node, &self.forwarding, input, auth, targets)
            .await
            .map_err(transfer::error)?;
        result_json(&vec![
            self.receipt(grant.id, local).map_err(transfer::error)?,
            remote,
        ])
    }

    type ReadStream = transfer::Output;
    async fn read(
        &self,
        request: Request<tonic::Streaming<wire::StorageInput>>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let permit = self
            .reads
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("storage read capacity is full"))?;
        let mut input = request.into_inner();
        let auth = transfer::first(self.node.clone(), &mut input, permit)
            .await
            .map_err(transfer::error)?;
        transfer::read(self.node.clone(), input, auth)
            .await
            .map(Response::new)
            .map_err(transfer::error)
    }

    async fn state(
        &self,
        request: Request<wire::StorageRequest>,
    ) -> Result<Response<wire::StorageResult>, Status> {
        let auth = self.authorize(request.into_inner())?;
        let _cancel = Cancel(auth.clone());
        let Action::Stat { header } = auth.value().action else {
            return Err(Status::permission_denied("a state grant is required"));
        };
        let result = fragments::inspect(&self.node.files, header, auth.checking())
            .await
            .map_err(transfer::error)?;
        result_json(&result)
    }

    async fn remove(
        &self,
        request: Request<wire::StorageRequest>,
    ) -> Result<Response<wire::StorageResult>, Status> {
        let auth = self.authorize(request.into_inner())?;
        let _cancel = Cancel(auth.clone());
        let removed = match auth.value().action {
            Action::Remove { receipt } => {
                fragments::remove(&self.node.files, receipt, auth.checking()).await
            }
            Action::Discard { header } => {
                fragments::discard(&self.node.files, header, auth.checking()).await
            }
            _ => return Err(Status::permission_denied("a remove grant is required")),
        }
        .map_err(transfer::error)?;
        result_json(&serde_json::json!({ "removed": removed }))
    }

    async fn publish(
        &self,
        request: Request<wire::StorageRequest>,
    ) -> Result<Response<wire::StorageResult>, Status> {
        let auth = self.authorize(request.into_inner())?;
        let _cancel = Cancel(auth.clone());
        let Action::Publish {
            proof,
            manifest: history,
        } = auth.value().action
        else {
            return Err(Status::permission_denied("a publication grant is required"));
        };
        let manifest = self
            .node
            .files
            .manifest
            .read()
            .unwrap()
            .clone()
            .ok_or_else(|| Status::failed_precondition("storage membership is missing"))?;
        let link = super::recovery::commit_historical(
            &self.node.files,
            proof,
            manifest,
            history,
            auth.checking(),
        )
        .await
        .map_err(transfer::error)?;
        result_json(&link)
    }

    type InventoryStream = transfer::Output;
    async fn inventory(
        &self,
        request: Request<tonic::Streaming<wire::StorageInput>>,
    ) -> Result<Response<Self::InventoryStream>, Status> {
        let permit = self
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("storage control capacity is full"))?;
        let mut input = request.into_inner();
        let auth = transfer::first(self.node.clone(), &mut input, permit)
            .await
            .map_err(transfer::error)?;
        transfer::inventory(self.node.clone(), input, auth)
            .await
            .map(Response::new)
            .map_err(transfer::error)
    }

    async fn inspect(
        &self,
        request: Request<wire::StorageInspect>,
    ) -> Result<Response<wire::StorageInspection>, Status> {
        let _permit = self
            .requests
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("storage inspection capacity is full"))?;
        self.inspect_authorize(&request)?;
        Ok(Response::new(wire::StorageInspection { identity_json: serde_json::to_vec(&serde_json::json!({
            "node_id": self.node.client.identity.node_id, "media": self.node.files.media,
            "manifest": *self.node.files.manifest.read().unwrap(), "capacity": self.node.files.capacity(self.node.boot).map_err(|_| Status::internal("storage capacity is unavailable"))?
        })).map_err(|_| Status::internal("encode storage inspection"))? }))
    }

    type LayoutsStream = transfer::Output;
    async fn layouts(
        &self,
        request: Request<wire::StorageInspect>,
    ) -> Result<Response<Self::LayoutsStream>, Status> {
        let permit = self
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("storage control capacity is full"))?;
        self.inspect_authorize(&request)?;
        let request = request.into_inner();
        let run = request
            .run
            .ok_or_else(|| Status::invalid_argument("storage run is required"))?;
        transfer::layouts(self.node.clone(), permit, run, request.manifest)
            .await
            .map(Response::new)
            .map_err(transfer::error)
    }
}

fn result_json(value: &impl serde::Serialize) -> Result<Response<wire::StorageResult>, Status> {
    let result_json =
        serde_json::to_vec(value).map_err(|_| Status::internal("encode storage result"))?;
    Ok(Response::new(wire::StorageResult { result_json }))
}

impl Service {
    fn receipt(
        &self,
        grant: uuid::Uuid,
        receipt: crate::model::storage_payload::Receipt,
    ) -> Result<SignedReceipt> {
        let accounting = self
            .node
            .files
            .capacity(self.node.boot)?
            .accounting
            .ok_or_else(|| Status::unavailable("storage accounting is not ready"))?;
        SignedReceipt::seal(
            grant,
            receipt,
            accounting,
            &self.node.client.identity.credential,
        )
    }

    fn inspect_authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        if request.metadata().get_all("authorization").iter().count() != 1 {
            return Err(Status::unauthenticated(
                "one storage credential is required",
            ));
        }
        let credential = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| Status::unauthenticated("storage credential is required"))?;
        aws_lc_rs::constant_time::verify_slices_are_equal(
            credential.as_bytes(),
            self.node.client.identity.credential.as_bytes(),
        )
        .map_err(|_| Status::unauthenticated("invalid storage credential"))?;
        if !self.node.ready() {
            return Err(Status::failed_precondition(
                "storage has no active control session",
            ));
        }
        Ok(())
    }

    fn authorize(&self, request: wire::StorageRequest) -> Result<Arc<Authorization>, Status> {
        let permit = self
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("storage control capacity is full"))?;
        Authorization::open(self.node.clone(), &request.grant_json, permit).map_err(transfer::error)
    }
}

pub(super) async fn serve(node: Arc<Node>, mut stopping: watch::Receiver<bool>) -> Result<()> {
    let instance = &node.config.instance;
    let listener = TcpListener::bind(instance.listen_storage.as_deref().unwrap_or("0.0.0.0:9003"))
        .await
        .context("storage listener")?;
    let connections = Arc::new(Semaphore::new(node.budget.request_connections));
    let incoming = async_stream::stream! {
        loop {
            let socket = match listener.accept().await {
                Ok((socket, _)) => socket,
                Err(error) => { yield Err(error); break; }
            };
            if let Err(error) = socket.set_nodelay(true) { yield Err(error); break; }
            if let Ok(permit) = connections.clone().try_acquire_owned() { yield Ok(Connection { socket, _permit: permit }); }
        }
    };
    let mut server = Server::builder()
        .concurrency_limit_per_connection(crate::model::storage_transfer::STREAMS_PER_CONNECTION)
        .max_concurrent_streams(crate::model::storage_transfer::STREAMS_PER_CONNECTION as u32)
        .http2_keepalive_interval(Some(Duration::from_secs(20)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)));
    if instance.storage_transport.as_deref().unwrap_or("tls") == "tls" {
        let cert = crate::storage::local::read_bounded(
            instance.storage_tls_cert.as_ref().unwrap(),
            256 * 1024,
        )
        .await?;
        let key = crate::storage::local::read_bounded(
            instance.storage_tls_key.as_ref().unwrap(),
            64 * 1024,
        )
        .await?;
        server =
            server.tls_config(ServerTlsConfig::new().identity(Identity::from_pem(cert, key)))?;
    }
    server
        .add_service(
            StorageNodeServer::new(Service {
                requests: Arc::new(Semaphore::new(node.budget.storage_control_slots)),
                reads: Arc::new(Semaphore::new(node.budget.storage_read_slots)),
                forwarding: crate::storage::native::transport::Pool::new(Duration::from_secs(300)),
                node,
            })
            .max_decoding_message_size(crate::model::storage_transfer::GRANT_LIMIT)
            .max_encoding_message_size(1024 * 1024),
        )
        .serve_with_incoming_shutdown(incoming, async move {
            if !*stopping.borrow_and_update() {
                let _ = stopping.changed().await;
            }
        })
        .await?;
    Ok(())
}
