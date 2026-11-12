use super::Node;
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
    sync::{OwnedSemaphorePermit, Semaphore, watch},
};
use tonic::{
    Request, Response, Status,
    transport::{Identity, Server, ServerTlsConfig},
};

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
    requests: Semaphore,
}

#[tonic::async_trait]
impl StorageNode for Service {
    async fn inspect(
        &self,
        request: Request<wire::StorageInspect>,
    ) -> Result<Response<wire::StorageInspection>, Status> {
        let _permit = self
            .requests
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("storage inspection capacity is full"))?;
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
        Ok(Response::new(wire::StorageInspection { identity_json: serde_json::to_vec(&serde_json::json!({
            "node_id": self.node.client.identity.node_id, "media": self.node.files.media,
            "manifest": *self.node.files.manifest.read().unwrap(), "capacity": self.node.files.capacity().map_err(|_| Status::internal("storage capacity is unavailable"))?
        })).map_err(|_| Status::internal("encode storage inspection"))? }))
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
        .concurrency_limit_per_connection(16)
        .max_concurrent_streams(16)
        .timeout(Duration::from_secs(15));
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
                node,
                requests: Semaphore::new(4),
            })
            .max_decoding_message_size(1024)
            .max_encoding_message_size(256 * 1024),
        )
        .serve_with_incoming_shutdown(incoming, async move {
            if !*stopping.borrow_and_update() {
                let _ = stopping.changed().await;
            }
        })
        .await?;
    Ok(())
}
