use super::{
    frozen::{Medium, Scope},
    server,
};
use crate::{
    config::{Budget, node::NodeConfig},
    rpc::wire::{
        self,
        storage_recovery_server::{StorageRecovery, StorageRecoveryServer},
    },
};
use anyhow::{Context, Result, ensure};
use futures_util::{StreamExt, TryStreamExt, stream::BoxStream};
use std::{path::Path, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{Request, Response, Status};
use uuid::Uuid;

pub(crate) async fn credential(path: &Path) -> Result<String> {
    let bytes = crate::storage::local::read_bounded(path, 4096).await?;
    let text = std::str::from_utf8(&bytes)?.trim_end_matches(['\r', '\n']);
    ensure!(
        text.len() >= 32 && text.bytes().all(|b| b.is_ascii_graphic()),
        "invalid recovery credential file"
    );
    Ok(text.to_owned())
}

struct Service {
    medium: Arc<Medium>,
    token: String,
    slots: Arc<Semaphore>,
}

impl Service {
    fn permit<T>(&self, request: &Request<T>) -> Result<OwnedSemaphorePermit, Status> {
        let token = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        if request.metadata().get_all("authorization").iter().count() != 1
            || token.is_none_or(|v| {
                aws_lc_rs::constant_time::verify_slices_are_equal(
                    v.as_bytes(),
                    self.token.as_bytes(),
                )
                .is_err()
            })
        {
            return Err(Status::unauthenticated("invalid recovery credential"));
        }
        self.slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("recovery reader is busy"))
    }

    fn medium(&self, id: &str) -> Result<Arc<Medium>, Status> {
        if Uuid::parse_str(id).ok() != Some(self.medium.identity.media.media_id) {
            return Err(Status::failed_precondition("source medium changed"));
        }
        Ok(self.medium.clone())
    }
}

#[tonic::async_trait]
impl StorageRecovery for Service {
    async fn identify(
        &self,
        request: Request<wire::RecoveryIdentify>,
    ) -> Result<Response<wire::StorageInspection>, Status> {
        let _permit = self.permit(&request)?;
        let m = self.medium.clone();
        tokio::task::spawn_blocking(move || m.check())
            .await
            .map_err(error)?
            .map_err(error)?;
        Ok(Response::new(wire::StorageInspection {
            identity_json: serde_json::to_vec(&self.medium.identity).map_err(error)?,
        }))
    }

    type InventoryStream = BoxStream<'static, Result<wire::StorageResult, Status>>;
    async fn inventory(
        &self,
        request: Request<wire::RecoveryScan>,
    ) -> Result<Response<Self::InventoryStream>, Status> {
        let permit = self.permit(&request)?;
        let request = request.into_inner();
        let medium = self.medium(&request.medium)?;
        if Uuid::parse_str(&request.view).ok() != Some(medium.identity.view) {
            return Err(Status::failed_precondition("frozen inventory view changed"));
        }
        let namespace = Uuid::parse_str(&request.namespace)
            .map_err(|_| Status::invalid_argument("invalid namespace"))?;
        let scope = match request.scope.as_str() {
            "layouts" => Scope::Layouts,
            "history" => Scope::History,
            "recovery" => Scope::Recovery,
            _ => return Err(Status::invalid_argument("invalid recovery scope")),
        };
        let prefix = request
            .prefix
            .try_into()
            .map_err(|_| Status::invalid_argument("invalid directory prefix"))?;
        let mut pages = medium.pages(namespace, scope, prefix);
        Ok(Response::new(async_stream::try_stream! {
            let _permit=permit;
            while let Some(page)=pages.try_next().await.map_err(error)? {
                yield wire::StorageResult {result_json:serde_json::to_vec(&page).map_err(error)?};
            }
        }.boxed()))
    }

    async fn read(
        &self,
        request: Request<wire::RecoveryRead>,
    ) -> Result<Response<wire::RecoveryBytes>, Status> {
        let permit = self.permit(&request)?;
        let request = request.into_inner();
        let medium = self.medium(&request.medium)?;
        let receipt = serde_json::from_slice(&request.receipt_json)
            .map_err(|_| Status::invalid_argument("invalid fragment receipt"))?;
        let read = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            medium.range(&receipt, request.offset, request.length)
        })
        .await
        .map_err(error)?
        .map_err(error)?;
        Ok(Response::new(read))
    }
}

fn error(e: impl std::fmt::Display) -> Status {
    tracing::debug!(error=%e,"offline recovery read failed");
    Status::failed_precondition("source data is unavailable or failed validation")
}

pub(crate) async fn serve(config: NodeConfig, budget: Budget) -> Result<()> {
    let path = std::env::var_os("MOKYU_STORAGE_RECOVERY_TOKEN_FILE")
        .context("set MOKYU_STORAGE_RECOVERY_TOKEN_FILE")?;
    let token = credential(Path::new(&path)).await?;
    let medium = Medium::open(config.instance.data_path.clone())?;
    let (mut server, incoming) =
        server::listener(&config.instance, budget.request_connections).await?;
    let service = Service {
        medium,
        token,
        slots: Arc::new(Semaphore::new(budget.storage_read_slots)),
    };
    server
        .add_service(
            StorageRecoveryServer::new(service)
                .max_decoding_message_size(32 * 1024)
                .max_encoding_message_size(8 * 1024 * 1024),
        )
        .serve_with_incoming_shutdown(incoming, crate::runtime::roles::shutdown_signal())
        .await?;
    Ok(())
}
