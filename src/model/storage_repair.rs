use super::{storage_payload::Receipt, storage_transfer::Completion};
use crate::{
    authorization::Principal,
    content::{codec::Key, fragment::Header},
};
use anyhow::Result;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct Repair {
    pub original: Receipt,
    pub candidate: Option<Header>,
    pub state: String,
    pub original_removed: bool,
    pub candidate_removed: bool,
    pub protected_until: i64,
    pub collecting: bool,
}

#[async_trait::async_trait]
pub(crate) trait Repairs: Send + Sync {
    async fn request_repair(
        &self,
        principal: &Principal,
        cluster: Uuid,
        fragment: Uuid,
    ) -> Result<Uuid>;
    async fn repair(&self, id: Uuid) -> Result<Repair>;
    async fn resume_repair(
        &self,
        id: Uuid,
        until: i64,
    ) -> Result<Option<super::storage_payload::Mapping>>;
    async fn prepare_repair(&self, id: Uuid, damaged: bool) -> Result<Repair>;
    async fn accept_repair(&self, key: &Key, id: Uuid, receipt: &Completion) -> Result<()>;
}
