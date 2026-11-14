use super::{storage_payload::Receipt, storage_transfer::Completion};
use crate::{
    authorization::Principal,
    content::{codec::Key, fragment::Header},
};
use anyhow::Result;
use uuid::Uuid;

// An unconfirmed slot may have a file on disk, but is never a readable receipt.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub(crate) enum Origin {
    Durable(Receipt),
    Planned { header: Header, peer: Receipt },
}

impl Origin {
    pub fn header(&self) -> &Header {
        match self {
            Self::Durable(r) => &r.header,
            Self::Planned { header, .. } => header,
        }
    }

    pub fn receipt(&self) -> Option<&Receipt> {
        match self {
            Self::Durable(r) => Some(r),
            Self::Planned { .. } => None,
        }
    }

    pub fn evidence(&self) -> &Receipt {
        match self {
            Self::Durable(r) | Self::Planned { peer: r, .. } => r,
        }
    }

    pub fn validate(&self) -> Result<()> {
        let proof = self.evidence();
        proof.validate()?;
        if let Self::Planned { header, .. } = self {
            header.validate()?;
            let mut expected = header.clone();
            expected.fragment = proof.header.fragment;
            expected.media = proof.header.media;
            expected.generation = proof.header.generation;
            expected.layout_revision = proof.header.layout_revision;
            expected.copy = proof.header.copy;
            expected.digest = proof.header.digest;
            anyhow::ensure!(
                header.copy != proof.header.copy
                    && expected == proof.header
                    && (header.digest == [0; 32] || header.digest == proof.header.digest),
                "unwritten fragment has no matching durable peer"
            );
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct Repair {
    pub original: Origin,
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
