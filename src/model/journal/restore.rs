use super::record::{BlockKey, Change, Material, Reference};
use crate::{
    authorization::Principal,
    content::codec,
    model::Id,
    storage::journal::{
        replay::Summary,
        source::{Cut, Source},
    },
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[schema(as=MetadataRecoverySelection)]
#[serde(deny_unknown_fields)]
pub(crate) struct Selection {
    pub project: Uuid,
    pub buckets: BTreeMap<Uuid, String>,
    pub destination: Option<Uuid>,
    #[serde(default)]
    pub exclusive_takeover: bool,
    #[serde(default)]
    pub complete_journal_upload: bool,
}

#[derive(Clone, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[schema(as=MetadataRecoveryProgress)]
pub(crate) struct Progress {
    pub buckets: Vec<Summary>,
    pub required_keys: u32,
    pub conflicts: Vec<String>,
    pub pending_uploads: Option<u32>,
    pub recovered_buckets: Vec<String>,
    pub candidate_files: u64,
}

#[derive(Clone, Serialize, utoipa::ToSchema)]
#[schema(as=MetadataRecovery)]
pub(crate) struct View {
    pub id: Uuid,
    pub state: String,
    pub revision: String,
    pub progress: Progress,
    pub error: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct Unlocked {
    pub source: Source,
    pub cut: Cut,
    pub actor: Option<Uuid>,
    pub expires_us: i64,
    pub storage_name: String,
    pub claim: Option<crate::storage::journal::history::Claim>,
}
impl Unlocked {
    pub fn seal(&self, id: Uuid, key: &codec::Key) -> Result<Vec<u8>> {
        codec::protect(
            &serde_json::to_vec(self)?,
            key,
            format!("metadata-recovery-{id}").as_bytes(),
        )
    }
    pub fn open(data: &[u8], id: Uuid, key: &codec::Key) -> Result<Self> {
        Ok(serde_json::from_slice(&codec::unprotect_bytes(
            data,
            key,
            format!("metadata-recovery-{id}").as_bytes(),
        )?)?)
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct Job {
    pub id: Uuid,
    pub state: String,
    pub source: Vec<u8>,
    pub selection: Option<sqlx::types::Json<Selection>>,
    pub progress: sqlx::types::Json<Progress>,
    pub error_detail: Option<String>,
    pub revision: i64,
    pub source_storage: Uuid,
    pub target_storage: Option<Uuid>,
}
impl Job {
    pub fn view(self) -> View {
        View {
            id: self.id,
            state: self.state,
            revision: self.revision.to_string(),
            progress: self.progress.0,
            error: self.error_detail,
        }
    }
}

pub(crate) struct Location {
    pub native: Id,
    pub kind: &'static str,
}
pub(crate) fn key_hash(reference: Reference) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&serde_json::to_vec(&reference)?).as_bytes())
}
pub(crate) fn nonce_id(nonce: Option<&[u8]>) -> Result<Option<i64>> {
    nonce
        .map(|value| {
            ensure!(value.len() == 12, "invalid journal nonce");
            let id = i64::from_be_bytes(value[4..].try_into()?);
            ensure!(id > 0, "invalid recovered nonce counter");
            Ok(id)
        })
        .transpose()
}

#[async_trait::async_trait]
pub(crate) trait RecoveryStore: Send + Sync {
    async fn recovery_create(
        &self,
        principal: &Principal,
        id: Uuid,
        storage: Uuid,
        source: &[u8],
        progress: &Progress,
    ) -> Result<()>;
    async fn recovery(&self, id: Uuid) -> Result<Job>;
    async fn recovery_protects(&self, storage: Uuid) -> Result<bool>;
    async fn recovery_recent(&self) -> Result<Vec<Uuid>>;
    async fn expire_recovery_previews(&self) -> Result<()>;
    async fn recovery_jobs(&self) -> Result<Vec<Uuid>>;
    async fn recovery_queue(
        &self,
        principal: &Principal,
        id: Uuid,
        revision: i64,
        selection: &Selection,
        source: &[u8],
        target: Uuid,
    ) -> Result<()>;
    async fn recovery_control(
        &self,
        principal: &Principal,
        id: Uuid,
        revision: i64,
        retry: bool,
    ) -> Result<()>;
    async fn recovery_intent(&self, id: Uuid, token: Uuid, source: &[u8]) -> Result<()>;
    async fn recovery_claim(&self, id: Uuid, token: Uuid) -> Result<bool>;
    async fn recovery_save(
        &self,
        id: Uuid,
        token: Uuid,
        state: &str,
        progress: &Progress,
        error: Option<&str>,
    ) -> Result<()>;
    async fn recovery_key(
        &self,
        job: Uuid,
        token: Uuid,
        key: &BlockKey,
        protected: &[u8],
    ) -> Result<()>;
    async fn recovery_key_conflict(&self, key: &BlockKey) -> Result<bool>;
    async fn recovery_location_conflict(
        &self,
        target: Uuid,
        kind: super::record::Kind,
        ids: &[Uuid],
    ) -> Result<bool>;
    async fn recovery_material(
        &self,
        job: Uuid,
        token: Uuid,
        target: Uuid,
        value: &Material,
        pack: Option<(Id, i32, i64)>,
    ) -> Result<Id>;
    async fn recovery_bucket(
        &self,
        job: Uuid,
        token: Uuid,
        bucket: Uuid,
        name: &str,
        generation: i64,
        sequence: i64,
        in_place: bool,
    ) -> Result<(Uuid, bool, Option<String>)>;
    async fn recovery_item(
        &self,
        job: Uuid,
        token: Uuid,
        change: &Change,
        locations: &[Location],
    ) -> Result<()>;
    async fn recovery_publish(
        &self,
        job: Uuid,
        token: Uuid,
        source: Uuid,
        project: Uuid,
        target: Uuid,
    ) -> Result<()>;
    async fn recovery_orphan(
        &self,
        job: Uuid,
        token: Uuid,
        address: Uuid,
        path: &str,
        size: i64,
    ) -> Result<()>;
    async fn recovery_adopt(
        &self,
        job: Uuid,
        token: Uuid,
        target: Uuid,
        descriptor: &crate::storage::marker::Journal,
    ) -> Result<()>;
}
