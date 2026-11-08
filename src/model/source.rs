use crate::model::Id;
use crate::{
    authorization::Principal,
    model::{PhysicalSource, task::Execution},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::ToSchema;
use uuid::Uuid;

#[cfg(test)]
#[path = "../../tests/recovery/source_quarantine.rs"]
mod tests;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ToSchema, clap::ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SourceType {
    Chunk,
    Pack,
    Raw,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceScope {
    pub storage_id: Uuid,
    pub source: SourceType,
    pub id: String,
}
impl SourceScope {
    pub fn validate(&self) -> Result<()> {
        self.identity()?;
        Ok(())
    }
    pub fn identity(&self) -> Result<Id> {
        let id = self
            .id
            .parse::<Id>()
            .map_err(|_| s3s::s3_error!(InvalidArgument))?;
        ensure!(id.is_valid(), s3s::s3_error!(InvalidArgument));
        Ok(id)
    }
    pub fn packed(&self) -> bool {
        self.source == SourceType::Pack
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct SourceSnapshot {
    pub scope: SourceScope,
    pub physical: PhysicalSource,
    pub state: String,
    pub revision: i64,
    pub quarantine_code: Option<String>,
    pub storage_revision: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier_revision: Option<i64>,
    pub identity_marker: Value,
    pub manifest_digest: Option<String>,
}
impl SourceSnapshot {
    pub fn physical_revision(&self) -> Result<i64> {
        if self.physical.storage()? == self.scope.storage_id {
            Ok(self.storage_revision)
        } else {
            self.tier_revision
                .ok_or(super::Error::OperationAborted.into())
        }
    }

    pub fn repairable(&self) -> bool {
        self.quarantine_code.is_some()
            && (self.state == "ready"
                || (self.scope.source == SourceType::Chunk
                    && self.state == "uploading"
                    && self.quarantine_code.as_deref() == Some("remote_missing")))
    }
    pub fn fingerprint(&self) -> Result<String> {
        Ok(blake3::hash(&serde_json::to_vec(self)?)
            .to_hex()
            .to_string())
    }
    pub fn location(&self) -> Uuid {
        match &self.physical {
            PhysicalSource::Chunk(c) => c.location_id,
            PhysicalSource::Pack(p) => p.location_id,
            PhysicalSource::Raw(r) => r.location_id,
        }
    }
    pub fn summary(&self) -> Result<Value> {
        Ok(
            json!({"storage_id":self.scope.storage_id,"source":self.scope.source,"id":self.scope.id,
            "location_id":self.location(),"state":self.state,"revision":self.revision.to_string(),
            "quarantine_code":self.quarantine_code,"fingerprint":self.fingerprint()?,
            "members":match &self.physical { PhysicalSource::Chunk(_) | PhysicalSource::Raw(_) => 1, PhysicalSource::Pack(p) => p.member_count }}),
        )
    }
}

#[derive(Serialize, ToSchema)]
pub(crate) struct QuarantinedSource {
    #[serde(flatten)]
    pub scope: SourceScope,
    pub location_id: Uuid,
    pub state: String,
    pub quarantined_at: String,
    pub code: String,
    pub revision: String,
}

#[derive(Deserialize, utoipa::IntoParams)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceQuery {
    pub storage_id: Uuid,
    pub source: SourceType,
    #[serde(default)]
    pub after: String,
    pub limit: Option<i64>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct SourcePage {
    pub items: Vec<QuarantinedSource>,
    pub next: Option<String>,
}

pub(crate) fn quarantine_code(code: &str) -> bool {
    matches!(
        code,
        "remote_missing"
            | "length_mismatch"
            | "authentication_failed"
            | "decompression_failed"
            | "hash_mismatch"
    )
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct SourceOperation {
    pub principal: Principal,
    pub mode: String,
    pub snapshot: SourceSnapshot,
    pub code: Option<String>,
    pub node: Option<Uuid>,
}

pub(crate) struct SourceUpdate<'a> {
    pub execution: Execution,
    pub operation: &'a SourceOperation,
    pub apply: bool,
}

pub(crate) fn manifest_digest(
    pack: &crate::content::pack::Pack,
    members: &[crate::content::pack::frame::Entry],
) -> Result<String> {
    crate::content::pack::frame::validate_entries(
        members,
        u64::try_from(pack.stored_size.context("missing pack size")?)?,
        pack.raw_size,
        pack.member_count,
        false,
    )?;
    Ok(blake3::hash(&serde_json::to_vec(members)?)
        .to_hex()
        .to_string())
}
