use crate::{content::codec, storage::external::Connection};
use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Snapshot {
    pub external_id: Uuid,
    pub revision: i64,
    pub connection: Connection,
    pub access_key: String,
    pub secret_key: String,
}

impl Snapshot {
    pub(crate) fn seal(&self, key: &codec::Key) -> Result<Vec<u8>> {
        self.connection.validate()?;
        crate::storage::external::credentials(&self.access_key, &self.secret_key)?;
        codec::protect(&serde_json::to_vec(self)?, key, self.context().as_bytes())
    }

    fn context(&self) -> String {
        format!("external-{}-test-{}", self.external_id, self.revision)
    }

    pub(crate) fn open(id: Uuid, revision: i64, bytes: &[u8], key: &codec::Key) -> Result<Self> {
        let value: Self = serde_json::from_str(&codec::unprotect(
            bytes,
            key,
            format!("external-{id}-test-{revision}").as_bytes(),
        )?)?;
        ensure!(
            value.external_id == id && value.revision == revision,
            "external snapshot differs"
        );
        value.connection.validate()?;
        crate::storage::external::credentials(&value.access_key, &value.secret_key)?;
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = ExternalTestPhase)]
pub(crate) enum Phase {
    Inspect,
    Write,
    ConditionalWrite,
    ReadAcl,
    WriteAcl,
    PublicRead,
    RestoreAcl,
    StartMultipart,
    CompleteMultipart,
    StartAbort,
    Abort,
    Cleanup,
    Done,
}

impl Phase {
    pub(crate) fn next(self) -> Self {
        match self {
            Self::Inspect => Self::Write,
            Self::Write => Self::ConditionalWrite,
            Self::ConditionalWrite => Self::ReadAcl,
            Self::ReadAcl => Self::WriteAcl,
            Self::WriteAcl => Self::PublicRead,
            Self::PublicRead => Self::RestoreAcl,
            Self::RestoreAcl => Self::StartMultipart,
            Self::StartMultipart => Self::CompleteMultipart,
            Self::CompleteMultipart => Self::StartAbort,
            Self::StartAbort => Self::Abort,
            Self::Abort => Self::Cleanup,
            Self::Cleanup | Self::Done => Self::Done,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = ExternalCapability)]
pub(crate) enum Capability {
    List,
    Head,
    Read,
    Range,
    Write,
    Readback,
    Delete,
    ConditionalRead,
    ConditionalWrite,
    AclRead,
    AclWrite,
    PublicRead,
    PrivateAccess,
    MultipartList,
    MultipartStart,
    MultipartParts,
    MultipartComplete,
    MultipartAbort,
    Version,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = ExternalCheckState)]
pub(crate) enum CheckState {
    Passed,
    Denied,
    Blocked,
    Unsupported,
    Skipped,
    Failed,
    Unknown,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = ExternalCheck)]
pub(crate) struct Check {
    pub state: CheckState,
    pub status: Option<u16>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = ExternalTestFile)]
pub(crate) enum TestFile {
    Write,
    Multipart,
    Abort,
}

impl TestFile {
    pub(crate) fn key(self, test: Uuid) -> String {
        let name = match self {
            Self::Write => "write",
            Self::Multipart => "multipart",
            Self::Abort => "abort",
        };
        format!(".mokyu-tests/{test}/{name}")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = ExternalTestArtifact)]
pub(crate) struct Artifact {
    pub file: TestFile,
    pub version: Option<String>,
    pub upload: Option<String>,
    pub etag: Option<String>,
    pub cleaned: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = ExternalTestProblem)]
pub(crate) enum Problem {
    Unavailable,
    InvalidResponse,
    IdentityConflict,
    UncertainResult,
    CleanupPending,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = ExternalTestReport)]
pub(crate) struct Report {
    pub connection: Connection,
    pub phase: Phase,
    pub checks: BTreeMap<Capability, Check>,
    pub artifacts: Vec<Artifact>,
    pub blocked: Option<Problem>,
}

impl Report {
    pub(crate) fn new(connection: Connection) -> Self {
        use Capability::*;
        Self {
            connection,
            phase: Phase::Inspect,
            artifacts: vec![],
            blocked: None,
            checks: [
                List,
                Head,
                Read,
                Range,
                Write,
                Readback,
                Delete,
                ConditionalRead,
                ConditionalWrite,
                AclRead,
                AclWrite,
                PublicRead,
                PrivateAccess,
                MultipartList,
                MultipartStart,
                MultipartParts,
                MultipartComplete,
                MultipartAbort,
                Version,
            ]
            .into_iter()
            .map(|value| {
                (
                    value,
                    Check {
                        state: CheckState::Skipped,
                        status: None,
                    },
                )
            })
            .collect(),
        }
    }

    pub(crate) fn check(&mut self, capability: Capability, state: CheckState, status: Option<u16>) {
        self.checks.insert(capability, Check { state, status });
    }

    pub(crate) fn passed(&self, capability: Capability) -> bool {
        self.checks
            .get(&capability)
            .is_some_and(|value| value.state == CheckState::Passed)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.connection.validate()?;
        ensure!(
            self.checks.len() == 19 && self.artifacts.len() <= 8,
            "invalid external test bounds"
        );
        ensure!(
            serde_json::to_vec(self)?.len() <= 48 * 1024,
            "external test report exceeds budget"
        );
        ensure!(
            self.checks.values().all(|value| value
                .status
                .is_none_or(|status| (100..=599).contains(&status))),
            "invalid external response status"
        );
        for item in &self.artifacts {
            ensure!(
                [&item.version, &item.upload, &item.etag]
                    .into_iter()
                    .flatten()
                    .all(|value| !value.is_empty()
                        && value.len() <= 4096
                        && !value.chars().any(char::is_control)),
                "invalid external artifact identity"
            );
            ensure!(
                item.upload.is_none() || item.version.is_none(),
                "ambiguous external artifact"
            );
        }
        if self.phase == Phase::Done {
            ensure!(
                self.blocked.is_none() && self.artifacts.iter().all(|item| item.cleaned),
                "external cleanup is incomplete"
            );
        }
        Ok(())
    }

    pub(crate) fn advance(&self, result: &Self) -> Result<()> {
        result.validate()?;
        ensure!(
            serde_json::to_value(&self.connection)? == serde_json::to_value(&result.connection)?,
            "external connection changed"
        );
        ensure!(
            self.phase != Phase::Done
                && ((result.blocked.is_some() && result.phase == self.phase)
                    || (result.blocked.is_none() && result.phase == self.phase.next())),
            "invalid external test transition"
        );
        ensure!(
            result.artifacts.len() >= self.artifacts.len(),
            "external artifacts disappeared"
        );
        for (old, new) in self.artifacts.iter().zip(&result.artifacts) {
            ensure!(
                old.file == new.file
                    && old.version == new.version
                    && old.upload == new.upload
                    && old.etag == new.etag
                    && (!old.cleaned || new.cleaned),
                "external artifact identity changed"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Plan {
    pub id: Uuid,
    pub report: Report,
    pub recover: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct State {
    pub report: Report,
    pub started: bool,
}

pub(crate) struct Start {
    pub external_id: Uuid,
    pub revision: i64,
    pub node: Option<Uuid>,
    pub connection_encrypted: Vec<u8>,
    pub report: Report,
}

pub(crate) struct Assignment {
    pub plan: Plan,
    pub external_id: Uuid,
    pub revision: i64,
    pub connection_encrypted: Vec<u8>,
}

#[derive(Serialize, ToSchema)]
#[schema(as = ExternalTest)]
pub(crate) struct Test {
    pub id: Uuid,
    pub external_id: Option<Uuid>,
    pub revision: i64,
    pub node_id: Option<Uuid>,
    pub attempts: i32,
    pub state: String,
    pub stopped: bool,
    pub report: Report,
    pub cleanup_required: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report() -> Report {
        Report::new(Connection {
            endpoint_url: "https://example.invalid".into(),
            region: "region".into(),
            bucket: "bucket".into(),
            prefix: String::new(),
            virtual_hosted: false,
            allow_http: false,
        })
    }

    #[test]
    fn test_progress_preserves_artifacts_and_unknown_outcomes() {
        let mut before = report();
        before.phase = Phase::Write;
        let mut next = before.clone();
        next.phase = before.phase.next();
        next.artifacts.push(Artifact {
            file: TestFile::Write,
            version: Some("v1".into()),
            upload: None,
            etag: None,
            cleaned: false,
        });
        before.advance(&next).unwrap();
        let mut invalid = next.clone();
        invalid.phase = Phase::Done;
        assert!(invalid.validate().is_err());
        invalid = next.clone();
        invalid.phase = next.phase.next();
        invalid.artifacts.clear();
        assert!(next.advance(&invalid).is_err());
        invalid = next.clone();
        invalid.blocked = Some(Problem::UncertainResult);
        next.advance(&invalid).unwrap();
        invalid.phase = next.phase.next();
        assert!(next.advance(&invalid).is_err());
    }
}
