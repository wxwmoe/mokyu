use crate::authorization::{Action, Permit, Principal};
use crate::model::{Id, raw::RawSource, task::Execution};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[schema(as = ReplicationOperation)]
pub(crate) enum Operation {
    Copy,
    Move,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[schema(as = ReplicationConflict)]
pub(crate) enum Conflict {
    Skip,
    #[default]
    Report,
    Overwrite,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[schema(as = ReplicationPermissions)]
pub(crate) enum Permissions {
    #[default]
    Inherit,
    Ignore,
    PublicRead,
    Private,
}

impl Permissions {
    pub(crate) fn public(self, source: bool, target: Option<bool>) -> bool {
        match self {
            Self::Inherit => source,
            Self::Ignore => target.unwrap_or(false),
            Self::PublicRead => true,
            Self::Private => false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(tag = "selection", rename_all = "snake_case", deny_unknown_fields)]
#[schema(as = ReplicationScope)]
pub(crate) enum Scope {
    All,
    Prefix { prefix: String },
    Objects { keys: Vec<String> },
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = ReplicationOptions)]
pub(crate) struct Options {
    pub source_bucket: Uuid,
    pub target_bucket: Uuid,
    pub operation: Operation,
    pub scope: Scope,
    #[serde(default)]
    pub target_prefix: String,
    pub target_key: Option<String>,
    #[serde(default)]
    pub conflict: Conflict,
    #[serde(default)]
    pub permissions: Permissions,
}

fn valid_key(key: &str, prefix: bool) -> bool {
    (prefix || !key.is_empty()) && key.len() <= 1024 && !key.contains('\0')
}

impl Options {
    pub(crate) fn validate(&self) -> Result<()> {
        let invalid = || s3s::s3_error!(InvalidArgument, "invalid replication scope");
        ensure!(
            !self.source_bucket.is_nil()
                && !self.target_bucket.is_nil()
                && valid_key(&self.target_prefix, true),
            invalid()
        );
        if let Some(key) = &self.target_key {
            ensure!(
                self.target_prefix.is_empty()
                    && valid_key(key, false)
                    && matches!(&self.scope, Scope::Objects { keys } if keys.len() == 1),
                invalid()
            );
        }
        match &self.scope {
            Scope::All => ensure!(self.source_bucket != self.target_bucket, invalid()),
            Scope::Prefix { prefix } => {
                ensure!(valid_key(prefix, true), invalid());
                if self.source_bucket == self.target_bucket {
                    ensure!(
                        !prefix.starts_with(&self.target_prefix)
                            && !self.target_prefix.starts_with(prefix),
                        invalid()
                    );
                }
            }
            Scope::Objects { keys } => {
                ensure!(!keys.is_empty() && keys.len() <= 1000, invalid());
                let selected: BTreeSet<_> = keys.iter().collect();
                ensure!(selected.len() == keys.len(), invalid());
                let mut targets = BTreeSet::new();
                for key in keys {
                    ensure!(valid_key(key, false), invalid());
                    let target = self.target(key)?;
                    ensure!(
                        targets.insert(target.clone())
                            && (self.source_bucket != self.target_bucket
                                || !selected.contains(&target)),
                        invalid()
                    );
                }
            }
        }
        Ok(())
    }

    pub(crate) fn target(&self, key: &str) -> Result<String> {
        let suffix = match &self.scope {
            Scope::Prefix { prefix } => key.strip_prefix(prefix),
            Scope::Objects { keys } if !keys.iter().any(|selected| selected == key) => None,
            _ => Some(key),
        }
        .ok_or_else(|| s3s::s3_error!(InvalidArgument, "object is outside replication scope"))?;
        let target = self
            .target_key
            .clone()
            .unwrap_or_else(|| format!("{}{suffix}", self.target_prefix));
        ensure!(valid_key(&target, false), s3s::s3_error!(InvalidArgument));
        Ok(target)
    }

    pub(crate) fn authority(&self, principal: Principal) -> Permit {
        let mut permit = Permit::for_action(principal, self.source_bucket, Action::Read);
        if !matches!(self.scope, Scope::Objects { .. }) {
            permit.add(self.source_bucket, Action::List);
        }
        if self.operation == Operation::Move {
            permit.add(self.source_bucket, Action::Delete);
        }
        permit.add(self.target_bucket, Action::Write);
        if self.permissions != Permissions::Ignore {
            permit.add(self.target_bucket, Action::Acl);
        }
        permit
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = ReplicationProgress)]
pub(crate) struct Progress {
    pub discovered: i64,
    pub completed: i64,
    pub skipped: i64,
    pub conflicts: i64,
    pub sources: i64,
    pub verified: i64,
}

#[derive(Serialize, ToSchema)]
#[schema(as = Replication)]
pub(crate) struct Replication {
    pub id: Uuid,
    pub state: String,
    pub phase: String,
    pub options: Options,
    pub progress: Progress,
    pub error: Option<String>,
}

#[derive(Serialize, ToSchema)]
#[schema(as = ReplicationItem)]
pub(crate) struct Item {
    pub id: Id,
    pub source_key: String,
    pub target_key: String,
    pub source_version: Option<Id>,
    pub output_version: Option<Id>,
    pub state: String,
    pub error: Option<String>,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = ReplicationCommand)]
pub(crate) enum Command {
    Pause,
    Resume,
    Cancel,
}

pub(crate) enum Step {
    Continue,
    Scan(super::transfer::Scan),
    Copy(super::transfer::Copy),
    Raw(super::transfer::RawCopy),
    Verify {
        storage: Uuid,
        sources: Vec<RawSource>,
    },
}

#[async_trait]
pub(crate) trait ReplicationStore: Send + Sync {
    async fn create(&self, principal: &Principal, options: &Options) -> Result<Uuid>;
    async fn view(&self, principal: &Principal, id: Uuid) -> Result<Replication>;
    async fn list(
        &self,
        principal: &Principal,
        after: Option<Uuid>,
        limit: i64,
    ) -> Result<(Vec<Replication>, Option<Uuid>)>;
    async fn items(
        &self,
        principal: &Principal,
        id: Uuid,
        after: Option<Id>,
        limit: i64,
    ) -> Result<Vec<Item>>;
    async fn command(&self, principal: &Principal, id: Uuid, command: Command) -> Result<()>;
    async fn step(&self, task: Execution) -> Result<Step>;
    async fn verified(&self, task: Execution, sources: &[RawSource]) -> Result<()>;
    async fn scanned(&self, task: Execution, value: &super::transfer::Scanned) -> Result<()>;
    async fn raw_progress(
        &self,
        task: Execution,
        plan: &super::transfer::RawCopy,
        progress: &super::transfer::RawProgress,
    ) -> Result<()>;
    async fn copied(
        &self,
        task: Execution,
        plan: &super::transfer::Copy,
        outputs: &[super::packing::PackOutput],
    ) -> Result<()>;
}

#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Snapshot {
    pub metadata: Value,
    pub checksums: Value,
    pub public: bool,
    pub size: i64,
    pub etag: String,
    pub payload: String,
}

pub(crate) fn same_source(left: &RawSource, right: &RawSource) -> bool {
    let mut left = left.clone();
    left.revision = right.revision;
    left == *right
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(scope: Scope) -> Options {
        Options {
            source_bucket: Uuid::new_v4(),
            target_bucket: Uuid::new_v4(),
            operation: Operation::Copy,
            scope,
            target_prefix: "out/".into(),
            target_key: None,
            conflict: Conflict::Report,
            permissions: Permissions::Inherit,
        }
    }

    #[test]
    fn mappings_preserve_s3_keys_and_reject_overlap() {
        let mut value = options(Scope::Prefix {
            prefix: "in/".into(),
        });
        for suffix in ["a//b", "../a", "%2f", "e\u{301}.jpg", "A.PNG"] {
            assert_eq!(
                value.target(&format!("in/{suffix}")).unwrap(),
                format!("out/{suffix}")
            );
        }
        assert!(value.target("other/a").is_err());
        value.target_bucket = value.source_bucket;
        assert!(value.validate().is_ok());
        value.target_prefix = "in/copy/".into();
        assert!(value.validate().is_err());
        value.target_prefix.clear();
        assert!(value.validate().is_err());
    }

    #[test]
    fn explicit_mapping_rejects_duplicates_and_source_clobbering() {
        let mut value = options(Scope::Objects {
            keys: vec!["a".into(), "a".into()],
        });
        assert!(value.validate().is_err());
        value.scope = Scope::Objects {
            keys: vec!["a".into(), "out/a".into()],
        };
        value.target_bucket = value.source_bucket;
        assert!(value.validate().is_err());
        value.scope = Scope::Objects {
            keys: vec!["a".into()],
        };
        value.target_prefix.clear();
        value.target_key = Some("renamed".into());
        assert!(value.validate().is_ok());
        assert_eq!(value.target("a").unwrap(), "renamed");
        assert!(value.target("unselected").is_err());
        value.target_key = Some("a".into());
        assert!(value.validate().is_err());
    }

    #[test]
    fn ignoring_permissions_uses_the_target_at_publication() {
        assert!(!Permissions::Ignore.public(true, None));
        assert!(Permissions::Ignore.public(false, Some(true)));
        assert!(!Permissions::Ignore.public(true, Some(false)));
        assert!(Permissions::Inherit.public(true, Some(false)));
        assert!(Permissions::PublicRead.public(false, None));
        assert!(!Permissions::Private.public(true, Some(true)));
        let mut value = options(Scope::All);
        value.permissions = Permissions::Ignore;
        assert!(
            !value.authority(Principal::Local).resources[&value.target_bucket]
                .contains(&Action::Acl)
        );
    }
}
