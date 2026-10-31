use crate::{
    authorization::Principal,
    model::replication::{self, Permissions, Progress, external::Connection},
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Location {
    Local {
        root: std::path::PathBuf,
        prefix: String,
    },
    Remote {
        root: String,
        path: String,
    },
}

impl Location {
    pub(crate) fn storage(record: &crate::model::storage::StorageRecord) -> Result<Self> {
        match record.store_type.as_str() {
            "local" => Ok(Self::Local {
                root: record
                    .path
                    .as_deref()
                    .context("local storage path is missing")?
                    .into(),
                prefix: record.prefix.clone(),
            }),
            "s3" => Self::remote(
                &crate::storage::external::Connection {
                    endpoint_url: record.endpoint.clone().context("S3 endpoint is missing")?,
                    region: record.region.clone().context("S3 region is missing")?,
                    bucket: record.bucket.clone().context("S3 bucket is missing")?,
                    prefix: record.prefix.clone(),
                    virtual_hosted: false,
                    allow_http: true,
                },
                "",
            ),
            _ => anyhow::bail!("unknown storage type"),
        }
    }

    pub(crate) fn remote(
        connection: &crate::storage::external::Connection,
        prefix: &str,
    ) -> Result<Self> {
        fn address(url: &str) -> Result<String> {
            let (_, address) = url.split_once("://").context("invalid S3 object address")?;
            Ok(format!("s3://{address}"))
        }
        let mut root = connection.clone();
        root.prefix.clear();
        Ok(Self::Remote {
            root: address(&root.object_prefix("")?)?,
            path: address(&connection.object_prefix(prefix)?)?,
        })
    }

    pub(crate) async fn resolve(&self) -> Result<Space> {
        use sha2::{Digest, Sha256};
        let (domain, path) = match self {
            Self::Local { root, prefix } => {
                fn path(value: &std::path::Path) -> Result<String> {
                    let value = value
                        .to_str()
                        .context("invalid storage path")?
                        .replace(std::path::MAIN_SEPARATOR, "/");
                    Ok(format!("file:{}/", value.trim_end_matches('/')))
                }
                let root = tokio::fs::canonicalize(root).await?;
                let physical = tokio::fs::canonicalize(root.join(prefix)).await?;
                (path(&root)?, path(&physical)?)
            }
            Self::Remote { root, path } => (root.clone(), path.clone()),
        };
        Ok(Space {
            domain: Sha256::digest(domain.as_bytes()).to_vec(),
            path,
        })
    }
}

pub(crate) const ORIGIN: &str = "mokyu-replica-origin";

pub(crate) fn marked(metadata: &serde_json::Value) -> bool {
    metadata["user"]
        .as_object()
        .is_some_and(|user| user.keys().any(|name| name.eq_ignore_ascii_case(ORIGIN)))
}

pub(crate) struct Space {
    pub domain: Vec<u8>,
    pub path: String,
}

impl Space {
    pub(crate) fn independent(&self, domain: &[u8], path: &str) -> bool {
        self.domain != domain && !overlaps(&self.path, path)
    }
}

fn overlaps(a: &str, b: &str) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

pub(crate) struct Placement {
    pub source: Space,
    pub target: Space,
}

#[derive(sqlx::FromRow)]
pub(crate) struct Reservation {
    pub source_bucket: Uuid,
    pub target_bucket: Option<Uuid>,
    pub target_prefix: String,
    pub source_space: String,
    pub target_space: String,
    pub target_domain: Vec<u8>,
}

impl Placement {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.source
                .independent(&self.target.domain, &self.target.path),
            s3s::s3_error!(
                InvalidArgument,
                "redundancy requires independent storage backends"
            )
        );
        Ok(())
    }

    pub(crate) fn check(
        &self,
        source: Uuid,
        settings: &Settings,
        existing: &Reservation,
    ) -> Result<()> {
        let target = (!settings.target_external).then_some(settings.target_bucket);
        ensure!(
            existing.target_bucket != Some(source) && target != Some(existing.source_bucket),
            s3s::s3_error!(InvalidArgument, "redundancy links cannot be chained")
        );
        ensure!(
            existing.source_bucket != source
                || (existing.target_domain != self.target.domain
                    && !overlaps(&existing.target_space, &self.target.path)),
            s3s::s3_error!(
                InvalidArgument,
                "redundancy targets must use independent storage backends"
            )
        );
        let conflict = match (target, existing.target_bucket) {
            (Some(a), Some(b)) => {
                a == b && overlaps(&settings.target_prefix, &existing.target_prefix)
            }
            _ => {
                overlaps(&self.target.path, &existing.target_space)
                    || target.is_none() && overlaps(&self.target.path, &existing.source_space)
                    || existing.target_bucket.is_none()
                        && overlaps(&self.source.path, &existing.target_space)
            }
        };
        ensure!(
            !conflict,
            s3s::s3_error!(InvalidArgument, "redundancy target namespaces overlap")
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[schema(as = RedundancyMode)]
pub(crate) enum Mode {
    #[default]
    Asynchronous,
    Synchronous,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[schema(as = RedundancySettings)]
pub(crate) struct Settings {
    pub target_bucket: Uuid,
    #[serde(default)]
    pub target_external: bool,
    #[serde(default)]
    pub target_prefix: String,
    #[serde(default)]
    pub mode: Mode,
    pub permissions: Option<Permissions>,
    #[serde(default)]
    pub propagate_deletes: bool,
    #[serde(default = "default_max_pending_keys")]
    #[schema(minimum = 1, maximum = 1000000, default = 4096)]
    pub max_pending_keys: i32,
}

fn default_max_pending_keys() -> i32 {
    4096
}

impl Settings {
    pub(crate) fn replication(&self, source: Uuid) -> Result<replication::Options> {
        ensure!(
            (1..=1_000_000).contains(&self.max_pending_keys),
            s3s::s3_error!(
                InvalidArgument,
                "pending key limit must be between 1 and 1000000"
            )
        );
        ensure!(
            self.mode != Mode::Synchronous || !self.target_external,
            s3s::s3_error!(
                InvalidArgument,
                "synchronous redundancy requires an internal target"
            )
        );
        let options = replication::Options {
            source_bucket: source,
            target_bucket: self.target_bucket,
            source_external: false,
            target_external: self.target_external,
            operation: replication::Operation::Copy,
            scope: replication::Scope::All,
            target_prefix: self.target_prefix.clone(),
            target_key: None,
            conflict: replication::Conflict::Overwrite,
            permissions: self.permissions.unwrap_or(if self.target_external {
                Permissions::Ignore
            } else {
                Permissions::Inherit
            }),
            propagate_deletes: self.propagate_deletes,
        };
        options.validate()?;
        Ok(options)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[schema(as = RedundancyAction)]
pub(crate) enum Action {
    Pause,
    Resume,
    Retry,
    Reconcile,
    Remove,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = RedundancyCommand)]
pub(crate) struct Command {
    pub revision: i64,
    pub action: Action,
    pub prefix: Option<String>,
}

impl Command {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.revision > 0
                && self.prefix.as_ref().is_none_or(|prefix| {
                    self.action == Action::Reconcile
                        && prefix.len() <= 1024
                        && !prefix.contains('\0')
                }),
            s3s::s3_error!(InvalidArgument, "invalid redundancy command")
        );
        Ok(())
    }
}

#[derive(Serialize, ToSchema)]
#[schema(as = RedundancyPreview)]
pub(crate) struct Preview {
    pub source_bucket: Uuid,
    pub settings: Settings,
    pub source_storage: Uuid,
    pub target_storage: Option<Uuid>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = RedundancyStatus)]
pub(crate) enum Status {
    Backfilling,
    Reconciling,
    CatchingUp,
    Healthy,
    Lagging,
    Pausing,
    Paused,
    Failed,
    Removing,
}

#[derive(Serialize, ToSchema)]
#[schema(as = RedundancyLink)]
pub(crate) struct Link {
    pub id: Uuid,
    pub source_bucket: Uuid,
    pub task_id: Uuid,
    pub settings: Settings,
    pub revision: i64,
    pub status: Status,
    pub progress: Progress,
    pub pending: i64,
    pub queued_keys: i32,
    pub confirmed: Option<i64>,
    pub reconciled_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

#[derive(sqlx::FromRow)]
pub(crate) struct Row {
    pub id: Uuid,
    pub source_bucket: Uuid,
    pub settings: sqlx::types::Json<Settings>,
    pub revision: i64,
    pub state: String,
    pub reconciled_at: Option<DateTime<Utc>>,
    pub progress: sqlx::types::Json<Progress>,
    pub task_state: String,
    pub error: Option<String>,
    pub pending: i64,
    pub queued_keys: i32,
    pub confirmed: i64,
}

impl Row {
    pub(crate) fn view(self) -> Link {
        let status = match self.state.as_str() {
            "removing" => Status::Removing,
            "pausing" => Status::Pausing,
            "paused" => Status::Paused,
            _ if self.task_state == "failed" || self.error.is_some() && self.pending > 0 => {
                Status::Failed
            }
            _ if self.progress.0.conflicts > 0 => Status::Lagging,
            "idle" if self.pending == 0 && self.reconciled_at.is_some() => Status::Healthy,
            "preparing" | "active" if self.reconciled_at.is_none() => Status::Backfilling,
            "reconciling" => Status::Reconciling,
            _ => Status::CatchingUp,
        };
        let confirmed =
            matches!(self.state.as_str(), "active" | "idle" | "catchup").then_some(self.confirmed);
        Link {
            id: self.id,
            task_id: self.id,
            source_bucket: self.source_bucket,
            settings: self.settings.0,
            revision: self.revision,
            status,
            progress: self.progress.0,
            pending: self.pending,
            queued_keys: self.queued_keys,
            confirmed,
            reconciled_at: self.reconciled_at,
            error: self.error,
        }
    }
}

#[async_trait]
pub(crate) trait RedundancyStore: Send + Sync {
    async fn preview(
        &self,
        principal: &Principal,
        bucket: Uuid,
        settings: &Settings,
    ) -> Result<Preview>;
    async fn create(
        &self,
        principal: &Principal,
        bucket: Uuid,
        settings: &Settings,
        connections: &[Connection],
    ) -> Result<Uuid>;
    async fn list(
        &self,
        principal: &Principal,
        bucket: Uuid,
        after: Option<Uuid>,
        limit: i64,
    ) -> Result<(Vec<Link>, Option<Uuid>)>;
    async fn get(&self, principal: &Principal, bucket: Uuid, id: Uuid) -> Result<Link>;
    async fn command(
        &self,
        principal: &Principal,
        bucket: Uuid,
        id: Uuid,
        command: &Command,
        connections: &[Connection],
    ) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn storage_domains_and_external_prefixes_use_opaque_object_keys() {
        let mut connection: crate::storage::external::Connection = serde_json::from_value(serde_json::json!({
            "endpoint_url":"https://STORE.example:443", "region":"one", "bucket":"primary", "prefix":"media"
        })).unwrap();
        let remote = Location::remote(&connection, "a/../b//")
            .unwrap()
            .resolve()
            .await
            .unwrap();
        assert_eq!(remote.path, "s3://store.example/primary/media/a/../b//");
        connection.virtual_hosted = true;
        connection.endpoint_url = "http://store.example".into();
        connection.allow_http = true;
        connection.prefix = "other".into();
        let alias = Location::remote(&connection, "")
            .unwrap()
            .resolve()
            .await
            .unwrap();
        assert_eq!(alias.domain, remote.domain);
        assert!(
            Placement {
                source: remote,
                target: alias
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn separate_sources_can_share_only_disjoint_external_prefixes() {
        let source = Uuid::new_v4();
        let ours = Uuid::new_v4();
        let settings: Settings = serde_json::from_value(serde_json::json!({
            "target_bucket":Uuid::new_v4(),"target_external":true,"target_prefix":"a/"
        }))
        .unwrap();
        let placement = Placement {
            source: Space {
                domain: vec![1],
                path: "file:/one/".into(),
            },
            target: Space {
                domain: vec![2],
                path: "s3://store/bucket/a/".into(),
            },
        };
        let mut other = Reservation {
            source_bucket: source,
            target_bucket: None,
            target_prefix: "b/".into(),
            source_space: "file:/two/".into(),
            target_space: "s3://store/bucket/b/".into(),
            target_domain: vec![2],
        };
        assert!(placement.check(ours, &settings, &other).is_ok());
        assert!(placement.check(source, &settings, &other).is_err());
        other.target_space = "s3://store/bucket/a/nested/".into();
        assert!(placement.check(ours, &settings, &other).is_err());
        other.target_bucket = Some(ours);
        assert!(placement.check(ours, &settings, &other).is_err());
    }

    #[test]
    fn external_redundancy_does_not_inherit_public_access_by_default() {
        let source = Uuid::new_v4();
        let mut settings: Settings =
            serde_json::from_value(serde_json::json!({"target_bucket":Uuid::new_v4()})).unwrap();
        assert_eq!(
            settings.replication(source).unwrap().permissions,
            Permissions::Inherit
        );
        assert_eq!(settings.max_pending_keys, 4096);
        settings.max_pending_keys = 0;
        assert!(settings.replication(source).is_err());
        settings.max_pending_keys = 1;
        settings.target_external = true;
        assert_eq!(
            settings.replication(source).unwrap().permissions,
            Permissions::Ignore
        );
        settings.mode = Mode::Synchronous;
        assert!(settings.replication(source).is_err());
        settings.target_external = false;
        assert!(settings.replication(source).is_ok());
        settings.target_external = true;
        settings.mode = Mode::Asynchronous;
        settings.permissions = Some(Permissions::Inherit);
        assert_eq!(
            settings.replication(source).unwrap().permissions,
            Permissions::Inherit
        );
        settings.propagate_deletes = true;
        assert!(settings.replication(source).is_err());
    }

    #[test]
    fn reconcile_prefix_is_bounded_and_cannot_scope_other_commands() {
        let mut command = Command {
            revision: 1,
            action: Action::Reconcile,
            prefix: Some("images/".into()),
        };
        assert!(command.validate().is_ok());
        command.action = Action::Remove;
        assert!(command.validate().is_err());
        command.prefix = None;
        assert!(command.validate().is_ok());
        command.revision = 0;
        assert!(command.validate().is_err());
    }
}
