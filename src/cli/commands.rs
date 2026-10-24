use crate::model::Id;
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Subcommand, Serialize, Deserialize)]
pub enum Command {
    Init {
        username: String,
        #[arg(long = "stdin")]
        password_stdin: bool,
        #[arg(skip)]
        password: Option<String>,
        #[arg(long)]
        require_change: bool,
    },
    Status {
        #[arg(long)]
        ready: bool,
    },
    #[command(subcommand)]
    Bucket(Buckets),
    #[command(name = "cred")]
    #[command(subcommand)]
    Credential(Credentials),
    #[command(subcommand)]
    Token(Tokens),
    Audit {
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        actor: Option<String>,
        #[arg(long)]
        action: Option<String>,
    },
    #[command(subcommand)]
    Domain(Domains),
    #[command(subcommand)]
    User(Users),
    #[command(subcommand)]
    Project(Projects),
    #[command(subcommand)]
    Quota(Quotas),
    #[command(subcommand)]
    Gc(Gc),
    #[command(subcommand)]
    Cleanup(Cleanup),
    #[command(subcommand)]
    Maintenance(Maintenance),
    #[command(subcommand)]
    Task(Tasks),
    #[command(name = "storage")]
    #[command(subcommand)]
    Storages(Storages),
    #[command(subcommand)]
    Settings(Settings),
    #[command(name = "key")]
    #[command(subcommand)]
    Keys(Keys),
    #[command(subcommand)]
    Cluster(Cluster),
    #[command(subcommand)]
    Setup(Setup),
    #[command(subcommand)]
    Integrity(Integrity),
    #[command(subcommand)]
    Pack(Packs),
    #[command(subcommand)]
    Cache(Cache),
}
#[derive(Subcommand)]
pub enum Cache {
    Status,
    Flush {
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value = "5m", requires = "wait")]
        timeout: String,
    },
}

impl Serialize for Cache {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(match self {
            Self::Status => "Status",
            Self::Flush { .. } => "Flush",
        })
    }
}

impl<'de> Deserialize<'de> for Cache {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match String::deserialize(deserializer)?.as_str() {
            "Status" => Ok(Self::Status),
            "Flush" => Ok(Self::Flush {
                wait: false,
                timeout: "5m".into(),
            }),
            value => Err(serde::de::Error::unknown_variant(
                value,
                &["Status", "Flush"],
            )),
        }
    }
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Packs {
    Status,
    Run {
        #[arg(long = "mode", default_value = "pack")]
        mode: String,
    },
    Unpack {
        id: Option<Id>,
        #[arg(long, conflicts_with = "id")]
        all: bool,
        #[arg(long)]
        execute: bool,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Integrity {
    Quarantines {
        #[arg(long)]
        storage: Uuid,
        #[arg(long, value_enum)]
        source: crate::model::source::SourceType,
        #[arg(long, default_value = "")]
        after: String,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    Quarantine {
        #[arg(long)]
        storage: Uuid,
        #[arg(long, value_enum)]
        source: crate::model::source::SourceType,
        id: String,
        #[arg(long)]
        confirm: Option<String>,
    },
    Recheck {
        #[arg(long)]
        storage: Uuid,
        #[arg(long, value_enum)]
        source: crate::model::source::SourceType,
        id: String,
        #[arg(long)]
        confirm: Option<String>,
    },
    Check {
        #[arg(long, value_enum, default_value = "metadata")]
        mode: crate::worker::integrity::Mode,
        #[arg(long)]
        bucket: Option<String>,
        #[arg(long, requires = "bucket")]
        key: Option<String>,
    },
    Issues {
        id: Uuid,
        #[arg(long, default_value = "0")]
        after: String,
        #[arg(long, default_value_t = 100)]
        limit: i64,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Buckets {
    List,
    Create {
        name: String,
        #[arg(long)]
        project: Option<Uuid>,
        #[arg(long)]
        storage: Option<Uuid>,
    },
    Delete {
        name: String,
    },
    Cors {
        name: String,
        file: PathBuf,
        #[arg(skip)]
        document: Option<Value>,
    },
    Purge {
        name: String,
        #[arg(long)]
        execute: bool,
        #[arg(skip)]
        confirm_bucket: Option<Uuid>,
        #[arg(skip)]
        confirmed: bool,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Gc {
    Status,
    MarkUnreferenced,
    Run,
    Pause,
    Resume,
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Cleanup {
    Status,
    Run,
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Maintenance {
    Enable,
    Disable,
    Status,
    Pause {
        task: String,
    },
    Resume {
        task: String,
    },
    Run {
        task: String,
    },
    PackCreation {
        #[arg(value_parser=["stop","resume"])]
        action: String,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Tasks {
    List,
    Show { id: Uuid },
    Pause { id: Uuid },
    Resume { id: Uuid },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Storages {
    List,
    Create {
        name: String,
        #[arg(value_parser = ["local", "s3"])]
        r#type: String,
        #[arg(long = "init")]
        initialize: bool,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(short = 'e', long)]
        endpoint: Option<String>,
        #[arg(short = 'b', long)]
        bucket: Option<String>,
        #[arg(short = 'r', long)]
        region: Option<String>,
        #[arg(short = 'p', long, default_value = "")]
        prefix: String,
        #[arg(short = 'a', long = "access")]
        access_key: Option<String>,
        #[arg(short = 's', long = "secret")]
        secret_key: Option<String>,
        #[arg(long = "read")]
        read_concurrency: Option<i32>,
        #[arg(long = "upload")]
        upload_concurrency: Option<i32>,
        #[arg(long = "control")]
        control_concurrency: Option<i32>,
    },
    Update {
        name: String,
        #[arg(long, value_parser = ["local", "s3"])]
        r#type: Option<String>,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        key: Option<String>,
        #[arg(short = 'e', long)]
        endpoint: Option<String>,
        #[arg(short = 'b', long)]
        bucket: Option<String>,
        #[arg(short = 'r', long)]
        region: Option<String>,
        #[arg(short = 'p', long)]
        prefix: Option<String>,
        #[arg(short = 'a', long = "access")]
        access_key: Option<String>,
        #[arg(short = 's', long = "secret")]
        secret_key: Option<String>,
        #[arg(long)]
        enabled: Option<bool>,
        #[arg(long = "read")]
        read_concurrency: Option<i32>,
        #[arg(long = "clear-read")]
        clear_read_concurrency: bool,
        #[arg(long = "upload")]
        upload_concurrency: Option<i32>,
        #[arg(long = "clear-upload")]
        clear_upload_concurrency: bool,
        #[arg(long = "control")]
        control_concurrency: Option<i32>,
        #[arg(long = "clear-control")]
        clear_control_concurrency: bool,
    },
    Check {
        name: String,
    },
    Show {
        name: String,
    },
    Key {
        #[command(subcommand)]
        command: StorageKey,
    },
    #[command(name = "profile")]
    Profile {
        #[command(subcommand)]
        command: StorageProfiles,
    },
    #[command(name = "rewrite")]
    Rewrite {
        storage: String,
        #[command(subcommand)]
        command: RewriteCommand,
    },
    Sweep {
        #[arg(long)]
        storage: Option<Uuid>,
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        preview: Option<Uuid>,
        #[arg(skip)]
        confirm_prefix: Option<String>,
        #[arg(long = "age", default_value = "48h")]
        older_than: String,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum StorageKey {
    Set { storage: String, key: String },
    Clear { storage: String },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum StorageProfiles {
    Show {
        name: String,
    },
    /// List applicable templates and current node constraints without saving.
    Presets {
        name: String,
    },
    /// Preview a preset; use --draft to emit JSON for an explicit profile set.
    Preset {
        name: String,
        #[arg(value_parser = ["reliable", "cached", "throughput"])]
        preset: String,
        #[arg(long)]
        draft: bool,
    },
    /// Validate a complete draft and show changes without saving.
    Preview {
        name: String,
        file: PathBuf,
        #[arg(skip)]
        document: Option<Value>,
    },
    Set {
        name: String,
        file: PathBuf,
        #[arg(skip)]
        document: Option<Value>,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum RewriteCommand {
    Preview {
        #[arg(long = "key")]
        target_key: Option<String>,
    },
    Start {
        #[arg(long = "key")]
        target_key: Option<String>,
        #[arg(long)]
        confirm: String,
    },
    Status,
    Pause,
    Resume,
    Retry,
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Settings {
    Get {
        section: String,
    },
    Set {
        section: String,
        file: PathBuf,
        #[arg(long = "rev")]
        revision: Option<i64>,
        #[arg(skip)]
        document: Option<Value>,
    },
    Reset {
        section: String,
    },
    #[command(subcommand)]
    #[command(name = "web")]
    Control(ControlSettings),
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum ControlSettings {
    Show,
    Set {
        #[arg(long)]
        origin: Option<String>,
        #[arg(long)]
        secure_cookie: Option<bool>,
        #[arg(long)]
        session_lifetime: Option<String>,
        #[arg(long)]
        gravatar_base_url: Option<String>,
        #[arg(long = "rev")]
        revision: Option<i64>,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Keys {
    List,
    ReplacePreview {
        source: Uuid,
        #[arg(long, conflicts_with = "name")]
        target: Option<Uuid>,
        #[arg(long)]
        name: Option<String>,
    },
    Replacement {
        id: Uuid,
    },
    Replace {
        id: Uuid,
    },
    Create {
        name: String,
        #[arg(value_parser = ["access", "signing", "block"])]
        purpose: String,
        #[arg(short = 'a', long, value_parser = ["aes-256-gcm", "chacha20-poly1305"])]
        algorithm: Option<String>,
    },
    Revoke {
        id: Uuid,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Cluster {
    Status,
    Create {
        name: String,
        #[arg(value_parser = ["data", "worker"])]
        role: String,
    },
    Rotate {
        name: String,
        #[arg(long)]
        confirm: bool,
    },
    Mode {
        #[arg(value_parser = ["single", "cluster"])]
        mode: String,
    },
    Nodes {
        #[arg(long)]
        name: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: i64,
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        role: Option<String>,
        #[arg(long)]
        contact: Option<String>,
        #[arg(long)]
        work: Option<String>,
        #[arg(long)]
        configuration: Option<String>,
        #[arg(long)]
        section: Option<String>,
    },
    Release {
        name: String,
        #[arg(long)]
        confirm: bool,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Setup {
    Token,
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Credentials {
    List {
        #[arg(long)]
        after: Option<String>,
    },
    Create {
        bucket: String,
        #[arg(long)]
        read_only: bool,
        #[arg(long)]
        label: Option<String>,
        #[arg(long = "ttl")]
        expires_in: Option<String>,
    },
    Grant {
        access_key: String,
        bucket: String,
        #[arg(long)]
        read_only: bool,
    },
    Revoke {
        access_key: String,
        bucket: String,
    },
    Disable {
        access_key: String,
    },
    Enable {
        access_key: String,
    },
    Delete {
        access_key: String,
    },
    Show {
        access_key: String,
    },
    Rotate {
        access_key: String,
        #[arg(long, default_value = "24h")]
        overlap: String,
        #[arg(long = "ttl")]
        expires_in: Option<String>,
    },
    Update {
        access_key: String,
        file: PathBuf,
        #[arg(skip)]
        document: Option<Value>,
    },
    Permissions {
        access_key: String,
        file: PathBuf,
        #[arg(skip)]
        document: Option<Value>,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Tokens {
    List {
        username: String,
        #[arg(long)]
        after: Option<Uuid>,
    },
    Create {
        username: String,
        file: PathBuf,
        #[arg(skip)]
        document: Option<Value>,
    },
    Update {
        id: Uuid,
        file: PathBuf,
        #[arg(skip)]
        document: Option<Value>,
    },
    Revoke {
        id: Uuid,
    },
}
impl Command {
    pub fn allows_bootstrap(&self) -> bool {
        matches!(
            self,
            Self::Init { .. } | Self::Status { .. } | Self::Cluster(Cluster::Release { .. })
        )
    }

    pub fn audit_action(&self) -> Option<&'static str> {
        match self {
            Self::Init { .. }
            | Self::Status { .. }
            | Self::Audit { .. }
            | Self::Bucket(Buckets::List)
            | Self::Credential(Credentials::List { .. } | Credentials::Show { .. })
            | Self::Token(Tokens::List { .. })
            | Self::Domain(Domains::List)
            | Self::User(Users::List)
            | Self::Project(Projects::List)
            | Self::Quota(Quotas::Show { .. })
            | Self::Gc(Gc::Status)
            | Self::Cleanup(Cleanup::Status)
            | Self::Maintenance(Maintenance::Status)
            | Self::Task(Tasks::List | Tasks::Show { .. })
            | Self::Integrity(Integrity::Issues { .. } | Integrity::Quarantines { .. })
            | Self::Pack(Packs::Status)
            | Self::Cache(Cache::Status)
            | Self::Storages(Storages::List | Storages::Show { .. })
            | Self::Storages(Storages::Profile {
                command:
                    StorageProfiles::Show { .. }
                    | StorageProfiles::Presets { .. }
                    | StorageProfiles::Preset { .. }
                    | StorageProfiles::Preview { .. },
            })
            | Self::Settings(Settings::Control(ControlSettings::Show) | Settings::Get { .. })
            | Self::Keys(Keys::List | Keys::Replacement { .. })
            | Self::Setup(Setup::Token)
            | Self::Cluster(Cluster::Status | Cluster::Nodes { .. }) => None,
            Self::Bucket(_) => Some("bucket.change"),
            Self::Credential(_) => Some("credential.change"),
            Self::Token(_) => Some("token.change"),
            Self::Domain(_) => Some("domain.change"),
            Self::User(_) => Some("user.change"),
            Self::Project(_) => Some("project.change"),
            Self::Quota(_) => Some("quota.update"),
            Self::Gc(_) => Some("gc.change"),
            Self::Cleanup(_) => Some("cleanup.run"),
            Self::Maintenance(_) => Some("maintenance.change"),
            Self::Task(_) => Some("task.change"),
            Self::Storages(_) => Some("storage.change"),
            Self::Settings(_) => Some("runtime.settings"),
            Self::Keys(_) => Some("managed_key.change"),
            Self::Cluster(_) => Some("cluster.change"),
            Self::Integrity(Integrity::Quarantine { .. }) => Some("source.quarantine"),
            Self::Integrity(Integrity::Recheck { .. }) => Some("source.recheck"),
            Self::Integrity(_) => Some("integrity.check"),
            Self::Pack(_) => Some("pack.change"),
            Self::Cache(_) => Some("cache.flush"),
        }
    }
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Domains {
    List,
    Set { host: String, bucket: String },
    Delete { host: String },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Quotas {
    #[command(name = "get")]
    Show {
        #[arg(value_parser=["project","bucket"])]
        scope: String,
        id: Uuid,
    },
    Set {
        #[arg(value_parser=["project","bucket"])]
        scope: String,
        id: Uuid,
        file: PathBuf,
        #[arg(skip)]
        document: Option<Value>,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Projects {
    List,
    Create {
        name: String,
    },
    Update {
        id: Uuid,
        name: String,
        #[arg(long, default_value = "")]
        description: String,
        #[arg(long)]
        allow_bucket_create: bool,
    },
    Delete {
        id: Uuid,
    },
    Mode {
        enabled: bool,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Users {
    List,
    Create {
        username: String,
        #[arg(long, default_value="admin", value_parser=["admin","member"])]
        role: String,
        #[arg(long)]
        require_change: bool,
        #[arg(long = "stdin")]
        password_stdin: bool,
        #[arg(skip)]
        password: Option<String>,
    },
    Password {
        username: String,
        #[arg(long)]
        require_change: bool,
        #[arg(long = "stdin")]
        password_stdin: bool,
        #[arg(skip)]
        password: Option<String>,
    },
    Disable {
        username: String,
    },
    Enable {
        username: String,
    },
    Role {
        username: String,
        #[arg(value_parser=["admin","member"])]
        role: String,
    },
    Membership {
        username: String,
        project: Uuid,
        file: PathBuf,
        #[arg(skip)]
        document: Option<Value>,
    },
    Leave {
        username: String,
        project: Uuid,
    },
    Delete {
        username: String,
    },
}
