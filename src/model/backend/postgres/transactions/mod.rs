//! PostgreSQL transaction boundaries and lock ordering.

use super::{Backend, normalize_error, queries};
use crate::app::{Bucket, Extent};
use crate::authorization::{Action, DEFAULT_PROJECT, Permit, Principal};
use crate::cluster::NodeState;
use crate::config;
use crate::content::codec::Chunk;
use crate::content::codec::{self, Key};
use crate::content::pack::Pack;
use crate::model::Id;
use crate::model::Mapping;
use crate::model::account::{
    Preferences, RefreshedSession, SessionLogin, SessionRefresh, VerifiedAccount, csrf_token,
    derived_token,
};
use crate::model::administration::BucketGrant;
use crate::model::administration::{
    Membership, Project, ProjectInput, User, UserCreate, UserPatch,
};
use crate::model::bucket::{
    BucketImpact, BucketUpdate, CreateBucket, Domain, TransferInput, TransferProject, Website,
    fingerprint,
};
use crate::model::cache::{CacheBatch, CacheRelease, CacheStage};
use crate::model::credential::{
    self, Credential, CredentialInput, CredentialMaterial, CredentialSettings, RotateCredential,
    Token, TokenInput, TokenMaterial,
};
use crate::model::integrity::{Detail, Finding};
use crate::model::key::{ManagedKeyCreate, ManagedKeyView};
use crate::model::key_replacement::{
    self, KeyReplacementInput, KeyReplacementPreview, KeyReplacementResult, ReplacementRow,
};
use crate::model::maintenance::{FragmentCleanup, FragmentDeletion};
use crate::model::maintenance::{MaintenanceCompletion, MaintenanceExecution, MaintenancePreview};
use crate::model::metrics::{
    AccessBatch, CapacityPoint, RuntimeHistory, RuntimePoint, ScopeQuery, StorageUsage,
    UsageSnapshot,
};
use crate::model::multipart::{
    MultipartCreate, PartStitch, SeedConsumption, Transfer, TransferFilter,
};
use crate::model::multipart::{RemoteCommit, RemoteOutput, RemotePart, RemoteUpload};
use crate::model::object::{ObjectTransfer, Outcome, RECEIPT, StorageMigration};
use crate::model::packing::PackOutput;
use crate::model::packing::RewriteTask;
use crate::model::packing::{PackMember, PackObject, PackSummary};
use crate::model::quota::{Limits, Quota, number, validate_scope};
use crate::model::settings::{ControlSettingsPatch, RuntimeSettingsRow};
use crate::model::storage::{StorageCreation, StorageEdit, StorageInitializer, StorageUpdate};
use crate::model::task::Execution;
use crate::model::{
    ChunkCreate, ObjectChange, PublishStore, QuotaStore, StreamCreate, StreamStore, StreamTail,
    check_write_conditions,
};
use crate::model::{
    Error, node_chunk as chunk, node_read as read, node_write as write, session::Run,
};
use crate::storage::{
    model::Record as BackendRecord,
    profile::{StorageProfile, validate as validate_storage_profile},
};
use crate::worker::integrity::Request as IntegrityRequest;
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use queries::SavedStorage;
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use std::collections::{BTreeMap, HashMap, HashSet};
use uuid::Uuid;

pub(super) mod native;
pub(super) mod notifications;
pub(super) mod raw_cache;
#[path = "../work/mod.rs"]
pub(super) mod work;

mod accounts;
pub(super) use accounts::*;
mod chunks;
pub(super) use chunks::*;
mod quota;
pub(super) use quota::*;
mod publication;
pub(super) use publication::*;
mod streams;
pub(super) use streams::*;
mod leases;
pub(super) use leases::*;
mod audit;
pub(super) use audit::*;
mod administration;
pub(super) use administration::*;
mod credentials;
pub(super) use credentials::*;
mod buckets;
pub(super) use buckets::*;
mod settings;
pub(super) use settings::*;
mod keys;
pub(super) use keys::*;
mod multipart;
pub(super) use multipart::*;
mod physical;
use physical::*;
mod cache;
pub(super) use cache::*;
mod packing;
pub(super) use packing::*;
mod rewrite;
pub(super) use rewrite::*;
mod maintenance;
pub(super) use maintenance::*;
mod integrity;
pub(super) use integrity::*;
mod objects;
pub(super) use objects::*;
mod metrics;
pub(super) use metrics::*;
mod tasks;
pub(super) use tasks::*;
mod storages;
pub(super) use storages::*;
mod nodes;
pub(super) use nodes::*;
mod authorization;
pub(super) use authorization::*;
mod source_repair;
pub(super) use source_repair::*;
mod raw;
pub(super) use raw::*;
mod remote_uploads;
pub(super) use remote_uploads::*;
mod node_sessions;
pub(super) use node_sessions::*;
mod node_reads;
pub(super) use node_reads::*;
mod node_multipart;
pub(super) use node_multipart::*;
mod node_writes;
pub(super) use node_writes::*;
pub(super) mod external;
pub(super) mod redundancy;
pub(super) mod replication;
pub(super) mod replication_changes;
pub(super) mod replication_import;
pub(super) mod replication_remote;
pub(super) mod transfer;
