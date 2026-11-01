//! SQLite transactions serialize writes before reading their decision state.

use super::queries;
use crate::app::Bucket;
use crate::authorization::Permit;
use crate::authorization::Principal;
use crate::authorization::{Action, DEFAULT_PROJECT};
use crate::content::codec::Chunk;
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
use crate::model::integrity::{Cursor, Detail, Finding, StorageSnapshot};
use crate::model::key::{ManagedKeyCreate, ManagedKeyView};
use crate::model::key_replacement::{
    self, KeyReplacementInput, KeyReplacementPreview, KeyReplacementResult, ReplacementRow,
};
use crate::model::maintenance::{FragmentCleanup, FragmentDeletion};
use crate::model::maintenance::{MaintenanceCompletion, MaintenanceExecution, MaintenancePreview};
use crate::model::metrics::ScopeQuery;
use crate::model::metrics::{AccessBatch, RuntimeHistory, RuntimePoint};
use crate::model::metrics::{CapacityPoint, StorageUsage, UsageSnapshot};
use crate::model::multipart::{
    MultipartCreate, PartStitch, SeedConsumption, Transfer, TransferFilter,
};
use crate::model::object::{
    CatalogCursor, CatalogFilter, MediaItem, ObjectChange, ObjectTransfer, Outcome, RECEIPT,
    StorageMigration,
};
use crate::model::packing::{PackMember, PackObject, PackOutput, PackSummary};
use crate::model::quota::{Limits, Quota, number, validate_scope};
use crate::model::settings::{ControlSettingsPatch, RuntimeSettingsRow};
use crate::model::storage::{StorageCreation, StorageEdit, StorageInitializer, StorageUpdate};
use crate::model::task::Execution;
use crate::model::{ChunkCreate, StreamCreate, StreamTail};
use crate::storage::{
    model::Record as BackendRecord,
    profile::{StorageProfile, validate as validate_storage_profile},
};
use crate::worker::integrity::Request as IntegrityRequest;
use anyhow::{Context, Result, ensure};
use chrono::Utc;
use queries::{QuotaAccount, QuotaReservation};
use serde_json::Value;
use serde_json::json;
use sqlx::{Connection, SqliteConnection};
use sqlx::{Sqlite, SqlitePool, Transaction};
use uuid::Uuid;
pub(super) use validate_replica_node as finish_replica_node;

#[cfg(test)]
#[path = "../../../../../tests/recovery/sqlite_cleanup.rs"]
mod cleanup_tests;

mod accounts;
pub(super) use accounts::*;
mod audit;
pub(super) use audit::*;
mod settings;
pub(super) use settings::*;
mod administration;
pub(super) use administration::*;
mod storages;
pub(super) use storages::*;
mod credentials;
pub(super) use credentials::*;
mod streams;
pub(super) use streams::*;
mod chunks;
pub(super) use chunks::*;
mod quota;
pub(super) use quota::*;
mod publication;
pub(super) use publication::*;
mod multipart;
pub(super) use multipart::*;
mod buckets;
pub(super) use buckets::*;
mod tasks;
pub(super) use tasks::*;
mod objects;
pub(super) use objects::*;
mod cache;
pub(super) use cache::*;
mod packing;
pub(super) use packing::*;
mod integrity;
pub(super) use integrity::*;
mod maintenance;
pub(super) use maintenance::*;
mod metrics;
pub(super) use metrics::*;
mod source_repair;
pub(super) use source_repair::*;
mod raw;
pub(super) use raw::*;
pub(super) mod external;
pub(super) mod redundancy;
pub(super) mod replication;
pub(super) mod replication_changes;
pub(super) mod replication_import;
pub(super) mod replication_remote;
pub(super) mod transfer;
