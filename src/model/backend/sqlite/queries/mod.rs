//! SQLite statements and native row decoding.

use crate::model::Id;
use sqlx::{
    Sqlite,
    query::{Query, QueryScalar},
    sqlite::SqliteArguments,
};
type Statement<'q> = Query<'q, Sqlite, SqliteArguments>;
type Scalar<'q, T> = QueryScalar<'q, Sqlite, T, SqliteArguments>;
type Rows<'q, T> = sqlx::query::QueryAs<'q, Sqlite, T, SqliteArguments>;
use super::{Backend, transactions};
use crate::app::Bucket;
use crate::app::{Extent, StoredStream};
use crate::authorization::Action;
use crate::authorization::Principal;
use crate::content::{codec::Chunk, pack::Pack};
use crate::model::Mapping;
use crate::model::account::{
    AccountStore, AuthSession, Preferences, Profile, RefreshedSession, Session, SessionLogin,
    SessionRefresh, SessionTimes, TokenIdentity, VerifiedAccount,
};
use crate::model::administration::{
    AdministrationStore, BucketGrant, Member, Membership, Project, ProjectInput, User, UserCreate,
    UserPatch,
};
use crate::model::bucket::{
    BucketImpact, BucketProject, BucketStorage, BucketStore, BucketUpdate, BucketView,
    CreateBucket, Domain, TransferInput, TransferProject, Website,
};
use crate::model::cache::{CacheBatch, CacheRelease, CacheStage, UploadCacheStore};
use crate::model::credential::{
    Credential, CredentialInput, CredentialMaterial, CredentialSettings, CredentialStore,
    RotateCredential, Token, TokenInput, TokenMaterial,
};
use crate::model::integrity::{Cursor, Detail, Finding, IntegrityStore, MappingRange};
use crate::model::key::{ManagedKeyCreate, ManagedKeyMaterial, ManagedKeyView};
use crate::model::key_replacement::{
    KeyReplacementInput, KeyReplacementPreview, KeyReplacementResult, KeyReplacementStorage,
    ReplacementRow,
};
use crate::model::maintenance::{
    FragmentCleanup, MaintenanceExecution, MaintenancePreview, MaintenanceStore, RecoveryFragment,
};
use crate::model::metrics::ScopeQuery;
use crate::model::metrics::StorageUsage;
use crate::model::metrics::{AccessBatch, RuntimePoint, WorkSummary};
use crate::model::multipart::{
    MultipartCreate, MultipartStore, PartStitch, ReceivedPart, SeedConsumption, Transfer,
    TransferFilter, Upload, UploadPart, WebUpload,
};
use crate::model::object::{
    CatalogCursor, CatalogFilter, MediaItem, ObjectChange, ObjectStore, ObjectTransfer, Outcome,
    StorageMigration,
};
use crate::model::packing::RewriteTask;
use crate::model::packing::{
    PackCandidate, PackMember, PackObject, PackOutput, PackSummary, PackingStore,
};
use crate::model::quota::Limits;
use crate::model::quota::Quota;
use crate::model::raw::{RawProcessing, RawRange, RawSource, RawStore, RawUpload};
use crate::model::runtime::{RuntimeState, RuntimeStore};
use crate::model::settings::{ControlSettingsPatch, RuntimeSettingsRow, RuntimeSettingsStore};
use crate::model::storage::{StorageCreation, StorageEdit, StorageRow, StorageView};
use crate::model::task::Execution;
use crate::model::task::{JobQuery, TaskControlStore};
use crate::model::task::{TaskClaim, TaskExecutionStore, TaskFinish, TaskRecord};
use crate::model::{
    ChunkCreate, ChunkStore, FragmentSource, PhysicalSource, PhysicalStore, ReadExtent,
    StreamCreate, StreamStore, StreamTail,
};
use crate::model::{ManagedKeyRecord, ManagedKeyStore};
use crate::model::{PublishStore, QuotaStore};
use crate::model::{StorageProfileStore, StorageRecord, StorageRecordStore};
use crate::storage::{model::Record as BackendRecord, profile::StorageProfile};
use crate::worker::integrity::Request as IntegrityRequest;
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::SqliteConnection;
use sqlx::{Row, sqlite::SqliteRow};
use uuid::Uuid;

mod runtime;
pub(super) use runtime::*;
mod accounts;
pub(super) use accounts::*;
mod settings;
pub(super) use settings::*;
mod authorization;
pub(super) use authorization::*;
mod storages;
pub(super) use storages::*;
mod administration;
pub(super) use administration::*;
mod credentials;
pub(super) use credentials::*;
mod audit;
mod streams;
pub(super) use streams::*;
mod chunks;
pub(super) use chunks::*;
mod physical;
pub(super) use physical::*;
mod quota;
pub(super) use quota::*;
mod publication;
pub(super) use publication::*;
mod multipart;
pub(super) use multipart::*;
mod buckets;
pub(super) use buckets::*;
mod objects;
pub(super) use objects::*;
mod keys;
pub(super) use keys::*;
mod tasks;
pub(super) use tasks::*;
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
pub(super) mod notices;
mod raw;
pub(super) mod raw_pages;
pub(super) use raw::*;
pub(super) mod external;
pub(super) mod image;
pub(super) mod redundancy;
pub(super) mod replica;
pub(super) mod replication;
pub(super) mod replication_changes;
pub(super) mod replication_import;
pub(super) mod replication_remote;
pub(super) mod tier;
pub(super) mod tier_reads;
pub(super) mod transfer;

pub(super) mod node_registry;

pub(super) mod work;

pub(super) mod work_sources;
