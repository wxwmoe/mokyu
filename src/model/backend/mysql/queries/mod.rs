//! MySQL statements and native row decoding.

use super::{Backend, transactions};
use crate::authorization::{Action, Principal};
use crate::model::Id;
use crate::model::account::{
    AccountStore, AuthSession, Preferences, Profile, RefreshedSession, Session, SessionLogin,
    SessionRefresh, SessionTimes, TokenIdentity, VerifiedAccount,
};
use crate::model::administration::{
    AdministrationStore, BucketGrant, Member, Membership, Project, ProjectInput, User, UserCreate,
    UserPatch,
};
use crate::model::credential::{
    Credential, CredentialInput, CredentialMaterial, CredentialSettings, CredentialStore,
    RotateCredential, Token, TokenInput, TokenMaterial,
};
use crate::model::settings::{ControlSettingsPatch, RuntimeSettingsRow, RuntimeSettingsStore};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{
    MySql, Row,
    mysql::MySqlArguments,
    query::{Query, QueryScalar},
};
use uuid::Uuid;
type Statement<'q> = Query<'q, MySql, MySqlArguments>;
type Scalar<'q, T> = QueryScalar<'q, MySql, T, MySqlArguments>;
type Rows<'q, T> = sqlx::query::QueryAs<'q, MySql, T, MySqlArguments>;
use crate::app::Bucket;
use crate::app::{Extent, StoredStream};
use crate::content::{codec::Chunk, pack::Pack};
use crate::model::Mapping;
use crate::model::PublishStore;
use crate::model::bucket::{
    BucketImpact, BucketProject, BucketStorage, BucketStore, BucketUpdate, BucketView,
    CreateBucket, Domain, TransferInput, TransferProject, Website,
};
use crate::model::cache::{CacheBatch, CacheRelease, CacheStage, UploadCacheStore};
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
use crate::model::quota::{Limits, Quota, QuotaStore};
use crate::model::raw::{RawProcessing, RawRange, RawSource, RawStore, RawUpload};
use crate::model::runtime::{RuntimeState, RuntimeStore};
use crate::model::storage::{StorageCreation, StorageEdit, StorageRow, StorageView};
use crate::model::task::Execution;
use crate::model::task::{
    JobQuery, TaskClaim, TaskControlStore, TaskExecutionStore, TaskFinish, TaskRecord,
};
use crate::model::{
    ChunkCreate, ChunkStore, FragmentSource, PhysicalSource, PhysicalStore, ReadExtent,
    StreamCreate, StreamStore, StreamTail,
};
use crate::model::{ManagedKeyRecord, ManagedKeyStore};
use crate::model::{StorageProfileStore, StorageRecord, StorageRecordStore};
use crate::storage::{model::Record as BackendRecord, profile::StorageProfile};
use crate::worker::integrity::Request as IntegrityRequest;

mod runtime;
pub(super) use runtime::*;
mod metrics;
pub(super) use metrics::*;
mod maintenance;
pub(super) use maintenance::*;
mod integrity;
pub(super) use integrity::*;
mod packing;
pub(super) use packing::*;
mod cache;
pub(super) use cache::*;
mod tasks;
pub(super) use tasks::*;
mod objects;
pub(super) use objects::*;
mod buckets;
pub(super) use buckets::*;
mod multipart;
pub(super) use multipart::*;
mod quota;
pub(super) use quota::*;
mod publication;
pub(super) use publication::*;
mod streams;
pub(super) use streams::*;
mod chunks;
pub(super) use chunks::*;
mod physical;
use physical::*;
mod keys;
pub(super) use keys::*;
mod audit;
pub(super) use audit::*;
mod credentials;
pub(super) use credentials::*;
mod authorization;
pub(super) use authorization::*;
mod accounts;
pub(super) use accounts::*;
mod administration;
mod settings;
mod storages;
pub(super) use storages::*;
mod source_repair;
pub(super) use source_repair::*;
mod raw;
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
