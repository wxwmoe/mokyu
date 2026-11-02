//! PostgreSQL statements, row decoding, and nontransactional operations.

use super::{Backend, connection, normalize_error, transactions};
use crate::app::{Bucket, Extent};
use crate::authorization::{Action, Permit, Principal};
use crate::cluster::NodeState;
use crate::content::codec::Chunk;
use crate::model::Id;
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
use crate::model::credential::{
    Credential, CredentialInput, CredentialMaterial, CredentialSettings, CredentialStore,
    RotateCredential, Token, TokenInput, TokenMaterial,
};
use crate::model::key::{ManagedKeyCreate, ManagedKeyMaterial, ManagedKeyView};
use crate::model::multipart::{
    MultipartCreate, MultipartStore, PartStitch, ReceivedPart, SeedConsumption, Transfer,
    TransferFilter, Upload, UploadPart, WebUpload,
};
use crate::model::multipart::{RemoteCommit, RemoteOutput, RemotePart, RemoteUpload};
use crate::model::settings::{ControlSettingsPatch, RuntimeSettingsRow};
use crate::model::task::{Execution, TaskFinish};
use crate::model::{
    ChunkCreate, ChunkStore, FragmentSource, ManagedKeyRecord, ManagedKeyStore, ReadExtent,
    RuntimeSettingsStore, StorageProfileStore, StorageRecord, StorageRecordStore, StreamCreate,
    TaskClaim, TaskControlStore, TaskExecutionStore, TaskRecord,
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use s3s::s3_error;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::{
    Postgres,
    postgres::PgArguments,
    query::{Query, QueryAs, QueryScalar},
};
use uuid::Uuid;
type Statement<'q> = Query<'q, Postgres, PgArguments>;
pub(super) type Rows<'q, T> = QueryAs<'q, Postgres, T, PgArguments>;
type Scalar<'q, T> = QueryScalar<'q, Postgres, T, PgArguments>;
use crate::app::StoredStream;
use crate::content::pack::Pack;
use crate::model::Mapping;
use crate::model::cache::{CacheBatch, CacheRelease, CacheStage, UploadCacheStore};
use crate::model::cluster::{ClusterStatus, ClusterStore, Leader, NodeView};
use crate::model::integrity::{Cursor, Detail, Finding, IntegrityStore, MappingRange};
use crate::model::key_replacement::{
    KeyReplacementInput, KeyReplacementPreview, KeyReplacementResult, KeyReplacementStorage,
    ReplacementRow,
};
use crate::model::maintenance::{FragmentCleanup, MaintenanceStore, RecoveryFragment};
use crate::model::maintenance::{MaintenanceExecution, MaintenancePreview};
use crate::model::metrics::{
    AccessBatch, MetricsStore, RuntimeHistory, RuntimePoint, ScopeQuery, StorageUsage,
    UsageSnapshot, WorkSummary,
};
use crate::model::node::{
    CONTACT_SECONDS, NodeCount, NodeFilter, NodePage, NodeReport, SavedReport, SettingCount,
};
use crate::model::object::{CatalogCursor, CatalogFilter, MediaItem};
use crate::model::object::{ObjectChange, ObjectStore, ObjectTransfer, Outcome, StorageMigration};
use crate::model::packing::RewriteTask;
use crate::model::packing::{PackCandidate, PackOutput, PackingStore};
use crate::model::packing::{PackMember, PackObject, PackSummary};
use crate::model::physical::{PhysicalSource, PhysicalStore, StagedUpload};
use crate::model::quota::Quota;
use crate::model::raw::{RawProcessing, RawRange, RawSource, RawStore, RawUpload};
use crate::model::runtime::{RuntimeState, RuntimeStore};
use crate::model::storage::{StorageCreation, StorageEdit, StorageRow, StorageView};
use crate::model::task::JobQuery;
use crate::storage::model::Record as BackendRecord;
use crate::storage::profile::StorageProfile;
use crate::worker::integrity::Request as IntegrityRequest;
use sqlx::{PgConnection, QueryBuilder, Row};

pub(super) mod native;
pub(super) mod notifications;
pub(super) mod pack_work;
pub(super) mod work;

mod streams;
pub(super) use streams::*;
mod runtime;
pub(super) use runtime::*;
mod replica_reads;
pub(super) use replica_reads::*;
mod chunks;
pub(super) use chunks::*;
mod quota;
pub(super) use quota::*;
mod tasks;
pub(super) use tasks::*;
mod permits;
pub(super) use permits::*;
mod storages;
pub(super) use storages::*;
mod keys;
pub(super) use keys::*;
mod leases;
pub(super) use leases::*;
mod authorization;
pub(super) use authorization::*;
mod audit;
pub(super) use audit::*;
mod accounts;
pub(super) use accounts::*;
mod administration;
pub(super) use administration::*;
mod credentials;
pub(super) use credentials::*;
mod buckets;
pub(super) use buckets::*;
mod multipart;
pub(super) use multipart::*;
mod cache;
pub(super) use cache::*;
mod physical;
pub(super) use physical::*;
mod packing;
pub(super) use packing::*;
mod maintenance;
pub(super) use maintenance::*;
mod integrity;
pub(super) use integrity::*;
mod objects;
pub(super) use objects::*;
mod metrics;
pub(super) use metrics::*;
mod nodes;
pub(super) use nodes::*;
mod rows;
pub(super) use rows::*;
mod source_repair;
pub(super) use source_repair::*;
mod raw;
pub(super) mod raw_pages;
pub(super) use raw::*;
mod node_sessions;
pub(super) use node_sessions::*;
mod node_reads;
pub(super) use node_reads::*;
mod node_writes;
pub(super) use node_writes::*;
pub(super) mod external;
pub(super) mod image;
pub(super) mod redundancy;
pub(super) mod refresh;
pub(super) mod replica;
pub(super) mod replication;
pub(super) mod replication_changes;
pub(super) mod replication_import;
pub(super) mod replication_remote;
pub(super) mod sources;
pub(super) mod tier;
pub(super) mod tier_reads;
pub(super) mod transfer;

pub(super) mod work_sources;
