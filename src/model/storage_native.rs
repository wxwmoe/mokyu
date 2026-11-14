use super::{
    Error,
    storage_payload::{Intent, Payload},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) mod access;
mod work;
pub(crate) use work::deletion;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", content = "version", rename_all = "snake_case")]
pub(crate) enum Condition {
    Create,
    Overwrite,
    Match(Uuid),
    Rebuild(Uuid),
}

impl Condition {
    pub fn check(&self, current: Option<Uuid>) -> Result<()> {
        match self {
            Self::Create => ensure!(current.is_none(), Error::ObjectAlreadyExists),
            Self::Match(id) | Self::Rebuild(id) => ensure!(
                !id.is_nil() && current == Some(*id),
                Error::StorageObjectChanged
            ),
            Self::Overwrite => (),
        }
        Ok(())
    }
}

pub(crate) struct Write {
    pub recovery: Option<RecoveryIdentity>,
    pub region: Option<String>,
    pub path: String,
    pub owner: Uuid,
    pub condition: Condition,
    pub intent: Intent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RecoveryIdentity {
    pub payload: Uuid,
    pub revision: u64,
}

impl Write {
    pub fn validate(&self) -> Result<()> {
        if let Some(region) = &self.region {
            crate::config::storage::label(region, "region")?;
        }
        path(&self.path)?;
        self.intent.validate()?;
        if let Some(identity) = self.recovery {
            ensure!(
                self.intent.area == crate::content::fragment::Area::Recovery
                    && identity.payload == self.intent.payload
                    && identity.revision == self.intent.revision,
                "recovery frame identity differs from its write"
            );
        }
        ensure!(!self.owner.is_nil(), "native write owner is missing");
        Ok(())
    }
}

pub(crate) fn path(value: &str) -> Result<[u8; 32]> {
    ensure!(
        !value.is_empty()
            && value.len() <= 1024
            && !value.as_bytes().contains(&0)
            && object_store::path::Path::parse(value)?.as_ref() == value,
        "invalid native storage path"
    );
    Ok(*blake3::hash(value.as_bytes()).as_bytes())
}

pub(crate) struct Object {
    pub path: String,
    pub payload: Payload,
    pub modified: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Fill {
    pub storage: Uuid,
    pub operation: Uuid,
    pub sequence: u32,
    #[serde(default)]
    pub repair: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup: Option<Uuid>,
    #[serde(default)]
    pub shard: u16,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Metadata {
    pub path: String,
    pub version: Uuid,
    pub size: u64,
    pub modified: i64,
}

impl From<Object> for Metadata {
    fn from(value: Object) -> Self {
        Self {
            path: value.path,
            version: value.payload.intent.operation,
            size: value.payload.intent.total,
            modified: value.modified,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadLease {
    pub operation: Uuid,
    pub modified: i64,
    tag: [u8; 32],
}

impl ReadLease {
    pub fn issue(
        key: &crate::content::codec::Key,
        namespace: Uuid,
        path: &str,
        object: &Object,
    ) -> Result<Self> {
        let mut lease = Self {
            operation: object.payload.intent.operation,
            modified: object.modified,
            tag: [0; 32],
        };
        lease.tag = lease.mac(key, namespace, path)?;
        Ok(lease)
    }

    fn mac(
        &self,
        key: &crate::content::codec::Key,
        namespace: Uuid,
        name: &str,
    ) -> Result<[u8; 32]> {
        let key = blake3::derive_key("mokyu/storage/read/v1", &key.material);
        let mut mac = blake3::Hasher::new_keyed(&key);
        mac.update(namespace.as_bytes());
        mac.update(&path(name)?);
        mac.update(self.operation.as_bytes());
        mac.update(&self.modified.to_be_bytes());
        Ok(*mac.finalize().as_bytes())
    }

    pub fn verify(
        &self,
        key: &crate::content::codec::Key,
        namespace: Uuid,
        path: &str,
    ) -> Result<()> {
        aws_lc_rs::constant_time::verify_slices_are_equal(
            &self.tag,
            &self.mac(key, namespace, path)?,
        )
        .map_err(|_| anyhow::anyhow!("invalid native read lease"))
    }
}

#[async_trait::async_trait]
pub(crate) trait NativeObjects: Send + Sync {
    async fn next_fill(&self, operation: Uuid) -> Result<Option<Fill>>;
    async fn authorize_fill(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: super::session::Run,
        work: Uuid,
    ) -> Result<(Fill, i64)>;
    async fn authorize_upload(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: super::session::Run,
        request: &access::Upload,
    ) -> Result<access::UploadAccess>;
    async fn authorize_read(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: super::session::Run,
        request: &access::Read,
    ) -> Result<i64>;
    async fn maintain(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: super::session::Run,
        request: &access::Maintenance,
    ) -> Result<Vec<Metadata>>;
    async fn accept_receipts(
        &self,
        key: &crate::content::codec::Key,
        operation: Uuid,
        receipts: &[super::storage_transfer::Completion],
    ) -> Result<()>;
    async fn reserve_object(&self, input: &Write, until: i64) -> Result<Object>;
    async fn reserve_targets(&self, operation: Uuid) -> Result<Vec<Uuid>>;
    async fn find_write(
        &self,
        namespace: Uuid,
        path: &str,
        owner: Uuid,
    ) -> Result<Option<(Object, Condition)>>;
    async fn read_object(
        &self,
        namespace: Uuid,
        path: &str,
        owner: Option<Uuid>,
        until: i64,
    ) -> Result<Object>;
    async fn resume_object(
        &self,
        namespace: Uuid,
        path: &str,
        operation: Uuid,
        until: i64,
    ) -> Result<Object>;
    async fn publish_object(&self, namespace: Uuid, payload: Uuid, owner: Uuid) -> Result<()>;
    async fn remove_object(
        &self,
        namespace: Uuid,
        path: &str,
        expected: Option<Uuid>,
    ) -> Result<()>;
    async fn list_objects(&self, namespace: Uuid, after: Option<&str>) -> Result<Vec<Metadata>>;
}
