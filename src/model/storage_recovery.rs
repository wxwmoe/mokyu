use super::{storage_cluster::Manifest, storage_payload::Intent};
use crate::{content::codec::Key, content::fragment::Area};
use anyhow::{Result, ensure};
use aws_lc_rs::signature::{self, Ed25519KeyPair, KeyPair};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) mod verify;

pub(crate) const PROOF_LIMIT: usize = 128 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct EvidenceEntry {
    pub name: String,
    pub pending: bool,
    pub proof: Option<Proof>,
    pub manifest: Option<Manifest>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct EvidencePage {
    pub number: u64,
    pub entries: Vec<EvidenceEntry>,
    pub head: Option<Link>,
    pub complete: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct InventoryEntry {
    pub prefix: u16,
    pub name: String,
    pub pending: bool,
    pub header: Option<crate::content::fragment::Header>,
    pub receipt: Option<super::storage_payload::Receipt>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct InventoryPage {
    pub number: u64,
    pub restart_prefix: u16,
    pub entries: Vec<InventoryEntry>,
    pub complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Physical {
    Payload {
        sequence: u64,
        intent: Intent,
        path: String,
    },
    Fragment {
        sequence: u64,
        receipt: super::storage_payload::Receipt,
    },
    State {
        sequence: u64,
        operation: Uuid,
        published: bool,
    },
}

impl Physical {
    pub fn validate(&self, namespace: Uuid) -> Result<()> {
        let sequence = match self {
            Self::Payload {
                sequence,
                intent,
                path,
            } => {
                intent.validate()?;
                super::storage_native::path(path)?;
                ensure!(intent.namespace == namespace, "foreign physical payload");
                *sequence
            }
            Self::Fragment { sequence, receipt } => {
                receipt.validate()?;
                ensure!(
                    receipt.header.namespace == namespace,
                    "foreign physical fragment"
                );
                *sequence
            }
            Self::State {
                sequence,
                operation,
                ..
            } => {
                ensure!(!operation.is_nil(), "invalid physical operation");
                *sequence
            }
        };
        ensure!(
            sequence > 0 && sequence <= i64::MAX as u64,
            "invalid physical commit sequence"
        );
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum CatalogEntry {
    Manifest(Manifest),
    Proof(Proof),
    Fragment(super::storage_payload::Receipt),
    BackupLocation {
        fragment: Uuid,
        media: Uuid,
        namespace: Uuid,
        cluster: Uuid,
    },
    Physical(Uuid, Physical),
    Business {
        payload: Uuid,
        position: u64,
        record: crate::storage::journal::batch::Record,
    },
    Snapshot {
        payload: Uuid,
        position: u64,
        row: crate::storage::journal::snapshot::Row,
    },
    SnapshotManifest {
        payload: Uuid,
        position: u64,
        manifest: crate::storage::journal::snapshot::Manifest,
    },
    SnapshotIndex {
        payload: Uuid,
        position: u64,
        index: crate::storage::journal::snapshot::Index,
    },
    History {
        payload: Uuid,
        position: u64,
        record: crate::storage::journal::history::Record,
    },
}

impl CatalogEntry {
    pub fn identity(&self) -> (i16, Uuid, Uuid, u64) {
        match self {
            Self::Manifest(value) => (0, value.cluster_id, value.record_id, value.revision as u64),
            Self::Proof(value) => (
                1,
                value.publication.namespace,
                value.publication.id,
                value.publication.revision,
            ),
            Self::Fragment(value) => (2, value.header.operation, value.header.fragment, 0),
            Self::BackupLocation {
                fragment, media, ..
            } => {
                let mut hash =
                    blake3::Hasher::new_derive_key("mokyu/storage/recovery-backup-location/v1");
                hash.update(fragment.as_bytes());
                hash.update(media.as_bytes());
                (
                    2,
                    *fragment,
                    Uuid::from_bytes(hash.finalize().as_bytes()[..16].try_into().unwrap()),
                    1,
                )
            }
            Self::Physical(epoch, value) => {
                let (category, owner, key, sequence) = match value {
                    Physical::Payload {
                        sequence, intent, ..
                    } => (3, intent.operation, intent.operation, *sequence),
                    Physical::Fragment { sequence, receipt } => (
                        4,
                        receipt.header.operation,
                        receipt.header.fragment,
                        *sequence,
                    ),
                    Physical::State {
                        sequence,
                        operation,
                        ..
                    } => (5, *operation, *operation, *sequence),
                };
                let mut digest = blake3::Hasher::new_derive_key("mokyu/storage/recovery-entry/v1");
                digest.update(epoch.as_bytes());
                digest.update(key.as_bytes());
                (
                    category,
                    owner,
                    Uuid::from_bytes(digest.finalize().as_bytes()[..16].try_into().unwrap()),
                    sequence,
                )
            }
            Self::Business {
                payload, position, ..
            }
            | Self::Snapshot {
                payload, position, ..
            }
            | Self::SnapshotManifest {
                payload, position, ..
            }
            | Self::SnapshotIndex {
                payload, position, ..
            }
            | Self::History {
                payload, position, ..
            } => (6, *payload, *payload, *position),
        }
    }

    pub fn validate(&self, namespace: Uuid, cluster: Uuid) -> Result<()> {
        match self {
            Self::Manifest(value) => {
                value.validate()?;
                ensure!(value.cluster_id == cluster, "foreign recovery manifest");
            }
            Self::Proof(value) => {
                value.publication.validate()?;
                ensure!(
                    value.publication.cluster == cluster
                        && value.publication.namespace == namespace
                        && value.signature.len() == 64,
                    "foreign recovery proof"
                );
            }
            Self::BackupLocation {
                fragment,
                media,
                namespace: source,
                cluster: owner,
            } => {
                ensure!(
                    !fragment.is_nil()
                        && !media.is_nil()
                        && *source == namespace
                        && *owner == cluster,
                    "foreign backup recovery location"
                );
            }
            Self::Fragment(value) => {
                value.validate()?;
                ensure!(
                    value.header.cluster == cluster
                        && value.header.namespace == namespace
                        && value.header.area == Area::Recovery,
                    "foreign recovery fragment"
                );
            }
            Self::Physical(epoch, value) => {
                ensure!(!epoch.is_nil(), "missing physical journal epoch");
                value.validate(namespace)?;
                let owner = match value {
                    Physical::Payload { intent, .. } => intent.cluster,
                    Physical::Fragment { receipt, .. } => receipt.header.cluster,
                    Physical::State { .. } => cluster,
                };
                ensure!(owner == cluster, "foreign physical cluster");
            }
            Self::Business {
                payload, position, ..
            }
            | Self::Snapshot {
                payload, position, ..
            }
            | Self::SnapshotManifest {
                payload, position, ..
            }
            | Self::SnapshotIndex {
                payload, position, ..
            }
            | Self::History {
                payload, position, ..
            } => ensure!(
                !payload.is_nil() && *position <= i64::MAX as u64,
                "invalid recovery business record"
            ),
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Cursor {
    pub media: Uuid,
    pub boot: Uuid,
    pub fence: Uuid,
    pub manifest: String,
    pub scan: Uuid,
    pub scope: String,
    pub page: u64,
    pub prefix: u16,
    pub complete: bool,
}

impl Cursor {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            [self.media, self.boot, self.fence, self.scan]
                .iter()
                .all(|v| !v.is_nil())
                && self.page <= i64::MAX as u64
                && self.prefix < 4096
                && self.manifest.len() == 64
                && self
                    .manifest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                && (self.scope == "recovery" || self.scope == "history" || self.scope == "layouts"),
            "invalid recovery scan cursor"
        );
        Ok(())
    }
}

pub(crate) struct CatalogPage {
    pub entries: Vec<CatalogEntry>,
    pub after: Option<(Uuid, u64)>,
}

#[async_trait::async_trait]
pub(crate) trait RecoveryCatalog: Send + Sync {
    async fn catalog_restore(
        &self,
        id: Uuid,
        operation: Uuid,
        epochs: &[crate::storage::marker::EpochCut],
    ) -> Result<()> {
        self.catalog_map(id, operation, epochs, true).await
    }
    async fn catalog_image(&self, id: Uuid, namespace: Uuid, path: &str) -> Result<Option<Intent>>;
    async fn catalog_shards(
        &self,
        id: Uuid,
        op: Uuid,
        seq: u32,
    ) -> Result<Vec<super::storage_payload::Receipt>>;
    async fn catalog_map(
        &self,
        id: Uuid,
        operation: Uuid,
        epochs: &[crate::storage::marker::EpochCut],
        install: bool,
    ) -> Result<()>;
    async fn catalog_bind(&self, id: Uuid, head: &Proof) -> Result<()>;
    async fn catalog_proofs(&self, id: Uuid, after: u64, through: u64) -> Result<Vec<Proof>>;
    async fn catalog_index(&self, id: Uuid, proof: &Proof) -> Result<()>;
    async fn catalog_file(&self, id: Uuid, path: &str) -> Result<Option<RecoveryFile>>;
    async fn catalog_files(&self, id: Uuid, after: Uuid) -> Result<Vec<RecoveryFile>>;
    async fn catalog_decoded(&self, id: Uuid, operation: Uuid, summary: &DecodedFile)
    -> Result<()>;
    async fn catalog_open(&self, id: Uuid, namespace: Uuid, anchor: &Manifest) -> Result<()>;
    async fn catalog_stage(&self, id: Uuid, entries: &[CatalogEntry]) -> Result<()>;
    async fn catalog_checkpoint(
        &self,
        id: Uuid,
        revision: i64,
        cursor: &Cursor,
        entries: &[CatalogEntry],
    ) -> Result<i64>;
    async fn catalog_cursors(
        &self,
        id: Uuid,
        after: Option<(Uuid, String)>,
    ) -> Result<Vec<(i64, Cursor)>>;
    async fn catalog_page(
        &self,
        id: Uuid,
        category: i16,
        owner: Option<Uuid>,
        after: Option<(Uuid, u64)>,
    ) -> Result<CatalogPage>;
    async fn catalog_entry(
        &self,
        id: Uuid,
        category: i16,
        key: Uuid,
        position: u64,
    ) -> Result<Option<CatalogEntry>>;
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct DecodedFile {
    pub journal: Uuid,
    pub epoch: Uuid,
    pub kind: crate::storage::journal::FrameType,
    pub first: u64,
    pub last: u64,
    pub previous: [u8; 32],
    pub tail: [u8; 32],
    pub starts_complete: bool,
    pub complete: bool,
    pub authentication: [u8; 32],
}

pub(crate) struct RecoveryFile {
    pub part: Part,
    pub through: u64,
    pub revision: u64,
    pub retired: bool,
    pub decoded: Option<DecodedFile>,
}

pub(crate) fn signing_key(key: &Key, cluster: Uuid) -> Result<Ed25519KeyPair> {
    let root = blake3::derive_key("mokyu/storage/recovery-authority/v1", &key.material);
    let seed = blake3::keyed_hash(&root, cluster.as_bytes());
    Ed25519KeyPair::from_seed_unchecked(seed.as_bytes())
        .map_err(|_| anyhow::anyhow!("cannot derive recovery signing key"))
}

pub(crate) fn public_key(key: &Key, cluster: Uuid) -> Result<[u8; 32]> {
    signing_key(key, cluster)?
        .public_key()
        .as_ref()
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid recovery public key"))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Link {
    pub id: Uuid,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Part {
    pub intent: Intent,
    pub path: String,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Root {
    pub intent: Intent,
    pub receipts: Vec<super::storage_payload::Receipt>,
}

impl Root {
    pub fn validate(&self, namespace: Uuid, cluster: Uuid) -> Result<()> {
        self.intent.validate()?;
        ensure!(
            self.intent.namespace == namespace
                && self.intent.cluster == cluster
                && self.intent.area == Area::Blocks
                && self.intent.acknowledgement == super::storage_cluster::Acknowledgement::All
                && self.intent.total > 0
                && self.intent.total <= crate::storage::marker::MAX_BYTES as u64
                && self.receipts.len() as u64 == self.intent.required_files()
                && self.receipts.len() <= 256,
            "invalid recovery root mapping"
        );
        let mut slots = std::collections::HashSet::new();
        for receipt in &self.receipts {
            receipt.validate()?;
            self.intent.accepts(&receipt.header)?;
            ensure!(
                slots.insert((
                    receipt.header.sequence,
                    receipt.header.copy,
                    receipt.header.shard
                )),
                "duplicate recovery root fragment"
            );
        }
        Ok(())
    }

    pub fn validate_layout(&self, manifest: &Manifest) -> Result<()> {
        ensure!(
            self.intent.cluster == manifest.cluster_id
                && self.intent.layout_revision == manifest.revision as u64
                && self.intent.takeover_epoch == manifest.takeover_epoch,
            "recovery root differs from its historical layout"
        );
        let group = manifest
            .groups
            .iter()
            .find(|g| g.id == self.intent.group)
            .ok_or_else(|| anyhow::anyhow!("recovery root group is missing"))?;
        let members = self.intent.members(group)?;
        let width = usize::from(self.intent.layout.data_shards + self.intent.layout.parity_shards);
        for receipt in &self.receipts {
            let h = &receipt.header;
            ensure!(
                members.get(usize::from(h.copy) * width + usize::from(h.shard)) == Some(&h.media),
                "recovery root is on another historical member"
            );
        }
        Ok(())
    }
}

impl Part {
    pub fn inspect(&self, bytes: &[u8]) -> Result<()> {
        self.visit(bytes, |_| Ok(()))
    }

    pub fn visit<'a>(
        &self,
        bytes: &'a [u8],
        mut visit: impl FnMut(crate::storage::journal::recovery::Public<'a>) -> Result<()>,
    ) -> Result<()> {
        self.validate(self.intent.namespace)?;
        ensure!(
            bytes.len() as u64 == self.intent.total
                && blake3::hash(bytes).as_bytes() == &self.digest,
            "recovery part differs from its publication"
        );
        let mut rest = bytes;
        let mut identity = None;
        while !rest.is_empty() {
            let public = crate::storage::journal::recovery::inspect_prefix(rest)?;
            ensure!(
                public.namespace == self.intent.namespace
                    && public.envelope.payload == self.intent.payload
                    && public.envelope.revision == self.intent.revision
                    && public.path == self.path,
                "recovery part has another identity"
            );
            let current = (public.journal, public.epoch);
            ensure!(
                identity.is_none_or(|previous| previous == current),
                "recovery part contains multiple journals"
            );
            identity = Some(current);
            rest = &rest[public.consumed()..];
            visit(public)?;
        }
        ensure!(identity.is_some(), "empty recovery part");
        Ok(())
    }

    pub fn validate(&self, namespace: Uuid) -> Result<()> {
        self.intent.validate()?;
        crate::storage::journal::recovery::path(&self.path)?;
        ensure!(
            self.intent.namespace == namespace
                && self.intent.area == Area::Recovery
                && self.intent.acknowledgement == super::storage_cluster::Acknowledgement::All
                && self.intent.total <= u64::from(super::journal::policy::MAX_LIMIT)
                && self.digest != [0; 32],
            "invalid published recovery part"
        );
        Ok(())
    }

    pub fn validate_layout(&self, manifest: &Manifest) -> Result<()> {
        ensure!(
            self.intent.cluster == manifest.cluster_id
                && self.intent.layout_revision == manifest.revision as u64
                && self.intent.takeover_epoch == manifest.takeover_epoch
                && manifest.groups.iter().any(
                    |group| group.id == self.intent.group && group.layout == self.intent.layout
                ),
            "recovery part differs from its historical layout"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Publication {
    pub format_version: u32,
    pub id: Uuid,
    pub namespace: Uuid,
    pub cluster: Uuid,
    pub manifest: String,
    pub takeover_epoch: u64,
    pub revision: u64,
    pub previous: Option<Link>,
    pub through: u64,
    pub parts: Vec<Part>,
    pub retired: Vec<Uuid>,
    pub identity: Option<serde_json::Value>,
    pub root: Option<Root>,
    pub prefix: String,
}

impl Publication {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.prefix.len() <= 1024
                && object_store::path::Path::parse(&self.prefix)?.as_ref() == self.prefix,
            "invalid recovery backend prefix"
        );
        if let Some(identity) = &self.identity {
            ensure!(
                crate::storage::marker::identity(identity)?.storage_id == self.namespace,
                "recovery identity belongs to another namespace"
            );
            crate::storage::marker::journal(identity)?;
        }
        if let Some(root) = &self.root {
            root.validate(self.namespace, self.cluster)?;
            ensure!(
                self.identity.is_some() && root.intent.takeover_epoch <= self.takeover_epoch,
                "recovery root has no identity or uses future ownership"
            );
        }
        ensure!(
            self.format_version == 1
                && !self.id.is_nil()
                && !self.namespace.is_nil()
                && !self.cluster.is_nil()
                && self.takeover_epoch > 0
                && self.revision > 0
                && self.revision <= i64::MAX as u64
                && self.through <= i64::MAX as u64
                && (self.revision == 1) == self.previous.is_none()
                && self.manifest.len() == 64
                && self
                    .manifest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                && self.parts.len() <= 256
                && self.retired.len() <= 256,
            "invalid recovery publication"
        );
        if let Some(previous) = &self.previous {
            ensure!(
                !previous.id.is_nil() && previous.id != self.id && previous.digest != [0; 32],
                "invalid recovery predecessor"
            );
        }
        let mut operations = std::collections::HashSet::new();
        let mut paths = std::collections::HashSet::new();
        for part in &self.parts {
            part.validate(self.namespace)?;
            ensure!(
                part.intent.cluster == self.cluster
                    && part.intent.takeover_epoch == self.takeover_epoch
                    && operations.insert(part.intent.operation)
                    && paths.insert(&part.path),
                "duplicate or foreign recovery part"
            );
        }
        for operation in &self.retired {
            ensure!(
                !operation.is_nil() && operations.insert(*operation),
                "duplicate recovery retirement"
            );
        }
        ensure!(
            serde_json::to_vec(self)?.len() <= PROOF_LIMIT - 256,
            "recovery publication is too large"
        );
        Ok(())
    }

    fn message(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut bytes = b"mokyu/storage/recovery-publication/v1\0".to_vec();
        bytes.extend_from_slice(&serde_json::to_vec(self)?);
        Ok(bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Proof {
    pub publication: Publication,
    pub signature: Vec<u8>,
}

impl Proof {
    pub fn sign(publication: Publication, key: &Key, manifest: &Manifest) -> Result<Self> {
        let signer = signing_key(key, publication.cluster)?;
        ensure!(
            signer.public_key().as_ref() == manifest.publication_key,
            "wrong recovery signing authority"
        );
        let signature = signer.sign(&publication.message()?).as_ref().to_vec();
        let proof = Self {
            publication,
            signature,
        };
        proof.verify(manifest)?;
        Ok(proof)
    }

    pub fn verify(&self, manifest: &Manifest) -> Result<()> {
        let publication = &self.publication;
        ensure!(
            publication.cluster == manifest.cluster_id
                && publication.takeover_epoch == manifest.takeover_epoch
                && publication.manifest == manifest.digest()?
                && self.signature.len() == 64,
            "recovery proof belongs to another historical manifest"
        );
        signature::UnparsedPublicKey::new(&signature::ED25519, manifest.publication_key)
            .verify(&publication.message()?, &self.signature)
            .map_err(|_| anyhow::anyhow!("recovery publication signature is invalid"))?;
        for part in &publication.parts {
            ensure!(
                part.intent.layout_revision <= manifest.revision as u64,
                "recovery part uses a future layout"
            );
            if part.intent.layout_revision == manifest.revision as u64 {
                part.validate_layout(manifest)?;
            }
        }
        if let Some(root) = &publication.root {
            ensure!(
                root.intent.layout_revision <= manifest.revision as u64,
                "recovery root uses a future layout"
            );
            if root.intent.layout_revision == manifest.revision as u64 {
                root.validate_layout(manifest)?;
            }
        }
        Ok(())
    }

    pub fn link(&self) -> Result<Link> {
        self.publication.validate()?;
        Ok(Link {
            id: self.publication.id,
            digest: *blake3::hash(&serde_json::to_vec(self)?).as_bytes(),
        })
    }

    pub fn follows(&self, previous: Option<&Self>) -> Result<()> {
        self.publication.validate()?;
        match previous {
            Some(previous) => {
                let value = &self.publication;
                let old = &previous.publication;
                let descriptor = |p: &Publication| {
                    p.identity
                        .as_ref()
                        .map(crate::storage::marker::journal)
                        .transpose()
                        .map(Option::flatten)
                };
                let takeover = match (descriptor(old)?, descriptor(value)?) {
                    (Some(before), Some(after)) => {
                        ensure!(
                            before.journal_id == after.journal_id,
                            "recovery publication changed its journal"
                        );
                        if before.writer_epoch == after.writer_epoch {
                            ensure!(before == after, "journal ownership changed without history");
                            false
                        } else {
                            ensure!(
                                value.through == 0
                                    && value.retired.is_empty()
                                    && value.parts.len() == 1
                                    && value.parts[0].path
                                        == format!(
                                            "{}/history/{}",
                                            crate::storage::journal::ROOT,
                                            after.writer_epoch.simple()
                                        )
                                    && after.head()? == Some(value.parts[0].digest),
                                "journal epoch changed without a committed takeover record"
                            );
                            true
                        }
                    }
                    (None, None) => false,
                    _ => anyhow::bail!("recovery journal descriptor disappeared or changed"),
                };
                if let (Some(before), Some(after)) = (&old.identity, &value.identity) {
                    let before = crate::storage::marker::identity(before)?;
                    let after = crate::storage::marker::identity(after)?;
                    ensure!(
                        before.deployment_id == after.deployment_id,
                        "recovery backend identity changed"
                    );
                }
                ensure!(
                    self.publication.namespace == previous.publication.namespace
                        && self.publication.cluster == previous.publication.cluster
                        && self.publication.prefix == previous.publication.prefix
                        && self.publication.takeover_epoch >= previous.publication.takeover_epoch
                        && self.publication.previous == Some(previous.link()?)
                        && previous.publication.revision.checked_add(1)
                            == Some(self.publication.revision)
                        && (takeover || self.publication.through >= previous.publication.through),
                    "recovery publication chain is incomplete or reordered"
                );
            }
            None => ensure!(
                self.publication.revision == 1 && self.publication.previous.is_none(),
                "recovery publication predecessor is missing"
            ),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_proofs_reject_substitution_and_missing_predecessors() -> Result<()> {
        let key = Key::new("aes-256-gcm", [3; 32])?;
        let cluster = Uuid::now_v7();
        let manifest = Manifest {
            format_version: 1,
            record_id: Uuid::now_v7(),
            cluster_id: cluster,
            deployment_id: Uuid::now_v7(),
            publication_key: public_key(&key, cluster)?,
            takeover_epoch: 1,
            revision: 1,
            previous: None,
            members: vec![],
            groups: vec![],
        };
        let publication = Publication {
            format_version: 1,
            id: Uuid::now_v7(),
            namespace: Uuid::now_v7(),
            cluster,
            manifest: manifest.digest()?,
            takeover_epoch: 1,
            revision: 1,
            previous: None,
            through: 42,
            parts: vec![],
            retired: vec![],
            identity: None,
            root: None,
            prefix: String::new(),
        };
        let first = Proof::sign(publication, &key, &manifest)?;
        first.follows(None)?;
        let mut next = first.publication.clone();
        next.id = Uuid::now_v7();
        next.revision = 2;
        next.previous = Some(first.link()?);
        next.through = 69;
        let second = Proof::sign(next, &key, &manifest)?;
        second.follows(Some(&first))?;
        assert!(second.follows(None).is_err());
        let mut corrupt = second.clone();
        corrupt.publication.through += 1;
        assert!(corrupt.verify(&manifest).is_err());
        let mut foreign = manifest.clone();
        foreign.publication_key = public_key(&Key::new("aes-256-gcm", [4; 32])?, cluster)?;
        assert!(second.verify(&foreign).is_err());
        let mut gap = second.publication.clone();
        gap.revision += 1;
        assert!(
            Proof::sign(gap, &key, &manifest)?
                .follows(Some(&first))
                .is_err()
        );
        let mut unsigned = second.clone();
        unsigned.signature.clear();
        assert!(unsigned.verify(&manifest).is_err());
        assert!(crate::storage::journal::recovery::path("metadata/changes/epoch/1.part").is_err());
        let namespace = first.publication.namespace;
        let journal = crate::storage::marker::Journal::create(namespace, &key)?;
        let mut identity = crate::storage::marker::create(manifest.deployment_id, namespace)?;
        identity["metadata_journal"] = serde_json::to_value(&journal)?;
        let mut origin = first.clone();
        origin.publication.identity = Some(identity.clone());
        let next_epoch = Uuid::now_v7();
        let descriptor = journal.transfer(namespace, next_epoch, [23; 32], &key, &key)?;
        identity["metadata_journal"] = serde_json::to_value(&descriptor)?;
        let mut handoff = origin.clone();
        handoff.publication.id = Uuid::now_v7();
        handoff.publication.revision = 2;
        handoff.publication.previous = Some(origin.link()?);
        handoff.publication.identity = Some(identity);
        handoff.publication.through = 0;
        handoff.publication.parts.push(Part {
            intent: Intent {
                operation: Uuid::now_v7(),
                namespace,
                payload: next_epoch,
                revision: 1,
                cluster,
                layout_revision: 1,
                takeover_epoch: 1,
                group: Uuid::now_v7(),
                layout: crate::model::storage_cluster::Layout::default(),
                members: Vec::new(),
                area: Area::Recovery,
                total: 512,
                acknowledgement: crate::model::storage_cluster::Acknowledgement::All,
            },
            path: format!("metadata/history/{}", next_epoch.simple()),
            digest: [23; 32],
        });
        handoff.follows(Some(&origin))?;
        for change in 0..6 {
            let mut invalid = handoff.clone();
            match change {
                0 => invalid.publication.parts.clear(),
                1 => invalid.publication.parts[0].digest = [24; 32],
                2 => invalid.publication.through = 1,
                3 => invalid.publication.identity = origin.publication.identity.clone(),
                4 => invalid.publication.prefix = "another".into(),
                _ => invalid.publication.identity = None,
            }
            assert!(invalid.follows(Some(&origin)).is_err());
        }
        let mut continued = handoff.clone();
        continued.publication.id = Uuid::now_v7();
        continued.publication.revision += 1;
        continued.publication.previous = Some(handoff.link()?);
        continued.publication.through = 2;
        continued.publication.parts.clear();
        continued.follows(Some(&handoff))?;
        Ok(())
    }
}
