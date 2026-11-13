use super::{storage_cluster::Manifest, storage_payload::Intent};
use crate::{content::codec::Key, content::fragment::Area};
use anyhow::{Result, ensure};
use aws_lc_rs::signature::{self, Ed25519KeyPair, KeyPair};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) mod verify;

pub(crate) const PROOF_LIMIT: usize = 128 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Physical {
    Payload {
        sequence: u64,
        intent: Intent,
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
            Self::Payload { sequence, intent } => {
                intent.validate()?;
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
    Physical(Physical),
    Business {
        payload: Uuid,
        position: u64,
        record: crate::storage::journal::batch::Record,
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
            Self::Physical(Physical::Payload { sequence, intent }) => {
                (3, intent.operation, intent.operation, *sequence)
            }
            Self::Physical(Physical::Fragment { sequence, receipt }) => (
                4,
                receipt.header.operation,
                receipt.header.fragment,
                *sequence,
            ),
            Self::Physical(Physical::State {
                sequence,
                operation,
                ..
            }) => (5, *operation, *operation, *sequence),
            Self::Business {
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
            Self::Fragment(value) => {
                value.validate()?;
                ensure!(
                    value.header.cluster == cluster
                        && value.header.namespace == namespace
                        && value.header.area == Area::Recovery,
                    "foreign recovery fragment"
                );
            }
            Self::Physical(value) => {
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

impl Part {
    pub fn inspect<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<crate::storage::journal::recovery::Public<'a>> {
        self.validate(self.intent.namespace)?;
        ensure!(
            bytes.len() as u64 == self.intent.total
                && blake3::hash(bytes).as_bytes() == &self.digest,
            "recovery part differs from its publication"
        );
        let public = crate::storage::journal::recovery::inspect(bytes)?;
        ensure!(
            public.namespace == self.intent.namespace
                && public.envelope.payload == self.intent.payload
                && public.envelope.revision == self.intent.revision
                && public.path == self.path,
            "recovery part has another identity"
        );
        Ok(public)
    }

    pub fn validate(&self, namespace: Uuid) -> Result<()> {
        self.intent.validate()?;
        crate::storage::journal::recovery::path(&self.path)?;
        ensure!(
            self.intent.namespace == namespace
                && self.intent.area == Area::Recovery
                && self.intent.acknowledgement == super::storage_cluster::Acknowledgement::All
                && self.intent.total <= 32 * 1024 * 1024
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
}

impl Publication {
    pub fn validate(&self) -> Result<()> {
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
            Some(previous) => ensure!(
                self.publication.namespace == previous.publication.namespace
                    && self.publication.cluster == previous.publication.cluster
                    && self.publication.previous == Some(previous.link()?)
                    && previous.publication.revision.checked_add(1)
                        == Some(self.publication.revision)
                    && self.publication.through >= previous.publication.through,
                "recovery publication chain is incomplete or reordered"
            ),
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
        Ok(())
    }
}
