use super::{
    storage_cluster::Manifest,
    storage_payload::Receipt,
    storage_recovery::{Proof, signing_key},
};
use crate::content::{
    codec::Key,
    fragment::{Area, Header},
};
use anyhow::{Result, ensure};
use aws_lc_rs::signature::{self, KeyPair};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const GRANT_LIMIT: usize = 256 * 1024;
pub(crate) const STREAMS_PER_CONNECTION: usize = 16;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum FragmentState {
    Missing,
    Pending {
        bytes: u64,
        complete: Option<Receipt>,
    },
    Complete(Receipt),
}

impl FragmentState {
    pub fn validate(&self, planned: &Header) -> Result<()> {
        let receipt = match self {
            Self::Missing => None,
            Self::Pending { bytes, complete } => {
                ensure!(
                    *bytes <= planned.file_bytes(),
                    "pending fragment exceeds its plan"
                );
                complete.as_ref()
            }
            Self::Complete(receipt) => Some(receipt),
        };
        if let Some(receipt) = receipt {
            receipt.validate()?;
            let mut expected = receipt.header.clone();
            if planned.digest == [0; 32] {
                expected.digest = [0; 32];
            }
            ensure!(expected == *planned, "storage state differs from its plan");
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Destination {
    pub url: String,
    pub authorization: SignedGrant,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Action {
    Write {
        header: Header,
    },
    Repair {
        header: Header,
    },
    Backup {
        header: Header,
    },
    BackupCheck {
        receipt: Receipt,
        evidence: Option<super::storage_recovery::Link>,
    },
    BackupRead {
        receipt: Receipt,
        offset: u64,
        length: u64,
    },
    Read {
        receipt: Receipt,
        offset: u64,
        length: u64,
    },
    Stat {
        header: Header,
    },
    Remove {
        receipt: Receipt,
    },
    Discard {
        header: Header,
    },
    Publish {
        proof: Proof,
        manifest: Manifest,
    },
    Scan {
        area: Area,
        prefix: u16,
    },
    History {
        namespace: Uuid,
    },
    Layouts,
    Namespaces,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Grant {
    pub id: Uuid,
    pub deployment: Uuid,
    pub node: Uuid,
    pub media: Uuid,
    pub boot: Uuid,
    pub fence: Uuid,
    pub cluster: Uuid,
    pub takeover_epoch: u64,
    pub manifest_revision: u64,
    pub issued: i64,
    pub expires: i64,
    pub action: Action,
}

impl Action {
    pub fn write_header(&self) -> Option<&Header> {
        match self {
            Self::Write { header } | Self::Repair { header } | Self::Backup { header } => {
                Some(header)
            }
            _ => None,
        }
    }
    pub fn read_range(&self) -> Option<(&Receipt, u64, u64)> {
        match self {
            Self::Read {
                receipt,
                offset,
                length,
            }
            | Self::BackupRead {
                receipt,
                offset,
                length,
            } => Some((receipt, *offset, *length)),
            _ => None,
        }
    }
}

impl Grant {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            [
                self.id,
                self.deployment,
                self.node,
                self.media,
                self.boot,
                self.fence,
                self.cluster
            ]
            .iter()
            .all(|v| !v.is_nil())
                && self.takeover_epoch > 0
                && self.manifest_revision > 0
                && self.manifest_revision <= i64::MAX as u64
                && self.issued > 0
                && self.expires > self.issued
                && self.expires - self.issued <= 90,
            "invalid storage grant identity or lifetime"
        );
        let header = match &self.action {
            Action::Write { header } | Action::Stat { header } | Action::Discard { header } => {
                Some(header)
            }
            Action::Backup { header } => {
                ensure!(
                    header.digest != [0; 32],
                    "backup requires a verified source"
                );
                Some(header)
            }
            Action::Repair { header } => {
                ensure!(
                    header.generation > 0,
                    "repair grant requires a replacement generation"
                );
                Some(header)
            }
            Action::Read {
                receipt,
                offset,
                length,
            }
            | Action::BackupRead {
                receipt,
                offset,
                length,
            } => {
                receipt.validate()?;
                ensure!(
                    offset
                        .checked_add(*length)
                        .is_some_and(|end| end <= receipt.header.shard_bytes),
                    "storage grant exceeds the fragment range"
                );
                Some(&receipt.header)
            }
            Action::Remove { receipt } | Action::BackupCheck { receipt, .. } => {
                receipt.validate()?;
                Some(&receipt.header)
            }
            Action::Publish { proof, manifest } => {
                proof.publication.validate()?;
                proof.verify(manifest)?;
                ensure!(
                    proof.publication.cluster == self.cluster
                        && proof.publication.takeover_epoch <= self.takeover_epoch
                        && manifest.revision as u64 <= self.manifest_revision,
                    "storage publication grant has another owner"
                );
                None
            }
            Action::Scan { prefix, .. } => {
                ensure!(*prefix < 4096, "invalid storage scan prefix");
                None
            }
            Action::History { namespace } => {
                ensure!(!namespace.is_nil(), "invalid storage history namespace");
                None
            }
            Action::Layouts | Action::Namespaces => None,
        };
        if let Some(header) = header {
            header.validate()?;
            ensure!(
                (header.media == self.media
                    || matches!(
                        self.action,
                        Action::Backup { .. }
                            | Action::BackupRead { .. }
                            | Action::BackupCheck { .. }
                    ))
                    && header.cluster == self.cluster
                    && (header.takeover_epoch == self.takeover_epoch
                        || !matches!(self.action, Action::Write { .. })
                            && header.takeover_epoch < self.takeover_epoch)
                    && header.layout_revision <= self.manifest_revision,
                "storage grant has another fragment owner"
            );
        }
        Ok(())
    }

    fn message(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut bytes = b"mokyu/storage/transfer-grant/v1\0".to_vec();
        bytes.extend_from_slice(&serde_json::to_vec(self)?);
        ensure!(
            bytes.len() <= GRANT_LIMIT - 256,
            "storage grant exceeds its bound"
        );
        Ok(bytes)
    }

    pub fn renewal(&self, next: &Self) -> Result<()> {
        let mut expected = self.clone();
        expected.issued = next.issued;
        expected.expires = next.expires;
        ensure!(
            expected == *next && next.issued >= self.issued && next.expires >= self.expires,
            "storage renewal changed the operation or shortened its lifetime"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SignedGrant {
    pub grant: Grant,
    pub signature: Vec<u8>,
}

impl SignedGrant {
    pub fn sign(grant: Grant, key: &Key, manifest: &Manifest) -> Result<Self> {
        let key = signing_key(key, grant.cluster)?;
        ensure!(
            key.public_key().as_ref() == manifest.publication_key,
            "wrong storage grant authority"
        );
        let signature = key.sign(&grant.message()?).as_ref().to_vec();
        let signed = Self { grant, signature };
        signed.verify(manifest)?;
        Ok(signed)
    }

    pub fn verify(&self, manifest: &Manifest) -> Result<()> {
        let grant = &self.grant;
        ensure!(
            grant.deployment == manifest.deployment_id
                && grant.cluster == manifest.cluster_id
                && grant.takeover_epoch == manifest.takeover_epoch
                && grant.manifest_revision <= manifest.revision as u64
                && manifest
                    .members
                    .iter()
                    .any(|member| member.media_id == grant.media && member.node_id == grant.node),
            "storage grant differs from the active membership"
        );
        let backup = manifest
            .members
            .iter()
            .find(|m| m.media_id == grant.media)
            .is_some_and(|m| m.topology.purpose == "backup");
        match &grant.action {
            Action::Backup { header }
            | Action::BackupRead {
                receipt: Receipt { header, .. },
                ..
            }
            | Action::BackupCheck {
                receipt: Receipt { header, .. },
                ..
            } => {
                ensure!(
                    backup && header.media != grant.media,
                    "backup grant requires independent backup media"
                );
            }
            Action::Write { .. }
            | Action::Read { .. }
            | Action::Repair { .. }
            | Action::Remove { .. }
            | Action::Discard { .. }
            | Action::Stat { .. } => {
                ensure!(!backup, "a primary grant cannot use backup media");
            }
            _ => (),
        }
        signature::UnparsedPublicKey::new(&signature::ED25519, manifest.publication_key)
            .verify(&grant.message()?, &self.signature)
            .map_err(|_| anyhow::anyhow!("invalid storage grant signature"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SignedReceipt {
    pub grant: Uuid,
    pub receipt: Receipt,
    pub accounting: super::storage_cluster::Accounting,
    pub tag: [u8; 32],
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Completion {
    pub authorization: SignedGrant,
    pub receipt: SignedReceipt,
}

impl Completion {
    pub fn verify(
        &self,
        key: &Key,
        manifest: &Manifest,
        target: &super::storage_cluster::Target,
    ) -> Result<()> {
        let grant = &self.authorization.grant;
        self.authorization.verify(manifest)?;
        let run = target.run.ok_or(super::Error::NodeExpired)?;
        let now = chrono::Utc::now().timestamp();
        ensure!(
            grant.node == target.device.node_id
                && grant.media == target.device.media_id
                && grant.boot == run.boot
                && grant.fence == run.fence
                && grant.issued <= now + 5
                && grant.expires > now
                && target.expires > now * 1_000_000,
            "storage receipt belongs to an inactive run"
        );
        let Some(header) = grant.action.write_header().or_else(|| match &grant.action {
            Action::BackupCheck { receipt, .. } => Some(&receipt.header),
            _ => None,
        }) else {
            anyhow::bail!("storage receipt requires a write grant");
        };
        let mut expected = self.receipt.receipt.header.clone();
        if header.digest == [0; 32] {
            expected.digest = [0; 32];
        }
        ensure!(
            expected == *header,
            "storage receipt differs from its write grant"
        );
        ensure!(
            self.receipt.accounting.boot == grant.boot,
            "storage receipt accounting belongs to another boot"
        );
        let credential = crate::content::codec::unprotect(
            target
                .credential
                .as_deref()
                .ok_or(super::Error::NodeAuthentication)?,
            key,
            crate::cluster::credential_binding(grant.node).as_bytes(),
        )?;
        self.receipt.verify(grant.id, &credential)
    }
}

impl SignedReceipt {
    pub fn seal(
        grant: Uuid,
        receipt: Receipt,
        accounting: super::storage_cluster::Accounting,
        credential: &str,
    ) -> Result<Self> {
        receipt.validate()?;
        accounting.validate()?;
        let mut value = Self {
            grant,
            receipt,
            accounting,
            tag: [0; 32],
        };
        value.tag = value.mac(credential)?;
        Ok(value)
    }

    fn mac(&self, credential: &str) -> Result<[u8; 32]> {
        let key = blake3::derive_key("mokyu/storage/receipt/v1", credential.as_bytes());
        let mut digest = blake3::Hasher::new_keyed(&key);
        digest.update(self.grant.as_bytes());
        serde_json::to_writer(&mut digest, &self.receipt)?;
        serde_json::to_writer(&mut digest, &self.accounting)?;
        Ok(*digest.finalize().as_bytes())
    }

    pub fn verify(&self, grant: Uuid, credential: &str) -> Result<()> {
        self.receipt.validate()?;
        self.accounting.validate()?;
        ensure!(
            self.grant == grant,
            "storage receipt belongs to another grant"
        );
        aws_lc_rs::constant_time::verify_slices_are_equal(&self.tag, &self.mac(credential)?)
            .map_err(|_| anyhow::anyhow!("invalid storage receipt authentication"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        storage_cluster::{Member, Topology},
        storage_recovery::{Publication, public_key},
    };

    #[test]
    fn storage_grants_bind_authority_operation_and_renewal() -> Result<()> {
        let key = Key::new("aes-256-gcm", [19; 32])?;
        let cluster = Uuid::now_v7();
        let media = Uuid::now_v7();
        let node = Uuid::now_v7();
        let manifest = Manifest {
            format_version: 1,
            record_id: Uuid::now_v7(),
            cluster_id: cluster,
            deployment_id: Uuid::now_v7(),
            publication_key: public_key(&key, cluster)?,
            takeover_epoch: 1,
            revision: 1,
            previous: None,
            members: vec![Member {
                media_id: media,
                node_id: node,
                state: "available".into(),
                topology: Topology {
                    url: "https://storage.example".into(),
                    region: None,
                    storage_group: "host".into(),
                    purpose: "primary".into(),
                },
            }],
            groups: vec![],
        };
        let namespace = Uuid::now_v7();
        let grant = Grant {
            id: Uuid::now_v7(),
            deployment: manifest.deployment_id,
            node,
            media,
            boot: Uuid::now_v7(),
            fence: Uuid::now_v7(),
            cluster,
            takeover_epoch: 1,
            manifest_revision: 1,
            issued: 1000,
            expires: 1090,
            action: Action::History { namespace },
        };
        let signed = SignedGrant::sign(grant.clone(), &key, &manifest)?;
        signed.verify(&manifest)?;
        let mut corrupt = signed.clone();
        corrupt.grant.action = Action::History {
            namespace: Uuid::now_v7(),
        };
        assert!(corrupt.verify(&manifest).is_err());
        let mut takeover = manifest.clone();
        takeover.takeover_epoch += 1;
        assert!(signed.verify(&takeover).is_err());
        let mut renewed = grant.clone();
        renewed.issued += 30;
        renewed.expires += 30;
        grant.renewal(&renewed)?;
        renewed.fence = Uuid::now_v7();
        assert!(grant.renewal(&renewed).is_err());
        renewed = grant.clone();
        renewed.expires += 1;
        assert!(SignedGrant::sign(renewed, &key, &manifest).is_err());
        let proof = Proof {
            publication: Publication {
                format_version: 1,
                id: grant.id,
                namespace,
                cluster,
                manifest: manifest.digest()?,
                takeover_epoch: 1,
                revision: 1,
                previous: None,
                through: 1,
                parts: vec![],
                retired: vec![],
                identity: None,
                root: None,
                prefix: String::new(),
            },
            signature: signed.signature,
        };
        assert!(proof.verify(&manifest).is_err());
        Ok(())
    }
}
