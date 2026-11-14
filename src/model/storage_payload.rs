use crate::content::fragment::{self, Area, Digest, Header};
use crate::model::storage_cluster::{Acknowledgement, Group, Layout};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const BATCH_LIMIT: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Intent {
    pub operation: Uuid,
    pub namespace: Uuid,
    pub payload: Uuid,
    pub revision: u64,
    pub cluster: Uuid,
    pub layout_revision: u64,
    pub takeover_epoch: u64,
    pub group: Uuid,
    pub layout: Layout,
    #[serde(default)]
    pub members: Vec<Uuid>,
    pub area: Area,
    pub total: u64,
    pub acknowledgement: Acknowledgement,
}

impl Intent {
    pub fn validate(&self) -> Result<()> {
        self.layout.validate()?;
        ensure!(
            self.members.is_empty()
                || self.members.len() == self.layout.width()
                    && self.members.iter().all(|id| !id.is_nil())
                    && self
                        .members
                        .iter()
                        .collect::<std::collections::HashSet<_>>()
                        .len()
                        == self.members.len(),
            "invalid payload member order"
        );
        ensure!(
            [
                self.operation,
                self.namespace,
                self.payload,
                self.cluster,
                self.group
            ]
            .iter()
            .all(|id| !id.is_nil()),
            "invalid payload identity"
        );
        ensure!(
            self.revision > 0
                && self.layout_revision > 0
                && self.takeover_epoch > 0
                && self.revision <= i64::MAX as u64
                && self.layout_revision <= i64::MAX as u64
                && self.takeover_epoch <= i64::MAX as u64
                && self.total <= i64::MAX as u64,
            "invalid payload revision or size"
        );
        ensure!(
            self.sequences() <= u32::MAX as u64,
            "payload has too many file sequences"
        );
        Ok(())
    }

    pub fn span(&self) -> u64 {
        fragment::FILE_LIMIT / u64::from(self.layout.stripe_bytes)
            * u64::from(self.layout.stripe_bytes)
            * u64::from(self.layout.data_shards)
    }

    pub fn members<'a>(&'a self, group: &'a Group) -> Result<&'a [Uuid]> {
        ensure!(
            group.id == self.group && group.layout == self.layout,
            "payload group differs from its historical layout"
        );
        if self.members.is_empty() {
            return Ok(&group.members);
        }
        ensure!(
            self.members.len() == group.members.len()
                && self.members.iter().all(|id| group.members.contains(id)),
            "payload members differ from their historical group"
        );
        Ok(&self.members)
    }

    pub fn sequences(&self) -> u64 {
        self.total.div_ceil(self.span()).max(1)
    }

    pub fn header(
        &self,
        sequence: u32,
        copy: u16,
        shard: u16,
        media: Uuid,
        fragment: Uuid,
    ) -> Result<Header> {
        self.validate()?;
        let offset = u64::from(sequence)
            .checked_mul(self.span())
            .context("payload offset overflow")?;
        let length = self.total.saturating_sub(offset).min(self.span());
        let stripe = u64::from(self.layout.stripe_bytes);
        let row = u64::from(self.layout.data_shards) * stripe;
        let shard_bytes = if self.layout.parity_shards > 0 {
            length.div_ceil(row) * stripe
        } else {
            length / row * stripe
                + (length % row)
                    .saturating_sub(u64::from(shard) * stripe)
                    .min(stripe)
        };
        let header = Header {
            area: self.area,
            namespace: self.namespace,
            payload: self.payload,
            fragment,
            media,
            cluster: self.cluster,
            group: self.group,
            operation: self.operation,
            layout_revision: self.layout_revision,
            revision: self.revision,
            generation: 0,
            takeover_epoch: self.takeover_epoch,
            offset,
            length,
            total: self.total,
            shard_bytes,
            sequence,
            layout: self.layout.clone(),
            copy,
            shard,
            digest: [0; 32],
        };
        self.accepts(&header)?;
        Ok(header)
    }

    pub fn required(&self, copy: u16, shard: u16) -> bool {
        match self.acknowledgement {
            Acknowledgement::All => true,
            Acknowledgement::DataAndParity => copy == 0,
            Acknowledgement::Data => copy == 0 && shard < self.layout.data_shards,
        }
    }

    pub fn required_files(&self) -> u64 {
        self.sequences()
            * match self.acknowledgement {
                Acknowledgement::All => self.layout.width() as u64,
                Acknowledgement::DataAndParity => {
                    u64::from(self.layout.data_shards + self.layout.parity_shards)
                }
                Acknowledgement::Data => u64::from(self.layout.data_shards),
            }
    }

    pub fn accepts(&self, header: &Header) -> Result<()> {
        header.validate()?;
        let offset = u64::from(header.sequence)
            .checked_mul(self.span())
            .context("payload range overflow")?;
        ensure!(
            header.operation == self.operation
                && header.namespace == self.namespace
                && header.payload == self.payload
                && header.revision == self.revision
                && header.cluster == self.cluster
                && header.layout_revision == self.layout_revision
                && header.takeover_epoch == self.takeover_epoch
                && header.group == self.group
                && header.layout == self.layout
                && header.area == self.area
                && header.total == self.total
                && u64::from(header.sequence) < self.sequences()
                && header.offset == offset
                && header.length == self.total.saturating_sub(offset).min(self.span()),
            "fragment differs from its payload intent"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Receipt {
    pub header: Header,
    pub root: Digest,
    pub commitment: Digest,
}

impl Receipt {
    pub fn newer_than(&self, previous: &Self) -> Result<bool> {
        self.validate()?;
        previous.validate()?;
        let mut expected = previous.header.clone();
        expected.fragment = self.header.fragment;
        expected.generation = self.header.generation;
        ensure!(
            expected == self.header && self.root == previous.root,
            "fragment generation changed its content or placement"
        );
        ensure!(
            self.header.generation != previous.header.generation || self == previous,
            "competing fragments occupy the same generation"
        );
        Ok(self.header.generation > previous.header.generation)
    }

    pub fn validate(&self) -> Result<()> {
        self.header.validate()?;
        ensure!(
            self.header.digest != [0; 32]
                && self.commitment == fragment::commitment(&self.header, &self.root)?,
            "invalid durable fragment receipt"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Mapping {
    pub header: Header,
    pub receipt: Option<Receipt>,
    pub deleted: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct Payload {
    pub intent: Intent,
    pub state: String,
    pub protected_until: i64,
    pub write_until: i64,
}

// These methods accept control-authorized work, never untrusted node input directly.
#[async_trait::async_trait]
pub(crate) trait StoragePayloads:
    super::storage_recovery::RecoveryCatalog
    + super::storage_native::NativeObjects
    + super::storage_repair::Repairs
    + super::storage_backup::Backups
    + Send
    + Sync
{
    async fn prepare(&self, intent: &Intent, until: i64) -> Result<Payload>;
    async fn stage(&self, operation: Uuid, headers: &[Header]) -> Result<()>;
    async fn receipts(&self, operation: Uuid, receipts: &[Receipt]) -> Result<()>;
    async fn publish(&self, operation: Uuid) -> Result<()>;
    async fn get(&self, operation: Uuid) -> Result<Payload>;
    async fn write_window(
        &self,
        operation: Uuid,
        first: u32,
        count: u32,
        until: i64,
    ) -> Result<Vec<Mapping>>;
    async fn read(
        &self,
        operation: Uuid,
        first: u32,
        count: u32,
        until: i64,
    ) -> Result<Vec<Mapping>>;
    async fn retire(&self, operation: Uuid) -> Result<()>;
    async fn resume_read(
        &self,
        operation: Uuid,
        first: u32,
        count: u32,
        until: i64,
    ) -> Result<Vec<Mapping>>;
    async fn reclaim(&self, operation: Uuid, after: Option<Uuid>) -> Result<Vec<Mapping>>;
    async fn collect_candidates(&self, namespace: Uuid, limit: usize) -> Result<Vec<Uuid>>;
    async fn removed(&self, operation: Uuid, fragments: &[Uuid]) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_sequences_cover_stripes_without_per_unit_files() -> Result<()> {
        for (data_shards, parity_shards, extra_copies) in
            [(1, 0, 1), (4, 0, 0), (4, 2, 0), (2, 1, 2), (20, 4, 0)]
        {
            let layout = Layout {
                data_shards,
                parity_shards,
                extra_copies,
                stripe_bytes: 65536,
            };
            let mut intent = Intent {
                operation: Uuid::from_u128(1),
                namespace: Uuid::from_u128(2),
                payload: Uuid::from_u128(3),
                revision: 1,
                cluster: Uuid::from_u128(4),
                layout_revision: 1,
                takeover_epoch: 1,
                group: Uuid::from_u128(5),
                layout,
                members: Vec::new(),
                area: Area::Blocks,
                total: 0,
                acknowledgement: Acknowledgement::All,
            };
            for total in [0, 1, 65537, intent.span(), intent.span() + 321] {
                intent.total = total;
                intent.validate()?;
                for sequence in 0..intent.sequences() as u32 {
                    let offset = u64::from(sequence) * intent.span();
                    let length = total.saturating_sub(offset).min(intent.span());
                    for shard in 0..data_shards + parity_shards {
                        let stripe = u64::from(intent.layout.stripe_bytes);
                        let row = u64::from(data_shards) * stripe;
                        let shard_bytes = if parity_shards > 0 {
                            length.div_ceil(row) * stripe
                        } else {
                            length / row * stripe
                                + (length % row)
                                    .saturating_sub(u64::from(shard) * stripe)
                                    .min(stripe)
                        };
                        let header = Header {
                            area: intent.area,
                            namespace: intent.namespace,
                            payload: intent.payload,
                            fragment: Uuid::from_u128(6),
                            generation: 0,
                            media: Uuid::from_u128(7),
                            cluster: intent.cluster,
                            group: intent.group,
                            operation: intent.operation,
                            layout_revision: 1,
                            revision: 1,
                            takeover_epoch: 1,
                            offset,
                            length,
                            total,
                            shard_bytes,
                            sequence,
                            layout: intent.layout.clone(),
                            copy: extra_copies,
                            shard,
                            digest: [0; 32],
                        };
                        intent.accepts(&header)?;
                        assert_eq!(Header::decode(&header.encode()?)?, header);
                        let mut wrong = header;
                        wrong.sequence += 1;
                        assert!(intent.accepts(&wrong).is_err());
                    }
                }
                assert!(intent.sequences() <= 2);
            }
        }
        Ok(())
    }
}
