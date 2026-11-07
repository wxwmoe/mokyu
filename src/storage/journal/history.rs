use super::{Format, FrameType, ROOT, Sequence, local::Local, source};
use crate::{
    content::codec::Key,
    storage::marker::{EpochCut, Journal},
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use object_store::{ObjectStore, PutMode, PutOptions, path::Path};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use uuid::Uuid;

const MAX_BYTES: u64 = 16 * 1024;

#[derive(Deserialize, Serialize)]
struct Record {
    storage: Uuid,
    next: Uuid,
    cut: EpochCut,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct Claim {
    pub descriptor: Journal,
    pub revision: (Option<String>, Option<String>),
    bytes: Vec<u8>,
}

fn path(epoch: Uuid) -> String {
    format!("{ROOT}/history/{}", epoch.simple())
}

impl Claim {
    pub fn freeze(
        journal: &Journal,
        storage: Uuid,
        cut: EpochCut,
        target: &Key,
        key: &Key,
        encrypted: bool,
        revision: (Option<String>, Option<String>),
    ) -> Result<Self> {
        ensure!(
            cut.epoch == journal.writer_epoch && (revision.0.is_some() || revision.1.is_some()),
            "invalid takeover origin"
        );
        let next = Uuid::new_v4();
        let format = Format {
            storage,
            journal: journal.journal_id,
            epoch: next,
            key,
            encrypted,
        };
        let sequence = Sequence {
            first: cut.sequence,
            last: cut.sequence,
            previous: journal.head()?.unwrap_or([0; 32]),
        };
        let bytes = format.encode(
            FrameType::History,
            &path(next),
            sequence,
            &serde_json::to_vec(&Record { storage, next, cut })?,
        )?;
        ensure!(
            bytes.len() as u64 <= MAX_BYTES,
            "history record exceeds limit"
        );
        let descriptor =
            journal.transfer(storage, next, *blake3::hash(&bytes).as_bytes(), target, key)?;
        Ok(Self {
            descriptor,
            revision,
            bytes,
        })
    }

    pub async fn verify(&self, store: &dyn ObjectStore) -> Result<()> {
        let bytes = source::bytes(store, &path(self.descriptor.writer_epoch), MAX_BYTES)
            .await?
            .context("missing committed takeover record")?;
        ensure!(
            bytes == self.bytes
                && self.descriptor.head()? == Some(*blake3::hash(&bytes).as_bytes()),
            "conflicting committed takeover"
        );
        Ok(())
    }

    pub async fn publish(&self, store: &dyn ObjectStore, local: Option<&Local>) -> Result<()> {
        let key = path(self.descriptor.writer_epoch);
        let want = self.descriptor.head()?.context("missing takeover digest")?;
        ensure!(
            *blake3::hash(&self.bytes).as_bytes() == want,
            "invalid takeover intent"
        );
        let result: Result<()> = if let Some(local) = local {
            local.create(&local.root.join(&key), &self.bytes)?;
            Ok(())
        } else {
            store
                .put_opts(
                    &Path::from(key.as_str()),
                    Bytes::copy_from_slice(&self.bytes).into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
                .map(|_| ())
                .map_err(Into::into)
        };
        match source::bytes(store, &key, MAX_BYTES).await? {
            Some(bytes) => ensure!(bytes == self.bytes, "conflicting journal takeover"),
            None => {
                result?;
                anyhow::bail!("takeover record is missing")
            }
        }
        Ok(())
    }
}

pub(crate) async fn chain(
    store: &dyn ObjectStore,
    journal: &Journal,
    storage: Uuid,
    key: &Key,
) -> Result<Vec<EpochCut>> {
    let mut expected = journal.head()?;
    let mut epoch = journal.writer_epoch;
    let mut visited = BTreeSet::from([epoch]);
    let mut cuts = Vec::new();
    while let Some(digest) = expected {
        let path = path(epoch);
        let data = source::bytes(store, &path, MAX_BYTES)
            .await?
            .context("missing takeover record")?;
        ensure!(
            *blake3::hash(&data).as_bytes() == digest,
            "takeover head disagrees"
        );
        let frame = Format {
            storage,
            journal: journal.journal_id,
            epoch,
            key,
            encrypted: false,
        }
        .decode(FrameType::History, &path, &data)?;
        let r: Record = serde_json::from_slice(&frame.bytes)?;
        ensure!(
            frame.consumed == data.len()
                && r.storage == storage
                && r.next == epoch
                && !r.cut.epoch.is_nil()
                && frame.sequence.first == r.cut.sequence
                && frame.sequence.last == r.cut.sequence
                && visited.insert(r.cut.epoch),
            "invalid takeover chain"
        );
        expected = (frame.sequence.previous != [0; 32]).then_some(frame.sequence.previous);
        epoch = r.cut.epoch;
        cuts.push(r.cut);
    }
    cuts.reverse();
    Ok(cuts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{ObjectStoreExt, memory::InMemory};

    #[tokio::test]
    async fn recovery_follows_only_the_rooted_chain() -> Result<()> {
        let store = InMemory::new();
        let storage = Uuid::new_v4();
        let original = Key::new("aes-256-gcm", [5; 32])?;
        let next = Key::new("aes-256-gcm", [7; 32])?;
        let root = Journal::create(storage, &original)?;
        assert!(serde_json::to_value(&root)?.get("history_head").is_none());
        let key = root.unlock(storage, &original)?;
        let cut = |epoch, sequence| EpochCut {
            epoch,
            sequence,
            tail: [sequence as u8; 32],
            salvaged: None,
        };
        let a = cut(root.writer_epoch, 3);
        let first = Claim::freeze(
            &root,
            storage,
            a.clone(),
            &next,
            &key,
            false,
            (Some("a".into()), None),
        )?;
        first.publish(&store, None).await?;
        first.publish(&store, None).await?;
        assert!(chain(&store, &root, storage, &key).await?.is_empty());
        let middle = &first.descriptor;
        assert!(middle.unlock(storage, &original).is_err());
        assert_eq!(middle.unlock(storage, &next)?.material, key.material);
        let b = cut(middle.writer_epoch, 11);
        let second = Claim::freeze(
            middle,
            storage,
            b.clone(),
            &original,
            &key,
            true,
            (Some("b".into()), None),
        )?;
        second.publish(&store, None).await?;
        assert_eq!(chain(&store, middle, storage, &key).await?, [a.clone()]);
        assert_eq!(
            chain(&store, &second.descriptor, storage, &key).await?,
            [a, b]
        );
        let first_path = Path::from(path(middle.writer_epoch));
        store
            .put(&first_path, Bytes::copy_from_slice(&second.bytes).into())
            .await?;
        assert!(
            chain(&store, &second.descriptor, storage, &key)
                .await
                .is_err()
        );
        assert!(first.publish(&store, None).await.is_err());
        store.delete(&first_path).await?;
        assert!(
            chain(&store, &second.descriptor, storage, &key)
                .await
                .is_err()
        );
        Ok(())
    }
}
