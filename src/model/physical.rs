use crate::content::{codec::Chunk, pack::Pack};
use crate::model::Id;
use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

pub(crate) type StagedUpload = (Id, Option<Uuid>, Option<String>, i64, Uuid);

#[derive(Clone, serde::Deserialize, serde::Serialize)]
#[serde(tag = "source", content = "metadata", rename_all = "snake_case")]
pub(crate) enum PhysicalSource {
    Chunk(Chunk),
    Pack(Pack),
    Raw(crate::model::raw::RawSource),
}

impl PhysicalSource {
    pub(crate) fn unit(&self) -> super::tiering::Unit {
        use super::tiering::Unit;
        match self {
            Self::Chunk(_) => Unit::Chunk,
            Self::Pack(_) => Unit::Pack,
            Self::Raw(_) => Unit::Raw,
        }
    }

    pub(crate) fn identity(&self) -> Uuid {
        match self {
            Self::Chunk(c) => c.location_id,
            Self::Pack(p) => p.location_id,
            Self::Raw(r) => r.location_id,
        }
    }

    pub(crate) fn address(&self) -> Uuid {
        match self {
            Self::Chunk(c) => c.address(),
            Self::Pack(p) => p.address(),
            Self::Raw(r) => r.address(),
        }
    }

    pub(crate) fn storage(&self) -> Result<Uuid> {
        use anyhow::Context;
        match self {
            Self::Chunk(c) => c.storage_id,
            Self::Pack(p) => p.storage_id,
            Self::Raw(r) => Some(r.storage_id),
        }
        .context("physical source has no backend")
    }

    pub(crate) fn bytes(&self) -> Result<i64> {
        use anyhow::Context;
        match self {
            Self::Chunk(c) => Some(c.file_size()? as i64),
            Self::Pack(p) => p.stored_size,
            Self::Raw(r) => Some(r.size),
        }
        .filter(|n| *n >= 0)
        .context("invalid physical source length")
    }

    pub(crate) fn block_key(&self) -> Option<Uuid> {
        match self {
            Self::Chunk(c) => c.block_key_id,
            Self::Pack(p) => p.block_key_id,
            Self::Raw(_) => None,
        }
    }

    pub(crate) fn relocate(&mut self, storage: Uuid, address: Uuid, local: bool) {
        match self {
            Self::Chunk(c) => {
                c.home_storage_id = c.home_storage().filter(|home| *home != storage);
                c.storage_id = Some(storage);
                c.address = Some(address);
            }
            Self::Pack(p) => {
                p.home_storage_id = p.home_storage().filter(|home| *home != storage);
                p.storage_id = Some(storage);
                p.address = Some(address);
            }
            Self::Raw(r) => {
                let home = r.home_storage();
                r.home_storage_id = (home != storage).then_some(home);
                r.storage_id = storage;
                r.address = Some(address);
                r.medium = if local { "local" } else { "shared_s3" }.into();
            }
        }
    }

    pub(crate) fn same_content(&self, other: &Self) -> bool {
        self.identity() == other.identity()
            && match (self, other) {
                (Self::Chunk(a), Self::Chunk(b)) => {
                    a.id == b.id
                        && a.uuid == b.uuid
                        && a.encoding_id == b.encoding_id
                        && a.raw_size == b.raw_size
                        && a.hash == b.hash
                        && a.format == b.format
                }
                (Self::Pack(a), Self::Pack(b)) => {
                    a.id == b.id
                        && a.raw_size == b.raw_size
                        && a.digest == b.digest
                        && a.member_count == b.member_count
                }
                (Self::Raw(a), Self::Raw(b)) => {
                    a.id == b.id
                        && a.size == b.size
                        && a.digest == b.digest
                        && !a.pending_upload
                        && !b.pending_upload
                }
                _ => false,
            }
    }

    fn bind_location(self, mut candidate: Self) -> sqlx::Result<Self> {
        if !self.same_content(&candidate)
            || candidate.address().is_nil()
            || candidate.address() == self.identity()
            || candidate.bytes().is_err()
        {
            return Err(sqlx::Error::Decode(
                "tier location does not match its immutable source".into(),
            ));
        }
        // Ownership comes from the database row even when the tier supplies new I/O material.
        let home = self.storage().map_err(|e| sqlx::Error::Decode(e.into()))?;
        let storage = candidate
            .storage()
            .map_err(|e| sqlx::Error::Decode(e.into()))?;
        let home = (storage != home).then_some(home);
        match (&self, &mut candidate) {
            (Self::Chunk(a), Self::Chunk(b)) => {
                b.home_storage_id = home;
                b.state.clone_from(&a.state);
            }
            (Self::Pack(a), Self::Pack(b)) => {
                b.home_storage_id = home;
                b.state.clone_from(&a.state);
            }
            (Self::Raw(a), Self::Raw(b)) => {
                b.state.clone_from(&a.state);
                b.home_storage_id = home;
                b.revision = a.revision;
                b.quarantine_code.clone_from(&a.quarantine_code);
            }
            _ => unreachable!(),
        }
        Ok(candidate)
    }
}

type Material = sqlx::Result<Option<sqlx::types::Json<PhysicalSource>>>;

#[cfg(test)]
mod tier_material_tests {
    use super::*;

    #[test]
    fn relocating_raw_preserves_quarantine_and_rejects_other_content() {
        let raw = super::super::raw::RawSource {
            id: 1i64.into(),
            location_id: Uuid::from_u128(1),
            home_storage_id: None,
            address: None,
            storage_id: Uuid::from_u128(2),
            size: 13,
            digest: Some(vec![7; 16]),
            state: "ready".into(),
            medium: "shared_s3".into(),
            pending_upload: false,
            revision: 2,
            quarantine_code: None,
        };
        let mut remote = PhysicalSource::Raw(raw.clone());
        remote.relocate(Uuid::from_u128(3), Uuid::from_u128(4), false);
        let PhysicalSource::Raw(cold) = &mut remote else {
            unreachable!()
        };
        assert_eq!(cold.home_storage(), raw.storage_id);
        cold.home_storage_id = None; // Previously stored tier material had no ownership field.
        let mut current = raw;
        current.state = "quarantined".into();
        current.revision = 3;
        current.quarantine_code = Some("hash_mismatch".into());
        let routed = PhysicalSource::Raw(current.clone())
            .bind_location(remote.clone())
            .unwrap();
        let PhysicalSource::Raw(routed) = routed else {
            panic!("expected raw")
        };
        assert_eq!(routed.address(), Uuid::from_u128(4));
        assert_eq!(routed.home_storage(), Uuid::from_u128(2));
        assert_eq!(routed.storage_id, Uuid::from_u128(3));
        assert_eq!(
            (routed.revision, routed.quarantine_code),
            (3, current.quarantine_code.clone())
        );
        let mut hot = remote.clone();
        hot.relocate(current.storage_id, Uuid::from_u128(5), false);
        let PhysicalSource::Raw(hot) = PhysicalSource::Raw(current.clone())
            .bind_location(hot)
            .unwrap()
        else {
            unreachable!()
        };
        assert_eq!(hot.home_storage_id, None);
        assert_eq!(hot.address(), Uuid::from_u128(5));
        current.digest = Some(vec![8; 16]);
        assert!(PhysicalSource::Raw(current).bind_location(remote).is_err());
    }
}
macro_rules! routed {
    ($name:ident, $variant:ident, $ty:ty) => {
        pub(crate) fn $name(base: $ty, material: Material) -> sqlx::Result<$ty> {
            let next = match material {
                Ok(Some(value)) => value.0,
                Ok(None) | Err(sqlx::Error::ColumnNotFound(_)) => return Ok(base),
                Err(error) => return Err(error),
            };
            match PhysicalSource::$variant(base).bind_location(next)? {
                PhysicalSource::$variant(value) => Ok(value),
                _ => Err(sqlx::Error::Decode("tier source type changed".into())),
            }
        }
    };
}
routed!(read_chunk, Chunk, Chunk);
routed!(read_pack, Pack, Pack);
routed!(read_raw, Raw, super::raw::RawSource);

#[async_trait]
pub(crate) trait PhysicalStore: Send + Sync {
    async fn has_chunks(&self) -> Result<bool>;
    async fn write_backlog(&self) -> Result<Vec<(Uuid, chrono::DateTime<chrono::Utc>)>>;
    async fn indexed(&self, location: Uuid) -> Result<bool>;

    async fn upload_pins(&self, after: Id, owner: Option<Uuid>) -> Result<Vec<StagedUpload>>;
    async fn pending_source(&self, chunk: Id) -> Result<Option<(Uuid, String)>>;
    async fn pending_only(&self, chunk: Id) -> Result<bool>;
    async fn register_encoding(&self, chunk: &Chunk) -> Result<()>;
    async fn recover_memory(&self, boot: Uuid) -> Result<()>;
    async fn buffer_chunk(&self, chunk: &Chunk, boot: Option<Uuid>) -> Result<()>;
    async fn buffered_chunks(
        &self,
        after: Id,
        before: Option<chrono::DateTime<chrono::Utc>>,
        stream: Option<Id>,
        chunk: Option<Id>,
        files_only: bool,
        urgent: &[Uuid],
    ) -> Result<Vec<Chunk>>;
    async fn synced_chunk(&self, chunk: &Chunk, failure: Option<&str>) -> Result<()>;
    async fn buffered_status(&self) -> Result<serde_json::Value>;
    async fn source(&self, chunk: Id, inspection: bool) -> Result<PhysicalSource>;
    async fn source_snapshot(
        &self,
        scope: &crate::model::source::SourceScope,
    ) -> Result<crate::model::source::SourceSnapshot>;
    async fn quarantines(
        &self,
        storage: Uuid,
        packed: bool,
        after: Id,
        limit: i64,
    ) -> Result<Vec<crate::model::source::QuarantinedSource>>;
    async fn source_update(&self, update: crate::model::source::SourceUpdate<'_>) -> Result<()>;
    async fn pack_chunks(&self, pack: Id) -> Result<Vec<Chunk>>;
    async fn pack_contains(&self, pack: Id, chunk: Id) -> Result<bool>;
    async fn inspection(&self, task: Uuid, pack: Id) -> Result<Option<Option<String>>>;
    async fn inspection_members(&self, pack: Id) -> Result<Vec<(Chunk, i64)>>;
    async fn record_inspection(&self, task: Uuid, pack: Id, code: Option<&str>) -> Result<()>;
}
