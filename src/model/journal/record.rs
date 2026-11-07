use crate::{
    content::{codec::Chunk, pack::Pack},
    model::{
        node_read::{Segment, Source},
        raw::RawSource,
    },
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Kind {
    Chunk,
    Member,
    Pack,
    Raw,
    Fragment,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct Reference {
    pub kind: Kind,
    pub id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<Uuid>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Material {
    Chunk {
        chunk: Chunk,
    },
    Member {
        pack: Uuid,
        chunk: Chunk,
    },
    Pack {
        pack: Pack,
    },
    Raw {
        source: RawSource,
    },
    Fragment {
        storage: Uuid,
        id: Uuid,
        key: String,
        size: i64,
        hash: Vec<u8>,
    },
}

pub(crate) struct Hold {
    pub unit: &'static str,
    pub storage: Uuid,
    pub address: Uuid,
    pub key: Option<Uuid>,
}

impl Material {
    pub fn reference(&self) -> Reference {
        let (kind, id, member) = match self {
            Self::Chunk { chunk } => (Kind::Chunk, chunk.address(), None),
            Self::Member { pack, chunk } => (Kind::Member, *pack, Some(chunk.uuid)),
            Self::Pack { pack } => (Kind::Pack, pack.address(), None),
            Self::Raw { source } => (Kind::Raw, source.address(), None),
            Self::Fragment { id, .. } => (Kind::Fragment, *id, None),
        };
        Reference { kind, id, member }
    }

    pub fn hold(&self) -> Result<Option<Hold>> {
        let (unit, storage, address, key) = match self {
            Self::Chunk { chunk } => (
                "chunk",
                chunk.storage_id.context("chunk storage missing")?,
                chunk.address(),
                chunk.block_key_id,
            ),
            Self::Pack { pack } => (
                "pack",
                pack.storage_id.context("pack storage missing")?,
                pack.address(),
                pack.block_key_id,
            ),
            Self::Raw { source } => {
                source.validate()?;
                ensure!(
                    source.medium == "shared_s3" && !source.pending_upload,
                    "journal raw source is pending"
                );
                ("raw", source.storage_id, source.address(), None)
            }
            Self::Fragment {
                storage,
                id,
                key,
                size,
                hash,
            } => {
                ensure!(
                    *size > 0 && hash.len() == 32 && key == &crate::storage::fragment_key(*id),
                    "invalid journal fragment"
                );
                ("fragment", *storage, *id, None)
            }
            Self::Member { .. } => return Ok(None),
        };
        ensure!(
            !storage.is_nil() && !address.is_nil(),
            "invalid journal source identity"
        );
        Ok(Some(Hold {
            unit,
            storage,
            address,
            key,
        }))
    }

    pub fn size(&self) -> i64 {
        match self {
            Self::Chunk { chunk } | Self::Member { chunk, .. } => i64::from(chunk.raw_size),
            Self::Pack { pack } => pack.raw_size,
            Self::Raw { source } => source.size,
            Self::Fragment { size, .. } => *size,
        }
    }
}

#[derive(Clone)]
pub(crate) struct BlockKey {
    pub id: Uuid,
    pub name: String,
    pub algorithm: String,
    pub material: [u8; 32],
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct WrappedKey {
    pub id: Uuid,
    pub name: String,
    pub algorithm: String,
    wrapped_material: String,
}

impl WrappedKey {
    fn binding(
        format: &crate::storage::journal::Format<'_>,
        id: Uuid,
        algorithm: &str,
    ) -> Result<(crate::content::codec::Key, Vec<u8>)> {
        let code = match algorithm {
            "aes-256-gcm" => 1,
            "chacha20-poly1305" => 2,
            _ => anyhow::bail!("unknown journal block key algorithm"),
        };
        ensure!(!id.is_nil(), "invalid journal block key identity");
        let derived = blake3::derive_key("mokyu/journal/06/block-key", &format.key.material);
        let mut aad = b"mokyu/journal/06/block-key".to_vec();
        for id in [format.storage, format.journal, id] {
            aad.extend_from_slice(id.as_bytes());
        }
        aad.push(code);
        Ok((
            crate::content::codec::Key::new("aes-256-gcm", derived)?,
            aad,
        ))
    }

    pub fn seal(key: &BlockKey, format: &crate::storage::journal::Format<'_>) -> Result<Self> {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        let (cipher, aad) = Self::binding(format, key.id, &key.algorithm)?;
        let wrapped = crate::content::codec::protect(&key.material, &cipher, &aad)?;
        Ok(Self {
            id: key.id,
            name: key.name.clone(),
            algorithm: key.algorithm.clone(),
            wrapped_material: URL_SAFE_NO_PAD.encode(wrapped),
        })
    }

    pub fn open(&self, format: &crate::storage::journal::Format<'_>) -> Result<BlockKey> {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        ensure!(
            self.wrapped_material.len() == 80 && !self.name.is_empty() && self.name.len() <= 128,
            "invalid journal block key record"
        );
        let (cipher, aad) = Self::binding(format, self.id, &self.algorithm)?;
        let wrapped = URL_SAFE_NO_PAD.decode(&self.wrapped_material)?;
        let material = crate::content::codec::unprotect_bytes(&wrapped, &cipher, &aad)?
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid journal block key length"))?;
        Ok(BlockKey {
            id: self.id,
            name: self.name.clone(),
            algorithm: self.algorithm.clone(),
            material,
        })
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct Span {
    pub material: Reference,
    pub offset: i64,
    pub source_offset: i64,
    pub length: i64,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct Attributes {
    pub size: i64,
    pub etag: String,
    pub metadata: Value,
    pub checksums: Value,
    pub payload: String,
    pub modified_us: i64,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Operation {
    Start,
    Ready,
    Put,
    Delete,
    Pause,
    Resume,
    Drop,
}

impl Operation {
    pub fn parse(value: &str) -> Result<Self> {
        Ok(serde_json::from_value(Value::String(value.to_owned()))?)
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct Change {
    pub bucket: Uuid,
    pub name: String,
    pub generation: i64,
    pub seq: i64,
    pub operation: Operation,
    pub key: Option<String>,
    pub attributes: Option<Attributes>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ranges: Vec<Span>,
}

impl Change {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.bucket.is_nil()
                && self.generation > 0
                && self.seq > 0
                && s3s::path::check_bucket_name(&self.name),
            "invalid journal bucket or sequence"
        );
        ensure!(
            self.key
                .as_ref()
                .is_none_or(|v| !v.is_empty() && v.len() <= 1024)
                && self.key.is_some()
                    == matches!(self.operation, Operation::Put | Operation::Delete),
            "invalid journal object key"
        );
        ensure!(
            self.attributes.is_some() == (self.operation == Operation::Put),
            "invalid journal attributes"
        );
        let mut offset = 0i64;
        for range in &self.ranges {
            ensure!(
                range.offset == offset
                    && range.length > 0
                    && range.source_offset >= 0
                    && range.source_offset.checked_add(range.length).is_some()
                    && !range.material.id.is_nil(),
                "invalid journal extent"
            );
            offset = offset
                .checked_add(range.length)
                .context("journal object length overflow")?;
        }
        ensure!(
            offset == self.attributes.as_ref().map_or(0, |v| v.size),
            "incomplete journal object"
        );
        Ok(())
    }
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct Prepared {
    pub change: Change,
    pub materials: Vec<Material>,
    pub keys: Vec<BlockKey>,
}

pub(crate) fn add_segment(
    storage: Uuid,
    segment: Segment,
    spans: &mut Vec<Span>,
    materials: &mut BTreeMap<Reference, Material>,
) -> Result<()> {
    let source = match segment.source {
        Source::Chunk { mut chunk } => {
            chunk.id = chunk.uuid.into();
            chunk.encoding_id = chunk.location_id.into();
            Material::Chunk { chunk }
        }
        Source::Pack {
            mut pack,
            mut chunk,
        } => {
            pack.id = pack.location_id.into();
            chunk.id = chunk.uuid.into();
            chunk.encoding_id = chunk.uuid.into();
            let id = pack.address();
            ensure!(
                pack.storage_id == Some(storage),
                "journal pack is on another storage"
            );
            let material = Material::Pack { pack };
            materials.entry(material.reference()).or_insert(material);
            Material::Member { pack: id, chunk }
        }
        Source::Raw { mut source } => {
            source.id = source.location_id.into();
            source.revision = 1;
            Material::Raw { source }
        }
        Source::Staged {
            fragment,
            storage,
            key,
            size,
            hash,
        } => Material::Fragment {
            storage,
            id: fragment,
            key,
            size,
            hash,
        },
        Source::Local { .. } | Source::Fragment { .. } => {
            anyhow::bail!("journal source is pending")
        }
    };
    if let Some(hold) = source.hold()? {
        ensure!(
            hold.storage == storage,
            "journal source is on another storage"
        );
    }
    ensure!(
        segment.source_offset >= 0
            && segment.length > 0
            && segment
                .source_offset
                .checked_add(segment.length)
                .is_some_and(|end| end <= source.size()),
        "journal source extent exceeds material"
    );
    let reference = source.reference();
    materials.entry(reference).or_insert(source);
    spans.push(Span {
        material: reference,
        offset: segment.offset,
        source_offset: segment.source_offset,
        length: segment.length,
    });
    Ok(())
}
