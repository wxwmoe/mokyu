use super::{replay::Catalog, source};
use crate::{
    app::App,
    content::{codec, pack},
    model::{
        Id,
        journal::{
            record::{Kind, Material, Reference},
            restore::Location,
        },
    },
    storage::{
        external::{Client, objects::Condition},
        location::Layout,
    },
};
use anyhow::{Context, Result, ensure};
use bytes::{Bytes, BytesMut};
use futures_util::TryStreamExt;
use md5::{Digest, Md5};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

fn role(m: &Material) -> &'static str {
    match m {
        Material::Raw { .. } => "raw",
        Material::Fragment { .. } => "fragment",
        _ => "chunk",
    }
}
pub(crate) fn path(m: &Material, origin: bool, layout: Layout) -> Result<String> {
    Ok(match m {
        Material::Chunk { chunk } => layout
            .chunk(if origin {
                chunk.address()
            } else {
                chunk.location_id
            })
            .to_string(),
        Material::Pack { pack } => layout
            .pack(if origin {
                pack.address()
            } else {
                pack.location_id
            })
            .to_string(),
        Material::Raw { source } => layout
            .object(if origin {
                source.address()
            } else {
                source.location_id
            })
            .to_string(),
        Material::Fragment { key, .. } => key.clone(),
        Material::Member { .. } => anyhow::bail!("pack member has no independent file"),
    })
}

pub(crate) struct Import<'a> {
    pub app: &'a App,
    pub job: Uuid,
    pub token: Uuid,
    pub target: Uuid,
    pub same: bool,
    pub source_layout: Layout,
    pub target_layout: Layout,
    pub origin: Arc<dyn ObjectStore>,
    pub destination: Arc<dyn ObjectStore>,
    pub client: Client,
    pub keys: BTreeMap<Uuid, codec::Key>,
    pub locations: BTreeMap<Reference, Id>,
}

impl Import<'_> {
    async fn save(&mut self, mat: &Material, pack: Option<(Id, i32, i64)>) -> Result<Id> {
        let id = self
            .app
            .journal
            .recovery_material(self.job, self.token, self.target, mat, pack)
            .await?;
        self.locations.insert(mat.reference(), id);
        Ok(id)
    }

    async fn put(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let digest = blake3::hash(&data);
        let size = data.len();
        let result = self
            .destination
            .put_opts(
                &Path::from(key),
                Bytes::from(data).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await;
        if let Err(error) = result {
            let existing = source::bytes(self.destination.as_ref(), key, size as u64)
                .await?
                .ok_or(error)?;
            ensure!(
                existing.len() == size && blake3::hash(&existing) == digest,
                "conflicting recovered file"
            );
        }
        Ok(())
    }

    async fn raw(
        &mut self,
        mat: &Material,
        source: &crate::model::raw::RawSource,
        from: &str,
        to: &str,
    ) -> Result<()> {
        let size = u64::try_from(source.size)?;
        let expected = source.digest.as_deref().context("missing raw digest")?;
        let mut pages =
            crate::storage::raw_manifest::Spool::new(&self.app.config.instance.data_path);
        let mut hasher = Md5::new();
        let object = self.origin.get(&Path::from(from)).await?;
        ensure!(object.meta.size == size, "recovered raw length mismatch");
        let copy = !(self.same && from == to);
        let existed = if copy {
            match self.destination.head(&Path::from(to)).await {
                Ok(m) => {
                    ensure!(m.size == size, "conflicting destination raw");
                    true
                }
                Err(object_store::Error::NotFound { .. }) => false,
                Err(e) => return Err(e.into()),
            }
        } else {
            false
        };
        let writing = copy && !existed;
        let part_bytes = usize::try_from(size.div_ceil(9999).max(8 * 1024 * 1024))?;
        let upload = if writing && size > part_bytes as u64 {
            Some(self.client.start_upload(to, &BTreeMap::new(), None).await?)
        } else {
            None
        };
        let result: Result<()> = async {
            let mut buffer = BytesMut::new();
            let mut parts = Vec::new();
            let mut read = 0u64;
            let mut body = object.into_stream();
            while let Some(bytes) = body.try_next().await? {
                read = read
                    .checked_add(bytes.len() as u64)
                    .context("raw size overflow")?;
                ensure!(read <= size, "raw file grew");
                pages.feed(&bytes).await?;
                hasher.update(&bytes);
                if writing {
                    buffer.extend_from_slice(&bytes);
                    if let Some(upload) = &upload {
                        while buffer.len() >= part_bytes {
                            let data = buffer.split_to(part_bytes).freeze();
                            parts.push(
                                self.client
                                    .upload_part(to, upload, u16::try_from(parts.len() + 1)?, data)
                                    .await?,
                            );
                        }
                    }
                }
            }
            ensure!(
                read == size && hasher.finalize().as_slice() == expected,
                "recovered raw digest mismatch"
            );
            if let Some(upload) = &upload {
                if !buffer.is_empty() {
                    parts.push(
                        self.client
                            .upload_part(
                                to,
                                upload,
                                u16::try_from(parts.len() + 1)?,
                                buffer.freeze(),
                            )
                            .await?,
                    )
                }
                self.client
                    .complete_upload(to, upload, &parts, &Condition::Absent)
                    .await?;
            } else if writing {
                self.put(to, buffer.to_vec()).await?;
            }
            if existed {
                let mut hash = Md5::new();
                let mut chunks = self.destination.get(&Path::from(to)).await?.into_stream();
                while let Some(part) = chunks.try_next().await? {
                    hash.update(&part);
                }
                ensure!(
                    hash.finalize().as_slice() == expected,
                    "destination raw digest mismatch"
                );
            }
            Ok(())
        }
        .await;
        if result.is_err()
            && let Some(upload) = &upload
        {
            let _ = self.client.abort_upload(to, upload).await;
        }
        result?;
        let id = self.save(mat, None).await?;
        let mut verified = pages.finish().await?;
        while let Some(group) = verified.next().await? {
            self.app.raw.put_pages(id, &group).await?;
        }
        self.app
            .raw
            .seal_pages(id, source.size, expected, None)
            .await?;
        Ok(())
    }

    pub async fn location(&mut self, catalog: &Catalog, reference: Reference) -> Result<Location> {
        let mat = catalog
            .materials
            .get(&reference)
            .context("missing journal material")?;
        let kind = role(mat);
        if let Some(id) = self.locations.get(&reference) {
            return Ok(Location { native: *id, kind });
        }
        let mut physical = mat;
        if let Material::Member { pack, .. } = mat {
            physical = catalog
                .materials
                .get(&Reference {
                    kind: Kind::Pack,
                    id: *pack,
                    member: None,
                })
                .context("missing recovery pack")?;
        }
        let from = path(physical, true, self.source_layout)?;
        let to = path(physical, false, self.target_layout)?;
        match physical {
            Material::Raw { source } => self.raw(mat, source, &from, &to).await?,
            Material::Chunk { chunk } => {
                codec::validate_metadata(chunk, &self.keys)?;
                let size = u64::try_from(chunk.stored_size.context("missing chunk size")?)?;
                let data = source::bytes(self.origin.as_ref(), &from, size)
                    .await?
                    .context("missing recovered chunk")?;
                codec::decode(
                    chunk,
                    data.clone(),
                    &self.keys,
                    0,
                    &self.app.storage.compression,
                )?;
                if !self.same || from != to {
                    self.put(&to, data).await?;
                }
                self.save(mat, None).await?;
            }
            Material::Pack { pack } => {
                pack.validate(&self.keys)?;
                let size = u64::try_from(pack.stored_size.context("missing pack size")?)?;
                let _memory = self
                    .app
                    .storage
                    .pack_memory(usize::try_from(size)?.saturating_add(pack.payload_size()?))
                    .await?;
                let data = source::bytes(self.origin.as_ref(), &from, size)
                    .await?
                    .context("missing recovered pack")?;
                let decoded = pack::decode(
                    pack,
                    data.clone(),
                    &self.keys,
                    &self.app.storage.compression,
                )?;
                if !self.same || from != to {
                    self.put(&to, data).await?;
                }
                let id = self.save(physical, None).await?;
                let mut offset = 0i64;
                for (i, member) in decoded.members.iter().enumerate() {
                    let r = Reference {
                        kind: Kind::Member,
                        id: pack.address(),
                        member: Some(member.id),
                    };
                    let from_header;
                    let material = if let Some(material) = catalog.materials.get(&r) {
                        material
                    } else {
                        from_header = Material::Member {
                            pack: pack.address(),
                            chunk: codec::Chunk {
                                id: member.id.into(),
                                uuid: member.id,
                                encoding_id: member.id.into(),
                                location_id: member.id,
                                address: None,
                                home_storage_id: None,
                                storage_id: pack.storage_id,
                                hash: member.hash.to_vec(),
                                raw_size: i32::try_from(member.len)?,
                                stored_size: None,
                                algorithm: "none".into(),
                                block_key_id: None,
                                compressed: false,
                                nonce: None,
                                format: 2,
                                state: "ready".into(),
                                created_at: pack.created_at,
                            },
                        };
                        &from_header
                    };
                    let Material::Member { chunk, .. } = material else {
                        anyhow::bail!("invalid pack member reference")
                    };
                    decoded.chunk(chunk)?;
                    self.save(material, Some((id, i32::try_from(i)?, offset)))
                        .await?;
                    offset += i64::try_from(member.len)?;
                }
            }
            Material::Fragment { size, hash, .. } => {
                ensure!(
                    *size <= crate::content::codec::MAX as i64,
                    "invalid recovery fragment size"
                );
                let data = source::bytes(self.origin.as_ref(), &from, u64::try_from(*size)?)
                    .await?
                    .context("missing recovered fragment")?;
                ensure!(
                    data.len() as i64 == *size && blake3::hash(&data).as_bytes().as_slice() == hash,
                    "recovered fragment mismatch"
                );
                if !self.same || from != to {
                    self.put(&to, data).await?;
                }
                self.save(mat, None).await?;
            }
            Material::Member { .. } => unreachable!(),
        }
        Ok(Location {
            native: *self
                .locations
                .get(&reference)
                .context("missing verified pack member")?,
            kind,
        })
    }
}
