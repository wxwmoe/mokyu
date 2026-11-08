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

enum CarrierDigest {
    Raw(Md5),
    Pack(Box<blake3::Hasher>),
}
impl CarrierDigest {
    fn new(packed: bool) -> Self {
        if packed {
            Self::Pack(Box::new(blake3::Hasher::new()))
        } else {
            Self::Raw(Md5::new())
        }
    }
    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Raw(h) => h.update(bytes),
            Self::Pack(h) => {
                h.update(bytes);
            }
        }
    }
    fn matches(self, expected: &[u8]) -> bool {
        match self {
            Self::Raw(h) => h.finalize().as_slice() == expected,
            Self::Pack(h) => h.finalize().as_bytes() == expected,
        }
    }
}
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
    pub client: Option<Client>,
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
        let backend = self.app.storage.store_backend(self.target)?;
        let _space = self.app.storage.local_space(&backend, size as u64)?;
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

    async fn copy_carrier(
        &self,
        from: &str,
        to: &str,
        size: u64,
        expected: &[u8],
        mut pages: Option<&mut crate::storage::raw_manifest::Spool<'_>>,
        member: Option<std::ops::Range<u64>>,
    ) -> Result<Vec<u8>> {
        let packed = pages.is_none();
        let mut hasher = CarrierDigest::new(packed);
        let mut header = Vec::new();
        let mut selected = Vec::new();
        let object = self.origin.get(&Path::from(from)).await?;
        ensure!(
            object.meta.size == size,
            "recovered carrier length mismatch"
        );
        let copy = !(self.same && from == to);
        let existed = if copy {
            match self.destination.head(&Path::from(to)).await {
                Ok(m) => {
                    ensure!(m.size == size, "conflicting recovery destination");
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
        let backend = self.app.storage.store_backend(self.target)?;
        let _space = if writing {
            self.app.storage.local_space(&backend, size)?
        } else {
            None
        };
        let local_paths = if copy && backend.is_local {
            let root = backend
                .raw_root
                .as_ref()
                .and_then(|p| p.parent())
                .context("missing local backend root")?;
            let directory = if packed { "packs" } else { "objects" };
            ensure!(
                to.starts_with(&format!("{directory}/")),
                "invalid recovery path"
            );
            let root = root.join(directory);
            let dest = root.parent().unwrap().join(to);
            let parent = dest.parent().context("missing raw directory")?;
            crate::storage::raw::directory(&root).await?;
            crate::storage::raw::directory(parent).await?;
            Some((
                dest.with_extension(format!("{}.recover", self.job.simple())),
                dest,
            ))
        } else {
            None
        };
        let mut local = if let Some((temp, _)) = local_paths.as_ref().filter(|_| writing) {
            let mut options = tokio::fs::OpenOptions::new();
            options.write(true).create(true).truncate(false);
            #[cfg(unix)]
            options.custom_flags(libc::O_NOFOLLOW);
            let file = options.open(&temp).await?.into_std().await;
            file.try_lock()?;
            #[cfg(unix)]
            ensure!(
                std::os::unix::fs::MetadataExt::nlink(&file.metadata()?) == 1,
                "aliased recovery temporary file"
            );
            file.set_len(0)?;
            Some(tokio::fs::File::from_std(file))
        } else {
            None
        };
        let client = self.client.as_ref();
        let upload = if writing && local.is_none() && size > part_bytes as u64 {
            Some(
                client
                    .context("missing S3 recovery client")?
                    .start_upload(to, &BTreeMap::new(), None)
                    .await?,
            )
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
                if let Some(pages) = pages.as_deref_mut() {
                    pages.feed(&bytes).await?;
                }
                if packed && header.len() < 8 {
                    let n = bytes.len().min(8 - header.len());
                    header.extend_from_slice(&bytes[..n]);
                }
                if let Some(span) = &member {
                    let start = read - bytes.len() as u64;
                    let a = start.max(span.start);
                    let b = read.min(span.end);
                    if a < b {
                        selected
                            .extend_from_slice(&bytes[(a - start) as usize..(b - start) as usize]);
                    }
                }
                hasher.update(&bytes);
                if let Some(out) = &mut local {
                    tokio::io::AsyncWriteExt::write_all(out, &bytes).await?;
                } else if writing {
                    buffer.extend_from_slice(&bytes);
                    if let Some(upload) = &upload {
                        while buffer.len() >= part_bytes {
                            let data = buffer.split_to(part_bytes).freeze();
                            parts.push(
                                client
                                    .context("missing S3 recovery client")?
                                    .upload_part(to, upload, u16::try_from(parts.len() + 1)?, data)
                                    .await?,
                            );
                        }
                    }
                }
            }
            ensure!(
                read == size
                    && hasher.matches(expected)
                    && (!packed || header.as_slice() == pack::frame::HEADER),
                "recovered carrier digest mismatch"
            );
            if let Some(upload) = &upload {
                if !buffer.is_empty() {
                    parts.push(
                        client
                            .context("missing S3 recovery client")?
                            .upload_part(
                                to,
                                upload,
                                u16::try_from(parts.len() + 1)?,
                                buffer.freeze(),
                            )
                            .await?,
                    )
                }
                client
                    .context("missing S3 recovery client")?
                    .complete_upload(to, upload, &parts, &Condition::Absent)
                    .await?;
            } else if let Some(out) = &mut local {
                let (temp, destination) = local_paths.as_ref().context("missing local raw path")?;
                tokio::io::AsyncWriteExt::flush(out).await?;
                out.sync_all().await?;
                tokio::fs::hard_link(temp, destination).await?;
            } else if writing {
                self.put(to, buffer.to_vec()).await?;
            }
            if existed {
                let mut hash = CarrierDigest::new(packed);
                let mut chunks = self.destination.get(&Path::from(to)).await?.into_stream();
                while let Some(part) = chunks.try_next().await? {
                    hash.update(&part);
                }
                ensure!(
                    hash.matches(expected),
                    "destination carrier digest mismatch"
                );
            }
            if let Some((temp, dest)) = &local_paths {
                match tokio::fs::remove_file(temp).await {
                    Ok(()) => (),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                    Err(e) => return Err(e.into()),
                }
                crate::storage::raw::sync_directory(dest.parent().unwrap()).await?;
            }
            Ok(())
        }
        .await;
        if result.is_err()
            && let Some(upload) = &upload
        {
            let _ = client
                .context("missing S3 recovery client")?
                .abort_upload(to, upload)
                .await;
        }
        result?;
        ensure!(
            member
                .as_ref()
                .is_none_or(|s| s.end - s.start == selected.len() as u64),
            "incomplete recovered member"
        );
        Ok(selected)
    }
    async fn raw(
        &mut self,
        mat: &Material,
        source: &crate::model::raw::RawSource,
        from: &str,
        to: &str,
    ) -> Result<()> {
        let expected = source.digest.as_deref().context("missing raw digest")?;
        let mut pages =
            crate::storage::raw_manifest::Spool::new(&self.app.config.instance.data_path);
        self.copy_carrier(
            from,
            to,
            u64::try_from(source.size)?,
            expected,
            Some(&mut pages),
            None,
        )
        .await?;
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
                let size = chunk.file_size()? as u64;
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
                let member = mat.pack_member()?.map(|(_, entry)| entry);
                let pack_ref = physical.reference();
                let (id, body) = if let Some(id) = self.locations.get(&pack_ref).copied() {
                    let member = member.as_ref().context("missing recovery member")?;
                    let span = member
                        .position
                        .range(member.raw_size, size, pack.member_count)?;
                    let body = crate::storage::read_retry::bounded(
                        self.origin.as_ref(),
                        &Path::from(from.as_str()),
                        member.position.encoding.stored_size as usize,
                        Some((span, size)),
                        None,
                    )
                    .await?;
                    (id, Some(body))
                } else {
                    let _memory = self.app.storage.pack_memory(crate::content::codec::MAX).await?;
                    let span = member
                        .as_ref()
                        .map(|m| m.position.range(m.raw_size, size, pack.member_count))
                        .transpose()?;
                    let expected = pack.digest.as_deref().context("missing pack digest")?;
                    let body = self
                        .copy_carrier(&from, &to, size, expected, None, span)
                        .await?;
                    let body = member.as_ref().map(|_| body);
                    (self.save(physical, None).await?, body)
                };
                if let Some(member) = member {
                    pack::frame::decode_entry(
                        pack,
                        &member,
                        body.context("missing recovery bytes")?,
                        &self.keys,
                        &self.app.storage.compression,
                        true,
                    )?;
                    self.save(
                        mat,
                        Some((
                            id,
                            i32::try_from(member.position.ordinal)?,
                            i64::try_from(member.position.offset)?,
                        )),
                    )
                    .await?;
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
