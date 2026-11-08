use crate::{
    content::{
        codec::{self, KeySource},
        compression::Pool,
        pack,
    },
    model::{
        PhysicalSource,
        tiering::{Outcome, Progress, Work},
    },
};
use anyhow::{Context, Result, ensure};
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, stream::BoxStream};
use md5::{Digest, Md5};
use object_store::{
    GetOptions, ObjectStore, ObjectStoreExt, multipart::MultipartStore, path::Path,
};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

const PART: u64 = Progress::PART_BYTES;

pub(crate) struct Site<'a> {
    backend: Arc<dyn ObjectStore>,
    path: Path,
    streaming: &'a super::s3::Streaming,
    multipart: &'a dyn MultipartStore,
}

impl<'a> Site<'a> {
    pub(crate) fn new(
        value: &PhysicalSource,
        backend: Arc<dyn ObjectStore>,
        streaming: Option<&'a super::s3::Streaming>,
        multipart: Option<&'a dyn MultipartStore>,
    ) -> Result<Self> {
        let address = value.address();
        ensure!(!address.is_nil(), "missing tier address");
        let path = match value {
            PhysicalSource::Chunk(_) => crate::storage::location::Layout::S3.chunk(address),
            PhysicalSource::Pack(_) => crate::storage::location::Layout::S3.pack(address),
            PhysicalSource::Raw(_) => super::raw::remote_path(address),
        };
        Ok(Self {
            backend,
            path,
            streaming: streaming.context("tiering requires an S3 backend")?,
            multipart: multipart.context("missing tier multipart connection")?,
        })
    }

    fn read(&self, offset: u64, length: u64) -> BoxStream<'_, Result<Bytes>> {
        Box::pin(async_stream::try_stream! {
            let end=offset.checked_add(length).context("tier read overflow")?;
            let options=GetOptions{range:(length!=0).then_some((offset..end).into()),..Default::default()};
            let result=self.backend.get_opts(&self.path,options).await?;
            ensure_range(result.range.start,result.range.end,offset,end)?;
            let mut input=result.into_stream();let mut count=0u64;
            while let Some(bytes)=input.next().await{
                let bytes=bytes?;count=count.checked_add(bytes.len() as u64).context("tier read overflow")?;
                if count>length{Err(codec::IntegrityError::Length)?}
                yield bytes;
            }
            if count!=length {Err(codec::IntegrityError::Length)?}
        })
    }
    async fn size(&self) -> Result<Option<u64>> {
        match self.backend.head(&self.path).await {
            Ok(meta) => Ok(Some(meta.size)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
    async fn bounded(&self, offset: u64, size: u64, limit: u64) -> Result<Bytes> {
        ensure!(size <= limit, codec::IntegrityError::Length);
        let mut buf = BytesMut::with_capacity(usize::try_from(size)?);
        let mut input = self.read(offset, size);
        while let Some(bytes) = input.next().await {
            buf.extend_from_slice(&bytes?);
        }
        Ok(buf.freeze())
    }
    async fn digests(&self, offset: u64, size: u64) -> Result<([u8; 16], [u8; 32])> {
        let mut md5 = Md5::new();
        let mut digest = blake3::Hasher::new();
        let mut input = self.read(offset, size);
        while let Some(bytes) = input.next().await {
            let bytes = bytes?;
            md5.update(&bytes);
            digest.update(&bytes);
        }
        Ok((md5.finalize().into(), *digest.finalize().as_bytes()))
    }
    async fn remove(&self) -> Result<()> {
        let (pending, more) = self.streaming.pending_multipart(&self.path).await?;
        ensure!(!more, "too many tier uploads");
        for id in pending {
            self.multipart.abort_multipart(&self.path, &id).await?;
        }
        match self.backend.delete(&self.path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

fn ensure_range(a: u64, b: u64, start: u64, end: u64) -> Result<()> {
    ensure!((a, b) == (start, end), codec::IntegrityError::Length);
    Ok(())
}

fn encoded(
    source: &PhysicalSource,
    target: PhysicalSource,
    bytes: Vec<u8>,
    keys: &dyn KeySource,
    pool: &Pool,
    recode: bool,
) -> Result<(PhysicalSource, Vec<u8>)> {
    match (source, target) {
        (PhysicalSource::Chunk(a), PhysicalSource::Chunk(mut b)) => {
            let (raw, _) = codec::decode(a, bytes.clone(), keys, 0, pool)?;
            if !recode {
                return Ok((PhysicalSource::Chunk(b), bytes));
            }
            let plain = if b.compressed {
                codec::open(a, bytes, keys)?
            } else {
                raw.to_vec()
            };
            b.stored_size = Some(i32::try_from(
                plain.len() + if b.algorithm == "none" { 0 } else { 16 },
            )?);
            let data = codec::seal(&b, plain, keys)?;
            Ok((PhysicalSource::Chunk(b), data))
        }
        (PhysicalSource::Pack(a), PhysicalSource::Pack(mut b)) => {
            let old = &a
                .tier_index
                .as_ref()
                .context("missing tier source members")?
                .entries;
            let new = b
                .tier_index
                .as_ref()
                .context("missing tier destination members")?
                .entries
                .clone();
            let data = if recode {
                pack::frame::recode(a, &mut b, &bytes, old, &new, keys, pool)?
            } else {
                pack::decode_index(a, bytes.clone(), old, keys, pool, true)?;
                bytes
            };
            Ok((PhysicalSource::Pack(b), data))
        }
        _ => anyhow::bail!("invalid encoded tier source"),
    }
}

async fn pace(start: Instant, bytes: u64, rate: u64) {
    tokio::time::sleep_until(start + Duration::from_secs_f64(bytes as f64 / rate.max(1) as f64))
        .await
}

async fn raw(
    source: &Site<'_>,
    target: &Site<'_>,
    from: &crate::model::raw::RawSource,
    to: &PhysicalSource,
    progress: &Progress,
    rate: u64,
) -> Result<Outcome> {
    from.validate()?;
    let size = u64::try_from(from.size)?;
    let md5: &[u8] = from.digest.as_deref().context("raw digest missing")?;
    let start = Instant::now();
    ensure!(
        source.size().await? == Some(size),
        codec::IntegrityError::Length
    );
    if let Some(stored) = target.size().await? {
        if stored != size {
            return Ok(Outcome::Discard);
        }
        let (a, b) = tokio::try_join!(source.digests(0, size), target.digests(0, size))?;
        ensure!(a.0.as_slice() == md5, codec::IntegrityError::Hash);
        if a != b {
            return Ok(Outcome::Discard);
        }
        return Ok(Outcome::Verified {
            target: to.clone(),
            digest: b.1,
        });
    }
    let part = Progress::raw_part_size(size);
    let count = u16::try_from(size.div_ceil(part))?;
    if size <= PART {
        let bytes = source.bounded(0, size, PART).await?;
        ensure!(
            Md5::digest(&bytes).as_slice() == md5,
            codec::IntegrityError::Hash
        );
        pace(start, size, rate).await;
        target
            .backend
            .put(&target.path, bytes.clone().into())
            .await?;
        let digest = *blake3::hash(&bytes).as_bytes();
        let received = target.digests(0, size).await?;
        if received != (md5.try_into()?, digest) {
            return Ok(Outcome::Discard);
        }
        return Ok(Outcome::Verified {
            target: to.clone(),
            digest,
        });
    }
    ensure!(
        progress.parts <= count && part <= 5 * 1024 * 1024 * 1024,
        "invalid tier checkpoint"
    );
    let streaming = target.streaming;
    let parts = target.multipart;
    let Some(upload) = &progress.upload else {
        let (mut existing, more) = streaming.pending_multipart(&target.path).await?;
        ensure!(!more, "too many tier uploads");
        existing.sort();
        let upload = if let Some(id) = existing.first() {
            id.clone()
        } else {
            parts.create_multipart(&target.path).await?
        };
        return Ok(Outcome::Checkpoint {
            progress: Progress {
                upload: Some(upload),
                parts: 0,
            },
        });
    };
    if progress.parts < count {
        let offset = u64::from(progress.parts) * part;
        let length = part.min(size - offset);
        let Some(page) = streaming
            .parts(&target.path, upload, progress.parts)
            .await?
        else {
            return Ok(Outcome::Checkpoint {
                progress: Progress::default(),
            });
        };
        let (hash, buffered) = if length <= PART {
            let bytes = source.bounded(offset, length, PART).await?;
            let hash: [u8; 16] = Md5::digest(&bytes).into();
            (hash, Some(bytes))
        } else {
            (source.digests(offset, length).await?.0, None)
        };
        pace(start, length, rate).await;
        if !page
            .items
            .iter()
            .any(|p| p.number == progress.parts + 1 && p.matches(length, &hash))
        {
            streaming
                .part(
                    &target.path,
                    upload,
                    usize::from(progress.parts),
                    length,
                    &hash,
                    || match &buffered {
                        Some(bytes) => Box::pin(futures_util::stream::once(std::future::ready(
                            Ok(bytes.clone()),
                        ))),
                        None => source.read(offset, length),
                    },
                )
                .await?;
        }
        return Ok(Outcome::Checkpoint {
            progress: Progress {
                upload: Some(upload.clone()),
                parts: progress.parts + 1,
            },
        });
    }
    let mut complete = Vec::with_capacity(usize::from(count));
    let mut after = 0;
    loop {
        let Some(page) = streaming.parts(&target.path, upload, after).await? else {
            return Ok(Outcome::Checkpoint {
                progress: Progress::default(),
            });
        };
        for p in page.items {
            if complete.len() >= usize::from(count)
                || usize::from(p.number) != complete.len() + 1
                || p.size != part.min(size - part * (u64::from(p.number) - 1))
            {
                return Ok(Outcome::Discard);
            }
            complete.push((i32::from(p.number), p.etag));
        }
        if let Some(next) = page.next {
            if next <= after {
                return Ok(Outcome::Discard);
            }
            after = next
        } else {
            break;
        }
    }
    if complete.len() != usize::from(count) {
        return Ok(Outcome::Discard);
    }
    streaming.complete(&target.path, upload, &complete).await?;
    let (a, b) = tokio::try_join!(source.digests(0, size), target.digests(0, size))?;
    ensure!(a.0.as_slice() == md5, codec::IntegrityError::Hash);
    if a != b {
        return Ok(Outcome::Discard);
    }
    Ok(Outcome::Verified {
        target: to.clone(),
        digest: b.1,
    })
}

pub(crate) async fn execute(
    work: &Work,
    source: Site<'_>,
    target: Option<Site<'_>>,
    keys: Arc<dyn KeySource>,
    pool: Arc<Pool>,
    cpu: Arc<Semaphore>,
    memory: Arc<OwnedSemaphorePermit>,
) -> Result<Outcome> {
    let Work::Copy {
        source: original,
        target: planned,
        rate,
        recode,
        progress,
    } = work
    else {
        source.remove().await?;
        return Ok(Outcome::Removed);
    };
    let destination = target.context("missing tier target")?;
    ensure!(
        original.same_content(planned)
            && original.address() != planned.address()
            && original.storage()? != planned.storage()?,
        "invalid tier source and target"
    );
    if let PhysicalSource::Raw(from) = original {
        return raw(&source, &destination, from, planned, progress, *rate).await;
    }
    let limit = match original {
        PhysicalSource::Chunk(c) => {
            codec::validate_metadata(c, keys.as_ref())?;
            codec::MAX_FILE
        }
        PhysicalSource::Pack(p) => {
            p.validate(keys.as_ref())?;
            p.payload_size()? + 16
        }
        _ => unreachable!(),
    };
    let started = Instant::now();
    let size = u64::try_from(original.bytes()?)?;
    ensure!(
        source.size().await? == Some(size),
        codec::IntegrityError::Length
    );
    let bytes = source.bounded(0, size, limit as u64).await?;
    let src = original.clone();
    let out = planned.clone();
    let recode = *recode;
    let permit = cpu.acquire_owned().await?;
    let (out, data, _memory) = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let (out, data) = encoded(&src, out, bytes.to_vec(), keys.as_ref(), &pool, recode)?;
        Ok::<_, anyhow::Error>((out, data, memory))
    })
    .await??;
    let digest = *blake3::hash(&data).as_bytes();
    let length = data.len() as u64;
    if let Some(stored) = destination.size().await? {
        if stored != length {
            return Ok(Outcome::Discard);
        }
    } else {
        pace(started, length, *rate).await;
        destination
            .backend
            .put(&destination.path, Bytes::from(data).into())
            .await?;
    }
    if destination.size().await? != Some(length)
        || destination.digests(0, length).await?.1 != digest
    {
        return Ok(Outcome::Discard);
    }
    Ok(Outcome::Verified {
        target: out,
        digest,
    })
}

impl super::Storage {
    pub(crate) async fn tier_work(&self, work: &Work) -> Result<Outcome> {
        for material in std::iter::once(work.source()).chain(work.target()) {
            for id in material.block_keys() {
                self.ensure_key_loaded(Some(id)).await?;
            }
        }
        let source = self.store_backend(work.source().storage()?)?;
        self.verify_raw_target(&source).await?;
        let target = work
            .target()
            .map(|v| self.store_backend(v.storage()?))
            .transpose()?;
        if let Some(store) = &target {
            self.verify_raw_target(store).await?;
        }
        let mut stores = vec![&source];
        stores.extend(target.as_ref());
        stores.sort_by_key(|s| s.profile.storage_id);
        let mut permits = Vec::new();
        for store in stores {
            permits.push(store.migration.acquire().await?);
        }
        fn site<'a>(s: &'a super::StoreBackend, v: &PhysicalSource) -> Result<Site<'a>> {
            Site::new(
                v,
                s.backend.clone(),
                s.streaming.as_ref(),
                s.multipart.as_deref(),
            )
        }
        let memory = self.pack_work(work.memory()?).await?;
        execute(
            work,
            site(&source, work.source())?,
            target
                .as_ref()
                .zip(work.target())
                .map(|(s, v)| site(s, v))
                .transpose()?,
            self.secrets.clone(),
            self.compression.clone(),
            self.cpu.clone(),
            memory,
        )
        .await
    }
}
