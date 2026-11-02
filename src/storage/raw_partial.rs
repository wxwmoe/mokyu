use super::{
    Area, CacheFile, CachePin, CachePolicy,
    cache::Queue,
    file_cache::Files,
    local::allocated,
    partial_map::Map,
    raw_segments::{self, Acquire, Proof, Remote, Retain},
};
use crate::{
    content::{codec::IntegrityError, raw_pages::PAGE},
    model::raw::RawSource,
    runtime::locks::Locks,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, stream::BoxStream};
use std::{
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use uuid::Uuid;

pub(crate) trait Shelf: Send + Sync {
    fn files(&self) -> Files<'_>;
    fn epoch(&self) -> u64;
    fn fills(&self) -> &Locks<(Uuid, u64)>;
    fn promotion(&self, id: Uuid) -> Option<super::cache::CacheWork>;
}

impl Shelf for super::Storage {
    fn files(&self) -> Files<'_> {
        self.file_cache()
    }
    fn epoch(&self) -> u64 {
        self.read_epoch()
    }
    fn fills(&self) -> &Locks<(Uuid, u64)> {
        &self.partial_fills
    }
    fn promotion(&self, id: Uuid) -> Option<super::cache::CacheWork> {
        self.cache_work(id, 1)
    }
}
impl super::Storage {
    pub(crate) fn partial(self: &Arc<Self>, raw: &RawSource) -> Option<Partial> {
        Partial::new(
            self.clone(),
            raw,
            self.local_cache(Some(raw.storage_id)).ok()?,
        )
    }
}

#[derive(Clone)]
pub(crate) struct Partial {
    shelf: Arc<dyn Shelf>,
    source: RawSource,
    policy: CachePolicy,
    epoch: u64,
}
impl Partial {
    pub(crate) fn new(
        shelf: Arc<dyn Shelf>,
        source: &RawSource,
        policy: CachePolicy,
    ) -> Option<Self> {
        if !policy.enabled || source.pending_upload || source.size <= 0 {
            return None;
        }
        let epoch = shelf.epoch();
        Some(Self {
            shelf,
            source: source.clone(),
            policy,
            epoch,
        })
    }
    fn path(&self) -> PathBuf {
        CacheFile::PartialRaw.path(self.shelf.files().root, self.source.location_id)
    }
    async fn state(&self) -> Result<Option<(Map, CachePin)>> {
        if self.shelf.epoch() != self.epoch {
            return Ok(None);
        }
        let files = self.shelf.files();
        let invalid = files
            .fifo
            .lock()
            .await
            .entries
            .get(&self.source.location_id)
            .filter(|e| !e.valid && e.file == CacheFile::PartialRaw)
            .map(|e| e.pins.clone());
        if let Some(pins) = invalid {
            if let Err(error) = files.invalidate(self.source.location_id, Some(&pins)).await {
                tracing::debug!(%error,"partial cache cleanup deferred");
            }
        }
        let Some((pin, format)) = files.pin(self.source.location_id).await else {
            return Ok(None);
        };
        if format != CacheFile::PartialRaw {
            return Ok(None);
        }
        let path = self.path();
        let result = async {
            let map = Map::load(&path.with_extension("map")).await?;
            ensure!(
                map.same(&self.source)
                    && tokio::fs::metadata(&path).await?.len() >= map.high_water(),
                "partial raw source changed"
            );
            Ok::<_, anyhow::Error>(map)
        }
        .await;
        match result {
            Ok(map) => Ok(Some((map, pin))),
            Err(error) => {
                drop(pin);
                files.reject(self.source.location_id).await;
                tracing::debug!(%error,"discarded invalid partial raw cache");
                Ok(None)
            }
        }
    }

    async fn discard_failed_write(&self) -> Result<()> {
        let files = self.shelf.files();
        let id = self.source.location_id;
        let path = self.path();
        let write = files.cache_writes.lock().await;
        let mut fifo = files.fifo.lock().await;
        if fifo
            .entries
            .get(&id)
            .is_some_and(|e| e.file != CacheFile::PartialRaw)
        {
            return Ok(());
        }
        let mut used = 0;
        for name in [
            &path,
            &path.with_extension("map"),
            &path.with_extension("map.tmp"),
        ] {
            match tokio::fs::symlink_metadata(name).await {
                Ok(meta) => used += allocated(&meta),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        if !fifo.entries.contains_key(&id) {
            fifo.insert(id, 0, CacheFile::PartialRaw);
        }
        let old = fifo.resize(id, used, CacheFile::PartialRaw)?;
        if old > used {
            files.disk.release(Area::Cache, old - used);
        } else {
            files.disk.account_existing(Area::Cache, used - old);
        }
        let entry = fifo.entries.get_mut(&id).unwrap();
        entry.valid = false;
        let pins = entry.pins.clone();
        drop(fifo);
        drop(write);
        files.invalidate(id, Some(&pins)).await
    }

    async fn write(
        &self,
        start: u64,
        data: &[u8],
        verified: bool,
        proof: Option<&Proof>,
    ) -> Result<()> {
        let files = self.shelf.files();
        let id = self.source.location_id;
        let path = self.path();
        let end = start
            .checked_add(data.len() as u64)
            .context(IntegrityError::Length)?;
        ensure!(end <= self.source.size as u64, IntegrityError::Length);
        let _write = files.cache_writes.lock().await;
        if self.shelf.epoch() != self.epoch {
            tracing::debug!(source=%id, "raw fill cancelled by a cache policy change");
            return Ok(());
        }
        let pinned = files.pin(id).await;
        if pinned
            .as_ref()
            .is_some_and(|(_, f)| *f != CacheFile::PartialRaw)
        {
            return Ok(());
        }
        let old = files
            .fifo
            .lock()
            .await
            .entries
            .get(&id)
            .map_or(0, |e| e.size);
        let mut map = if pinned.is_some() {
            Map::load(&path.with_extension("map")).await?
        } else {
            Map::new(&self.source)?
        };
        ensure!(map.same(&self.source), "partial raw source changed");
        map.written.mark(start..end);
        if verified {
            ensure!(
                start.is_multiple_of(PAGE as u64)
                    && (end.is_multiple_of(PAGE as u64) || end == map.size()),
                IntegrityError::Length
            );
            map.verified
                .mark(start / PAGE as u64..end.div_ceil(PAGE as u64));
        }
        let encoded = map.encode()?;
        let growth = (end - start).div_ceil(4096) * 4096
            + 8192
            + (encoded.len() as u64).div_ceil(4096) * 4096;
        if self.policy.capacity.is_some_and(|n| growth > n) {
            return Ok(());
        }
        let mut policy = self.policy;
        policy.max_entries = policy.max_entries.saturating_add(usize::from(old > 0));
        let Some(ticket) = files.reserve(growth, policy, false, &path).await? else {
            return Ok(());
        };
        let mut file = tokio::fs::OpenOptions::new()
            .custom_flags(libc::O_NOFOLLOW)
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .await?;
        file.seek(std::io::SeekFrom::Start(start)).await?;
        file.write_all(data).await?;
        if !verified && let Some(proof) = proof {
            for page in start / PAGE as u64..end.div_ceil(PAGE as u64) {
                let at = page * PAGE as u64;
                let until = (at + PAGE as u64).min(map.size());
                if !map.verified.covers(page..page + 1)
                    && map.written.covers(at..until)
                    && proof.contains(page)
                {
                    let mut bytes = vec![0; (until - at) as usize];
                    file.seek(std::io::SeekFrom::Start(at)).await?;
                    file.read_exact(&mut bytes).await?;
                    proof.check(at, &bytes)?;
                    map.verified.mark(page..page + 1);
                }
            }
        }
        file.sync_data().await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("raw-part-data").await;
        let bytes = map.encode()?;
        let tmp = path.with_extension("map.tmp");
        let mut marker = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&tmp)
            .await?;
        marker.write_all(&bytes).await?;
        marker.sync_all().await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("raw-part-map").await;
        if self.shelf.epoch() != self.epoch {
            anyhow::bail!("raw cache policy changed");
        }
        tokio::fs::rename(&tmp, path.with_extension("map")).await?;
        let charge = allocated(&file.metadata().await?) + allocated(&marker.metadata().await?);
        let mut fifo = files.fifo.lock().await;
        if !fifo.entries.contains_key(&id) {
            fifo.insert(id, 0, CacheFile::PartialRaw);
        }
        fifo.resize(id, charge, CacheFile::PartialRaw)?;
        ticket.commit_written(charge.saturating_sub(old));
        if old > charge {
            files.disk.release(Area::Cache, old - charge);
        }
        drop(fifo);
        drop(_write);
        drop(pinned);
        if map.written.covers(0..map.size()) {
            self.schedule_promotion();
        }
        Ok(())
    }

    fn schedule_promotion(&self) {
        let Some(work) = self.shelf.promotion(self.source.location_id) else {
            return;
        };
        let cache = self.clone();
        tokio::spawn(async move {
            let _work = work;
            if let Err(error) = cache.promote().await {
                tracing::debug!(%error,"raw cache promotion skipped");
            }
        });
    }

    async fn promote(&self) -> Result<()> {
        use md5::{Digest, Md5};
        let id = self.source.location_id;
        let files = self.shelf.files();
        let Some((pin, CacheFile::PartialRaw)) = files.pin(id).await else {
            return Ok(());
        };
        let path = self.path();
        let map = Map::load(&path.with_extension("map")).await?;
        if !map.same(&self.source) || !map.written.covers(0..map.size()) {
            return Ok(());
        }
        let mut file = File::open(&path).await?;
        ensure!(
            file.metadata().await?.len() == self.source.size as u64,
            IntegrityError::Length
        );
        let mut buf = vec![0; 256 * 1024];
        let mut hash = Md5::new();
        loop {
            let n = file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hash.update(&buf[..n]);
        }
        if Some(hash.finalize().as_slice()) != self.source.digest.as_deref() {
            drop(pin);
            files.reject(id).await;
            anyhow::bail!(IntegrityError::Hash);
        }
        let _write = files.cache_writes.lock().await;
        let still = files.fifo.lock().await.entries.get(&id).is_some_and(|e| {
            e.file == CacheFile::PartialRaw && e.valid && Arc::ptr_eq(&pin.0, &e.pins)
        });
        if !still || self.shelf.epoch() != self.epoch {
            return Ok(());
        }
        #[cfg(feature = "fault-injection")]
        crate::faults::point("raw-part-promote").await;
        tokio::fs::rename(&path, CacheFile::Raw.path(files.root, id)).await?;
        tokio::fs::remove_file(path.with_extension("map")).await?;
        let size = self.source.size as u64;
        let old = files.fifo.lock().await.resize(id, size, CacheFile::Raw)?;
        files.disk.release(Area::Cache, old.saturating_sub(size));
        if old < size {
            files.disk.account_existing(Area::Cache, size - old);
        }
        Ok(())
    }
}

impl super::Fifo {
    fn resize(&mut self, id: Uuid, bytes: u64, format: CacheFile) -> Result<u64> {
        let entry = self
            .entries
            .get_mut(&id)
            .context("raw cache entry missing")?;
        let old = entry.size;
        if entry.queue == Queue::Small {
            self.small_bytes = self.small_bytes - old + bytes;
        }
        entry.size = bytes;
        entry.file = format;
        Ok(old)
    }
}

pub(crate) async fn restore_file(
    files: &Files<'_>,
    name: &str,
    path: &Path,
    max: usize,
) -> Result<bool> {
    if let Some(base) = name.strip_suffix(".map")
        && super::parse_chunk_name(base).is_some()
    {
        if !tokio::fs::try_exists(path.with_extension("part")).await? {
            tokio::fs::remove_file(path).await?;
        }
        return Ok(true);
    }
    let Some(id) = name.strip_suffix(".part").and_then(super::parse_chunk_name) else {
        return Ok(false);
    };
    if !tokio::fs::try_exists(CacheFile::Raw.path(files.root, id)).await?
        && let Some(size) = restore(path, id).await
        && files.disk.limits[Area::Cache as usize]
            .is_none_or(|n| files.disk.used()[Area::Cache as usize].saturating_add(size) <= n)
    {
        let mut fifo = files.fifo.lock().await;
        if fifo.entries.len() < max && !fifo.entries.contains_key(&id) {
            fifo.insert(id, size, CacheFile::PartialRaw);
            files.disk.account_existing(Area::Cache, size);
            return Ok(true);
        }
    }
    CacheFile::remove(path.to_owned()).await?;
    Ok(true)
}

pub(crate) async fn restore(path: &Path, id: Uuid) -> Option<u64> {
    let map = Map::load(&path.with_extension("map")).await.ok()?;
    let data = tokio::fs::symlink_metadata(path).await.ok()?;
    let marker = tokio::fs::symlink_metadata(path.with_extension("map"))
        .await
        .ok()?;
    (data.is_file()
        && marker.is_file()
        && map.id() == id
        && data.len() >= map.high_water()
        && data.len() <= map.size())
    .then(|| allocated(&data) + allocated(&marker))
}

impl Partial {
    pub(crate) fn stream(
        self,
        remote: Remote,
        requested: Range<u64>,
        strict: bool,
        proof: Option<Arc<Proof>>,
        width: usize,
        acquire: Acquire,
        retain: Retain,
        recovery: Arc<super::read_retry::Recovery>,
    ) -> BoxStream<'static, Result<(Bytes, bool)>> {
        Box::pin(async_stream::try_stream! {
            let page=PAGE as u64;let size=self.source.size as u64;
            let range=if strict {requested.start/page*page..requested.end.div_ceil(page).saturating_mul(page).min(size)}else{requested.clone()};
            let id=self.source.location_id;
            let mut pos=range.start;
            let mut checked_promotion=false;
            while pos<range.end {
                let window_end=pos.saturating_add(16*raw_segments::SEGMENT).min(range.end);
                if self.shelf.epoch()!=self.epoch {
                    let mut direct=remote.clone().stream(pos.max(requested.start)..requested.end,if strict{proof.clone()}else{None},width,acquire.clone(),retain.clone(),recovery.clone());
                    while let Some(bytes)=direct.next().await {yield (bytes?,true);}
                    break;
                }
                let state=self.state().await?;
                if !checked_promotion && let Some((map,_))=&state && map.written.covers(0..map.size()) {
                    self.schedule_promotion();
                    checked_promotion=true;
                }
                let (mut hit,mut until)=state.as_ref().map_or((false,window_end),|(map,_)|map.written.next(pos,window_end));
                let mut pinned=state.map(|(_,pin)|pin);
                let mut path=self.path();
                if pinned.is_none() && let Some((pin,CacheFile::Raw))=self.shelf.files().pin(id).await {
                    let full=CacheFile::Raw.path(self.shelf.files().root,id);
                    hit=true;until=window_end;pinned=Some(pin);path=full;
                }
                if strict {
                    if hit && until<(pos+page).min(size) {hit=false;until=(pos+page).min(size);}
                    else if hit {until=(pos+page).min(until);}
                    else {until=until.div_ceil(page).saturating_mul(page).min(size);}
                }
                if hit {
                    let end=until.min(if strict{pos+page}else{pos+256*1024});
                    let memory=retain((end-pos) as usize).await?;
                    let result=async {
                        let mut file=match File::open(&path).await {
                            Err(error) if error.kind()==std::io::ErrorKind::NotFound && path==self.path() => File::open(CacheFile::Raw.path(self.shelf.files().root,id)).await?,
                            result=>result?,
                        };file.seek(std::io::SeekFrom::Start(pos)).await?;
                        let mut data=vec![0;(end-pos) as usize];file.read_exact(&mut data).await?;
                        if strict {proof.as_ref().context("missing raw page proof")?.check(pos,&data)?;}
                        Ok::<_,anyhow::Error>(Bytes::from(data))
                    }.await;
                    if let Ok(bytes)=result {
                        let from=requested.start.saturating_sub(pos) as usize;
                        let to=requested.end.min(end).saturating_sub(pos) as usize;
                        pos=end;
                        yield (raw_segments::held(bytes.slice(from..to),memory),false);
                        continue;
                    }
                    drop(pinned);self.shelf.files().reject(id).await;
                    hit=false;
                }
                if !hit {
                    let mut locks=Vec::new();
                    let first=pos/page;
                    for n in first..until.div_ceil(page) {
                        let lock=self.shelf.fills().get((id,n));
                        match lock.clone().try_lock_owned() {
                            Ok(guard)=>locks.push((n,guard)),
                            Err(_) if n==first=>{drop(lock.lock_owned().await);break;}
                            Err(_)=>{until=n*page;break;}
                        }
                    }
                    if locks.is_empty(){continue;}
                    if let Some((updated,_))=self.state().await? {
                        let (filled,end)=updated.written.next(pos,until);
                        if filled && (!strict || end>=(pos+page).min(size)){continue;}
                        if !strict {until=end;}
                    }
                    if let Some((_,CacheFile::Raw))=self.shelf.files().pin(id).await {continue;}
                    let mut input=remote.clone().stream(pos..until,if strict{proof.clone()}else{None},width,acquire.clone(),retain.clone(),recovery.clone());
                    while let Some(bytes)=input.next().await {
                        let data=bytes?;let end=pos+data.len() as u64;
                        let part=self.clone();let copy=data.clone();let current=pos;let authority=proof.clone();
                        let written=tokio::spawn(async move {
                            let result=part.write(current,&copy,strict,authority.as_deref()).await;
                            if result.is_err(){part.discard_failed_write().await?;}
                            result
                        }).await?;
                        if let Err(error)=written {tracing::debug!(%error,"partial raw cache write skipped");}
                        let from=requested.start.saturating_sub(pos) as usize;
                        let to=requested.end.min(end).saturating_sub(pos) as usize;
                        pos=end;
                        locks.retain(|(n,_)| (n+1)*page>pos);
                        yield (data.slice(from..to),true);
                    }
                    if pos!=until {Err(IntegrityError::Length)?;}
                }
            }
        })
    }
}
