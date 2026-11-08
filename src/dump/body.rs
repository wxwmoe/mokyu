use super::{origin::Backend, selection::Item};
use crate::{
    content::{
        codec, compression,
        pack::{Pack, frame},
    },
    model::journal::record::{Attributes, Material, WrappedKey},
    storage::{
        journal::{self, Format, import},
        location::Layout,
        pack_ranges,
    },
};
use anyhow::{Context, Result, ensure};
use bytes::{Bytes, BytesMut};
use futures_util::TryStreamExt;
use md5::{Digest, Md5};
use object_store::{GetOptions, ObjectStoreExt, path::Path};
use rusqlite::{Connection, params};
use std::{collections::BTreeMap, ops::Range};
use uuid::Uuid;
const RAW_WINDOW: u64 = 1024 * 1024;

pub(super) struct Reader<'a> {
    pub db: &'a Connection,
    pub backend: &'a Backend,
    pool: compression::Pool,
    key: codec::Key,
    layout: Layout,
}
struct Extent {
    offset: u64,
    from: u64,
    length: u64,
    material: Material,
    pack: Option<Pack>,
}
impl Reader<'_> {
    pub fn new<'a>(db: &'a Connection, backend: &'a Backend) -> Result<Reader<'a>> {
        db.execute_batch("CREATE TEMP TABLE IF NOT EXISTS raw_pages(id BLOB,offset INTEGER,digest BLOB,PRIMARY KEY(id,offset)); CREATE TEMP TABLE IF NOT EXISTS raw_ready(id BLOB PRIMARY KEY);")?;
        Ok(Reader {
            db,
            backend,
            pool: compression::Pool::new(Default::default(), 1)?,
            key: codec::Key::new("aes-256-gcm", backend.source.material)?,
            layout: Layout::for_local(backend.source.connection.r#type == "local"),
        })
    }
    pub fn attributes(&self, b: Uuid, item: &Item) -> Result<Attributes> {
        let v: Vec<u8> = self.db.query_row(
            "SELECT attributes FROM objects WHERE bucket=? AND seq=? AND key=?",
            params![b, item.seq, item.key],
            |r| r.get(0),
        )?;
        Ok(serde_json::from_slice(&v)?)
    }
    fn keys(&self, id: Option<Uuid>) -> Result<BTreeMap<Uuid, codec::Key>> {
        let mut keys = BTreeMap::new();
        if let Some(id) = id {
            let b: Vec<u8> = self
                .db
                .query_row("SELECT value FROM keys WHERE id=?", [id], |r| r.get(0))?;
            let key = serde_json::from_slice::<WrappedKey>(&b)?.open(&Format {
                storage: self.backend.source.connection.id,
                journal: self.backend.source.journal.journal_id,
                epoch: self.backend.source.journal.writer_epoch,
                encrypted: true,
                key: &self.key,
            })?;
            ensure!(key.id == id, "block key identity mismatch");
            keys.insert(id, codec::Key::new(&key.algorithm, key.material)?);
        }
        Ok(keys)
    }
    async fn range(&self, path: &str, range: Range<u64>, total: u64) -> Result<Bytes> {
        let file = self
            .backend
            .store
            .get_opts(
                &Path::from(path),
                GetOptions {
                    range: Some(range.clone().into()),
                    ..Default::default()
                },
            )
            .await
            .with_context(|| format!("reading {path}"))?;
        ensure!(
            file.meta.size == total && file.range == range,
            "carrier range or length changed at {path}"
        );
        let bytes = file.bytes().await?;
        ensure!(
            bytes.len() as u64 == range.end - range.start,
            "short source range at {path}"
        );
        Ok(bytes)
    }
    async fn raw(
        &self,
        path: &str,
        id: Uuid,
        size: u64,
        md5: &[u8],
        from: u64,
        length: u64,
    ) -> Result<Bytes> {
        if size <= RAW_WINDOW {
            let bytes = self.range(path, 0..size, size).await?;
            ensure!(
                Md5::digest(&bytes).as_slice() == md5,
                "raw source digest mismatch at {path}"
            );
            return Ok(bytes.slice(from as usize..(from + length) as usize));
        }
        let ready = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM raw_ready WHERE id=?)",
            [id],
            |r| r.get::<_, bool>(0),
        )?;
        if !ready {
            let get = self.backend.store.get(&Path::from(path)).await?;
            ensure!(get.meta.size == size, "raw source size changed");
            let mut stream = get.into_stream();
            let mut whole = Md5::new();
            let mut page = blake3::Hasher::new();
            let mut at = 0u64;
            let mut put = self
                .db
                .prepare_cached("INSERT OR REPLACE INTO raw_pages VALUES(?,?,?)")?;
            while let Some(bytes) = stream.try_next().await? {
                whole.update(&bytes);
                let mut input = bytes.as_ref();
                while !input.is_empty() {
                    let n = input.len().min((RAW_WINDOW - at % RAW_WINDOW) as usize);
                    page.update(&input[..n]);
                    at = at.checked_add(n as u64).context("source length overflow")?;
                    ensure!(at <= size, "raw source grew");
                    if at % RAW_WINDOW == 0 || at == size {
                        put.execute(params![
                            id,
                            i64::try_from((at - 1) / RAW_WINDOW * RAW_WINDOW)?,
                            page.finalize().as_bytes()
                        ])?;
                        page.reset();
                    }
                    input = &input[n..];
                }
            }
            ensure!(
                at == size && whole.finalize().as_slice() == md5,
                "raw source digest mismatch at {path}"
            );
            self.db
                .execute("INSERT OR IGNORE INTO raw_ready VALUES(?)", [id])?;
        }
        let end = from + length;
        let start = from / RAW_WINDOW * RAW_WINDOW;
        let stop = end
            .div_ceil(RAW_WINDOW)
            .saturating_mul(RAW_WINDOW)
            .min(size);
        let data = self.range(path, start..stop, size).await?;
        for (i, page) in data.chunks(RAW_WINDOW as usize).enumerate() {
            let at = start + i as u64 * RAW_WINDOW;
            let digest: [u8; 32] = self.db.query_row(
                "SELECT digest FROM raw_pages WHERE id=? AND offset=?",
                params![id, i64::try_from(at)?],
                |r| r.get(0),
            )?;
            ensure!(
                blake3::hash(page).as_bytes() == &digest,
                "raw source changed at {path}, offset {at}"
            );
        }
        Ok(data.slice((from - start) as usize..(end - start) as usize))
    }
    fn extents(&self, b: Uuid, s: i64, start: u64, end: u64, limit: u64) -> Result<Vec<Extent>> {
        let mut stmt=self.db.prepare_cached("SELECT s.offset,s.source_offset,s.length,m.value,p.value FROM spans s JOIN materials m ON s.material=m.ref LEFT JOIN materials p ON m.pack=p.ref WHERE s.bucket=?1 AND s.seq=?2 AND s.offset>=COALESCE((SELECT MAX(offset) FROM spans WHERE bucket=?1 AND seq=?2 AND offset<=?3),0) AND s.offset<?4 ORDER BY s.offset LIMIT 128")?;
        let mut rows = stmt.query(params![b, s, i64::try_from(start)?, i64::try_from(end)?])?;
        let mut result = Vec::new();
        let mut next = start;
        let mut stored = 0u64;
        while let Some(row) = rows.next()? {
            let offset = u64::try_from(row.get::<_, i64>(0)?)?;
            let from = u64::try_from(row.get::<_, i64>(1)?)?;
            let len = u64::try_from(row.get::<_, i64>(2)?)?;
            let mat: Material = serde_json::from_slice(&row.get::<_, Vec<u8>>(3)?)?;
            let stop = offset.checked_add(len).context("bad range")?.min(end);
            ensure!(
                offset <= next && next < stop,
                "discontinuous object range map"
            );
            let pack = match row.get::<_, Option<Vec<u8>>>(4)? {
                Some(v) => {
                    let Material::Pack { pack } = serde_json::from_slice(&v)? else {
                        anyhow::bail!("missing physical pack")
                    };
                    Some(pack)
                }
                None => None,
            };
            if let Material::Member { chunk, .. } = &mat {
                let n = u64::try_from(chunk.stored_size.context("missing member size")?)?;
                if stored + n > limit && !result.is_empty() {
                    break;
                }
                stored += n;
            }
            let source = from
                .checked_add(next - offset)
                .context("bad source offset")?;
            ensure!(
                source
                    .checked_add(stop - next)
                    .is_some_and(|n| n <= mat.size() as u64),
                "object range exceeds source"
            );
            result.push(Extent {
                offset: next,
                from: source,
                length: stop - next,
                material: mat,
                pack,
            });
            next = stop;
        }
        ensure!(!result.is_empty(), "object is missing a range at {start}");
        Ok(result)
    }
    pub async fn read(
        &self,
        b: Uuid,
        item: &Item,
        start: u64,
        end: u64,
        max: u64,
    ) -> Result<Bytes> {
        ensure!(
            start < end && end <= item.size as u64 && max > 0,
            "invalid object read"
        );
        let extents = self.extents(b, item.seq, start, end.min(start.saturating_add(max)), max)?;
        let mut sizes = BTreeMap::new();
        let mut members = Vec::new();
        for e in &extents {
            if let Some((id, entry)) = e.material.pack_member()? {
                let pack = e.pack.as_ref().context("missing member pack")?;
                let size = u64::try_from(pack.stored_size.context("missing pack length")?)?;
                sizes.insert(id, size);
                members.push((
                    id,
                    entry
                        .position
                        .range(entry.raw_size, size, pack.member_count)?,
                ));
            }
        }
        let mut groups = Vec::new();
        pack_ranges::runs(members, |spans, range| groups.push((spans.to_vec(), range)));
        let mut encoded = BTreeMap::new();
        for (spans, range) in groups {
            let id = spans[0].0;
            let path = self.layout.pack(id);
            let data = self.range(path.as_ref(), range.clone(), sizes[&id]).await?;
            for (_, start, end) in spans {
                encoded.insert(
                    (id, start),
                    data.slice((start - range.start) as usize..(end - range.start) as usize),
                );
            }
        }
        let last = extents.last().unwrap();
        let mut output = BytesMut::with_capacity((last.offset + last.length - start) as usize);
        for e in extents {
            let raw = match &e.material {
                Material::Member {
                    pack: id,
                    chunk,
                    offset,
                    ..
                } => {
                    let (_, entry) = e.material.pack_member()?.context("missing pack member")?;
                    frame::decode_entry(
                        e.pack.as_ref().context("missing pack")?,
                        &entry,
                        encoded
                            .get(&(*id, *offset))
                            .context("missing member bytes")?
                            .to_vec(),
                        &self.keys(chunk.block_key_id)?,
                        &self.pool,
                        true,
                    )?
                }
                Material::Chunk { chunk } => {
                    let path = import::path(&e.material, true, self.layout)?;
                    let body =
                        journal::source::bytes(&*self.backend.store, &path, codec::MAX_FILE as u64)
                            .await?
                            .with_context(|| format!("missing {path}"))?;
                    let keys = self.keys(chunk.block_key_id)?;
                    codec::validate_metadata(chunk, &keys)?;
                    codec::decode(chunk, body, &keys, 0, &self.pool)?.0
                }
                Material::Raw { source } => {
                    source.validate()?;
                    let path = import::path(&e.material, true, self.layout)?;
                    let data = self
                        .raw(
                            &path,
                            source.address(),
                            u64::try_from(source.size)?,
                            source.digest.as_deref().context("missing raw digest")?,
                            e.from,
                            e.length,
                        )
                        .await?;
                    output.extend_from_slice(&data);
                    continue;
                }
                Material::Fragment {
                    key, size, hash, ..
                } => {
                    ensure!(
                        *size > 0 && *size <= codec::MAX as i64,
                        "invalid fragment size"
                    );
                    let body = self.range(key, 0..*size as u64, *size as u64).await?;
                    ensure!(
                        blake3::hash(&body).as_bytes().as_slice() == hash,
                        "fragment digest mismatch"
                    );
                    body
                }
                Material::Pack { .. } => anyhow::bail!("object range directly references a pack"),
            };
            output.extend_from_slice(
                raw.get(e.from as usize..(e.from + e.length) as usize)
                    .context("invalid decoded range")?,
            );
        }
        Ok(output.freeze())
    }
}

#[cfg(test)]
#[path = "../../tests/dump/body.rs"]
mod tests;
