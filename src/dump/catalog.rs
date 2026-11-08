use crate::{
    content::pack::frame,
    model::journal::record::{Change, Kind, Material, Operation, Reference, Span, WrappedKey},
    storage::journal::{
        Format,
        batch::{Record, range_digest},
        snapshot::{BucketCut, Row},
        stream::Sink,
    },
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use uuid::Uuid;

pub(super) struct Catalog(pub Connection);

impl Catalog {
    fn ranges(&self, b: Uuid, g: i64, s: i64, offset: i64, spans: Vec<Span>) -> Result<()> {
        let (n, mut end, digest) = self
            .0
            .query_row(
                "SELECT n,end,digest FROM pages WHERE (bucket,generation,seq)=(?1,?2,?3)",
                params![b, g, s],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, [u8; 32]>(2)?,
                    ))
                },
            )
            .optional()?
            .unwrap_or_default();
        ensure!(
            end == offset && (1..=128).contains(&spans.len()),
            "discontinuous journal range page"
        );
        let digest = range_digest(digest, &spans)?;
        let count = n
            .checked_add(spans.len() as i64)
            .context("too many ranges")?;
        let mut put = self
            .0
            .prepare_cached("INSERT INTO ranges VALUES(?1,?2,?3,?4,?5,?6,?7)")?;
        for span in spans {
            ensure!(
                span.offset == end
                    && span.length > 0
                    && span.source_offset >= 0
                    && span.source_offset.checked_add(span.length).is_some()
                    && !span.material.id.is_nil(),
                "invalid journal extent"
            );
            end = end
                .checked_add(span.length)
                .context("journal offset overflow")?;
            put.execute(params![
                b,
                g,
                s,
                span.offset,
                span.source_offset,
                span.length,
                serde_json::to_vec(&span.material)?
            ])?;
        }
        self.0.execute(
            "INSERT OR REPLACE INTO pages VALUES(?1,?2,?3,?4,?5,?6)",
            params![b, g, s, count, end, digest],
        )?;
        Ok(())
    }

    fn complete(&self, c: &Change, g: i64, count: u64, digest: [u8; 32]) -> Result<()> {
        let p = params![c.bucket, g, c.seq];
        let (n, end, hash) = self
            .0
            .query_row(
                "SELECT n,end,digest FROM pages WHERE (bucket,generation,seq)=(?1,?2,?3)",
                p,
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, [u8; 32]>(2)?,
                    ))
                },
            )
            .optional()?
            .unwrap_or_default();
        ensure!(
            c.ranges.is_empty() && u64::try_from(n)? == count && digest == hash,
            "incomplete journal range map"
        );
        c.validate_metadata(end)?;
        self.0.execute(
            "DELETE FROM pages WHERE (bucket,generation,seq)=(?1,?2,?3)",
            p,
        )?;
        Ok(())
    }

    fn object(&self, c: &Change, g: i64) -> Result<()> {
        let db = &self.0;
        let p = params![c.bucket, c.generation, c.key];
        let old: Option<i64> = db
            .query_row(
                "SELECT seq FROM objects WHERE (bucket,generation,key)=(?1,?2,?3)",
                p,
                |r| r.get(0),
            )
            .optional()?;
        if old.is_none_or(|s| s <= c.seq) {
            if let Some(s) = old.filter(|&s| s != c.seq || g == 0) {
                db.execute(
                    "DELETE FROM spans WHERE bucket=?1 AND seq=?2",
                    params![c.bucket, s],
                )?;
            }
            db.execute(
                "INSERT OR REPLACE INTO objects VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    c.bucket,
                    c.generation,
                    c.key,
                    c.seq,
                    c.attributes.as_ref().map(|a| a.size),
                    c.attributes.as_ref().map(serde_json::to_vec).transpose()?
                ],
            )?;
            if g == 0 {
                self.store_spans(c, g)?;
            }
        }
        Ok(())
    }

    fn store_spans(&self, c: &Change, g: i64) -> Result<()> {
        self.0.execute("INSERT INTO spans SELECT bucket,seq,offset,source_offset,length,material FROM ranges WHERE (bucket,generation,seq)=(?1,?2,?3)",params![c.bucket,g,c.seq])?;
        Ok(())
    }
    fn apply_change(&self, c: &Change) -> Result<()> {
        let db = &self.0;
        let (id, g, s) = (c.bucket, c.generation, c.seq);
        let (prev, excluded) = db
            .query_row("SELECT seq,excluded FROM marks WHERE bucket=?", [id], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, Option<i64>>(1)?))
            })
            .optional()?
            .unwrap_or_default();
        ensure!(
            prev.checked_add(1) == Some(s),
            "missing or repeated journal sequence"
        );
        db.execute("INSERT INTO marks VALUES(?1,?2,NULL) ON CONFLICT(bucket) DO UPDATE SET seq=excluded.seq",params![id,s])?;
        let newest=db.query_row("SELECT generation,name,visible FROM buckets WHERE id=? ORDER BY generation DESC LIMIT 1",[id],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,bool>(2)?))).optional()?;
        if c.operation == Operation::Start {
            ensure!(
                newest
                    .as_ref()
                    .map(|n| n.0)
                    .or(excluded)
                    .unwrap_or(0)
                    .checked_add(1)
                    == Some(g),
                "journal baseline generation mismatch"
            );
            db.execute("UPDATE marks SET excluded=NULL WHERE bucket=?", [id])?;
            db.execute("DELETE FROM objects WHERE bucket=? AND generation IN(SELECT generation FROM buckets WHERE id=?1 AND visible=0)",[id])?;
            db.execute("DELETE FROM buckets WHERE id=? AND visible=0", [id])?;
            db.execute(
                "INSERT INTO buckets(id,generation,name,seq) VALUES(?,?,?,?)",
                params![id, g, c.name, s],
            )?;
            return Ok(());
        }
        if let Some(old) = excluded {
            ensure!(old == g, "unexpected excluded bucket generation");
            return Ok(());
        }
        let (generation, name, visible) = newest.context("missing journal baseline")?;
        ensure!(
            g == generation && c.name == name,
            "journal bucket identity changed"
        );
        db.execute(
            "UPDATE buckets SET seq=?3 WHERE id=?1 AND generation=?2",
            params![id, g, s],
        )?;
        match c.operation {
            Operation::Put | Operation::Delete => self.object(c, g)?,
            Operation::Pause | Operation::Resume => {
                db.execute(
                    "UPDATE buckets SET paused=?3 WHERE id=?1 AND generation=?2",
                    params![id, g, c.operation == Operation::Pause],
                )?;
            }
            Operation::Ready => {
                ensure!(!visible, "journal baseline already published");
                db.execute(
                    "DELETE FROM buckets WHERE id=?1 AND generation<>?2",
                    params![id, g],
                )?;
                db.execute("UPDATE buckets SET visible=1 WHERE id=?", [id])?;
            }
            Operation::Drop => {
                db.execute("DELETE FROM objects WHERE bucket=?", [id])?;
                db.execute("DELETE FROM buckets WHERE id=?", [id])?;
            }
            Operation::Start => unreachable!(),
        }
        Ok(())
    }

    fn clear_ranges(&self, b: Uuid, g: i64, s: i64) -> Result<()> {
        self.0.execute(
            "DELETE FROM ranges WHERE (bucket,generation,seq)=(?1,?2,?3)",
            params![b, g, s],
        )?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/dump/catalog.rs"]
mod tests;

impl Sink for Catalog {
    fn scope(&mut self, cuts: &[BucketCut], excluded: &[BucketCut]) -> Result<()> {
        for cut in cuts.iter().chain(excluded) {
            for table in ["objects", "ranges", "pages", "marks", "events", "spans"] {
                self.0
                    .execute(&format!("DELETE FROM {table} WHERE bucket=?"), [cut.id])?;
            }
            self.0.execute("DELETE FROM buckets WHERE id=?", [cut.id])?;
        }
        for (active, group) in [(true, cuts), (false, excluded)] {
            for c in group {
                ensure!(
                    !c.id.is_nil() && c.generation > 0 && c.sequence > 0,
                    "invalid snapshot cut"
                );
                self.0.execute(
                    "INSERT INTO marks VALUES(?,?,?)",
                    params![c.id, c.sequence, (!active).then_some(c.generation)],
                )?;
                if active {
                    ensure!(
                        s3s::path::check_bucket_name(&c.name),
                        "invalid snapshot bucket name"
                    );
                    self.0.execute(
                        "INSERT INTO buckets VALUES(?,?,?,?,?,1)",
                        params![c.id, c.generation, c.name, c.sequence, c.stopped],
                    )?;
                }
            }
        }
        Ok(())
    }
    fn row(&mut self, row: Row, f: &Format<'_>) -> Result<()> {
        let (b, g, s, k, attr, count, digest) = match row {
            Row::Key(k) => return self.stage(Record::Key(k), f),
            Row::Material(m) => return self.stage(Record::Material(m), f),
            Row::Ranges {
                bucket,
                seq,
                offset,
                ranges,
                ..
            } => return self.ranges(bucket, 0, seq, offset, ranges),
            Row::Object {
                bucket,
                generation,
                seq,
                key,
                attributes,
                range_count,
                range_digest,
            } => (
                bucket,
                generation,
                seq,
                key,
                Some(attributes),
                range_count,
                range_digest,
            ),
            Row::Delete {
                bucket,
                generation,
                seq,
                key,
            } => (bucket, generation, seq, key, None, 0, [0; 32]),
            _ => return Ok(()),
        };
        let state = self
            .0
            .query_row(
                "SELECT name,seq FROM buckets WHERE id=? AND generation=?",
                params![b, g],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
            )
            .optional()?;
        if let Some((name, watermark)) = state {
            ensure!(s <= watermark, "snapshot object beyond cut");
            let c = Change {
                bucket: b,
                generation: g,
                seq: s,
                name,
                operation: if attr.is_some() {
                    Operation::Put
                } else {
                    Operation::Delete
                },
                key: Some(k),
                attributes: attr,
                ranges: Vec::new(),
            };
            self.complete(&c, 0, count, digest)?;
            self.object(&c, 0)?;
        }
        self.clear_ranges(b, 0, s)
    }
    fn stage(&mut self, record: Record, f: &Format<'_>) -> Result<()> {
        let db = &self.0;
        match record {
            Record::Key(k) => {
                let opened = k.open(f)?;
                let value: Option<Vec<u8>> = db
                    .query_row("SELECT value FROM keys WHERE id=?", [k.id], |r| r.get(0))
                    .optional()?;
                if let Some(v) = value {
                    let old = serde_json::from_slice::<WrappedKey>(&v)?.open(f)?;
                    ensure!(
                        old.algorithm == opened.algorithm && old.material == opened.material,
                        "conflicting recovery key"
                    );
                } else {
                    db.execute(
                        "INSERT INTO keys VALUES(?,?)",
                        params![k.id, serde_json::to_vec(&k)?],
                    )?;
                }
            }
            Record::Material(m) => {
                let reference = m.reference();
                ensure!(
                    !reference.id.is_nil() && m.size() >= 0,
                    "invalid recovery material"
                );
                let hold = m.hold()?.context("missing journal source")?;
                ensure!(
                    hold.storage == f.storage,
                    "journal source belongs to another storage"
                );
                let (pack, ord) = match &m {
                    Material::Member { pack, ordinal, .. } => (
                        Some(serde_json::to_vec(&Reference {
                            kind: Kind::Pack,
                            id: *pack,
                            member: None,
                        })?),
                        Some(*ordinal),
                    ),
                    _ => (None, None),
                };
                let id = serde_json::to_vec(&reference)?;
                let body = serde_json::to_vec(&m)?;
                if db.execute(
                    "INSERT OR IGNORE INTO materials VALUES(?1,?2,?3,?4,?5,?6)",
                    params![id, m.size(), pack, ord, hold.key, body],
                )? == 0
                {
                    ensure!(
                        db.query_row(
                            "SELECT value=?2 FROM materials WHERE ref=?1",
                            params![id, body],
                            |r| r.get::<_, bool>(0)
                        )?,
                        "conflicting recovery material"
                    );
                }
            }
            Record::Ranges {
                bucket,
                generation,
                seq,
                offset,
                ranges,
            } => self.ranges(bucket, generation, seq, offset, ranges)?,
            Record::Change {
                value: c,
                range_count,
                range_digest,
            } => {
                self.complete(&c, c.generation, range_count, range_digest)?;
                let v = serde_json::to_vec(&c)?;
                if db.execute(
                    "INSERT OR IGNORE INTO events(bucket,seq,value,digest) VALUES(?,?,?,?)",
                    params![c.bucket, c.seq, v, range_digest],
                )? == 0
                {
                    ensure!(
                        db.query_row(
                            "SELECT value=?3 AND digest=?4 FROM events WHERE bucket=?1 AND seq=?2",
                            params![c.bucket, c.seq, v, range_digest],
                            |r| r.get::<_, bool>(0)
                        )?,
                        "conflicting repeated journal event"
                    );
                } else {
                    self.store_spans(&c, c.generation)?;
                }
                self.clear_ranges(c.bucket, c.generation, c.seq)?;
            }
        }
        Ok(())
    }
    fn apply(&mut self) -> Result<()> {
        loop {
            let value: Option<Vec<u8>> = self
                .0
                .query_row(
                    "SELECT value FROM events WHERE applied=0 ORDER BY bucket,seq LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .optional()?;
            let Some(value) = value else { break };
            let c: Change = serde_json::from_slice(&value)?;
            self.apply_change(&c)?;
            self.0.execute(
                "UPDATE events SET applied=1 WHERE bucket=? AND seq=?",
                params![c.bucket, c.seq],
            )?;
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        let db = &self.0;
        db.execute_batch("DELETE FROM buckets WHERE visible=0;
          DELETE FROM objects WHERE attributes IS NULL OR NOT EXISTS(SELECT 1 FROM buckets b WHERE b.id=objects.bucket AND b.generation=objects.generation);
          CREATE UNIQUE INDEX object_sequence ON objects(bucket,seq);
          DELETE FROM spans WHERE NOT EXISTS(SELECT 1 FROM objects o WHERE o.bucket=spans.bucket AND o.seq=spans.seq);
          CREATE INDEX span_material ON spans(material);
          DELETE FROM materials WHERE pack IS NOT NULL AND ref NOT IN(SELECT material FROM spans);
          CREATE INDEX pack_members ON materials(pack,ordinal);")?;
        let bad=db.query_row("SELECT EXISTS(SELECT 1 FROM spans s LEFT JOIN materials m ON m.ref=s.material LEFT JOIN materials p ON p.ref=m.pack LEFT JOIN keys k ON k.id=m.block_key WHERE m.ref IS NULL OR m.size-s.length<s.source_offset OR (m.pack IS NOT NULL AND p.ref IS NULL) OR (m.block_key IS NOT NULL AND k.id IS NULL))",[],|r|r.get::<_,bool>(0))?;
        ensure!(!bad, "missing or invalid journal range material");
        let mut packs =
            db.prepare("SELECT DISTINCT pack FROM materials WHERE pack IS NOT NULL ORDER BY pack")?;
        let mut members =
            db.prepare("SELECT value FROM materials WHERE pack=? ORDER BY ordinal")?;
        for id in packs.query_map([], |r| r.get::<_, Vec<u8>>(0))? {
            let id = id?;
            let body: Vec<u8> =
                db.query_row("SELECT value FROM materials WHERE ref=?", [&id], |r| {
                    r.get(0)
                })?;
            let Material::Pack { pack } = serde_json::from_slice(&body)? else {
                anyhow::bail!("invalid pack reference")
            };
            let mut entries = Vec::new();
            for body in members.query_map([&id], |r| r.get::<_, Vec<u8>>(0))? {
                ensure!(
                    entries.len() < crate::content::pack::MAX_MEMBERS,
                    "too many pack members"
                );
                entries.push(
                    serde_json::from_slice::<Material>(&body?)?
                        .pack_member()?
                        .context("missing pack member")?
                        .1,
                );
            }
            frame::validate_entries(
                &entries,
                pack.stored_size.context("missing pack size")?.try_into()?,
                pack.raw_size,
                pack.member_count,
                false,
            )?;
        }
        Ok(())
    }
    fn list(&mut self, first: u64, path: String) -> Result<()> {
        self.0.execute(
            "INSERT INTO paths VALUES(?,?)",
            params![first.to_be_bytes(), path],
        )?;
        Ok(())
    }
    fn pop(&mut self) -> Result<Option<(u64, String)>> {
        Ok(self
            .0
            .query_row(
                "DELETE FROM paths WHERE first=(SELECT MIN(first) FROM paths) RETURNING first,path",
                [],
                |r| Ok((u64::from_be_bytes(r.get(0)?), r.get(1)?)),
            )
            .optional()?)
    }
}
