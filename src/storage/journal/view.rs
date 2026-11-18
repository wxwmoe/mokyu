use super::{Format, catalog::Catalog, source::Source};
use crate::model::{
    Id,
    journal::record::{BlockKey, Change, Material, Operation, Reference, Span, WrappedKey},
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};
use uuid::Uuid;

#[derive(Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[schema(as = MetadataRecoveryBucket)]
pub(crate) struct Summary {
    pub id: Uuid,
    pub name: String,
    pub generation: String,
    pub sequence: String,
    pub stopped: bool,
    pub objects: String,
    pub logical_bytes: String,
}

pub(crate) struct View {
    pub buckets: BTreeMap<Uuid, Summary>,
    db: Mutex<Connection>,
}

impl View {
    pub fn open(catalog: Catalog) -> Result<Self> {
        let db = catalog.db;
        db.execute_batch(
            "CREATE INDEX bucket_material ON spans(bucket,material);
            CREATE TEMP TABLE imported(ref BLOB PRIMARY KEY, id TEXT NOT NULL); COMMIT;",
        )?;
        let mut buckets = BTreeMap::new();
        {
            let mut query = db.prepare("SELECT b.id,b.name,b.generation,b.seq,b.paused,count(o.key),coalesce(sum(o.size),0)
                FROM buckets b LEFT JOIN objects o ON o.bucket=b.id AND o.generation=b.generation
                AND o.key NOT IN ('~m','~w') AND substr(o.key,1,3) NOT IN ('~h/','~t/')
                GROUP BY b.id,b.generation")?;
            for row in query.query_map([], |r| {
                Ok(Summary {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    generation: r.get::<_, i64>(2)?.to_string(),
                    sequence: r.get::<_, i64>(3)?.to_string(),
                    stopped: r.get(4)?,
                    objects: r.get::<_, i64>(5)?.to_string(),
                    logical_bytes: r.get::<_, i64>(6)?.to_string(),
                })
            })? {
                let row = row?;
                buckets.insert(row.id, row);
            }
        }
        Ok(Self {
            buckets,
            db: Mutex::new(db),
        })
    }

    pub fn required_keys(&self, bucket: Uuid) -> Result<BTreeSet<Uuid>> {
        let db = self.db.lock().unwrap();
        let mut q = db.prepare_cached("SELECT DISTINCT m.block_key FROM spans s JOIN materials m ON m.ref=s.material WHERE s.bucket=? AND m.block_key IS NOT NULL")?;
        Ok(q.query_map([bucket], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub fn key(&self, id: Uuid, source: &Source) -> Result<BlockKey> {
        let value: Vec<u8> =
            self.db
                .lock()
                .unwrap()
                .query_row("SELECT value FROM keys WHERE id=?", [id], |r| r.get(0))?;
        let key = crate::content::codec::Key::new("aes-256-gcm", source.material)?;
        let wrapped: WrappedKey = serde_json::from_slice(&value)?;
        ensure!(wrapped.id == id, "recovery key identity differs");
        wrapped.open(&Format {
            storage: source.connection.id,
            journal: source.journal.journal_id,
            epoch: source.journal.writer_epoch,
            encrypted: true,
            key: &key,
        })
    }

    pub fn next_object(
        &self,
        bucket: Uuid,
        generation: i64,
        after: &str,
    ) -> Result<Option<Change>> {
        let db = self.db.lock().unwrap();
        let mut q = db.prepare_cached("SELECT key,seq,attributes FROM objects WHERE bucket=?1 AND generation=?2 AND key>?3 ORDER BY key LIMIT 1")?;
        let row = q
            .query_row(params![bucket, generation, after], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            })
            .optional()?;
        let Some((key, seq, attrs)) = row else {
            return Ok(None);
        };
        let mut q=db.prepare_cached("SELECT offset,source_offset,length,material FROM spans WHERE bucket=? AND seq=? ORDER BY offset")?;
        let mut rows = q.query(params![bucket, seq])?;
        let mut ranges = Vec::new();
        while let Some(r) = rows.next()? {
            ranges.push(Span {
                offset: r.get(0)?,
                source_offset: r.get(1)?,
                length: r.get(2)?,
                material: serde_json::from_slice(r.get_ref(3)?.as_blob()?)?,
            });
        }
        Ok(Some(Change {
            bucket,
            name: self
                .buckets
                .get(&bucket)
                .context("missing recovery bucket")?
                .name
                .clone(),
            generation,
            seq,
            operation: Operation::Put,
            key: Some(key),
            attributes: Some(serde_json::from_slice(&attrs)?),
            ranges,
        }))
    }

    pub fn material(&self, reference: Reference) -> Result<Material> {
        let db = self.db.lock().unwrap();
        let bytes: Vec<u8> = db
            .prepare_cached("SELECT value FROM materials WHERE ref=?")?
            .query_row([serde_json::to_vec(&reference)?], |r| r.get(0))?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub fn materials_after(&self, bucket: Uuid, after: &[u8]) -> Result<Vec<(Vec<u8>, Material)>> {
        let db = self.db.lock().unwrap();
        let mut q=db.prepare_cached("SELECT refs.material,m.value FROM
            (SELECT DISTINCT material FROM spans WHERE bucket=? AND material>? ORDER BY material LIMIT 128) refs
            JOIN materials m ON refs.material=m.ref ORDER BY refs.material")?;
        let mut rows = q.query(params![bucket, after])?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            result.push((
                row.get(0)?,
                serde_json::from_slice(row.get_ref(1)?.as_blob()?)?,
            ));
        }
        Ok(result)
    }

    pub fn known(&self, reference: Reference) -> Result<bool> {
        Ok(self
            .db
            .lock()
            .unwrap()
            .prepare_cached("SELECT EXISTS(SELECT 1 FROM materials WHERE ref=?)")?
            .query_row([serde_json::to_vec(&reference)?], |r| r.get(0))?)
    }

    pub fn location(&self, reference: Reference) -> Result<Option<Id>> {
        let db = self.db.lock().unwrap();
        let id: Option<String> = db
            .prepare_cached("SELECT id FROM imported WHERE ref=?")?
            .query_row([serde_json::to_vec(&reference)?], |r| r.get(0))
            .optional()?;
        id.map(|id| id.parse().map_err(anyhow::Error::from))
            .transpose()
    }

    pub fn remember(&self, reference: Reference, id: Id) -> Result<()> {
        self.db
            .lock()
            .unwrap()
            .prepare_cached("INSERT INTO imported VALUES(?,?)")?
            .execute(params![serde_json::to_vec(&reference)?, id.to_string()])?;
        Ok(())
    }
}
