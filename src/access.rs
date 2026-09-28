use crate::{app::App, config};
use anyhow::Result;
use chrono::{DateTime, Timelike, Utc};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct ChunkCounts {
    reads: i64,
    ranges: i64,
    bytes: i64,
    origins: i64,
}
#[derive(Default)]
struct PackCounts {
    downloads: i64,
    bytes: i64,
    partial: i64,
    partial_bytes: i64,
    useful: i64,
}
#[derive(Default)]
struct Buffer {
    chunks: HashMap<(i64, DateTime<Utc>), ChunkCounts>,
    packs: HashMap<(i64, DateTime<Utc>), PackCounts>,
    members: HashMap<(i64, i64, DateTime<Utc>), i64>,
    dropped: bool,
}
pub struct Access {
    buffer: Mutex<Buffer>,
    limit: usize,
    pub wake: tokio::sync::Notify,
}
impl Access {
    pub fn new(limit: usize) -> Self {
        Self {
            buffer: Mutex::new(Buffer::default()),
            limit,
            wake: tokio::sync::Notify::new(),
        }
    }
    fn hour() -> DateTime<Utc> {
        Utc::now()
            .with_minute(0)
            .unwrap()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap()
    }
    pub fn chunk(&self, id: i64, range: bool, bytes: usize, origin: bool) {
        let key = (id, Self::hour());
        let mut buffer = self.buffer.lock().unwrap();
        if buffer.chunks.len() + buffer.packs.len() + buffer.members.len() >= self.limit
            && !buffer.chunks.contains_key(&key)
        {
            buffer.dropped = true;
            self.wake.notify_one();
            return;
        }
        let counts = buffer.chunks.entry(key).or_default();
        counts.reads += 1;
        counts.ranges += i64::from(range);
        counts.bytes += bytes as i64;
        counts.origins += i64::from(origin && range);
    }
    pub fn pack(
        &self,
        id: i64,
        bytes: usize,
        partial: bool,
        useful: usize,
        members: impl Iterator<Item = i64>,
    ) {
        let key = (id, Self::hour());
        let members: Vec<i64> = if partial {
            members.collect()
        } else {
            Vec::new()
        };
        let mut buffer = self.buffer.lock().unwrap();
        let added = usize::from(!buffer.packs.contains_key(&key))
            + members
                .iter()
                .map(|c| {
                    usize::from(!buffer.chunks.contains_key(&(*c, key.1)))
                        + usize::from(!buffer.members.contains_key(&(id, *c, key.1)))
                })
                .sum::<usize>();
        if buffer.chunks.len() + buffer.packs.len() + buffer.members.len() + added > self.limit {
            buffer.dropped = true;
            self.wake.notify_one();
            return;
        }
        let counts = buffer.packs.entry(key).or_default();
        counts.downloads += 1;
        counts.bytes += bytes as i64;
        counts.partial += i64::from(partial);
        counts.partial_bytes += if partial { bytes as i64 } else { 0 };
        counts.useful += useful as i64;
        for c in members {
            *buffer.members.entry((id, c, key.1)).or_default() += 1;
            buffer.chunks.entry((c, key.1)).or_default().origins += 1;
        }
    }
    async fn flush(&self, app: &App) -> Result<()> {
        let pending = std::mem::take(&mut *self.buffer.lock().unwrap());
        let mut tx = app.db.begin().await?;
        // Each capped map is persisted in one bulk query.
        let mut ids = Vec::new();
        let mut hours = Vec::new();
        let mut reads = Vec::new();
        let mut ranges = Vec::new();
        let mut bytes = Vec::new();
        let mut origins = Vec::new();
        for ((id, hour), c) in pending.chunks {
            ids.push(id);
            hours.push(hour);
            reads.push(c.reads);
            ranges.push(c.ranges);
            bytes.push(c.bytes);
            origins.push(c.origins);
        }
        sqlx::query("INSERT INTO chunk_access_stats(chunk_id,reads,range_reads,bytes,last_read_at) SELECT c.id,sum(s.reads),sum(s.ranges),sum(s.bytes),now() FROM unnest($1::bigint[],$2::bigint[],$3::bigint[],$4::bigint[]) s(id,reads,ranges,bytes) JOIN chunks c ON c.id=s.id GROUP BY c.id ON CONFLICT(chunk_id) DO UPDATE SET reads=chunk_access_stats.reads+EXCLUDED.reads,range_reads=chunk_access_stats.range_reads+EXCLUDED.range_reads,bytes=chunk_access_stats.bytes+EXCLUDED.bytes,last_read_at=EXCLUDED.last_read_at")
            .bind(&ids).bind(&reads).bind(&ranges).bind(&bytes).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO chunk_access_windows(chunk_id,window_start,reads,range_reads,bytes,range_origin_reads) SELECT c.id,s.hour,s.reads,s.ranges,s.bytes,s.origins FROM unnest($1::bigint[],$2::timestamptz[],$3::bigint[],$4::bigint[],$5::bigint[],$6::bigint[]) s(id,hour,reads,ranges,bytes,origins) JOIN chunks c ON c.id=s.id ON CONFLICT(window_start,chunk_id) DO UPDATE SET reads=chunk_access_windows.reads+EXCLUDED.reads,range_reads=chunk_access_windows.range_reads+EXCLUDED.range_reads,bytes=chunk_access_windows.bytes+EXCLUDED.bytes,range_origin_reads=chunk_access_windows.range_origin_reads+EXCLUDED.range_origin_reads")
            .bind(&ids).bind(hours).bind(reads).bind(ranges).bind(bytes).bind(origins).execute(&mut *tx).await?;
        let mut ids = Vec::new();
        let mut hours = Vec::new();
        let mut reads = Vec::new();
        let mut partial = Vec::new();
        let mut partial_bytes = Vec::new();
        let mut bytes = Vec::new();
        let mut useful = Vec::new();
        for ((id, hour), p) in pending.packs {
            ids.push(id);
            hours.push(hour);
            reads.push(p.downloads);
            partial.push(p.partial);
            partial_bytes.push(p.partial_bytes);
            bytes.push(p.bytes);
            useful.push(p.useful);
        }
        sqlx::query("INSERT INTO pack_access_windows(pack_id,window_start,downloads,downloaded_bytes,partial_downloads,useful_bytes,partial_bytes) SELECT p.id,s.hour,s.reads,s.bytes,s.partial,s.useful,s.partial_bytes FROM unnest($1::bigint[],$2::timestamptz[],$3::bigint[],$4::bigint[],$5::bigint[],$6::bigint[],$7::bigint[]) s(id,hour,reads,bytes,partial,useful,partial_bytes) JOIN packs p ON p.id=s.id ON CONFLICT(window_start,pack_id) DO UPDATE SET downloads=pack_access_windows.downloads+EXCLUDED.downloads,downloaded_bytes=pack_access_windows.downloaded_bytes+EXCLUDED.downloaded_bytes,partial_downloads=pack_access_windows.partial_downloads+EXCLUDED.partial_downloads,useful_bytes=pack_access_windows.useful_bytes+EXCLUDED.useful_bytes,partial_bytes=pack_access_windows.partial_bytes+EXCLUDED.partial_bytes,updated_at=now()")
            .bind(ids).bind(hours).bind(reads).bind(bytes).bind(partial).bind(useful).bind(partial_bytes).execute(&mut *tx).await?;
        let (mut packs, mut chunks, mut hours, mut counts) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for ((p, c, h), n) in pending.members {
            packs.push(p);
            chunks.push(c);
            hours.push(h);
            counts.push(n);
        }
        sqlx::query("INSERT INTO pack_member_access_windows(pack_id,chunk_id,window_start,downloads) SELECT p.id,c.id,s.hour,s.n FROM unnest($1::bigint[],$2::bigint[],$3::timestamptz[],$4::bigint[]) s(p,c,hour,n) JOIN packs p ON p.id=s.p JOIN chunks c ON c.id=s.c ON CONFLICT(window_start,pack_id,chunk_id) DO UPDATE SET downloads=pack_member_access_windows.downloads+EXCLUDED.downloads")
            .bind(packs).bind(chunks).bind(hours).bind(counts).execute(&mut *tx).await?;
        let stale = (config::seconds(&app.config.statistics.access_flush_interval)? * 2) as f64;
        sqlx::query("UPDATE mokyu_meta SET access_coverage_since=CASE WHEN $1 OR access_flushed_at<now()-$2*interval '1 second' THEN now() ELSE access_coverage_since END,access_flushed_at=now()")
            .bind(pending.dropped).bind(stale).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}
pub async fn run(app: Arc<App>) -> Result<()> {
    let period = Duration::from_secs(config::seconds(
        &app.config.statistics.access_flush_interval,
    )?);
    loop {
        tokio::select! {_=tokio::time::sleep(period)=>{},_=app.storage.access.wake.notified()=>{}}
        if let Err(e) = app.storage.access.flush(&app).await {
            app.storage.access.buffer.lock().unwrap().dropped = true;
            tracing::warn!(error=%e,"access statistics flush failed; observation coverage has a gap");
        }
        for sql in [
            "DELETE FROM chunk_access_windows WHERE (window_start,chunk_id) IN (SELECT window_start,chunk_id FROM chunk_access_windows WHERE window_start<now()-$1*interval '1 second' ORDER BY window_start LIMIT $2)",
            "DELETE FROM pack_access_windows WHERE (window_start,pack_id) IN (SELECT window_start,pack_id FROM pack_access_windows WHERE window_start<now()-$1*interval '1 second' ORDER BY window_start LIMIT $2)",
            "DELETE FROM pack_member_access_windows WHERE (window_start,pack_id,chunk_id) IN (SELECT window_start,pack_id,chunk_id FROM pack_member_access_windows WHERE window_start<now()-$1*interval '1 second' ORDER BY window_start LIMIT $2)",
        ] {
            if let Err(e) = sqlx::query(sql)
                .bind(config::seconds(&app.config.statistics.access_retention)? as f64)
                .bind(app.config.cleanup.batch_size as i64)
                .execute(&app.db)
                .await
            {
                tracing::warn!(error=%e,"access statistics cleanup failed");
            }
        }
    }
}
