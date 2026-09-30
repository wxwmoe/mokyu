use crate::{
    app::App,
    config,
    pack_tasks::{Mapping, Output},
    storage::ReadContext,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::collections::HashMap;
use uuid::Uuid;

// A conservative byte-equivalent charge for extra remote round trips, in addition to payloads.
const EXTRA_GET_BYTES: i64 = 64 * 1024;

#[derive(serde::Serialize)]
struct Benefit {
    observed_bytes: i64,
    projected_bytes: i64,
    rewrite_bytes: i64,
    extra_gets: i64,
    request_penalty_bytes: i64,
    net_savings_bytes: i64,
}
impl Benefit {
    fn new(observed: i64, projected: i64, rewrite: i64, extra_gets: i64) -> Self {
        let penalty = extra_gets.saturating_mul(EXTRA_GET_BYTES);
        Self {
            observed_bytes: observed,
            projected_bytes: projected,
            rewrite_bytes: rewrite,
            extra_gets,
            request_penalty_bytes: penalty,
            net_savings_bytes: observed
                .saturating_sub(projected)
                .saturating_sub(rewrite)
                .saturating_sub(penalty),
        }
    }
    fn worthwhile(&self, c: &crate::pack::Config) -> Result<bool> {
        Ok(self.net_savings_bytes > 0
            && self.net_savings_bytes as u64 >= config::bytes(&c.range_min_savings_bytes)?
            && i128::from(self.net_savings_bytes) * 100
                >= i128::from(self.observed_bytes) * i128::from(c.range_min_savings_percent))
    }
}

impl App {
    pub(crate) async fn range_batch(&self, task: Uuid, detail: &Value) -> Result<bool> {
        ensure!(
            self.config.pack.range_optimization,
            "Range optimization is disabled"
        );
        let cutoff = detail["upper_time"]
            .as_str()
            .context("missing Range snapshot")?
            .parse::<chrono::DateTime<chrono::Utc>>()?;
        // Check only packs with new pressure evidence, at most once per sweep. Larger downloads go first.
        let id:Option<i64>=sqlx::query_scalar("SELECT p.id FROM packs p JOIN pack_access_windows w ON w.pack_id=p.id WHERE p.state='ready' AND p.created_at<=$1 AND (p.range_checked_at IS NULL OR p.range_checked_at<$1) AND w.window_start>=date_trunc('hour',now()-$2*interval '1 second') GROUP BY p.id HAVING sum(w.partial_downloads)>=$3 AND max(w.updated_at)>COALESCE(p.range_checked_at,'-infinity'::timestamptz) ORDER BY sum(w.partial_bytes) DESC,p.id LIMIT 1")
            .bind(cutoff).bind(config::seconds(&self.config.pack.range_window)? as f64).bind(i64::from(self.config.pack.range_min_downloads)).fetch_optional(&self.db).await?;
        let Some(id) = id else {
            sqlx::query("UPDATE tasks SET state='completed',updated_at=now() WHERE id=$1 AND state='running'").bind(task).execute(&self.db).await?;
            return Ok(false);
        };
        let (processed, decision) = self.optimize_range_pack(task, id).await?;
        let mut tx = self.db.begin().await?;
        sqlx::query("UPDATE packs SET range_checked_at=now() WHERE id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE tasks SET cursor=$2,processed=processed+$3,detail=detail||jsonb_build_object('last_range',$4::jsonb,'evaluated',COALESCE((detail->>'evaluated')::bigint,0)+1),updated_at=now() WHERE id=$1 AND state='running'").bind(task).bind(id.to_string()).bind(processed as i64).bind(decision).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(true)
    }
    async fn optimize_range_pack(&self, task: Uuid, id: i64) -> Result<(usize, Value)> {
        let mut report = json!({"pack_id":id.to_string(),"applied":false});
        let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE source_pack=$1) OR EXISTS(SELECT 1 FROM chunks c JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id WHERE c.pack_id=$1 AND s.state='writing')").bind(id).fetch_one(&self.db).await?;
        if blocked {
            report["reason"] = json!("active_upload");
            return Ok((0, report));
        }
        let window = config::seconds(&self.config.pack.range_window)? as f64;
        let (partial,observed,full):(i64,i64,i64)=sqlx::query_as("SELECT COALESCE(sum(partial_downloads),0)::bigint,COALESCE(sum(partial_bytes),0)::bigint,COALESCE(sum(downloads-partial_downloads),0)::bigint FROM pack_access_windows WHERE pack_id=$1 AND window_start>=date_trunc('hour',now()-$2*interval '1 second')").bind(id).bind(window).fetch_one(&self.db).await?;
        let hits:HashMap<i64,i64>=sqlx::query_as::<_,(i64,i64)>("SELECT chunk_id,sum(downloads)::bigint FROM pack_member_access_windows WHERE pack_id=$1 AND window_start>=date_trunc('hour',now()-$2*interval '1 second') GROUP BY chunk_id").bind(id).bind(window).fetch_all(&self.db).await?.into_iter().collect();
        let (old, raw, count): (i64, i64, i32) =
            sqlx::query_as("SELECT stored_size,raw_size,member_count FROM packs WHERE id=$1")
                .bind(id)
                .fetch_one(&self.db)
                .await?;
        let rows:Vec<Mapping>=sqlx::query_as("SELECT c.*,m.offset_bytes,c.pack_id FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=$1 AND c.pack_id=$1 AND c.state='ready' AND EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id) ORDER BY m.ordinal LIMIT 4096").bind(id).fetch_all(&self.db).await?;
        if rows.len() != count as usize
            || rows
                .iter()
                .map(|r| i64::from(r.chunk.raw_size))
                .sum::<i64>()
                != raw
            || hits.values().sum::<i64>() < partial
            || partial < i64::from(self.config.pack.range_min_downloads)
            || observed == 0
        {
            report["reason"] = json!("incomplete_evidence_or_layout");
            return Ok((0, report));
        }
        let mut groups: Vec<Vec<Mapping>> = Vec::new();
        for row in &rows {
            let join = if let Some(last) = groups.last().and_then(|g| g.last()) {
                self.pack_creation_allowed()
                    && hits.contains_key(&last.chunk.id) == hits.contains_key(&row.chunk.id)
                    && last.offset_bytes + i64::from(last.chunk.raw_size) == row.offset_bytes
                    && groups
                        .last()
                        .unwrap()
                        .iter()
                        .map(|r| r.chunk.raw_size as u64)
                        .sum::<u64>()
                        + row.chunk.raw_size as u64
                        <= config::bytes(&self.config.pack.max_size)?
                    && Self::same_references(
                        last.chunk.id,
                        row.chunk.id,
                        row.offset_bytes - last.offset_bytes,
                    )
                    .fetch_one(&self.db)
                    .await?
            } else {
                false
            };
            if join {
                groups.last_mut().unwrap().push(row.clone());
            } else {
                groups.push(vec![row.clone()]);
            }
        }
        if groups.len() < 2 {
            report["reason"] = json!("no_smaller_contiguous_layout");
            return Ok((0, report));
        }
        let evaluate = |sources: Vec<(Vec<i64>, i64)>| {
            let mut traffic = 0i64;
            let mut requests = 0i64;
            let mut bytes = 0i64;
            for (ids, size) in &sources {
                // Sum is an upper bound on the union of member requests; never assume co-access from equal counts.
                let downloads = ids
                    .iter()
                    .map(|id| hits.get(id).copied().unwrap_or(0))
                    .sum::<i64>()
                    .min(partial);
                traffic = traffic.saturating_add(downloads.saturating_mul(*size));
                requests = requests.saturating_add(downloads);
                bytes = bytes.saturating_add(*size);
            }
            traffic = traffic.saturating_add(full.saturating_mul(bytes.saturating_sub(old).max(0)));
            let extra = requests
                .saturating_sub(partial)
                .saturating_add(full.saturating_mul(sources.len().saturating_sub(1) as i64))
                .max(0);
            Benefit::new(observed, traffic, old.saturating_add(bytes), extra)
        };
        let predicted = evaluate(
            groups
                .iter()
                .map(|g| {
                    (
                        g.iter().map(|r| r.chunk.id).collect(),
                        g.iter()
                            .map(|r| {
                                i64::from(r.chunk.stored_size.unwrap_or(r.chunk.raw_size + 16))
                            })
                            .sum::<i64>()
                            + if g.len() > 1 {
                                12 + 44 * g.len() as i64
                            } else {
                                0
                            },
                    )
                })
                .collect(),
        );
        report["partial_downloads"] = json!(partial);
        report["benefit"] = serde_json::to_value(&predicted)?;
        if !predicted.worthwhile(&self.config.pack)? {
            report["reason"] = json!("insufficient_net_savings");
            return Ok((0, report));
        }
        self.pin_pack_inputs(task, &rows).await?;
        let context = ReadContext::default();
        let mut outputs: Vec<Output> = Vec::new();
        for group in &groups {
            outputs.extend(
                self.pack_output(
                    task,
                    group,
                    &context,
                    None,
                    "",
                    self.pack_creation_allowed(),
                )
                .await?,
            );
        }
        let actual = evaluate(
            outputs
                .iter()
                .map(|o| (o.chunks.clone(), o.size()))
                .collect(),
        );
        report["benefit"] = serde_json::to_value(&actual)?;
        if !actual.worthwhile(&self.config.pack)? {
            report["reason"] = json!("encoded_layout_not_beneficial");
            return Ok((0, report));
        }
        self.commit_outputs(task, &rows, &outputs, true).await?;
        report["applied"] = json!(true);
        report["reason"] = json!("repeated_partial_downloads");
        Ok((rows.len(), report))
    }
    pub(crate) async fn defer_range_repack(&self, stream: Uuid, from: i64) -> Result<()> {
        // Keep the reason on logical chunks, independent of old packs and task-history retention.
        sqlx::query("UPDATE chunks c SET repack_after=now()+$4*interval '1 second' FROM mokyu_meta g WHERE c.range_split_at IS NOT NULL AND c.repack_after<=now() AND c.id IN (SELECT chunk_id FROM extents WHERE stream_id=$1 AND offset_bytes>=$5 ORDER BY offset_bytes LIMIT 1026) AND (g.access_coverage_since IS NULL OR g.access_coverage_since>date_trunc('hour',now()-$2*interval '1 second') OR g.access_flushed_at IS NULL OR g.access_flushed_at<now()-$3*interval '1 second' OR EXISTS(SELECT 1 FROM chunk_access_windows w WHERE w.chunk_id=c.id AND w.range_reads>0 AND w.window_start>=date_trunc('hour',now()-$2*interval '1 second')))")
            .bind(stream).bind(config::seconds(&self.config.pack.range_repack_window)? as f64).bind((2*config::seconds(&self.config.statistics.access_flush_interval)?) as f64).bind(config::seconds(&self.config.pack.range_repack_retry_interval)? as f64).bind(from).execute(&self.db).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn range_benefit_includes_rewrite_and_extra_requests() {
        let c = crate::pack::Config::default();
        let mib = 1024 * 1024;
        assert!(
            Benefit::new(8 * 32 * mib, 8 * mib, 64 * mib, 0)
                .worthwhile(&c)
                .unwrap()
        );
        assert!(
            !Benefit::new(32 * mib, mib, 64 * mib, 0)
                .worthwhile(&c)
                .unwrap()
        );
        assert!(
            !Benefit::new(8 * 32 * mib, 8 * 16 * mib, 64 * mib, 0)
                .worthwhile(&c)
                .unwrap()
        );
        assert!(
            !Benefit::new(8 * 32 * mib, 8 * mib, 64 * mib, 4096)
                .worthwhile(&c)
                .unwrap()
        );
    }
}
