use crate::model::Id;
use crate::model::task::Execution;
use crate::{
    app::App,
    config,
    storage::ReadContext,
    worker::packing::{Mapping, Output},
};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::collections::HashMap;
use uuid::Uuid;

// A conservative byte-equivalent charge for extra remote round trips, in addition to payloads.
const EXTRA_GET_BYTES: i64 = 64 * 1024;

#[derive(serde::Serialize)]
pub(crate) struct Benefit {
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
    pub(crate) fn worthwhile(&self, c: &crate::content::pack::Config) -> Result<bool> {
        Ok(self.net_savings_bytes > 0
            && self.net_savings_bytes as u64 >= config::bytes(&c.range_min_savings_bytes)?
            && i128::from(self.net_savings_bytes) * 100
                >= i128::from(self.observed_bytes) * i128::from(c.range_min_savings_percent))
    }
}

pub(crate) fn benefit(
    partial: i64,
    observed: i64,
    full: i64,
    old: i64,
    hits: &std::collections::BTreeMap<Id, i64>,
    sources: &[(Vec<Id>, i64)],
) -> Benefit {
    let mut traffic = 0i64;
    let mut requests = 0i64;
    let mut bytes = 0i64;
    for (ids, size) in sources {
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
}

impl App {
    pub(crate) async fn range_batch(&self, task: Execution, detail: &Value) -> Result<bool> {
        let storage: Uuid = detail["storage_id"]
            .as_str()
            .context("missing Range storage")?
            .parse()?;
        let settings = self.storage.pack_config(storage)?;
        if !settings.range_optimization {
            self.packing.complete_range_task(task).await?;
            return Ok(false);
        }
        let cutoff = detail["upper_time"]
            .as_str()
            .context("missing Range snapshot")?
            .parse::<chrono::DateTime<chrono::Utc>>()?;
        // Check only packs with new pressure evidence, at most once per sweep. Larger downloads go first.
        let id: Option<Id> = self
            .packing
            .next_range_pack(
                storage,
                cutoff,
                config::seconds(&settings.range_window)? as f64,
                i64::from(settings.range_min_downloads),
            )
            .await?;
        let Some(id) = id else {
            self.packing.complete_range_task(task).await?;
            return Ok(false);
        };
        let (processed, decision) = self.optimize_range_pack(task, id, &settings).await?;
        self.packing
            .record_range(task, id, processed, decision)
            .await?;
        Ok(true)
    }
    async fn optimize_range_pack(
        &self,
        task: Execution,
        id: Id,
        settings: &crate::content::pack::Config,
    ) -> Result<(usize, Value)> {
        let mut report = json!({"pack_id":id.to_string(),"applied":false});
        let blocked: bool = self.packing.range_pack_blocked(id).await?;
        if blocked {
            report["reason"] = json!("active_upload");
            return Ok((0, report));
        }
        let window = config::seconds(&settings.range_window)? as f64;
        let (partial, observed, full): (i64, i64, i64) =
            self.packing.pack_range_totals(id, window).await?;
        let hits: HashMap<Id, i64> = self
            .packing
            .pack_member_hits(id, window)
            .await?
            .into_iter()
            .collect();
        let (old, raw, count): (i64, i64, i32) = self.packing.pack_dimensions(id).await?;
        let rows: Vec<Mapping> = self.packing.range_pack_layout(id).await?;
        if rows.len() != count as usize
            || rows
                .iter()
                .map(|r| i64::from(r.chunk.raw_size))
                .sum::<i64>()
                != raw
            || hits.values().sum::<i64>() < partial
            || partial < i64::from(settings.range_min_downloads)
            || observed == 0
        {
            report["reason"] = json!("incomplete_evidence_or_layout");
            return Ok((0, report));
        }
        let mut groups: Vec<Vec<Mapping>> = Vec::new();
        for row in &rows {
            let join = if let Some(last) = groups.last().and_then(|g| g.last()) {
                self.pack_creation_allowed()
                    && settings.enabled
                    && hits.contains_key(&last.chunk.id) == hits.contains_key(&row.chunk.id)
                    && last.offset_bytes + i64::from(last.chunk.raw_size) == row.offset_bytes
                    && groups
                        .last()
                        .unwrap()
                        .iter()
                        .map(|r| r.chunk.raw_size as u64)
                        .sum::<u64>()
                        + row.chunk.raw_size as u64
                        <= config::bytes(&settings.max_size)?
                    && self
                        .packing
                        .same_references(
                            last.chunk.id,
                            row.chunk.id,
                            row.offset_bytes - last.offset_bytes,
                        )
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
        let hits = hits.into_iter().collect();
        let evaluate =
            |sources: Vec<(Vec<Id>, i64)>| benefit(partial, observed, full, old, &hits, &sources);
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
                                12 + crate::content::pack::MEMBER_SIZE as i64 * g.len() as i64
                            } else {
                                0
                            },
                    )
                })
                .collect(),
        );
        report["partial_downloads"] = json!(partial);
        report["benefit"] = serde_json::to_value(&predicted)?;
        if !predicted.worthwhile(settings)? {
            report["reason"] = json!("insufficient_net_savings");
            return Ok((0, report));
        }
        self.pin_pack_inputs(task, &rows).await?;
        let context = ReadContext::default();
        let mut outputs: Vec<Output> = Vec::new();
        let mut fills = Vec::new();
        for group in &groups {
            outputs.extend(
                self.pack_output(
                    task,
                    group,
                    &context,
                    None,
                    "",
                    self.pack_creation_allowed(),
                    None,
                    &mut fills,
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
        if !actual.worthwhile(settings)? {
            report["reason"] = json!("encoded_layout_not_beneficial");
            return Ok((0, report));
        }
        self.commit_outputs(task, &rows, &outputs, true).await?;
        crate::storage::cache::publish_fills(fills);
        report["applied"] = json!(true);
        report["reason"] = json!("repeated_partial_downloads");
        Ok((rows.len(), report))
    }
    pub(crate) async fn defer_range_repack(
        &self,
        stream: Id,
        from: i64,
        settings: &crate::content::pack::Config,
    ) -> Result<()> {
        // Keep the reason on logical chunks, independent of old packs and task-history retention.
        self.packing
            .defer_range_repack(
                stream,
                config::seconds(&settings.range_repack_window)? as f64,
                (2 * config::seconds(&self.config.metrics.access_flush_interval)?) as f64,
                config::seconds(&settings.range_repack_retry_interval)? as f64,
                from,
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn range_benefit_includes_rewrite_and_extra_requests() {
        let c = crate::content::pack::Config::default();
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
