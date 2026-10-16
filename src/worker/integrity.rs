use crate::model::task::Execution;
use crate::{
    app::{Active, App, StoredStream},
    config,
    content::codec::{Chunk, IntegrityError},
};
use anyhow::{Context, Result, ensure};
use futures_util::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub concurrency: usize,
    pub requests_per_second: Option<u32>,
    pub bandwidth: Option<String>,
    pub request_timeout: String,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            concurrency: 1,
            requests_per_second: None,
            bandwidth: None,
            request_timeout: "30s".into(),
        }
    }
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.concurrency > 0,
            "integrity.concurrency must be positive"
        );
        ensure!(
            self.requests_per_second.is_none_or(|n| n > 0),
            "integrity.requests_per_second must be positive"
        );
        if let Some(v) = &self.bandwidth {
            config::bytes(v)?;
        }
        ensure!(
            config::seconds(&self.request_timeout)? <= 120,
            "integrity.request_timeout must be 1s..120s"
        );
        Ok(())
    }
}

#[derive(
    Clone,
    Copy,
    Default,
    Debug,
    Deserialize,
    Serialize,
    clap::ValueEnum,
    utoipa::ToSchema,
    PartialEq,
)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    Metadata,
    Head,
    Full,
}

#[derive(Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct Request {
    #[serde(default)]
    pub mode: Mode,
    pub bucket: Option<String>,
    pub key: Option<String>,
}

use crate::model::integrity::{Cursor, Detail, Finding};
impl Finding {
    fn object(s: &StoredStream, code: &str, offset: i64, detail: Value) -> Self {
        Self {
            subject: format!("object:{}:{offset}", s.id),
            code: code.into(),
            chunk_id: None,
            location_id: None,
            stream_id: Some(s.id),
            bucket_id: Some(s.bucket_id),
            key: Some(s.object_key.clone()),
            detail,
        }
    }
    fn chunk(c: &Chunk, code: &str) -> Self {
        Self {
            subject: format!("chunk:{}", c.id),
            code: code.into(),
            chunk_id: Some(c.id),
            location_id: Some(c.location_id),
            stream_id: None,
            bucket_id: None,
            key: None,
            detail: json!({}),
        }
    }
}

impl App {
    async fn acquire_profile_rate(
        &self,
        storage_id: Uuid,
        profile: &crate::storage::profile::StorageProfile,
        requests: usize,
        bytes: i64,
    ) -> Result<bool> {
        let request_rate = profile.integrity_rps.map(|rate| rate as f64);
        let byte_rate = profile
            .integrity_bandwidth
            .as_deref()
            .map(config::bytes)
            .transpose()?
            .map(|rate| rate as f64);
        if self.role == "single" {
            return self.storage.acquire_integrity_rate(
                storage_id,
                request_rate.or(self
                    .config
                    .local_integrity
                    .requests_per_second
                    .map(f64::from)),
                byte_rate.or(self
                    .config
                    .local_integrity
                    .bandwidth
                    .as_deref()
                    .map(config::bytes)
                    .transpose()?
                    .map(|rate| rate as f64)),
                requests as f64,
                bytes.max(0) as f64,
            );
        }
        let mut rates = Vec::new();
        if let Some(rate) = request_rate {
            rates.push(("integrity_rps", rate, requests as f64));
        }
        if let Some(rate) = byte_rate {
            rates.push(("integrity_bandwidth", rate, bytes.max(0) as f64));
        }
        if rates.is_empty() {
            return Ok(true);
        }
        self.cluster()?
            .acquire_integrity_rate(storage_id, &rates)
            .await
    }

    pub async fn integrity_start(&self, input: Request) -> Result<Value> {
        if input
            .key
            .as_ref()
            .is_some_and(|k| k.is_empty() || k.len() > 1024 || input.bucket.is_none())
        {
            return Err(s3s::s3_error!(
                InvalidArgument,
                "key requires a bucket and 1..1024 UTF-8 bytes"
            )
            .into());
        }
        let bucket = if let Some(name) = &input.bucket {
            Some(self.bucket(name, false).await?)
        } else {
            None
        };
        if let (Some(b), Some(key)) = (&bucket, &input.key) {
            self.current(b.id, key).await?;
        }
        let _coord = self.coord.lock().await;
        self.ensure_mode()?;
        let (task, child_count) = self
            .integrity_store
            .start(input, bucket, self.node.get().map(|node| node.node_id))
            .await?;
        self.wake_tasks.notify_waiters();
        let mut result = json!({"task_id":task});
        if self.role != "single" {
            result["children"] = json!(child_count);
        }
        Ok(result)
    }

    pub async fn integrity_batch(
        self: &Arc<Self>,
        id: Execution,
        saved: Option<&str>,
        detail: Value,
    ) -> Result<bool> {
        let mut d: Detail = serde_json::from_value(detail)?;
        let mut cursor: Cursor = saved
            .map(serde_json::from_str)
            .transpose()?
            .unwrap_or_default();
        let mut issues = Vec::new();
        let profile = if let Some(storage_id) = d.storage_id {
            Some(self.storage_profiles.load(storage_id).await?)
        } else {
            None
        };
        let _storage_permit = if let Some(storage_id) = d.storage_id {
            Some(self.storage.profile_permit(storage_id, "integrity").await?)
        } else {
            None
        };
        if d.phase == "metadata" {
            self.inspect_mapping(&mut d, &mut cursor, &mut issues)
                .await?;
        } else if d.phase == "chunks" {
            let count = profile
                .as_ref()
                .map_or(self.config.local_integrity.concurrency, |p| {
                    p.integrity_concurrency as usize
                })
                .min(self.budget.read_concurrency)
                .min(self.budget.data_slots.saturating_sub(1).max(1))
                .min(64);
            // Single mode already admits each storage against its own token balance.
            if self.role != "single"
                && d.mode != Mode::Metadata
                && Instant::now() < *self.integrity_pacing.lock().unwrap()
            {
                return Ok(false);
            }
            let mut rows = self
                .integrity_store
                .chunks(
                    &d,
                    cursor.chunk,
                    if d.mode == Mode::Metadata {
                        64
                    } else {
                        count as i64
                    },
                )
                .await?;
            if let Some(limit) = profile
                .as_ref()
                .and_then(|p| p.integrity_byte_budget.as_deref())
                .map(config::bytes)
                .transpose()?
            {
                let mut used = 0u64;
                let keep = rows
                    .iter()
                    .enumerate()
                    .take_while(|(_, chunk)| {
                        let size = chunk.raw_size.max(0) as u64;
                        if used > 0 && used.saturating_add(size) > limit {
                            return false;
                        }
                        used = used.saturating_add(size);
                        true
                    })
                    .count();
                rows.truncate(keep.max(1).min(rows.len()));
            }
            if rows.is_empty() {
                d.phase = "done".into();
            } else {
                if let Some(storage_id) = d.storage_id {
                    let estimated_bytes = rows.iter().map(|chunk| chunk.raw_size as i64).sum();
                    if !self
                        .acquire_profile_rate(
                            storage_id,
                            profile.as_ref().context("missing storage profile")?,
                            rows.len(),
                            estimated_bytes,
                        )
                        .await?
                    {
                        #[cfg(feature = "fault-injection")]
                        crate::faults::point("integrity-rate-waiting").await;
                        return Ok(false);
                    }
                }
                let started = Instant::now();
                let requests = rows.len();
                let results: Vec<_> = futures_util::stream::iter(
                    rows.iter().map(|c| self.inspect_chunk(id.id, c, &d)),
                )
                .buffered(count)
                .try_collect()
                .await?;
                let mut bytes = 0;
                for (c, result) in rows.iter().zip(results) {
                    cursor.chunk = c.id;
                    match result {
                        None => d.skipped += 1,
                        Some((size, issue)) => {
                            d.chunks_checked += 1;
                            bytes += size;
                            if let Some(issue) = issue {
                                issues.push(issue);
                            }
                        }
                    }
                }
                d.bytes_checked += bytes;
                if self.role != "single" && d.mode != Mode::Metadata {
                    let request_delay = profile
                        .as_ref()
                        .and_then(|p| p.integrity_rps)
                        .map(|r| r as f64)
                        .or_else(|| {
                            self.config
                                .local_integrity
                                .requests_per_second
                                .map(f64::from)
                        })
                        .map_or(0.0, |r| requests as f64 / r);
                    let byte_delay = profile
                        .as_ref()
                        .and_then(|p| p.integrity_bandwidth.as_deref())
                        .or(self.config.local_integrity.bandwidth.as_deref())
                        .map(config::bytes)
                        .transpose()?
                        .map_or(0.0, |r| bytes as f64 / r as f64);
                    *self.integrity_pacing.lock().unwrap() = started
                        .checked_add(Duration::from_secs_f64(request_delay.max(byte_delay)))
                        .context("integrity rate delay overflow")?;
                }
            }
        }
        if d.phase == "done" {
            d.finished_at = Some(chrono::Utc::now());
        }
        #[cfg(feature = "fault-injection")]
        crate::faults::point("integrity-before-commit").await;
        self.integrity_store
            .checkpoint(id, serde_json::to_string(&cursor)?, d, issues)
            .await?;
        Ok(true)
    }

    async fn inspect_mapping(
        &self,
        d: &mut Detail,
        cursor: &mut Cursor,
        issues: &mut Vec<Finding>,
    ) -> Result<()> {
        if d.upper_object.is_none() {
            d.phase = "done".into();
            return Ok(());
        }
        let coord = self.coord.lock().await;
        let row = self.integrity_store.mapping(d, cursor).await?;
        let Some((mut s, visible_bucket, visible_key)) = row else {
            if cursor.stream.take().is_some() {
                d.skipped += 1;
            } else {
                d.phase = "chunks".into();
            }
            return Ok(());
        };
        let identity_mismatch = s.bucket_id != visible_bucket || s.object_key != visible_key;
        s.bucket_id = visible_bucket;
        s.object_key = visible_key;
        let pin = self.pin(s.id);
        if cursor.stream.is_none() {
            cursor.bucket = Some(s.bucket_id);
            cursor.key = Some(s.object_key.clone());
            cursor.stream = Some(s.id);
            cursor.after_extent = -1;
            cursor.position = 0;
        }
        drop(coord);
        #[cfg(feature = "fault-injection")]
        crate::faults::point("integrity-mapping-pinned").await;
        let mut found = Vec::new();
        if identity_mismatch || s.state != "ready" || s.stream_type != "object" {
            found.push(Finding::object(
                &s,
                "object_metadata",
                -1,
                json!({"state":s.state,"stream_type":s.stream_type}),
            ));
        }
        let rows = self
            .integrity_store
            .ranges(s.id, cursor.after_extent)
            .await?;
        let more = rows.len() > 64;
        for r in rows.iter().take(64) {
            if r.offset_bytes != cursor.position {
                found.push(Finding::object(&s,"mapping_gap",r.offset_bytes,json!({"expected_offset":cursor.position.to_string(),"actual_offset":r.offset_bytes.to_string()})));
            }
            if r.fragment_id.is_some()
                || r.chunk_id.is_none()
                || r.state.as_deref() != Some("ready")
                || r.raw_size
                    .is_none_or(|n| i64::from(r.source_offset) + i64::from(r.length) > i64::from(n))
            {
                found.push(Finding::object(
                    &s,
                    "mapping_source",
                    r.offset_bytes,
                    json!({"chunk_id":r.chunk_id.map(|n|n.to_string())}),
                ));
            }
            cursor.position = r
                .offset_bytes
                .checked_add(i64::from(r.length))
                .unwrap_or_else(|| {
                    found.push(Finding::object(
                        &s,
                        "mapping_source",
                        r.offset_bytes,
                        json!({"overflow":true}),
                    ));
                    i64::MAX
                });
            cursor.after_extent = r.offset_bytes;
        }
        if !more {
            if cursor.position != s.size {
                found.push(Finding::object(
                    &s,
                    "object_length",
                    -1,
                    json!({"expected":s.size.to_string(),"actual":cursor.position.to_string()}),
                ));
            }
            cursor.stream = None;
        }
        let current: bool = self
            .integrity_store
            .current(s.bucket_id, &s.object_key, s.id)
            .await?;
        if current {
            issues.extend(found);
            if !more {
                d.objects_checked += 1;
            }
        } else {
            d.skipped += 1;
            cursor.stream = None;
        }
        drop(pin);
        Ok(())
    }

    async fn chunk_pin(&self, c: &Chunk, d: &Detail) -> Result<Option<Active>> {
        let _coord = self.coord.lock().await;
        let id = self.integrity_store.chunk_stream(c.id, d).await?;
        Ok(id.map(|id| self.pin(id)))
    }
    async fn inspect_chunk(
        &self,
        task: Uuid,
        c: &Chunk,
        d: &Detail,
    ) -> Result<Option<(u64, Option<Finding>)>> {
        let _slot = self.slots.acquire().await?;
        let Some(_pin) = self.chunk_pin(c, d).await? else {
            return Ok(None);
        };
        #[cfg(feature = "fault-injection")]
        crate::faults::point("integrity-chunk-pinned").await;
        let mut size = 0;
        let mut physical_detail = json!({});
        let result = async {
            size = self
                .storage
                .inspect(task, c, d.mode, &mut physical_detail)
                .await?;
            Ok::<_, anyhow::Error>(())
        };
        let result = tokio::time::timeout(
            Duration::from_secs(config::seconds(
                &self.config.local_integrity.request_timeout,
            )?),
            result,
        )
        .await
        .context("integrity backend timeout; resume to retry")?;
        let issue = match result {
            Ok(()) => None,
            Err(e) => {
                let code = if let Some(e) = e.downcast_ref::<IntegrityError>() {
                    e.code()
                } else if matches!(
                    e.downcast_ref::<object_store::Error>(),
                    Some(object_store::Error::NotFound { .. })
                ) {
                    "remote_missing"
                } else {
                    let category = if matches!(
                        e.downcast_ref::<object_store::Error>(),
                        Some(
                            object_store::Error::PermissionDenied { .. }
                                | object_store::Error::Unauthenticated { .. }
                        )
                    ) {
                        "backend_permission"
                    } else {
                        "backend_request"
                    };
                    let diagnostic: String = format!("{e:#}").chars().take(2048).collect();
                    anyhow::bail!("{category}: {diagnostic}; resume to retry");
                };
                // A concurrent deletion/replacement is not evidence of damaged live data.
                if self.chunk_pin(c, d).await?.is_none() {
                    return Ok(None);
                }
                let mut finding = Finding::chunk(c, code);
                finding.location_id = physical_detail["location_id"]
                    .as_str()
                    .and_then(|s| Uuid::parse_str(s).ok());
                finding.detail = physical_detail;
                Some(finding)
            }
        };
        Ok(Some((size, issue)))
    }

    pub async fn integrity_issues(&self, id: Uuid, after: i64, limit: i64) -> Result<Value> {
        self.integrity_issues_filtered(id, after, limit, None).await
    }
    pub(crate) async fn integrity_issues_filtered(
        &self,
        id: Uuid,
        after: i64,
        limit: i64,
        code: Option<&str>,
    ) -> Result<Value> {
        if code.is_some_and(|s| s.len() > 64) {
            return Err(s3s::s3_error!(InvalidArgument).into());
        }
        if after < 0 || !(1..=200).contains(&limit) {
            return Err(s3s::s3_error!(InvalidArgument).into());
        }
        let exists: bool = self.integrity_store.exists(id).await?;
        if !exists {
            return Err(s3s::s3_error!(NoSuchKey).into());
        }
        let mut rows: Vec<Value> = self
            .integrity_store
            .findings(id, after, limit + 1, code)
            .await?;
        let more = rows.len() > limit as usize;
        rows.truncate(limit as usize);
        let next = more.then(|| rows.last().unwrap()["id"].clone());
        let groups: Vec<(String, String)> = self.integrity_store.groups(id).await?;
        Ok(
            json!({"issues":rows,"next_after":next,"groups":groups.into_iter().collect::<std::collections::BTreeMap<_,_>>()}),
        )
    }

    pub async fn integrity_objects(
        &self,
        id: Uuid,
        issue: i64,
        after: Option<(Uuid, String)>,
    ) -> Result<Value> {
        let mut rows = self.integrity_store.objects(id, issue, after).await?;
        let more = rows.len() > 100;
        rows.truncate(100);
        let next = more.then(|| {
            json!([
                rows.last().unwrap()["bucket_id"],
                rows.last().unwrap()["key"]
            ])
        });
        Ok(json!({"objects":rows,"next":next}))
    }
}
