use crate::{
    app::{Active, App, StoredStream},
    codec::{Chunk, IntegrityError},
    config,
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

#[derive(Deserialize)]
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

#[derive(Clone, Copy, Default, Debug, Deserialize, Serialize, clap::ValueEnum, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    Metadata,
    Head,
    Full,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    #[serde(default)]
    pub mode: Mode,
    pub bucket: Option<String>,
    pub key: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct Detail {
    mode: Mode,
    phase: String,
    bucket_id: Option<Uuid>,
    bucket: Option<String>,
    key: Option<String>,
    upper_chunk_id: String,
    upper_object: Option<(Uuid, String)>,
    objects_checked: u64,
    chunks_checked: u64,
    bytes_checked: u64,
    issues: u64,
    skipped: u64,
    finished_at: Option<chrono::DateTime<chrono::Utc>>,
}
#[derive(Default, Deserialize, Serialize)]
struct Cursor {
    bucket: Option<Uuid>,
    key: Option<String>,
    stream: Option<Uuid>,
    after_extent: i64,
    position: i64,
    chunk: i64,
}
struct Finding {
    subject: String,
    code: String,
    chunk_id: Option<i64>,
    storage_id: Option<Uuid>,
    stream_id: Option<Uuid>,
    bucket_id: Option<Uuid>,
    key: Option<String>,
    detail: Value,
}
impl Finding {
    fn object(s: &StoredStream, code: &str, offset: i64, detail: Value) -> Self {
        Self {
            subject: format!("object:{}:{offset}", s.id),
            code: code.into(),
            chunk_id: None,
            storage_id: None,
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
            storage_id: Some(c.storage_id),
            stream_id: None,
            bucket_id: None,
            key: None,
            detail: json!({}),
        }
    }
}

impl App {
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
        // Limit queued work as well as running work; repeated clicks must not grow an unbounded queue.
        let busy: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE kind='integrity' AND state IN ('queued','running'))").fetch_one(&self.db).await?;
        if busy {
            return Err(
                s3s::s3_error!(OperationAborted, "an integrity check is already active").into(),
            );
        }
        let upper: i64 = sqlx::query_scalar("SELECT COALESCE(max(id),0) FROM chunks")
            .fetch_one(&self.db)
            .await?;
        let id = Uuid::new_v4();
        let mut detail = Detail {
            mode: input.mode,
            phase: "metadata".into(),
            bucket_id: bucket.as_ref().map(|b| b.id),
            bucket: input.bucket,
            key: input.key,
            upper_chunk_id: upper.to_string(),
            upper_object: None,
            objects_checked: 0,
            chunks_checked: 0,
            bytes_checked: 0,
            issues: 0,
            skipped: 0,
            finished_at: None,
        };
        let mut query = sqlx::QueryBuilder::new(
            "SELECT o.bucket_id,o.key FROM objects o WHERE o.stream_id IS NOT NULL",
        );
        scope(&mut query, &detail);
        query.push(" ORDER BY o.bucket_id DESC,o.key DESC LIMIT 1");
        detail.upper_object = query.build_query_as().fetch_optional(&self.db).await?;
        sqlx::query("INSERT INTO tasks(id,kind,bucket_id,state,detail) VALUES($1,'integrity',$2,'queued',$3)").bind(id).bind(detail.bucket_id).bind(serde_json::to_value(detail)?).execute(&self.db).await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id}))
    }

    pub async fn integrity_batch(
        self: &Arc<Self>,
        id: Uuid,
        saved: Option<&str>,
        detail: Value,
    ) -> Result<bool> {
        let mut d: Detail = serde_json::from_value(detail)?;
        let mut cursor: Cursor = saved
            .map(serde_json::from_str)
            .transpose()?
            .unwrap_or_default();
        let mut issues = Vec::new();
        if d.phase == "metadata" {
            self.inspect_mapping(&mut d, &mut cursor, &mut issues)
                .await?;
        } else if d.phase == "chunks" {
            let count = self
                .config
                .integrity
                .concurrency
                .min(self.budget.read_concurrency)
                .min(self.budget.data_slots.saturating_sub(1).max(1))
                .min(64);
            if d.mode != Mode::Metadata && Instant::now() < *self.integrity_pacing.lock().unwrap() {
                return Ok(false);
            }
            let mut query =
                sqlx::QueryBuilder::new("SELECT c.* FROM chunks c WHERE c.state='ready' AND c.id>");
            query.push_bind(cursor.chunk).push(" AND c.id<=").push_bind(d.upper_chunk_id.parse::<i64>()?).push(" AND EXISTS(SELECT 1 FROM extents e JOIN objects o ON o.stream_id=e.stream_id WHERE e.chunk_id=c.id");
            scope(&mut query, &d);
            query
                .push(") ORDER BY c.id LIMIT ")
                .push_bind(if d.mode == Mode::Metadata {
                    64
                } else {
                    count as i64
                });
            let rows: Vec<Chunk> = query.build_query_as().fetch_all(&self.db).await?;
            if rows.is_empty() {
                d.phase = "done".into();
            } else {
                let started = Instant::now();
                let requests = rows.len();
                let results: Vec<_> =
                    futures_util::stream::iter(rows.iter().map(|c| self.inspect_chunk(id, c, &d)))
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
                if d.mode != Mode::Metadata {
                    let request_delay = self
                        .config
                        .integrity
                        .requests_per_second
                        .map_or(0.0, |r| requests as f64 / r as f64);
                    let byte_delay = self
                        .config
                        .integrity
                        .bandwidth
                        .as_deref()
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
        let mut tx = self.db.begin().await?;
        for i in issues {
            let inserted = sqlx::query("INSERT INTO integrity_issues(task_id,subject,code,chunk_id,storage_id,stream_id,bucket_id,object_key,detail) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(task_id,subject,code) DO NOTHING")
                .bind(id).bind(i.subject).bind(&i.code).bind(i.chunk_id).bind(i.storage_id).bind(i.stream_id).bind(i.bucket_id).bind(i.key).bind(i.detail).execute(&mut *tx).await?.rows_affected();
            d.issues += inserted;
            if inserted > 0 {
                tracing::warn!(task_id=%id,chunk_id=?i.chunk_id,stream_id=?i.stream_id,code=%i.code,"integrity finding");
            }
        }
        sqlx::query("UPDATE tasks SET cursor=$2,detail=$3,processed=$4,updated_at=now(),state=CASE WHEN $5 AND state='running' THEN 'completed' ELSE state END WHERE id=$1")
            .bind(id).bind(serde_json::to_string(&cursor)?).bind(serde_json::to_value(&d)?).bind((d.objects_checked+d.chunks_checked) as i64).bind(d.phase=="done").execute(&mut *tx).await?;
        tx.commit().await?;
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
        let mut query = sqlx::QueryBuilder::new(
            "SELECT s.*,o.bucket_id AS visible_bucket,o.key AS visible_key FROM objects o JOIN streams s ON s.id=o.stream_id WHERE true",
        );
        scope(&mut query, d);
        if let Some(stream) = cursor.stream {
            query.push(" AND o.stream_id=").push_bind(stream);
        } else if let (Some(bucket), Some(key)) = (cursor.bucket, &cursor.key) {
            query
                .push(" AND (o.bucket_id,o.key)>(")
                .push_bind(bucket)
                .push(",")
                .push_bind(key)
                .push(")");
        }
        query.push(" ORDER BY o.bucket_id,o.key LIMIT 1");
        #[derive(sqlx::FromRow)]
        struct Object {
            #[sqlx(flatten)]
            stream: StoredStream,
            visible_bucket: Uuid,
            visible_key: String,
        }
        let row: Option<Object> = query.build_query_as().fetch_optional(&self.db).await?;
        let Some(row) = row else {
            if cursor.stream.take().is_some() {
                d.skipped += 1;
            } else {
                d.phase = "chunks".into();
            }
            return Ok(());
        };
        let mut s = row.stream;
        let identity_mismatch =
            s.bucket_id != row.visible_bucket || s.object_key != row.visible_key;
        s.bucket_id = row.visible_bucket;
        s.object_key = row.visible_key;
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
        if identity_mismatch || s.state != "ready" || s.kind != "object" {
            found.push(Finding::object(
                &s,
                "object_metadata",
                -1,
                json!({"state":s.state,"kind":s.kind}),
            ));
        }
        #[derive(sqlx::FromRow)]
        struct Range {
            offset_bytes: i64,
            length: i32,
            source_offset: i32,
            fragment_id: Option<Uuid>,
            chunk_id: Option<i64>,
            state: Option<String>,
            raw_size: Option<i32>,
        }
        let rows: Vec<Range> = sqlx::query_as("SELECT e.*,c.state,c.raw_size FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=$1 AND e.offset_bytes>$2 ORDER BY e.offset_bytes LIMIT 65")
            .bind(s.id).bind(cursor.after_extent).fetch_all(&self.db).await?;
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
        let current: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM objects WHERE bucket_id=$1 AND key=$2 AND stream_id=$3)",
        )
        .bind(s.bucket_id)
        .bind(&s.object_key)
        .bind(s.id)
        .fetch_one(&self.db)
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
        let mut query = sqlx::QueryBuilder::new(
            "SELECT o.stream_id FROM extents e JOIN objects o ON o.stream_id=e.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE c.state='ready' AND c.id=",
        );
        query.push_bind(c.id);
        scope(&mut query, d);
        query.push(" LIMIT 1");
        let id: Option<Uuid> = query.build_query_scalar().fetch_optional(&self.db).await?;
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
            Duration::from_secs(config::seconds(&self.config.integrity.request_timeout)?),
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
                finding.storage_id = physical_detail["storage_id"]
                    .as_str()
                    .and_then(|s| Uuid::parse_str(s).ok());
                finding.detail = physical_detail;
                Some(finding)
            }
        };
        Ok(Some((size, issue)))
    }

    pub async fn integrity_issues(&self, id: Uuid, after: i64, limit: i64) -> Result<Value> {
        if after < 0 || !(1..=200).contains(&limit) {
            return Err(s3s::s3_error!(InvalidArgument).into());
        }
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=$1 AND kind='integrity')",
        )
        .bind(id)
        .fetch_one(&self.db)
        .await?;
        if !exists {
            return Err(s3s::s3_error!(NoSuchKey).into());
        }
        let mut rows: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(i)||jsonb_build_object('id',i.id::text,'chunk_id',i.chunk_id::text) FROM integrity_issues i WHERE task_id=$1 AND id>$2 ORDER BY id LIMIT $3").bind(id).bind(after).bind(limit+1).fetch_all(&self.db).await?;
        let more = rows.len() > limit as usize;
        rows.truncate(limit as usize);
        let next = more.then(|| rows.last().unwrap()["id"].clone());
        Ok(json!({"issues":rows,"next_after":next}))
    }

    pub async fn integrity_objects(
        &self,
        id: Uuid,
        issue: i64,
        after: Option<(Uuid, String)>,
    ) -> Result<Value> {
        let row: Option<(Option<i64>, Option<Uuid>)> = sqlx::query_as(
            "SELECT chunk_id,stream_id FROM integrity_issues WHERE task_id=$1 AND id=$2",
        )
        .bind(id)
        .bind(issue)
        .fetch_optional(&self.db)
        .await?;
        let (chunk, stream) = row.ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        let mut query = sqlx::QueryBuilder::new(
            "SELECT jsonb_build_object('bucket_id',o.bucket_id,'bucket',b.name,'key',o.key,'version',o.stream_id) FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE ",
        );
        if let Some(chunk) = chunk {
            query
                .push(
                    "EXISTS(SELECT 1 FROM extents e WHERE e.stream_id=o.stream_id AND e.chunk_id=",
                )
                .push_bind(chunk)
                .push(")");
        } else {
            query.push("o.stream_id=").push_bind(stream);
        }
        if let Some((bucket, key)) = after {
            query
                .push(" AND (o.bucket_id,o.key)>(")
                .push_bind(bucket)
                .push(",")
                .push_bind(key)
                .push(")");
        }
        query.push(" ORDER BY o.bucket_id,o.key LIMIT 101");
        let mut rows: Vec<Value> = query.build_query_scalar().fetch_all(&self.db).await?;
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

fn scope(query: &mut sqlx::QueryBuilder<sqlx::Postgres>, d: &Detail) {
    if let Some(bucket) = d.bucket_id {
        query.push(" AND o.bucket_id=").push_bind(bucket);
    }
    if let Some(key) = &d.key {
        query.push(" AND o.key=").push_bind(key);
    }
    if let Some((bucket, key)) = &d.upper_object {
        query
            .push(" AND (o.bucket_id,o.key)<=(")
            .push_bind(*bucket)
            .push(",")
            .push_bind(key)
            .push(")");
    }
}
