use crate::{app::App, config};
use anyhow::Result;
use bytes::Buf;
use chrono::{DateTime, Utc};
use hyper::{
    Method, Response,
    body::{Body, Frame, SizeHint},
};
use serde_json::{Value, json};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

#[derive(Default)]
pub struct Counters {
    started: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    canceled: AtomicU64,
    active: AtomicU64,
    client_errors: AtomicU64,
    server_errors: AtomicU64,
    bytes: AtomicU64,
    micros: AtomicU64,
    // Fixed buckets: <=1ms, <=2ms, ..., <=2^23ms, then overflow.
    latency: [AtomicU64; 25],
}

pub struct Operation {
    counters: Arc<Counters>,
    start: Instant,
    finished: bool,
}

impl Counters {
    pub fn begin(self: &Arc<Self>) -> Operation {
        self.started.fetch_add(1, Relaxed);
        self.active.fetch_add(1, Relaxed);
        Operation {
            counters: self.clone(),
            start: Instant::now(),
            finished: false,
        }
    }
    pub fn status(&self, status: u16) {
        if (400..500).contains(&status) {
            self.client_errors.fetch_add(1, Relaxed);
        }
        if status >= 500 {
            self.server_errors.fetch_add(1, Relaxed);
        }
    }
    pub fn bytes(&self, bytes: u64) {
        self.bytes.fetch_add(bytes, Relaxed);
    }
    pub fn snapshot(&self) -> Value {
        let completed = self.completed.load(Relaxed);
        let failed = self.failed.load(Relaxed);
        let canceled = self.canceled.load(Relaxed);
        let buckets = self.latency.each_ref().map(|n| n.load(Relaxed));
        let percentile = |percent: u64| -> Option<u64> {
            let total: u64 = buckets.iter().sum();
            if total == 0 {
                return None;
            }
            let target = total.saturating_mul(percent).div_ceil(100);
            let mut count = 0;
            for (index, n) in buckets.iter().enumerate() {
                count += n;
                if count >= target {
                    return (index < 24).then(|| 1u64 << index);
                }
            }
            None
        };
        json!({"started":self.started.load(Relaxed),"completed":completed,
            "active":self.active.load(Relaxed),"failed":failed,"canceled":canceled,
            "client_errors":self.client_errors.load(Relaxed),"server_errors":self.server_errors.load(Relaxed),
            "bytes":self.bytes.load(Relaxed),
            "failure_rate":ratio(failed + canceled, completed).map(|v| v.min(1.0)),
            "duration_ms":{"mean":ratio(self.micros.load(Relaxed),completed).map(|v|v/1000.0),
                "p50":percentile(50),"p95":percentile(95),"p99":percentile(99)}})
    }
}

pub fn ratio(n: u64, d: u64) -> Option<f64> {
    (d != 0).then(|| n as f64 / d as f64)
}

pub async fn process_memory() -> Value {
    let status = tokio::fs::read_to_string("/proc/self/status")
        .await
        .unwrap_or_default();
    let bytes = |key: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(key))
            .and_then(|v| v.split_whitespace().next()?.parse::<u64>().ok())
            .and_then(|v| v.checked_mul(1024))
    };
    json!({"rss_bytes":bytes("VmRSS:"),"peak_rss_bytes":bytes("VmHWM:")})
}

#[derive(Clone)]
pub struct RequestContext {
    pub id: String,
    pub failed: Arc<AtomicBool>,
}
pub struct Request {
    pub context: RequestContext,
    pub span: tracing::Span,
    operation: Operation,
    method: Method,
    status: u16,
    bytes: u64,
    expected_bytes: Option<u64>,
}
impl Request {
    pub fn new(counters: &Arc<Counters>, listener: &'static str, method: Method) -> Self {
        let id = uuid::Uuid::new_v4().to_string();
        let span = tracing::info_span!("request",request_id=%id,listener,method=%method);
        Self {
            context: RequestContext {
                id,
                failed: Arc::new(AtomicBool::new(false)),
            },
            span,
            operation: counters.begin(),
            method,
            status: 0,
            bytes: 0,
            expected_bytes: None,
        }
    }
    pub fn response<B: Body>(
        mut self,
        mut response: Response<B>,
        s3: bool,
    ) -> Response<ObservedBody<B>> {
        let value = self
            .context
            .id
            .parse()
            .expect("UUID is a valid HTTP header");
        response.headers_mut().insert("x-request-id", value);
        if s3 {
            response
                .headers_mut()
                .insert("x-amz-request-id", self.context.id.parse().unwrap());
        }
        self.status = response.status().as_u16();
        self.operation.counters.status(self.status);
        self.expected_bytes = response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());
        let no_body = self.method == Method::HEAD
            || matches!(self.status, 204 | 304)
            || self.expected_bytes == Some(0)
            || response.body().is_end_stream();
        if no_body {
            self.finish(false);
        }
        response.map(|inner| ObservedBody {
            inner: Box::pin(inner),
            request: self,
        })
    }
    fn finish(&mut self, stream_error: bool) {
        if self.operation.finished {
            return;
        }
        let application_error = self.context.failed.load(Relaxed);
        let failed = self.status >= 400 || stream_error || application_error;
        self.operation.finish(failed);
        let _entered = self.span.enter();
        let duration_ms = self.operation.start.elapsed().as_secs_f64() * 1000.0;
        if failed {
            tracing::warn!(
                status = self.status,
                bytes = self.bytes,
                duration_ms,
                stream_error,
                application_error,
                "request finished"
            );
        } else {
            tracing::debug!(
                status = self.status,
                bytes = self.bytes,
                duration_ms,
                "request finished"
            );
        }
    }
}
impl Drop for Request {
    fn drop(&mut self) {
        if !self.operation.finished {
            let _entered = self.span.enter();
            tracing::warn!(
                status = self.status,
                bytes = self.bytes,
                duration_ms = self.operation.start.elapsed().as_secs_f64() * 1000.0,
                "request canceled before completion"
            );
        }
    }
}

pub struct ObservedBody<B> {
    inner: Pin<Box<B>>,
    request: Request,
}
impl<B: Body> Body for ObservedBody<B> {
    type Data = B::Data;
    type Error = B::Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let span = this.request.span.clone();
        let _entered = span.enter();
        let result = this.inner.as_mut().poll_frame(cx);
        match &result {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    let bytes = data.remaining() as u64;
                    this.request.bytes += bytes;
                    this.request.operation.counters.bytes(bytes);
                }
                // Hyper may stop polling as soon as Content-Length bytes have been produced.
                if this
                    .request
                    .expected_bytes
                    .is_some_and(|n| this.request.bytes >= n)
                    || this.inner.is_end_stream()
                {
                    this.request.finish(
                        this.request
                            .expected_bytes
                            .is_some_and(|n| this.request.bytes != n),
                    );
                }
            }
            Poll::Ready(None) => this.request.finish(
                this.request
                    .expected_bytes
                    .is_some_and(|n| this.request.bytes != n),
            ),
            Poll::Ready(Some(Err(_))) => this.request.finish(true),
            Poll::Pending => {}
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

// s3s serializes its errors into an in-memory XML body; successful object bodies stay streaming.
pub fn s3_request_id(response: &mut s3s::HttpResponse, id: &str) {
    if !(response.status().is_client_error() || response.status().is_server_error()) {
        return;
    }
    let Some(bytes) = response.body().bytes() else {
        return;
    };
    let Ok(xml) = std::str::from_utf8(&bytes) else {
        return;
    };
    let Some(end) = xml.strip_suffix("</Error>") else {
        return;
    };
    let mut xml = end.to_owned();
    if let Some(start) = xml.find("<RequestId>")
        && let Some(end) = xml[start..].find("</RequestId>")
    {
        xml.replace_range(start..start + end + 12, "");
    }
    xml.push_str(&format!("<RequestId>{id}</RequestId></Error>"));
    response
        .headers_mut()
        .insert("content-length", xml.len().to_string().parse().unwrap());
    *response.body_mut() = s3s::Body::from(xml);
}

impl Operation {
    pub fn finish(&mut self, failed: bool) {
        if !self.finished {
            if failed {
                self.counters.failed.fetch_add(1, Relaxed);
            }
            self.record();
        }
    }
    fn record(&mut self) {
        let elapsed = self.start.elapsed();
        let ms = elapsed.as_micros().div_ceil(1000).max(1) as u64;
        let index = (64 - (ms - 1).leading_zeros() as usize).min(24);
        self.counters.latency[index].fetch_add(1, Relaxed);
        self.counters
            .micros
            .fetch_add(elapsed.as_micros() as u64, Relaxed);
        self.counters.active.fetch_sub(1, Relaxed);
        self.counters.completed.fetch_add(1, Relaxed);
        self.finished = true;
    }
}
impl Drop for Operation {
    fn drop(&mut self) {
        if !self.finished {
            self.counters.canceled.fetch_add(1, Relaxed);
            self.record();
        }
    }
}

pub struct Statistics {
    started: Instant,
    started_at: DateTime<Utc>,
    pub http: [Arc<Counters>; 3],
    pub gc_deleted: AtomicU64,
    pub gc_failures: AtomicU64,
    inventory: Mutex<Value>,
}
impl Default for Statistics {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            started_at: Utc::now(),
            http: std::array::from_fn(|_| Arc::default()),
            gc_deleted: 0.into(),
            gc_failures: 0.into(),
            inventory: Mutex::new(
                json!({"snapshot":null,"collecting":false,"last_attempt_at":null,"last_error":null}),
            ),
        }
    }
}
impl Statistics {
    pub fn runtime(&self) -> Value {
        json!({"started_at":self.started_at,"uptime_seconds":self.started.elapsed().as_secs(),
            "http":{"s3":self.http[0].snapshot(),"web":self.http[1].snapshot(),"manage":self.http[2].snapshot()},
            "gc_deleted":self.gc_deleted.load(Relaxed),"gc_failures":self.gc_failures.load(Relaxed)})
    }
    pub fn inventory(&self, interval: u64) -> Value {
        let mut value = self.inventory.lock().unwrap().clone();
        let age = value["snapshot"]["as_of"]
            .as_str()
            .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
            .map(|v| Utc::now().signed_duration_since(v).num_seconds().max(0) as u64);
        value["age_seconds"] = json!(age);
        value["stale"] = json!(
            !value["last_error"].is_null() || age.is_none_or(|v| v > interval.saturating_mul(2))
        );
        value["refresh_interval_seconds"] = json!(interval);
        value
    }
}

async fn collect(app: &App) -> Result<Value> {
    let mut tx = app.db.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    // One worker, bounded working memory and wait time; large sorts may spill to PostgreSQL disk.
    sqlx::raw_sql("SET LOCAL work_mem='16MB'; SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL lock_timeout='1s'")
        .execute(&mut *tx).await?;
    let timeout = config::seconds(&app.config.statistics.query_timeout)? * 1000;
    sqlx::query("SELECT set_config('statement_timeout',$1,true)")
        .bind(timeout.to_string())
        .execute(&mut *tx)
        .await?;
    let as_of: DateTime<Utc> = sqlx::query_scalar("SELECT transaction_timestamp()")
        .fetch_one(&mut *tx)
        .await?;
    let buckets: Vec<Value> = sqlx::query_scalar(
        "SELECT jsonb_build_object('id',b.id,'name',b.name,'objects',count(o.stream_id),'logical_bytes',COALESCE(sum(s.size),0))
         FROM buckets b LEFT JOIN objects o ON o.bucket_id=b.id AND o.stream_id IS NOT NULL
         LEFT JOIN streams s ON s.id=o.stream_id
         GROUP BY GROUPING SETS ((b.id,b.name),()) ORDER BY b.id NULLS FIRST LIMIT 1002")
        .fetch_all(&mut *tx).await?;
    let chunks: Value = sqlx::query_scalar(
        "SELECT jsonb_build_object(
            'states',COALESCE(jsonb_object_agg(state,n),'{}'),
            'stored_bytes',COALESCE(sum(bytes) FILTER(WHERE state IN ('ready','deleting')),0),
            'unconfirmed_bytes',COALESCE(sum(bytes) FILTER(WHERE state IN ('uploading','failed')),0))
         FROM (SELECT state,count(*) n,COALESCE(sum(stored_size),0) bytes FROM chunks GROUP BY state) c")
        .fetch_one(&mut *tx).await?;
    let live: Value = sqlx::query_scalar(
        "WITH refs AS (
            SELECT e.chunk_id,sum(e.length) bytes FROM objects o JOIN extents e ON e.stream_id=o.stream_id
            WHERE e.chunk_id IS NOT NULL GROUP BY e.chunk_id)
         SELECT jsonb_build_object('chunks',count(*),'reference_bytes',COALESCE(sum(r.bytes),0),
            'raw_bytes',COALESCE(sum(c.raw_size),0),'stored_bytes',COALESCE(sum(c.stored_size),0),
            'payload_bytes',COALESCE(sum(c.stored_size-CASE WHEN c.algorithm='none' THEN 0 ELSE 16 END),0))
         FROM refs r JOIN chunks c ON c.id=r.chunk_id")
        .fetch_one(&mut *tx).await?;
    let grace = config::seconds(&app.config.gc.unreferenced_grace)? as f64;
    let unreferenced: Value = sqlx::query_scalar(
        "SELECT jsonb_build_object('chunks',count(*),'stored_bytes',COALESCE(sum(stored_size),0),
            'eligible_chunks',count(*) FILTER(WHERE unreferenced_at<now()-$1*interval '1 second' AND owner_stream IS NULL),
            'eligible_bytes',COALESCE(sum(stored_size) FILTER(WHERE unreferenced_at<now()-$1*interval '1 second' AND owner_stream IS NULL),0))
         FROM chunks c WHERE state IN ('ready','failed','deleting') AND unreferenced_at IS NOT NULL
            AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)")
        .bind(grace).fetch_one(&mut *tx).await?;
    let tasks: Value = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_object_agg(state,n),'{}') FROM (SELECT state,count(*) n FROM tasks GROUP BY state) t")
        .fetch_one(&mut *tx).await?;
    let uploads: Value = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_object_agg(state,n),'{}') FROM (SELECT state,count(*) n FROM uploads WHERE state IN ('active','completing') GROUP BY state) u")
        .fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(
        json!({"as_of":as_of,"collected_at":Utc::now(),"objects":buckets[0]["objects"],
        "logical_bytes":buckets[0]["logical_bytes"],"buckets":buckets.iter().skip(1).take(1000).collect::<Vec<_>>(),
        "buckets_truncated":buckets.len()>1001,"chunks":chunks,"live":live,
        "unreferenced":unreferenced,"tasks":tasks,"uploads":uploads}),
    )
}

pub async fn run(app: Arc<App>) -> Result<()> {
    let interval = Duration::from_secs(config::seconds(&app.config.statistics.refresh_interval)?);
    let timeout = Duration::from_secs(config::seconds(&app.config.statistics.query_timeout)?);
    loop {
        {
            let mut saved = app.statistics.inventory.lock().unwrap();
            saved["collecting"] = json!(true);
            saved["last_attempt_at"] = json!(Utc::now());
        }
        let result = tokio::time::timeout(timeout, collect(&app)).await;
        {
            let mut saved = app.statistics.inventory.lock().unwrap();
            saved["collecting"] = json!(false);
            match result {
                Ok(Ok(snapshot)) => {
                    saved["snapshot"] = snapshot;
                    saved["last_error"] = Value::Null;
                }
                error => {
                    tracing::warn!(
                        ?error,
                        "storage statistics refresh failed; keeping previous snapshot"
                    );
                    saved["last_error"] = json!("refresh_failed");
                }
            }
        }
        tokio::time::sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn request_ids_and_stream_lifetime() {
        let c = Arc::<Counters>::default();
        let request = Request::new(&c, "s3", Method::GET);
        let id = request.context.id.clone();
        let mut error = s3s::s3_error!(AccessDenied).to_http_response().unwrap();
        s3_request_id(&mut error, &id);
        let xml = String::from_utf8(error.body().bytes().unwrap().to_vec()).unwrap();
        assert!(xml.contains(&format!("<RequestId>{id}</RequestId>")));
        let mut response = request.response(error, true);
        assert_eq!(response.headers()["x-amz-request-id"], id);
        assert_eq!(response.headers()["x-request-id"], id);
        while futures_util::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx))
            .await
            .is_some()
        {}
        drop(response);
        assert_eq!(c.snapshot()["failed"], 1);
        let stream = async_stream::stream! {
            yield Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"hello"));
        };
        let request = Request::new(&c, "s3", Method::GET);
        let response = Response::builder()
            .header("content-length", "5")
            .body(axum::body::Body::from_stream(stream))
            .unwrap();
        let mut response = request.response(response, true);
        futures_util::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx))
            .await
            .unwrap()
            .unwrap();
        // A fixed-length response need not be polled again for EOF by the HTTP server.
        drop(response);
        assert_eq!(c.snapshot()["canceled"], 0);
        let stream = async_stream::stream! {
            yield Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"hello"));
            yield Err(std::io::Error::other("stream failed after headers"));
        };
        let request = Request::new(&c, "web", Method::GET);
        let mut response =
            request.response(Response::new(axum::body::Body::from_stream(stream)), false);
        assert_eq!(c.snapshot()["active"], 1);
        let frame =
            futures_util::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx)).await;
        assert!(matches!(frame, Some(Ok(_))));
        assert_eq!(c.snapshot()["active"], 1);
        let frame =
            futures_util::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx)).await;
        assert!(matches!(frame, Some(Err(_))));
        assert_eq!(c.snapshot()["failed"], 2);
        drop(response);
        let request = Request::new(&c, "web", Method::GET);
        drop(request.response(Response::new(axum::body::Body::from("unread")), false));
        assert_eq!(c.snapshot()["canceled"], 1);
        let request = Request::new(&c, "web", Method::HEAD);
        drop(request.response(Response::new(axum::body::Body::from("not sent")), false));
        assert_eq!(c.snapshot()["canceled"], 1);
        assert_eq!(c.snapshot()["active"], 0);
    }
    #[test]
    fn counters_include_failures_cancellation_and_empty_latency() {
        let c = Arc::<Counters>::default();
        assert_eq!(c.snapshot()["duration_ms"]["p95"], Value::Null);
        let mut success = c.begin();
        success.finish(false);
        success.finish(true);
        c.begin().finish(true);
        drop(c.begin());
        c.status(403);
        c.status(503);
        c.bytes(42);
        let v = c.snapshot();
        assert_eq!(v["started"], 3);
        assert_eq!(v["completed"], 3);
        assert_eq!(v["active"], 0);
        assert_eq!(v["failed"], 1);
        assert_eq!(v["canceled"], 1);
        assert_eq!(v["bytes"], 42);
        assert_eq!(v["client_errors"], 1);
        assert_eq!(v["server_errors"], 1);
        assert!(v["duration_ms"]["p95"].as_u64().unwrap() >= 1);
    }
}
