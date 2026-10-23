use crate::app::Bucket;
use anyhow::{Context, Result, ensure};
use axum::http::{HeaderMap, HeaderValue};
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

const UNIT: usize = 64;
const ENTRIES: usize = 256;
const INDEX_BYTES: usize = 64 * 1024;
const ENTRY_BYTES: usize = 64 * 1024;
pub(crate) const CACHE_BYTES: usize = 1024 * 1024;

struct Rule {
    rule: CorsRule,
    methods: HeaderValue,
    expose: Option<HeaderValue>,
    age: HeaderValue,
}
struct Rules {
    rules: Vec<Rule>,
    _memory: Option<Arc<OwnedSemaphorePermit>>,
}
impl Rules {
    fn parse(value: &Value) -> Result<Self> {
        let rules: Vec<CorsRule> = Vec::deserialize(value)?;
        let rules = rules
            .into_iter()
            .map(|rule| {
                Ok(Rule {
                    methods: rule.methods.join(", ").parse()?,
                    expose: (!rule.expose.is_empty())
                        .then(|| rule.expose.join(", ").parse())
                        .transpose()?,
                    age: rule.max_age.to_string().parse()?,
                    rule,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            rules,
            _memory: None,
        })
    }
    fn bytes(&self) -> usize {
        let mut bytes =
            std::mem::size_of::<Self>() + 64 + self.rules.capacity() * std::mem::size_of::<Rule>();
        for item in &self.rules {
            for values in [
                &item.rule.origins,
                &item.rule.methods,
                &item.rule.headers,
                &item.rule.expose,
            ] {
                bytes += values.capacity() * std::mem::size_of::<String>();
                bytes += values.iter().map(String::capacity).sum::<usize>();
            }
            // HeaderValue retains immutable byte allocations and their ownership headers.
            bytes += item.methods.len()
                + item.age.len()
                + item.expose.as_ref().map_or(0, HeaderValue::len)
                + 3 * 128;
        }
        bytes
    }
}
struct Header {
    value: HeaderValue,
    _memory: Arc<OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for Header {
    fn as_ref(&self) -> &[u8] {
        self.value.as_bytes()
    }
}
fn charged_header(value: &HeaderValue, memory: &Arc<OwnedSemaphorePermit>) -> Result<HeaderValue> {
    Ok(HeaderValue::from_maybe_shared(bytes::Bytes::from_owner(
        Header {
            value: value.clone(),
            _memory: memory.clone(),
        },
    ))?)
}

struct Entry {
    revision: i64,
    rules: Arc<Rules>,
}
#[derive(Default)]
struct Index {
    entries: HashMap<Uuid, Entry>,
    fifo: VecDeque<Uuid>,
}

pub(crate) struct Cache {
    limit: usize,
    memory: Arc<Semaphore>,
    index: Mutex<Index>,
    hits: AtomicU64,
    parses: AtomicU64,
    invalidated: AtomicU64,
    evicted: AtomicU64,
    bypassed: AtomicU64,
}
impl Cache {
    pub(crate) fn new(bytes: usize) -> Self {
        Self {
            limit: bytes,
            memory: Arc::new(Semaphore::new(bytes.saturating_sub(INDEX_BYTES) / UNIT)),
            index: Mutex::new(Index::default()),
            hits: 0.into(),
            parses: 0.into(),
            invalidated: 0.into(),
            evicted: 0.into(),
            bypassed: 0.into(),
        }
    }
    fn uncached(&self, value: &Value) -> Result<Arc<Rules>> {
        self.bypassed.fetch_add(1, Relaxed);
        self.parses.fetch_add(1, Relaxed);
        Ok(Arc::new(Rules::parse(value)?))
    }
    fn rules(&self, bucket: &Bucket) -> Result<Arc<Rules>> {
        let mut index = self.index.lock().unwrap();
        if let Some(entry) = index.entries.get(&bucket.id) {
            if entry.revision == bucket.settings_revision {
                self.hits.fetch_add(1, Relaxed);
                return Ok(entry.rules.clone());
            }
            if entry.revision > bucket.settings_revision {
                drop(index);
                return self.uncached(&bucket.cors);
            }
        }
        if self.limit <= INDEX_BYTES || !bounded_document(&bucket.cors) {
            drop(index);
            return self.uncached(&bucket.cors);
        }
        // Parsing a bounded document under the index lock coalesces concurrent first loads.
        self.parses.fetch_add(1, Relaxed);
        let mut rules = Rules::parse(&bucket.cors)?;
        let bytes = rules.bytes();
        if index.entries.remove(&bucket.id).is_some() {
            index.fifo.retain(|id| *id != bucket.id);
            self.invalidated.fetch_add(1, Relaxed);
        }
        if bytes <= ENTRY_BYTES {
            loop {
                if index.entries.len() < ENTRIES
                    && let Ok(memory) = self
                        .memory
                        .clone()
                        .try_acquire_many_owned(bytes.div_ceil(UNIT) as u32)
                {
                    let memory = Arc::new(memory);
                    for rule in &mut rules.rules {
                        rule.methods = charged_header(&rule.methods, &memory)?;
                        rule.age = charged_header(&rule.age, &memory)?;
                        rule.expose = rule
                            .expose
                            .as_ref()
                            .map(|value| charged_header(value, &memory))
                            .transpose()?;
                    }
                    rules._memory = Some(memory);
                    let rules = Arc::new(rules);
                    index.entries.insert(
                        bucket.id,
                        Entry {
                            revision: bucket.settings_revision,
                            rules: rules.clone(),
                        },
                    );
                    index.fifo.push_back(bucket.id);
                    return Ok(rules);
                }
                let Some(old) = index.fifo.pop_front() else {
                    break;
                };
                index.entries.remove(&old);
                self.evicted.fetch_add(1, Relaxed);
            }
        }
        self.bypassed.fetch_add(1, Relaxed);
        Ok(Arc::new(rules))
    }
    pub(crate) fn snapshot(&self) -> Value {
        serde_json::json!({"limit_bytes":self.limit,"index_limit_bytes":INDEX_BYTES.min(self.limit),
            "live_bytes":self.limit.saturating_sub(INDEX_BYTES).saturating_sub(self.memory.available_permits()*UNIT),
            "entries":self.index.lock().unwrap().entries.len(),"hits":self.hits.load(Relaxed),
            "parses":self.parses.load(Relaxed),"invalidated":self.invalidated.load(Relaxed),
            "evicted":self.evicted.load(Relaxed),"bypassed":self.bypassed.load(Relaxed)})
    }
}

fn bounded_document(value: &Value) -> bool {
    let Some(rules) = value.as_array() else {
        return false;
    };
    let mut bytes = 0usize;
    for rule in rules {
        bytes = bytes.saturating_add(std::mem::size_of::<Rule>() + 256);
        for field in ["origins", "methods", "headers", "expose"] {
            if let Some(values) = rule.get(field).and_then(Value::as_array) {
                for value in values {
                    let Some(value) = value.as_str() else {
                        return false;
                    };
                    bytes = bytes.saturating_add(
                        value.len().saturating_mul(3) + std::mem::size_of::<String>(),
                    );
                    if bytes > ENTRY_BYTES {
                        return false;
                    }
                }
            }
        }
        if bytes > ENTRY_BYTES {
            return false;
        }
    }
    true
}

#[derive(Clone, serde::Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CorsRule {
    pub origins: Vec<String>,
    pub methods: Vec<String>,
    #[serde(default)]
    pub headers: Vec<String>,
    #[serde(default)]
    pub expose: Vec<String>,
    #[serde(default)]
    pub max_age: u32,
}
pub fn validate_cors(value: &Value) -> Result<()> {
    let rules: Vec<CorsRule> = Vec::deserialize(value).context("invalid CORS document")?;
    ensure!(rules.len() <= 100, "too many CORS rules");
    for r in rules {
        ensure!(
            !r.origins.is_empty() && !r.methods.is_empty(),
            "CORS rule requires origins and methods"
        );
        for m in r.methods {
            ensure!(
                ["GET", "HEAD", "PUT", "POST", "DELETE", "OPTIONS"].contains(&m.as_str()),
                "invalid CORS method"
            );
        }
        for h in r.headers.into_iter().chain(r.expose) {
            ensure!(
                h == "*" || hyper::header::HeaderName::from_bytes(h.as_bytes()).is_ok(),
                "invalid CORS header"
            );
        }
        for o in r.origins {
            ensure!(
                o == "*"
                    || ((o.starts_with("https://") || o.starts_with("http://"))
                        && !o.contains(['\r', '\n'])),
                "invalid CORS origin"
            );
        }
    }
    Ok(())
}
pub fn cors_headers(
    cache: &Cache,
    bucket: &Bucket,
    headers: &HeaderMap,
    method: &str,
) -> Result<HeaderMap> {
    let mut out = HeaderMap::new();
    if bucket
        .cors
        .as_array()
        .context("invalid CORS document")?
        .is_empty()
    {
        return Ok(out);
    }
    out.insert(
        "vary",
        HeaderValue::from_static(if method == "OPTIONS" {
            "Origin, Access-Control-Request-Method, Access-Control-Request-Headers"
        } else {
            "Origin"
        }),
    );
    let Some(origin) = headers.get("origin").and_then(|s| s.to_str().ok()) else {
        return Ok(out);
    };
    let (requested, names) = if method == "OPTIONS" {
        let Some(requested) = headers
            .get("access-control-request-method")
            .and_then(|s| s.to_str().ok())
        else {
            return Ok(out);
        };
        (
            requested,
            headers
                .get("access-control-request-headers")
                .and_then(|s| s.to_str().ok())
                .unwrap_or(""),
        )
    } else {
        (method, "")
    };
    let rules = cache.rules(bucket)?;
    for prepared in &rules.rules {
        let r = &prepared.rule;
        if !r.origins.iter().any(|o| o == "*" || o == origin)
            || !r.methods.iter().any(|m| m == requested)
        {
            continue;
        }
        if !names
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .all(|n| {
                r.headers
                    .iter()
                    .any(|a| a == "*" || a.eq_ignore_ascii_case(n))
            })
        {
            continue;
        }
        out.insert(
            "access-control-allow-origin",
            HeaderValue::from_str(if r.origins.iter().any(|o| o == "*") {
                "*"
            } else {
                origin
            })?,
        );
        out.insert("access-control-allow-methods", prepared.methods.clone());
        if !names.is_empty() {
            out.insert(
                "access-control-allow-headers",
                HeaderValue::from_str(names)?,
            );
        }
        if let Some(expose) = &prepared.expose {
            out.insert("access-control-expose-headers", expose.clone());
        }
        out.insert("access-control-max-age", prepared.age.clone());
        break;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn bucket() -> Bucket {
        Bucket {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            name: "bucket".into(),
            storage_id: None,
            state: "active".into(),
            settings_revision: 1,
            uploads_paused: false,
            public_base_url: String::new(),
            cors: json!([{"origins":["https://one.test","https://two.test"],
                "methods":["GET","HEAD"],"headers":["range"],"expose":["ETag"],"max_age":60}]),
            website_enabled: false,
            index_document: "index.html".into(),
            error_document: String::new(),
            created_at: chrono::Utc::now(),
        }
    }
    fn request(origin: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("origin", origin.parse().unwrap());
        headers
    }

    #[test]
    fn concurrent_requests_share_rules_without_sharing_origin_or_preflight() {
        let cache = Cache::new(CACHE_BYTES);
        let bucket = bucket();
        let empty = cors_headers(&cache, &bucket, &HeaderMap::new(), "GET").unwrap();
        assert_eq!(empty["vary"], "Origin");
        assert_eq!(cache.parses.load(Relaxed), 0);
        let start = std::sync::Barrier::new(32);
        std::thread::scope(|scope| {
            for number in 0..32 {
                let cache = &cache;
                let bucket = &bucket;
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    let origin = if number % 2 == 0 {
                        "https://one.test"
                    } else {
                        "https://two.test"
                    };
                    let out = cors_headers(cache, bucket, &request(origin), "GET").unwrap();
                    assert_eq!(out["access-control-allow-origin"], origin);
                    assert_eq!(out["access-control-expose-headers"], "ETag");
                });
            }
        });
        assert_eq!(cache.parses.load(Relaxed), 1);
        assert_eq!(cache.hits.load(Relaxed), 31);
        assert!(
            !cors_headers(&cache, &bucket, &request("https://other.test"), "GET")
                .unwrap()
                .contains_key("access-control-allow-origin")
        );
        let mut headers = request("https://two.test");
        headers.insert(
            "access-control-request-method",
            HeaderValue::from_static("HEAD"),
        );
        headers.insert(
            "access-control-request-headers",
            HeaderValue::from_static("Range"),
        );
        let out = cors_headers(&cache, &bucket, &headers, "OPTIONS").unwrap();
        assert_eq!(out["access-control-allow-headers"], "Range");
        assert_eq!(
            out["vary"],
            "Origin, Access-Control-Request-Method, Access-Control-Request-Headers"
        );
        headers.insert(
            "access-control-request-headers",
            HeaderValue::from_static("Authorization"),
        );
        assert!(
            !cors_headers(&cache, &bucket, &headers, "OPTIONS")
                .unwrap()
                .contains_key("access-control-allow-origin")
        );
        assert_eq!(cache.parses.load(Relaxed), 1);
    }

    #[test]
    fn older_revision_cannot_replace_current_rules_or_mask_invalid_configuration() {
        let cache = Cache::new(CACHE_BYTES);
        let old = bucket();
        let retained = cache.rules(&old).unwrap();
        let mut current = old.clone();
        current.settings_revision += 1;
        current.cors[0]["origins"] = json!(["https://current.test"]);
        let latest = cache.rules(&current).unwrap();
        assert!(!Arc::ptr_eq(&retained, &latest));
        assert!(!Arc::ptr_eq(&cache.rules(&old).unwrap(), &retained));
        assert!(Arc::ptr_eq(&cache.rules(&current).unwrap(), &latest));
        assert_eq!(
            cache.index.lock().unwrap().entries[&old.id].revision,
            current.settings_revision
        );
        current.settings_revision += 1;
        current.cors = json!([{"invalid": true}]);
        assert!(cors_headers(&cache, &current, &request("https://one.test"), "GET").is_err());
    }

    #[test]
    fn evicted_rules_keep_their_charge_until_readers_release_them() {
        let first = bucket();
        let charge = Rules::parse(&first.cors).unwrap().bytes().div_ceil(UNIT) * UNIT;
        let cache = Cache::new(INDEX_BYTES + charge);
        let retained = cache.rules(&first).unwrap();
        let response = cors_headers(&cache, &first, &request("https://one.test"), "GET").unwrap();
        assert_eq!(cache.memory.available_permits(), 0);
        let second = bucket();
        let fallback = cache.rules(&second).unwrap();
        assert!(fallback._memory.is_none());
        assert!(cache.index.lock().unwrap().entries.is_empty());
        assert_eq!(cache.memory.available_permits(), 0);
        drop(retained);
        assert_eq!(cache.memory.available_permits(), 0);
        drop(response);
        assert_eq!(cache.memory.available_permits() * UNIT, charge);
        assert!(cache.rules(&second).unwrap()._memory.is_some());
        let mut large = bucket();
        large.cors[0]["origins"] = json!([format!("https://{}.test", "a".repeat(ENTRY_BYTES))]);
        assert!(cache.rules(&large).unwrap()._memory.is_none());
        assert_eq!(cache.index.lock().unwrap().entries.len(), 1);
        let disabled = Cache::new(0);
        assert_eq!(
            cors_headers(&disabled, &first, &request("https://one.test"), "GET").unwrap()["access-control-allow-origin"],
            "https://one.test"
        );
        assert_eq!(disabled.snapshot()["entries"], 0);
        let bounded = Cache::new(CACHE_BYTES);
        for _ in 0..ENTRIES + 2 {
            bounded.rules(&bucket()).unwrap();
        }
        assert_eq!(bounded.index.lock().unwrap().entries.len(), ENTRIES);
        assert_eq!(bounded.evicted.load(Relaxed), 2);
    }
}
