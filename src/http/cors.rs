use crate::app::Bucket;
use anyhow::{Context, Result, ensure};
use axum::http::{HeaderMap, HeaderValue};
use serde::Deserialize;
use serde_json::Value;
use std::sync::{Arc, OnceLock};
use tokio::sync::OwnedSemaphorePermit;
mod cache;
pub(crate) use cache::{Cache, Policy, Validation, run};

struct Rule {
    rule: CorsRule,
    methods: HeaderValue,
    expose: Option<HeaderValue>,
    age: HeaderValue,
}
struct Rules {
    rules: Vec<Rule>,
    _memory: Arc<OnceLock<Arc<OwnedSemaphorePermit>>>,
}
impl Rules {
    fn parse(value: &Value, memory: Arc<OnceLock<Arc<OwnedSemaphorePermit>>>) -> Result<Self> {
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
            _memory: memory,
        })
    }
}

struct Header {
    value: HeaderValue,
    _memory: Arc<OnceLock<Arc<OwnedSemaphorePermit>>>,
}
impl AsRef<[u8]> for Header {
    fn as_ref(&self) -> &[u8] {
        self.value.as_bytes()
    }
}
fn charged_header(
    value: &HeaderValue,
    memory: &Arc<OnceLock<Arc<OwnedSemaphorePermit>>>,
) -> Result<HeaderValue> {
    Ok(HeaderValue::from_maybe_shared(bytes::Bytes::from_owner(
        Header {
            value: value.clone(),
            _memory: memory.clone(),
        },
    ))?)
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
    validation: Option<Validation>,
    generation: u64,
) -> Result<HeaderMap> {
    let mut out = HeaderMap::new();
    let policy = cache.resolve(bucket, validation, generation);
    if policy
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
    let rules = policy.rules(cache)?;
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
