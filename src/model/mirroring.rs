use crate::authorization::Principal;
use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use utoipa::ToSchema;
use uuid::Uuid;

#[cfg(test)]
#[path = "../../tests/recovery/mirroring.rs"]
mod contracts;

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(default, deny_unknown_fields)]
#[schema(as = MirroringConfig)]
pub(crate) struct Config {
    pub enabled: bool,
    pub rules: Vec<Rule>,
    pub connect_timeout_seconds: u32,
    pub header_timeout_seconds: u32,
    pub idle_timeout_seconds: u32,
    pub negative_ttl_seconds: u32,
    pub max_object_size: Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            rules: Vec::new(),
            connect_timeout_seconds: 10,
            header_timeout_seconds: 30,
            idle_timeout_seconds: 60,
            negative_ttl_seconds: 30,
            max_object_size: None,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = MirroringRule)]
pub(crate) struct Rule {
    pub id: Uuid,
    pub name: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub strip_prefix: bool,
    #[serde(default)]
    pub add_prefix: String,
    #[serde(default)]
    pub public_read: bool,
    pub source: Source,
    pub backup: Option<Source>,
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[schema(as = MirroringSource)]
pub(crate) enum Source {
    Http {
        url: String,
        #[serde(default)]
        allow_http: bool,
        #[serde(default)]
        allow_private: bool,
        #[serde(default)]
        follow_redirects: bool,
        /// Omitted on reads. Omit on update to retain unchanged-source headers; an empty map clears them.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        headers: Option<BTreeMap<String, String>>,
        #[serde(default)]
        header_names: Vec<String>,
    },
    S3 {
        external_id: Uuid,
        #[serde(default)]
        prefix: String,
    },
}

#[derive(Serialize, ToSchema)]
#[schema(as = MirroringView)]
pub(crate) struct View {
    pub revision: i64,
    pub config: Config,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = MirroringUpdate)]
pub(crate) struct Update {
    pub revision: i64,
    pub config: Config,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = MirroringPreviewInput)]
pub(crate) struct PreviewInput {
    pub key: String,
    pub config: Config,
}

#[derive(Serialize, ToSchema)]
#[schema(as = MirroringPreview)]
pub(crate) struct Preview {
    pub enabled: bool,
    pub rule_id: Option<Uuid>,
    pub source: Option<String>,
    pub source_key: Option<String>,
    pub public_read: bool,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.rules.len() <= 64, "too many mirroring rules");
        ensure!(
            !self.enabled || !self.rules.is_empty(),
            "enabled mirroring needs a rule"
        );
        ensure!(
            (1..=300).contains(&self.connect_timeout_seconds)
                && (1..=3600).contains(&self.header_timeout_seconds)
                && (1..=3600).contains(&self.idle_timeout_seconds)
                && self.negative_ttl_seconds <= 3600
                && self
                    .max_object_size
                    .is_none_or(|v| v > 0 && v <= 5 * 1024_u64.pow(4)),
            "invalid mirroring limits"
        );
        let mut ids = HashSet::new();
        for rule in &self.rules {
            ensure!(
                !rule.id.is_nil() && ids.insert(rule.id),
                "invalid mirroring rule identity"
            );
            ensure!(
                !rule.name.is_empty()
                    && rule.name.len() <= 128
                    && !rule.name.chars().any(char::is_control),
                "invalid mirroring rule name"
            );
            for value in [&rule.prefix, &rule.add_prefix] {
                ensure!(
                    value.len() <= 1024 && !value.contains('\0'),
                    "invalid mirroring prefix"
                );
            }
            rule.source.validate()?;
            if let Some(backup) = &rule.backup {
                backup.validate()?;
            }
        }
        ensure!(
            serde_json::to_vec(self)?.len() <= 64 * 1024,
            "mirroring configuration is too large"
        );
        Ok(())
    }

    pub fn select(&self, key: &str) -> Option<&Rule> {
        self.enabled
            .then(|| self.rules.iter().find(|rule| key.starts_with(&rule.prefix)))
            .flatten()
    }

    pub fn redact(&mut self) {
        for rule in &mut self.rules {
            rule.source.redact();
            if let Some(source) = &mut rule.backup {
                source.redact();
            }
        }
    }

    pub fn retain_headers(&mut self, previous: &Self) -> Result<()> {
        for rule in &mut self.rules {
            let old = previous.rules.iter().find(|old| old.id == rule.id);
            rule.source.retain_headers(old.map(|old| &old.source))?;
            if let Some(backup) = &mut rule.backup {
                backup.retain_headers(old.and_then(|old| old.backup.as_ref()))?;
            }
        }
        Ok(())
    }

    pub fn preview(&self, key: &str) -> Result<Preview> {
        self.validate()?;
        valid_key(key)?;
        let rule = self.select(key);
        let source_key = rule.map(|rule| rule.key(key)).transpose()?;
        let source = rule
            .zip(source_key.as_deref())
            .map(|(rule, key)| rule.source.address(key))
            .transpose()?;
        let source_key = rule
            .zip(source_key.as_deref())
            .map(|(rule, key)| rule.source.key(key))
            .transpose()?;
        Ok(Preview {
            enabled: self.enabled,
            rule_id: rule.map(|rule| rule.id),
            source,
            source_key,
            public_read: rule.is_some_and(|rule| rule.public_read),
        })
    }
}

pub(crate) fn valid_key(key: &str) -> Result<()> {
    ensure!(
        !key.is_empty() && key.len() <= 1024 && !key.contains('\0'),
        "invalid object key"
    );
    Ok(())
}

impl Rule {
    pub fn key(&self, key: &str) -> Result<String> {
        valid_key(key)?;
        ensure!(
            key.starts_with(&self.prefix),
            "rule does not match object key"
        );
        let suffix = if self.strip_prefix {
            &key[self.prefix.len()..]
        } else {
            key
        };
        let key = format!("{}{suffix}", self.add_prefix);
        valid_key(&key)?;
        Ok(key)
    }
}

impl Source {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::S3 {
                external_id,
                prefix,
            } => {
                ensure!(!external_id.is_nil(), "external bucket is missing");
                ensure!(
                    prefix.len() <= 1024 && !prefix.contains('\0'),
                    "invalid origin prefix"
                );
            }
            Self::Http {
                url,
                allow_http,
                headers,
                ..
            } => {
                let parsed: axum::http::Uri = url.parse()?;
                ensure!(
                    url.len() <= 2048
                        && !url.contains('#')
                        && parsed.host().is_some()
                        && parsed
                            .authority()
                            .is_some_and(|v| !v.as_str().contains('@'))
                        && parsed.query().is_none()
                        && (parsed.scheme_str() == Some("https")
                            || *allow_http && parsed.scheme_str() == Some("http")),
                    "origin must be an allowed HTTP URL without credentials, query or fragment"
                );
                if let Some(headers) = headers {
                    ensure!(headers.len() <= 16, "too many origin headers");
                    for (name, value) in headers {
                        let parsed = axum::http::HeaderName::from_bytes(name.as_bytes())?;
                        ensure!(
                            parsed.as_str() == name
                                && !matches!(
                                    name.as_str(),
                                    "host"
                                        | "connection"
                                        | "content-length"
                                        | "transfer-encoding"
                                        | "range"
                                        | "accept-encoding"
                                        | "cookie"
                                        | "set-cookie"
                                        | "forwarded"
                                        | "proxy-authorization"
                                        | "proxy-connection"
                                        | "te"
                                        | "trailer"
                                        | "upgrade"
                                )
                                && !name.starts_with("if-")
                                && !name.starts_with("x-mokyu-")
                                && !name.starts_with("x-forwarded-")
                                && !name.starts_with("x-amz-")
                                && value.len() <= 4096,
                            "invalid origin header"
                        );
                        axum::http::HeaderValue::from_str(value)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn key(&self, key: &str) -> Result<String> {
        valid_key(key)?;
        let key = match self {
            Self::S3 { prefix, .. } => format!("{prefix}{key}"),
            Self::Http { .. } => key.to_owned(),
        };
        valid_key(&key)?;
        Ok(key)
    }

    pub fn address(&self, key: &str) -> Result<String> {
        valid_key(key)?;
        match self {
            Self::Http { url, .. } => {
                // URI parsing preserves opaque dot and empty segments; Url joining would rename keys.
                let target = format!(
                    "{}/{}",
                    url.trim_end_matches('/'),
                    crate::app::listing::encode_key(key)
                );
                let _: axum::http::Uri = target.parse()?;
                Ok(target)
            }
            Self::S3 { external_id, .. } => Ok(format!(
                "external://{external_id}/{}",
                crate::app::listing::encode_key(&self.key(key)?)
            )),
        }
    }

    fn redact(&mut self) {
        if let Self::Http {
            headers,
            header_names,
            ..
        } = self
        {
            *header_names = headers
                .as_ref()
                .map(|headers| headers.keys().cloned().collect())
                .unwrap_or_default();
            *headers = None;
        }
    }

    fn retain_headers(&mut self, old: Option<&Self>) -> Result<()> {
        if let Self::Http {
            url,
            headers,
            header_names,
            ..
        } = self
        {
            if headers.is_none() {
                if let Some(Self::Http {
                    url: old_url,
                    headers: old_headers,
                    ..
                }) = old
                {
                    ensure!(
                        url == old_url || old_headers.as_ref().is_none_or(BTreeMap::is_empty),
                        "provide origin headers again when changing the URL"
                    );
                    *headers = old_headers.clone();
                } else {
                    *headers = Some(BTreeMap::new());
                }
            }
            header_names.clear();
        }
        Ok(())
    }
}

pub(crate) struct Record {
    pub revision: i64,
    pub encrypted: Option<Vec<u8>>,
}

pub(crate) struct Start {
    pub bucket: Uuid,
    pub key: String,
    pub revision: i64,
    pub rule: Uuid,
    pub task: Uuid,
    pub storage: Uuid,
}

#[async_trait]
pub(crate) trait MirroringStore: Send + Sync {
    async fn get(&self, bucket: Uuid) -> Result<Record>;
    async fn save(
        &self,
        principal: &Principal,
        bucket: Uuid,
        revision: i64,
        config: &Config,
        encrypted: Vec<u8>,
    ) -> Result<i64>;
    async fn request(&self, input: Start) -> Result<Uuid>;
    async fn bind_source(
        &self,
        task: crate::model::task::Execution,
        source: Option<(Uuid, i64)>,
    ) -> Result<()>;
    async fn failed(
        &self,
        task: crate::model::task::Execution,
        missing_ttl: Option<u32>,
    ) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        serde_json::from_value(serde_json::json!({"enabled":true,"rules":[{
            "id":Uuid::now_v7(),"name":"Origin","prefix":"media/","strip_prefix":true,"add_prefix":"archive/",
            "source":{"type":"http","url":"https://origin.example/base","headers":{"authorization":"Bearer private"}},"backup":null
        }]})).unwrap()
    }

    #[test]
    fn opaque_keys_and_first_match_are_preserved() {
        let mut config = config();
        let mut fallback = config.rules[0].clone();
        fallback.id = Uuid::now_v7();
        fallback.prefix.clear();
        fallback.strip_prefix = false;
        config.rules.push(fallback);
        for (key, tail) in [
            ("a//../b", "a//../b"),
            ("%2F?#", "%252F%3F%23"),
            ("a b", "a%20b"),
        ] {
            let preview = config.preview(&format!("media/{key}")).unwrap();
            assert_eq!(preview.rule_id, Some(config.rules[0].id));
            assert_eq!(
                preview.source.unwrap(),
                format!("https://origin.example/base/archive/{tail}")
            );
        }
        config.enabled = false;
        assert!(config.preview("media/a").unwrap().source.is_none());
    }

    #[test]
    fn external_origins_keep_independent_literal_prefixes() {
        let external_id = Uuid::now_v7();
        let mut config = config();
        let original: Source = serde_json::from_value(serde_json::json!({
            "type": "s3", "external_id": external_id
        }))
        .unwrap();
        assert_eq!(original.key("file").unwrap(), "file");
        for prefix in ["", "photos/", "photo-", "/", "a//../", "%2F "] {
            config.rules[0].source = Source::S3 {
                external_id,
                prefix: prefix.into(),
            };
            config.rules[0].backup = Some(Source::S3 {
                external_id,
                prefix: "backup-".into(),
            });
            let key = "a//../b +%2F";
            let mapped = config.rules[0].key(&format!("media/{key}")).unwrap();
            let preview = config.preview(&format!("media/{key}")).unwrap();
            let expected = format!("{prefix}archive/{key}");
            assert_eq!(preview.source_key.as_deref(), Some(expected.as_str()));
            assert_eq!(
                preview.source.unwrap(),
                format!(
                    "external://{external_id}/{}",
                    crate::app::listing::encode_key(&expected)
                )
            );
            assert_eq!(
                config.rules[0]
                    .backup
                    .as_ref()
                    .unwrap()
                    .key(&mapped)
                    .unwrap(),
                format!("backup-archive/{key}")
            );
            let roundtrip: Config =
                serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
            assert_eq!(
                roundtrip
                    .preview(&format!("media/{key}"))
                    .unwrap()
                    .source_key,
                preview.source_key
            );
        }
        for prefix in ["a\0b".into(), "x".repeat(1025)] {
            assert!(
                Source::S3 {
                    external_id,
                    prefix
                }
                .validate()
                .is_err()
            );
        }
        let source = Source::S3 {
            external_id,
            prefix: "x".repeat(1024),
        };
        assert!(source.key("file").is_err());
    }

    #[test]
    fn secrets_are_redacted_and_cannot_follow_an_origin_change() {
        let original = config();
        let mut view = original.clone();
        view.redact();
        assert!(
            !serde_json::to_string(&view)
                .unwrap()
                .contains("Bearer private")
        );
        view.retain_headers(&original).unwrap();
        assert_eq!(
            serde_json::to_value(&view).unwrap(),
            serde_json::to_value(&original).unwrap()
        );
        view.redact();
        if let Source::Http { url, .. } = &mut view.rules[0].source {
            *url = "https://other.example".into();
        }
        assert!(view.retain_headers(&original).is_err());
    }

    #[test]
    fn invalid_rules_and_unsafe_headers_are_rejected() {
        let mut config = config();
        config.rules.push(config.rules[0].clone());
        assert!(config.validate().is_err());
        for name in [
            "Host",
            "host",
            "range",
            "cookie",
            "if-match",
            "x-mokyu-origin-hop",
            "x-amz-date",
        ] {
            let mut config = self::config();
            if let Source::Http { headers, .. } = &mut config.rules[0].source {
                *headers = Some(BTreeMap::from([(name.into(), "value".into())]));
            }
            assert!(config.validate().is_err(), "{name}");
        }
    }
}
