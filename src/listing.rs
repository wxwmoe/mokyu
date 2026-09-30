use crate::app::{App, StoredStream};
use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
#[derive(Serialize, Deserialize, Clone)]
struct Cursor {
    bucket: Uuid,
    prefix: String,
    delimiter: String,
    key: String,
    inclusive: bool,
}
pub struct Listing {
    pub objects: Vec<StoredStream>,
    pub prefixes: Vec<String>,
    pub next: Option<String>,
}
pub(crate) fn successor(s: &str) -> Option<String> {
    let mut chars: Vec<char> = s.chars().collect();
    while let Some(c) = chars.pop() {
        let mut n = c as u32 + 1;
        if (0xd800..=0xdfff).contains(&n) {
            n = 0xe000;
        }
        if let Some(next) = char::from_u32(n) {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}
impl App {
    pub async fn list(
        &self,
        bucket: Uuid,
        prefix: &str,
        delimiter: &str,
        after: Option<&str>,
        token: Option<&str>,
        limit: usize,
    ) -> Result<Listing> {
        if limit > 1000 {
            return Err(s3s::s3_error!(InvalidArgument).into());
        }
        let mut cursor = if let Some(token) = token {
            let decoded = URL_SAFE_NO_PAD
                .decode(token)
                .map_err(|_| s3s::s3_error!(InvalidArgument, "invalid continuation token"))?;
            let c: Cursor = serde_json::from_slice(&decoded)
                .map_err(|_| s3s::s3_error!(InvalidArgument, "invalid continuation token"))?;
            if c.bucket != bucket || c.prefix != prefix || c.delimiter != delimiter {
                return Err(s3s::s3_error!(
                    InvalidArgument,
                    "continuation token belongs to another listing"
                )
                .into());
            }
            c
        } else {
            Cursor {
                bucket,
                prefix: prefix.into(),
                delimiter: delimiter.into(),
                key: after.unwrap_or(prefix).to_string(),
                inclusive: after.is_none(),
            }
        };
        let mut result = Listing {
            objects: Vec::new(),
            prefixes: Vec::new(),
            next: None,
        };
        if limit == 0 {
            return Ok(result);
        }
        let pattern = format!(
            "{}%",
            prefix
                .replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_")
        );
        let mut count = 0;
        let mut saved = cursor.clone();
        loop {
            let rows:Vec<StoredStream>=sqlx::query_as("SELECT s.* FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=$1 AND o.key LIKE $2 AND (o.key>$3 OR ($4 AND o.key=$3)) ORDER BY o.key LIMIT 64")
                .bind(bucket).bind(&pattern).bind(&cursor.key).bind(cursor.inclusive).fetch_all(&self.db).await?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                if row.object_key < cursor.key
                    || (!cursor.inclusive && row.object_key == cursor.key)
                {
                    continue;
                }
                let rest = row
                    .object_key
                    .strip_prefix(prefix)
                    .context("listing prefix mismatch")?;
                let group = if delimiter.is_empty() {
                    None
                } else {
                    rest.find(delimiter)
                        .map(|n| row.object_key[..prefix.len() + n + delimiter.len()].to_string())
                };
                if let Some(group) = group {
                    let next = successor(&group);
                    cursor.key = next.clone().unwrap_or_default();
                    cursor.inclusive = true;
                    if after.is_none_or(|a| group.as_str() > a) {
                        if count == limit {
                            result.next = Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&saved)?));
                            return Ok(result);
                        }
                        result.prefixes.push(group);
                        count += 1;
                        saved = cursor.clone();
                    }
                    if next.is_none() {
                        return Ok(result);
                    }
                } else {
                    if count == limit {
                        result.next = Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&saved)?));
                        return Ok(result);
                    }
                    cursor.key = row.object_key.clone();
                    cursor.inclusive = false;
                    result.objects.push(row);
                    count += 1;
                    saved = cursor.clone();
                }
            }
        }
        Ok(result)
    }
}
pub fn encode_key(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~/".contains(&b) {
            out.push(b as char);
        } else {
            use std::fmt::Write;
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prefix_seek_handles_unicode() {
        assert_eq!(successor("abc/"), Some("abc0".into()));
        assert!(successor("\u{10ffff}").is_none());
        assert_eq!(encode_key("a +%\u{4e2d}"), "a%20%2B%25%E4%B8%AD");
    }
}
