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
pub struct Listing<T = StoredStream> {
    pub objects: Vec<T>,
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
pub(crate) struct Page<T> {
    cursor: Cursor,
    saved: Cursor,
    after: Option<String>,
    limit: usize,
    done: bool,
    result: Listing<T>,
}

impl<T> Page<T> {
    pub(crate) fn new(
        bucket: Uuid,
        prefix: &str,
        delimiter: &str,
        after: Option<&str>,
        token: Option<&str>,
        limit: usize,
    ) -> Result<Self> {
        if limit > 1000 {
            return Err(s3s::s3_error!(InvalidArgument).into());
        }
        let cursor = if let Some(token) = token {
            let decoded = URL_SAFE_NO_PAD
                .decode(token)
                .map_err(|_| s3s::s3_error!(InvalidArgument, "invalid continuation token"))?;
            let cursor: Cursor = serde_json::from_slice(&decoded)
                .map_err(|_| s3s::s3_error!(InvalidArgument, "invalid continuation token"))?;
            if cursor.bucket != bucket || cursor.prefix != prefix || cursor.delimiter != delimiter {
                return Err(s3s::s3_error!(
                    InvalidArgument,
                    "continuation token belongs to another listing"
                )
                .into());
            }
            cursor
        } else {
            Cursor {
                bucket,
                prefix: prefix.into(),
                delimiter: delimiter.into(),
                key: after.unwrap_or(prefix).into(),
                inclusive: after.is_none(),
            }
        };
        Ok(Self {
            saved: cursor.clone(),
            cursor,
            after: after.map(str::to_owned),
            limit,
            done: limit == 0,
            result: Listing {
                objects: Vec::new(),
                prefixes: Vec::new(),
                next: None,
            },
        })
    }

    pub(crate) fn position(&self) -> Option<(&str, bool)> {
        (!self.done).then_some((&self.cursor.key, self.cursor.inclusive))
    }

    pub(crate) fn append(&mut self, rows: Vec<T>, key: impl Fn(&T) -> &str) -> Result<()> {
        if rows.is_empty() {
            self.done = true;
        }
        for row in rows {
            let key = key(&row);
            if key < self.cursor.key.as_str() || (!self.cursor.inclusive && key == self.cursor.key)
            {
                continue;
            }
            let rest = key
                .strip_prefix(&self.cursor.prefix)
                .context("listing prefix mismatch")?;
            let group = if self.cursor.delimiter.is_empty() {
                None
            } else {
                rest.find(&self.cursor.delimiter).map(|n| {
                    key[..self.cursor.prefix.len() + n + self.cursor.delimiter.len()].to_string()
                })
            };
            if let Some(group) = group {
                let next = successor(&group);
                self.cursor.key = next.clone().unwrap_or_default();
                self.cursor.inclusive = true;
                if self
                    .after
                    .as_deref()
                    .is_none_or(|after| group.as_str() > after)
                {
                    if self.full()? {
                        return Ok(());
                    }
                    self.result.prefixes.push(group);
                    self.saved = self.cursor.clone();
                }
                if next.is_none() {
                    self.done = true;
                    return Ok(());
                }
            } else {
                if self.full()? {
                    return Ok(());
                }
                self.cursor.key = key.to_owned();
                self.cursor.inclusive = false;
                self.result.objects.push(row);
                self.saved = self.cursor.clone();
            }
        }
        Ok(())
    }

    fn full(&mut self) -> Result<bool> {
        if self.result.objects.len() + self.result.prefixes.len() != self.limit {
            return Ok(false);
        }
        self.result.next = Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&self.saved)?));
        self.done = true;
        Ok(true)
    }

    pub(crate) fn finish(self) -> Listing<T> {
        self.result
    }
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
        let mut page = Page::new(bucket, prefix, delimiter, after, token, limit)?;
        while let Some((key, inclusive)) = page.position() {
            let rows = self
                .buckets
                .list_objects(bucket, prefix, key, inclusive)
                .await?;
            page.append(rows, |row| &row.object_key)?;
        }
        Ok(page.finish())
    }
}

pub(crate) fn bucket_cursor(
    access: &str,
    prefix: &str,
    region: Option<&str>,
    token: Option<&str>,
    limit: i32,
) -> Result<String> {
    if !(1..=10000).contains(&limit) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let Some(token) = token else {
        return Ok(String::new());
    };
    let bytes = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    let (key, p, r, last): (String, String, Option<String>, String) =
        serde_json::from_slice(&bytes).map_err(|_| s3s::s3_error!(InvalidArgument))?;
    if key != access || p != prefix || r.as_deref() != region {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "continuation token belongs to another listing"
        )
        .into());
    }
    Ok(last)
}

pub(crate) fn bucket_token(
    access: &str,
    prefix: &str,
    region: Option<&str>,
    last: &str,
) -> Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&(access, prefix, region, last))?))
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

    #[test]
    fn bucket_tokens_preserve_the_listing_scope() -> Result<()> {
        let token = bucket_token("reader", "prefix", Some("region"), "last")?;
        assert_eq!(
            bucket_cursor("reader", "prefix", Some("region"), Some(&token), 1)?,
            "last"
        );
        for (key, prefix, region) in [
            ("other", "prefix", Some("region")),
            ("reader", "other", Some("region")),
            ("reader", "prefix", None),
        ] {
            assert!(bucket_cursor(key, prefix, region, Some(&token), 1).is_err());
        }
        for limit in [0, 10001] {
            assert!(bucket_cursor("reader", "", None, None, limit).is_err());
        }
        Ok(())
    }
}
