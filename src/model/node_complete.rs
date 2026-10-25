use super::{node_read, session::Run};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;

pub(crate) const REQUEST_LIMIT: usize = 4 * 1024 * 1024;
pub(crate) const SOURCES: usize = 8;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Part {
    pub number: i32,
    pub etag: String,
    pub checksums: BTreeMap<String, String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    pub upload: Uuid,
    pub parts: Vec<Part>,
    pub if_match: Option<String>,
    pub if_none: Option<String>,
    pub checksum_type: Option<String>,
    pub size: Option<i64>,
    pub checksums: BTreeMap<String, String>,
}

impl Manifest {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            !self.upload.is_nil() && !self.parts.is_empty() && self.parts.len() <= 10000,
            s3s::s3_error!(InvalidPart)
        );
        let mut previous = 0;
        for part in &self.parts {
            ensure!(
                part.number > previous && part.number <= 10000,
                s3s::s3_error!(InvalidPartOrder)
            );
            ensure!(
                part.etag.len() == 32 && part.etag.bytes().all(|value| value.is_ascii_hexdigit()),
                s3s::s3_error!(InvalidPart)
            );
            validate_checksums(&part.checksums)?;
            previous = part.number;
        }
        validate_checksums(&self.checksums)?;
        ensure!(
            self.size.is_none_or(|size| size >= 0)
                && self
                    .checksum_type
                    .as_deref()
                    .is_none_or(|value| matches!(value, "COMPOSITE" | "FULL_OBJECT")),
            s3s::s3_error!(InvalidRequest)
        );
        for value in [&self.if_match, &self.if_none].into_iter().flatten() {
            ensure!(value.len() <= 256, s3s::s3_error!(InvalidArgument));
            s3s::dto::ETagCondition::parse_http_header(value.as_bytes())?;
        }
        ensure!(
            serde_json::to_vec(self)?.len() <= REQUEST_LIMIT - 4096,
            s3s::s3_error!(InvalidRequest)
        );
        Ok(())
    }

    pub(crate) fn hash(&self) -> Result<String> {
        self.validate()?;
        Ok(blake3::hash(&serde_json::to_vec(self)?)
            .to_hex()
            .to_string())
    }

    pub(crate) fn select(
        &self,
        upload: &super::multipart::Upload,
        parts: &[Selected],
    ) -> Result<Summary> {
        self.validate()?;
        ensure!(self.parts.len() == parts.len(), s3s::s3_error!(InvalidPart));
        ensure!(
            self.checksum_type
                .as_ref()
                .is_none_or(|value| Some(value) == upload.checksum_type.as_ref()),
            s3s::s3_error!(BadDigest)
        );
        let mut total = 0i64;
        let mut digest = Md5::new();
        for (index, (requested, selected)) in self.parts.iter().zip(parts).enumerate() {
            ensure!(
                requested.number == selected.number
                    && requested.etag == selected.etag
                    && selected.size >= 0,
                s3s::s3_error!(InvalidPart)
            );
            if upload.checksum_type.as_deref() == Some("COMPOSITE") {
                ensure!(
                    requested.number as usize == index + 1,
                    s3s::s3_error!(InvalidPartOrder)
                );
            }
            ensure!(
                index + 1 == parts.len() || selected.size >= 5 * 1024 * 1024,
                s3s::s3_error!(EntityTooSmall)
            );
            for (name, value) in &requested.checksums {
                ensure!(
                    selected.checksums.get(name).and_then(Value::as_str) == Some(value),
                    s3s::s3_error!(BadDigest)
                );
            }
            if let Some(algorithm) = &upload.checksum_algorithm {
                ensure!(
                    requested
                        .checksums
                        .contains_key(&algorithm.to_ascii_lowercase()),
                    s3s::s3_error!(InvalidPart, "part checksum required by upload")
                );
            }
            total = total
                .checked_add(selected.size)
                .context("multipart length overflow")?;
            digest.update(hex::decode(&selected.etag)?);
        }
        ensure!(
            self.size.is_none_or(|size| size == total),
            s3s::s3_error!(InvalidRequest, "multipart size mismatch")
        );
        Ok(Summary {
            upload: upload.id,
            size: total,
            etag: format!("{}-{}", hex::encode(digest.finalize()), parts.len()),
            parts: parts.len(),
            algorithm: upload.checksum_algorithm.clone(),
            checksum_type: upload.checksum_type.clone(),
            result: None,
        })
    }
}

fn validate_checksums(values: &BTreeMap<String, String>) -> Result<()> {
    ensure!(values.len() <= 10, s3s::s3_error!(InvalidRequest));
    for (name, value) in values {
        ensure!(
            matches!(
                name.as_str(),
                "crc32"
                    | "crc32c"
                    | "crc64nvme"
                    | "sha1"
                    | "sha256"
                    | "sha512"
                    | "md5"
                    | "xxhash64"
                    | "xxhash3"
                    | "xxhash128"
            ) && !value.is_empty()
                && value.len() <= 128,
            s3s::s3_error!(InvalidRequest)
        );
        hyper::header::HeaderValue::from_str(value)?;
    }
    Ok(())
}

#[derive(Clone, sqlx::FromRow)]
pub(crate) struct Selected {
    pub number: i32,
    pub size: i64,
    pub etag: String,
    pub checksums: Value,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Summary {
    pub upload: Uuid,
    pub size: i64,
    pub etag: String,
    pub parts: usize,
    pub algorithm: Option<String>,
    pub checksum_type: Option<String>,
    pub result: Option<Value>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Conditions {
    pub if_match: Option<String>,
    pub if_none: Option<String>,
    pub checksums: BTreeMap<String, String>,
}

pub(crate) fn checksums<'a>(
    mut sums: Value,
    algorithm: Option<&str>,
    checksum_type: Option<&str>,
    parts: impl IntoIterator<Item = &'a Value>,
    expected: &BTreeMap<String, String>,
) -> Result<Value> {
    if let Some(algorithm) = algorithm {
        let name = algorithm.to_ascii_lowercase();
        if checksum_type == Some("COMPOSITE") {
            let mut check =
                crate::upload::Integrity::new(&hyper::HeaderMap::new(), Some(algorithm))?;
            let mut count = 0;
            for part in parts {
                check.update(
                    &STANDARD.decode(part[&name].as_str().context("part checksum missing")?)?,
                );
                count += 1;
            }
            let (_, composite) = check.finish(None)?;
            sums[&name] = serde_json::json!(format!(
                "{}-{count}",
                composite[&name].as_str().context("composite missing")?
            ));
        }
        sums["type"] = serde_json::json!(checksum_type);
    }
    for (name, value) in expected {
        ensure!(
            sums.get(name).and_then(Value::as_str) == Some(value),
            s3s::s3_error!(BadDigest)
        );
    }
    Ok(sums)
}

pub(crate) struct Sources {
    pub run: Run,
    pub operation: Uuid,
    pub first: i32,
    pub reads: Vec<Uuid>,
    pub shared_permits: Vec<Uuid>,
    pub cached: Vec<node_read::Version>,
}

pub(crate) struct SourceBatch {
    pub first: i32,
    pub next: Option<i32>,
    pub parts: Vec<node_read::Reply>,
}

pub(crate) struct Lookup {
    pub run: Run,
    pub access: String,
    pub revision: i64,
    pub bucket: String,
    pub key: String,
    pub upload: Uuid,
    pub hash: String,
}

impl Lookup {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            !self.upload.is_nil()
                && self.revision >= 0
                && self.hash.len() == 64
                && self.hash.bytes().all(|value| value.is_ascii_hexdigit()),
            "invalid completion lookup"
        );
        ensure!(
            !self.access.is_empty()
                && self.access.len() <= 256
                && !self.bucket.is_empty()
                && self.bucket.len() <= 255
                && !self.key.is_empty()
                && self.key.len() <= 1024
                && [&self.access, &self.bucket, &self.key]
                    .into_iter()
                    .all(|value| !value.contains('\0')),
            "invalid completion identity"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_identity_binds_parts_checksums_and_conditions() -> Result<()> {
        let mut input = Manifest {
            upload: Uuid::now_v7(),
            parts: vec![Part {
                number: 1,
                etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
                checksums: BTreeMap::new(),
            }],
            if_match: None,
            if_none: None,
            checksum_type: None,
            size: None,
            checksums: BTreeMap::new(),
        };
        let first = input.hash()?;
        input.if_none = Some("*".into());
        assert_ne!(first, input.hash()?);
        input.parts.push(input.parts[0].clone());
        assert!(input.validate().is_err());
        input.parts.pop();
        input.parts[0]
            .checksums
            .insert("sha256".into(), "\r\n".into());
        assert!(input.validate().is_err());
        Ok(())
    }
}
