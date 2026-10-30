use super::{
    Snapshot,
    external::{Part, Phase, Plan},
};
use crate::{app::Metadata, model::Id, storage::external::objects::Object};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Write {
    pub stream: Id,
    pub storage: Uuid,
    pub offset: u64,
    pub size: u64,
    pub operation: Option<Uuid>,
}

impl Write {
    pub(crate) fn validate(&self, plan: &Plan) -> Result<()> {
        ensure!(
            plan.options.source_external
                && !plan.options.target_external
                && plan.state.phase == Phase::Import
                && !self.storage.is_nil()
                && self.operation.is_none_or(|id| id.get_version_num() == 7)
                && (self.offset, self.size)
                    == plan.state.range(i32::from(plan.state.position) + 1)?,
            "invalid native import write"
        );
        Ok(())
    }
}

pub(crate) fn metadata(object: &Object) -> Result<Value> {
    object.validate()?;
    let get = |name: &str| object.metadata.get(name).cloned();
    let user: s3s::dto::Metadata = object
        .metadata
        .iter()
        .filter_map(|(name, value)| {
            name.strip_prefix("x-amz-meta-")
                .map(|name| (name.into(), value.clone()))
        })
        .collect();
    Ok(serde_json::to_value(Metadata {
        content_type: get("content-type"),
        cache_control: get("cache-control"),
        content_disposition: get("content-disposition"),
        content_encoding: get("content-encoding"),
        content_language: get("content-language"),
        expires: get("expires"),
        user: (!user.is_empty()).then_some(user),
    })?)
}

pub(crate) fn receipt(number: i32, stream: &crate::app::StoredStream) -> Result<Part> {
    ensure!(
        stream.etag.len() == 32 && hex::decode(&stream.etag)?.len() == 16,
        "native import checksum missing"
    );
    let digest = STANDARD.decode(
        stream
            .checksums
            .get("sha256")
            .and_then(Value::as_str)
            .context("native import SHA256 missing")?,
    )?;
    let part = Part {
        number,
        size: stream.size,
        etag: format!("\"{}\"", stream.etag),
        digest,
    };
    part.validate_record()?;
    Ok(part)
}

pub(crate) fn snapshot(plan: &Plan) -> Result<Snapshot> {
    let source = plan
        .state
        .source
        .as_ref()
        .context("import source missing")?;
    ensure!(plan.state.phase == Phase::Complete, "import is incomplete");
    plan.validate()?;
    let mut md5 = Md5::new();
    let mut sha256 = Sha256::new();
    for part in &plan.parts {
        let digest = hex::decode(part.etag.trim_matches('"'))?;
        ensure!(digest.len() == 16, "invalid native part digest");
        md5.update(digest);
        sha256.update(&part.digest);
    }
    let (etag, checksums) = if plan.parts.is_empty() {
        (
            hex::encode(Md5::digest([])),
            json!({"sha256":STANDARD.encode(Sha256::digest([]))}),
        )
    } else if plan.parts.len() == 1 {
        let part = &plan.parts[0];
        (
            part.etag.trim_matches('"').into(),
            json!({"sha256":STANDARD.encode(&part.digest)}),
        )
    } else {
        (
            format!("{}-{}", hex::encode(md5.finalize()), plan.parts.len()),
            json!({"sha256":format!("{}-{}", STANDARD.encode(sha256.finalize()), plan.parts.len()),"type":"COMPOSITE"}),
        )
    };
    Ok(Snapshot {
        metadata: metadata(source)?,
        checksums,
        public: plan.options.permissions.public(
            plan.state.source_public.unwrap_or(false),
            plan.state.target_public,
        ),
        size: i64::try_from(source.size)?,
        etag,
        payload: "chunks".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn imported_metadata_round_trips_supported_headers() -> Result<()> {
        let object = Object {
            size: 7,
            etag: "\"opaque-provider-etag\"".into(),
            version: None,
            modified: None,
            metadata: [
                ("content-type", "image/jpeg"),
                ("cache-control", "max-age=60"),
                ("content-disposition", "inline"),
                ("content-encoding", "gzip"),
                ("content-language", "en"),
                ("expires", "Wed, 21 Oct 2015 07:28:00 GMT"),
                ("x-amz-meta-label", "source"),
            ]
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect(),
        };
        let snapshot = Snapshot {
            size: 7,
            etag: "native".into(),
            metadata: metadata(&object)?,
            ..Default::default()
        };
        let native = super::super::external::Native::new(Uuid::new_v4().into(), &snapshot)?;
        assert_eq!(native.object.metadata, object.metadata);
        assert_ne!(native.object.etag, object.etag);
        Ok(())
    }
}
