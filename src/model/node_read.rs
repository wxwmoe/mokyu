use crate::{
    app::StoredStream,
    content::{codec::Chunk, pack::Pack},
    model::{
        ReadRoute,
        raw::RawSource,
        session::Run,
        source::{SourceScope, SourceType},
    },
    storage::{model::Connection, profile::StorageProfile},
};
use anyhow::{Context, Result, ensure};
use axum::http::HeaderMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;

pub(crate) const MAX_ITEMS: usize = 1024;
pub(crate) const LANES: usize = 16;
pub(crate) const MAX_CACHED: usize = 256;
pub(crate) const TARGET_BYTES: usize = 1024 * 1024;
pub(crate) const READ_SECONDS: i64 = 90;
pub(crate) const READ_LIMIT: i64 = 256;

pub(crate) struct Report {
    pub(crate) run: Run,
    pub(crate) read: Uuid,
    pub(crate) window: Uuid,
    pub(crate) segments: Vec<usize>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum MaterialType {
    Bucket,
    Object,
    Mapping,
    Storage,
    Key,
    Credential,
    Policy,
    ImagePolicy,
    ImageStyle,
    RawPages,
}

impl MaterialType {
    fn name(self) -> &'static str {
        match self {
            Self::Bucket => "bucket",
            Self::Object => "object",
            Self::Mapping => "mapping",
            Self::Storage => "storage",
            Self::Key => "block-key",
            Self::Credential => "s3-credential",
            Self::Policy => "read-policy",
            Self::ImagePolicy => "image-policy",
            Self::ImageStyle => "image-style",
            Self::RawPages => "raw-pages",
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct Version {
    pub(crate) r#type: MaterialType,
    pub(crate) id: String,
    pub(crate) revision: [u8; 32],
}

impl Version {
    pub(crate) fn new(deployment: Uuid, r#type: MaterialType, id: String, json: &[u8]) -> Self {
        let mut digest = blake3::Hasher::new_derive_key("mokyu node read material v1");
        for bytes in [
            deployment.as_bytes().as_slice(),
            r#type.name().as_bytes(),
            id.as_bytes(),
            json,
        ] {
            digest.update(&(bytes.len() as u64).to_be_bytes());
            digest.update(bytes);
        }
        Self {
            r#type,
            id,
            revision: *digest.finalize().as_bytes(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Material {
    pub(crate) version: Version,
    pub(crate) json: Option<Vec<u8>>,
}

#[derive(Clone, Default)]
pub(crate) struct Permits {
    pub(crate) held: Vec<Uuid>,
    pub(crate) released: Vec<Uuid>,
}

#[derive(Clone)]
pub(crate) struct Query {
    pub(crate) run: Run,
    pub(crate) operation: Uuid,
    pub(crate) route: ReadRoute,
    pub(crate) access: Option<String>,
    pub(crate) head: bool,
    pub(crate) web: bool,
    pub(crate) mapping: bool,
    pub(crate) protection: bool,
    pub(crate) policy: bool,
    pub(crate) headers: HeaderMap,
    pub(crate) overrides: HeaderMap,
    pub(crate) cached: Vec<Version>,
    pub(crate) max_items: usize,
    pub(crate) max_bytes: usize,
    pub(crate) lanes: usize,
    pub(crate) permits: Permits,
    pub(crate) body: Option<CachedBody>,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct CachedBody {
    pub(crate) object: Version,
    pub(crate) representation: String,
    pub(crate) policy: u32,
    pub(crate) mapping: Version,
    pub(crate) storages: Vec<Version>,
    pub(crate) verified: bool,
}

impl Query {
    pub(crate) fn prepare_image(&mut self, image: &crate::processing::image::request::Selection) {
        if image.rule.is_some() && self.body.as_ref().is_some_and(|body| !body.verified) {
            self.body = None;
        }
        if self.mapping && (image.rule.is_some() || image.settings.policy.protect_original) {
            self.head = false;
            for name in [
                "range",
                "if-range",
                "if-match",
                "if-none-match",
                "if-modified-since",
                "if-unmodified-since",
                "accept-encoding",
                "x-amz-checksum-mode",
            ] {
                self.headers.remove(name);
            }
            self.headers.insert(
                "accept-encoding",
                axum::http::HeaderValue::from_static("identity"),
            );
        }
    }
    pub(crate) fn digest(&self) -> Result<[u8; 32]> {
        let route = match self.route.base() {
            ReadRoute::Image { .. } => unreachable!(),
            ReadRoute::Bucket { name, key, alias } => {
                serde_json::json!(["bucket", name, key, alias])
            }
            ReadRoute::Host { host, key } => serde_json::json!(["host", host, key]),
        };
        fn headers(values: &HeaderMap) -> Vec<(&str, &[u8])> {
            let mut fields = values
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes()))
                .collect::<Vec<_>>();
            fields.sort();
            fields
        }
        let bytes = serde_json::to_vec(&(
            route,
            &self.access,
            self.head,
            self.web,
            self.mapping,
            self.protection,
            self.policy,
            self.lanes,
            self.route.image(),
            headers(&self.headers),
            headers(&self.overrides),
        ))?;
        Ok(*blake3::hash(&bytes).as_bytes())
    }

    pub(crate) fn select(
        &self,
        stream: &StoredStream,
        reply: &mut Reply,
        policy: Option<&Policy>,
    ) -> Result<()> {
        use crate::http::{compression::Encoding, gateway, object};
        use axum::http::header;
        let encoding = if self.access.is_some() {
            Encoding::Identity
        } else {
            let metadata = serde_json::from_value(stream.metadata.clone())?;
            let mut headers = object::metadata_headers(&metadata)?;
            headers.extend(self.overrides.clone());
            let Some(encoding) = object::representation(
                stream,
                &metadata,
                &self.headers,
                &headers,
                self.headers.contains_key("x-amz-checksum-mode"),
                !policy.is_some_and(|p| p.http_compression_enabled),
            ) else {
                reply.outcome = Outcome::NotAcceptable;
                reply.http_status = 406;
                return Ok(());
            };
            encoding
        };
        reply.representation = encoding.name().into();
        reply.etag = encoding.etag(&format!("\"{}\"", stream.etag));
        match object::conditions(stream, &self.headers, &reply.etag) {
            Ok(true) => {
                reply.outcome = Outcome::NotModified;
                reply.http_status = 304;
                return Ok(());
            }
            Err(error)
                if error
                    .downcast_ref::<s3s::S3Error>()
                    .is_some_and(|e| e.code() == &s3s::S3ErrorCode::PreconditionFailed) =>
            {
                reply.outcome = Outcome::PreconditionFailed;
                reply.http_status = 412;
                return Ok(());
            }
            Err(error) => return Err(error),
            Ok(false) => {}
        }
        let range =
            if encoding == Encoding::Identity && gateway::if_range_matches(stream, &self.headers) {
                self.headers.get(header::RANGE)
            } else {
                None
            };
        let span = if let Some(range) = range {
            let parsed = range
                .to_str()
                .ok()
                .and_then(|value| s3s::dto::Range::parse(value).ok());
            let Some(span) =
                parsed.and_then(|range| gateway::checked_range(range, stream.size).ok())
            else {
                reply.outcome = Outcome::InvalidRange;
                reply.http_status = 416;
                return Ok(());
            };
            reply.http_status = 206;
            span
        } else {
            0..stream.size as u64
        };
        reply.offset = span.start;
        reply.length = span.end - span.start;
        reply.body_reused = policy.is_none_or(|p| p.body_cache_enabled)
            && !self.head
            && self.body.as_ref().is_some_and(|body| {
                reply.object.as_ref() == Some(&body.object)
                    && (body.representation == reply.representation
                        || body.representation == "identity")
                    && body.policy == crate::http::object::POLICY as u32
            });
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct Cursor {
    pub(crate) lease: Uuid,
    pub(crate) stream: crate::model::Id,
    pub(crate) revision: i64,
    pub(crate) offset: i64,
    pub(crate) end: i64,
}

pub(crate) struct Window {
    pub(crate) run: Run,
    pub(crate) cursor: Cursor,
    pub(crate) cached: Vec<Version>,
    pub(crate) max_items: usize,
    pub(crate) max_bytes: usize,
    pub(crate) lanes: usize,
    pub(crate) permits: Permits,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    Ready,
    NoSuchBucket,
    NoSuchKey,
    AccessDenied,
    NotModified,
    PreconditionFailed,
    InvalidRange,
    Pending,
    NotAcceptable,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum GrantType {
    Read,
    StorageRead,
    Window,
    Write,
    StorageWrite,
    Native,
    Work,
}

#[derive(Clone)]
pub(crate) struct Grant {
    pub(crate) r#type: GrantType,
    pub(crate) id: Uuid,
    pub(crate) valid_for_ms: u64,
    pub(crate) storage: Option<Uuid>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct GrantRef {
    pub(crate) r#type: GrantType,
    pub(crate) id: Uuid,
}

#[derive(Default)]
pub(crate) struct KeepAlive {
    pub(crate) renew: Vec<GrantRef>,
    pub(crate) release: Vec<GrantRef>,
}

pub(crate) struct GrantResult {
    pub(crate) grant: GrantRef,
    pub(crate) accepted: bool,
    pub(crate) valid_for_ms: u64,
}

pub(crate) struct Reply {
    pub(crate) outcome: Outcome,
    pub(crate) materials: Vec<Material>,
    pub(crate) bucket: Option<Version>,
    pub(crate) object: Option<Version>,
    pub(crate) mapping: Option<Version>,
    pub(crate) credential: Option<Version>,
    pub(crate) policy: Option<Version>,
    pub(crate) image_policy: Option<Version>,
    pub(crate) image_style: Option<Version>,
    pub(crate) source_key: String,
    pub(crate) grants: Vec<Grant>,
    pub(crate) next: Option<Cursor>,
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) http_status: u16,
    pub(crate) representation: String,
    pub(crate) etag: String,
    pub(crate) body_reused: bool,
    pub(crate) origin_work: Option<Vec<u8>>,
    pub(crate) origin_range: Option<Vec<u8>>,
}

impl Reply {
    pub(crate) fn image(
        &mut self,
        deployment: Uuid,
        bucket: Uuid,
        image: &crate::processing::image::request::Selection,
        cached: &[Version],
    ) -> Result<()> {
        self.image_policy = Some(self.material(
            deployment,
            MaterialType::ImagePolicy,
            bucket.to_string(),
            &image.settings,
            cached,
        )?);
        if let Some(style) = &image.style {
            self.image_style = Some(self.material(
                deployment,
                MaterialType::ImageStyle,
                format!("{bucket}:{}", style.name),
                style,
                cached,
            )?);
        }
        self.source_key = image.key.clone();
        Ok(())
    }
    pub(crate) fn new(outcome: Outcome) -> Self {
        Self {
            outcome,
            materials: Vec::new(),
            bucket: None,
            object: None,
            mapping: None,
            credential: None,
            policy: None,
            image_policy: None,
            image_style: None,
            source_key: String::new(),
            grants: Vec::new(),
            next: None,
            offset: 0,
            length: 0,
            http_status: 200,
            representation: "identity".into(),
            etag: String::new(),
            body_reused: false,
            origin_work: None,
            origin_range: None,
        }
    }

    pub(crate) fn material(
        &mut self,
        deployment: Uuid,
        r#type: MaterialType,
        id: String,
        value: &impl Serialize,
        cached: &[Version],
    ) -> Result<Version> {
        material(&mut self.materials, deployment, r#type, id, value, cached)
    }

    pub(crate) fn bytes(&self) -> usize {
        // Reserve protobuf envelopes, references, grants and the continuation cursor.
        4096 + self.grants.len() * 128
            + self
                .materials
                .iter()
                .map(|material| {
                    material.version.id.len() * 2 + 128 + material.json.as_ref().map_or(0, Vec::len)
                })
                .sum::<usize>()
    }
}

pub(crate) fn material(
    materials: &mut Vec<Material>,
    deployment: Uuid,
    r#type: MaterialType,
    id: String,
    value: &impl Serialize,
    cached: &[Version],
) -> Result<Version> {
    Ok(binary_material(
        materials,
        deployment,
        r#type,
        id,
        serde_json::to_vec(value)?,
        cached,
    ))
}

pub(crate) fn binary_material(
    materials: &mut Vec<Material>,
    deployment: Uuid,
    r#type: MaterialType,
    id: String,
    json: Vec<u8>,
    cached: &[Version],
) -> Version {
    let version = Version::new(deployment, r#type, id, &json);
    if !materials.iter().any(|material| material.version == version) {
        materials.push(Material {
            json: (!cached.contains(&version)).then_some(json),
            version: version.clone(),
        });
    }
    version
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Policy {
    pub(crate) storage_id: Uuid,
    pub(crate) revision: i64,
    pub(crate) cors_cache_enabled: bool,
    pub(crate) public_cache_enabled: bool,
    pub(crate) http_compression_enabled: bool,
    pub(crate) body_cache_enabled: bool,
    pub(crate) refresh_seconds: u64,
    pub(crate) max_age_seconds: u64,
    pub(crate) read_integrity: String,
}

impl TryFrom<&StorageProfile> for Policy {
    type Error = anyhow::Error;

    fn try_from(profile: &StorageProfile) -> Result<Self> {
        let (refresh, age) = profile.public_cache_times()?;
        Ok(Self {
            storage_id: profile.storage_id,
            revision: profile.revision,
            cors_cache_enabled: profile.cors_cache_enabled
                || profile.public_cache_validation == "background",
            public_cache_enabled: profile.public_cache_validation == "background",
            http_compression_enabled: profile.http_compression_enabled,
            body_cache_enabled: profile.file_delivery == "application",
            refresh_seconds: refresh,
            max_age_seconds: age,
            read_integrity: profile.read_integrity.clone(),
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Object {
    pub(crate) id: crate::model::Id,
    pub(crate) bucket: Uuid,
    pub(crate) key: String,
    pub(crate) payload: String,
    pub(crate) revision: i64,
    pub(crate) size: i64,
    pub(crate) etag: String,
    pub(crate) metadata: Value,
    pub(crate) public: bool,
    pub(crate) checksums: Value,
    pub(crate) compression: Value,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) modified_at: DateTime<Utc>,
}

impl Object {
    pub(crate) fn from_stream(stream: &StoredStream) -> Result<Self> {
        let id = stream.id;
        // Pinned readers can outlive replacement of the current object.
        ensure!(
            id.is_valid()
                && matches!(stream.state.as_str(), "ready" | "retired")
                && stream.size >= 0,
            "invalid readable stream"
        );
        Ok(Self {
            id,
            bucket: stream.bucket_id,
            key: stream.object_key.clone(),
            payload: stream.payload_type.clone(),
            revision: stream.payload_revision,
            size: stream.size,
            etag: stream.etag.clone(),
            metadata: stream.metadata.clone(),
            public: stream.public_read,
            checksums: stream.checksums.clone(),
            compression: stream.http_compression.clone(),
            created_at: stream.created_at,
            modified_at: stream.touched_at,
        })
    }

    pub(crate) fn stream(&self) -> StoredStream {
        StoredStream {
            id: self.id.into(),
            bucket_id: self.bucket,
            object_key: self.key.clone(),
            stream_type: "object".into(),
            payload_type: self.payload.clone(),
            payload_revision: self.revision,
            state: "ready".into(),
            size: self.size,
            etag: self.etag.clone(),
            metadata: self.metadata.clone(),
            public_read: self.public,
            checksums: self.checksums.clone(),
            http_compression: self.compression.clone(),
            created_at: self.created_at,
            touched_at: self.modified_at,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Source {
    Chunk {
        chunk: Chunk,
    },
    Pack {
        pack: Pack,
        chunk: Chunk,
        ordinal: u32,
        offset: u64,
    },
    Raw {
        source: RawSource,
    },
    Staged {
        fragment: Uuid,
        storage: Uuid,
        key: String,
        size: i64,
        hash: Vec<u8>,
    },
    Local {
        node: Uuid,
        chunk: Chunk,
    },
    Fragment {
        node: Uuid,
        source: crate::model::FragmentSource,
    },
}

impl Source {
    pub(crate) fn cached_source(&self) -> Option<super::tiering::CachedSource> {
        use super::tiering::CachedSource;
        match self {
            Self::Chunk { chunk } | Self::Pack { chunk, .. } => {
                Some(CachedSource::Chunk(chunk.uuid))
            }
            Self::Raw { source } => Some(CachedSource::Raw(source.location_id)),
            _ => None,
        }
    }
    pub(crate) fn demand(&self) -> Option<super::tiering::Source> {
        use super::tiering::Source as Demand;
        match self {
            Self::Chunk { chunk } => Some(Demand::Chunk(chunk.location_id)),
            Self::Pack { pack, chunk, .. } => Some(Demand::Pack {
                pack: pack.location_id,
                member: chunk.uuid,
            }),
            Self::Raw { source } => Some(Demand::Raw(source.location_id)),
            _ => None,
        }
    }
    pub(crate) fn pin(&self) -> Result<(&'static str, Uuid)> {
        Ok(match self {
            Self::Chunk { chunk } => ("chunk", chunk.address()),
            Self::Pack { pack, .. } => ("pack", pack.address()),
            Self::Raw { source } => ("raw", source.address()),
            Self::Staged { fragment, .. } => ("fragment", *fragment),
            Self::Local { chunk, .. } => (
                "chunk",
                match chunk.encoding_id {
                    crate::model::Id::Uuid(id) => id,
                    crate::model::Id::Integer(_) => chunk.location_id,
                },
            ),
            Self::Fragment { source, .. } => ("fragment", source.location_id),
        })
    }
    pub(crate) fn storage(&self) -> Result<Option<Uuid>> {
        Ok(match self {
            Self::Chunk { chunk } => Some(chunk.storage_id.context("chunk storage is missing")?),
            Self::Pack { pack, .. } => Some(pack.storage_id.context("pack storage is missing")?),
            Self::Raw { source } => (!source.pending_upload).then_some(source.storage_id),
            Self::Staged { storage, .. } => Some(*storage),
            Self::Local { .. } | Self::Fragment { .. } => None,
        })
    }

    pub(crate) fn key(&self) -> Option<Uuid> {
        match self {
            Self::Chunk { chunk } => chunk.block_key_id,
            Self::Pack { chunk, .. } => chunk.block_key_id,
            _ => None,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Segment {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) page_revision: Option<[u8; 32]>,
    pub(crate) offset: i64,
    pub(crate) length: i64,
    pub(crate) source_offset: i64,
    pub(crate) source: Source,
}

impl Segment {
    pub(crate) fn pages_id(source: &RawSource, first: i64, count: i64) -> String {
        format!("{}:{}:{first}:{count}", source.location_id, source.revision)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Mapping {
    pub(crate) stream: crate::model::Id,
    pub(crate) revision: i64,
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) segments: Vec<Segment>,
}

impl Mapping {
    pub(super) fn report_scopes(
        &self,
        segments: &[usize],
    ) -> Result<BTreeMap<String, SourceScope>> {
        let mut scopes = BTreeMap::new();
        for &index in segments {
            let segment = self
                .segments
                .get(index)
                .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
            let (storage_id, source, id) = match &segment.source {
                Source::Chunk { chunk } => (
                    chunk.home_storage().context("chunk storage missing")?,
                    SourceType::Chunk,
                    chunk.encoding_id,
                ),
                Source::Pack { pack, .. } => (
                    pack.home_storage().context("pack storage missing")?,
                    SourceType::Pack,
                    pack.id,
                ),
                Source::Raw { source }
                    if !source.pending_upload && source.medium == "shared_s3" =>
                {
                    (source.home_storage(), SourceType::Raw, source.id)
                }
                _ => return Err(s3s::s3_error!(InvalidArgument).into()),
            };
            let scope = SourceScope {
                storage_id,
                source,
                id: id.to_string(),
            };
            scopes.insert(serde_json::to_string(&scope)?, scope);
        }
        Ok(scopes)
    }

    pub(crate) fn truncate(&mut self, end: i64) {
        self.end = self.end.min(end);
        self.segments.retain(|part| part.offset < self.end);
        if let Some(last) = self.segments.last_mut() {
            last.length = last.length.min(self.end - last.offset);
        }
    }

    pub(crate) fn shrink(&mut self) -> bool {
        if self.segments.len() > 1 {
            let last = &self.segments[self.segments.len().div_ceil(2) - 1];
            self.truncate(last.offset + last.length);
            return true;
        }
        let Some(part) = self.segments.first() else {
            return false;
        };
        if !matches!(part.source, Source::Raw { .. }) {
            return false;
        }
        let page = crate::content::raw_pages::PAGE as i64;
        let start = part.source_offset;
        let end = start + part.length;
        let middle_page = start / page + (((end - 1) / page) - start / page) / 2 + 1;
        let Some(mid) = middle_page.checked_mul(page) else {
            return false;
        };
        if mid >= end {
            return false;
        }
        self.truncate(part.offset + mid - start);
        true
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Storage {
    pub(crate) connection: Connection,
    pub(crate) profile: StorageProfile,
    pub(crate) revision: i64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BlockKey {
    pub(crate) id: Uuid,
    pub(crate) algorithm: String,
    pub(crate) material: [u8; 32],
}

impl BlockKey {
    pub(crate) fn unprotect(
        record: crate::model::key::ManagedKeyMaterial,
        key: &crate::content::codec::Key,
    ) -> anyhow::Result<Self> {
        let material = crate::content::codec::unprotect_bytes(
            &record.encrypted,
            key,
            format!("managed-key-{}", record.id).as_bytes(),
        )?;
        anyhow::ensure!(
            record.fingerprint == blake3::hash(&material).as_bytes(),
            "managed key fingerprint mismatch"
        );
        Ok(Self {
            id: record.id,
            algorithm: record.algorithm,
            material: material
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid block key material"))?,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Credential {
    pub(crate) access: String,
    pub(crate) secret: String,
    pub(crate) revision: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn material_reuse_requires_exact_deployment_purpose_identity_and_content() {
        let deployment = Uuid::new_v4();
        let mut initial = Reply::new(Outcome::Ready);
        let first = initial
            .material(
                deployment,
                MaterialType::Storage,
                "one".into(),
                &"value",
                &[],
            )
            .unwrap();
        assert!(initial.materials[0].json.is_some());
        let cached = [first.clone()];
        let mut reused = Reply::new(Outcome::Ready);
        let version = reused
            .material(
                deployment,
                MaterialType::Storage,
                "one".into(),
                &"value",
                &cached,
            )
            .unwrap();
        assert!(version == first);
        assert!(reused.materials[0].json.is_none());
        reused
            .material(
                deployment,
                MaterialType::Storage,
                "one".into(),
                &"value",
                &cached,
            )
            .unwrap();
        assert_eq!(reused.materials.len(), 1);
        for (deployment, r#type, id, value) in [
            (Uuid::new_v4(), MaterialType::Storage, "one", "value"),
            (deployment, MaterialType::Credential, "one", "value"),
            (deployment, MaterialType::Storage, "two", "value"),
            (deployment, MaterialType::Storage, "one", "changed"),
        ] {
            let mut reply = Reply::new(Outcome::Ready);
            let changed = reply
                .material(deployment, r#type, id.into(), &value, &cached)
                .unwrap();
            assert!(changed != first);
            assert!(reply.materials[0].json.is_some());
        }
    }
}
