use crate::{
    app::StoredStream,
    content::{codec::Chunk, pack::Pack},
    model::{ReadRoute, raw::RawSource, session::Run},
    storage::{model::Connection, profile::StorageProfile},
};
use anyhow::{Context, Result, ensure};
use axum::http::HeaderMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub(crate) const MAX_ITEMS: usize = 1024;
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

pub(crate) struct Material {
    pub(crate) version: Version,
    pub(crate) json: Option<Vec<u8>>,
}

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
    pub(crate) fn digest(&self) -> Result<[u8; 32]> {
        let route = match &self.route {
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
            headers(&self.headers),
            headers(&self.overrides),
        ))?;
        Ok(*blake3::hash(&bytes).as_bytes())
    }

    pub(crate) fn select(&self, stream: &StoredStream, reply: &mut Reply) -> Result<()> {
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
                false,
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
        reply.body_reused = !self.head
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
    pub(crate) stream: Uuid,
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
    pub(crate) grants: Vec<Grant>,
    pub(crate) next: Option<Cursor>,
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) http_status: u16,
    pub(crate) representation: String,
    pub(crate) etag: String,
    pub(crate) body_reused: bool,
}

impl Reply {
    pub(crate) fn new(outcome: Outcome) -> Self {
        Self {
            outcome,
            materials: Vec::new(),
            bucket: None,
            object: None,
            mapping: None,
            credential: None,
            policy: None,
            grants: Vec::new(),
            next: None,
            offset: 0,
            length: 0,
            http_status: 200,
            representation: "identity".into(),
            etag: String::new(),
            body_reused: false,
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
    let json = serde_json::to_vec(value)?;
    let version = Version::new(deployment, r#type, id, &json);
    if !materials.iter().any(|material| material.version == version) {
        materials.push(Material {
            json: (!cached.contains(&version)).then_some(json),
            version: version.clone(),
        });
    }
    Ok(version)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Policy {
    pub(crate) storage_id: Uuid,
    pub(crate) revision: i64,
    pub(crate) cors_cache_enabled: bool,
    pub(crate) public_cache_enabled: bool,
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
            refresh_seconds: refresh,
            max_age_seconds: age,
            read_integrity: profile.read_integrity.clone(),
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Object {
    pub(crate) id: Uuid,
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
        let crate::model::Id::Uuid(id) = stream.id else {
            anyhow::bail!("node reads require UUID stream identities");
        };
        ensure!(
            !id.is_nil() && stream.state == "ready" && stream.size >= 0,
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
    pub(crate) fn pin(&self) -> Result<(&'static str, Uuid)> {
        Ok(match self {
            Self::Chunk { chunk } => ("chunk", chunk.location_id),
            Self::Pack { pack, .. } => ("pack", pack.location_id),
            Self::Raw { source } => ("raw", source.location_id),
            Self::Staged { fragment, .. } => ("fragment", *fragment),
            Self::Local { chunk, .. } => {
                let crate::model::Id::Uuid(id) = chunk.encoding_id else {
                    anyhow::bail!("invalid encoding identity");
                };
                ("chunk", id)
            }
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
            Self::Pack { pack, .. } => pack.block_key_id,
            _ => None,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Segment {
    pub(crate) offset: i64,
    pub(crate) length: i64,
    pub(crate) source_offset: i64,
    pub(crate) source: Source,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Mapping {
    pub(crate) stream: Uuid,
    pub(crate) revision: i64,
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) segments: Vec<Segment>,
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
