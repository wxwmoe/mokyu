use super::wire;
use crate::model::{ReadRoute, node_read as read, session};
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use std::collections::HashSet;
use tonic::Status;

pub(super) const REQUEST_LIMIT: usize = 128 * 1024;
pub(crate) const HEADERS: &[&str] = &[
    "range",
    "if-range",
    "if-match",
    "if-none-match",
    "if-modified-since",
    "if-unmodified-since",
    "accept-encoding",
    "cache-control",
    "pragma",
    "x-amz-checksum-mode",
];
pub(crate) const OVERRIDES: &[&str] = &[
    "content-type",
    "content-disposition",
    "content-encoding",
    "content-language",
    "cache-control",
    "expires",
];

fn invalid() -> Status {
    Status::invalid_argument("invalid read request")
}

pub(super) fn keep_alive(
    renew: Vec<wire::GrantRef>,
    release: Vec<wire::GrantRef>,
) -> Result<read::KeepAlive, Status> {
    if renew.len() + release.len() > 256 {
        return Err(invalid());
    }
    let mut seen = HashSet::new();
    let mut parse = |values: Vec<wire::GrantRef>| {
        values
            .into_iter()
            .map(|value| {
                let id = super::uuid(&value.id)?;
                if !seen.insert(id) {
                    return Err(invalid());
                }
                let r#type = match wire::GrantType::try_from(value.r#type) {
                    Ok(wire::GrantType::ReadProtection) => read::GrantType::Read,
                    Ok(wire::GrantType::StorageRead) => read::GrantType::StorageRead,
                    Ok(wire::GrantType::ReadWindow) => read::GrantType::Window,
                    Ok(wire::GrantType::WriteProtection) => read::GrantType::Write,
                    Ok(wire::GrantType::StorageWrite) => read::GrantType::StorageWrite,
                    _ => return Err(invalid()),
                };
                Ok(read::GrantRef { r#type, id })
            })
            .collect::<Result<Vec<_>, Status>>()
    };
    Ok(read::KeepAlive {
        renew: parse(renew)?,
        release: parse(release)?,
    })
}

pub(super) fn grant_result(result: read::GrantResult) -> wire::GrantResult {
    wire::GrantResult {
        grant: Some(wire::GrantRef {
            r#type: match result.grant.r#type {
                read::GrantType::Read => wire::GrantType::ReadProtection,
                read::GrantType::StorageRead => wire::GrantType::StorageRead,
                read::GrantType::Window => wire::GrantType::ReadWindow,
                read::GrantType::Write => wire::GrantType::WriteProtection,
                read::GrantType::StorageWrite => wire::GrantType::StorageWrite,
            } as i32,
            id: result.grant.id.to_string(),
        }),
        accepted: result.accepted,
        valid_for_ms: result.valid_for_ms,
    }
}

fn limits(items: u32, bytes: u32) -> Result<(usize, usize), Status> {
    let items = if items == 0 {
        read::MAX_ITEMS
    } else {
        items as usize
    };
    let bytes = if bytes == 0 {
        read::TARGET_BYTES
    } else {
        bytes as usize
    };
    if items > read::MAX_ITEMS || !(16 * 1024..=super::MESSAGE_LIMIT - 32 * 1024).contains(&bytes) {
        return Err(invalid());
    }
    Ok((items, bytes))
}

pub(super) fn run(input: Option<wire::Run>) -> Result<session::Run, Status> {
    let input = input.ok_or_else(invalid)?;
    Ok(session::Run {
        boot: super::uuid(&input.boot_id)?,
        fence: super::uuid(&input.fence)?,
    })
}

pub(super) fn observations(
    input: wire::ObservationRequest,
) -> Result<crate::model::node_observation::Batch, Status> {
    if prost::Message::encoded_len(&input) > super::OBSERVATION_LIMIT {
        return Err(Status::resource_exhausted(
            "observation batch exceeds message budget",
        ));
    }
    let batch = crate::model::node_observation::Batch {
        run: run(input.run)?,
        compression: input
            .compression
            .into_iter()
            .map(|value| {
                Ok(crate::model::CompressionObservation {
                    stream: super::uuid(&value.stream_id)?.into(),
                    version: value.version.try_into().map_err(|_| invalid())?,
                    size: value.size.try_into().map_err(|_| invalid())?,
                    encoding: match value.encoding.as_str() {
                        "gzip" => "gzip",
                        "br" => "br",
                        "zstd" => "zstd",
                        _ => return Err(invalid()),
                    },
                })
            })
            .collect::<Result<_, Status>>()?,
        credentials: input.credentials,
    };
    batch.validate().map_err(|_| invalid())?;
    Ok(batch)
}

pub(super) fn report(input: wire::ReadReportRequest) -> Result<read::Report, Status> {
    if input.segments.is_empty() || input.segments.len() > 16 {
        return Err(invalid());
    }
    let mut seen = HashSet::new();
    if input
        .segments
        .iter()
        .any(|value| *value as usize >= read::MAX_ITEMS || !seen.insert(*value))
    {
        return Err(invalid());
    }
    Ok(read::Report {
        run: run(input.run)?,
        read: super::uuid(&input.read_id)?,
        window: super::uuid(&input.window_id)?,
        segments: input
            .segments
            .into_iter()
            .map(|value| value as usize)
            .collect(),
    })
}

fn headers(input: Vec<wire::ReadHeader>, overrides: bool) -> Result<HeaderMap, Status> {
    if input.len() > 32
        || input
            .iter()
            .map(|field| field.name.len() + field.value.len())
            .sum::<usize>()
            > 16 * 1024
    {
        return Err(invalid());
    }
    let mut headers = HeaderMap::new();
    for field in input {
        let name = HeaderName::from_bytes(field.name.as_bytes()).map_err(|_| invalid())?;
        let allowed = if overrides {
            OVERRIDES.contains(&name.as_str())
        } else {
            HEADERS.contains(&name.as_str())
        };
        if !allowed
            || (headers.contains_key(&name)
                && (overrides
                    || !matches!(
                        name.as_str(),
                        "accept-encoding" | "cache-control" | "pragma"
                    )))
        {
            return Err(invalid());
        }
        headers.append(
            name,
            HeaderValue::from_bytes(&field.value).map_err(|_| invalid())?,
        );
    }
    Ok(headers)
}

fn text(value: &str, maximum: usize) -> bool {
    value.len() <= maximum && !value.contains('\0')
}

pub(crate) fn cached(values: Vec<wire::Cached>) -> Result<Vec<read::Version>, Status> {
    if values.len() > read::MAX_CACHED {
        return Err(invalid());
    }
    let mut seen = HashSet::new();
    values
        .into_iter()
        .map(|value| {
            let r#type = match wire::MaterialType::try_from(value.r#type) {
                Ok(wire::MaterialType::Bucket) => read::MaterialType::Bucket,
                Ok(wire::MaterialType::ObjectMetadata) => read::MaterialType::Object,
                Ok(wire::MaterialType::ObjectMapping) => read::MaterialType::Mapping,
                Ok(wire::MaterialType::Storage) => read::MaterialType::Storage,
                Ok(wire::MaterialType::BlockKey) => read::MaterialType::Key,
                Ok(wire::MaterialType::S3Credential) => read::MaterialType::Credential,
                Ok(wire::MaterialType::ReadPolicy) => read::MaterialType::Policy,
                _ => return Err(invalid()),
            };
            if value.id.is_empty()
                || !text(&value.id, 256)
                || !seen.insert((r#type, value.id.clone()))
            {
                return Err(invalid());
            }
            Ok(read::Version {
                r#type,
                id: value.id,
                revision: value.revision.try_into().map_err(|_| invalid())?,
            })
        })
        .collect()
}

pub(crate) fn query(input: wire::ReadRequest) -> Result<read::Query, Status> {
    let run = run(input.run)?;
    let operation = super::uuid(&input.operation_id)?;
    let head = match wire::ReadMethod::try_from(input.method) {
        Ok(wire::ReadMethod::Get) => false,
        Ok(wire::ReadMethod::Head) => true,
        _ => return Err(invalid()),
    };
    let web = match wire::ReadSurface::try_from(input.surface) {
        Ok(wire::ReadSurface::S3) => false,
        Ok(wire::ReadSurface::Web) => true,
        _ => return Err(invalid()),
    };
    if input.include.is_empty()
        || input.include.len() > 4
        || !text(&input.access_key, 128)
        || (web && !input.access_key.is_empty())
    {
        return Err(invalid());
    }
    let mut seen = HashSet::new();
    let mut mapping = false;
    let mut protection = false;
    let mut policy = false;
    for include in input.include {
        if !seen.insert(include) {
            return Err(invalid());
        }
        match wire::ReadInclude::try_from(include) {
            Ok(wire::ReadInclude::Object) => {}
            Ok(wire::ReadInclude::Mapping) if !head => mapping = true,
            Ok(wire::ReadInclude::Policy) => policy = true,
            Ok(wire::ReadInclude::Protection) if !head && !web && !input.access_key.is_empty() => {
                protection = true
            }
            _ => return Err(invalid()),
        }
    }
    let route = match input
        .route
        .and_then(|route| route.route)
        .ok_or_else(invalid)?
    {
        wire::read_route::Route::Bucket(route) => {
            if (route.name.is_empty() && route.alias_name.is_empty())
                || !text(&route.name, 255)
                || !text(&route.key, 1024)
                || !text(&route.alias_name, 255)
                || !text(&route.alias_key, 1024)
                || (!input.access_key.is_empty() && !route.alias_name.is_empty())
                || (route.alias_name.is_empty() && !route.alias_key.is_empty())
            {
                return Err(invalid());
            }
            ReadRoute::Bucket {
                name: route.name,
                key: route.key,
                alias: (!route.alias_name.is_empty())
                    .then_some((route.alias_name, route.alias_key)),
            }
        }
        wire::read_route::Route::Host(route) => {
            if !web || route.host.is_empty() || !text(&route.host, 255) || !text(&route.key, 1024) {
                return Err(invalid());
            }
            ReadRoute::Host {
                host: crate::control::buckets::host(&route.host).map_err(|_| invalid())?,
                key: route.key,
            }
        }
    };
    let (max_items, max_bytes) = limits(input.max_items, input.max_bytes)?;
    let body = input
        .body
        .map(|body| {
            let object = cached(vec![body.object.ok_or_else(invalid)?])?.remove(0);
            let mapping = cached(vec![body.mapping.ok_or_else(invalid)?])?.remove(0);
            let storages = cached(body.storages)?;
            if object.r#type != read::MaterialType::Object
                || mapping.r#type != read::MaterialType::Mapping
                || storages.len() > 16
                || storages
                    .iter()
                    .any(|version| version.r#type != read::MaterialType::Storage)
                || body.policy == 0
                || !matches!(
                    body.representation.as_str(),
                    "identity" | "gzip" | "br" | "zstd"
                )
            {
                return Err(invalid());
            }
            Ok(read::CachedBody {
                object,
                representation: body.representation,
                policy: body.policy,
                mapping,
                storages,
                verified: body.verified,
            })
        })
        .transpose()?;
    Ok(read::Query {
        run,
        operation,
        route,
        access: (!input.access_key.is_empty()).then_some(input.access_key),
        head,
        web,
        mapping,
        protection,
        policy,
        headers: headers(input.headers, false)?,
        overrides: headers(input.overrides, true)?,
        cached: cached(input.cached)?,
        max_items,
        max_bytes,
        body,
    })
}

pub(super) fn window(input: wire::ReadWindowRequest) -> Result<read::Window, Status> {
    let cursor = input.cursor.ok_or_else(invalid)?;
    if cursor.payload_revision == 0
        || cursor.payload_revision > i64::MAX as u64
        || cursor.offset >= cursor.end
        || cursor.end > i64::MAX as u64
    {
        return Err(invalid());
    }
    let (max_items, max_bytes) = limits(input.max_items, input.max_bytes)?;
    Ok(read::Window {
        run: run(input.run)?,
        cursor: read::Cursor {
            lease: super::uuid(&cursor.lease_id)?,
            stream: super::uuid(&cursor.stream_id)?,
            revision: cursor.payload_revision as i64,
            offset: cursor.offset as i64,
            end: cursor.end as i64,
        },
        cached: cached(input.cached)?,
        max_items,
        max_bytes,
    })
}

pub(crate) fn version(value: read::Version) -> wire::Cached {
    let r#type = match value.r#type {
        read::MaterialType::Bucket => wire::MaterialType::Bucket,
        read::MaterialType::Object => wire::MaterialType::ObjectMetadata,
        read::MaterialType::Mapping => wire::MaterialType::ObjectMapping,
        read::MaterialType::Storage => wire::MaterialType::Storage,
        read::MaterialType::Key => wire::MaterialType::BlockKey,
        read::MaterialType::Credential => wire::MaterialType::S3Credential,
        read::MaterialType::Policy => wire::MaterialType::ReadPolicy,
    };
    wire::Cached {
        r#type: r#type as i32,
        id: value.id,
        revision: value.revision.to_vec(),
    }
}

pub(crate) fn reply(input: read::Reply) -> wire::ReadReply {
    let outcome = match input.outcome {
        read::Outcome::Ready => wire::ReadOutcome::Ready,
        read::Outcome::NoSuchBucket => wire::ReadOutcome::NoSuchBucket,
        read::Outcome::NoSuchKey => wire::ReadOutcome::NoSuchKey,
        read::Outcome::AccessDenied => wire::ReadOutcome::AccessDenied,
        read::Outcome::NotModified => wire::ReadOutcome::NotModified,
        read::Outcome::PreconditionFailed => wire::ReadOutcome::PreconditionFailed,
        read::Outcome::InvalidRange => wire::ReadOutcome::InvalidRange,
        read::Outcome::Pending => wire::ReadOutcome::Pending,
        read::Outcome::NotAcceptable => wire::ReadOutcome::NotAcceptable,
    };
    wire::ReadReply {
        outcome: outcome as i32,
        materials: input.materials.into_iter().map(material).collect(),
        bucket: input.bucket.map(version),
        object: input.object.map(version),
        mapping: input.mapping.map(version),
        credential: input.credential.map(version),
        policy: input.policy.map(version),
        grants: input.grants.into_iter().map(grant).collect(),
        next: input.next.map(|cursor| wire::ReadCursor {
            lease_id: cursor.lease.to_string(),
            stream_id: cursor.stream.to_string(),
            payload_revision: cursor.revision as u64,
            offset: cursor.offset as u64,
            end: cursor.end as u64,
        }),
        offset: input.offset,
        length: input.length,
        http_status: u32::from(input.http_status),
        representation: input.representation,
        etag: input.etag,
        body_reused: input.body_reused,
    }
}

pub(super) fn material(value: read::Material) -> wire::Material {
    wire::Material {
        version: Some(version(value.version)),
        value: Some(match value.json {
            Some(json) => wire::material::Value::Json(json),
            None => wire::material::Value::Reuse(true),
        }),
    }
}

pub(super) fn grant(value: read::Grant) -> wire::Grant {
    wire::Grant {
        r#type: match value.r#type {
            read::GrantType::Read => wire::GrantType::ReadProtection,
            read::GrantType::StorageRead => wire::GrantType::StorageRead,
            read::GrantType::Window => wire::GrantType::ReadWindow,
            read::GrantType::Write => wire::GrantType::WriteProtection,
            read::GrantType::StorageWrite => wire::GrantType::StorageWrite,
        } as i32,
        id: value.id.to_string(),
        valid_for_ms: value.valid_for_ms,
        storage_id: value.storage.map_or_else(String::new, |id| id.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn observation_batches_bound_values_and_reject_duplicates() {
        let batch = wire::ObservationRequest {
            run: Some(wire::Run {
                boot_id: Uuid::new_v4().to_string(),
                fence: Uuid::new_v4().to_string(),
            }),
            compression: vec![wire::CompressionObservation {
                stream_id: Uuid::new_v4().to_string(),
                version: 1,
                encoding: "gzip".into(),
                size: 300,
            }],
            credentials: vec!["reader".into()],
        };
        assert!(observations(batch.clone()).is_ok());
        let mut duplicate = batch.clone();
        duplicate.compression.push(duplicate.compression[0].clone());
        assert!(observations(duplicate).is_err());
        let mut duplicate = batch.clone();
        duplicate.credentials.push("reader".into());
        assert!(observations(duplicate).is_err());
        let mut oversized = batch.clone();
        oversized.credentials = (0..64).map(|index| index.to_string()).collect();
        assert!(observations(oversized).is_err());
        for field in ["version", "size", "encoding", "stream"] {
            let mut invalid = batch.clone();
            let entry = &mut invalid.compression[0];
            match field {
                "version" => entry.version = u32::MAX,
                "size" => entry.size = u64::MAX,
                "encoding" => entry.encoding = "identity".into(),
                _ => entry.stream_id = Uuid::nil().to_string(),
            }
            assert!(observations(invalid).is_err());
        }
        assert!(
            observations(wire::ObservationRequest {
                run: batch.run.clone(),
                ..Default::default()
            })
            .is_err()
        );
        let mut oversized = batch;
        oversized.credentials = vec!["x".repeat(super::super::OBSERVATION_LIMIT)];
        assert_eq!(
            observations(oversized).err().unwrap().code(),
            tonic::Code::ResourceExhausted
        );
    }

    fn request() -> wire::ReadRequest {
        wire::ReadRequest {
            run: Some(wire::Run {
                boot_id: Uuid::new_v4().to_string(),
                fence: Uuid::new_v4().to_string(),
            }),
            operation_id: Uuid::new_v4().to_string(),
            route: Some(wire::ReadRoute {
                route: Some(wire::read_route::Route::Bucket(wire::BucketRoute {
                    name: "bucket".into(),
                    key: "object".into(),
                    ..Default::default()
                })),
            }),
            method: wire::ReadMethod::Get as i32,
            surface: wire::ReadSurface::S3 as i32,
            include: vec![
                wire::ReadInclude::Object as i32,
                wire::ReadInclude::Mapping as i32,
            ],
            ..Default::default()
        }
    }

    #[test]
    fn requests_bound_headers_cache_and_mapping_intent() {
        let input = request();
        let parsed = query(input.clone()).unwrap();
        assert!(parsed.mapping);
        assert_eq!(parsed.max_items, 1024);
        assert_eq!(parsed.max_bytes, read::TARGET_BYTES);
        let mut head = input.clone();
        head.method = wire::ReadMethod::Head as i32;
        assert!(query(head.clone()).is_err());
        head.include = vec![wire::ReadInclude::Object as i32];
        assert!(!query(head.clone()).unwrap().mapping);
        head.include.push(wire::ReadInclude::Policy as i32);
        assert!(query(head).unwrap().policy);
        for include in [vec![], vec![0], vec![1, 1], vec![1, 2, 4]] {
            let mut changed = input.clone();
            changed.include = include;
            assert!(query(changed).is_err());
        }
        for (items, bytes) in [(1025, 0), (0, 1), (0, super::super::MESSAGE_LIMIT as u32)] {
            let mut changed = input.clone();
            changed.max_items = items;
            changed.max_bytes = bytes;
            assert!(query(changed).is_err());
        }
        for name in ["authorization", "host", "x-admin", "content-length"] {
            let mut changed = input.clone();
            changed.headers.push(wire::ReadHeader {
                name: name.into(),
                value: b"hidden".to_vec(),
            });
            assert!(query(changed).is_err());
        }
        let value = wire::Cached {
            r#type: wire::MaterialType::Storage as i32,
            id: "one".into(),
            revision: vec![1; 32],
        };
        let mut changed = input.clone();
        changed.cached = vec![value.clone(); 257];
        assert!(query(changed).is_err());
        assert!(cached(vec![value.clone(), value.clone()]).is_err());
        let mut invalid = value.clone();
        invalid.revision.pop();
        assert!(cached(vec![invalid]).is_err());
        assert!(cached(vec![value]).is_ok());
        let mut changed = input;
        changed.run = None;
        assert!(query(changed).is_err());
    }

    #[test]
    fn public_routes_use_the_configured_domain_rules_including_ports() {
        let mut input = request();
        input.surface = wire::ReadSurface::Web as i32;
        for (host, valid) in [
            ("WEB.example:8443", true),
            ("web.example", true),
            ("web.example:", false),
            ("user@web.example", false),
            ("web.example.", false),
        ] {
            input.route = Some(wire::ReadRoute {
                route: Some(wire::read_route::Route::Host(wire::HostRoute {
                    host: host.into(),
                    key: "object".into(),
                })),
            });
            let parsed = query(input.clone());
            assert_eq!(parsed.is_ok(), valid, "{host}");
            if let Ok(parsed) = parsed {
                assert!(
                    matches!(parsed.route, ReadRoute::Host { host: normalized, .. } if normalized == host.to_ascii_lowercase())
                );
            }
        }
    }

    #[test]
    fn protected_metadata_requires_a_signed_object_read() {
        let mut input = request();
        input.include = vec![
            wire::ReadInclude::Object as i32,
            wire::ReadInclude::Protection as i32,
        ];
        assert!(query(input.clone()).is_err());
        input.access_key = "copy-reader".into();
        let parsed = query(input.clone()).unwrap();
        assert!(parsed.protection && !parsed.mapping);
        input.method = wire::ReadMethod::Head as i32;
        assert!(query(input.clone()).is_err());
        input.method = wire::ReadMethod::Get as i32;
        input.include.push(5);
        assert!(query(input).is_err());
    }

    #[test]
    fn alias_only_routes_do_not_accept_signed_or_empty_bucket_targets() {
        let mut input = request();
        input.route = Some(wire::ReadRoute {
            route: Some(wire::read_route::Route::Bucket(wire::BucketRoute {
                name: String::new(),
                key: String::new(),
                alias_name: "public-bucket".into(),
                alias_key: "object".into(),
            })),
        });
        assert!(query(input.clone()).is_ok());
        input.access_key = "signed".into();
        assert!(query(input.clone()).is_err());
        input.access_key.clear();
        let Some(wire::read_route::Route::Bucket(route)) =
            input.route.as_mut().and_then(|route| route.route.as_mut())
        else {
            unreachable!()
        };
        route.alias_name.clear();
        assert!(query(input).is_err());
    }

    #[test]
    fn resource_batches_reject_duplicates_unknown_kinds_and_oversized_work() {
        let grant = wire::GrantRef {
            r#type: wire::GrantType::ReadProtection as i32,
            id: Uuid::new_v4().to_string(),
        };
        assert!(keep_alive(vec![grant.clone()], Vec::new()).is_ok());
        assert!(
            keep_alive(
                vec![wire::GrantRef {
                    r#type: wire::GrantType::WriteProtection as i32,
                    id: grant.id.clone()
                }],
                Vec::new()
            )
            .is_ok()
        );
        assert!(keep_alive(vec![grant.clone()], vec![grant.clone()]).is_err());
        assert!(keep_alive(vec![grant.clone(); 257], Vec::new()).is_err());
        for (r#type, id) in [
            (0, grant.id.clone()),
            (i32::MAX, grant.id.clone()),
            (1, Uuid::nil().to_string()),
        ] {
            assert!(keep_alive(vec![wire::GrantRef { r#type, id }], Vec::new()).is_err());
        }
    }
}
