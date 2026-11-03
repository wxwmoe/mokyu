use super::queries;
use crate::model::node_read as read;
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

pub(super) async fn node_read_mapping(
    db: &mut queries::node_requests::Connection,
    node: Uuid,
    stream: &crate::app::StoredStream,
    start: i64,
    end: i64,
    limit: usize,
    extents: Option<Vec<crate::app::Extent>>,
) -> Result<crate::model::node_read::Mapping> {
    let (raw, extents) = if stream.payload_type == "raw" {
        (
            queries::node_read_raw(stream.id, start, end, limit as i64)
                .fetch_all(&mut *db)
                .await?
                .into_iter()
                .map(|row| (stream.id, row))
                .collect(),
            Vec::new(),
        )
    } else {
        let extents = match extents {
            Some(rows) => rows,
            None => {
                queries::node_read_extents(stream.id, start, end, limit as i64)
                    .fetch_all(&mut *db)
                    .await?
            }
        };
        (Vec::new(), extents)
    };
    queries::sources::Sources::load(db, raw, extents)
        .await?
        .mapping(node, stream, start, end)
}

pub(super) async fn node_read_materials(
    db: &mut queries::node_requests::Connection,
    deployment: Uuid,
    key: &crate::content::codec::Key,
    mapping: &mut crate::model::node_read::Mapping,
    max_bytes: usize,
    cached: &[crate::model::node_read::Version],
    reply: &mut crate::model::node_read::Reply,
) -> Result<Vec<crate::model::node_read::Storage>> {
    use crate::model::{
        Error,
        node_read::{BlockKey, MaterialType, Storage},
    };
    use std::collections::{BTreeSet, HashMap};
    let storages: Vec<_> = mapping
        .segments
        .iter()
        .map(|segment| segment.source.storage())
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let keys: Vec<_> = mapping
        .segments
        .iter()
        .filter_map(|segment| segment.source.key())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let records = queries::node_read_storages(&storages)
        .fetch_all(&mut *db)
        .await?;
    let mut profiles: HashMap<_, _> = queries::node_read_profiles(&storages)
        .fetch_all(&mut *db)
        .await?
        .into_iter()
        .map(|row| (row.storage_id, row))
        .collect();
    ensure!(records.len() == storages.len(), Error::SourceUnavailable);
    let mut materials = Vec::new();
    for record in records {
        let mut profile = profiles
            .remove(&record.id)
            .context("read storage profile is missing")?;
        profile.write_durability = None;
        let storage = Storage {
            connection: crate::storage::model::Connection::unprotect(&record, key)?,
            revision: profile.revision,
            profile,
        };
        reply.material(
            deployment,
            MaterialType::Storage,
            record.id.to_string(),
            &storage,
            cached,
        )?;
        materials.push(storage);
    }
    let records = queries::node_read_keys(&keys).fetch_all(&mut *db).await?;
    ensure!(records.len() == keys.len(), Error::SourceUnavailable);
    for record in records {
        let block = BlockKey::unprotect(record, key)?;
        reply.material(
            deployment,
            MaterialType::Key,
            block.id.to_string(),
            &block,
            cached,
        )?;
    }
    use crate::content::raw_pages::{HASH, PAGE};
    let page = PAGE as i64;
    let mut remaining = (max_bytes / HASH).clamp(1, 16384) as i64;
    let mut end = mapping.end;
    for segment in &mut mapping.segments {
        let read::Source::Raw { source } = &segment.source else {
            continue;
        };
        if !source.pending_upload
            && !materials.iter().any(|s| {
                s.connection.id == source.storage_id
                    && (s.profile.read_integrity == "inline" || s.profile.cache_enabled)
            })
        {
            continue;
        }
        if remaining == 0 {
            end = segment.offset;
            break;
        }
        let first = segment.source_offset / page;
        let last = (segment.source_offset + segment.length - 1) / page;
        let take = (last - first + 1).min(remaining);
        remaining -= take;
        segment.page_revision =
            super::raw_pages::read(db, source.id, source.size, first, take as usize)
                .await?
                .map(|bytes| {
                    read::binary_material(
                        &mut reply.materials,
                        deployment,
                        MaterialType::RawPages,
                        read::Segment::pages_id(source, first, take),
                        bytes,
                        cached,
                    )
                    .revision
                });
        if take < last - first + 1 {
            end = segment.offset + ((first + take) * page - segment.source_offset);
            break;
        }
    }
    mapping.truncate(end);
    reply.mapping = Some(reply.material(
        deployment,
        MaterialType::Mapping,
        format!(
            "{}:{}:{}:{}",
            mapping.stream, mapping.revision, mapping.start, mapping.end
        ),
        mapping,
        cached,
    )?);
    Ok(materials)
}

pub(super) async fn node_read_body_matches(
    db: &mut queries::node_requests::Connection,
    node: &crate::cluster::NodeState,
    key: &crate::content::codec::Key,
    stream: &crate::app::StoredStream,
    body: &crate::model::node_read::CachedBody,
    cached: Option<&[read::Version]>,
    output: &mut Vec<read::Material>,
) -> Result<bool> {
    use crate::model::node_read::{self as read, MaterialType, Outcome, Reply};
    let mut mapping = node_read_mapping(
        db,
        node.node_id,
        stream,
        0,
        stream.size,
        read::MAX_ITEMS,
        None,
    )
    .await?;
    if mapping.end != stream.size {
        return Ok(false);
    }
    let mut probe = Reply::new(Outcome::Ready);
    node_read_materials(
        db,
        node.deployment_id,
        key,
        &mut mapping,
        read::TARGET_BYTES,
        &[],
        &mut probe,
    )
    .await?;
    if mapping.end != stream.size {
        return Ok(false);
    }
    if !body.verified {
        for material in &probe.materials {
            if material.version.r#type == MaterialType::Storage {
                let Some(json) = &material.json else {
                    return Ok(false);
                };
                let storage: read::Storage = serde_json::from_slice(json)?;
                if storage.profile.read_integrity == "inline" {
                    return Ok(false);
                }
            }
        }
    }
    let policies: Vec<_> = probe
        .materials
        .iter()
        .filter(|material| material.version.r#type == MaterialType::Storage)
        .map(|material| &material.version)
        .collect();
    let matches = probe.mapping.as_ref() == Some(&body.mapping)
        && policies.len() == body.storages.len()
        && policies
            .iter()
            .all(|version| body.storages.contains(version));
    if matches && let Some(cached) = cached {
        for mut material in probe
            .materials
            .into_iter()
            .filter(|m| m.version.r#type == MaterialType::Storage)
        {
            if cached.contains(&material.version) {
                material.json = None;
            }
            output.push(material);
        }
    }
    Ok(matches)
}
