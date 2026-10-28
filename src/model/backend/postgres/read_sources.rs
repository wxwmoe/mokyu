use super::queries;
use crate::{
    app::{Extent, StoredStream},
    model::{
        Error, Id,
        node_read::{Mapping, Segment, Source},
    },
};
use anyhow::{Context, Result, ensure};
use sqlx::PgConnection;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Default)]
pub(super) struct Sources {
    raw: HashMap<Id, Vec<queries::NodeReadRaw>>,
    extents: HashMap<Id, Vec<Extent>>,
    chunks: HashMap<Id, queries::NodeReadChunk>,
    encodings: HashMap<Id, crate::content::codec::Chunk>,
    packs: HashMap<Id, crate::content::pack::Pack>,
    pending: HashMap<Id, Option<Uuid>>,
    fragments: HashMap<Uuid, queries::NodeReadFragment>,
}

impl Sources {
    pub(super) async fn load(
        db: &mut PgConnection,
        raw: Vec<(Id, queries::NodeReadRaw)>,
        extents: Vec<Extent>,
    ) -> Result<Self> {
        let mut raw_by_stream: HashMap<Id, Vec<queries::NodeReadRaw>> = HashMap::new();
        for (id, row) in raw {
            raw_by_stream.entry(id).or_default().push(row);
        }
        if extents.is_empty() {
            return Ok(Self {
                raw: raw_by_stream,
                ..Self::default()
            });
        }
        let ids: Vec<_> = extents.iter().filter_map(|row| row.chunk_id).collect();
        let chunks: HashMap<_, _> = queries::node_read_chunks(&ids)
            .fetch_all(&mut *db)
            .await?
            .into_iter()
            .map(|row| (row.chunk.id, row))
            .collect();
        let encodings: HashMap<_, _> = queries::node_read_encodings(&ids)
            .fetch_all(&mut *db)
            .await?
            .into_iter()
            .map(|chunk| (chunk.id, chunk))
            .collect();
        let pack_ids: Vec<_> = chunks
            .values()
            .filter(|row| !encodings.contains_key(&row.chunk.id))
            .filter_map(|row| row.pack_id)
            .collect();
        let packs: HashMap<_, _> = queries::node_read_packs(&pack_ids)
            .fetch_all(&mut *db)
            .await?
            .into_iter()
            .map(|pack| (pack.id, pack))
            .collect();
        let pending: HashMap<_, _> = queries::node_read_pending(&ids)
            .fetch_all(&mut *db)
            .await?
            .into_iter()
            .collect();
        let fragment_ids: Vec<_> = extents.iter().filter_map(|row| row.fragment_id).collect();
        let fragments: HashMap<_, _> = queries::node_read_fragments(&fragment_ids)
            .fetch_all(&mut *db)
            .await?
            .into_iter()
            .map(|row| (row.source.location_id, row))
            .collect();

        let mut by_stream: HashMap<Id, Vec<Extent>> = HashMap::new();
        for row in extents {
            by_stream.entry(row.stream_id).or_default().push(row);
        }
        Ok(Self {
            raw: raw_by_stream,
            extents: by_stream,
            chunks,
            encodings,
            packs,
            pending,
            fragments,
        })
    }

    pub(super) fn mapping(
        &self,
        node: Uuid,
        stream: &StoredStream,
        start: i64,
        end: i64,
    ) -> Result<Mapping> {
        let mut segments = Vec::new();
        if stream.payload_type == "raw" {
            for row in self.raw.get(&stream.id).into_iter().flatten() {
                row.range.validate()?;
                ensure!(
                    (!row.range.source.pending_upload && row.range.source.medium == "shared_s3")
                        || (row.range.source.pending_upload && row.node_id == Some(node)),
                    Error::SourceUnavailable
                );
                segments.push(Segment {
                    offset: row.range.offset_bytes,
                    length: row.range.length,
                    source_offset: row.range.source_offset,
                    source: Source::Raw {
                        source: row.range.source.clone(),
                    },
                });
            }
        } else {
            ensure!(stream.payload_type == "chunks", Error::SourceUnavailable);
            for row in self.extents.get(&stream.id).into_iter().flatten() {
                let source = if let Some(id) = row.chunk_id {
                    let chunk = self.chunks.get(&id).ok_or(Error::SourceUnavailable)?;
                    ensure!(
                        i64::from(row.source_offset) + i64::from(row.length)
                            <= i64::from(chunk.chunk.raw_size),
                        Error::SourceUnavailable
                    );
                    if let Some(chunk) = self.encodings.get(&id) {
                        Source::Chunk {
                            chunk: chunk.clone(),
                        }
                    } else if let Some(pack) = chunk.pack_id.and_then(|id| self.packs.get(&id)) {
                        Source::Pack {
                            pack: pack.clone(),
                            chunk: chunk.chunk.clone(),
                        }
                    } else if self.pending.get(&id) == Some(&Some(node)) {
                        Source::Local {
                            node,
                            chunk: chunk.chunk.clone(),
                        }
                    } else {
                        return Err(Error::SourceUnavailable.into());
                    }
                } else {
                    let Some(Id::Uuid(id)) = row.fragment_id else {
                        return Err(Error::SourceUnavailable.into());
                    };
                    let fragment = self.fragments.get(&id).ok_or(Error::SourceUnavailable)?;
                    ensure!(
                        row.source_offset >= 0
                            && row.source_offset + row.length <= fragment.source.size,
                        Error::SourceUnavailable
                    );
                    if let (Some(storage), Some(key)) = (
                        fragment.source.staging_storage_id,
                        &fragment.source.staging_object_key,
                    ) {
                        Source::Staged {
                            fragment: id,
                            storage,
                            key: key.clone(),
                            size: i64::from(fragment.source.size),
                            hash: fragment.source.hash.clone(),
                        }
                    } else {
                        ensure!(fragment.node_id == Some(node), Error::SourceUnavailable);
                        Source::Fragment {
                            node,
                            source: fragment.source.clone(),
                        }
                    }
                };
                segments.push(Segment {
                    offset: row.offset_bytes,
                    length: i64::from(row.length),
                    source_offset: i64::from(row.source_offset),
                    source,
                });
            }
        }
        let mut position = start;
        for segment in &mut segments {
            let source_end = segment
                .offset
                .checked_add(segment.length)
                .context("read extent overflow")?;
            ensure!(
                segment.offset <= position && source_end > position,
                Error::SourceUnavailable
            );
            segment.source_offset += position - segment.offset;
            segment.offset = position;
            segment.length = source_end.min(end) - position;
            position += segment.length;
        }
        ensure!(
            position > start && position <= end,
            Error::SourceUnavailable
        );
        let Id::Uuid(id) = stream.id else {
            anyhow::bail!("node reads require UUID stream identities");
        };
        Ok(Mapping {
            stream: id,
            revision: stream.payload_revision,
            start,
            end: position,
            segments,
        })
    }
}
