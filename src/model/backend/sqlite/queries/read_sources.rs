use super::{NodeReadChunk, NodeReadFragment, NodeReadRaw, Rows};
use crate::{app::Extent, content::codec::Chunk, model::Id};
use sqlx::types::Json;
use uuid::Uuid;

pub(in super::super) fn node_read_extents(
    stream: Id,
    start: i64,
    end: i64,
    limit: i64,
) -> Rows<'static, Extent> {
    sqlx::query_as("SELECT * FROM extents WHERE stream_id=? AND offset_bytes>=?-4194304 AND offset_bytes+length>? AND offset_bytes<? ORDER BY offset_bytes LIMIT ?").bind(stream).bind(start).bind(start).bind(end).bind(limit)
}
pub(in super::super) fn node_read_chunks(ids: &[Id]) -> Rows<'_, NodeReadChunk> {
    sqlx::query_as("SELECT c.*,COALESCE(l.location_id,zeroblob(16)) AS location_id FROM chunks c LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id WHERE c.id IN (SELECT CAST(value AS INTEGER) FROM json_each(?)) AND c.state='ready' ORDER BY c.id").bind(Json(ids))
}
pub(in super::super) fn node_pack_members(
    ids: &[Id],
) -> Rows<'_, crate::model::physical::PackMemberRow> {
    sqlx::query_as("SELECT m.chunk_id,c.uuid,c.hash,c.raw_size,m.ordinal,m.offset_bytes,m.encoding_uuid,m.encoded_size,m.compressed,m.encryption_algorithm,m.block_key_id,m.nonce FROM pack_members m JOIN chunks c ON c.id=m.chunk_id AND c.pack_id=m.pack_id WHERE c.id IN (SELECT CAST(value AS INTEGER) FROM json_each(?)) ORDER BY c.id")
        .bind(Json(ids))
}

pub(in super::super) fn node_read_encodings(ids: &[Id]) -> Rows<'_, Chunk> {
    sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=0 AND tl.physical_id=l.location_id) AS tier_material,c.id,c.uuid,l.encoding_id AS current_encoding_id,l.location_id,l.storage_id,c.hash,c.raw_size,l.standalone_size,l.encryption_algorithm,l.block_key_id,l.standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunks c JOIN chunk_encodings l ON l.encoding_id=(SELECT e.encoding_id FROM chunk_encodings e WHERE e.chunk_id=c.id AND e.state='ready' AND e.quarantined_at IS NULL ORDER BY e.encoding_id LIMIT 1) WHERE c.id IN (SELECT CAST(value AS INTEGER) FROM json_each(?)) ORDER BY c.id").bind(Json(ids))
}
pub(in super::super) fn node_read_packs(ids: &[Id]) -> Rows<'_, crate::content::pack::Pack> {
    sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=1 AND tl.physical_id=p.location_id) AS tier_material,p.* FROM packs p WHERE p.id IN (SELECT CAST(value AS INTEGER) FROM json_each(?)) AND p.state='ready' AND p.quarantined_at IS NULL ORDER BY p.id").bind(Json(ids))
}
pub(in super::super) fn node_read_pending(ids: &[Id]) -> Rows<'_, (Id, Option<Uuid>)> {
    sqlx::query_as("SELECT p.chunk_id,COALESCE(p.node_id,(SELECT id FROM nodes WHERE enabled AND role='control' LIMIT 1)) FROM pending_uploads p WHERE p.chunk_id IN (SELECT CAST(value AS INTEGER) FROM json_each(?)) ORDER BY p.chunk_id").bind(Json(ids))
}
pub(in super::super) fn node_read_fragments(ids: &[Id]) -> Rows<'_, NodeReadFragment> {
    sqlx::query_as("SELECT f.id,f.location_id AS fragment_location,f.size,f.hash,f.staging_storage_id,f.staging_object_key,f.file_id,f.file_offset,m.storage_id AS file_storage,CASE WHEN m.source_id IS NOT NULL THEN m.id END AS raw_source,COALESCE(m.node_id,(SELECT id FROM nodes WHERE enabled AND role='control' LIMIT 1)) AS node_id FROM fragments f LEFT JOIN multipart_files m ON m.id=f.file_id WHERE f.id IN (SELECT CAST(value AS INTEGER) FROM json_each(?)) ORDER BY f.id").bind(Json(ids))
}
pub(in super::super) fn node_read_storages(
    ids: &[Uuid],
) -> Rows<'_, crate::storage::model::Record> {
    sqlx::query_as("SELECT s.* FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id IN (SELECT unhex(value) FROM json_each(?)) AND s.enabled AND s.type='s3' ORDER BY s.id").bind(Json(ids.iter().map(|id|id.simple().to_string()).collect::<Vec<_>>()))
}
pub(in super::super) fn node_read_profiles(
    ids: &[Uuid],
) -> Rows<'_, crate::storage::profile::StorageProfile> {
    sqlx::query_as("SELECT p.* FROM storage_profiles p WHERE p.storage_id IN (SELECT unhex(value) FROM json_each(?)) ORDER BY p.storage_id").bind(Json(ids.iter().map(|id|id.simple().to_string()).collect::<Vec<_>>()))
}
pub(in super::super) fn node_read_keys(
    ids: &[Uuid],
) -> Rows<'_, crate::model::key::ManagedKeyMaterial> {
    sqlx::query_as("SELECT id,name,purpose,key_algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE id IN (SELECT unhex(value) FROM json_each(?)) AND purpose='block' AND secret_encrypted IS NOT NULL ORDER BY id").bind(Json(ids.iter().map(|id|id.simple().to_string()).collect::<Vec<_>>()))
}
pub(in super::super) fn node_read_raw(
    stream: Id,
    start: i64,
    end: i64,
    limit: i64,
) -> Rows<'static, NodeReadRaw> {
    sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=2 AND tl.physical_id=r.location_id) AS tier_material,r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,e.offset_bytes,e.length,e.source_offset,COALESCE((SELECT node_id FROM raw_uploads a WHERE a.source_id=r.id),(SELECT id FROM nodes WHERE enabled AND role='control' LIMIT 1)) AS node_id FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id WHERE e.stream_id=? AND e.offset_bytes>=COALESCE((SELECT offset_bytes FROM raw_ranges WHERE stream_id=? AND offset_bytes<=? ORDER BY offset_bytes DESC LIMIT 1),?) AND e.offset_bytes+e.length>? AND e.offset_bytes<? ORDER BY e.offset_bytes LIMIT ?").bind(stream).bind(stream).bind(start).bind(start).bind(start).bind(end).bind(limit)
}
