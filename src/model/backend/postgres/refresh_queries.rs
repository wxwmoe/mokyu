use super::queries::{NodeReadRaw, Rows};
use crate::{
    app::{Bucket, Extent, StoredStream},
    model::{Id, ReadRoute},
};
use serde_json::json;
use uuid::Uuid;

#[derive(sqlx::FromRow)]
pub(super) struct Resolved {
    pub ordinal: i32,
    #[sqlx(flatten)]
    pub bucket: Bucket,
}

pub(super) fn buckets(routes: &[ReadRoute]) -> Rows<'static, Resolved> {
    let values: Vec<_> = routes
        .iter()
        .enumerate()
        .map(|(ordinal, route)| match route {
            ReadRoute::Bucket { name, alias, .. } => {
                json!({"ordinal":ordinal,"name":name,"alias":alias.as_ref().map(|(name, _)|name)})
            }
            ReadRoute::Host { host, .. } => json!({"ordinal":ordinal,"host":host}),
        })
        .collect();
    sqlx::query_as("SELECT r.ordinal,b.* FROM jsonb_to_recordset($1) AS r(ordinal int,name text,alias text,host text) JOIN LATERAL (SELECT id FROM buckets WHERE r.host IS NULL AND name IN(r.name,r.alias) UNION ALL SELECT bucket_id AS id FROM bucket_domains WHERE host=r.host) candidate ON true JOIN buckets b ON b.id=candidate.id ORDER BY b.id,r.ordinal FOR SHARE OF b")
        .bind(json!(values))
}

pub(super) fn streams<'a>(buckets: &'a [Uuid], keys: &'a [String]) -> Rows<'a, StoredStream> {
    sqlx::query_as("SELECT s.* FROM unnest($1::uuid[],$2::text[]) AS wanted(bucket_id,object_key) JOIN objects o USING(bucket_id,object_key) JOIN streams s ON s.id=o.stream_id WHERE s.state='ready' ORDER BY s.id FOR SHARE OF o,s")
        .bind(buckets).bind(keys)
}

pub(super) fn extents(ids: &[Id], limit: i64) -> Rows<'_, Extent> {
    sqlx::query_as("SELECT e.* FROM unnest($1::uuid[]) AS wanted(id) CROSS JOIN LATERAL (SELECT * FROM extents WHERE stream_id=wanted.id ORDER BY offset_bytes LIMIT $2 FOR SHARE) e ORDER BY e.stream_id,e.offset_bytes").bind(ids).bind(limit)
}

#[derive(sqlx::FromRow)]
pub(super) struct Raw {
    pub stream_id: Id,
    #[sqlx(flatten)]
    pub source: NodeReadRaw,
}

pub(super) fn raw(ids: &[Id], limit: i64) -> Rows<'_, Raw> {
    sqlx::query_as("SELECT e.stream_id,r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,e.offset_bytes,e.length,e.source_offset,u.node_id FROM unnest($1::uuid[]) AS wanted(id) CROSS JOIN LATERAL (SELECT * FROM raw_ranges WHERE stream_id=wanted.id ORDER BY offset_bytes LIMIT $2 FOR SHARE) e JOIN raw_sources r ON r.id=e.source_id LEFT JOIN raw_uploads u ON u.source_id=r.id ORDER BY e.stream_id,e.offset_bytes FOR SHARE OF r").bind(ids).bind(limit)
}
