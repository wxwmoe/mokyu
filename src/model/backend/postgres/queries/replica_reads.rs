use super::*;

pub(super) fn read_extents(stream: Id, from: i64, until: i64) -> Rows<'static, ReadExtent> {
    // Page before joining; a missing ready chunk must fail row decoding.
    sqlx::query_as("SELECT e.*,c.pack_id AS demand_pack,m.offset_bytes AS pack_offset,m.encoded_size AS pack_length,c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at FROM (SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=$2 AND offset_bytes<$3 ORDER BY offset_bytes LIMIT 64) e LEFT JOIN chunks c ON c.id=e.chunk_id AND c.state='ready' LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' LEFT JOIN pack_members m ON m.pack_id=c.pack_id AND m.chunk_id=c.id ORDER BY e.offset_bytes")
        .bind(stream)
        .bind(from)
        .bind(until)
}

#[derive(Clone, sqlx::FromRow)]
pub(in super::super) struct ReplicaIdentity {
    pub deployment_id: Uuid,
    pub schema_version: i64,
    pub deployment_key_fingerprint: Vec<u8>,
    pub deployment_mode: String,
}

pub(in super::super) fn replica_identity() -> Rows<'static, ReplicaIdentity> {
    sqlx::query_as(
        "SELECT deployment_id,schema_version,deployment_key_fingerprint,deployment_mode FROM mokyu_meta WHERE singleton",
    )
}

pub(in super::super) fn check_replica(identity: &ReplicaIdentity) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT pg_is_in_recovery() AND current_setting('server_version_num')::int>=180000 AND EXISTS(SELECT 1 FROM mokyu_meta WHERE singleton AND deployment_id=$1 AND schema_version=$2 AND deployment_key_fingerprint=$3 AND deployment_mode='cluster')")
        .bind(identity.deployment_id).bind(identity.schema_version).bind(&identity.deployment_key_fingerprint)
}

pub(in super::super) async fn replica_page(
    connection: &mut sqlx::PgConnection,
    identity: &ReplicaIdentity,
    stream: Id,
    lease: Uuid,
    from: i64,
    until: i64,
) -> Result<Option<Vec<ReadExtent>>> {
    use sqlx::{FromRow, Row};
    // The lease marker and the page share one snapshot; an older lease cannot authorize this request.
    let rows = sqlx::query("WITH checked AS MATERIALIZED (SELECT pg_is_in_recovery() AND EXISTS(SELECT 1 FROM mokyu_meta WHERE singleton AND deployment_id=$5 AND schema_version=$6 AND deployment_key_fingerprint=$7 AND deployment_mode='cluster') AS valid,EXISTS(SELECT 1 FROM stream_leases WHERE id=$4 AND stream_id=$1 AND lease_type='read') AS visible) SELECT checked.valid,checked.visible,e.*,c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at FROM checked LEFT JOIN LATERAL (SELECT * FROM extents WHERE checked.valid AND checked.visible AND stream_id=$1 AND offset_bytes>=$2 AND offset_bytes<$3 ORDER BY offset_bytes LIMIT 64) e ON TRUE LEFT JOIN chunks c ON c.id=e.chunk_id AND c.state='ready' LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' ORDER BY e.offset_bytes")
        .bind(stream).bind(from).bind(until).bind(lease)
        .bind(identity.deployment_id).bind(identity.schema_version).bind(&identity.deployment_key_fingerprint)
        .fetch_all(connection).await?;
    let first = rows.first().context("replica validation returned no row")?;
    ensure!(
        first.try_get::<bool, _>("valid")?,
        "replica identity or role changed"
    );
    if !first.try_get::<bool, _>("visible")? {
        return Ok(None);
    }
    let mut page = Vec::with_capacity(rows.len());
    let mut position = from;
    for row in &rows {
        let row = ReadExtent::from_row(row)?;
        let extent = &row.extent;
        ensure!(
            extent.stream_id == stream
                && extent.offset_bytes == position
                && extent.length > 0
                && extent.fragment_id.is_none()
                && extent.chunk_id == Some(row.chunk.id)
                && extent.source_offset >= 0
                && i64::from(extent.source_offset) + i64::from(extent.length)
                    <= i64::from(row.chunk.raw_size)
                && row.chunk.hash.len() == 32
                && row.chunk.state == "ready",
            "invalid replica mapping"
        );
        position = position
            .checked_add(i64::from(extent.length))
            .context("replica mapping overflow")?;
        page.push(row);
    }
    ensure!(
        page.len() == 64 || position >= until,
        "replica mapping is incomplete"
    );
    Ok(Some(page))
}

pub(in super::super) async fn node_replica_page(
    connection: &mut sqlx::PgConnection,
    identity: &ReplicaIdentity,
    cursor: &crate::model::node_read::Cursor,
    limit: usize,
) -> Result<Option<Vec<Extent>>> {
    use sqlx::{FromRow, Row};
    // A replicated read marker binds this page to the primary's leased payload revision.
    let rows = sqlx::query("WITH checked AS MATERIALIZED (SELECT pg_is_in_recovery() AND EXISTS(SELECT 1 FROM mokyu_meta WHERE singleton AND deployment_id=$1 AND schema_version=$2 AND deployment_key_fingerprint=$3 AND deployment_mode='cluster') AS valid,EXISTS(SELECT 1 FROM node_reads n JOIN stream_leases l ON l.id=n.read_id JOIN streams s ON s.id=l.stream_id WHERE n.read_id=$4 AND n.replica_allowed AND l.stream_id=$5 AND l.lease_type='read' AND n.payload_revision=$6 AND s.payload_revision=$6 AND s.payload_type='chunks' AND n.offset_bytes<=$7 AND n.end_bytes=$8) AS visible) SELECT checked.valid,checked.visible,e.* FROM checked LEFT JOIN LATERAL (SELECT * FROM extents WHERE checked.valid AND checked.visible AND stream_id=$5 AND offset_bytes>=$7-4194304 AND offset_bytes+length>$7 AND offset_bytes<$8 ORDER BY offset_bytes LIMIT $9) e ON true ORDER BY e.offset_bytes")
        .bind(identity.deployment_id).bind(identity.schema_version).bind(&identity.deployment_key_fingerprint)
        .bind(cursor.lease).bind(cursor.stream).bind(cursor.revision).bind(cursor.offset).bind(cursor.end).bind(limit as i64)
        .fetch_all(connection).await?;
    let first = rows.first().context("replica validation returned no row")?;
    ensure!(
        first.try_get::<bool, _>("valid")?,
        "replica identity or role changed"
    );
    if !first.try_get::<bool, _>("visible")? {
        return Ok(None);
    }
    let mut page = Vec::with_capacity(rows.len());
    let mut position = cursor.offset;
    for row in &rows {
        let extent = Extent::from_row(row)?;
        if extent.fragment_id.is_some() {
            return Ok(None);
        }
        let end = extent
            .offset_bytes
            .checked_add(i64::from(extent.length))
            .context("replica mapping overflow")?;
        ensure!(
            extent.stream_id == cursor.stream
                && extent.chunk_id.is_some()
                && extent.offset_bytes <= position
                && end > position
                && extent.length > 0
                && extent.source_offset >= 0
                && i64::from(extent.source_offset) + i64::from(extent.length)
                    <= crate::content::codec::MAX as i64,
            "invalid replica mapping"
        );
        position = end.min(cursor.end);
        page.push(extent);
    }
    ensure!(
        page.len() == limit || position == cursor.end,
        "replica mapping is incomplete"
    );
    Ok(Some(page))
}

pub(super) fn read_start(stream: Id, position: i64) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT offset_bytes FROM extents WHERE stream_id=$1 AND offset_bytes<=$2 ORDER BY offset_bytes DESC LIMIT 1")
        .bind(stream)
        .bind(position)
}
