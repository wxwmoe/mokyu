use super::*;

pub(super) type TaskFenceRow = (String, Option<Uuid>, Option<Uuid>, String, Value);

pub(in super::super) fn storage_for_stream(stream: Id) -> Scalar<'static, Option<Uuid>> {
    sqlx::query_scalar(
        "SELECT b.storage_id FROM streams s JOIN buckets b ON b.id=s.bucket_id WHERE s.id=$1",
    )
    .bind(stream)
}

pub(in super::super) fn extents_for_copy(stream: Id, from: i64) -> Rows<'static, Extent> {
    sqlx::query_as("SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=$2 ORDER BY offset_bytes LIMIT 500")
        .bind(stream)
        .bind(from)
}

pub(super) fn ready_chunk(id: Id) -> Rows<'static, Chunk> {
    sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=0 AND tl.physical_id=l.encoding_id) AS tier_material,c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at FROM chunks c LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE c.id=$1 AND c.state='ready'")
        .bind(id)
}

pub(super) fn fragment_source(id: Id) -> Rows<'static, FragmentSource> {
    sqlx::query_as(
        "SELECT f.id AS fragment_location,f.size,f.hash,f.staging_storage_id,f.staging_object_key,f.file_id,f.file_offset,m.storage_id AS file_storage,CASE WHEN m.source_id IS NOT NULL THEN m.id END AS raw_source FROM fragments f LEFT JOIN multipart_files m ON m.id=f.file_id WHERE f.id=$1",
    )
    .bind(id)
}

pub(super) fn extents(stream: Id, from: i64, until: i64) -> Rows<'static, Extent> {
    sqlx::query_as("SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=$2 AND offset_bytes<$3 ORDER BY offset_bytes LIMIT 64")
        .bind(stream)
        .bind(from)
        .bind(until)
}

pub(in super::super) fn lock_object_stream<'q>(
    bucket: Uuid,
    key: &'q str,
) -> Scalar<'q, Option<Id>> {
    sqlx::query_scalar(
        "SELECT stream_id FROM objects WHERE bucket_id=$1 AND object_key=$2 FOR UPDATE",
    )
    .bind(bucket)
    .bind(key)
}

pub(in super::super) fn ensure_object(bucket: Uuid, key: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO objects(bucket_id,object_key,write_token) VALUES($1,$2,$3) ON CONFLICT(bucket_id,object_key) DO NOTHING")
        .bind(bucket).bind(key).bind(Id::default())
}

pub(in super::super) fn extend_read_lease(id: Uuid, ttl: i64) -> Statement<'static> {
    sqlx::query("UPDATE stream_leases SET expires_at=now()+$2*interval '1 second' WHERE id=$1 AND lease_type='read' AND expires_at>now()")
        .bind(id)
        .bind(ttl)
}

pub(in super::super) fn delete_read_lease(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM stream_leases WHERE id=$1 AND lease_type='read'").bind(id)
}

pub(in super::super) fn extend_write_lease(stream: Id, ttl: i64) -> Statement<'static> {
    sqlx::query("UPDATE stream_leases SET expires_at=now()+$2*interval '1 second' WHERE stream_id=$1 AND lease_type='write' AND expires_at>now() AND EXISTS(SELECT 1 FROM streams WHERE id=$1 AND state='writing')")
        .bind(stream)
        .bind(ttl)
}

pub(in super::super) fn share_object_stream<'q>(
    bucket: Uuid,
    key: &'q str,
) -> Scalar<'q, Option<Id>> {
    sqlx::query_scalar(
        "SELECT stream_id FROM objects WHERE bucket_id=$1 AND object_key=$2 FOR SHARE",
    )
    .bind(bucket)
    .bind(key)
}

pub(in super::super) fn current_stream<'q>(
    bucket: Uuid,
    key: &'q str,
    lock: bool,
) -> Rows<'q, crate::app::StoredStream> {
    sqlx::query_as(if lock {
        "SELECT s.* FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=$1 AND o.object_key=$2 AND s.state='ready' FOR SHARE"
    } else {
        "SELECT s.* FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=$1 AND o.object_key=$2 AND s.state='ready'"
    }).bind(bucket).bind(key)
}

pub(in super::super) async fn read_snapshot<'e>(
    pool: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    route: &crate::model::ReadRoute,
    access: Option<&str>,
) -> Result<Option<crate::model::ReadSnapshot>> {
    use crate::model::ReadRoute;
    use sqlx::{FromRow, Row};
    let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "SELECT b.*,s.id AS stream_id,s.bucket_id AS stream_bucket_id,s.object_key AS stream_object_key,s.stream_type AS stream_stream_type,s.payload_type AS stream_payload_type,s.payload_revision AS stream_payload_revision,s.state AS stream_state,s.size AS stream_size,s.etag AS stream_etag,s.metadata AS stream_metadata,s.public_read AS stream_public_read,s.checksums AS stream_checksums,s.http_compression AS stream_http_compression,s.created_at AS stream_created_at,s.touched_at AS stream_touched_at,(SELECT tl.material FROM tier_locations tl WHERE tl.unit=2 AND tl.physical_id=r.id) AS tier_material,r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,e.offset_bytes,e.length,e.source_offset,c.secret_encrypted AS read_secret,EXISTS(SELECT 1 FROM s3_credential_grants g WHERE g.access_key=c.access_key_id AND g.bucket_id=b.id AND c.project_id=b.project_id AND 'object.read'=ANY(g.actions)) AS read_allowed FROM buckets b LEFT JOIN objects o ON o.bucket_id=b.id AND o.object_key=",
    );
    match route.base() {
        crate::model::ReadRoute::Image { .. } => unreachable!(),
        ReadRoute::Bucket {
            key,
            alias: Some((alias, alias_key)),
            ..
        } => {
            query
                .push("CASE WHEN b.name=")
                .push_bind(alias)
                .push(" THEN ")
                .push_bind(alias_key)
                .push(" ELSE ")
                .push_bind(key)
                .push(" END");
        }
        ReadRoute::Bucket { key, .. } | ReadRoute::Host { key, .. } => {
            query.push_bind(key);
        }
    }
    query.push(" LEFT JOIN streams s ON s.id=o.stream_id AND s.state='ready' LEFT JOIN raw_ranges e ON e.stream_id=s.id AND e.offset_bytes=0 AND e.length=s.size AND NOT EXISTS(SELECT 1 FROM raw_ranges other WHERE other.stream_id=s.id AND other.offset_bytes<>0) LEFT JOIN raw_sources r ON r.id=e.source_id LEFT JOIN s3_credentials c ON c.access_key_id=")
        .push_bind(access).push(" AND c.enabled AND (c.expires_at IS NULL OR c.expires_at>now()) WHERE b.id=(");
    match route.base() {
        crate::model::ReadRoute::Image { .. } => unreachable!(),
        ReadRoute::Host { host, .. } => {
            query
                .push("SELECT bucket_id FROM bucket_domains WHERE host=")
                .push_bind(host);
        }
        ReadRoute::Bucket {
            name, alias: None, ..
        } => {
            query
                .push("SELECT id FROM buckets WHERE name=")
                .push_bind(name);
        }
        ReadRoute::Bucket {
            name,
            alias: Some((alias, _)),
            ..
        } => {
            query
                .push("SELECT id FROM buckets WHERE name IN (")
                .push_bind(name)
                .push(",")
                .push_bind(alias)
                .push(") ORDER BY CASE WHEN name=")
                .push_bind(alias)
                .push(" THEN 0 ELSE 1 END LIMIT 1");
        }
    }
    query.push(")");
    let Some(row) = query.build().fetch_optional(pool).await? else {
        return Ok(None);
    };
    let stream = if row.try_get::<Option<Id>, _>("stream_id")?.is_some() {
        Some(crate::app::StoredStream {
            id: row.try_get("stream_id")?,
            bucket_id: row.try_get("stream_bucket_id")?,
            object_key: row.try_get("stream_object_key")?,
            stream_type: row.try_get("stream_stream_type")?,
            payload_type: row.try_get("stream_payload_type")?,
            payload_revision: row.try_get("stream_payload_revision")?,
            state: row.try_get("stream_state")?,
            size: row.try_get("stream_size")?,
            etag: row.try_get("stream_etag")?,
            metadata: row.try_get("stream_metadata")?,
            public_read: row.try_get("stream_public_read")?,
            checksums: row.try_get("stream_checksums")?,
            http_compression: row.try_get("stream_http_compression")?,
            created_at: row.try_get("stream_created_at")?,
            touched_at: row.try_get("stream_touched_at")?,
        })
    } else {
        None
    };
    let raw = if row.try_get::<Option<Id>, _>("raw_id")?.is_some() {
        Some(crate::model::raw::RawRange::from_row(&row)?)
    } else {
        None
    };
    Ok(Some(crate::model::ReadSnapshot {
        image: None,
        bucket: Bucket::from_row(&row)?,
        stream,
        raw,
        secret: row.try_get("read_secret")?,
        allowed: row.try_get("read_allowed")?,
    }))
}

pub(in super::super) async fn cached_stream(
    pool: &PgPool,
    bucket: Uuid,
    key: &str,
) -> Result<Option<crate::model::CachedStream>> {
    use sqlx::{FromRow, Row};
    let rows = sqlx::query("SELECT s.id,s.bucket_id,s.object_key,s.stream_type,s.state,s.size,s.etag,s.metadata,s.public_read,s.checksums,s.http_compression,s.created_at,s.touched_at,s.payload_type,s.payload_revision,b.settings_revision AS bucket_revision,(b.state='active') AS bucket_active,e.offset_bytes AS cache_offset,e.length AS cache_length,e.source_offset AS cache_source,e.chunk_id AS cache_chunk,c.id AS cache_ready,c.raw_size AS cache_size,c.hash AS cache_hash FROM objects o JOIN streams s ON s.id=o.stream_id JOIN buckets b ON b.id=o.bucket_id LEFT JOIN LATERAL (SELECT * FROM extents WHERE stream_id=s.id ORDER BY offset_bytes LIMIT 65) e ON TRUE LEFT JOIN chunks c ON c.id=e.chunk_id AND c.state='ready' WHERE o.bucket_id=$1 AND o.object_key=$2 AND s.state='ready' ORDER BY e.offset_bytes LIMIT 65").bind(bucket).bind(key).fetch_all(pool).await?;
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    Ok(Some(crate::model::CachedStream {
        stream: crate::app::StoredStream::from_row(first)?,
        bucket_revision: first.try_get("bucket_revision")?,
        bucket_active: first.try_get("bucket_active")?,
        extents: rows
            .iter()
            .map(crate::model::CachedExtent::from_row)
            .collect::<Result<_, _>>()?,
    }))
}

pub(in super::super) fn lock_owned_task<'q>(id: &'q Uuid, token: &'q Uuid) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()) FOR SHARE")
        .bind(id)
        .bind(token)
}

pub(in super::super) fn lock_bucket_uploads(id: Uuid) -> Rows<'static, (String, bool)> {
    sqlx::query_as("SELECT state,uploads_paused FROM buckets WHERE id=$1 FOR SHARE").bind(id)
}

pub(in super::super) fn completing_upload<'q>(
    id: Option<Uuid>,
    bucket: Uuid,
    key: &'q str,
) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM uploads WHERE id=$1 AND bucket_id=$2 AND object_key=$3 AND state='completing')")
        .bind(id)
        .bind(bucket)
        .bind(key)
}

pub(in super::super) fn insert_stream(input: &StreamCreate) -> Scalar<'_, Id> {
    sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,metadata,public_read,write_authorization,owner_task_id,owner_lease_token) VALUES($1,$2,$3,'writing',$4,$5,$6,$7,$8) RETURNING id")

        .bind(input.bucket)
        .bind(&input.key)
        .bind(input.purpose.stream_type())
        .bind(&input.metadata)
        .bind(input.public)
        .bind(sqlx::types::Json(&input.authority))
        .bind(input.owner.map(|value| value.0))
        .bind(input.owner.map(|value| value.1))
}

pub(in super::super) fn claim_object<'q>(bucket: Uuid, key: &'q str, token: Id) -> Statement<'q> {
    sqlx::query("INSERT INTO objects(bucket_id,object_key,write_token) VALUES($1,$2,$3) ON CONFLICT(bucket_id,object_key) DO UPDATE SET write_token=excluded.write_token")
        .bind(bucket)
        .bind(key)
        .bind(token)
}

pub(in super::super) fn insert_fragment(tail: &crate::model::StreamTail) -> Statement<'_> {
    sqlx::query("INSERT INTO fragments(id,size,hash,owner_stream,staging_storage_id,staging_object_key,file_id,file_offset) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
        .bind(tail.fragment).bind(tail.size).bind(tail.hash.as_slice()).bind(tail.stream)
        .bind(tail.staging_storage).bind(tail.staging_key.as_deref()).bind(tail.file).bind(tail.file_offset)
}

pub(in super::super) fn seal_fragment(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE fragments SET sealed=true WHERE id=$1").bind(id)
}

pub(in super::super) fn insert_fragment_extent(
    stream: Id,
    offset: i64,
    length: i32,
    fragment: Id,
) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO extents(stream_id,offset_bytes,length,fragment_id) VALUES($1,$2,$3,$4)",
    )
    .bind(stream)
    .bind(offset)
    .bind(length)
    .bind(fragment)
}

pub(in super::super) fn resize_stream(id: Id, size: i64) -> Statement<'static> {
    sqlx::query("UPDATE streams SET size=$2,touched_at=now() WHERE id=$1")
        .bind(id)
        .bind(size)
}
