use super::*;

pub(super) const CHUNK_COLUMNS: &str = "c.id,c.uuid,c.current_encoding_id,COALESCE(l.location_id,0x00000000000000000000000000000000) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,IF(l.encoding_id IS NULL,c.block_key_id,l.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,IF(l.encoding_id IS NULL,c.nonce,l.nonce) AS nonce,c.format,c.state,c.created_at";

const ENCODING_FINGERPRINT: &str = "UNHEX(SHA2(CONCAT_WS(':',HEX(e.block_key_id),e.encryption_algorithm,e.standalone_compressed,p.compression_enabled,p.compression_strategy,p.compression_level,p.compression_sample_level,p.compression_min_savings_percent,p.compression_min_savings_bytes,'standalone'),256))";

pub(in super::super) fn chunk_query(ready_encoding: bool) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new("SELECT ");
    query
        .push(CHUNK_COLUMNS)
        .push(if ready_encoding { ",(SELECT tl.material FROM tier_locations tl WHERE tl.unit=0 AND tl.physical_id=l.location_id) AS tier_material" } else { "" })
        .push(" FROM chunks c LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id");
    if ready_encoding {
        query.push(" AND l.state='ready'");
    }
    query
}

pub(in super::super) fn lock_owned_task(task: Uuid, token: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM tasks WHERE id=? AND state='running' AND execution_token=? LOCK IN SHARE MODE",
    )
    .bind(task)
    .bind(token)
}

pub(in super::super) fn lock_bucket_uploads(id: Uuid) -> Rows<'static, (String, bool)> {
    sqlx::query_as("SELECT state,uploads_paused FROM buckets WHERE id=? LOCK IN SHARE MODE")
        .bind(id)
}

pub(in super::super) fn completing_upload(
    id: Option<Uuid>,
    bucket: Uuid,
    key: &str,
) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM uploads WHERE id=? AND bucket_id=? AND object_key=? AND state='completing')").bind(id).bind(bucket).bind(key.as_bytes())
}

pub(in super::super) fn insert_stream(input: &StreamCreate) -> Statement<'_> {
    sqlx::query("INSERT INTO streams(bucket_id,object_key,stream_type,state,metadata,public_read,write_authorization,owner_task_id,owner_execution_token) VALUES(?,?,?,'writing',?,?,?,?,?)").bind(input.bucket).bind(input.key.as_bytes()).bind(input.purpose.stream_type()).bind(&input.metadata).bind(input.public).bind(sqlx::types::Json(&input.authority)).bind(input.owner.map(|v|v.0)).bind(input.owner.map(|v|v.1))
}

pub(in super::super) fn claim_object(bucket: Uuid, key: &str, token: Id) -> Statement<'_> {
    sqlx::query("INSERT INTO objects(bucket_id,object_key,write_token) VALUES(?,?,?) ON DUPLICATE KEY UPDATE write_token=?").bind(bucket).bind(key.as_bytes()).bind(token).bind(token)
}

pub(in super::super) fn insert_fragment(tail: &StreamTail) -> Statement<'_> {
    sqlx::query(
        "INSERT INTO fragments(location_id,size,hash,owner_stream,file_id,file_offset,staging_storage_id,staging_object_key) VALUES(?,?,?,?,?,?,?,?)",
    )
    .bind(tail.fragment)
    .bind(tail.size)
    .bind(tail.hash.as_slice())
    .bind(tail.stream)
    .bind(tail.file)
    .bind(tail.file_offset).bind(tail.staging_storage).bind(&tail.staging_key)
}

pub(in super::super) fn seal_fragment(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE fragments SET sealed=TRUE WHERE id=?").bind(id)
}

pub(in super::super) fn insert_fragment_extent(tail: &StreamTail, fragment: Id) -> Statement<'_> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,fragment_id) VALUES(?,?,?,?)")
        .bind(tail.stream)
        .bind(tail.offset)
        .bind(tail.size)
        .bind(fragment)
}

pub(in super::super) fn lock_writing_stream(id: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=? AND state='writing' FOR UPDATE").bind(id)
}

pub(in super::super) fn resize_writing_stream(id: Id, size: i64) -> Statement<'static> {
    sqlx::query("UPDATE streams SET size=GREATEST(size,?),touched_at=CURRENT_TIMESTAMP(6) WHERE id=? AND state='writing'").bind(size).bind(id)
}

pub(in super::super) async fn read_snapshot<'e>(
    pool: impl sqlx::Executor<'e, Database = sqlx::MySql>,
    route: &crate::model::ReadRoute,
    access: Option<&str>,
) -> Result<Option<crate::model::ReadSnapshot>> {
    use crate::model::ReadRoute;
    use sqlx::{FromRow, Row};
    let mut query = sqlx::QueryBuilder::<sqlx::MySql>::new(
        "SELECT b.*,s.id AS stream_id,s.bucket_id AS stream_bucket_id,s.object_key AS stream_object_key,s.stream_type AS stream_stream_type,s.payload_type AS stream_payload_type,s.payload_revision AS stream_payload_revision,s.state AS stream_state,s.size AS stream_size,s.etag AS stream_etag,s.metadata AS stream_metadata,s.public_read AS stream_public_read,s.checksums AS stream_checksums,s.http_compression AS stream_http_compression,s.created_at AS stream_created_at,s.touched_at AS stream_touched_at,(SELECT tl.material FROM tier_locations tl WHERE tl.unit=2 AND tl.physical_id=r.location_id) AS tier_material,r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,e.offset_bytes,e.length,e.source_offset,c.secret_encrypted AS read_secret,EXISTS(SELECT 1 FROM s3_credential_grants g WHERE g.access_key=c.access_key_id AND g.bucket_id=b.id AND c.project_id=b.project_id AND (g.actions&2)<>0) AS read_allowed FROM buckets b LEFT JOIN objects o ON o.bucket_id=b.id AND o.object_key=",
    );
    match route.base() {
        ReadRoute::Image { .. } => unreachable!(),
        ReadRoute::Bucket {
            key,
            alias: Some((alias, alias_key)),
            ..
        } => {
            query
                .push("CASE WHEN b.name=")
                .push_bind(alias)
                .push(" THEN ")
                .push_bind(alias_key.as_bytes())
                .push(" ELSE ")
                .push_bind(key.as_bytes())
                .push(" END");
        }
        ReadRoute::Bucket { key, .. } | ReadRoute::Host { key, .. } => {
            query.push_bind(key.as_bytes());
        }
    }
    query.push(" LEFT JOIN streams s ON s.id=o.stream_id AND s.state='ready' LEFT JOIN raw_ranges e ON e.stream_id=s.id AND e.offset_bytes=0 AND e.length=s.size AND NOT EXISTS(SELECT 1 FROM raw_ranges other WHERE other.stream_id=s.id AND other.offset_bytes<>0) LEFT JOIN raw_sources r ON r.id=e.source_id LEFT JOIN s3_credentials c ON c.access_key_id=")
        .push_bind(access).push(" AND c.enabled AND (c.expires_at IS NULL OR c.expires_at>CURRENT_TIMESTAMP(6)) WHERE b.id=(");
    match route.base() {
        ReadRoute::Image { .. } => unreachable!(),
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
            object_key: String::from_utf8(row.try_get("stream_object_key")?)?,
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

#[async_trait::async_trait]
impl StreamStore for Backend {
    async fn snapshot(
        &self,
        route: &crate::model::ReadRoute,
        access: Option<&str>,
    ) -> Result<Option<crate::model::ReadSnapshot>> {
        super::super::image_transactions::snapshot(self, route, access).await
    }
    async fn cached(&self, bucket: Uuid, key: &str) -> Result<Option<crate::model::CachedStream>> {
        use sqlx::FromRow;
        let rows = sqlx::query("SELECT s.*,b.settings_revision AS bucket_revision,(b.state='active') AS bucket_active,e.offset_bytes AS cache_offset,e.length AS cache_length,e.source_offset AS cache_source,e.chunk_id AS cache_chunk,c.id AS cache_ready,c.raw_size AS cache_size,c.hash AS cache_hash FROM objects o JOIN streams s ON s.id=o.stream_id JOIN buckets b ON b.id=o.bucket_id LEFT JOIN (SELECT * FROM extents WHERE stream_id=(SELECT stream_id FROM objects WHERE bucket_id=? AND object_key=?) ORDER BY offset_bytes LIMIT 65) e ON e.stream_id=s.id LEFT JOIN chunks c ON c.id=e.chunk_id AND c.state='ready' WHERE o.bucket_id=? AND o.object_key=? AND s.state='ready' ORDER BY e.offset_bytes LIMIT 65").bind(bucket).bind(key.as_bytes()).bind(bucket).bind(key.as_bytes()).fetch_all(&self.pool).await?;
        let Some(first) = rows.first() else {
            return Ok(None);
        };
        Ok(Some(crate::model::CachedStream {
            stream: StoredStream::from_row(first)?,
            bucket_revision: first.try_get("bucket_revision")?,
            bucket_active: first.try_get("bucket_active")?,
            extents: rows
                .iter()
                .map(crate::model::CachedExtent::from_row)
                .collect::<Result<_, _>>()?,
        }))
    }

    async fn current(&self, bucket: Uuid, key: &str) -> Result<StoredStream> {
        sqlx::query_as("SELECT s.* FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=? AND o.object_key=? AND s.state='ready'").bind(bucket).bind(key.as_bytes()).fetch_optional(&self.pool).await?.ok_or_else(|| s3s::s3_error!(NoSuchKey).into())
    }
    async fn create(&self, input: StreamCreate) -> Result<Id> {
        transactions::create_stream(&self.pool, input).await
    }
    async fn append_tail(&self, tail: StreamTail) -> Result<()> {
        transactions::append_tail(&self.pool, tail).await
    }
    async fn finish(&self, stream: Id, size: i64, etag: &str, checksums: Value) -> Result<bool> {
        Ok(sqlx::query("UPDATE streams SET size=?,etag=?,checksums=?,touched_at=CURRENT_TIMESTAMP(6) WHERE id=? AND state='writing'").bind(size).bind(etag).bind(checksums).bind(stream).execute(&self.pool).await?.rows_affected()==1)
    }

    async fn learn_compression(
        &self,
        observation: crate::model::CompressionObservation,
        node: Option<Uuid>,
    ) -> Result<bool> {
        observation.validate()?;
        anyhow::ensure!(node.is_none(), "MySQL only supports single mode");
        self.check_owner().await?;
        transactions::learn_compression(&self.pool, observation).await
    }
}

pub(in super::super) fn fail_chunk_allocations(input: &ChunkCreate) -> Statement<'_> {
    sqlx::query("UPDATE chunks SET state='failed',unreferenced_at=CURRENT_TIMESTAMP(6),owner_stream=NULL WHERE storage_id=? AND active_hash=? AND raw_size=? AND state IN ('preparing','uploading') AND NOT EXISTS (SELECT 1 FROM streams s JOIN tasks t ON t.id=s.owner_task_id WHERE s.id=chunks.owner_stream AND s.state='writing' AND t.state='running' AND t.execution_token=s.owner_execution_token) AND NOT EXISTS(SELECT 1 FROM transfer_sources t WHERE t.candidate=chunks.owner_stream)").bind(input.storage).bind(input.hash.as_slice()).bind(input.raw_size)
}

pub(in super::super) fn retire_failed_encodings(input: &ChunkCreate) -> Statement<'_> {
    // Inspect failed chunks before locking any active encoding.
    sqlx::query("UPDATE chunks c STRAIGHT_JOIN chunk_encodings e ON e.chunk_id=c.id SET e.state='retired',e.unreferenced_at=CURRENT_TIMESTAMP(6) WHERE c.storage_id=? AND c.hash=? AND c.raw_size=? AND c.state='failed' AND e.state='uploading'").bind(input.storage).bind(input.hash.as_slice()).bind(input.raw_size)
}

pub(in super::super) async fn reusable_chunk(
    connection: &mut sqlx::MySqlConnection,
    input: &ChunkCreate,
) -> Result<Option<Chunk>> {
    let mut query = chunk_query(true);
    query
        .push(" WHERE c.storage_id=")
        .push_bind(input.storage)
        .push(" AND c.active_hash=")
        .push_bind(input.hash.as_slice())
        .push(" AND c.raw_size=")
        .push_bind(input.raw_size)
        .push(" AND c.state='ready' FOR UPDATE /*!80000 OF c */");
    Ok(query.build_query_as().fetch_optional(connection).await?)
}

pub(in super::super) fn insert_chunk(input: &ChunkCreate) -> Statement<'_> {
    sqlx::query("INSERT INTO chunks(uuid,storage_id,hash,raw_size,encryption_algorithm,block_key_id,owner_stream,state,unreferenced_at) VALUES(?,?,?,?,?,?,?,'preparing',CURRENT_TIMESTAMP(6))").bind(Uuid::now_v7()).bind(input.storage).bind(input.hash.as_slice()).bind(input.raw_size).bind(&input.algorithm).bind(input.key).bind(input.stream)
}

pub(in super::super) fn allocate_encoding(chunk: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,state,created_at,unreferenced_at,encryption_algorithm,block_key_id,encoding_fingerprint) SELECT id,?,storage_id,'uploading',created_at,CURRENT_TIMESTAMP(6),encryption_algorithm,block_key_id,UNHEX(REPEAT('00',32)) FROM chunks WHERE id=?").bind(Uuid::now_v7()).bind(chunk)
}

pub(in super::super) fn set_chunk_encoding(chunk: Id, encoding: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET current_encoding_id=? WHERE id=? AND state='preparing'")
        .bind(encoding)
        .bind(chunk)
}

pub(in super::super) async fn allocated_chunk(
    connection: &mut sqlx::MySqlConnection,
    id: Id,
) -> Result<Chunk> {
    let mut query = chunk_query(false);
    query.push(" WHERE c.id=").push_bind(id);
    Ok(query.build_query_as().fetch_one(connection).await?)
}

pub(in super::super) async fn refresh_encoding_fingerprint(
    connection: &mut sqlx::MySqlConnection,
    encoding: Id,
) -> Result<()> {
    let mut query = sqlx::QueryBuilder::new(
        "UPDATE chunk_encodings e JOIN storage_profiles p ON p.storage_id=e.storage_id SET e.encoding_fingerprint=",
    );
    query
        .push(ENCODING_FINGERPRINT)
        .push(" WHERE e.encoding_id=")
        .push_bind(encoding);
    query.build().execute(connection).await?;
    Ok(())
}
