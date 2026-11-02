use super::*;

#[async_trait::async_trait]
impl PhysicalStore for Backend {
    async fn recover_memory(&self, boot: Uuid) -> Result<()> {
        transactions::recover_chunk_memory(&self.pool, boot).await
    }
    async fn buffer_chunk(&self, chunk: &Chunk, boot: Option<Uuid>) -> Result<()> {
        ensure!(
            buffer_chunk(chunk, boot)
                .execute(&self.pool)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        Ok(())
    }
    async fn buffered_chunks(
        &self,
        after: Id,
        before: Option<chrono::DateTime<chrono::Utc>>,
        stream: Option<Id>,
        chunk: Option<Id>,
        files_only: bool,
        urgent: &[Uuid],
    ) -> Result<Vec<Chunk>> {
        let mut query = chunk_query(false);
        query.push(" JOIN storage_profiles sp ON sp.storage_id=c.storage_id");
        query.push(" WHERE c.state='ready' AND l.sync_pending AND l.quarantined_at IS NULL AND l.state IN ('ready','uploading') AND c.id>").push_bind(after);
        if files_only {
            query.push(" AND l.boot_id IS NULL");
            write_delay(&mut query, "l.created_at", "c.storage_id", urgent);
        }
        if let Some(before) = before {
            query
                .push(" AND l.created_at<=")
                .push_bind(before.naive_utc());
        }
        if let Some(stream) = stream {
            query
                .push(" AND EXISTS(SELECT 1 FROM extents e WHERE e.chunk_id=c.id AND e.stream_id=")
                .push_bind(stream)
                .push(")");
        }
        if let Some(chunk) = chunk {
            query.push(" AND c.id=").push_bind(chunk);
        }
        query.push(" ORDER BY c.id LIMIT 128");
        Ok(query.build_query_as().fetch_all(&self.pool).await?)
    }
    async fn synced_chunk(&self, chunk: &Chunk, failure: Option<&str>) -> Result<()> {
        sqlx::query("UPDATE chunk_encodings SET sync_pending=CASE WHEN ? IS NULL THEN FALSE ELSE sync_pending END,boot_id=CASE WHEN ? IS NULL THEN NULL ELSE boot_id END,quarantine_code=?,quarantined_at=CASE WHEN ? IS NULL THEN NULL ELSE CURRENT_TIMESTAMP(6) END,quarantine_revision=quarantine_revision+CASE WHEN ? IS NULL THEN 0 ELSE 1 END WHERE encoding_id=? AND location_id=? AND sync_pending AND quarantined_at IS NULL AND state IN ('ready','uploading')").bind(failure).bind(failure).bind(failure).bind(failure).bind(failure).bind(chunk.encoding_id).bind(chunk.location_id).execute(&self.pool).await?;
        Ok(())
    }
    async fn buffered_status(&self) -> Result<Value> {
        let (files, bytes, oldest, volatile): (i64,i64,Option<String>,i64) = sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(c.raw_size),0) AS SIGNED),CAST(MIN(l.created_at) AS CHAR),CAST(COALESCE(SUM(CASE WHEN l.boot_id IS NOT NULL THEN 1 ELSE 0 END),0) AS SIGNED) FROM chunk_encodings l JOIN chunks c ON c.current_encoding_id=l.encoding_id WHERE c.state='ready' AND l.sync_pending AND l.state IN ('ready','uploading') AND l.quarantined_at IS NULL").fetch_one(&self.pool).await?;
        Ok(serde_json::json!({"pending":files,"bytes":bytes,"oldest":oldest,"volatile":volatile}))
    }

    async fn source_snapshot(
        &self,
        scope: &crate::model::source::SourceScope,
    ) -> Result<crate::model::source::SourceSnapshot> {
        transactions::read_source_snapshot(&self.pool, scope).await
    }
    async fn quarantines(
        &self,
        storage: Uuid,
        packed: bool,
        after: Id,
        limit: i64,
    ) -> Result<Vec<crate::model::source::QuarantinedSource>> {
        quarantines(
            &mut *self.pool.acquire().await?,
            storage,
            packed,
            after,
            limit,
        )
        .await
    }
    async fn source_update(&self, update: crate::model::source::SourceUpdate<'_>) -> Result<()> {
        transactions::source_update(&self.pool, update).await
    }

    async fn write_backlog(&self) -> Result<Vec<(Uuid, chrono::DateTime<chrono::Utc>)>> {
        let rows: Vec<(Uuid, chrono::NaiveDateTime)> = sqlx::query_as("SELECT storage_id,MIN(created_at) AS oldest FROM (SELECT c.storage_id,p.created_at FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id UNION ALL SELECT r.storage_id,r.created_at FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE r.pending_upload AND r.state='ready' AND r.quarantine_code IS NULL AND EXISTS(SELECT 1 FROM raw_ranges e WHERE e.source_id=r.id) UNION ALL SELECT r.storage_id,r.created_at FROM raw_sources r WHERE r.durability IN ('buffered','volatile') AND r.state='ready' AND r.quarantine_code IS NULL AND EXISTS(SELECT 1 FROM raw_ranges e WHERE e.source_id=r.id) UNION ALL SELECT l.storage_id,l.created_at FROM chunk_encodings l WHERE l.sync_pending AND l.quarantined_at IS NULL AND l.state IN ('ready','uploading') AND EXISTS(SELECT 1 FROM chunks c JOIN extents e ON e.chunk_id=c.id WHERE c.id=l.chunk_id AND c.current_encoding_id=l.encoding_id)) waiting GROUP BY storage_id").fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|(id, time)| (id, time.and_utc()))
            .collect())
    }
    async fn has_chunks(&self) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunks)")
            .fetch_one(&self.pool)
            .await?)
    }
    async fn indexed(&self, location: Uuid) -> Result<bool> {
        Ok(super::work_sweep::physical_indexed(location)
            .fetch_one(&self.pool)
            .await?)
    }
    async fn upload_pins(
        &self,
        after: Id,
        _owner: Option<Uuid>,
    ) -> Result<Vec<crate::model::physical::StagedUpload>> {
        Ok(sqlx::query_as("SELECT p.chunk_id,IF(p.cache_key IS NULL,NULL,c.storage_id),p.cache_key,p.staged_size,c.storage_id FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE p.chunk_id>? ORDER BY p.chunk_id LIMIT 512").bind(after).fetch_all(&self.pool).await?)
    }
    async fn pending_source(&self, chunk: Id) -> Result<Option<(Uuid, String)>> {
        Ok(sqlx::query_as("SELECT c.storage_id,p.cache_key FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE p.chunk_id=? AND p.cache_key IS NOT NULL").bind(chunk).fetch_optional(&self.pool).await?)
    }
    async fn pending_only(&self, chunk: Id) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=?) AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=? AND state='ready') AND NOT EXISTS(SELECT 1 FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE c.id=? AND p.state='ready')").bind(chunk).bind(chunk).bind(chunk).fetch_one(&self.pool).await?)
    }
    async fn register_encoding(&self, chunk: &Chunk) -> Result<()> {
        transactions::register_encoding(&self.pool, chunk).await
    }
    async fn source(&self, id: Id, inspection: bool) -> Result<PhysicalSource> {
        let mut connection = self.pool.begin().await?;
        let selected = physical_source(&mut connection, id, inspection).await?;
        connection.commit().await?;
        Ok(selected)
    }
    async fn pack_chunks(&self, pack: Id) -> Result<Vec<Chunk>> {
        let mut query = chunk_query(true);
        query.push(" WHERE c.state='ready' AND c.id IN (SELECT chunk_id FROM pack_members WHERE pack_id=").push_bind(pack).push(")");
        Ok(query.build_query_as().fetch_all(&self.pool).await?)
    }
    async fn pack_contains(&self, pack: Id, chunk: Id) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pack_members WHERE pack_id=? AND chunk_id=?)",
        )
        .bind(pack)
        .bind(chunk)
        .fetch_one(&self.pool)
        .await?)
    }
    async fn inspection(&self, task: Uuid, pack: Id) -> Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar(
            "SELECT error_code FROM integrity_packs WHERE task_id=? AND pack_id=?",
        )
        .bind(task)
        .bind(pack)
        .fetch_optional(&self.pool)
        .await?)
    }
    async fn inspection_members(&self, pack: Id) -> Result<Vec<(Chunk, i64)>> {
        let mut query = sqlx::QueryBuilder::new("SELECT ");
        query.push(CHUNK_COLUMNS).push(",m.offset_bytes FROM pack_members m JOIN chunks c ON c.id=m.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE m.pack_id=").push_bind(pack).push(" ORDER BY m.ordinal LIMIT 4097");
        Ok(query
            .build()
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(|row| Ok((sqlx::FromRow::from_row(row)?, row.try_get("offset_bytes")?)))
            .collect::<sqlx::Result<_>>()?)
    }
    async fn record_inspection(&self, task: Uuid, pack: Id, code: Option<&str>) -> Result<()> {
        sqlx::query("INSERT INTO integrity_packs(task_id,pack_id,error_code) VALUES(?,?,?) ON DUPLICATE KEY UPDATE task_id=integrity_packs.task_id").bind(task).bind(pack).bind(code).execute(&self.pool).await?;
        Ok(())
    }
}

pub(super) fn object_key(row: &sqlx::mysql::MySqlRow) -> sqlx::Result<String> {
    String::from_utf8(row.try_get("object_key")?)
        .map_err(|error| sqlx::Error::Decode(Box::new(error)))
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for StoredStream {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            bucket_id: row.try_get("bucket_id")?,
            object_key: object_key(row)?,
            stream_type: row.try_get("stream_type")?,
            payload_type: row.try_get("payload_type")?,
            payload_revision: row.try_get("payload_revision")?,
            state: row.try_get("state")?,
            size: row.try_get("size")?,
            etag: row.try_get("etag")?,
            metadata: row.try_get("metadata")?,
            public_read: row.try_get("public_read")?,
            checksums: row.try_get("checksums")?,
            http_compression: row.try_get("http_compression")?,
            created_at: row.try_get("created_at")?,
            touched_at: row.try_get("touched_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for Extent {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        Ok(Self {
            stream_id: row.try_get("stream_id")?,
            offset_bytes: row.try_get("offset_bytes")?,
            length: row.try_get("length")?,
            chunk_id: row.try_get("chunk_id")?,
            fragment_id: row.try_get("fragment_id")?,
            source_offset: row.try_get("source_offset")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for Chunk {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        crate::model::physical::read_chunk(
            Self {
                id: row.try_get("id")?,
                uuid: row.try_get("uuid")?,
                encoding_id: row.try_get("current_encoding_id")?,
                address: None,
                location_id: row.try_get("location_id")?,
                storage_id: row.try_get("storage_id")?,
                hash: row.try_get("hash")?,
                raw_size: row.try_get("raw_size")?,
                stored_size: row.try_get("standalone_size")?,
                algorithm: row.try_get("encryption_algorithm")?,
                block_key_id: row.try_get("block_key_id")?,
                compressed: row.try_get("standalone_compressed")?,
                nonce: row.try_get("nonce")?,
                format: row.try_get("format")?,
                state: row.try_get("state")?,
                created_at: row.try_get("created_at")?,
            },
            row.try_get("tier_material"),
        )
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for ReadExtent {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        Ok(Self {
            demand_pack: sqlx::Row::try_get(row, "demand_pack")?,
            extent: sqlx::FromRow::from_row(row)?,
            chunk: sqlx::FromRow::from_row(row)?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for FragmentSource {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        Ok(Self {
            location_id: row.try_get("fragment_location")?,
            size: row.try_get("size")?,
            hash: row.try_get("hash")?,
            file_id: row.try_get("file_id")?,
            file_offset: row.try_get("file_offset")?,
            file_storage: row.try_get("file_storage")?,
            raw_source: row.try_get("raw_source")?,
            staging_storage_id: row.try_get("staging_storage_id")?,
            staging_object_key: row.try_get("staging_object_key")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for Pack {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        crate::model::physical::read_pack(
            Self {
                id: row.try_get("id")?,
                address: None,
                location_id: row.try_get("location_id")?,
                storage_id: row.try_get("storage_id")?,
                algorithm: row.try_get("encryption_algorithm")?,
                block_key_id: row.try_get("block_key_id")?,
                raw_size: row.try_get("raw_size")?,
                stored_size: row.try_get("stored_size")?,
                compressed: row.try_get("pack_compressed")?,
                nonce: row.try_get("nonce")?,
                digest: row.try_get("digest")?,
                member_count: row.try_get("member_count")?,
                state: row.try_get("state")?,
                created_at: row.try_get("created_at")?,
            },
            row.try_get("tier_material"),
        )
    }
}

pub(in super::super) async fn physical_source(
    db: &mut sqlx::MySqlConnection,
    id: Id,
    inspection: bool,
) -> Result<PhysicalSource> {
    let mut query = chunk_query(true);
    query
        .push(" WHERE c.id=")
        .push_bind(id)
        .push(" AND c.state='ready' AND l.encoding_id IS NOT NULL");
    if !inspection {
        query.push(" AND l.quarantined_at IS NULL");
    }
    if let Some(chunk) = query.build_query_as().fetch_optional(&mut *db).await? {
        return Ok(PhysicalSource::Chunk(chunk));
    }
    let pack=sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=1 AND tl.physical_id=p.location_id) AS tier_material,p.* FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE c.id=? AND c.state='ready' AND p.state='ready' AND (? OR p.quarantined_at IS NULL)").bind(id).bind(inspection).fetch_optional(&mut *db).await?.context(crate::model::Error::SourceUnavailable)?;
    Ok(PhysicalSource::Pack(pack))
}
