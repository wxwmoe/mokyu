use super::*;

#[derive(sqlx::FromRow)]
struct InspectionMember {
    #[sqlx(flatten)]
    chunk: Chunk,
    offset_bytes: i64,
}

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
        let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "SELECT c.id,c.current_encoding_id,l.encoding_id AS location_id,c.storage_id,c.hash,c.raw_size,l.standalone_size,l.encryption_algorithm,l.block_key_id,l.standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunks c JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id",
        );
        query.push(" JOIN storage_profiles sp ON sp.storage_id=c.storage_id");
        query.push(" WHERE c.state='ready' AND l.sync_pending AND l.quarantined_at IS NULL AND l.state IN ('ready','uploading') AND c.id>").push_bind(after);
        if files_only {
            query.push(" AND l.boot_id IS NULL");
            write_delay(&mut query, "l.created_at", "c.storage_id", urgent);
        }
        if let Some(before) = before {
            query.push(" AND l.created_at<=").push_bind(before);
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
        sqlx::query("UPDATE chunk_encodings SET sync_pending=CASE WHEN $2::text IS NULL THEN FALSE ELSE sync_pending END,boot_id=CASE WHEN $2::text IS NULL THEN NULL ELSE boot_id END,quarantine_code=$2,quarantined_at=CASE WHEN $2::text IS NULL THEN NULL ELSE clock_timestamp() END,quarantine_revision=quarantine_revision+CASE WHEN $2::text IS NULL THEN 0 ELSE 1 END WHERE encoding_id=$1 AND sync_pending AND quarantined_at IS NULL AND state IN ('ready','uploading')").bind(chunk.encoding_id).bind(failure).execute(&self.pool).await?;
        Ok(())
    }
    async fn buffered_status(&self) -> Result<Value> {
        let (files, bytes, oldest, volatile): (i64,i64,Option<String>,i64) = sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(c.raw_size),0) AS BIGINT),MIN(l.created_at)::text,CAST(COALESCE(SUM(CASE WHEN l.boot_id IS NOT NULL THEN 1 ELSE 0 END),0) AS BIGINT) FROM chunk_encodings l JOIN chunks c ON c.current_encoding_id=l.encoding_id WHERE c.state='ready' AND l.sync_pending AND l.state IN ('ready','uploading') AND l.quarantined_at IS NULL").fetch_one(&self.pool).await?;
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
        Ok(sqlx::query_as("SELECT storage_id,MIN(created_at) AS oldest FROM (SELECT c.storage_id,p.created_at FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id UNION ALL SELECT r.storage_id,r.created_at FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE r.pending_upload AND r.state='ready' AND r.quarantine_code IS NULL AND EXISTS(SELECT 1 FROM raw_ranges e WHERE e.source_id=r.id) UNION ALL SELECT r.storage_id,r.created_at FROM raw_sources r WHERE r.durability IN ('buffered','volatile') AND r.state='ready' AND r.quarantine_code IS NULL AND EXISTS(SELECT 1 FROM raw_ranges e WHERE e.source_id=r.id) UNION ALL SELECT l.storage_id,l.created_at FROM chunk_encodings l WHERE l.sync_pending AND l.quarantined_at IS NULL AND l.state IN ('ready','uploading') AND EXISTS(SELECT 1 FROM chunks c JOIN extents e ON e.chunk_id=c.id WHERE c.id=l.chunk_id AND c.current_encoding_id=l.encoding_id)) waiting GROUP BY storage_id").fetch_all(&self.pool).await?)
    }
    async fn has_chunks(&self) -> Result<bool> {
        Ok(chunks_indexed().fetch_one(&self.pool).await?)
    }
    async fn indexed(&self, location: Uuid) -> Result<bool> {
        Ok(physical_indexed(location).fetch_one(&self.pool).await?)
    }

    async fn upload_pins(
        &self,
        after: Id,
        owner: Option<Uuid>,
    ) -> Result<Vec<crate::model::physical::StagedUpload>> {
        Ok(sqlx::query_as("SELECT u.chunk_id,u.staging_storage_id,u.staging_object_key,u.staged_size,c.storage_id FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE u.chunk_id>$1 AND u.node_id IS NOT DISTINCT FROM $2 ORDER BY u.chunk_id LIMIT 512").bind(after).bind(owner).fetch_all(&self.pool).await?)
    }
    async fn pending_source(&self, chunk: Id) -> Result<Option<(Uuid, String)>> {
        Ok(sqlx::query_as("SELECT staging_storage_id,staging_object_key FROM pending_uploads WHERE chunk_id=$1 AND staging_storage_id IS NOT NULL").bind(chunk).fetch_optional(&self.pool).await?)
    }
    async fn pending_only(&self, chunk: Id) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE chunk_id=$1 AND c.pack_id IS NULL AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=$1 AND state='ready'))").bind(chunk).fetch_one(&self.pool).await?)
    }
    async fn register_encoding(&self, chunk: &Chunk) -> Result<()> {
        register_encoding(chunk, None).execute(&self.pool).await?;
        Ok(())
    }
    async fn pack_chunks(&self, pack: Id) -> Result<Vec<Chunk>> {
        Ok(sqlx::query_as("SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at FROM chunks c LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE c.id IN (SELECT chunk_id FROM pack_members WHERE pack_id=$1) AND c.state='ready'").bind(pack).fetch_all(&self.pool).await?)
    }
    async fn pack_contains(&self, pack: Id, chunk: Id) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pack_members WHERE pack_id=$1 AND chunk_id=$2)",
        )
        .bind(pack)
        .bind(chunk)
        .fetch_one(&self.pool)
        .await?)
    }
    async fn inspection(&self, task: Uuid, pack: Id) -> Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar(
            "SELECT error_code FROM integrity_packs WHERE task_id=$1 AND pack_id=$2",
        )
        .bind(task)
        .bind(pack)
        .fetch_optional(&self.pool)
        .await?)
    }
    async fn pack_index(
        &self,
        pack: Id,
        chunk: Option<Id>,
    ) -> Result<Vec<(Id, crate::content::pack::frame::Entry)>> {
        let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "SELECT m.chunk_id,c.id AS uuid,c.hash,c.raw_size,m.ordinal,m.offset_bytes,m.encoding_uuid,m.encoded_size,m.compressed,m.encryption_algorithm,m.block_key_id,m.nonce FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=",
        );
        query.push_bind(pack);
        if let Some(chunk) = chunk {
            query.push(" AND m.chunk_id=").push_bind(chunk);
        }
        query
            .push(" ORDER BY m.ordinal LIMIT 4097")
            .build_query_as::<crate::model::physical::PackMemberRow>()
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(crate::model::physical::PackMemberRow::entry)
            .collect()
    }
    async fn inspection_members(&self, pack: Id) -> Result<Vec<(Chunk, i64)>> {
        Ok(sqlx::query_as::<_, InspectionMember>("SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at,m.offset_bytes FROM pack_members m JOIN chunks c ON c.id=m.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE m.pack_id=$1 ORDER BY m.ordinal LIMIT 4097").bind(pack).fetch_all(&self.pool).await?.into_iter().map(|m| (m.chunk,m.offset_bytes)).collect())
    }
    async fn record_inspection(&self, task: Uuid, pack: Id, code: Option<&str>) -> Result<()> {
        sqlx::query("INSERT INTO integrity_packs(task_id,pack_id,error_code) VALUES($1,$2,$3) ON CONFLICT DO NOTHING").bind(task).bind(pack).bind(code).execute(&self.pool).await?;
        Ok(())
    }
    async fn source(&self, chunk: Id, inspection: bool) -> Result<PhysicalSource> {
        physical_source(&mut *self.pool.acquire().await?, chunk, inspection).await
    }
}

pub(in super::super) fn lock_staging(key: String) -> Statement<'static> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))").bind(key)
}

pub(in super::super) fn staged_size<'q>(storage: Uuid, key: &'q str) -> Scalar<'q, i64> {
    sqlx::query_scalar(
        "SELECT bytes FROM staging_objects WHERE storage_id=$1 AND object_key=$2 FOR UPDATE",
    )
    .bind(storage)
    .bind(key)
}

pub(in super::super) fn staged_usage(storage: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar(
        "SELECT COALESCE(sum(bytes),0)::bigint FROM staging_objects WHERE storage_id=$1",
    )
    .bind(storage)
}

pub(in super::super) fn insert_staged_object<'q>(
    storage: Uuid,
    key: &'q str,
    bytes: i64,
) -> Statement<'q> {
    sqlx::query("INSERT INTO staging_objects(storage_id,object_key,bytes) VALUES($1,$2,$3)")
        .bind(storage)
        .bind(key)
        .bind(bytes)
}
