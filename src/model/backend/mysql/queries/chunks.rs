use super::*;

pub(in super::super) fn lock_chunk(id: Id) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT state FROM chunks WHERE id=? FOR UPDATE").bind(id)
}

pub(in super::super) fn insert_chunk_extent(
    stream: Id,
    offset: i64,
    length: i32,
    chunk: Id,
) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES(?,?,?,?)")
        .bind(stream)
        .bind(offset)
        .bind(length)
        .bind(chunk)
}

pub(in super::super) fn pin_chunk(id: Id, owner: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO cache_pins(chunk_id,pin_type,owner_id) SELECT chunk_id,IF(source_pack_id IS NULL,'upload','pack'),? FROM pending_uploads WHERE chunk_id=? ON DUPLICATE KEY UPDATE chunk_id=cache_pins.chunk_id").bind(owner).bind(id)
}

pub(in super::super) fn attach_pending_upload(
    chunk: Id,
    stream: Id,
    offset: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE pending_uploads SET stream_id=?,offset_bytes=? WHERE chunk_id=? AND owner_task IS NULL AND source_pack_id IS NULL AND EXISTS(SELECT 1 FROM streams WHERE id=? AND stream_type='object' AND state='writing')").bind(stream).bind(offset).bind(chunk).bind(stream)
}

pub(in super::super) fn complete_chunk(chunk: &Chunk) -> Statement<'_> {
    sqlx::query("UPDATE chunks SET state='ready',unreferenced_at=NULL,owner_stream=NULL,reference_changed_at=CURRENT_TIMESTAMP(6) WHERE id=? AND current_encoding_id=? AND state='uploading'").bind(chunk.id).bind(chunk.encoding_id)
}

pub(in super::super) fn complete_encoding(encoding: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET sync_pending=CASE WHEN boot_id IS NOT NULL THEN FALSE ELSE sync_pending END,boot_id=NULL,state='ready',unreferenced_at=NULL,stored_at=CURRENT_TIMESTAMP(6) WHERE encoding_id=? AND state='uploading' AND standalone_size IS NOT NULL").bind(encoding)
}

pub(in super::super) fn encoding_key(id: Uuid, algorithm: &str) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("SELECT id FROM managed_keys WHERE id=? AND key_algorithm=? AND purpose='block' AND NOT recovery_read_only AND revoked_at IS NULL LOCK IN SHARE MODE").bind(id).bind(algorithm)
}

pub(in super::super) fn copy_extents(
    destination: Id,
    source: Id,
    offset: i64,
    end: i64,
) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) SELECT ?,offset_bytes,length,chunk_id,source_offset FROM extents WHERE stream_id=? AND offset_bytes>=? AND offset_bytes<?").bind(destination).bind(source).bind(offset).bind(end)
}

pub(in super::super) fn chunk_ids(query: &mut sqlx::QueryBuilder<MySql>, ids: &[Id]) {
    query.push("(");
    if ids.is_empty() {
        query.push("NULL");
    } else {
        let mut values = query.separated(",");
        for id in ids {
            values.push_bind(*id);
        }
    }
    query.push(")");
}

pub(in super::super) async fn lock_ready_chunks(
    connection: &mut sqlx::MySqlConnection,
    ids: &[Id],
) -> Result<Vec<i64>> {
    let mut query = sqlx::QueryBuilder::new("SELECT id FROM chunks WHERE state='ready' AND id IN ");
    chunk_ids(&mut query, ids);
    query.push(" ORDER BY id FOR UPDATE");
    Ok(query.build_query_scalar().fetch_all(connection).await?)
}

pub(in super::super) async fn reference_chunks(
    connection: &mut sqlx::MySqlConnection,
    ids: &[Id],
    owner: Id,
) -> Result<()> {
    changed_references(connection, ids, true).await?;
    let mut query = sqlx::QueryBuilder::new(
        "INSERT INTO cache_pins(chunk_id,pin_type,owner_id) SELECT chunk_id,IF(source_pack_id IS NULL,'upload','pack'),",
    );
    query
        .push_bind(owner)
        .push(" FROM pending_uploads WHERE chunk_id IN ");
    chunk_ids(&mut query, ids);
    query.push(" ON DUPLICATE KEY UPDATE chunk_id=cache_pins.chunk_id");
    query.build().execute(connection).await?;
    Ok(())
}

#[async_trait::async_trait]
impl ChunkStore for Backend {
    async fn prepare_repair(
        &self,
        input: ChunkCreate,
        node: Option<crate::model::recovery::RepairNode>,
    ) -> Result<crate::model::recovery::RepairAllocation> {
        transactions::prepare_repair(&self.pool, input, node).await
    }
    async fn repair_metadata(&self, repair: &crate::model::recovery::Repair) -> Result<()> {
        transactions::repair_metadata(&self.pool, repair).await
    }
    async fn finish_repair(
        &self,
        repair: &crate::model::recovery::Repair,
        offset: i64,
    ) -> Result<Chunk> {
        transactions::finish_repair(&self.pool, repair, offset).await
    }

    async fn storage_for_stream(&self, stream: Id) -> Result<Option<Uuid>> {
        Ok(sqlx::query_scalar(
            "SELECT b.storage_id FROM streams s JOIN buckets b ON b.id=s.bucket_id WHERE s.id=?",
        )
        .bind(stream)
        .fetch_one(&self.pool)
        .await?)
    }
    async fn extents_for_copy(&self, stream: Id, from: i64) -> Result<Vec<Extent>> {
        Ok(sqlx::query_as("SELECT * FROM extents WHERE stream_id=? AND offset_bytes>=? ORDER BY offset_bytes LIMIT 500").bind(stream).bind(from).fetch_all(&self.pool).await?)
    }
    async fn ready_chunk(&self, id: Id) -> Result<Option<Chunk>> {
        let mut query = chunk_query(true);
        query
            .push(" WHERE c.id=")
            .push_bind(id)
            .push(" AND c.state='ready'");
        Ok(query.build_query_as().fetch_optional(&self.pool).await?)
    }
    async fn fragment(&self, id: Id) -> Result<FragmentSource> {
        Ok(
            sqlx::query_as("SELECT f.location_id AS fragment_location,f.size,f.hash,f.staging_storage_id,f.staging_object_key,f.file_id,f.file_offset,m.storage_id AS file_storage,CASE WHEN m.source_id IS NOT NULL THEN m.id END AS raw_source FROM fragments f LEFT JOIN multipart_files m ON m.id=f.file_id WHERE f.id=?")
                .bind(id)
                .fetch_one(&self.pool)
                .await?,
        )
    }
    async fn extents(&self, stream: Id, from: i64, until: i64) -> Result<Vec<Extent>> {
        Ok(sqlx::query_as("SELECT * FROM extents WHERE stream_id=? AND offset_bytes>=? AND offset_bytes<? ORDER BY offset_bytes LIMIT 64").bind(stream).bind(from).bind(until).fetch_all(&self.pool).await?)
    }
    async fn read_extents(&self, stream: Id, from: i64, until: i64) -> Result<Vec<ReadExtent>> {
        let mut query = sqlx::QueryBuilder::new("SELECT e.*,");
        query.push(CHUNK_COLUMNS).push(",(SELECT p.location_id FROM packs p WHERE p.id=c.pack_id) AS demand_pack FROM (SELECT * FROM extents WHERE stream_id=").push_bind(stream).push(" AND offset_bytes>=").push_bind(from).push(" AND offset_bytes<").push_bind(until).push(" ORDER BY offset_bytes LIMIT 64) e LEFT JOIN chunks c ON c.id=e.chunk_id AND c.state='ready' LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' ORDER BY e.offset_bytes");
        Ok(query.build_query_as().fetch_all(&self.pool).await?)
    }
    async fn read_start(&self, stream: Id, position: i64) -> Result<Option<i64>> {
        Ok(sqlx::query_scalar("SELECT offset_bytes FROM extents WHERE stream_id=? AND offset_bytes<=? ORDER BY offset_bytes DESC LIMIT 1").bind(stream).bind(position).fetch_optional(&self.pool).await?)
    }
    async fn allocate(&self, input: ChunkCreate) -> Result<(Chunk, bool)> {
        transactions::allocate_chunk(&self.pool, input).await
    }
    async fn can_stage_reuse(&self, id: Id) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT pack_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=?) FROM chunks WHERE id=?").bind(id).bind(id).fetch_one(&self.pool).await?)
    }
    async fn start_upload(&self, stream: Id, chunk: &Chunk) -> Result<()> {
        transactions::start_chunk_upload(&self.pool, stream, chunk).await
    }
    async fn reference(&self, stream: Id, offset: i64, chunk: Id, size: i32) -> Result<()> {
        transactions::reference_chunk(&self.pool, stream, offset, chunk, size).await
    }
    async fn complete(&self, stream: Id, offset: i64, chunk: &Chunk) -> Result<()> {
        transactions::complete_chunk_write(&self.pool, stream, offset, chunk).await
    }
    async fn copy_references(
        &self,
        destination: Id,
        source: Id,
        offset: i64,
        end: i64,
        chunks: &[Id],
    ) -> Result<()> {
        transactions::copy_chunk_references(&self.pool, destination, source, offset, end, chunks)
            .await
    }
}

pub(in super::super) fn register_encoding(chunk: &Chunk) -> Statement<'_> {
    sqlx::query("UPDATE chunk_encodings SET location_id=?,standalone_size=?,standalone_compressed=?,nonce=?,nonce_id=? WHERE encoding_id=? AND chunk_id=? AND storage_id=? AND encryption_algorithm=? AND block_key_id<=>? AND state='uploading'").bind(chunk.location_id).bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(crate::content::nonce::id(chunk.nonce.as_deref())).bind(chunk.encoding_id).bind(chunk.id).bind(chunk.storage_id).bind(&chunk.algorithm).bind(chunk.block_key_id)
}

pub(in super::super) fn mark_chunk_uploading(stream: Id, chunk: &Chunk) -> Statement<'_> {
    sqlx::query("UPDATE chunks SET standalone_size=?,standalone_compressed=?,nonce=?,state='uploading' WHERE id=? AND current_encoding_id=? AND owner_stream=? AND state='preparing'").bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(chunk.id).bind(chunk.encoding_id).bind(stream)
}
