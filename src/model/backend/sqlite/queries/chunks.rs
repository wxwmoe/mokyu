use super::*;

// Include the exact partial-index predicate so SQLite can use the dedup index.
pub(in super::super) fn fail_chunk_allocations(input: &ChunkCreate) -> Statement<'_> {
    sqlx::query("UPDATE chunks SET state='failed',unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),owner_stream=NULL WHERE storage_id=? AND hash=? AND raw_size=? AND state IN ('preparing','uploading','ready') AND state<>'ready' AND NOT EXISTS (SELECT 1 FROM streams s JOIN tasks t ON t.id=s.owner_task_id WHERE s.id=chunks.owner_stream AND s.state='writing' AND t.state='running' AND t.execution_token=s.owner_execution_token) AND NOT EXISTS(SELECT 1 FROM transfer_sources t WHERE t.candidate=chunks.owner_stream)").bind(input.storage).bind(input.hash.as_slice()).bind(input.raw_size)
}

pub(in super::super) fn retire_failed_encodings(input: &ChunkCreate) -> Statement<'_> {
    sqlx::query("UPDATE chunk_encodings AS e SET state='retired',unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE state='uploading' AND EXISTS(SELECT 1 FROM chunks c WHERE c.id=e.chunk_id AND c.storage_id=? AND c.hash=? AND c.raw_size=? AND c.state='failed')").bind(input.storage).bind(input.hash.as_slice()).bind(input.raw_size)
}

pub(in super::super) async fn reusable_chunk(
    connection: &mut sqlx::SqliteConnection,
    input: &ChunkCreate,
) -> Result<Option<Chunk>> {
    let mut query = chunk_query(true);
    query
        .push(" WHERE c.storage_id=")
        .push_bind(input.storage)
        .push(" AND c.hash=")
        .push_bind(input.hash.as_slice())
        .push(" AND c.raw_size=")
        .push_bind(input.raw_size)
        .push(" AND c.state IN ('preparing','uploading','ready') AND c.state='ready'");
    Ok(query.build_query_as().fetch_optional(connection).await?)
}

pub(in super::super) fn insert_chunk(input: &ChunkCreate) -> Statement<'_> {
    sqlx::query("INSERT INTO chunks(uuid,storage_id,hash,raw_size,encryption_algorithm,block_key_id,owner_stream,state,unreferenced_at) VALUES(?,?,?,?,?,?,?,'preparing',CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(Uuid::now_v7()).bind(input.storage).bind(input.hash.as_slice()).bind(input.raw_size).bind(&input.algorithm).bind(input.key).bind(input.stream)
}

pub(in super::super) fn allocate_encoding(chunk: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,state,created_at,unreferenced_at,encryption_algorithm,block_key_id,encoding_fingerprint) SELECT id,?,storage_id,'uploading',created_at,CAST(unixepoch('subsec')*1000000 AS INTEGER),encryption_algorithm,block_key_id,'[]' FROM chunks WHERE id=?").bind(Uuid::now_v7()).bind(chunk)
}

pub(in super::super) fn set_chunk_encoding(chunk: Id, encoding: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET current_encoding_id=? WHERE id=? AND state='preparing'")
        .bind(encoding)
        .bind(chunk)
}

pub(in super::super) async fn allocated_chunk(
    connection: &mut sqlx::SqliteConnection,
    id: Id,
) -> Result<Chunk> {
    let mut query = chunk_query(false);
    query.push(" WHERE c.id=").push_bind(id);
    Ok(query.build_query_as().fetch_one(connection).await?)
}

pub(in super::super) async fn refresh_encoding_fingerprint(
    connection: &mut sqlx::SqliteConnection,
    encoding: Id,
) -> Result<()> {
    let mut query =
        sqlx::QueryBuilder::new("UPDATE chunk_encodings AS e SET encoding_fingerprint=");
    query
        .push(ENCODING_FINGERPRINT)
        .push(" FROM storage_profiles p WHERE p.storage_id=e.storage_id AND e.encoding_id=")
        .push_bind(encoding);
    query.build().execute(connection).await?;
    Ok(())
}

pub(in super::super) fn lock_chunk(id: Id) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT state FROM chunks WHERE id=?").bind(id)
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
    sqlx::query("INSERT INTO cache_pins(chunk_id,pin_type,owner_id) SELECT chunk_id,IIF(source_pack_id IS NULL,'upload','pack'),? FROM pending_uploads WHERE chunk_id=? ON CONFLICT(chunk_id,pin_type,owner_id) DO NOTHING").bind(owner).bind(id)
}

pub(in super::super) fn attach_pending_upload(
    chunk: Id,
    stream: Id,
    offset: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE pending_uploads SET stream_id=?,offset_bytes=? WHERE chunk_id=? AND owner_task IS NULL AND source_pack_id IS NULL AND EXISTS(SELECT 1 FROM streams WHERE id=? AND stream_type='object' AND state='writing')").bind(stream).bind(offset).bind(chunk).bind(stream)
}

pub(in super::super) fn complete_chunk(chunk: &Chunk) -> Statement<'_> {
    sqlx::query("UPDATE chunks SET state='ready',unreferenced_at=NULL,owner_stream=NULL,reference_changed_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND current_encoding_id=? AND state='uploading'").bind(chunk.id).bind(chunk.encoding_id)
}

pub(in super::super) fn complete_encoding(encoding: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET sync_pending=CASE WHEN boot_id IS NOT NULL THEN FALSE ELSE sync_pending END,boot_id=NULL,state='ready',unreferenced_at=NULL,stored_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE encoding_id=? AND state='uploading' AND standalone_size IS NOT NULL").bind(encoding)
}

pub(in super::super) fn encoding_key(id: Uuid, algorithm: &str) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("SELECT id FROM managed_keys WHERE id=? AND key_algorithm=? AND purpose='block' AND revoked_at IS NULL").bind(id).bind(algorithm)
}

pub(in super::super) fn copy_extents(
    destination: Id,
    source: Id,
    offset: i64,
    end: i64,
) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) SELECT ?,offset_bytes,length,chunk_id,source_offset FROM extents WHERE stream_id=? AND offset_bytes>=? AND offset_bytes<?").bind(destination).bind(source).bind(offset).bind(end)
}

pub(in super::super) fn chunk_ids(query: &mut sqlx::QueryBuilder<Sqlite>, ids: &[Id]) {
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
    connection: &mut sqlx::SqliteConnection,
    ids: &[Id],
) -> Result<Vec<i64>> {
    let mut query = sqlx::QueryBuilder::new("SELECT id FROM chunks WHERE state='ready' AND id IN ");
    chunk_ids(&mut query, ids);
    query.push(" ORDER BY id");
    Ok(query.build_query_scalar().fetch_all(connection).await?)
}

pub(in super::super) async fn reference_chunks(
    connection: &mut sqlx::SqliteConnection,
    ids: &[Id],
    owner: Id,
) -> Result<()> {
    changed_references(connection, ids, true).await?;
    let mut query = sqlx::QueryBuilder::new(
        "INSERT INTO cache_pins(chunk_id,pin_type,owner_id) SELECT chunk_id,IIF(source_pack_id IS NULL,'upload','pack'),",
    );
    query
        .push_bind(owner)
        .push(" FROM pending_uploads WHERE chunk_id IN ");
    chunk_ids(&mut query, ids);
    query.push(" ON CONFLICT(chunk_id,pin_type,owner_id) DO NOTHING");
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
        transactions::prepare_repair(&self.write, input, node).await
    }
    async fn repair_metadata(&self, repair: &crate::model::recovery::Repair) -> Result<()> {
        transactions::repair_metadata(&self.write, repair).await
    }
    async fn finish_repair(
        &self,
        repair: &crate::model::recovery::Repair,
        offset: i64,
    ) -> Result<Chunk> {
        transactions::finish_repair(&self.write, repair, offset).await
    }

    async fn storage_for_stream(&self, stream: Id) -> Result<Option<Uuid>> {
        Ok(sqlx::query_scalar(
            "SELECT b.storage_id FROM streams s JOIN buckets b ON b.id=s.bucket_id WHERE s.id=?",
        )
        .bind(stream)
        .fetch_one(&self.read)
        .await?)
    }
    async fn extents_for_copy(&self, stream: Id, from: i64) -> Result<Vec<Extent>> {
        Ok(sqlx::query_as("SELECT * FROM extents WHERE stream_id=? AND offset_bytes>=? ORDER BY offset_bytes LIMIT 500").bind(stream).bind(from).fetch_all(&self.read).await?)
    }
    async fn ready_chunk(&self, id: Id) -> Result<Option<Chunk>> {
        let mut query = chunk_query(true);
        query
            .push(" WHERE c.id=")
            .push_bind(id)
            .push(" AND c.state='ready'");
        Ok(query.build_query_as().fetch_optional(&self.read).await?)
    }
    async fn fragment(&self, id: Id) -> Result<FragmentSource> {
        Ok(
            sqlx::query_as("SELECT f.location_id AS fragment_location,f.size,f.hash,f.file_id,f.file_offset,m.storage_id AS file_storage,CASE WHEN m.source_id IS NOT NULL THEN m.id END AS raw_source FROM fragments f LEFT JOIN multipart_files m ON m.id=f.file_id WHERE f.id=?")
                .bind(id)
                .fetch_one(&self.read)
                .await?,
        )
    }
    async fn extents(&self, stream: Id, from: i64, until: i64) -> Result<Vec<Extent>> {
        Ok(sqlx::query_as("SELECT * FROM extents WHERE stream_id=? AND offset_bytes>=? AND offset_bytes<? ORDER BY offset_bytes LIMIT 64").bind(stream).bind(from).bind(until).fetch_all(&self.read).await?)
    }
    async fn read_extents(&self, stream: Id, from: i64, until: i64) -> Result<Vec<ReadExtent>> {
        let mut query = sqlx::QueryBuilder::new("SELECT e.*,");
        query.push(CHUNK_COLUMNS).push(",(SELECT p.location_id FROM packs p WHERE p.id=c.pack_id) AS demand_pack FROM (SELECT * FROM extents WHERE stream_id=").push_bind(stream).push(" AND offset_bytes>=").push_bind(from).push(" AND offset_bytes<").push_bind(until).push(" ORDER BY offset_bytes LIMIT 64) e LEFT JOIN chunks c ON c.id=e.chunk_id AND c.state='ready' LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' ORDER BY e.offset_bytes");
        Ok(query.build_query_as().fetch_all(&self.read).await?)
    }
    async fn read_start(&self, stream: Id, position: i64) -> Result<Option<i64>> {
        Ok(sqlx::query_scalar("SELECT offset_bytes FROM extents WHERE stream_id=? AND offset_bytes<=? ORDER BY offset_bytes DESC LIMIT 1").bind(stream).bind(position).fetch_optional(&self.read).await?)
    }
    async fn allocate(&self, input: ChunkCreate) -> Result<(Chunk, bool)> {
        transactions::allocate_chunk(&self.write, input).await
    }
    async fn can_stage_reuse(&self, id: Id) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT pack_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=?) FROM chunks WHERE id=?").bind(id).bind(id).fetch_one(&self.read).await?)
    }
    async fn start_upload(&self, stream: Id, chunk: &Chunk) -> Result<()> {
        transactions::start_chunk_upload(&self.write, stream, chunk).await
    }
    async fn reference(&self, stream: Id, offset: i64, chunk: Id, size: i32) -> Result<()> {
        transactions::reference_chunk(&self.write, stream, offset, chunk, size).await
    }
    async fn complete(&self, stream: Id, offset: i64, chunk: &Chunk) -> Result<()> {
        transactions::complete_chunk_write(&self.write, stream, offset, chunk).await
    }
    async fn copy_references(
        &self,
        destination: Id,
        source: Id,
        offset: i64,
        end: i64,
        chunks: &[Id],
    ) -> Result<()> {
        transactions::copy_chunk_references(&self.write, destination, source, offset, end, chunks)
            .await
    }
}
