use super::*;

pub(in super::super) fn claim_chunk_hash(storage: Uuid, hash: &[u8]) -> Statement<'_> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::uuid::text || ':' || encode($2,'hex'), 734922709851001))")
        .bind(storage)
        .bind(hash)
}

pub(in super::super) fn try_claim_chunk_hash(storage: Uuid, hash: &[u8]) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1::uuid::text || ':' || encode($2,'hex'), 734922709851001))")
        .bind(storage).bind(hash)
}

pub(in super::super) fn fail_chunk_allocations(input: &ChunkCreate, single: bool) -> Statement<'_> {
    sqlx::query("UPDATE chunks SET state='failed',unreferenced_at=now(),owner_stream=NULL WHERE storage_id=$1 AND hash=$2 AND raw_size=$3 AND state IN ('preparing','uploading') AND NOT EXISTS (SELECT 1 FROM streams s LEFT JOIN tasks t ON t.id=s.owner_task_id WHERE s.id=chunks.owner_stream AND s.state='writing' AND ((s.owner_task_id IS NULL AND NOT $4) OR (t.state='running' AND t.lease_token=s.owner_lease_token AND (t.lease_expires_at IS NULL OR t.lease_expires_at>clock_timestamp()))) AND ($4 OR EXISTS(SELECT 1 FROM stream_leases l WHERE l.stream_id=s.id AND l.lease_type='write' AND l.expires_at>clock_timestamp()))) AND NOT EXISTS(SELECT 1 FROM transfer_sources t WHERE t.candidate=chunks.owner_stream)")
        .bind(input.storage)
        .bind(input.hash.as_slice())
        .bind(input.raw_size)
        .bind(single)
}

pub(in super::super) fn reusable_chunk(input: &ChunkCreate) -> Rows<'_, Chunk> {
    sqlx::query_as("SELECT * FROM chunks WHERE storage_id=$1 AND hash=$2 AND raw_size=$3 AND state='ready' ORDER BY id LIMIT 1")
        .bind(input.storage)
        .bind(input.hash.as_slice())
        .bind(input.raw_size)
}

pub(in super::super) fn insert_chunk(input: &ChunkCreate) -> Rows<'_, Chunk> {
    sqlx::query_as("INSERT INTO chunks(storage_id,hash,raw_size,encryption_algorithm,block_key_id,owner_stream,state,unreferenced_at) VALUES($1,$2,$3,$4,$5,$6,'preparing',now()) ON CONFLICT (storage_id,hash,raw_size) WHERE state IN ('preparing','uploading','ready') DO NOTHING RETURNING *,current_encoding_id AS location_id")
        .bind(input.storage)
        .bind(input.hash.as_slice())
        .bind(input.raw_size)
        .bind(&input.algorithm)
        .bind(input.key)
        .bind(input.stream)
}

pub(in super::super) fn can_stage_chunk_reuse(id: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT pack_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=$1) FROM chunks WHERE id=$1")
        .bind(id)
}

pub(in super::super) fn mark_chunk_uploading(stream: Id, chunk: &Chunk) -> Statement<'_> {
    sqlx::query("UPDATE chunks SET standalone_size=$2,standalone_compressed=$3,nonce=$4,state='uploading' WHERE id=$1 AND state='preparing' AND owner_stream=$5 AND current_encoding_id=$6 AND current_encoding_id=$7 AND storage_id=$8")
        .bind(chunk.id)
        .bind(chunk.stored_size)
        .bind(chunk.compressed)
        .bind(&chunk.nonce)
        .bind(stream)
        .bind(chunk.encoding_id)
        .bind(chunk.location_id)
        .bind(chunk.storage_id)
}

pub(in super::super) fn owned_chunk(stream: Id, chunk: &Chunk) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT owner_stream=$2 AND current_encoding_id=$3 AND current_encoding_id=$4 AND storage_id=$5 FROM chunks WHERE id=$1")
        .bind(chunk.id).bind(stream).bind(chunk.encoding_id).bind(chunk.location_id).bind(chunk.storage_id)
}

pub(in super::super) fn register_encoding(chunk: &Chunk, owner: Option<Id>) -> Statement<'_> {
    sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,storage_id,standalone_size,standalone_compressed,nonce,nonce_id,state,created_at,unreferenced_at,encryption_algorithm,block_key_id,owner_stream,recovery_pending) VALUES($1,$2,$3,$4,$5,$6,$10,'uploading',$7,now(),$8,$9,$11,$11::uuid IS NOT NULL)").bind(chunk.encoding_id).bind(chunk.id).bind(chunk.storage_id).bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(chunk.created_at).bind(&chunk.algorithm).bind(chunk.block_key_id).bind(crate::content::nonce::id(chunk.nonce.as_deref())).bind(owner)
}

pub(in super::super) fn lock_chunk(id: Id) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT state FROM chunks WHERE id=$1 FOR NO KEY UPDATE").bind(id)
}

pub(in super::super) fn insert_chunk_extent(
    stream: Id,
    offset: i64,
    length: i32,
    chunk: Id,
) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES($1,$2,$3,$4)")
        .bind(stream)
        .bind(offset)
        .bind(length)
        .bind(chunk)
}

pub(in super::super) fn reference_chunk(id: Id) -> Statement<'static> {
    sqlx::query(
        "UPDATE chunks SET unreferenced_at=NULL WHERE id=$1 AND unreferenced_at IS NOT NULL",
    )
    .bind(id)
}

pub(in super::super) fn pin_chunk(id: Id, owner: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO cache_pins(chunk_id,pin_type,owner_id) SELECT chunk_id,CASE WHEN source_pack_id IS NULL THEN 'upload' ELSE 'pack' END,$2 FROM pending_uploads WHERE chunk_id=$1 ON CONFLICT DO NOTHING")
        .bind(id)
        .bind(owner)
}

pub(in super::super) fn attach_pending_upload(
    chunk: Id,
    stream: Id,
    offset: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE pending_uploads SET stream_id=$2,offset_bytes=$3 WHERE chunk_id=$1 AND owner_task IS NULL AND source_pack_id IS NULL AND EXISTS(SELECT 1 FROM streams WHERE id=$2 AND stream_type='object' AND state='writing')")
        .bind(chunk)
        .bind(stream)
        .bind(offset)
}

pub(in super::super) fn extend_writing_stream(id: Id, size: i64) -> Statement<'static> {
    sqlx::query("UPDATE streams SET size=GREATEST(size,$2),touched_at=now() WHERE id=$1 AND state='writing'")
        .bind(id)
        .bind(size)
}

pub(in super::super) fn complete_chunk(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET state='ready',unreferenced_at=NULL,owner_stream=NULL WHERE id=$1 AND state='uploading'")
        .bind(id)
}

pub(in super::super) fn complete_encoding(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET sync_pending=CASE WHEN boot_id IS NOT NULL THEN FALSE ELSE sync_pending END,boot_id=NULL,state='ready',owner_stream=NULL,recovery_pending=FALSE,unreferenced_at=NULL,stored_at=clock_timestamp() WHERE encoding_id=$1 AND state='uploading'")
        .bind(id)
}

pub(in super::super) fn resize_writing_stream(id: Id, size: i64) -> Statement<'static> {
    sqlx::query("UPDATE streams SET size=$2,touched_at=now() WHERE id=$1 AND state='writing'")
        .bind(id)
        .bind(size)
}

pub(in super::super) fn lock_ready_chunks<'q>(ids: &'q [Id]) -> Scalar<'q, Id> {
    sqlx::query_scalar(
        "SELECT id FROM chunks WHERE id=ANY($1) AND state='ready' ORDER BY id FOR NO KEY UPDATE",
    )
    .bind(ids)
}

pub(in super::super) fn copy_extents(
    destination: Id,
    source: Id,
    offset: i64,
    end: i64,
) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) SELECT $1,offset_bytes,length,chunk_id,source_offset FROM extents WHERE stream_id=$2 AND offset_bytes>=$3 AND offset_bytes<$4")
        .bind(destination)
        .bind(source)
        .bind(offset)
        .bind(end)
}

pub(in super::super) fn reference_chunks<'q>(ids: &'q [Id]) -> Statement<'q> {
    sqlx::query(
        "UPDATE chunks SET unreferenced_at=NULL WHERE id=ANY($1) AND unreferenced_at IS NOT NULL",
    )
    .bind(ids)
}

pub(in super::super) fn pin_chunks<'q>(ids: &'q [Id], owner: Id) -> Statement<'q> {
    sqlx::query("INSERT INTO cache_pins(chunk_id,pin_type,owner_id) SELECT chunk_id,CASE WHEN source_pack_id IS NULL THEN 'upload' ELSE 'pack' END,$2 FROM pending_uploads WHERE chunk_id=ANY($1) ON CONFLICT DO NOTHING")
        .bind(ids)
        .bind(owner)
}

pub(in super::super) fn enabled_storages() -> Rows<'static, StorageRecord> {
    sqlx::query_as(
        "SELECT s.id,s.type AS store_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.access_key_encrypted AS access,s.secret_key_encrypted AS secret,s.identity_marker,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.enabled ORDER BY s.created_at,s.id",
    )
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
        Ok(storage_for_stream(stream).fetch_one(&self.pool).await?)
    }

    async fn can_stage_reuse(&self, id: Id) -> Result<bool> {
        Ok(can_stage_chunk_reuse(id).fetch_one(&self.pool).await?)
    }

    async fn start_upload(&self, stream: Id, chunk: &Chunk) -> Result<()> {
        transactions::start_chunk_upload(&self.pool, stream, chunk, self.single).await
    }

    async fn allocate(&self, input: ChunkCreate) -> Result<(Chunk, bool)> {
        transactions::allocate_chunk(&self.pool, input, self.single).await
    }

    async fn reference(&self, stream: Id, offset: i64, chunk_id: Id, raw_size: i32) -> Result<()> {
        transactions::reference_chunk(&self.pool, stream, offset, chunk_id, raw_size, self.single)
            .await
    }

    async fn complete(&self, stream: Id, offset: i64, chunk: &Chunk) -> Result<()> {
        transactions::complete_chunk_write(&self.pool, stream, offset, chunk, self.single).await
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

    async fn extents_for_copy(&self, stream: Id, from: i64) -> Result<Vec<Extent>> {
        Ok(extents_for_copy(stream, from).fetch_all(&self.pool).await?)
    }

    async fn ready_chunk(&self, id: Id) -> Result<Option<Chunk>> {
        Ok(ready_chunk(id).fetch_optional(&self.pool).await?)
    }

    async fn fragment(&self, id: Id) -> Result<FragmentSource> {
        Ok(fragment_source(id).fetch_one(&self.pool).await?)
    }

    async fn extents(&self, stream: Id, from: i64, until: i64) -> Result<Vec<Extent>> {
        Ok(extents(stream, from, until).fetch_all(&self.pool).await?)
    }

    async fn read_extents(&self, stream: Id, from: i64, until: i64) -> Result<Vec<ReadExtent>> {
        Ok(read_extents(stream, from, until)
            .fetch_all(&self.pool)
            .await?)
    }

    async fn read_start(&self, stream: Id, position: i64) -> Result<Option<i64>> {
        Ok(read_start(stream, position)
            .fetch_optional(&self.pool)
            .await?)
    }
}
