use super::*;

pub(in super::super) fn repair_part(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT write_token FROM parts WHERE write_token=? FOR UPDATE").bind(stream)
}

pub(in super::super) fn repair_target(
    storage: Uuid,
    key: Option<Uuid>,
    algorithm: &str,
) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=? AND s.enabled AND s.block_key_id <=> ? AND ((s.block_key_id IS NULL AND ?='none') OR EXISTS(SELECT 1 FROM managed_keys k WHERE k.id=s.block_key_id AND k.key_algorithm=? AND k.revoked_at IS NULL)) LOCK IN SHARE MODE").bind(storage).bind(key).bind(algorithm).bind(algorithm)
}

pub(in super::super) fn repair_pending(chunk: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=?)").bind(chunk)
}

pub(in super::super) fn insert_repair(chunk: &Chunk, stream: Id) -> Statement<'_> {
    sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,state,created_at,unreferenced_at,owner_stream,recovery_pending,encryption_algorithm,block_key_id,standalone_compressed,encoding_fingerprint) VALUES(?,?,?,'uploading',?,CURRENT_TIMESTAMP(6),?,TRUE,?,?,FALSE,UNHEX(REPEAT('00',32)))").bind(chunk.id).bind(chunk.location_id).bind(chunk.storage_id).bind(chunk.created_at).bind(stream).bind(&chunk.algorithm).bind(chunk.block_key_id)
}

pub(in super::super) fn repair_candidate(encoding: Id, stream: Id) -> Rows<'static, Chunk> {
    sqlx::query_as("SELECT c.id,c.uuid,l.encoding_id AS current_encoding_id,l.location_id,c.storage_id,c.hash,c.raw_size,l.standalone_size,l.encryption_algorithm,l.block_key_id,l.standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE l.encoding_id=? AND l.owner_stream=? AND l.owner_task IS NULL AND l.state='uploading' AND l.recovery_pending AND l.quarantined_at IS NULL FOR UPDATE").bind(encoding).bind(stream)
}

pub(in super::super) fn save_repair_metadata(chunk: &Chunk, stream: Id) -> Statement<'_> {
    sqlx::query("UPDATE chunk_encodings SET standalone_size=?,standalone_compressed=?,nonce=?,nonce_id=? WHERE encoding_id=? AND owner_stream=? AND state='uploading'").bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(crate::content::nonce::id(chunk.nonce.as_deref())).bind(chunk.encoding_id).bind(stream)
}

pub(in super::super) fn retire_repair_sources(chunk: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',sync_pending=FALSE,boot_id=NULL,unreferenced_at=CURRENT_TIMESTAMP(6) WHERE chunk_id=? AND (state='ready' OR (state='uploading' AND boot_id IS NOT NULL)) AND quarantined_at IS NOT NULL").bind(chunk)
}

pub(in super::super) fn publish_repair(encoding: Id, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET sync_pending=CASE WHEN boot_id IS NOT NULL THEN FALSE ELSE sync_pending END,boot_id=NULL,state='ready',owner_stream=NULL,recovery_pending=FALSE,unreferenced_at=NULL,stored_at=CURRENT_TIMESTAMP(6) WHERE encoding_id=? AND owner_stream=? AND state='uploading' AND quarantined_at IS NULL").bind(encoding).bind(stream)
}

pub(in super::super) fn discard_verified_repair(encoding: Id, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',owner_stream=NULL,recovery_pending=FALSE,unreferenced_at=CURRENT_TIMESTAMP(6),stored_at=CURRENT_TIMESTAMP(6) WHERE encoding_id=? AND owner_stream=? AND state='uploading'").bind(encoding).bind(stream)
}

pub(in super::super) fn publish_repaired_chunk(chunk: &Chunk) -> Statement<'_> {
    sqlx::query("UPDATE chunks SET current_encoding_id=?,pack_id=NULL,standalone_size=?,standalone_compressed=?,nonce=?,encryption_algorithm=?,block_key_id=?,unreferenced_at=NULL WHERE id=? AND state='ready'").bind(chunk.encoding_id).bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(&chunk.algorithm).bind(chunk.block_key_id).bind(chunk.id)
}

pub(in super::super) fn retire_stream_encodings(stream: Id, batch: i64) -> Statement<'static> {
    // A state-index scan can lock unrelated uploads before filtering their owner.
    sqlx::query("UPDATE chunk_encodings FORCE INDEX (encodings_owner_stream) SET owner_stream=NULL,state='retired',unreferenced_at=CURRENT_TIMESTAMP(6) WHERE owner_stream=? AND state='uploading' ORDER BY encoding_id LIMIT ?").bind(stream).bind(batch)
}

pub(in super::super) fn repair_owner(
    stream: Id,
) -> Rows<'static, (Uuid, String, Option<Uuid>, Option<Uuid>)> {
    sqlx::query_as("SELECT bucket_id,stream_type,owner_task_id,owner_execution_token FROM streams WHERE id=? AND state='writing'").bind(stream)
}

pub(in super::super) fn repair_upload(stream: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM uploads WHERE id=(SELECT r.upload_id FROM quota_writes w JOIN quota_reservations r ON r.id=w.reservation_id WHERE w.stream_id=?) AND state='active' FOR UPDATE").bind(stream)
}

pub(in super::super) fn source_audit<'a>(
    principal: &'a Principal,
    action: &'a str,
    target: &'a str,
    mut detail: Value,
) -> Result<Statement<'a>> {
    let (source, fallback, access_key) = match principal {
        Principal::Service { access_key, .. } => {
            detail["access_key_id"] = serde_json::json!(access_key);
            ("s3", "S3 credential", Some(access_key.as_str()))
        }
        Principal::User { .. } => ("web", "Deleted account", None),
        Principal::Token { .. } => ("token", "Deleted account", None),
        Principal::Local => ("cli", "Local CLI", None),
        _ => ("cli", "Internal operation", None),
    };
    if let Some(id) = crate::model::audit::current() {
        detail["request_audit_id"] = serde_json::json!(id.to_string());
    }
    ensure!(
        serde_json::to_vec(&detail)?.len() <= 262144,
        "audit detail exceeds its bound"
    );
    Ok(sqlx::query("INSERT INTO audit_events(actor_id,actor_label,token_id,source,action,target,outcome,finished_at,detail) VALUES(?,COALESCE((SELECT username FROM web_users WHERE id=?),NULLIF(substr((SELECT label FROM s3_credentials WHERE access_key_id=?),1,64),''),?),?,?,?,?, 'succeeded',CURRENT_TIMESTAMP(6),?)")
        .bind(principal.user_id()).bind(principal.user_id()).bind(access_key).bind(fallback).bind(principal.token_id()).bind(source).bind(action).bind(target).bind(detail))
}

pub(in super::super) fn raw_page(stream: Id, start: i64, end: i64) -> Rows<'static, RawRange> {
    sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=2 AND tl.physical_id=r.location_id) AS tier_material,r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,e.offset_bytes,e.length,e.source_offset FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id WHERE e.stream_id=? AND e.offset_bytes+e.length>=? AND e.offset_bytes<=? ORDER BY e.offset_bytes LIMIT 128")
            .bind(stream).bind(start).bind(end)
}

const RAW_COPY_READY: &str = "r.state='ready' AND r.quarantine_code IS NULL AND r.durability='durable' AND NOT r.pending_upload AND r.medium IN ('local','shared_s3') AND r.size>=0 AND length(r.digest)=16";

pub(in super::super) fn raw_copyable(stream: Id, storage: Uuid) -> sqlx::QueryBuilder<MySql> {
    let mut q = sqlx::QueryBuilder::new("SELECT EXISTS(SELECT 1 FROM raw_ranges WHERE stream_id=");
    q.push_bind(stream).push(") AND NOT EXISTS(SELECT 1 FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id WHERE e.stream_id=");
    q.push_bind(stream)
        .push(" AND NOT (r.storage_id=")
        .push_bind(storage)
        .push(" AND ")
        .push(RAW_COPY_READY)
        .push("))");
    q
}

pub(in super::super) fn raw_copy_sources(
    storage: Uuid,
    sources: &[Id],
) -> sqlx::QueryBuilder<MySql> {
    let mut q = sqlx::QueryBuilder::new("SELECT r.id FROM raw_sources r WHERE r.storage_id=");
    q.push_bind(storage)
        .push(" AND ")
        .push(RAW_COPY_READY)
        .push(" AND r.id IN (");
    let mut list = q.separated(",");
    for id in sources {
        list.push_bind(*id);
    }
    q.push(") ORDER BY r.id FOR UPDATE");
    q
}

pub(in super::super) fn raw_copy_target(
    stream: Id,
    storage: Uuid,
    revision: i64,
) -> Scalar<'static, Uuid> {
    // Lock the destination stream exclusively and share the configuration with other copies.
    sqlx::query_scalar("SELECT b.id FROM buckets b JOIN storages t ON t.id=b.storage_id JOIN storage_profiles p ON p.storage_id=t.id WHERE b.id=(SELECT bucket_id FROM streams WHERE id=? AND state='writing' FOR UPDATE) AND t.id=? AND t.enabled AND t.type IN ('local','s3') AND t.block_key_id IS NULL AND NOT p.compression_enabled AND NOT p.pack_enabled AND NOT p.cdc_enabled AND p.revision=? LOCK IN SHARE MODE").bind(stream).bind(storage).bind(revision)
}

pub(in super::super) fn raw_copy_position(stream: Id, offset: i64) -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT id FROM streams WHERE id=? AND size=? AND state='writing' AND payload_type='raw'",
    )
    .bind(stream)
    .bind(offset)
}

pub(in super::super) fn raw_copy_ranges(
    destination: Id,
    source: Id,
    offset: i64,
    end: i64,
    sources: &[Id],
) -> sqlx::QueryBuilder<MySql> {
    let mut q = sqlx::QueryBuilder::new(
        "INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id,source_offset) SELECT ",
    );
    q.push_bind(destination).push(",e.offset_bytes,e.length,e.source_id,e.source_offset FROM raw_ranges e WHERE e.stream_id=").push_bind(source);
    q.push(" AND e.offset_bytes>=")
        .push_bind(offset)
        .push(" AND e.offset_bytes<=")
        .push_bind(end);
    q.push(" AND (e.offset_bytes<")
        .push_bind(end)
        .push(" OR e.length=0) AND e.source_id IN (");
    let mut list = q.separated(",");
    for id in sources {
        list.push_bind(*id);
    }
    q.push(")");
    q
}

pub(in super::super) fn raw_copy_reference(sources: &[Id]) -> sqlx::QueryBuilder<MySql> {
    let mut q = sqlx::QueryBuilder::new("UPDATE raw_sources SET revision=revision+1 WHERE id IN (");
    let mut list = q.separated(",");
    for id in sources {
        list.push_bind(*id);
    }
    q.push(")");
    q
}

pub(in super::super) fn raw_target(
    stream: Id,
    storage: Uuid,
) -> Rows<'static, (String, bool, i64, bool, bool)> {
    sqlx::query_as("SELECT t.type,(t.block_key_id IS NULL AND NOT p.compression_enabled AND NOT p.pack_enabled),p.revision,p.cdc_enabled,((p.deduplication_enabled OR p.cdc_enabled) AND p.write_processing='background') FROM streams s JOIN buckets b ON b.id=s.bucket_id JOIN storages t ON t.id=b.storage_id JOIN storage_profiles p ON p.storage_id=t.id WHERE s.id=? AND t.id=? AND t.enabled AND s.state='writing' FOR UPDATE").bind(stream).bind(storage)
}

pub(in super::super) fn raw_prepare(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET payload_type='raw' WHERE id=? AND state='writing' AND payload_type='chunks' AND NOT EXISTS(SELECT 1 FROM extents WHERE stream_id=?) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE stream_id=?)").bind(stream).bind(stream).bind(stream)
}

pub(in super::super) fn raw_insert(
    id: Uuid,
    stream: Id,
    storage: Uuid,
    revision: i64,
    medium: &'static str,
    buffered: bool,
) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO raw_sources(location_id,owner_stream,storage_id,profile_revision,medium,durability) VALUES(?,?,?,?,?,?)",
    )
    .bind(id)
    .bind(stream)
    .bind(storage)
    .bind(revision).bind(medium).bind(if buffered { "buffered" } else { "durable" })
}

pub(in super::super) fn raw_finish(
    stream: Id,
    source: Id,
    size: i64,
    digest: &[u8],
    final_source: bool,
) -> Statement<'_> {
    sqlx::query("UPDATE raw_sources SET size=?,digest=?,durability=CASE WHEN ? AND medium='shared_s3' THEN 'durable' ELSE durability END,state='ready',owner_stream=NULL,upload_id=NULL WHERE id=? AND owner_stream=? AND state='writing'").bind(size).bind(digest).bind(final_source).bind(source).bind(stream)
}

pub(in super::super) fn raw_attach(stream: Id, source: Id, size: i64) -> Statement<'static> {
    sqlx::query("INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id) VALUES(?,0,?,?)")
        .bind(stream)
        .bind(size)
        .bind(source)
}

pub(in super::super) fn raw_sources_lock(stream: Id) -> Rows<'static, RawSource> {
    sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id IN (SELECT source_id FROM raw_ranges WHERE stream_id=?) ORDER BY r.id FOR UPDATE").bind(stream)
}

pub(in super::super) fn raw_continuity(stream: Id) -> Rows<'static, (i64, bool)> {
    sqlx::query_as("SELECT totals.end_bytes,totals.contiguous<>0 AND NOT EXISTS(SELECT 1 FROM extents WHERE stream_id=?) FROM (SELECT COALESCE(MAX(e.offset_bytes+e.length),0) AS end_bytes,COALESCE(MIN(e.offset_bytes=e.previous_end AND e.source_offset+e.length<=r.size),1) AS contiguous FROM (SELECT *,COALESCE(MAX(offset_bytes+length) OVER(ORDER BY offset_bytes ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING),0) AS previous_end FROM raw_ranges WHERE stream_id=?) e JOIN raw_sources r ON r.id=e.source_id) totals").bind(stream).bind(stream)
}

pub(in super::super) fn raw_cleanup_ranges(stream: Id, batch: i64) -> Rows<'static, (i64, Id)> {
    sqlx::query_as("SELECT offset_bytes,source_id FROM raw_ranges WHERE stream_id=? ORDER BY offset_bytes LIMIT ?").bind(stream).bind(batch)
}

pub(in super::super) fn raw_touch_unreferenced(source: Id) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET unreferenced_at=UTC_TIMESTAMP(6) WHERE id=?").bind(source)
}

pub(in super::super) fn raw_detach(stream: Id, end: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_ranges WHERE stream_id=? AND offset_bytes<=?")
        .bind(stream)
        .bind(end)
}

#[async_trait::async_trait]
impl RawStore for Backend {
    async fn put_pages(&self, source: Id, group: &crate::content::raw_pages::Group) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        super::super::raw_pages::put(&mut tx, source, group).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn seal_pages(
        &self,
        source: Id,
        size: i64,
        digest: &[u8],
        tail: Option<&crate::content::raw_pages::Group>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        if let Some(group) = tail {
            super::super::raw_pages::put(&mut tx, source, group).await?;
        }
        super::super::raw_pages::seal(&mut tx, source, size, digest).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn page_hashes(
        &self,
        source: Id,
        size: i64,
        first: i64,
        count: usize,
    ) -> Result<Option<Vec<u8>>> {
        let mut conn = self.pool.acquire().await?;
        super::super::raw_pages::read(&mut conn, source, size, first, count).await
    }

    async fn recover_memory(&self, boot: Uuid) -> Result<()> {
        transactions::recover_raw_memory(&self.pool, boot).await
    }
    async fn buffered_status(&self) -> Result<serde_json::Value> {
        let (pending, bytes, oldest, volatile): (i64,i64,Option<chrono::DateTime<chrono::Utc>>,i64) = sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(size),0) AS SIGNED),MIN(created_at),CAST(COALESCE(SUM(CASE WHEN medium='memory' THEN 1 ELSE 0 END),0) AS SIGNED) FROM raw_sources WHERE durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL").fetch_one(&self.pool).await?;
        Ok(serde_json::json!({"pending":pending,"bytes":bytes,"oldest":oldest,"volatile":volatile}))
    }

    async fn buffered_source(&self, source: Id) -> Result<Option<RawSource>> {
        Ok(sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id=? AND r.durability IN ('buffered','volatile') AND r.state='ready' AND r.quarantine_code IS NULL").bind(source).fetch_optional(&self.pool).await?)
    }
    async fn buffered_duplicate(
        &self,
        storage: Uuid,
        hash: &[u8],
        size: i64,
    ) -> Result<Option<RawSource>> {
        Ok(sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.storage_id=? AND r.content_hash=? AND r.size=? AND r.durability IN ('buffered','volatile') AND r.state='ready' AND r.quarantine_code IS NULL").bind(storage).bind(hash).bind(size).fetch_optional(&self.pool).await?)
    }
    async fn buffer(&self, source: Id) -> Result<()> {
        ensure!(sqlx::query("UPDATE raw_sources SET durability='buffered' WHERE id=? AND state='writing' AND owner_stream IS NOT NULL").bind(source).execute(&self.pool).await?.rows_affected()==1, crate::model::Error::OperationAborted);
        Ok(())
    }
    async fn buffered(
        &self,
        after: Id,
        before: Option<chrono::DateTime<chrono::Utc>>,
        stream: Option<Id>,
        normal: bool,
        urgent: &[Uuid],
    ) -> Result<Vec<RawSource>> {
        let mut query = sqlx::QueryBuilder::<MySql>::new(
            "SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r JOIN storage_profiles sp ON sp.storage_id=r.storage_id WHERE r.durability IN ('buffered','volatile') AND r.state='ready' AND r.quarantine_code IS NULL AND r.id>",
        );
        query.push_bind(after);
        if let Some(before) = before {
            query
                .push(" AND r.created_at<=")
                .push_bind(before.naive_utc());
        }
        if let Some(stream) = stream {
            query.push(" AND EXISTS(SELECT 1 FROM raw_ranges e WHERE e.source_id=r.id AND e.stream_id=").push_bind(stream).push(")");
        }
        if normal {
            query.push(" AND r.medium<>'memory'");
            write_delay(&mut query, "r.created_at", "r.storage_id", urgent);
        }
        query.push(" ORDER BY r.id LIMIT 128");
        Ok(query.build_query_as().fetch_all(&self.pool).await?)
    }
    async fn synced(&self, source: &RawSource, failure: Option<&str>) -> Result<()> {
        if let Some(code) = failure {
            ensure!(
                crate::model::source::quarantine_code(code),
                "invalid source quarantine code"
            );
            sqlx::query("UPDATE raw_sources SET quarantine_code=?,quarantined_at=CURRENT_TIMESTAMP(6),revision=revision+1 WHERE id=? AND revision=? AND durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL").bind(code).bind(source.id).bind(source.revision).execute(&self.pool).await?;
        } else {
            sqlx::query("UPDATE raw_sources SET durability='durable',medium=CASE WHEN medium='memory' THEN 'local' ELSE medium END,boot_id=NULL WHERE id=? AND revision=? AND durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL").bind(source.id).bind(source.revision).execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn inline_conversion(
        &self,
        stream: Id,
        source: Id,
    ) -> Result<crate::model::cache::EncodingPolicy> {
        transactions::inline_conversion(&self.pool, stream, source).await
    }
    async fn begin_conversion(
        &self,
        stream: Id,
        storage: Uuid,
        revision: i64,
        encoding: &crate::model::cache::EncodingPolicy,
        buffered: bool,
    ) -> Result<(Id, Uuid)> {
        transactions::begin_raw(
            &self.pool,
            stream,
            storage,
            revision,
            Some(encoding),
            buffered,
        )
        .await
    }
    async fn commit_conversion_batch(
        &self,
        task: Execution,
        candidate: Id,
        rows: &[crate::model::Mapping],
        outputs: &[crate::model::packing::PackOutput],
    ) -> Result<()> {
        transactions::commit_conversion_batch(&self.pool, task, candidate, rows, outputs).await
    }
    async fn finish_conversion(
        &self,
        task: Execution,
        job: &crate::model::raw::Conversion,
        source: &RawSource,
        candidate: Id,
        lease: Option<Uuid>,
    ) -> Result<bool> {
        transactions::finish_conversion(&self.pool, task, job, source, candidate, lease).await
    }
    async fn uploads_before(
        &self,
        before: chrono::DateTime<chrono::Utc>,
        _owner: Option<Uuid>,
    ) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE r.created_at<=?)").bind(before).fetch_one(&self.pool).await?)
    }
    async fn upload_status(&self) -> Result<Value> {
        let (pending,bytes,failed):(i64,i64,i64)=sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(r.size),0) AS SIGNED),CAST(COALESCE(SUM(CASE WHEN u.last_error IS NOT NULL THEN 1 ELSE 0 END),0) AS SIGNED) FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id").fetch_one(&self.pool).await?;
        Ok(serde_json::json!({"pending":pending,"bytes":bytes,"failed":failed}))
    }
    async fn abandon_upload(
        &self,
        work: &RawUpload,
        _node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::abandon_raw_upload(&self.pool, work).await
    }

    async fn cached_checkpoint(&self, location: Uuid) -> Result<Option<Id>> {
        Ok(sqlx::query_scalar("SELECT source_id FROM multipart_files m WHERE m.id=? AND source_id IS NOT NULL AND m.state<>'deleting' AND EXISTS(SELECT 1 FROM fragments WHERE file_id=m.id)").bind(location).fetch_optional(&self.pool).await?)
    }
    async fn cached_checkpoints(&self, after: Uuid) -> Result<Vec<(Uuid, Id)>> {
        Ok(sqlx::query_as("SELECT m.id,m.source_id FROM multipart_files m JOIN storages s ON s.id=m.storage_id WHERE m.id>? AND s.type='s3' AND m.source_id IS NOT NULL AND m.state<>'deleting' AND EXISTS(SELECT 1 FROM fragments WHERE file_id=m.id) ORDER BY m.id LIMIT 128").bind(after).fetch_all(&self.pool).await?)
    }
    async fn has_upload(&self, source: Id) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw_uploads WHERE source_id=?)")
                .bind(source)
                .fetch_one(&self.pool)
                .await?,
        )
    }
    async fn stage_upload(
        &self,
        stream: Id,
        source: Id,
        size: i64,
        digest: &[u8],
        revision: i64,
        deduplicate: bool,
        background: bool,
        boot: Option<Uuid>,
        _node: crate::model::raw::Processor<'_>,
    ) -> Result<()> {
        transactions::finish_raw(
            &self.pool,
            stream,
            source,
            size,
            digest,
            Some((revision, deduplicate, background, boot)),
        )
        .await
    }
    async fn uploads(&self, after: Id, _owner: Option<Uuid>) -> Result<Vec<RawUpload>> {
        Ok(raw_uploads(after).fetch_all(&self.pool).await?)
    }
    async fn claim_upload(
        &self,
        token: Uuid,
        before: Option<chrono::DateTime<chrono::Utc>>,
        _node: crate::model::raw::Processor<'_>,
        urgent: &[Uuid],
    ) -> Result<Option<RawUpload>> {
        transactions::claim_raw_upload(&self.pool, token, before, urgent).await
    }
    async fn retry_upload(&self, work: &RawUpload, error: Option<&str>) -> Result<()> {
        raw_upload_retry(work, error).execute(&self.pool).await?;
        Ok(())
    }
    async fn set_pending_upload(
        &self,
        source: Id,
        token: Uuid,
        upload: Option<&str>,
    ) -> Result<()> {
        anyhow::ensure!(
            upload.is_none_or(|s| !s.is_empty() && s.len() <= 4096),
            "invalid raw multipart upload id"
        );
        anyhow::ensure!(
            raw_upload_id(source, token, upload)
                .execute(&self.pool)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        Ok(())
    }
    async fn upload_streams(&self, source: Id) -> Result<Vec<Id>> {
        Ok(raw_upload_streams(source).fetch_all(&self.pool).await?)
    }
    async fn reuse_upload(
        &self,
        work: &RawUpload,
        hash: &[u8],
        streams: &[Id],
        _node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::reuse_raw_upload(&self.pool, work, hash, streams).await
    }
    async fn complete_upload(
        &self,
        work: &RawUpload,
        hash: Option<&[u8]>,
        _node: crate::model::raw::Processor<'_>,
    ) -> Result<()> {
        transactions::complete_raw_upload(&self.pool, work, hash).await
    }
    async fn current(&self, source: Id) -> Result<Option<RawSource>> {
        Ok(raw_source(source).fetch_optional(&self.pool).await?)
    }

    async fn reuse(
        &self,
        stream: Id,
        source: Id,
        storage: Uuid,
        size: i64,
        digest: &[u8],
        hash: &[u8],
    ) -> Result<bool> {
        transactions::reuse_raw(&self.pool, stream, source, storage, size, digest, hash).await
    }

    async fn claim_processing(
        &self,
        token: Uuid,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<Option<RawProcessing>> {
        transactions::claim_raw_processing(&self.pool, node.is_none(), token).await
    }
    async fn renew_processing(
        &self,
        work: &RawProcessing,
        _node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        Ok(sqlx::query("UPDATE raw_processing SET lease_expires_at=TIMESTAMPADD(SECOND,120,UTC_TIMESTAMP(6)) WHERE stream_id=? AND token=? AND (lease_expires_at IS NULL OR lease_expires_at>UTC_TIMESTAMP(6))")
            .bind(work.stream).bind(work.token).execute(&self.pool).await?.rows_affected() == 1)
    }
    async fn save_processing_hash(
        &self,
        work: &RawProcessing,
        revision: i64,
        hash: &[u8],
    ) -> Result<()> {
        anyhow::ensure!(hash.len() == 32, "invalid raw processing hash");
        anyhow::ensure!(sqlx::query("UPDATE raw_processing SET content_hash=?,source_revision=? WHERE stream_id=? AND token=? AND (lease_expires_at IS NULL OR lease_expires_at>UTC_TIMESTAMP(6))")
            .bind(hash).bind(revision).bind(work.stream).bind(work.token).execute(&self.pool).await?.rows_affected()==1,
            crate::model::Error::OperationAborted);
        Ok(())
    }
    async fn retry_processing(&self, work: &RawProcessing, error: Option<&str>) -> Result<()> {
        let error = error.map(|message| message.chars().take(1024).collect::<String>());
        sqlx::query("UPDATE raw_processing SET token=NULL,lease_expires_at=NULL,last_error=?,attempts=attempts+?,next_attempt_at=TIMESTAMPADD(SECOND,?,UTC_TIMESTAMP(6)) WHERE stream_id=? AND token=?")
            .bind(&error).bind(i32::from(error.is_some())).bind(if error.is_some() {30i64} else {1}).bind(work.stream).bind(work.token).execute(&self.pool).await?;
        Ok(())
    }
    async fn cancel_processing(&self, work: &RawProcessing) -> Result<()> {
        raw_processing_delete(work).execute(&self.pool).await?;
        Ok(())
    }
    async fn finish_processing(
        &self,
        work: &RawProcessing,
        source: &RawSource,
        hash: &[u8],
        lease: Option<Uuid>,
        _node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::finish_raw_processing(&self.pool, work, source, hash, lease).await
    }
    async fn processing_status(&self) -> Result<Value> {
        let (pending,failed,bytes): (i64,i64,i64)=sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(w.last_error IS NOT NULL),0) AS SIGNED),CAST(COALESCE(SUM(r.size),0) AS SIGNED) FROM raw_processing w JOIN raw_sources r ON r.id=w.source_id")
            .fetch_one(&self.pool).await?;
        Ok(serde_json::json!({"pending":pending,"failed":failed,"bytes":bytes}))
    }

    async fn set_upload(&self, source: Id, upload: &str) -> Result<()> {
        anyhow::ensure!(
            !upload.is_empty() && upload.len() <= 4096,
            "invalid raw multipart upload id"
        );
        let changed = sqlx::query("UPDATE raw_sources SET upload_id=? WHERE id=? AND state='writing' AND medium='shared_s3' AND upload_id IS NULL")
            .bind(upload).bind(source).execute(&self.pool).await?.rows_affected();
        anyhow::ensure!(changed == 1, crate::model::Error::OperationAborted);
        Ok(())
    }
    async fn upload_id(&self, source: Id) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT upload_id FROM raw_sources WHERE id=?")
                .bind(source)
                .fetch_optional(&self.pool)
                .await?
                .flatten(),
        )
    }
    async fn deduplicate(&self, stream: Id, source: Id, hash: &[u8]) -> Result<bool> {
        transactions::deduplicate_raw(&self.pool, stream, source, hash).await
    }

    async fn quarantines(
        &self,
        storage: Uuid,
        after: Id,
        limit: i64,
    ) -> Result<Vec<crate::model::source::QuarantinedSource>> {
        use crate::model::source::{QuarantinedSource, SourceScope, SourceType};
        let rows: Vec<(Id,Uuid,String,String,String,i64)> = sqlx::query_as("SELECT id,location_id,state,DATE_FORMAT(quarantined_at,'%Y-%m-%dT%H:%i:%s.%fZ'),quarantine_code,revision FROM raw_sources WHERE storage_id=? AND id>? AND quarantined_at IS NOT NULL ORDER BY id LIMIT ?")
            .bind(storage).bind(after).bind(limit).fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(
                |(id, location_id, state, quarantined_at, code, revision)| QuarantinedSource {
                    scope: SourceScope {
                        storage_id: storage,
                        source: SourceType::Raw,
                        id: id.to_string(),
                    },
                    location_id,
                    state,
                    quarantined_at,
                    code,
                    revision: revision.to_string(),
                },
            )
            .collect())
    }
    async fn begin(
        &self,
        stream: Id,
        storage: Uuid,
        profile_revision: i64,
        buffered: bool,
    ) -> Result<(Id, Uuid)> {
        transactions::begin_raw(
            &self.pool,
            stream,
            storage,
            profile_revision,
            None,
            buffered,
        )
        .await
    }
    async fn finish(&self, stream: Id, source: Id, size: i64, digest: &[u8]) -> Result<()> {
        transactions::finish_raw(&self.pool, stream, source, size, digest, None).await
    }
    async fn copyable(&self, stream: Id, storage: Uuid) -> Result<bool> {
        Ok(raw_copyable(stream, storage)
            .build_query_scalar()
            .fetch_one(&self.pool)
            .await?)
    }
    async fn copy_references(
        &self,
        destination: Id,
        source: Id,
        storage: Uuid,
        revision: i64,
        rows: &[RawRange],
    ) -> Result<()> {
        transactions::copy_raw_references(&self.pool, destination, source, storage, revision, rows)
            .await
    }
    async fn page(&self, stream: Id, start: i64, end: i64) -> Result<Vec<RawRange>> {
        Ok(raw_page(stream, start, end).fetch_all(&self.pool).await?)
    }
    async fn candidates(
        &self,
        storage: Uuid,
        grace: i64,
        minimum: i64,
        limit: i64,
    ) -> Result<Vec<RawSource>> {
        anyhow::ensure!(
            (1..=128).contains(&limit) && grace >= 0 && minimum >= 0,
            "invalid raw cleanup bounds"
        );
        Ok(sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=2 AND tl.physical_id=r.location_id) AS tier_material,r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r LEFT JOIN tier_locations th ON th.unit=2 AND th.physical_id=r.location_id WHERE COALESCE(th.storage_id,r.storage_id)=? AND r.owner_stream IS NULL AND r.unreferenced_at<DATE_SUB(CURRENT_TIMESTAMP(6),INTERVAL ? SECOND) AND r.created_at<DATE_SUB(CURRENT_TIMESTAMP(6),INTERVAL ? SECOND) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=r.id) AND NOT EXISTS(SELECT 1 FROM transfer_sources p JOIN transfer_runs t ON t.task_id=p.task_id WHERE p.source_type='raw' AND p.source_id=r.id AND t.phase NOT IN ('complete','cancelled')) AND NOT EXISTS(SELECT 1 FROM native_uploads nu JOIN uploads u ON u.id=nu.upload_id WHERE nu.raw_id=r.id AND u.state IN ('active','completing')) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=r.id) AND NOT EXISTS(SELECT 1 FROM tier_moves mv WHERE mv.unit=2 AND mv.physical_id=r.location_id) AND NOT EXISTS(SELECT 1 FROM tier_locations th WHERE th.unit=2 AND th.physical_id=r.location_id AND th.activated_at>CAST(UNIX_TIMESTAMP() AS SIGNED)-?) ORDER BY r.unreferenced_at,r.id LIMIT ?")
            .bind(storage).bind(grace).bind(minimum).bind(minimum).bind(limit).fetch_all(&self.pool).await?)
    }
    async fn claim(&self, source: Id, revision: i64, grace: i64, minimum: i64) -> Result<bool> {
        Ok(sqlx::query("UPDATE raw_sources SET state='deleting',revision=revision+1 WHERE id=? AND revision=? AND owner_stream IS NULL AND unreferenced_at<DATE_SUB(CURRENT_TIMESTAMP(6),INTERVAL ? SECOND) AND created_at<DATE_SUB(CURRENT_TIMESTAMP(6),INTERVAL ? SECOND) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM transfer_sources p JOIN transfer_runs t ON t.task_id=p.task_id WHERE p.source_type='raw' AND p.source_id=raw_sources.id AND t.phase NOT IN ('complete','cancelled')) AND NOT EXISTS(SELECT 1 FROM native_uploads nu JOIN uploads u ON u.id=nu.upload_id WHERE nu.raw_id=raw_sources.id AND u.state IN ('active','completing')) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM node_read_sources ns JOIN node_read_windows w ON w.id=ns.window_id JOIN node_reads nr ON nr.read_id=w.read_id WHERE ns.location_id=COALESCE((SELECT a.address FROM tier_locations a WHERE a.unit=2 AND a.physical_id=raw_sources.location_id),raw_sources.location_id) AND nr.expires_at>UTC_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM tier_moves mv WHERE mv.unit=2 AND mv.physical_id=raw_sources.location_id) AND NOT EXISTS(SELECT 1 FROM tier_locations th WHERE th.unit=2 AND th.physical_id=raw_sources.location_id AND th.activated_at>CAST(UNIX_TIMESTAMP() AS SIGNED)-?)")
            .bind(source).bind(revision).bind(grace).bind(minimum).bind(minimum).execute(&self.pool).await?.rows_affected()==1)
    }
    async fn removed(&self, source: Id) -> Result<()> {
        let mut tx = super::super::queries::tier::begin(self).await?;
        let unit = crate::model::tiering::Unit::Raw;
        let address = super::super::queries::tier::gc_identity(&mut *tx, unit, source).await?;
        let changed=sqlx::query("DELETE FROM raw_sources WHERE id=? AND state='deleting' AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM transfer_sources p JOIN transfer_runs t ON t.task_id=p.task_id WHERE p.source_type='raw' AND p.source_id=raw_sources.id AND t.phase NOT IN ('complete','cancelled')) AND NOT EXISTS(SELECT 1 FROM native_uploads nu JOIN uploads u ON u.id=nu.upload_id WHERE nu.raw_id=raw_sources.id AND u.state IN ('active','completing')) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=raw_sources.id)")
            .bind(source).execute(&mut *tx).await?.rows_affected();
        if changed == 1
            && let Some(address) = address
        {
            super::super::queries::tier::forget(&mut *tx, unit, address).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

pub(in super::super) fn raw_lock_stream(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT id FROM streams WHERE id=? AND state='writing' AND payload_type='raw' FOR UPDATE ",
    )
    .bind(stream)
}

pub(in super::super) fn raw_source(id: Id) -> Rows<'static, RawSource> {
    sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id=? FOR UPDATE").bind(id)
}

pub(in super::super) fn raw_duplicate(
    storage: Uuid,
    hash: &[u8],
    size: i64,
) -> Rows<'_, RawSource> {
    sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.storage_id=? AND r.content_hash=? AND r.size=? FOR UPDATE")
        .bind(storage).bind(hash).bind(size)
}

pub(in super::super) fn raw_hash(id: Id, hash: Option<&[u8]>) -> Statement<'_> {
    sqlx::query("UPDATE raw_sources SET content_hash=? WHERE id=?")
        .bind(hash)
        .bind(id)
}

pub(in super::super) fn raw_share(
    stream: Id,
    previous: Id,
    replacement: Id,
    size: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE raw_ranges SET source_id=? WHERE stream_id=? AND source_id=? AND offset_bytes=0 AND source_offset=0 AND length=?")
        .bind(replacement).bind(stream).bind(previous).bind(size)
}

pub(in super::super) fn raw_reference(source: Id) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET revision=revision+1 WHERE id=?").bind(source)
}

pub(in super::super) fn lock_local_upload(upload: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT state FROM uploads WHERE id=? FOR UPDATE").bind(upload)
}

pub(in super::super) fn current_multipart_file(
    upload: Uuid,
    node: Option<Uuid>,
) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM multipart_files WHERE upload_id=? AND node_id<=>? AND part_stream IS NULL AND state='open' ORDER BY id LIMIT 1",
    )
    .bind(upload).bind(node)
}

pub(in super::super) fn insert_multipart_file(
    id: Uuid,
    upload: Uuid,
    node: Option<Uuid>,
) -> Statement<'static> {
    sqlx::query("INSERT INTO multipart_files(id,upload_id,storage_id,node_id) SELECT ?,u.id,b.storage_id,? FROM uploads u JOIN buckets b ON b.id=u.bucket_id JOIN storages s ON s.id=b.storage_id WHERE u.id=? AND u.state='active' ").bind(id).bind(node).bind(upload)
}

pub(in super::super) fn insert_multipart_checkpoint(
    upload: Uuid,
    source: Id,
) -> Statement<'static> {
    sqlx::query("INSERT INTO multipart_files(id,upload_id,storage_id,source_id,state) SELECT r.location_id,?,r.storage_id,r.id,'sealed' FROM raw_sources r WHERE r.id=? AND r.state='writing'  AND r.owner_stream IS NOT NULL ON DUPLICATE KEY UPDATE id=multipart_files.id").bind(upload).bind(source)
}

pub(in super::super) fn remap_multipart_range<'a>(
    upload: Uuid,
    source: Id,
    range: &'a crate::model::multipart::FileRemap,
) -> Statement<'a> {
    sqlx::query("UPDATE fragments SET file_id=(SELECT location_id FROM raw_sources WHERE id=?),file_offset=? WHERE id=? AND file_id=? AND file_offset=? AND size=? AND hash=? AND sealed AND EXISTS(SELECT 1 FROM extents e JOIN parts p ON p.stream_id=e.stream_id WHERE e.fragment_id=fragments.id AND p.upload_id=?)").bind(source).bind(range.destination).bind(range.fragment).bind(range.file).bind(range.offset).bind(range.size).bind(range.hash.as_slice()).bind(upload)
}

pub(in super::super) fn pending_multipart_reclaim(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE multipart_files SET reclaim_pending=true WHERE id=? AND state='sealed'")
        .bind(id)
}

pub(in super::super) fn cache_policy(chunk: Id) -> Rows<'static, (String, String, Option<Value>)> {
    sqlx::query_as("SELECT COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(u.encoding_policy,'$.pack_max_size')) AS CHAR CHARACTER SET ascii) COLLATE ascii_bin,p.pack_max_size),COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(u.encoding_policy,'$.pack_flush_timeout')) AS CHAR CHARACTER SET ascii) COLLATE ascii_bin,p.pack_upload_flush_timeout),u.encoding_policy FROM chunks c JOIN storage_profiles p ON p.storage_id=c.storage_id JOIN pending_uploads u ON u.chunk_id=c.id WHERE c.id=?").bind(chunk)
}

pub(in super::super) fn pack_policy_due(
    storage: Uuid,
    policy: &str,
    seconds: u64,
) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM storage_maintenance WHERE storage_id=? AND policy=? AND last_scheduled_at>TIMESTAMPADD(SECOND,-?,UTC_TIMESTAMP(6)))").bind(storage).bind(policy).bind(seconds as i64)
}

pub(in super::super) fn pack_policy_scheduled(storage: Uuid, policy: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_maintenance(storage_id,policy,last_scheduled_at) VALUES(?,?,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE last_scheduled_at=UTC_TIMESTAMP(6)").bind(storage).bind(policy)
}

pub(in super::super) fn raw_enqueue_processing(stream: Id, source: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO raw_processing(stream_id,source_id,payload_revision) SELECT id,?,payload_revision FROM streams WHERE id=?").bind(source).bind(stream)
}

pub(in super::super) fn raw_processing_due(single: bool) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT w.stream_id FROM raw_processing w JOIN streams s ON s.id=w.stream_id WHERE NOT EXISTS(SELECT 1 FROM mokyu_meta m WHERE m.deployment_mode='single_control' AND m.remote_work) AND NOT EXISTS(SELECT 1 FROM raw_uploads u WHERE u.source_id=w.source_id) AND s.state<>'writing' AND (s.state<>'ready' OR EXISTS(SELECT 1 FROM objects o WHERE o.stream_id=s.id)) AND w.next_attempt_at<=UTC_TIMESTAMP(6) AND (? OR w.token IS NULL OR w.lease_expires_at<UTC_TIMESTAMP(6)) ORDER BY w.next_attempt_at,w.stream_id LIMIT 1 FOR UPDATE").bind(single)
}

pub(in super::super) fn raw_processing_claim(
    stream: Id,
    token: Uuid,
    single: bool,
) -> Statement<'static> {
    sqlx::query("UPDATE raw_processing SET token=?,lease_expires_at=CASE WHEN ? THEN NULL ELSE TIMESTAMPADD(SECOND,120,UTC_TIMESTAMP(6)) END WHERE stream_id=?").bind(token).bind(single).bind(stream)
}

pub(in super::super) fn raw_processing_record(stream: Id) -> Rows<'static, RawProcessing> {
    sqlx::query_as("SELECT w.stream_id AS stream,w.source_id AS source,s.bucket_id AS bucket,CONVERT(s.object_key USING utf8mb4) AS object_key,w.payload_revision,w.content_hash,w.source_revision,w.token FROM raw_processing w JOIN streams s ON s.id=w.stream_id WHERE w.stream_id=?").bind(stream)
}

pub(in super::super) fn raw_processing_owned(work: &RawProcessing) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT source_id FROM raw_processing WHERE stream_id=? AND token=? AND (lease_expires_at IS NULL OR lease_expires_at>UTC_TIMESTAMP(6)) FOR UPDATE").bind(work.stream).bind(work.token)
}

pub(in super::super) fn raw_processing_delete(work: &RawProcessing) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_processing WHERE stream_id=? AND token=?")
        .bind(work.stream)
        .bind(work.token)
}

pub(in super::super) fn raw_processing_stream(work: &RawProcessing) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=? AND state='ready' AND payload_type='raw' AND payload_revision=? AND EXISTS(SELECT 1 FROM objects WHERE stream_id=streams.id) FOR UPDATE").bind(work.stream).bind(work.payload_revision)
}

pub(in super::super) fn raw_processing_changed(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET payload_revision=payload_revision+1 WHERE id=?").bind(stream)
}

pub(in super::super) fn nonce_counter(key: Uuid, type_tag: u8) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT n.reserved_id FROM nonce_counters n JOIN managed_keys k ON k.id=n.block_key_id WHERE n.block_key_id=? AND n.type_tag=? AND k.purpose='block' AND k.revoked_at IS NULL FOR UPDATE").bind(key).bind(i64::from(type_tag))
}

pub(in super::super) fn advance_nonce(
    key: Uuid,
    type_tag: u8,
    previous: i64,
    last: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE nonce_counters SET reserved_id=? WHERE block_key_id=? AND type_tag=? AND reserved_id=?").bind(last).bind(key).bind(i64::from(type_tag)).bind(previous)
}

pub(in super::super) fn initialize_nonce_counters(key: Uuid) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO nonce_counters(block_key_id,type_tag,reserved_id) VALUES(?,0,0),(?,1,0)",
    )
    .bind(key)
    .bind(key)
}

pub(in super::super) fn replacement_storages(source: Uuid) -> Rows<'static, KeyReplacementStorage> {
    sqlx::query_as("SELECT s.id,s.name,s.enabled,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.block_key_id=? ORDER BY s.id FOR UPDATE").bind(source)
}

pub(in super::super) fn replacement_key(id: Uuid) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT name,key_algorithm FROM managed_keys WHERE id=? AND purpose='block' AND revoked_at IS NULL FOR UPDATE").bind(id)
}

pub(in super::super) fn replacement_capacity(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT count(*)=2 AND min(reserved_id)>=0 AND max(reserved_id)<=9223372036854775743 FROM nonce_counters WHERE block_key_id=?").bind(id)
}

pub(in super::super) fn insert_key_replacement<'q>(
    actor: &'q str,
    plan: &'q KeyReplacementPreview,
) -> Statement<'q> {
    sqlx::query("INSERT INTO key_replacements(id,actor,plan,expires_at) VALUES(?,?,?,?)")
        .bind(plan.id)
        .bind(actor)
        .bind(sqlx::types::Json(plan))
        .bind(plan.expires_at)
}

pub(in super::super) fn key_replacement<'q>(id: Uuid, actor: &'q str) -> Rows<'q, ReplacementRow> {
    sqlx::query_as("SELECT plan,result FROM key_replacements WHERE id=? AND actor=? FOR UPDATE")
        .bind(id)
        .bind(actor)
}

pub(in super::super) fn complete_key_replacement(result: &KeyReplacementResult) -> Statement<'_> {
    sqlx::query("UPDATE key_replacements SET result=?,expires_at=CURRENT_TIMESTAMP(6)+INTERVAL 7 DAY WHERE id=? AND result IS NULL").bind(sqlx::types::Json(result)).bind(result.preview.id)
}

pub(in super::super) fn raw_receiving(source: Id, stream: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT storage_id FROM raw_sources WHERE id=? AND owner_stream=? AND state='writing' AND medium='shared_s3' AND upload_id IS NULL FOR UPDATE").bind(source).bind(stream)
}

pub(in super::super) fn raw_discard(source: Id, stream: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_sources WHERE id=? AND owner_stream=? AND state='writing' AND upload_id IS NULL AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM native_uploads nu JOIN uploads u ON u.id=nu.upload_id WHERE nu.raw_id=raw_sources.id AND u.state IN ('active','completing')) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=raw_sources.id)").bind(source).bind(stream)
}

pub(in super::super) fn raw_reusable<'a>(
    storage: Uuid,
    hash: &'a [u8],
    size: i64,
    digest: &'a [u8],
) -> Scalar<'a, Id> {
    sqlx::query_scalar("SELECT id FROM raw_sources WHERE storage_id=? AND content_hash=? AND size=? AND digest=? AND state='ready' AND medium='shared_s3' AND NOT pending_upload AND quarantine_code IS NULL").bind(storage).bind(hash).bind(size).bind(digest)
}

pub(in super::super) fn raw_uploads(after: Id) -> Rows<'static, RawUpload> {
    sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,u.token,u.deduplicate,u.background,u.content_hash FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE u.node_id IS NULL AND u.source_id>? ORDER BY u.source_id LIMIT 128").bind(after)
}

pub(in super::super) fn raw_upload_record(source: Id) -> Rows<'static, RawUpload> {
    sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,u.token,u.deduplicate,u.background,u.content_hash FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE u.source_id=? FOR UPDATE").bind(source)
}

pub(in super::super) fn raw_upload_due(
    before: Option<chrono::DateTime<chrono::Utc>>,
    urgent: &[Uuid],
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT u.source_id FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id JOIN storage_profiles sp ON sp.storage_id=r.storage_id WHERE u.node_id IS NULL AND u.next_attempt_at<=CURRENT_TIMESTAMP(6) AND r.state='ready'",
    );
    if let Some(before) = before {
        query
            .push(" AND r.created_at<=")
            .push_bind(before.naive_utc());
    } else {
        query.push(" AND (NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=r.processing_task AND t.state<>'completed') OR TIMESTAMPADD(SECOND,sp.write_backlog_max_age,r.created_at)<=CURRENT_TIMESTAMP(6))");
        write_delay(&mut query, "r.created_at", "r.storage_id", urgent);
    }
    query.push(" ORDER BY CASE WHEN TIMESTAMPADD(SECOND,sp.write_backlog_max_age,r.created_at)<=CURRENT_TIMESTAMP(6) THEN 0 ELSE 1 END,u.background,u.next_attempt_at,u.source_id LIMIT 1 FOR UPDATE SKIP LOCKED");
    query
}

pub(in super::super) fn raw_upload_claim(source: Id, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE raw_uploads SET token=? WHERE source_id=?")
        .bind(token)
        .bind(source)
}

pub(in super::super) fn raw_queue(
    source: Id,
    deduplicate: bool,
    background: bool,
) -> Statement<'static> {
    sqlx::query("INSERT INTO raw_uploads(source_id,deduplicate,background) VALUES(?,?,?)")
        .bind(source)
        .bind(deduplicate)
        .bind(background)
}

pub(in super::super) fn raw_stage(
    source: Id,
    revision: i64,
    boot: Option<Uuid>,
) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET pending_upload=true,medium=CASE WHEN ? IS NULL THEN 'local' ELSE 'memory' END,boot_id=?,durability=CASE WHEN ? IS NULL THEN durability ELSE 'volatile' END WHERE id=? AND state='ready' AND upload_id IS NULL AND profile_revision=?").bind(boot).bind(boot).bind(boot).bind(source).bind(revision)
}

pub(in super::super) fn raw_upload_delete(source: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_uploads WHERE source_id=?").bind(source)
}

pub(in super::super) fn raw_optional_delete(source: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_processing WHERE source_id=?").bind(source)
}

pub(in super::super) fn raw_upload_streams(source: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT DISTINCT stream_id FROM raw_ranges WHERE source_id=? ORDER BY stream_id LIMIT 128",
    )
    .bind(source)
}

pub(in super::super) fn raw_upload_stream_lock(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=? FOR UPDATE").bind(stream)
}

pub(in super::super) fn raw_upload_share(
    stream: Id,
    source: Id,
    candidate: Id,
) -> Statement<'static> {
    sqlx::query("UPDATE raw_ranges SET source_id=? WHERE stream_id=? AND source_id=?")
        .bind(candidate)
        .bind(stream)
        .bind(source)
}

pub(in super::super) fn raw_upload_references(source: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=?)").bind(source)
}

pub(in super::super) fn raw_uploaded(source: Id) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET pending_upload=false,medium='shared_s3',boot_id=NULL,durability='durable',upload_id=NULL,revision=revision+1 WHERE id=? AND pending_upload").bind(source)
}

pub(in super::super) fn raw_upload_hash(source: Id, hash: Option<&[u8]>) -> Statement<'_> {
    sqlx::query("UPDATE raw_uploads SET content_hash=?,token=NULL,next_attempt_at=CURRENT_TIMESTAMP(6),last_error=NULL WHERE source_id=?").bind(hash).bind(source)
}

pub(in super::super) fn raw_upload_retry<'a>(
    work: &'a RawUpload,
    error: Option<&'a str>,
) -> Statement<'a> {
    sqlx::query("UPDATE raw_uploads SET token=NULL,last_error=?,attempts=attempts+?,next_attempt_at=CURRENT_TIMESTAMP(6)+INTERVAL 1 SECOND WHERE source_id=? AND token=?").bind(error).bind(i32::from(error.is_some())).bind(work.source.id).bind(work.token)
}

pub(in super::super) fn raw_upload_id(
    source: Id,
    token: Uuid,
    upload: Option<&str>,
) -> Statement<'_> {
    sqlx::query("UPDATE raw_sources SET upload_id=? WHERE id=? AND state='ready' AND pending_upload AND EXISTS(SELECT 1 FROM raw_uploads u WHERE u.source_id=raw_sources.id AND u.token=?)").bind(upload).bind(source).bind(token)
}

pub(in super::super) fn raw_upload_mode() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT id=1 FROM mokyu_meta WHERE id=1")
}

pub(in super::super) fn raw_upload_abandon(source: Id) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET state='deleting',revision=revision+1 WHERE id=? AND owner_stream IS NULL AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM native_uploads nu JOIN uploads u ON u.id=nu.upload_id WHERE nu.raw_id=raw_sources.id AND u.state IN ('active','completing')) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=raw_sources.id)").bind(source)
}

pub(in super::super) fn completion_state(
    upload: Uuid,
) -> Rows<'static, (String, Option<String>, bool)> {
    sqlx::query_as("SELECT state,manifest_hash,(output_stream IS NOT NULL OR COALESCE(completion_expires_at>CURRENT_TIMESTAMP(6),false)) FROM uploads WHERE id=? FOR UPDATE").bind(upload)
}

pub(in super::super) fn completion_parts(upload: Uuid) -> Rows<'static, (i32, Id)> {
    sqlx::query_as("SELECT part_number,stream_id FROM parts WHERE upload_id=? AND stream_id IS NOT NULL ORDER BY part_number").bind(upload)
}

pub(in super::super) fn conversion_stream(stream: Id) -> Rows<'static, (Uuid, String, i64)> {
    sqlx::query_as("SELECT bucket_id,CONVERT(object_key USING utf8mb4),payload_revision FROM streams WHERE id=?").bind(stream)
}

pub(in super::super) fn conversion_key(storage: Uuid, key: Option<Uuid>) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT block_key_id <=> ? FROM storages WHERE id=?")
        .bind(key)
        .bind(storage)
}

pub(in super::super) fn conversion_link(source: Id, task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET processing_task=? WHERE id=?")
        .bind(task)
        .bind(source)
}

pub(in super::super) fn conversion_owned(
    candidate: Id,
    task: Uuid,
    token: Uuid,
) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=? AND state='writing' AND owner_task_id=? AND owner_execution_token=? FOR UPDATE").bind(candidate).bind(task).bind(token)
}

pub(in super::super) fn conversion_chunk(chunk: Id, candidate: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT id FROM chunks WHERE id=? AND state='preparing' AND owner_stream=? FOR UPDATE",
    )
    .bind(chunk)
    .bind(candidate)
}

pub(in super::super) fn conversion_ready(chunk: Id, candidate: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET state='ready',owner_stream=NULL,unreferenced_at=NULL WHERE id=? AND owner_stream=? AND state='preparing'").bind(chunk).bind(candidate)
}

pub(in super::super) fn conversion_retire_encoding(encoding: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=CURRENT_TIMESTAMP(6) WHERE encoding_id=? AND state IN ('preparing','uploading')").bind(encoding)
}

pub(in super::super) fn conversion_output_pack(pack: Id, task: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM packs WHERE id=? AND owner_task=? AND state='preparing' AND stored_size IS NOT NULL AND digest IS NOT NULL FOR UPDATE").bind(pack).bind(task)
}

pub(in super::super) fn conversion_output_encoding(
    encoding: Id,
    task: Uuid,
    chunk: Id,
) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT encoding_id FROM chunk_encodings WHERE encoding_id=? AND owner_task=? AND chunk_id=? AND state='uploading' AND standalone_size IS NOT NULL FOR UPDATE").bind(encoding).bind(task).bind(chunk)
}

pub(in super::super) fn conversion_publish_encoding(encoding: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET sync_pending=CASE WHEN boot_id IS NOT NULL THEN FALSE ELSE sync_pending END,boot_id=NULL,state='ready',owner_task=NULL,unreferenced_at=NULL,stored_at=CURRENT_TIMESTAMP(6) WHERE encoding_id=? AND state='uploading'").bind(encoding)
}

pub(in super::super) fn conversion_layout(chunk: Id, pack: Option<Id>) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET pack_id=? WHERE id=?")
        .bind(pack)
        .bind(chunk)
}

pub(in super::super) fn conversion_current(stream: Id, revision: i64) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=? AND payload_revision=? AND payload_type='raw' AND state='ready' AND EXISTS(SELECT 1 FROM objects WHERE stream_id=streams.id) FOR UPDATE").bind(stream).bind(revision)
}

pub(in super::super) fn conversion_move(candidate: Id, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE extents SET stream_id=? WHERE stream_id=?")
        .bind(stream)
        .bind(candidate)
}

pub(in super::super) fn conversion_detach(stream: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_ranges WHERE stream_id=?").bind(stream)
}

pub(in super::super) fn conversion_changed(stream: Id) -> Statement<'static> {
    sqlx::query(
        "UPDATE streams SET payload_type='chunks',payload_revision=payload_revision+1 WHERE id=?",
    )
    .bind(stream)
}

pub(in super::super) fn conversion_abandon(candidate: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='abandoned',size=0,owner_task_id=NULL,owner_execution_token=NULL,touched_at=CURRENT_TIMESTAMP(6) WHERE id=?").bind(candidate)
}

pub(in super::super) fn conversion_unpin(candidate: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM cache_pins WHERE owner_id=?").bind(candidate)
}

pub(in super::super) fn conversion_no_pending(candidate: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM extents e JOIN pending_uploads p ON p.chunk_id=e.chunk_id WHERE e.stream_id=?)").bind(candidate)
}

pub(in super::super) fn conversion_cancel(source: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM tasks WHERE id=(SELECT processing_task FROM raw_sources WHERE id=?) AND state<>'running'").bind(source)
}

pub(in super::super) fn conversion_inline(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET payload_type='chunks' WHERE id=? AND state='writing' AND payload_type='raw'").bind(stream)
}

pub(in super::super) fn conversion_job(
    source: Id,
) -> Scalar<'static, sqlx::types::Json<crate::model::raw::Conversion>> {
    sqlx::query_scalar("SELECT detail FROM tasks WHERE id=(SELECT processing_task FROM raw_sources WHERE id=?) FOR UPDATE").bind(source)
}

pub(in super::super) fn buffer_chunk(chunk: &Chunk, boot: Option<Uuid>) -> Statement<'_> {
    sqlx::query("UPDATE chunk_encodings SET sync_pending=TRUE,boot_id=? WHERE encoding_id=? AND chunk_id=? AND location_id=? AND state='uploading'").bind(boot).bind(chunk.encoding_id).bind(chunk.id).bind(chunk.location_id)
}

pub(in super::super) fn release_memory_pins(boot: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM cache_pins WHERE chunk_id IN (SELECT c.id FROM chunks c JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id WHERE l.boot_id<>? AND l.sync_pending AND l.state IN ('ready','uploading') AND l.quarantined_at IS NULL)").bind(boot)
}

pub(in super::super) fn release_memory_chunks(boot: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM pending_uploads WHERE chunk_id IN (SELECT c.id FROM chunks c JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id WHERE l.boot_id<>? AND l.sync_pending AND l.state IN ('ready','uploading') AND l.quarantined_at IS NULL)").bind(boot)
}

pub(in super::super) fn retire_memory_chunks(boot: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',sync_pending=FALSE,boot_id=NULL,unreferenced_at=CURRENT_TIMESTAMP(6) WHERE boot_id<>? AND sync_pending AND state IN ('ready','uploading') AND quarantined_at IS NULL AND encoding_id IN (SELECT c.current_encoding_id FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE p.state='ready' AND p.quarantined_at IS NULL)").bind(boot)
}

pub(in super::super) fn quarantine_memory_chunks(boot: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET quarantine_code='remote_missing',quarantined_at=CURRENT_TIMESTAMP(6),quarantine_revision=quarantine_revision+1 WHERE boot_id<>? AND sync_pending AND state IN ('ready','uploading') AND quarantined_at IS NULL AND encoding_id IN (SELECT current_encoding_id FROM chunks)").bind(boot)
}

pub(in super::super) fn release_memory_raw(boot: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_uploads WHERE source_id IN (SELECT id FROM raw_sources WHERE durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL AND medium='memory' AND boot_id<>?)").bind(boot)
}

pub(in super::super) fn release_memory_processing(boot: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_processing WHERE source_id IN (SELECT id FROM raw_sources WHERE durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL AND medium='memory' AND boot_id<>?)").bind(boot)
}

pub(in super::super) fn quarantine_memory_raw(boot: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET pending_upload=FALSE,quarantine_code='remote_missing',quarantined_at=CURRENT_TIMESTAMP(6),revision=revision+1 WHERE durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL AND medium='memory' AND boot_id<>? AND quarantine_code IS NULL").bind(boot)
}
