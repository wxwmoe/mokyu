use super::*;

pub(in super::super) fn missing_raw_pages(source: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT page_count IS NULL FROM raw_sources WHERE id=$1").bind(source)
}

const RAW_COPY_READY: &str = "r.state='ready' AND r.quarantine_code IS NULL AND r.durability='durable' AND NOT r.pending_upload AND r.medium IN ('local','shared_s3') AND r.size>=0 AND length(r.digest)=16";

pub(in super::super) fn raw_page(stream: Id, start: i64, end: i64) -> Rows<'static, RawRange> {
    sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=2 AND tl.physical_id=r.id) AS tier_material,r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,e.offset_bytes,e.length,e.source_offset FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id WHERE e.stream_id=$1 AND e.offset_bytes+e.length>=$2 AND e.offset_bytes<=$3 ORDER BY e.offset_bytes LIMIT 128")
            .bind(stream).bind(start).bind(end)
}

pub(in super::super) fn raw_copyable(stream: Id, storage: Uuid) -> sqlx::QueryBuilder<Postgres> {
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
) -> sqlx::QueryBuilder<Postgres> {
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

pub(in super::super) fn raw_copy_position(stream: Id, offset: i64) -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT id FROM streams WHERE id=$1 AND size=$2 AND state='writing' AND payload_type='raw'",
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
) -> sqlx::QueryBuilder<Postgres> {
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

pub(in super::super) fn raw_copy_reference(sources: &[Id]) -> sqlx::QueryBuilder<Postgres> {
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
    // Keep settings stable without blocking another writer's shared bucket lock.
    sqlx::query_as("SELECT t.type,(t.block_key_id IS NULL AND NOT p.compression_enabled AND NOT p.pack_enabled),p.revision,p.cdc_enabled,((p.deduplication_enabled OR p.cdc_enabled) AND p.write_processing='background') FROM streams s JOIN buckets b ON b.id=s.bucket_id JOIN storages t ON t.id=b.storage_id JOIN storage_profiles p ON p.storage_id=t.id WHERE s.id=$1 AND t.id=$2 AND t.enabled AND s.state='writing' FOR UPDATE OF s FOR SHARE OF b,t,p").bind(stream).bind(storage)
}

pub(in super::super) fn raw_prepare(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET payload_type='raw' WHERE id=$1 AND state='writing' AND payload_type='chunks' AND NOT EXISTS(SELECT 1 FROM extents WHERE stream_id=$2) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE stream_id=$3)").bind(stream).bind(stream).bind(stream)
}

pub(in super::super) fn raw_insert(
    stream: Id,
    storage: Uuid,
    revision: i64,
    medium: &'static str,
    buffered: bool,
) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "INSERT INTO raw_sources(owner_stream,storage_id,profile_revision,medium,durability) VALUES($1,$2,$3,$4,$5) RETURNING id",
    )

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
    sqlx::query("UPDATE raw_sources SET size=$1,digest=$2,durability=CASE WHEN $5 AND medium='shared_s3' THEN 'durable' ELSE durability END,state='ready',owner_stream=NULL,upload_id=NULL WHERE id=$3 AND owner_stream=$4 AND state='writing'").bind(size).bind(digest).bind(source).bind(stream).bind(final_source)
}

pub(in super::super) fn raw_attach(stream: Id, source: Id, size: i64) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id) VALUES($1,0,$2,$3)",
    )
    .bind(stream)
    .bind(size)
    .bind(source)
}

pub(in super::super) fn raw_sources_lock(stream: Id) -> Rows<'static, RawSource> {
    sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id IN (SELECT source_id FROM raw_ranges WHERE stream_id=$1) ORDER BY r.id FOR UPDATE").bind(stream)
}

pub(in super::super) fn raw_continuity(stream: Id) -> Rows<'static, (i64, bool)> {
    sqlx::query_as("SELECT COALESCE(MAX(e.offset_bytes+e.length),0),COALESCE(bool_and(e.offset_bytes=e.previous_end AND e.source_offset+e.length<=r.size),TRUE) AND NOT EXISTS(SELECT 1 FROM extents WHERE stream_id=$1) FROM (SELECT *,LAG(offset_bytes+length,1,0) OVER(ORDER BY offset_bytes) AS previous_end FROM raw_ranges WHERE stream_id=$2) e JOIN raw_sources r ON r.id=e.source_id").bind(stream).bind(stream)
}

pub(in super::super) fn raw_cleanup_ranges(stream: Id, batch: i64) -> Rows<'static, (i64, Id)> {
    sqlx::query_as("SELECT offset_bytes,source_id FROM raw_ranges WHERE stream_id=$1 ORDER BY offset_bytes LIMIT $2").bind(stream).bind(batch)
}

pub(in super::super) fn raw_touch_unreferenced(source: Id) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET unreferenced_at=now() WHERE id=$1").bind(source)
}

pub(in super::super) fn raw_detach(stream: Id, end: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_ranges WHERE stream_id=$1 AND offset_bytes<=$2")
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
        let (pending, bytes, oldest, volatile): (i64,i64,Option<chrono::DateTime<chrono::Utc>>,i64) = sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(size),0) AS BIGINT),MIN(created_at),CAST(COALESCE(SUM(CASE WHEN medium='memory' THEN 1 ELSE 0 END),0) AS BIGINT) FROM raw_sources WHERE durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL").fetch_one(&self.pool).await?;
        Ok(serde_json::json!({"pending":pending,"bytes":bytes,"oldest":oldest,"volatile":volatile}))
    }

    async fn buffered_source(&self, source: Id) -> Result<Option<RawSource>> {
        Ok(sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id=$1 AND r.durability IN ('buffered','volatile') AND r.state='ready' AND r.quarantine_code IS NULL").bind(source).fetch_optional(&self.pool).await?)
    }
    async fn buffered_duplicate(
        &self,
        storage: Uuid,
        hash: &[u8],
        size: i64,
    ) -> Result<Option<RawSource>> {
        Ok(sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.storage_id=$1 AND r.content_hash=$2 AND r.size=$3 AND r.durability IN ('buffered','volatile') AND r.state='ready' AND r.quarantine_code IS NULL").bind(storage).bind(hash).bind(size).fetch_optional(&self.pool).await?)
    }
    async fn buffer(&self, source: Id) -> Result<()> {
        ensure!(sqlx::query("UPDATE raw_sources SET durability='buffered' WHERE id=$1 AND state='writing' AND owner_stream IS NOT NULL").bind(source).execute(&self.pool).await?.rows_affected()==1, crate::model::Error::OperationAborted);
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
        let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r JOIN storage_profiles sp ON sp.storage_id=r.storage_id WHERE r.durability IN ('buffered','volatile') AND r.state='ready' AND r.quarantine_code IS NULL AND r.id>",
        );
        query.push_bind(after);
        if let Some(before) = before {
            query.push(" AND r.created_at<=").push_bind(before);
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
            sqlx::query("UPDATE raw_sources SET quarantine_code=$1,quarantined_at=clock_timestamp(),revision=revision+1 WHERE id=$2 AND revision=$3 AND durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL").bind(code).bind(source.id).bind(source.revision).execute(&self.pool).await?;
        } else {
            sqlx::query("UPDATE raw_sources SET durability='durable',medium=CASE WHEN medium='memory' THEN 'local' ELSE medium END,boot_id=NULL WHERE id=$1 AND revision=$2 AND durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL").bind(source.id).bind(source.revision).execute(&self.pool).await?;
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
        owner: Option<Uuid>,
    ) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE r.created_at<=$1 AND ($2::uuid IS NULL OR u.node_id=$2)) OR EXISTS(SELECT 1 FROM parts p JOIN streams s ON s.id=p.stream_id JOIN uploads u ON u.id=p.upload_id WHERE p.remote_node_id IS NOT NULL AND p.remote_etag IS NULL AND ($2::uuid IS NULL OR p.remote_node_id=$2) AND s.created_at<=$1 AND u.state IN ('active','completing'))").bind(before).bind(owner).fetch_one(&self.pool).await?)
    }
    async fn upload_status(&self) -> Result<Value> {
        let (pending,bytes,failed):(i64,i64,i64)=sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(size),0) AS bigint),CAST(COALESCE(SUM(CASE WHEN last_error IS NOT NULL THEN 1 ELSE 0 END),0) AS bigint) FROM (SELECT r.size,u.last_error FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id UNION ALL SELECT s.size,p.remote_error FROM parts p JOIN streams s ON s.id=p.stream_id JOIN uploads u ON u.id=p.upload_id WHERE p.remote_node_id IS NOT NULL AND p.remote_etag IS NULL AND u.state IN ('active','completing')) waiting").fetch_one(&self.pool).await?;
        Ok(serde_json::json!({"pending":pending,"bytes":bytes,"failed":failed}))
    }
    async fn abandon_upload(
        &self,
        work: &RawUpload,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::abandon_raw_upload(&self.pool, work, node).await
    }

    async fn cached_checkpoint(&self, location: Uuid) -> Result<Option<Id>> {
        Ok(sqlx::query_scalar("SELECT source_id FROM multipart_files m WHERE m.id=$1 AND source_id IS NOT NULL AND m.state<>'deleting' AND EXISTS(SELECT 1 FROM fragments WHERE file_id=m.id)").bind(location).fetch_optional(&self.pool).await?)
    }
    async fn cached_checkpoints(&self, after: Uuid) -> Result<Vec<(Uuid, Id)>> {
        Ok(sqlx::query_as("SELECT m.id,m.source_id FROM multipart_files m JOIN storages s ON s.id=m.storage_id WHERE m.id>$1 AND s.type='s3' AND m.source_id IS NOT NULL AND m.state<>'deleting' AND EXISTS(SELECT 1 FROM fragments WHERE file_id=m.id) ORDER BY m.id LIMIT 128").bind(after).fetch_all(&self.pool).await?)
    }
    async fn has_upload(&self, source: Id) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw_uploads WHERE source_id=$1)")
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
        node: crate::model::raw::Processor<'_>,
    ) -> Result<()> {
        transactions::finish_raw(
            &self.pool,
            stream,
            source,
            size,
            digest,
            Some((revision, deduplicate, background, boot)),
            node,
        )
        .await
    }
    async fn uploads(&self, after: Id, owner: Option<Uuid>) -> Result<Vec<RawUpload>> {
        Ok(raw_uploads(after, owner).fetch_all(&self.pool).await?)
    }
    async fn claim_upload(
        &self,
        token: Uuid,
        before: Option<chrono::DateTime<chrono::Utc>>,
        node: crate::model::raw::Processor<'_>,
        urgent: &[Uuid],
    ) -> Result<Option<RawUpload>> {
        transactions::claim_raw_upload(&self.pool, token, before, node, urgent).await
    }
    async fn renew_upload(
        &self,
        work: &RawUpload,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::renew_raw_upload(&self.pool, work, node).await
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
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::reuse_raw_upload(&self.pool, work, hash, streams, node).await
    }
    async fn complete_upload(
        &self,
        work: &RawUpload,
        hash: Option<&[u8]>,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<()> {
        transactions::complete_raw_upload(&self.pool, work, hash, node).await
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
        transactions::claim_raw_processing(&self.pool, node, token).await
    }
    async fn renew_processing(
        &self,
        work: &RawProcessing,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::renew_raw_processing(&self.pool, work, node).await
    }
    async fn save_processing_hash(
        &self,
        work: &RawProcessing,
        revision: i64,
        hash: &[u8],
    ) -> Result<()> {
        anyhow::ensure!(hash.len() == 32, "invalid raw processing hash");
        anyhow::ensure!(sqlx::query("UPDATE raw_processing SET content_hash=$1,source_revision=$2 WHERE stream_id=$3 AND token=$4 AND (lease_expires_at IS NULL OR lease_expires_at>clock_timestamp())")
            .bind(hash).bind(revision).bind(work.stream).bind(work.token).execute(&self.pool).await?.rows_affected()==1,
            crate::model::Error::OperationAborted);
        Ok(())
    }
    async fn retry_processing(&self, work: &RawProcessing, error: Option<&str>) -> Result<()> {
        let error = error.map(|message| message.chars().take(1024).collect::<String>());
        sqlx::query("UPDATE raw_processing SET token=NULL,lease_expires_at=NULL,last_error=$1,attempts=attempts+$2,next_attempt_at=clock_timestamp()+$3*interval '1 second' WHERE stream_id=$4 AND token=$5")
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
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::finish_raw_processing(&self.pool, work, source, hash, lease, node).await
    }
    async fn processing_status(&self) -> Result<Value> {
        let (pending,failed,bytes): (i64,i64,i64)=sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM((w.last_error IS NOT NULL)::integer),0) AS bigint),CAST(COALESCE(SUM(r.size),0) AS bigint) FROM raw_processing w JOIN raw_sources r ON r.id=w.source_id")
            .fetch_one(&self.pool).await?;
        Ok(serde_json::json!({"pending":pending,"failed":failed,"bytes":bytes}))
    }

    async fn set_upload(&self, source: Id, upload: &str) -> Result<()> {
        anyhow::ensure!(
            !upload.is_empty() && upload.len() <= 4096,
            "invalid raw multipart upload id"
        );
        let changed = sqlx::query("UPDATE raw_sources SET upload_id=$1 WHERE id=$2 AND state='writing' AND medium='shared_s3' AND upload_id IS NULL")
            .bind(upload).bind(source).execute(&self.pool).await?.rows_affected();
        anyhow::ensure!(changed == 1, crate::model::Error::OperationAborted);
        Ok(())
    }
    async fn upload_id(&self, source: Id) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT upload_id FROM raw_sources WHERE id=$1")
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
        let rows: Vec<(Id,Uuid,String,String,String,i64)> = sqlx::query_as("SELECT id,id,state,to_char(quarantined_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),quarantine_code,revision FROM raw_sources WHERE storage_id=$1 AND id>$2 AND quarantined_at IS NOT NULL ORDER BY id LIMIT $3")
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
        transactions::finish_raw(&self.pool, stream, source, size, digest, None, None).await
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
        Ok(raw_gc_candidates(storage, grace, minimum, limit)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn claim(&self, source: Id, revision: i64, grace: i64, minimum: i64) -> Result<bool> {
        Ok(raw_gc_claim(source, revision, grace, minimum)
            .execute(&self.pool)
            .await?
            .rows_affected()
            == 1)
    }
    async fn removed(&self, source: Id) -> Result<()> {
        let mut tx = super::super::queries::tier::begin(self).await?;
        let unit = crate::model::tiering::Unit::Raw;
        let address = super::super::queries::tier::gc_identity(&mut *tx, unit, source).await?;
        let changed = raw_gc_finish(source)
            .execute(&mut *tx)
            .await?
            .rows_affected();
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
        "SELECT id FROM streams WHERE id=$1 AND state='writing' AND payload_type='raw' FOR UPDATE ",
    )
    .bind(stream)
}

pub(in super::super) fn raw_source(id: Id) -> Rows<'static, RawSource> {
    sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id=$1 FOR UPDATE").bind(id)
}

pub(in super::super) fn raw_duplicate(
    storage: Uuid,
    hash: &[u8],
    size: i64,
) -> Rows<'_, RawSource> {
    sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.storage_id=$1 AND r.content_hash=$2 AND r.size=$3 FOR UPDATE")
        .bind(storage).bind(hash).bind(size)
}

pub(in super::super) fn raw_hash(id: Id, hash: Option<&[u8]>) -> Statement<'_> {
    sqlx::query("UPDATE raw_sources SET content_hash=$1 WHERE id=$2")
        .bind(hash)
        .bind(id)
}

pub(in super::super) fn raw_share(
    stream: Id,
    previous: Id,
    replacement: Id,
    size: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE raw_ranges SET source_id=$1 WHERE stream_id=$2 AND source_id=$3 AND offset_bytes=0 AND source_offset=0 AND length=$4")
        .bind(replacement).bind(stream).bind(previous).bind(size)
}

pub(in super::super) fn raw_reference(source: Id) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET revision=revision+1 WHERE id=$1").bind(source)
}

pub(in super::super) fn lock_local_upload(upload: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT CASE WHEN remote_manifest_hash IS NOT NULL AND state='active' THEN 'completing' ELSE state END FROM uploads WHERE id=$1 FOR UPDATE").bind(upload)
}

pub(in super::super) fn current_multipart_file(
    upload: Uuid,
    node: Option<Uuid>,
) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM multipart_files WHERE upload_id=$1 AND node_id IS NOT DISTINCT FROM $2 AND state='open' ORDER BY id LIMIT 1",
    )
    .bind(upload).bind(node)
}

pub(in super::super) fn insert_multipart_file(
    upload: Uuid,
    node: Option<Uuid>,
) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("INSERT INTO multipart_files(upload_id,storage_id,node_id) SELECT u.id,b.storage_id,$2 FROM uploads u JOIN buckets b ON b.id=u.bucket_id JOIN storages s ON s.id=b.storage_id WHERE u.id=$1 AND u.state='active' AND EXISTS(SELECT 1 FROM mokyu_meta WHERE singleton AND (deployment_mode='cluster' AND $2::uuid IS NOT NULL OR deployment_mode='single' AND $2::uuid IS NULL OR deployment_mode='single_control')) RETURNING id").bind(upload).bind(node)
}

pub(in super::super) fn insert_multipart_checkpoint(
    upload: Uuid,
    source: Id,
) -> Statement<'static> {
    sqlx::query("INSERT INTO multipart_files(id,upload_id,storage_id,source_id,state) SELECT r.id,$2,r.storage_id,r.id,'sealed' FROM raw_sources r WHERE r.id=$1 AND r.state='writing' AND EXISTS(SELECT 1 FROM mokyu_meta WHERE singleton AND deployment_mode IN ('single','single_control')) AND r.owner_stream IS NOT NULL ON CONFLICT(id) DO NOTHING").bind(source).bind(upload)
}

pub(in super::super) fn remap_multipart_range<'a>(
    upload: Uuid,
    source: Id,
    range: &'a crate::model::multipart::FileRemap,
) -> Statement<'a> {
    sqlx::query("UPDATE fragments SET file_id=$1,file_offset=$2 WHERE id=$3 AND file_id=$4 AND file_offset=$5 AND size=$6 AND hash=$7 AND sealed AND EXISTS(SELECT 1 FROM extents e JOIN parts p ON p.stream_id=e.stream_id WHERE e.fragment_id=fragments.id AND p.upload_id=$8)").bind(source).bind(range.destination).bind(range.fragment).bind(range.file).bind(range.offset).bind(range.size).bind(range.hash.as_slice()).bind(upload)
}

pub(in super::super) fn pending_multipart_reclaim(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE multipart_files SET reclaim_pending=true WHERE id=$1 AND state='sealed'")
        .bind(id)
}

pub(in super::super) fn cache_policy(chunk: Id) -> Rows<'static, (String, String, Option<Value>)> {
    sqlx::query_as("SELECT COALESCE(u.encoding_policy->>'pack_max_size',p.pack_max_size),COALESCE(u.encoding_policy->>'pack_flush_timeout',p.pack_upload_flush_timeout),u.encoding_policy FROM chunks c JOIN storage_profiles p ON p.storage_id=c.storage_id JOIN pending_uploads u ON u.chunk_id=c.id WHERE c.id=$1").bind(chunk)
}

pub(in super::super) fn pack_policy_due(
    storage: Uuid,
    policy: &str,
    seconds: u64,
) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM storage_maintenance WHERE storage_id=$1 AND policy=$2 AND last_scheduled_at>now()-$3*interval '1 second')").bind(storage).bind(policy).bind(seconds as i64)
}

pub(in super::super) fn pack_policy_scheduled(storage: Uuid, policy: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_maintenance(storage_id,policy,last_scheduled_at) VALUES($1,$2,now()) ON CONFLICT(storage_id,policy) DO UPDATE SET last_scheduled_at=EXCLUDED.last_scheduled_at").bind(storage).bind(policy)
}

pub(in super::super) fn raw_enqueue_processing(stream: Id, source: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO raw_processing(stream_id,source_id,payload_revision) SELECT id,$1,payload_revision FROM streams WHERE id=$2").bind(source).bind(stream)
}

pub(in super::super) fn raw_processing_due(single: bool) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT w.stream_id FROM raw_processing w JOIN streams s ON s.id=w.stream_id WHERE NOT EXISTS(SELECT 1 FROM raw_uploads u WHERE u.source_id=w.source_id) AND s.state<>'writing' AND (s.state<>'ready' OR EXISTS(SELECT 1 FROM objects o WHERE o.stream_id=s.id)) AND w.next_attempt_at<=clock_timestamp() AND ($1 OR w.token IS NULL OR w.lease_expires_at IS NULL OR w.lease_expires_at<clock_timestamp()) ORDER BY w.next_attempt_at,w.stream_id LIMIT 1 FOR UPDATE OF w SKIP LOCKED").bind(single)
}

pub(in super::super) fn raw_processing_claim(
    stream: Id,
    token: Uuid,
    single: bool,
) -> Statement<'static> {
    sqlx::query("UPDATE raw_processing SET token=$1,lease_expires_at=CASE WHEN $2 THEN NULL ELSE clock_timestamp()+interval '120 seconds' END WHERE stream_id=$3").bind(token).bind(single).bind(stream)
}

pub(in super::super) fn raw_processing_record(stream: Id) -> Rows<'static, RawProcessing> {
    sqlx::query_as("SELECT w.stream_id AS stream,w.source_id AS source,s.bucket_id AS bucket,s.object_key AS object_key,w.payload_revision,w.content_hash,w.source_revision,w.token FROM raw_processing w JOIN streams s ON s.id=w.stream_id WHERE w.stream_id=$1").bind(stream)
}

pub(in super::super) fn raw_processing_owned(work: &RawProcessing) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT source_id FROM raw_processing WHERE stream_id=$1 AND token=$2 AND (lease_expires_at IS NULL OR lease_expires_at>clock_timestamp()) FOR UPDATE").bind(work.stream).bind(work.token)
}

pub(in super::super) fn raw_processing_delete(work: &RawProcessing) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_processing WHERE stream_id=$1 AND token=$2")
        .bind(work.stream)
        .bind(work.token)
}

pub(in super::super) fn raw_processing_stream(work: &RawProcessing) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=$1 AND state='ready' AND payload_type='raw' AND payload_revision=$2 AND EXISTS(SELECT 1 FROM objects WHERE stream_id=streams.id) FOR UPDATE").bind(work.stream).bind(work.payload_revision)
}

pub(in super::super) fn raw_processing_changed(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET payload_revision=payload_revision+1 WHERE id=$1").bind(stream)
}

pub(in super::super) fn raw_processing_readers(stream: Id, lease: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM stream_leases WHERE stream_id=$1 AND lease_type='read' AND id<>$2 AND expires_at>clock_timestamp())").bind(stream).bind(lease)
}

pub(in super::super) fn renew_raw_processing(work: &RawProcessing) -> Statement<'_> {
    sqlx::query("UPDATE raw_processing SET lease_expires_at=clock_timestamp()+interval '120 seconds' WHERE stream_id=$1 AND token=$2 AND (lease_expires_at IS NULL OR lease_expires_at>clock_timestamp())")
        .bind(work.stream).bind(work.token)
}

pub(in super::super) fn nonce_counter(key: Uuid, type_tag: u8) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT n.reserved_id FROM nonce_counters n JOIN managed_keys k ON k.id=n.block_key_id WHERE n.block_key_id=$1 AND n.type_tag=$2 AND k.purpose='block' AND k.revoked_at IS NULL FOR UPDATE OF n,k").bind(key).bind(i64::from(type_tag))
}

pub(in super::super) fn advance_nonce(
    key: Uuid,
    type_tag: u8,
    previous: i64,
    last: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE nonce_counters SET reserved_id=$1 WHERE block_key_id=$2 AND type_tag=$3 AND reserved_id=$4").bind(last).bind(key).bind(i64::from(type_tag)).bind(previous)
}

pub(in super::super) fn initialize_nonce_counters(key: Uuid) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO nonce_counters(block_key_id,type_tag,reserved_id) VALUES($1,0,0),($1,1,0)",
    )
    .bind(key)
}

pub(in super::super) fn raw_stage(
    source: Id,
    revision: i64,
    boot: Option<Uuid>,
) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET pending_upload=true,medium=CASE WHEN $3::uuid IS NULL THEN 'local' ELSE 'memory' END,boot_id=$3,durability=CASE WHEN $3::uuid IS NULL THEN durability ELSE 'volatile' END WHERE id=$1 AND state='ready' AND upload_id IS NULL AND profile_revision=$2").bind(source).bind(revision).bind(boot)
}

pub(in super::super) fn raw_upload_delete(source: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_uploads WHERE source_id=$1").bind(source)
}

pub(in super::super) fn raw_optional_delete(source: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_processing WHERE source_id=$1").bind(source)
}

pub(in super::super) fn raw_upload_streams(source: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT DISTINCT stream_id FROM raw_ranges WHERE source_id=$1 ORDER BY stream_id LIMIT 128",
    )
    .bind(source)
}

pub(in super::super) fn raw_upload_stream_lock(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=$1 FOR UPDATE").bind(stream)
}

pub(in super::super) fn raw_upload_share(
    stream: Id,
    source: Id,
    candidate: Id,
) -> Statement<'static> {
    sqlx::query("UPDATE raw_ranges SET source_id=$1 WHERE stream_id=$2 AND source_id=$3 AND NOT EXISTS(SELECT 1 FROM stream_leases WHERE stream_id=$2 AND lease_type='read' AND expires_at>clock_timestamp())")
        .bind(candidate)
        .bind(stream)
        .bind(source)
}

pub(in super::super) fn raw_upload_references(source: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=$1)").bind(source)
}

pub(in super::super) fn raw_uploaded(source: Id) -> Statement<'static> {
    sqlx::query("WITH released AS (UPDATE tasks SET execution_scope='cluster',target_node_id=NULL WHERE id=(SELECT processing_task FROM raw_sources WHERE id=$1)) UPDATE raw_sources SET pending_upload=false,medium='shared_s3',boot_id=NULL,durability='durable',upload_id=NULL,revision=revision+1 WHERE id=$1 AND pending_upload").bind(source)
}

pub(in super::super) fn raw_upload_hash(source: Id, hash: Option<&[u8]>) -> Statement<'_> {
    sqlx::query("UPDATE raw_uploads SET content_hash=$1,token=NULL,lease_expires_at=NULL,next_attempt_at=clock_timestamp(),last_error=NULL WHERE source_id=$2").bind(hash).bind(source)
}

pub(in super::super) fn raw_upload_retry<'a>(
    work: &'a RawUpload,
    error: Option<&'a str>,
) -> Statement<'a> {
    sqlx::query("UPDATE raw_uploads SET token=NULL,lease_expires_at=NULL,last_error=$1,attempts=attempts+$2,next_attempt_at=clock_timestamp()+interval '1 second' WHERE source_id=$3 AND token=$4").bind(error).bind(i32::from(error.is_some())).bind(work.source.id).bind(work.token)
}

pub(in super::super) fn raw_upload_id(
    source: Id,
    token: Uuid,
    upload: Option<&str>,
) -> Statement<'_> {
    sqlx::query("UPDATE raw_sources SET upload_id=$1 WHERE id=$2 AND state='ready' AND pending_upload AND EXISTS(SELECT 1 FROM raw_uploads u WHERE u.source_id=raw_sources.id AND u.token=$3 AND (u.lease_expires_at IS NULL OR u.lease_expires_at>clock_timestamp()))").bind(upload).bind(source).bind(token)
}

pub(in super::super) fn raw_upload_mode(cluster: bool) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT deployment_mode IN ($1,'single_control') FROM mokyu_meta FOR SHARE")
        .bind(if cluster { "cluster" } else { "single" })
}

pub(in super::super) fn raw_upload_abandon(source: Id) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET state='deleting',revision=revision+1 WHERE id=$1 AND owner_stream IS NULL AND NOT EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.kind='raw' AND n.location_id=raw_sources.id AND l.expires_at>clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.task_type='integrity' AND t.state='running' AND t.detail->>'operation' IN ('recheck','verify') AND t.detail->>'source_location'=raw_sources.id::text AND ((t.lease_node_id IS NULL AND t.lease_expires_at IS NULL) OR t.lease_expires_at>clock_timestamp())) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM uploads WHERE raw_source_id=raw_sources.id)").bind(source)
}

pub(in super::super) fn raw_upload_owned(
    work: &RawUpload,
    node: Option<Uuid>,
) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT token IS NOT NULL AND token=$2 AND node_id IS NOT DISTINCT FROM $3 AND (lease_expires_at IS NULL OR lease_expires_at>clock_timestamp()) FROM raw_uploads WHERE source_id=$1 FOR UPDATE").bind(work.source.id).bind(work.token).bind(node)
}

pub(in super::super) fn renew_raw_upload(
    work: &RawUpload,
    node: Option<Uuid>,
) -> Statement<'static> {
    sqlx::query("UPDATE raw_uploads SET lease_expires_at=clock_timestamp()+interval '120 seconds' WHERE source_id=$1 AND token=$2 AND node_id=$3 AND lease_expires_at>clock_timestamp()").bind(work.source.id).bind(work.token).bind(node)
}

pub(in super::super) fn release_raw_uploads(node: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE raw_uploads SET token=NULL,lease_expires_at=NULL,next_attempt_at=clock_timestamp() WHERE node_id=$1").bind(node)
}

pub(in super::super) fn cache_nodes() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT node_id FROM pending_uploads WHERE node_id IS NOT NULL UNION SELECT node_id FROM raw_uploads WHERE node_id IS NOT NULL UNION SELECT remote_node_id FROM parts WHERE remote_node_id IS NOT NULL AND remote_etag IS NULL",
    )
}

pub(in super::super) fn scope_upload_task(
    task: Uuid,
    node: Uuid,
    parent: Option<Uuid>,
) -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET execution_scope='node',target_node_id=$2,parent_task_id=$3 WHERE id=$1",
    )
    .bind(task)
    .bind(node)
    .bind(parent)
}

pub(in super::super) fn completion_state(
    upload: Uuid,
) -> Rows<'static, (String, Option<String>, bool)> {
    sqlx::query_as("SELECT state,manifest_hash,(output_stream IS NOT NULL OR COALESCE(completion_expires_at>clock_timestamp(),false)) FROM uploads WHERE id=$1 FOR UPDATE").bind(upload)
}

pub(in super::super) fn completion_parts(upload: Uuid) -> Rows<'static, (i32, Id)> {
    sqlx::query_as("SELECT part_number,stream_id FROM parts WHERE upload_id=$1 AND stream_id IS NOT NULL ORDER BY part_number").bind(upload)
}

pub(in super::super) fn renew_completion(upload: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET completion_expires_at=clock_timestamp()+interval '120 seconds',touched_at=clock_timestamp() WHERE id=$1 AND completion_token=$2 AND state='completing' AND completion_expires_at>clock_timestamp()").bind(upload).bind(token)
}

pub(in super::super) fn remote_upload(upload: Uuid) -> Rows<'static, RemoteUpload> {
    sqlx::query_as("SELECT u.id AS remote_upload,u.state AS remote_state,r.id AS remote_source,r.storage_id AS remote_storage,r.profile_revision AS remote_profile,r.upload_id AS remote_upstream,u.remote_manifest_hash AS remote_manifest FROM uploads u JOIN raw_sources r ON r.id=u.raw_source_id WHERE u.id=$1").bind(upload)
}

pub(in super::super) fn remote_upload_lock(upload: Uuid) -> Rows<'static, (String, Option<Uuid>)> {
    sqlx::query_as("SELECT state,raw_source_id FROM uploads WHERE id=$1 FOR UPDATE").bind(upload)
}

pub(in super::super) fn insert_remote(
    upload: Uuid,
    storage: Uuid,
    revision: i64,
) -> Statement<'static> {
    sqlx::query("WITH candidate AS (INSERT INTO raw_sources(storage_id,profile_revision,medium) SELECT b.storage_id,p.revision,'shared_s3' FROM uploads u JOIN buckets b ON b.id=u.bucket_id JOIN storages s ON s.id=b.storage_id JOIN storage_profiles p ON p.storage_id=s.id WHERE u.id=$1 AND b.storage_id=$2 AND p.revision=$3 AND s.type='s3' AND NOT p.cdc_enabled AND NOT p.compression_enabled AND s.block_key_id IS NULL AND NOT p.pack_enabled AND s.enabled AND NOT EXISTS(SELECT 1 FROM parts WHERE upload_id=u.id AND stream_id IS NOT NULL) RETURNING id) UPDATE uploads SET raw_source_id=(SELECT id FROM candidate) WHERE id=$1 AND EXISTS(SELECT 1 FROM candidate)").bind(upload).bind(storage).bind(revision)
}

pub(in super::super) fn claim_remote(upload: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE uploads u SET remote_token=$2,remote_expires_at=clock_timestamp()+interval '120 seconds' FROM raw_sources r WHERE u.id=$1 AND u.state IN ('active','completing') AND r.id=u.raw_source_id AND r.upload_id IS NULL AND r.state='writing' AND (u.remote_expires_at IS NULL OR u.remote_expires_at<=clock_timestamp())").bind(upload).bind(token)
}

pub(in super::super) fn remote_initialized(
    upload: Uuid,
    token: Uuid,
    upstream: Option<&str>,
) -> Scalar<'_, bool> {
    sqlx::query_scalar("WITH owned AS (UPDATE uploads SET remote_token=NULL,remote_expires_at=NULL WHERE id=$1 AND remote_token=$2 AND remote_expires_at>clock_timestamp() AND state IN ('active','completing') RETURNING raw_source_id), written AS (UPDATE raw_sources SET upload_id=$3 WHERE id=(SELECT raw_source_id FROM owned) AND upload_id IS NULL RETURNING id) SELECT EXISTS(SELECT 1 FROM written)").bind(upload).bind(token).bind(upstream)
}

pub(in super::super) fn remote_part_queue(input: &RemoteCommit<'_>) -> Statement<'static> {
    sqlx::query("UPDATE parts SET remote_node_id=$4,remote_etag=NULL,remote_token=NULL,remote_expires_at=NULL,remote_retry_at=clock_timestamp(),remote_error=NULL,remote_cached=$5 WHERE upload_id=$1 AND part_number=$2 AND write_token=$3 AND EXISTS(SELECT 1 FROM uploads WHERE id=$1 AND raw_source_id IS NOT NULL AND remote_manifest_hash IS NULL)").bind(input.upload).bind(input.number).bind(input.stream).bind(input.node.map(|n| n.0.node_id)).bind(input.cached)
}

pub(in super::super) fn remote_part_due(
    node: Uuid,
    stream: Option<Id>,
) -> Rows<'static, (Uuid, i32)> {
    sqlx::query_as("SELECT p.upload_id,p.part_number FROM parts p JOIN uploads u ON u.id=p.upload_id JOIN raw_sources r ON r.id=u.raw_source_id WHERE p.remote_node_id=$1 AND p.stream_id IS NOT NULL AND ($2::uuid IS NULL OR p.stream_id=$2) AND p.remote_etag IS NULL AND p.remote_retry_at<=clock_timestamp() AND (p.remote_expires_at IS NULL OR p.remote_expires_at<=clock_timestamp()) AND u.state IN ('active','completing') AND r.upload_id IS NOT NULL ORDER BY p.remote_retry_at,p.upload_id,p.part_number LIMIT 1 FOR UPDATE OF p SKIP LOCKED").bind(node).bind(stream)
}

pub(in super::super) fn remote_part_claim(
    upload: Uuid,
    number: i32,
    token: Uuid,
) -> Statement<'static> {
    sqlx::query("UPDATE parts SET remote_token=$3,remote_expires_at=clock_timestamp()+interval '120 seconds' WHERE upload_id=$1 AND part_number=$2").bind(upload).bind(number).bind(token)
}

pub(in super::super) async fn remote_part(
    db: &mut sqlx::PgConnection,
    upload: Uuid,
    number: i32,
) -> Result<RemotePart> {
    Ok(sqlx::query_as("SELECT u.id AS remote_upload,u.state AS remote_state,r.id AS remote_source,r.storage_id AS remote_storage,r.profile_revision AS remote_profile,r.upload_id AS remote_upstream,u.remote_manifest_hash AS remote_manifest,s.*,p.part_number,p.remote_token FROM parts p JOIN uploads u ON u.id=p.upload_id JOIN raw_sources r ON r.id=u.raw_source_id JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=$1 AND p.part_number=$2").bind(upload).bind(number).fetch_one(db).await?)
}

pub(in super::super) fn remote_part_finish<'a>(
    work: &'a RemotePart,
    etag: Option<&'a str>,
    node: Uuid,
) -> Statement<'a> {
    sqlx::query("UPDATE parts p SET remote_etag=COALESCE($6,remote_etag),remote_error=NULL,remote_token=CASE WHEN $6 IS NULL THEN p.remote_token END,remote_expires_at=CASE WHEN $6 IS NULL THEN clock_timestamp()+interval '120 seconds' END FROM uploads u WHERE p.upload_id=$1 AND p.part_number=$2 AND p.stream_id=$3 AND p.remote_token=$4 AND p.remote_node_id=$5 AND p.remote_expires_at>clock_timestamp() AND u.id=p.upload_id AND u.state IN ('active','completing')").bind(work.upload.upload).bind(work.part_number).bind(work.stream.id).bind(work.remote_token).bind(node).bind(etag)
}

pub(in super::super) fn freeze_remote(upload: Uuid, token: Uuid, hash: &str) -> Statement<'_> {
    sqlx::query("UPDATE uploads SET remote_manifest_hash=$3 WHERE id=$1 AND state='completing' AND completion_token=$2 AND completion_expires_at>clock_timestamp() AND manifest_hash=$3 AND (remote_manifest_hash IS NULL OR remote_manifest_hash=$3) AND raw_source_id IS NOT NULL").bind(upload).bind(token).bind(hash)
}

pub(in super::super) fn remote_output_lock(upload: Uuid, token: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT raw_source_id FROM uploads WHERE id=$1 AND state='completing' AND completion_token=$2 AND completion_expires_at>clock_timestamp() AND remote_manifest_hash=manifest_hash FOR UPDATE").bind(upload).bind(token)
}

pub(in super::super) fn remote_source_finish<'a>(
    source: Uuid,
    input: &'a RemoteOutput<'a>,
) -> Statement<'a> {
    sqlx::query("UPDATE raw_sources SET state='ready',size=$2,digest=$3,upload_id=NULL WHERE id=$1 AND quarantine_code IS NULL AND (state='writing' OR (state='ready' AND size=$2 AND digest=$3 AND (content_hash IS NULL OR content_hash=$4)))").bind(source).bind(input.size).bind(input.digest).bind(input.hash)
}

pub(in super::super) fn remote_cleanup_due() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM uploads u WHERE u.raw_source_id IS NOT NULL AND u.state IN ('aborted','completed') AND (u.remote_expires_at IS NULL OR u.remote_expires_at<=clock_timestamp()) AND (u.completion_expires_at IS NULL OR u.completion_expires_at<=clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM parts p WHERE p.upload_id=u.id AND p.remote_expires_at>clock_timestamp()) ORDER BY touched_at LIMIT 1 FOR UPDATE SKIP LOCKED",
    )
}

pub(in super::super) fn remote_cleanup_claim(upload: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET remote_token=$2,remote_expires_at=clock_timestamp()+interval '120 seconds' WHERE id=$1").bind(upload).bind(token)
}

pub(in super::super) fn remote_processing_hash(stream: Id, hash: &[u8]) -> Statement<'_> {
    sqlx::query("UPDATE raw_processing SET content_hash=$2 WHERE stream_id=$1")
        .bind(stream)
        .bind(hash)
}

pub(in super::super) fn conversion_stream(stream: Id) -> Rows<'static, (Uuid, String, i64)> {
    sqlx::query_as("SELECT bucket_id,object_key,payload_revision FROM streams WHERE id=$1")
        .bind(stream)
}

pub(in super::super) fn conversion_key(storage: Uuid, key: Option<Uuid>) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT block_key_id IS NOT DISTINCT FROM $1 FROM storages WHERE id=$2")
        .bind(key)
        .bind(storage)
}

pub(in super::super) fn conversion_link(source: Id, task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET processing_task=$1 WHERE id=$2")
        .bind(task)
        .bind(source)
}

pub(in super::super) fn conversion_owned(
    candidate: Id,
    task: Uuid,
    token: Uuid,
) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=$1 AND state='writing' AND owner_task_id=$2 AND owner_lease_token=$3 FOR UPDATE").bind(candidate).bind(task).bind(token)
}

pub(in super::super) fn conversion_chunk(chunk: Id, candidate: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT id FROM chunks WHERE id=$1 AND state='preparing' AND owner_stream=$2 FOR UPDATE",
    )
    .bind(chunk)
    .bind(candidate)
}

pub(in super::super) fn conversion_ready(chunk: Id, candidate: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET state='ready',owner_stream=NULL,unreferenced_at=NULL WHERE id=$1 AND owner_stream=$2 AND state='preparing'").bind(chunk).bind(candidate)
}

pub(in super::super) fn conversion_retire_encoding(encoding: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=clock_timestamp() WHERE encoding_id=$1 AND state IN ('preparing','uploading')").bind(encoding)
}

pub(in super::super) fn conversion_output_pack(pack: Id, task: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM packs WHERE id=$1 AND owner_task=$2 AND state='preparing' AND stored_size IS NOT NULL AND digest IS NOT NULL FOR UPDATE").bind(pack).bind(task)
}

pub(in super::super) fn conversion_output_encoding(
    encoding: Id,
    task: Uuid,
    chunk: Id,
) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT encoding_id FROM chunk_encodings WHERE encoding_id=$1 AND owner_task=$2 AND chunk_id=$3 AND state='uploading' AND standalone_size IS NOT NULL FOR UPDATE").bind(encoding).bind(task).bind(chunk)
}

pub(in super::super) fn conversion_publish_encoding(encoding: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET sync_pending=CASE WHEN boot_id IS NOT NULL THEN FALSE ELSE sync_pending END,boot_id=NULL,state='ready',owner_task=NULL,unreferenced_at=NULL,stored_at=clock_timestamp() WHERE encoding_id=$1 AND state='uploading'").bind(encoding)
}

pub(in super::super) fn conversion_layout(chunk: Id, pack: Option<Id>) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET pack_id=$1 WHERE id=$2")
        .bind(pack)
        .bind(chunk)
}

pub(in super::super) fn conversion_current(stream: Id, revision: i64) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=$1 AND payload_revision=$2 AND payload_type='raw' AND state='ready' AND (EXISTS(SELECT 1 FROM objects WHERE stream_id=streams.id) OR EXISTS(SELECT 1 FROM parts p JOIN uploads u ON u.id=p.upload_id WHERE p.stream_id=streams.id AND u.state IN ('active','completing'))) FOR UPDATE").bind(stream).bind(revision)
}

pub(in super::super) fn conversion_move(candidate: Id, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE extents SET stream_id=$1 WHERE stream_id=$2")
        .bind(stream)
        .bind(candidate)
}

pub(in super::super) fn conversion_detach(stream: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_ranges WHERE stream_id=$1").bind(stream)
}

pub(in super::super) fn conversion_changed(stream: Id) -> Statement<'static> {
    sqlx::query(
        "UPDATE streams SET payload_type='chunks',payload_revision=payload_revision+1 WHERE id=$1",
    )
    .bind(stream)
}

pub(in super::super) fn conversion_abandon(candidate: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='abandoned',size=0,owner_task_id=NULL,owner_lease_token=NULL,touched_at=clock_timestamp() WHERE id=$1").bind(candidate)
}

pub(in super::super) fn conversion_unpin(candidate: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM cache_pins WHERE owner_id=$1").bind(candidate)
}

pub(in super::super) fn conversion_no_pending(candidate: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM extents e JOIN pending_uploads p ON p.chunk_id=e.chunk_id WHERE e.stream_id=$1)").bind(candidate)
}

pub(in super::super) fn conversion_cancel(source: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM tasks WHERE id=(SELECT processing_task FROM raw_sources WHERE id=$1) AND state<>'running'").bind(source)
}

pub(in super::super) fn conversion_inline(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET payload_type='chunks' WHERE id=$1 AND state='writing' AND payload_type='raw'").bind(stream)
}

pub(in super::super) fn conversion_job(
    source: Id,
) -> Scalar<'static, sqlx::types::Json<crate::model::raw::Conversion>> {
    sqlx::query_scalar("SELECT detail FROM tasks WHERE id=(SELECT processing_task FROM raw_sources WHERE id=$1) FOR UPDATE").bind(source)
}

pub(in super::super) fn buffer_chunk(chunk: &Chunk, boot: Option<Uuid>) -> Statement<'_> {
    sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,storage_id,standalone_size,standalone_compressed,nonce,nonce_id,state,created_at,unreferenced_at,encryption_algorithm,block_key_id,sync_pending,boot_id) VALUES($1,$2,$3,$4,$5,$6,$10,'uploading',$7,now(),$8,$9,TRUE,$11) ON CONFLICT(encoding_id) DO UPDATE SET sync_pending=TRUE,boot_id=EXCLUDED.boot_id WHERE chunk_encodings.state='uploading'").bind(chunk.encoding_id).bind(chunk.id).bind(chunk.storage_id).bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(chunk.created_at).bind(&chunk.algorithm).bind(chunk.block_key_id).bind(crate::content::nonce::id(chunk.nonce.as_deref())).bind(boot)
}

pub(in super::super) fn release_memory_pins(boot: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM cache_pins WHERE chunk_id IN (SELECT c.id FROM chunks c JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id WHERE l.boot_id<>$1 AND l.sync_pending AND l.state IN ('ready','uploading') AND l.quarantined_at IS NULL)").bind(boot)
}

pub(in super::super) fn release_memory_chunks(boot: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM pending_uploads WHERE chunk_id IN (SELECT c.id FROM chunks c JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id WHERE l.boot_id<>$1 AND l.sync_pending AND l.state IN ('ready','uploading') AND l.quarantined_at IS NULL)").bind(boot)
}

pub(in super::super) fn retire_memory_chunks(boot: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',sync_pending=FALSE,boot_id=NULL,unreferenced_at=clock_timestamp() WHERE boot_id<>$1 AND sync_pending AND state IN ('ready','uploading') AND quarantined_at IS NULL AND encoding_id IN (SELECT c.current_encoding_id FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE p.state='ready' AND p.quarantined_at IS NULL)").bind(boot)
}

pub(in super::super) fn quarantine_memory_chunks(boot: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET quarantine_code='remote_missing',quarantined_at=clock_timestamp(),quarantine_revision=quarantine_revision+1 WHERE boot_id<>$1 AND sync_pending AND state IN ('ready','uploading') AND quarantined_at IS NULL AND encoding_id IN (SELECT current_encoding_id FROM chunks)").bind(boot)
}

pub(in super::super) fn release_memory_raw(boot: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_uploads WHERE source_id IN (SELECT id FROM raw_sources WHERE durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL AND medium='memory' AND boot_id<>$1)").bind(boot)
}

pub(in super::super) fn release_memory_processing(boot: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_processing WHERE source_id IN (SELECT id FROM raw_sources WHERE durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL AND medium='memory' AND boot_id<>$1)").bind(boot)
}
