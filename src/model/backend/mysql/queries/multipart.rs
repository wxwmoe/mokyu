use super::*;

pub(in super::super) fn web_upload_identity(
    user: Uuid,
    client: Uuid,
) -> Rows<'static, (Uuid, Vec<u8>)> {
    sqlx::query_as("SELECT upload_id,request_hash FROM web_uploads WHERE user_id=? AND client_id=?")
        .bind(user)
        .bind(client)
}

pub(in super::super) fn web_upload_expected(bucket: Uuid, key: &str) -> Scalar<'_, Option<Id>> {
    sqlx::query_scalar("SELECT stream_id FROM objects WHERE bucket_id=? AND object_key=?")
        .bind(bucket)
        .bind(key.as_bytes())
}

pub(in super::super) fn active_upload_count() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT COUNT(*) FROM uploads WHERE state IN ('active','completing')")
}

pub(in super::super) fn insert_multipart<'a>(
    id: Uuid,
    input: &'a MultipartCreate<'_>,
) -> Statement<'a> {
    sqlx::query("INSERT INTO uploads(id,bucket_id,object_key,access_key,metadata,public_read,checksum_algorithm,checksum_type) VALUES(?,?,?,?,?,?,?,?)").bind(id).bind(input.bucket).bind(input.key.as_bytes()).bind(input.access).bind(&input.metadata).bind(input.public).bind(&input.algorithm).bind(&input.checksum_type)
}

pub(in super::super) fn insert_web_upload(
    id: Uuid,
    web: &WebUpload,
    expected: Option<Id>,
) -> Statement<'_> {
    sqlx::query("INSERT INTO web_uploads(upload_id,user_id,client_id,request_hash,file_name,expected_size,part_size,modified_at,expected_stream_id) VALUES(?,?,?,?,?,?,?,?,?)").bind(id).bind(web.user).bind(web.client_id).bind(&web.request_hash).bind(&web.file_name).bind(web.size).bind(web.part_size).bind(web.modified_at).bind(expected)
}

pub(in super::super) fn lock_upload(upload: Uuid) -> Rows<'static, Upload> {
    sqlx::query_as("SELECT * FROM uploads WHERE id=? FOR UPDATE").bind(upload)
}

pub(in super::super) fn lock_upload_part(
    upload: Uuid,
    number: i32,
) -> Rows<'static, (Option<Id>, Id, i64)> {
    sqlx::query_as("SELECT stream_id,write_token,quota_size FROM parts WHERE upload_id=? AND part_number=? FOR UPDATE").bind(upload).bind(number)
}

pub(in super::super) fn begin_upload_part(
    upload: Uuid,
    number: i32,
    stream: Id,
) -> Statement<'static> {
    sqlx::query("INSERT INTO parts(upload_id,part_number,write_token) VALUES(?,?,?) ON DUPLICATE KEY UPDATE write_token=?").bind(upload).bind(number).bind(stream).bind(stream)
}

pub(in super::super) fn publish_upload_part(
    upload: Uuid,
    number: i32,
    stream: Id,
    size: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE parts SET stream_id=?,quota_size=? WHERE upload_id=? AND part_number=? AND write_token=?").bind(stream).bind(size).bind(upload).bind(number).bind(stream)
}

pub(in super::super) fn touch_upload(upload: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET touched_at=CURRENT_TIMESTAMP(6) WHERE id=?").bind(upload)
}

pub(in super::super) fn begin_upload_completion<'a>(
    input: &'a crate::model::multipart::Completion<'a>,
) -> Statement<'a> {
    sqlx::query("UPDATE uploads SET state='completing',manifest_hash=?,completion_token=?,completion_expires_at=DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL 120 SECOND),touched_at=CURRENT_TIMESTAMP(6) WHERE id=?").bind(input.hash).bind(input.token).bind(input.upload)
}

pub(in super::super) fn abort_multipart(id: Uuid, expired: bool) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET state='aborted',touched_at=CURRENT_TIMESTAMP(6) WHERE id=? AND state IN ('active','completing') AND output_stream IS NULL AND (NOT ? OR (state='active' AND EXISTS(SELECT 1 FROM buckets b JOIN storage_profiles p ON p.storage_id=b.storage_id WHERE b.id=uploads.bucket_id AND uploads.touched_at<TIMESTAMPADD(SECOND,-(CAST(LEFT(p.multipart_idle_timeout,LENGTH(p.multipart_idle_timeout)-1) AS UNSIGNED)*CASE RIGHT(p.multipart_idle_timeout,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END),CURRENT_TIMESTAMP(6)))))").bind(id).bind(expired)
}

pub(in super::super) fn lock_upload_active(upload: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT state='active' FROM uploads WHERE id=? LOCK IN SHARE MODE")
        .bind(upload)
}

pub(in super::super) fn remove_upload_extent(stream: Id, offset: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM extents WHERE stream_id=? AND offset_bytes=?")
        .bind(stream)
        .bind(offset)
}

pub(in super::super) fn slice_upload_extent(
    stream: Id,
    offset: i64,
    prefix: i32,
    length: i32,
) -> Statement<'static> {
    sqlx::query("UPDATE extents SET source_offset=?,length=? WHERE stream_id=? AND offset_bytes=?")
        .bind(prefix)
        .bind(length)
        .bind(stream)
        .bind(offset)
}

pub(in super::super) fn resize_upload_stream(stream: Id, size: i64) -> Statement<'static> {
    sqlx::query("UPDATE streams SET size=? WHERE id=?")
        .bind(size)
        .bind(stream)
}

pub(in super::super) fn lock_upload_extents(
    stream: Id,
    offset: i64,
    end: i64,
) -> Rows<'static, Extent> {
    sqlx::query_as("SELECT * FROM extents WHERE stream_id=? AND offset_bytes>=GREATEST(0,?-4194304) AND offset_bytes<? AND offset_bytes+length>? ORDER BY offset_bytes FOR UPDATE").bind(stream).bind(offset).bind(end).bind(offset)
}

pub(in super::super) fn insert_upload_chunk_extent(
    stream: Id,
    offset: i64,
    length: i32,
    chunk: Id,
    source: i32,
) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) VALUES(?,?,?,?,?)").bind(stream).bind(offset).bind(length).bind(chunk).bind(source)
}

pub(in super::super) fn insert_upload_extent(
    extent: &Extent,
    offset: i64,
    length: i32,
    source: i32,
) -> Statement<'_> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,fragment_id,source_offset) VALUES(?,?,?,?,?,?)").bind(extent.stream_id).bind(offset).bind(length).bind(extent.chunk_id).bind(extent.fragment_id).bind(source)
}

pub(in super::super) async fn mark_unreferenced_chunks(
    connection: &mut sqlx::MySqlConnection,
    chunks: &[Id],
) -> Result<()> {
    changed_references(connection, chunks, false).await
}

pub(in super::super) async fn changed_references(
    connection: &mut sqlx::MySqlConnection,
    chunks: &[Id],
    added: bool,
) -> Result<()> {
    if chunks.is_empty() {
        return Ok(());
    }
    let mut query = sqlx::QueryBuilder::new(
        "UPDATE chunks c SET unreferenced_at=IF(EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id),NULL,COALESCE(unreferenced_at,CURRENT_TIMESTAMP(6))),reference_changed_at=CURRENT_TIMESTAMP(6) WHERE id IN ",
    );
    chunk_ids(&mut query, chunks);
    query.build().execute(&mut *connection).await?;
    if added {
        return Ok(());
    }
    let mut query = sqlx::QueryBuilder::new(
        "INSERT INTO pack_changes(pack_id,reason) SELECT DISTINCT pack_id,",
    );
    query
        .push("'reclaim'")
        .push(" FROM chunks WHERE pack_id IS NOT NULL AND id IN ");
    chunk_ids(&mut query, chunks);
    query.push(" ON DUPLICATE KEY UPDATE generation=pack_changes.generation+1,next_check_at=CURRENT_TIMESTAMP(6),reason='reclaim'");
    query.build().execute(&mut *connection).await?;
    Ok(())
}

pub(in super::super) async fn ready_stream_queues(
    connection: &mut sqlx::MySqlConnection,
    stream: Id,
) -> Result<()> {
    sqlx::query("INSERT INTO pack_maintenance(stream_id) SELECT id FROM streams WHERE id=? AND state='ready' AND stream_type='object' AND (SELECT offset_bytes FROM extents WHERE stream_id=? AND chunk_id IS NOT NULL ORDER BY offset_bytes LIMIT 1 OFFSET 1) IS NOT NULL ON DUPLICATE KEY UPDATE generation=pack_maintenance.generation+1,progress_cursor=0,reason='pack',updated_at=CURRENT_TIMESTAMP(6),next_check_at=CURRENT_TIMESTAMP(6)").bind(stream).bind(stream).execute(&mut *connection).await?;
    Ok(())
}

pub(in super::super) async fn durable_uploads(
    connection: &mut sqlx::MySqlConnection,
    ids: &[Uuid],
) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let mut query = sqlx::QueryBuilder::new(
        "UPDATE uploads SET durable_at=CURRENT_TIMESTAMP(6) WHERE durable_at IS NULL AND id IN (",
    );
    let mut values = query.separated(",");
    for id in ids {
        values.push_bind(*id);
    }
    query.push(")");
    query.build().execute(connection).await?;
    Ok(())
}

const TRANSFER_VIEW: &str = "SELECT u.id,u.bucket_id,b.name bucket_name,u.object_key,u.state,IF(w.upload_id IS NULL,'s3','web') source,w.user_id,COALESCE(NULLIF(a.display_name,''),NULLIF(a.username,''),c.label,'S3 application') owner,w.file_name,CAST(w.expected_size AS CHAR) expected_size,CAST(w.part_size AS CHAR) part_size,w.modified_at,CAST(IF(u.state='completed',COALESCE(s.size,w.expected_size,0),COALESCE((SELECT SUM(quota_size) FROM parts WHERE upload_id=u.id),0)) AS CHAR) received_bytes,(SELECT COUNT(*) FROM parts WHERE upload_id=u.id) parts,CASE WHEN u.state<>'completed' THEN 'pending' WHEN u.durable_at IS NOT NULL THEN 'stored' WHEN u.output_stream IS NULL THEN 'unavailable' WHEN EXISTS(SELECT 1 FROM extents e JOIN chunks ch ON ch.id=e.chunk_id WHERE e.stream_id=u.output_stream AND NOT EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=ch.id AND l.state='ready') AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.id=ch.pack_id AND p.state='ready')) THEN 'pending' ELSE 'stored' END remote_state,u.created_at,u.touched_at,p.multipart_idle_timeout FROM uploads u JOIN buckets b ON b.id=u.bucket_id JOIN storage_profiles p ON p.storage_id=b.storage_id LEFT JOIN web_uploads w ON w.upload_id=u.id LEFT JOIN web_users a ON a.id=w.user_id LEFT JOIN s3_credentials c ON c.access_key_id=u.access_key LEFT JOIN streams s ON s.id=u.output_stream WHERE ";

pub(in super::super) fn upload_transfers(
    admin: bool,
    user: Uuid,
    token: Option<Uuid>,
    filter: &TransferFilter,
    after: Option<(DateTime<Utc>, Uuid)>,
    id: Option<Uuid>,
    limit: i64,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(TRANSFER_VIEW);
    if admin {
        query.push("TRUE");
    } else {
        if let Some(token) = token {
            query
                .push("EXISTS(SELECT 1 FROM token_bucket_access g WHERE g.token_id=")
                .push_bind(token);
        } else {
            query
                .push("EXISTS(SELECT 1 FROM user_bucket_access g WHERE g.user_id=")
                .push_bind(user);
        }
        query
            .push(" AND g.bucket_id=u.bucket_id AND (g.actions&4)<>0 AND (w.user_id=")
            .push_bind(user)
            .push(" OR (g.actions&32)<>0))");
    }
    if let Some(id) = id {
        query.push(" AND u.id=").push_bind(id);
    } else {
        match filter.state.as_deref() {
            Some("all") => {}
            Some(state) => {
                query.push(" AND u.state=").push_bind(state);
            }
            None => {
                query.push(" AND u.state IN ('active','completing')");
            }
        }
    }
    if let Some(bucket) = filter.bucket {
        query.push(" AND u.bucket_id=").push_bind(bucket);
    }
    if let Some(source) = &filter.source {
        query.push(if source == "web" {
            " AND w.upload_id IS NOT NULL"
        } else {
            " AND w.upload_id IS NULL"
        });
    }
    if filter.own == Some(true) {
        query.push(" AND w.user_id=").push_bind(user);
    }
    if let Some((time, id)) = after {
        query
            .push(" AND (u.created_at,u.id)<(")
            .push_bind(time)
            .push(",")
            .push_bind(id)
            .push(")");
    }
    query
        .push(" ORDER BY u.created_at DESC,u.id DESC LIMIT ")
        .push_bind(limit + 1);
    query
}

#[async_trait::async_trait]
impl MultipartStore for Backend {
    async fn pending_files(
        &self,
        storage: Option<Uuid>,
        _node: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<crate::model::multipart::MultipartFile>> {
        Ok(sqlx::query_as("SELECT id,upload_id,storage_id,source_id,state FROM multipart_files WHERE reclaim_pending AND state='sealed' AND (? IS NULL OR storage_id=?) ORDER BY id LIMIT ?").bind(storage).bind(storage).bind(limit).fetch_all(&self.pool).await?)
    }
    async fn clear_reclaim(&self, id: Uuid) -> Result<()> {
        sqlx::query(
            "UPDATE multipart_files SET reclaim_pending=false WHERE id=? AND state='sealed'",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
    async fn file_location(
        &self,
        id: Uuid,
    ) -> Result<Option<crate::model::multipart::MultipartFile>> {
        Ok(sqlx::query_as(
            "SELECT id,upload_id,storage_id,source_id,state FROM multipart_files WHERE id=?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?)
    }
    async fn file_ranges(&self, id: Uuid) -> Result<Vec<(i64, i32)>> {
        Ok(sqlx::query_as("SELECT file_offset,size FROM fragments WHERE file_id=? ORDER BY file_offset LIMIT 4097").bind(id).fetch_all(&self.pool).await?)
    }
    async fn remap_file(
        &self,
        upload: Uuid,
        source: Id,
        ranges: &[crate::model::multipart::FileRemap],
    ) -> Result<()> {
        transactions::remap_multipart_file(&self.pool, upload, source, ranges).await
    }
    async fn file(&self, upload: Uuid, node: Option<Uuid>) -> Result<Uuid> {
        transactions::multipart_file(&self.pool, upload, node).await
    }
    async fn seal_files(&self, upload: Uuid) -> Result<()> {
        sqlx::query("UPDATE multipart_files SET state='sealed' WHERE upload_id=? AND part_stream IS NULL AND state='open'")
            .bind(upload)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn local_ranges(
        &self,
        stream: Id,
        after: i64,
    ) -> Result<Vec<crate::model::multipart::LocalPartRange>> {
        Ok(sqlx::query_as("SELECT e.offset_bytes,e.length,e.source_offset,e.fragment_id,f.location_id AS fragment_location,f.size,f.hash,f.staging_storage_id,f.staging_object_key,f.file_id,f.file_offset,m.storage_id AS file_storage,CASE WHEN m.source_id IS NOT NULL THEN m.id END AS raw_source FROM extents e LEFT JOIN fragments f ON f.id=e.fragment_id LEFT JOIN multipart_files m ON m.id=f.file_id WHERE e.stream_id=? AND e.offset_bytes>=? ORDER BY e.offset_bytes LIMIT 64").bind(stream).bind(after).fetch_all(&self.pool).await?)
    }
    async fn unused_files(
        &self,
        storage: Option<Uuid>,
        _node: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<crate::model::multipart::MultipartFile>> {
        Ok(sqlx::query_as("SELECT f.id,f.upload_id,f.storage_id,f.source_id,f.state FROM multipart_files f WHERE f.part_stream IS NULL AND (? IS NULL OR storage_id=?) AND NOT EXISTS(SELECT 1 FROM fragments WHERE file_id=f.id) AND (f.state<>'open' OR NOT EXISTS(SELECT 1 FROM uploads WHERE id=f.upload_id AND state IN ('active','completing'))) ORDER BY f.id LIMIT ?").bind(storage).bind(storage).bind(limit).fetch_all(&self.pool).await?)
    }
    async fn claim_file(&self, id: Uuid) -> Result<bool> {
        Ok(sqlx::query("UPDATE multipart_files AS f SET state='deleting' WHERE id=? AND f.part_stream IS NULL AND NOT EXISTS(SELECT 1 FROM fragments WHERE file_id=f.id) AND (f.state<>'open' OR NOT EXISTS(SELECT 1 FROM uploads WHERE id=f.upload_id AND state IN ('active','completing')))").bind(id).execute(&self.pool).await?.rows_affected()>0)
    }
    async fn remove_file(&self, id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM multipart_files WHERE id=? AND state='deleting'")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn upload(&self, id: Uuid, bucket: Uuid, key: &str, access: &str) -> Result<Upload> {
        sqlx::query_as("SELECT * FROM uploads WHERE id=? AND bucket_id=? AND object_key=? AND access_key=? AND state<>'aborted'").bind(id).bind(bucket).bind(key.as_bytes()).bind(access).fetch_optional(&self.pool).await?.ok_or_else(||s3s::s3_error!(NoSuchUpload).into())
    }
    async fn create(&self, input: MultipartCreate<'_>) -> Result<(Uuid, bool)> {
        let _admission = self.upload_admission.lock().await;
        transactions::create_multipart(&self.pool, input).await
    }
    async fn begin_part(
        &self,
        authority: &crate::authorization::Permit,
        upload: Uuid,
        number: i32,
        stream: Id,
    ) -> Result<()> {
        transactions::begin_upload_part(&self.pool, authority, upload, number, stream).await
    }
    async fn commit_part(
        &self,
        authority: &crate::authorization::Permit,
        bucket: Uuid,
        upload: Uuid,
        number: i32,
        stream: Id,
    ) -> Result<()> {
        transactions::commit_upload_part(&self.pool, authority, bucket, upload, number, stream)
            .await
    }
    async fn active(&self, upload: Uuid) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT state='active' FROM uploads WHERE id=?")
                .bind(upload)
                .fetch_one(&self.pool)
                .await?,
        )
    }
    async fn part(&self, upload: Uuid, number: i32) -> Result<Option<StoredStream>> {
        Ok(sqlx::query_as("SELECT s.* FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=? AND p.part_number=? AND s.state='ready'").bind(upload).bind(number).fetch_optional(&self.pool).await?)
    }
    async fn tail(&self, stream: Id) -> Result<Option<Extent>> {
        Ok(sqlx::query_as(
            "SELECT * FROM extents WHERE stream_id=? ORDER BY offset_bytes DESC LIMIT 1",
        )
        .bind(stream)
        .fetch_optional(&self.pool)
        .await?)
    }
    async fn pair(&self, upload: Uuid, number: i32) -> Result<Option<(Value, Vec<StoredStream>)>> {
        let Some(metadata) =
            sqlx::query_scalar("SELECT metadata FROM uploads WHERE id=? AND state='active'")
                .bind(upload)
                .fetch_optional(&self.pool)
                .await?
        else {
            return Ok(None);
        };
        let parts=sqlx::query_as("SELECT s.* FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=? AND p.part_number IN (?,?) AND s.state='ready' ORDER BY p.part_number").bind(upload).bind(number).bind(number+1).fetch_all(&self.pool).await?;
        Ok(Some((metadata, parts)))
    }
    async fn consume_seed(&self, input: SeedConsumption<'_>) -> Result<()> {
        transactions::consume_upload_seed(&self.pool, input).await
    }
    async fn stitch(&self, input: PartStitch) -> Result<()> {
        transactions::stitch_upload_parts(&self.pool, input).await
    }
    async fn retire_work(&self, stream: Id) -> Result<()> {
        transactions::retire_upload_work(&self.pool, stream).await
    }
    async fn begin_complete(&self, input: crate::model::multipart::Completion<'_>) -> Result<bool> {
        transactions::begin_upload_completion(&self.pool, input).await
    }
    async fn retry_complete(&self, upload: Uuid, token: Uuid) -> Result<()> {
        sqlx::query("UPDATE uploads SET state='active',completion_token=NULL,completion_expires_at=NULL,touched_at=CURRENT_TIMESTAMP(6) WHERE id=? AND completion_token=? AND state='completing' AND output_stream IS NULL").bind(upload).bind(token).execute(&self.pool).await?;
        Ok(())
    }
    async fn renew_complete(
        &self,
        upload: Uuid,
        token: Uuid,
        _node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        Ok(sqlx::query("UPDATE uploads SET completion_expires_at=DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL 120 SECOND),touched_at=CURRENT_TIMESTAMP(6) WHERE id=? AND completion_token=? AND state='completing' AND completion_expires_at>CURRENT_TIMESTAMP(6)").bind(upload).bind(token).execute(&self.pool).await?.rows_affected()==1)
    }
    async fn abort(&self, authority: &crate::authorization::Permit, upload: Uuid) -> Result<()> {
        transactions::abort_multipart(&self.pool, authority, upload, false)
            .await
            .map(|_| ())
    }
    async fn expire(&self, upload: Uuid) -> Result<bool> {
        transactions::abort_multipart(
            &self.pool,
            &crate::authorization::Permit::system(),
            upload,
            true,
        )
        .await
    }
    async fn parts(&self, upload: Uuid, after: i32, limit: i64) -> Result<Vec<UploadPart>> {
        Ok(sqlx::query_as("SELECT p.part_number,s.size,s.etag,s.touched_at,s.checksums FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=? AND p.part_number>? ORDER BY p.part_number LIMIT ?").bind(upload).bind(after).bind(limit).fetch_all(&self.pool).await?)
    }
    async fn uploads(
        &self,
        bucket: Uuid,
        access: &str,
        prefix: &str,
        after: &str,
        inclusive: bool,
        id: Option<Uuid>,
    ) -> Result<Vec<Upload>> {
        Ok(sqlx::query_as("SELECT * FROM uploads WHERE bucket_id=? AND access_key=? AND state IN ('active','completing') AND LEFT(object_key,OCTET_LENGTH(?))=? AND (object_key>? OR (object_key=? AND (? OR id>?))) ORDER BY object_key,id LIMIT 64").bind(bucket).bind(access).bind(prefix.as_bytes()).bind(prefix.as_bytes()).bind(after.as_bytes()).bind(after.as_bytes()).bind(inclusive).bind(id).fetch_all(&self.pool).await?)
    }
    async fn touch(&self, upload: Uuid) -> Result<()> {
        ensure!(
            sqlx::query(
                "UPDATE uploads SET touched_at=CURRENT_TIMESTAMP(6) WHERE id=? AND state='active'"
            )
            .bind(upload)
            .execute(&self.pool)
            .await?
            .rows_affected()
                == 1,
            s3s::s3_error!(NoSuchUpload)
        );
        Ok(())
    }
    async fn transfers(
        &self,
        principal: &Principal,
        user: Uuid,
        filter: &TransferFilter,
        after: Option<(DateTime<Utc>, Uuid)>,
        id: Option<Uuid>,
        limit: i64,
    ) -> Result<(Vec<Transfer>, bool)> {
        transactions::upload_transfers(&self.pool, principal, user, filter, after, id, limit).await
    }
    async fn bucket_name(&self, bucket: Uuid) -> Result<String> {
        Ok(sqlx::query_scalar("SELECT name FROM buckets WHERE id=?")
            .bind(bucket)
            .fetch_one(&self.pool)
            .await?)
    }
    async fn web_parts(&self, upload: Uuid, after: i32) -> Result<Vec<ReceivedPart>> {
        Ok(sqlx::query_as("SELECT p.part_number AS number,CAST(s.size AS CHAR) AS size,s.etag,JSON_UNQUOTE(JSON_EXTRACT(s.checksums,'$.sha256')) AS sha256 FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=? AND p.part_number>? ORDER BY p.part_number LIMIT 1001").bind(upload).bind(after).fetch_all(&self.pool).await?)
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for Upload {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            bucket_id: row.try_get("bucket_id")?,
            object_key: object_key(row)?,
            state: row.try_get("state")?,
            metadata: row.try_get("metadata")?,
            public_read: row.try_get("public_read")?,
            checksum_algorithm: row.try_get("checksum_algorithm")?,
            checksum_type: row.try_get("checksum_type")?,
            manifest_hash: row.try_get("manifest_hash")?,
            result: row.try_get("result")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for Transfer {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            bucket_id: row.try_get("bucket_id")?,
            bucket_name: row.try_get("bucket_name")?,
            object_key: object_key(row)?,
            state: row.try_get("state")?,
            source: row.try_get("source")?,
            user_id: row.try_get("user_id")?,
            owner: row.try_get("owner")?,
            file_name: row.try_get("file_name")?,
            expected_size: row.try_get("expected_size")?,
            part_size: row.try_get("part_size")?,
            modified_at: row.try_get("modified_at")?,
            received_bytes: row.try_get("received_bytes")?,
            parts: row.try_get("parts")?,
            remote_state: row.try_get("remote_state")?,
            created_at: row.try_get("created_at")?,
            touched_at: row.try_get("touched_at")?,
            multipart_idle_timeout: row.try_get("multipart_idle_timeout")?,
            expires_at: None,
            can_resume: false,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for ReceivedPart {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        Ok(Self {
            number: row.try_get("number")?,
            size: row.try_get("size")?,
            etag: row.try_get("etag")?,
            sha256: row.try_get("sha256")?,
        })
    }
}

#[derive(sqlx::FromRow)]
pub(in super::super) struct QuotaAccount {
    pub scope: String,
    pub scope_id: Uuid,
    pub used_bytes: i64,
    pub reserved_bytes: i64,
    pub inflight_bytes: i64,
    pub object_count: i64,
    pub bucket_count: i64,
    pub logical_bytes_limit: Option<i64>,
    pub inflight_bytes_limit: Option<i64>,
    pub bucket_count_limit: Option<i64>,
}
