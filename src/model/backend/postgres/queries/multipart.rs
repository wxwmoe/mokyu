use super::*;

pub(in super::super) fn multipart_upload<'q>(
    id: Uuid,
    bucket: Uuid,
    key: &'q str,
    access: &'q str,
) -> Rows<'q, Upload> {
    sqlx::query_as("SELECT * FROM uploads WHERE id=$1 AND bucket_id=$2 AND object_key=$3 AND access_key=$4 AND state<>'aborted'").bind(id).bind(bucket).bind(key).bind(access)
}

pub(in super::super) fn multipart_bucket(bucket: Uuid) -> Rows<'static, (String, bool)> {
    sqlx::query_as("SELECT state,uploads_paused FROM buckets WHERE id=$1 FOR SHARE").bind(bucket)
}

pub(in super::super) fn web_upload_identity(
    user: Uuid,
    client: Uuid,
) -> Rows<'static, (Uuid, Vec<u8>)> {
    sqlx::query_as(
        "SELECT upload_id,request_hash FROM web_uploads WHERE user_id=$1 AND client_id=$2",
    )
    .bind(user)
    .bind(client)
}

pub(in super::super) fn web_upload_expected<'q>(
    bucket: Uuid,
    key: &'q str,
) -> Scalar<'q, Option<Id>> {
    sqlx::query_scalar("SELECT stream_id FROM objects WHERE bucket_id=$1 AND object_key=$2")
        .bind(bucket)
        .bind(key)
}

pub(in super::super) fn lock_upload_admission() -> Statement<'static> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('mokyu:uploads:max-active', 0))")
}

pub(in super::super) fn active_upload_count() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM uploads WHERE state IN ('active','completing')")
}

pub(in super::super) fn insert_multipart<'q>(input: &'q MultipartCreate<'_>) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("INSERT INTO uploads(bucket_id,object_key,access_key,metadata,public_read,checksum_algorithm,checksum_type) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id").bind(input.bucket).bind(input.key).bind(input.access).bind(&input.metadata).bind(input.public).bind(&input.algorithm).bind(&input.checksum_type)
}

pub(in super::super) fn insert_web_upload<'q>(
    id: Uuid,
    web: &'q WebUpload,
    expected: Option<Id>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO web_uploads(upload_id,user_id,client_id,request_hash,file_name,expected_size,part_size,modified_at,expected_stream_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)").bind(id).bind(web.user).bind(web.client_id).bind(&web.request_hash).bind(&web.file_name).bind(web.size).bind(web.part_size).bind(web.modified_at).bind(expected)
}

pub(in super::super) fn lock_upload_state(upload: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT CASE WHEN remote_manifest_hash IS NOT NULL AND state='active' THEN 'completing' ELSE state END FROM uploads WHERE id=$1 FOR UPDATE").bind(upload)
}

pub(in super::super) fn begin_upload_part(
    upload: Uuid,
    number: i32,
    stream: Id,
) -> Statement<'static> {
    sqlx::query("INSERT INTO parts(upload_id,part_number,write_token) VALUES($1,$2,$3) ON CONFLICT(upload_id,part_number) DO UPDATE SET write_token=excluded.write_token").bind(upload).bind(number).bind(stream)
}

pub(in super::super) fn multipart_bucket_state(bucket: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT state FROM buckets WHERE id=$1 FOR SHARE").bind(bucket)
}

pub(in super::super) fn lock_upload_part(
    upload: Uuid,
    number: i32,
) -> Rows<'static, (Option<Id>, Id)> {
    sqlx::query_as(
        "SELECT stream_id,write_token FROM parts WHERE upload_id=$1 AND part_number=$2 FOR UPDATE",
    )
    .bind(upload)
    .bind(number)
}

pub(in super::super) fn publish_upload_part(
    upload: Uuid,
    number: i32,
    stream: Id,
) -> Statement<'static> {
    sqlx::query("UPDATE parts SET stream_id=$3 WHERE upload_id=$1 AND part_number=$2")
        .bind(upload)
        .bind(number)
        .bind(stream)
}

pub(in super::super) fn ready_upload_stream(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='ready' WHERE id=$1").bind(stream)
}

pub(in super::super) fn retire_upload_stream(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1").bind(stream)
}

pub(in super::super) fn touch_upload(upload: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET touched_at=now() WHERE id=$1").bind(upload)
}

pub(in super::super) fn upload_active(upload: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT state='active' FROM uploads WHERE id=$1").bind(upload)
}

pub(in super::super) fn upload_part(
    upload: Uuid,
    number: i32,
) -> Rows<'static, crate::app::StoredStream> {
    sqlx::query_as("SELECT s.* FROM parts p JOIN streams s ON p.stream_id=s.id WHERE p.upload_id=$1 AND p.part_number=$2 AND s.state='ready'").bind(upload).bind(number)
}

pub(in super::super) fn upload_stream_tail(stream: Id) -> Rows<'static, Extent> {
    sqlx::query_as("SELECT * FROM extents WHERE stream_id=$1 ORDER BY offset_bytes DESC LIMIT 1")
        .bind(stream)
}

pub(in super::super) fn lock_upload_active(upload: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT state='active' FROM uploads WHERE id=$1 FOR SHARE").bind(upload)
}

pub(in super::super) fn remove_upload_extent(stream: Id, offset: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM extents WHERE stream_id=$1 AND offset_bytes=$2")
        .bind(stream)
        .bind(offset)
}

pub(in super::super) fn slice_upload_extent(
    stream: Id,
    offset: i64,
    prefix: i32,
    length: i32,
) -> Statement<'static> {
    sqlx::query(
        "UPDATE extents SET source_offset=$3,length=$4 WHERE stream_id=$1 AND offset_bytes=$2",
    )
    .bind(stream)
    .bind(offset)
    .bind(prefix)
    .bind(length)
}

pub(in super::super) fn resize_upload_stream(stream: Id, size: i64) -> Statement<'static> {
    sqlx::query("UPDATE streams SET size=$2 WHERE id=$1")
        .bind(stream)
        .bind(size)
}

pub(in super::super) fn active_upload_metadata(upload: Uuid) -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT metadata FROM uploads WHERE id=$1 AND state='active'").bind(upload)
}

pub(in super::super) fn upload_part_pair(
    upload: Uuid,
    number: i32,
) -> Rows<'static, crate::app::StoredStream> {
    sqlx::query_as("SELECT s.* FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=$1 AND p.part_number IN ($2,$2+1) AND s.state='ready' ORDER BY p.part_number").bind(upload).bind(number)
}

pub(in super::super) fn begin_upload_completion<'a>(
    input: &'a crate::model::multipart::Completion<'a>,
) -> Statement<'a> {
    sqlx::query("UPDATE uploads SET state='completing',manifest_hash=$2,completion_token=$3,completion_expires_at=clock_timestamp()+interval '120 seconds',touched_at=clock_timestamp() WHERE id=$1").bind(input.upload).bind(input.hash).bind(input.token)
}

pub(in super::super) fn abort_multipart(id: Uuid, expired: bool) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET state='aborted',touched_at=now() WHERE id=$1 AND state IN ('active','completing') AND output_stream IS NULL AND (NOT $2 OR (state='active' AND NOT EXISTS(SELECT 1 FROM parts part JOIN stream_leases l ON l.stream_id=part.write_token WHERE part.upload_id=uploads.id AND l.expires_at>now()) AND EXISTS(SELECT 1 FROM buckets b JOIN storage_profiles p ON p.storage_id=b.storage_id WHERE b.id=uploads.bucket_id AND uploads.touched_at<now()-(left(p.multipart_idle_timeout,-1)::bigint*CASE right(p.multipart_idle_timeout,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END)*interval '1 second')))").bind(id).bind(expired)
}

pub(in super::super) fn upload_bucket(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT bucket_id FROM uploads WHERE id=$1").bind(id)
}

pub(in super::super) fn upload_parts_page(
    upload: Uuid,
    after: i32,
    limit: i64,
) -> Rows<'static, UploadPart> {
    sqlx::query_as("SELECT p.part_number,s.size,s.etag,s.touched_at,s.checksums FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=$1 AND p.part_number>$2 ORDER BY p.part_number LIMIT $3").bind(upload).bind(after).bind(limit)
}

pub(in super::super) fn multipart_page<'q>(
    bucket: Uuid,
    access: &'q str,
    prefix: &'q str,
    after: &'q str,
    inclusive: bool,
    id: Option<Uuid>,
) -> Rows<'q, Upload> {
    sqlx::query_as("SELECT * FROM uploads WHERE bucket_id=$1 AND access_key=$2 AND state IN ('active','completing') AND starts_with(object_key,$3) AND (object_key>$4 OR (object_key=$4 AND ($5 OR id>$6))) ORDER BY object_key,id LIMIT 64").bind(bucket).bind(access).bind(prefix).bind(after).bind(inclusive).bind(id)
}

pub(in super::super) fn lock_upload_extents(
    stream: Id,
    offset: i64,
    end: i64,
) -> Rows<'static, Extent> {
    sqlx::query_as("SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=GREATEST(0,$2-4194304) AND offset_bytes<$3 AND offset_bytes+length>$2 ORDER BY offset_bytes FOR UPDATE").bind(stream).bind(offset).bind(end)
}

pub(in super::super) fn insert_upload_chunk_extent(
    stream: Id,
    offset: i64,
    length: i32,
    chunk: Id,
    source: i32,
) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) VALUES($1,$2,$3,$4,$5)").bind(stream).bind(offset).bind(length).bind(chunk).bind(source)
}

pub(in super::super) fn insert_upload_extent<'q>(
    e: &'q Extent,
    offset: i64,
    length: i32,
    source: i32,
) -> Statement<'q> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,fragment_id,source_offset) VALUES($1,$2,$3,$4,$5,$6)").bind(e.stream_id).bind(offset).bind(length).bind(e.chunk_id).bind(e.fragment_id).bind(source)
}

pub(in super::super) fn touch_active_upload(upload: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET touched_at=now() WHERE id=$1 AND state='active'").bind(upload)
}

pub(in super::super) fn durable_uploads<'q>(ids: &'q [Uuid]) -> Statement<'q> {
    sqlx::query("UPDATE uploads SET durable_at=now() WHERE id=ANY($1) AND durable_at IS NULL")
        .bind(ids)
}

pub(in super::super) fn web_upload_bucket_name(bucket: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT name FROM buckets WHERE id=$1").bind(bucket)
}

pub(in super::super) fn web_upload_parts(upload: Uuid, after: i32) -> Rows<'static, ReceivedPart> {
    sqlx::query_as("SELECT p.part_number number,s.size::text,s.etag,s.checksums->>'sha256' sha256 FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=$1 AND p.part_number>$2 ORDER BY p.part_number LIMIT 1001").bind(upload).bind(after)
}

pub(in super::super) fn mark_unreferenced_chunks(chunks: &[Id]) -> Statement<'_> {
    sqlx::query("UPDATE chunks c SET unreferenced_at=COALESCE(unreferenced_at,now()) WHERE id=ANY($1) AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)").bind(chunks)
}

const TRANSFER_VIEW: &str = "SELECT u.id,u.bucket_id,b.name bucket_name,u.object_key,u.state,CASE WHEN w.upload_id IS NULL THEN 's3' ELSE 'web' END source,w.user_id,COALESCE(NULLIF(a.display_name,''),NULLIF(a.username,''),c.label,'S3 application') owner,w.file_name,w.expected_size::text,w.part_size::text,w.modified_at,CASE WHEN u.state='completed' THEN COALESCE(s.size,w.expected_size,0) ELSE COALESCE((SELECT sum(quota_size) FROM parts WHERE upload_id=u.id),0) END::text received_bytes,(SELECT count(*) FROM parts WHERE upload_id=u.id) parts,CASE WHEN u.state<>'completed' THEN 'pending' WHEN u.durable_at IS NOT NULL THEN 'stored' WHEN u.output_stream IS NULL THEN 'unavailable' WHEN EXISTS(SELECT 1 FROM extents e JOIN chunks ch ON ch.id=e.chunk_id WHERE e.stream_id=u.output_stream AND NOT EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=ch.id AND l.state='ready') AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.id=ch.pack_id AND p.state='ready')) THEN 'pending' ELSE 'stored' END remote_state,u.created_at,u.touched_at,p.multipart_idle_timeout FROM uploads u JOIN buckets b ON b.id=u.bucket_id JOIN storage_profiles p ON p.storage_id=b.storage_id LEFT JOIN web_uploads w ON w.upload_id=u.id LEFT JOIN web_users a ON a.id=w.user_id LEFT JOIN s3_credentials c ON c.access_key_id=u.access_key LEFT JOIN streams s ON s.id=u.output_stream WHERE ";

pub(in super::super) fn upload_transfers(
    admin: bool,
    user: Uuid,
    token: Option<Uuid>,
    q: &TransferFilter,
    cursor: Option<(DateTime<Utc>, Uuid)>,
    id: Option<Uuid>,
    limit: i64,
) -> sqlx::QueryBuilder<Postgres> {
    let mut sql = sqlx::QueryBuilder::<Postgres>::new(TRANSFER_VIEW);
    if admin {
        sql.push("true");
    } else {
        if let Some(token) = token {
            sql.push("EXISTS(SELECT 1 FROM token_bucket_access g WHERE g.token_id=")
                .push_bind(token);
        } else {
            sql.push("EXISTS(SELECT 1 FROM user_bucket_access g WHERE g.user_id=")
                .push_bind(user);
        }
        sql.push(" AND g.bucket_id=u.bucket_id AND 'object.write'=ANY(g.actions) AND (w.user_id=")
            .push_bind(user)
            .push(" OR 'bucket.settings'=ANY(g.actions)))");
    }
    if let Some(id) = id {
        sql.push(" AND u.id=").push_bind(id);
    } else {
        match q.state.as_deref() {
            Some("all") => {}
            Some(state) => {
                sql.push(" AND u.state=").push_bind(state);
            }
            None => {
                sql.push(" AND u.state IN ('active','completing')");
            }
        }
    }
    if let Some(bucket) = q.bucket {
        sql.push(" AND u.bucket_id=").push_bind(bucket);
    }
    if let Some(source) = &q.source {
        sql.push(if source == "web" {
            " AND w.upload_id IS NOT NULL"
        } else {
            " AND w.upload_id IS NULL"
        });
    }
    if q.own == Some(true) {
        sql.push(" AND w.user_id=").push_bind(user);
    }
    if let Some(cursor) = cursor {
        sql.push(" AND (u.created_at,u.id)<(")
            .push_bind(cursor.0)
            .push(",")
            .push_bind(cursor.1)
            .push(")");
    }
    sql.push(" ORDER BY u.created_at DESC,u.id DESC LIMIT ")
        .push_bind(limit + 1);
    sql
}

#[async_trait::async_trait]
impl MultipartStore for Backend {
    async fn pending_files(
        &self,
        storage: Option<Uuid>,
        node: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<crate::model::multipart::MultipartFile>> {
        Ok(sqlx::query_as("SELECT id,upload_id,storage_id,source_id,state FROM multipart_files WHERE node_id IS NOT DISTINCT FROM $3 AND reclaim_pending AND state='sealed' AND ($1::uuid IS NULL OR storage_id=$1) ORDER BY id LIMIT $2").bind(storage).bind(limit).bind(node).fetch_all(&self.pool).await?)
    }
    async fn clear_reclaim(&self, id: Uuid) -> Result<()> {
        sqlx::query(
            "UPDATE multipart_files SET reclaim_pending=false WHERE id=$1 AND state='sealed'",
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
            "SELECT id,upload_id,storage_id,source_id,state FROM multipart_files WHERE id=$1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?)
    }
    async fn file_ranges(&self, id: Uuid) -> Result<Vec<(i64, i32)>> {
        Ok(sqlx::query_as("SELECT file_offset,size FROM fragments WHERE file_id=$1 ORDER BY file_offset LIMIT 4097").bind(id).fetch_all(&self.pool).await?)
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
        sqlx::query(
            "UPDATE multipart_files SET state='sealed' WHERE upload_id=$1 AND state='open'",
        )
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
        Ok(sqlx::query_as("SELECT e.offset_bytes,e.length,e.source_offset,CASE WHEN f.staging_storage_id IS NULL THEN e.fragment_id END AS fragment_id,f.id AS fragment_location,f.size,f.hash,f.file_id,f.file_offset,m.storage_id AS file_storage,CASE WHEN m.source_id IS NOT NULL THEN m.id END AS raw_source FROM extents e LEFT JOIN fragments f ON f.id=e.fragment_id LEFT JOIN multipart_files m ON m.id=f.file_id WHERE e.stream_id=$1 AND e.offset_bytes>=$2 ORDER BY e.offset_bytes LIMIT 64").bind(stream).bind(after).fetch_all(&self.pool).await?)
    }
    async fn unused_files(
        &self,
        storage: Option<Uuid>,
        node: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<crate::model::multipart::MultipartFile>> {
        Ok(sqlx::query_as("SELECT f.id,f.upload_id,f.storage_id,f.source_id,f.state FROM multipart_files f WHERE f.node_id IS NOT DISTINCT FROM $3 AND ($1::uuid IS NULL OR storage_id=$1) AND NOT EXISTS(SELECT 1 FROM fragments WHERE file_id=f.id) AND (f.state<>'open' OR NOT EXISTS(SELECT 1 FROM uploads WHERE id=f.upload_id AND state IN ('active','completing'))) ORDER BY f.id LIMIT $2").bind(storage).bind(limit).bind(node).fetch_all(&self.pool).await?)
    }
    async fn claim_file(&self, id: Uuid) -> Result<bool> {
        Ok(sqlx::query("UPDATE multipart_files AS f SET state='deleting' WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM fragments WHERE file_id=f.id) AND (f.state<>'open' OR NOT EXISTS(SELECT 1 FROM uploads WHERE id=f.upload_id AND state IN ('active','completing')))").bind(id).execute(&self.pool).await?.rows_affected()>0)
    }
    async fn remove_file(&self, id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM multipart_files WHERE id=$1 AND state='deleting'")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn upload(&self, id: Uuid, bucket: Uuid, key: &str, access: &str) -> Result<Upload> {
        Ok(multipart_upload(id, bucket, key, access)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchUpload))?)
    }
    async fn active(&self, upload: Uuid) -> Result<bool> {
        Ok(upload_active(upload).fetch_one(&self.pool).await?)
    }
    async fn part(&self, upload: Uuid, number: i32) -> Result<Option<crate::app::StoredStream>> {
        Ok(upload_part(upload, number)
            .fetch_optional(&self.pool)
            .await?)
    }
    async fn tail(&self, stream: Id) -> Result<Option<Extent>> {
        Ok(upload_stream_tail(stream)
            .fetch_optional(&self.pool)
            .await?)
    }
    async fn pair(
        &self,
        upload: Uuid,
        number: i32,
    ) -> Result<Option<(Value, Vec<crate::app::StoredStream>)>> {
        let Some(metadata) = active_upload_metadata(upload)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some((
            metadata,
            upload_part_pair(upload, number)
                .fetch_all(&self.pool)
                .await?,
        )))
    }
    async fn retire_work(&self, stream: Id) -> Result<()> {
        retire_upload_stream(stream).execute(&self.pool).await?;
        Ok(())
    }
    async fn retry_complete(&self, upload: Uuid, token: Uuid) -> Result<()> {
        sqlx::query("UPDATE uploads SET state='active',completion_token=NULL,completion_expires_at=NULL,touched_at=clock_timestamp() WHERE id=$1 AND completion_token=$2 AND state='completing' AND output_stream IS NULL").bind(upload).bind(token).execute(&self.pool).await?;
        Ok(())
    }
    async fn renew_complete(
        &self,
        upload: Uuid,
        token: Uuid,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::renew_completion(&self.pool, upload, token, node).await
    }
    async fn parts(&self, upload: Uuid, after: i32, limit: i64) -> Result<Vec<UploadPart>> {
        Ok(upload_parts_page(upload, after, limit)
            .fetch_all(&self.pool)
            .await?)
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
        Ok(multipart_page(bucket, access, prefix, after, inclusive, id)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn touch(&self, upload: Uuid) -> Result<()> {
        if touch_active_upload(upload)
            .execute(&self.pool)
            .await?
            .rows_affected()
            != 1
        {
            return Err(s3s::s3_error!(NoSuchUpload).into());
        }
        Ok(())
    }
    async fn bucket_name(&self, bucket: Uuid) -> Result<String> {
        Ok(web_upload_bucket_name(bucket).fetch_one(&self.pool).await?)
    }
    async fn web_parts(&self, upload: Uuid, after: i32) -> Result<Vec<ReceivedPart>> {
        Ok(web_upload_parts(upload, after)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn create(&self, input: MultipartCreate<'_>) -> Result<(Uuid, bool)> {
        transactions::create_multipart(&self.pool, input).await
    }
    async fn begin_part(
        &self,
        authority: &Permit,
        upload: Uuid,
        number: i32,
        stream: Id,
    ) -> Result<()> {
        transactions::begin_upload_part(&self.pool, authority, upload, number, stream).await
    }
    async fn commit_part(
        &self,
        authority: &Permit,
        bucket: Uuid,
        upload: Uuid,
        number: i32,
        stream: Id,
    ) -> Result<()> {
        transactions::commit_upload_part(
            &self.pool, authority, bucket, upload, number, stream, None,
        )
        .await
    }
    async fn consume_seed(&self, input: SeedConsumption<'_>) -> Result<()> {
        transactions::consume_upload_seed(&self.pool, input).await
    }
    async fn stitch(&self, input: PartStitch) -> Result<()> {
        transactions::stitch_upload_parts(&self.pool, input).await
    }
    async fn begin_complete(&self, input: crate::model::multipart::Completion<'_>) -> Result<bool> {
        transactions::begin_upload_completion(&self.pool, input).await
    }
    async fn abort(&self, authority: &Permit, upload: Uuid) -> Result<()> {
        transactions::abort_multipart(&self.pool, authority, upload, false)
            .await
            .map(|_| ())
    }
    async fn expire(&self, upload: Uuid) -> Result<bool> {
        transactions::abort_multipart(&self.pool, &Permit::system(), upload, true).await
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
}
