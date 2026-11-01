use super::*;

pub(in super::super) fn integrity_upper_chunk(storage: Option<Uuid>) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT COALESCE(MAX(id),0) FROM chunks WHERE (? IS NULL OR storage_id=?)")
        .bind(storage)
        .bind(storage)
}

fn integrity_scope(query: &mut sqlx::QueryBuilder<MySql>, detail: &Detail) {
    if let Some(storage) = detail.storage_id {
        query
            .push(" AND EXISTS(SELECT 1 FROM buckets b WHERE b.id=o.bucket_id AND b.storage_id=")
            .push_bind(storage)
            .push(")");
    }
    if let Some(bucket) = detail.bucket_id {
        query.push(" AND o.bucket_id=").push_bind(bucket);
    }
    if let Some(key) = &detail.key {
        query
            .push(" AND o.object_key=")
            .push_bind(key.as_bytes().to_vec());
    }
    if let Some((bucket, key)) = &detail.upper_object {
        query
            .push(" AND (o.bucket_id<")
            .push_bind(*bucket)
            .push(" OR(o.bucket_id=")
            .push_bind(*bucket)
            .push(" AND o.object_key<=")
            .push_bind(key.as_bytes().to_vec())
            .push("))");
    }
}

pub(in super::super) async fn integrity_upper_object(
    connection: &mut sqlx::MySqlConnection,
    detail: &Detail,
) -> Result<Option<(Uuid, String)>> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT o.bucket_id,o.object_key FROM objects o WHERE o.stream_id IS NOT NULL",
    );
    integrity_scope(&mut query, detail);
    query.push(" ORDER BY o.bucket_id DESC,o.object_key DESC LIMIT 1");
    let row: Option<(Uuid, Vec<u8>)> = query.build_query_as().fetch_optional(connection).await?;
    row.map(|(bucket, key)| Ok((bucket, String::from_utf8(key)?)))
        .transpose()
}

pub(in super::super) fn insert_integrity_finding(task: Uuid, issue: &Finding) -> Statement<'_> {
    // The task row serializes checkpoints; INSERT SELECT preserves an exact inserted count.
    sqlx::query("INSERT INTO integrity_issues(task_id,subject,code,chunk_id,location_id,stream_id,bucket_id,object_key,detail) SELECT ?,?,?,?,?,?,?,?,? WHERE NOT EXISTS(SELECT 1 FROM integrity_issues WHERE task_id=? AND subject=? AND code=?)").bind(task).bind(&issue.subject).bind(&issue.code).bind(issue.chunk_id).bind(issue.location_id).bind(issue.stream_id).bind(issue.bucket_id).bind(issue.key.as_deref().map(str::as_bytes)).bind(&issue.detail).bind(task).bind(&issue.subject).bind(&issue.code)
}

pub(in super::super) fn integrity_checkpoint(
    task: Uuid,
    cursor: String,
    detail: Value,
    processed: i64,
    done: bool,
) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET progress_cursor=?,detail=?,processed=?,updated_at=CURRENT_TIMESTAMP(6),state=IF(?,'completed',state) WHERE id=?").bind(cursor).bind(detail).bind(processed).bind(done).bind(task)
}

fn integrity_finding_value(row: sqlx::mysql::MySqlRow) -> Result<Value> {
    Ok(serde_json::json!({
        "id": row.try_get::<i64,_>("id")?.to_string(),
        "task_id": row.try_get::<Uuid,_>("task_id")?,
        "subject": row.try_get::<String,_>("subject")?,
        "code": row.try_get::<String,_>("code")?,
        "chunk_id": row.try_get::<Option<i64>,_>("chunk_id")?.map(|id| id.to_string()),
        "location_id": row.try_get::<Option<Uuid>,_>("location_id")?,
        "stream_id": row.try_get::<Option<Id>,_>("stream_id")?,
        "bucket_id": row.try_get::<Option<Uuid>,_>("bucket_id")?,
        "object_key": row.try_get::<Option<Vec<u8>>,_>("object_key")?.map(String::from_utf8).transpose()?,
        "detail": row.try_get::<Value,_>("detail")?,
        "created_at": row.try_get::<DateTime<Utc>,_>("created_at")?,
    }))
}

#[async_trait::async_trait]
impl IntegrityStore for Backend {
    async fn start(
        &self,
        input: IntegrityRequest,
        bucket: Option<Bucket>,
        node: Option<Uuid>,
    ) -> Result<(Uuid, usize)> {
        ensure!(node.is_none(), "MySQL does not support cluster nodes");
        transactions::start_integrity(&self.pool, input, bucket).await
    }
    async fn chunks(&self, detail: &Detail, after: Id, limit: i64) -> Result<Vec<Chunk>> {
        let mut query = chunk_query(true);
        query.push(" WHERE c.state='ready' AND c.id>").push_bind(after).push(" AND c.id<=").push_bind(detail.upper_chunk_id.parse::<i64>()?).push(" AND EXISTS(SELECT 1 FROM extents e JOIN objects o ON o.stream_id=e.stream_id WHERE e.chunk_id=c.id");
        integrity_scope(&mut query, detail);
        query.push(") ORDER BY c.id LIMIT ").push_bind(limit);
        Ok(query.build_query_as().fetch_all(&self.pool).await?)
    }
    async fn mapping(
        &self,
        detail: &Detail,
        cursor: &Cursor,
    ) -> Result<Option<(StoredStream, Uuid, String)>> {
        let mut query = sqlx::QueryBuilder::<MySql>::new(
            "SELECT s.*,o.bucket_id AS visible_bucket,o.object_key AS visible_key FROM objects o JOIN streams s ON s.id=o.stream_id WHERE TRUE",
        );
        integrity_scope(&mut query, detail);
        if let Some(stream) = cursor.stream {
            query.push(" AND o.stream_id=").push_bind(stream);
        } else if let (Some(bucket), Some(key)) = (cursor.bucket, &cursor.key) {
            query
                .push(" AND (o.bucket_id>")
                .push_bind(bucket)
                .push(" OR(o.bucket_id=")
                .push_bind(bucket)
                .push(" AND o.object_key>")
                .push_bind(key.as_bytes().to_vec())
                .push("))");
        }
        query.push(" ORDER BY o.bucket_id,o.object_key LIMIT 1");
        let row = query.build().fetch_optional(&self.pool).await?;
        row.map(|r| {
            Ok((
                sqlx::FromRow::from_row(&r)?,
                r.try_get("visible_bucket")?,
                String::from_utf8(r.try_get("visible_key")?)?,
            ))
        })
        .transpose()
    }
    async fn ranges(&self, stream: Id, after: i64) -> Result<Vec<MappingRange>> {
        Ok(sqlx::query_as("SELECT e.offset_bytes,e.length,e.source_offset,e.fragment_id,e.chunk_id,c.state,c.raw_size FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=? AND e.offset_bytes>? ORDER BY e.offset_bytes LIMIT 65").bind(stream).bind(after).fetch_all(&self.pool).await?)
    }
    async fn current(&self, bucket: Uuid, key: &str, stream: Id) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM objects WHERE bucket_id=? AND object_key=? AND stream_id=?)").bind(bucket).bind(key.as_bytes()).bind(stream).fetch_one(&self.pool).await?)
    }
    async fn chunk_stream(&self, chunk: Id, detail: &Detail) -> Result<Option<Id>> {
        let mut query = sqlx::QueryBuilder::<MySql>::new(
            "SELECT o.stream_id FROM extents e JOIN objects o ON o.stream_id=e.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE c.state='ready' AND c.id=",
        );
        query.push_bind(chunk);
        integrity_scope(&mut query, detail);
        query.push(" ORDER BY o.bucket_id,o.object_key LIMIT 1");
        Ok(query
            .build_query_scalar()
            .fetch_optional(&self.pool)
            .await?)
    }
    async fn checkpoint(
        &self,
        task: Execution,
        cursor: String,
        detail: Detail,
        issues: Vec<Finding>,
    ) -> Result<()> {
        transactions::checkpoint_integrity(&self.pool, task, cursor, detail, issues).await
    }
    async fn exists(&self, task: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=? AND task_type='integrity')",
        )
        .bind(task)
        .fetch_one(&self.pool)
        .await?)
    }
    async fn report_task(&self, task: Uuid) -> Result<Option<Value>> {
        sqlx::query("SELECT * FROM tasks WHERE id=? AND task_type='integrity'")
            .bind(task)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| task_value(row, false))
            .transpose()
    }
    async fn report_upper(&self, task: Uuid) -> Result<Id> {
        Ok(
            sqlx::query_scalar("SELECT COALESCE(MAX(id),0) FROM integrity_issues WHERE task_id=?")
                .bind(task)
                .fetch_one(&self.pool)
                .await?,
        )
    }
    async fn report_page(&self, task: Uuid, after: Id, upper: Id) -> Result<Vec<Value>> {
        sqlx::query("SELECT * FROM integrity_issues WHERE task_id=? AND id>? AND id<=? ORDER BY id LIMIT 100").bind(task).bind(after).bind(upper).fetch_all(&self.pool).await?.into_iter().map(integrity_finding_value).collect()
    }
    async fn findings(
        &self,
        task: Uuid,
        after: Id,
        limit: i64,
        code: Option<&str>,
    ) -> Result<Vec<Value>> {
        sqlx::query("SELECT * FROM integrity_issues WHERE task_id=? AND id>? AND (? IS NULL OR code=?) ORDER BY id LIMIT ?").bind(task).bind(after).bind(code).bind(code).bind(limit).fetch_all(&self.pool).await?.into_iter().map(integrity_finding_value).collect()
    }
    async fn groups(&self, task: Uuid) -> Result<Vec<(String, String)>> {
        Ok(sqlx::query_as("SELECT code,CAST(COUNT(*) AS CHAR) FROM integrity_issues WHERE task_id=? GROUP BY code ORDER BY code").bind(task).fetch_all(&self.pool).await?)
    }
    async fn objects(
        &self,
        task: Uuid,
        issue: Id,
        after: Option<(Uuid, String)>,
    ) -> Result<Vec<Value>> {
        let (chunk, stream): (Option<i64>, Option<Id>) = sqlx::query_as(
            "SELECT chunk_id,stream_id FROM integrity_issues WHERE task_id=? AND id=?",
        )
        .bind(task)
        .bind(issue)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        let mut query = sqlx::QueryBuilder::<MySql>::new(
            "SELECT o.bucket_id,b.name AS bucket_name,o.object_key,o.stream_id FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE ",
        );
        if let Some(chunk) = chunk {
            query
                .push("EXISTS(SELECT 1 FROM extents WHERE stream_id=o.stream_id AND chunk_id=")
                .push_bind(chunk)
                .push(")");
        } else {
            query.push("o.stream_id=").push_bind(stream);
        }
        if let Some((bucket, key)) = after {
            query
                .push(" AND (o.bucket_id>")
                .push_bind(bucket)
                .push(" OR(o.bucket_id=")
                .push_bind(bucket)
                .push(" AND o.object_key>")
                .push_bind(key.into_bytes())
                .push("))");
        }
        query.push(" ORDER BY o.bucket_id,o.object_key LIMIT 101");
        query
            .build()
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                Ok(serde_json::json!({
                    "bucket_id": row.try_get::<Uuid,_>("bucket_id")?,
                    "bucket": row.try_get::<String,_>("bucket_name")?,
                    "key": object_key(&row)?,
                    "version": row.try_get::<Id,_>("stream_id")?,
                }))
            })
            .collect()
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for MappingRange {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        Ok(Self {
            offset_bytes: row.try_get("offset_bytes")?,
            length: row.try_get("length")?,
            source_offset: row.try_get("source_offset")?,
            fragment_id: row.try_get("fragment_id")?,
            chunk_id: row.try_get("chunk_id")?,
            state: row.try_get("state")?,
            raw_size: row.try_get("raw_size")?,
        })
    }
}

pub(super) fn pack_value(row: sqlx::mysql::MySqlRow) -> Result<Value> {
    let mut value = serde_json::json!({
        "id": row.try_get::<i64,_>("id")?.to_string(),
        "location_id": row.try_get::<Uuid,_>("location_id")?,
        "storage_id": row.try_get::<Uuid,_>("storage_id")?,
        "encryption_algorithm": row.try_get::<String,_>("encryption_algorithm")?,
        "block_key_id": row.try_get::<Option<Uuid>,_>("block_key_id")?,
        "raw_size": row.try_get::<i64,_>("raw_size")?,
        "stored_size": row.try_get::<Option<i64>,_>("stored_size")?,
        "pack_compressed": row.try_get::<bool,_>("pack_compressed")?,
        "member_count": row.try_get::<i32,_>("member_count")?,
        "state": row.try_get::<String,_>("state")?,
        "owner_task": row.try_get::<Option<Uuid>,_>("owner_task")?,
        "created_at": row.try_get::<DateTime<Utc>,_>("created_at")?,
    });
    for field in ["nonce", "digest", "pack_fingerprint"] {
        value[field] = row
            .try_get::<Option<Vec<u8>>, _>(field)?
            .map(|bytes| format!("\\x{}", hex::encode(bytes)))
            .into();
    }
    for field in [
        "unreferenced_at",
        "deleted_at",
        "range_checked_at",
        "stored_at",
    ] {
        value[field] = serde_json::to_value(row.try_get::<Option<DateTime<Utc>>, _>(field)?)?;
    }
    Ok(value)
}
