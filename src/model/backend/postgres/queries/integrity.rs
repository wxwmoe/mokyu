use super::*;

pub(in super::super) fn initialize_integrity_rate<'q>(
    storage: Uuid,
    metric: &'q str,
    tokens: String,
) -> Statement<'q> {
    sqlx::query("INSERT INTO storage_rate_state(storage_id,metric,tokens,replenished_at,updated_at) VALUES($1,$2,$3::text::numeric,now(),now()) ON CONFLICT(storage_id,metric) DO NOTHING").bind(storage).bind(metric).bind(tokens)
}

pub(in super::super) fn lock_integrity_rate<'q>(
    storage: Uuid,
    metric: &'q str,
) -> Rows<'q, (f64, DateTime<Utc>)> {
    sqlx::query_as("SELECT tokens::double precision,replenished_at FROM storage_rate_state WHERE storage_id=$1 AND metric=$2 FOR UPDATE").bind(storage).bind(metric)
}

pub(in super::super) fn update_integrity_rate<'q>(
    storage: Uuid,
    metric: &'q str,
    tokens: String,
) -> Statement<'q> {
    sqlx::query("UPDATE storage_rate_state SET tokens=$3::text::numeric,replenished_at=now(),updated_at=now() WHERE storage_id=$1 AND metric=$2").bind(storage).bind(metric).bind(tokens)
}

pub(in super::super) fn integrity_busy() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='integrity' AND NOT (detail ? 'source_operation') AND state IN ('queued','running'))",
    )
}

pub(in super::super) fn integrity_storage(storage: Uuid) -> Rows<'static, (Uuid, String)> {
    sqlx::query_as("SELECT id,type FROM storages WHERE id=$1 AND enabled").bind(storage)
}

pub(in super::super) fn integrity_storages() -> Rows<'static, (Uuid, String)> {
    sqlx::query_as("SELECT id,type FROM storages WHERE enabled ORDER BY created_at,id")
}

pub(in super::super) fn integrity_upper_chunk(storage: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT COALESCE((SELECT id FROM chunks WHERE storage_id=$1 ORDER BY id DESC LIMIT 1),'00000000-0000-0000-0000-000000000000'::uuid)").bind(storage)
}

pub(in super::super) fn integrity_upper_chunk_all() -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT COALESCE((SELECT id FROM chunks ORDER BY id DESC LIMIT 1),'00000000-0000-0000-0000-000000000000'::uuid)",
    )
}

pub(in super::super) fn scope_integrity_task<'q>(
    child: Uuid,
    parent: Uuid,
    total: i64,
    scope: &'q str,
    node: Option<Uuid>,
) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET parent_task_id=$2,batch_total=$3,execution_scope=$4,target_node_id=$5 WHERE id=$1").bind(child).bind(parent).bind(total).bind(scope).bind(node)
}

pub(in super::super) fn task_batch_total(parent: Uuid, count: i64) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET batch_total=$2 WHERE id=$1")
        .bind(parent)
        .bind(count)
}

pub(in super::super) fn insert_integrity_finding<'q>(
    task: Uuid,
    issue: &'q Finding,
) -> Statement<'q> {
    sqlx::query("INSERT INTO integrity_issues(task_id,subject,code,chunk_id,location_id,stream_id,bucket_id,object_key,detail) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(task_id,subject,code) DO NOTHING").bind(task).bind(&issue.subject).bind(&issue.code).bind(issue.chunk_id).bind(issue.location_id).bind(issue.stream_id).bind(issue.bucket_id).bind(&issue.key).bind(&issue.detail)
}

pub(in super::super) fn integrity_checkpoint(
    task: Uuid,
    cursor: String,
    detail: Value,
    processed: i64,
    done: bool,
) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET cursor=$2,detail=$3,processed=$4,updated_at=now(),state=CASE WHEN $5 AND state='running' THEN 'completed' ELSE state END WHERE id=$1").bind(task).bind(cursor).bind(detail).bind(processed).bind(done)
}

fn integrity_ranges(stream: Id, after: i64) -> Rows<'static, IntegrityRange> {
    sqlx::query_as("SELECT e.*,c.state,c.raw_size FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=$1 AND e.offset_bytes>$2 ORDER BY e.offset_bytes LIMIT 65").bind(stream).bind(after)
}

pub(in super::super) fn integrity_current<'q>(
    bucket: Uuid,
    key: &'q str,
    stream: Id,
) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM objects WHERE bucket_id=$1 AND object_key=$2 AND stream_id=$3)").bind(bucket).bind(key).bind(stream)
}

pub(in super::super) fn integrity_task_exists(task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=$1 AND task_type='integrity')")
        .bind(task)
}

pub(in super::super) fn integrity_findings<'q>(
    task: Uuid,
    after: Id,
    limit: i64,
    code: Option<&'q str>,
) -> Scalar<'q, Value> {
    sqlx::query_scalar("SELECT to_jsonb(i)||jsonb_build_object('id',i.id::text,'chunk_id',i.chunk_id::text) FROM integrity_issues i WHERE task_id=$1 AND id>$2 AND ($4::text IS NULL OR code=$4) ORDER BY id LIMIT $3").bind(task).bind(after).bind(limit).bind(code)
}

pub(in super::super) fn integrity_groups(task: Uuid) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT code,count(*)::text FROM integrity_issues WHERE task_id=$1 GROUP BY code ORDER BY code").bind(task)
}

pub(in super::super) fn integrity_issue_source(
    task: Uuid,
    issue: Id,
) -> Rows<'static, (Option<Id>, Option<Id>)> {
    sqlx::query_as("SELECT chunk_id,stream_id FROM integrity_issues WHERE task_id=$1 AND id=$2")
        .bind(task)
        .bind(issue)
}

pub(in super::super) async fn integrity_upper_object(
    db: &mut sqlx::PgConnection,
    detail: &Detail,
) -> Result<Option<(Uuid, String)>> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT o.bucket_id,o.object_key FROM objects o WHERE o.stream_id IS NOT NULL",
    );
    integrity_scope(&mut query, detail);
    query.push(" ORDER BY o.bucket_id DESC,o.object_key DESC LIMIT 1");
    Ok(query.build_query_as().fetch_optional(db).await?)
}

pub(in super::super) fn integrity_chunks(
    d: &Detail,
    after: Id,
    limit: i64,
) -> Result<sqlx::QueryBuilder<sqlx::Postgres>> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at FROM chunks c LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE c.state='ready' AND c.id>",
    );
    query.push_bind(after).push(" AND c.id<=").push_bind(d.upper_chunk_id.parse::<Id>()?).push(" AND EXISTS(SELECT 1 FROM extents e JOIN objects o ON o.stream_id=e.stream_id WHERE e.chunk_id=c.id");
    integrity_scope(&mut query, d);
    query.push(") ORDER BY c.id LIMIT ").push_bind(limit);
    Ok(query)
}

pub(in super::super) fn integrity_chunk_stream(
    chunk: Id,
    d: &Detail,
) -> sqlx::QueryBuilder<sqlx::Postgres> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT o.stream_id FROM extents e JOIN objects o ON o.stream_id=e.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE c.state='ready' AND c.id=",
    );
    query.push_bind(chunk);
    integrity_scope(&mut query, d);
    query.push(" LIMIT 1");
    query
}

pub(in super::super) async fn physical_source(
    db: &mut sqlx::PgConnection,
    chunk: Id,
    inspection: bool,
) -> Result<PhysicalSource> {
    if let Some(physical) = sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=0 AND tl.physical_id=l.encoding_id) AS tier_material,c.id,l.encoding_id AS current_encoding_id,l.encoding_id AS location_id,c.storage_id,c.hash,c.raw_size,l.standalone_size AS standalone_size,l.encryption_algorithm AS encryption_algorithm,l.block_key_id,l.standalone_compressed AS standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE c.id=$1 AND l.state='ready' AND c.state='ready' AND ($2 OR l.quarantined_at IS NULL)").bind(chunk).bind(inspection).fetch_optional(&mut *db).await? { return Ok(PhysicalSource::Chunk(physical)); }
    let pack = sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=1 AND tl.physical_id=p.id) AS tier_material,p.* FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE c.id=$1 AND c.state='ready' AND p.state='ready' AND ($2 OR p.quarantined_at IS NULL)").bind(chunk).bind(inspection).fetch_optional(&mut *db).await?.context(crate::model::Error::SourceUnavailable)?;
    Ok(PhysicalSource::Pack(pack))
}

pub(in super::super) fn integrity_scope(
    query: &mut sqlx::QueryBuilder<sqlx::Postgres>,
    d: &Detail,
) {
    if let Some(storage_id) = d.storage_id {
        query
            .push(
                " AND EXISTS (SELECT 1 FROM buckets sb WHERE sb.id=o.bucket_id AND sb.storage_id=",
            )
            .push_bind(storage_id)
            .push(")");
    }
    if let Some(bucket) = d.bucket_id {
        query.push(" AND o.bucket_id=").push_bind(bucket);
    }
    if let Some(key) = &d.key {
        query.push(" AND o.object_key=").push_bind(key);
    }
    if let Some((bucket, key)) = &d.upper_object {
        query
            .push(" AND (o.bucket_id,o.object_key)<=(")
            .push_bind(*bucket)
            .push(",")
            .push_bind(key)
            .push(")");
    }
}

#[derive(sqlx::FromRow)]
struct IntegrityRange {
    offset_bytes: i64,
    length: i32,
    source_offset: i32,
    fragment_id: Option<Id>,
    chunk_id: Option<Id>,
    state: Option<String>,
    raw_size: Option<i32>,
}

#[async_trait::async_trait]
impl IntegrityStore for Backend {
    async fn start(
        &self,
        input: IntegrityRequest,
        bucket: Option<Bucket>,
        node: Option<Uuid>,
    ) -> Result<(Uuid, usize)> {
        transactions::start_integrity(&self.pool, self.single, input, bucket, node).await
    }
    async fn chunks(&self, d: &Detail, after: Id, limit: i64) -> Result<Vec<Chunk>> {
        Ok(integrity_chunks(d, after, limit)?
            .build_query_as()
            .fetch_all(&self.pool)
            .await?)
    }
    async fn mapping(
        &self,
        d: &Detail,
        cursor: &Cursor,
    ) -> Result<Option<(StoredStream, Uuid, String)>> {
        let mut query = sqlx::QueryBuilder::new(
            "SELECT s.*,o.bucket_id AS visible_bucket,o.object_key AS visible_key FROM objects o JOIN streams s ON s.id=o.stream_id WHERE true",
        );
        integrity_scope(&mut query, d);
        if let Some(stream) = cursor.stream {
            query.push(" AND o.stream_id=").push_bind(stream);
        } else if let (Some(bucket), Some(key)) = (cursor.bucket, &cursor.key) {
            query
                .push(" AND (o.bucket_id,o.object_key)>(")
                .push_bind(bucket)
                .push(",")
                .push_bind(key)
                .push(")");
        }
        query.push(" ORDER BY o.bucket_id,o.object_key LIMIT 1");
        #[derive(sqlx::FromRow)]
        struct Object {
            #[sqlx(flatten)]
            stream: StoredStream,
            visible_bucket: Uuid,
            visible_key: String,
        }
        let row: Option<Object> = query.build_query_as().fetch_optional(&self.pool).await?;
        Ok(row.map(|r| (r.stream, r.visible_bucket, r.visible_key)))
    }
    async fn ranges(&self, stream: Id, after: i64) -> Result<Vec<MappingRange>> {
        Ok(integrity_ranges(stream, after)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| MappingRange {
                offset_bytes: r.offset_bytes,
                length: r.length,
                source_offset: r.source_offset,
                fragment_id: r.fragment_id,
                chunk_id: r.chunk_id,
                state: r.state,
                raw_size: r.raw_size,
            })
            .collect())
    }
    async fn current(&self, bucket: Uuid, key: &str, stream: Id) -> Result<bool> {
        Ok(integrity_current(bucket, key, stream)
            .fetch_one(&self.pool)
            .await?)
    }
    async fn chunk_stream(&self, chunk: Id, d: &Detail) -> Result<Option<Id>> {
        Ok(integrity_chunk_stream(chunk, d)
            .build_query_scalar()
            .fetch_optional(&self.pool)
            .await?)
    }
    async fn checkpoint(
        &self,
        id: Execution,
        cursor: String,
        detail: Detail,
        issues: Vec<Finding>,
    ) -> Result<()> {
        transactions::checkpoint_integrity(&self.pool, id, cursor, detail, issues).await
    }
    async fn exists(&self, task: Uuid) -> Result<bool> {
        Ok(integrity_task_exists(task).fetch_one(&self.pool).await?)
    }
    async fn report_task(&self, task: Uuid) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT to_jsonb(t) FROM tasks t WHERE id=$1 AND task_type='integrity'",
        )
        .bind(task)
        .fetch_optional(&self.pool)
        .await?
        .map(|value| task_value(value, self.single)))
    }
    async fn report_upper(&self, task: Uuid) -> Result<Id> {
        Ok(
            sqlx::query_scalar("SELECT COALESCE((SELECT id FROM integrity_issues WHERE task_id=$1 ORDER BY id DESC LIMIT 1),'00000000-0000-0000-0000-000000000000'::uuid)")
                .bind(task)
                .fetch_one(&self.pool)
                .await?,
        )
    }
    async fn report_page(&self, task: Uuid, after: Id, upper: Id) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT to_jsonb(i)||jsonb_build_object('id',i.id::text,'chunk_id',i.chunk_id::text) FROM integrity_issues i WHERE task_id=$1 AND id>$2 AND id<=$3 ORDER BY id LIMIT 100")
            .bind(task).bind(after).bind(upper).fetch_all(&self.pool).await?)
    }
    async fn findings(
        &self,
        task: Uuid,
        after: Id,
        limit: i64,
        code: Option<&str>,
    ) -> Result<Vec<Value>> {
        Ok(integrity_findings(task, after, limit, code)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn groups(&self, task: Uuid) -> Result<Vec<(String, String)>> {
        Ok(integrity_groups(task).fetch_all(&self.pool).await?)
    }
    async fn objects(
        &self,
        id: Uuid,
        issue: Id,
        after: Option<(Uuid, String)>,
    ) -> Result<Vec<Value>> {
        let row: Option<(Option<Id>, Option<Id>)> = integrity_issue_source(id, issue)
            .fetch_optional(&self.pool)
            .await?;
        let (chunk, stream) = row.ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        let mut query = sqlx::QueryBuilder::new(
            "SELECT jsonb_build_object('bucket_id',o.bucket_id,'bucket',b.name,'key',o.object_key,'version',o.stream_id) FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE ",
        );
        if let Some(chunk) = chunk {
            query
                .push(
                    "EXISTS(SELECT 1 FROM extents e WHERE e.stream_id=o.stream_id AND e.chunk_id=",
                )
                .push_bind(chunk)
                .push(")");
        } else {
            query.push("o.stream_id=").push_bind(stream);
        }
        if let Some((bucket, key)) = after {
            query
                .push(" AND (o.bucket_id,o.object_key)>(")
                .push_bind(bucket)
                .push(",")
                .push_bind(key)
                .push(")");
        }
        query.push(" ORDER BY o.bucket_id,o.object_key LIMIT 101");
        let rows: Vec<Value> = query.build_query_scalar().fetch_all(&self.pool).await?;
        Ok(rows)
    }
}
