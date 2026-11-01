use super::*;

pub(in super::super) fn insert_media_operation(
    id: Uuid,
    user: Uuid,
    token: Option<Uuid>,
    client: Uuid,
    hash: &[u8],
) -> Statement<'_> {
    sqlx::query("INSERT INTO media_operations(id,user_id,token_id,client_id,request_hash) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE id=media_operations.id").bind(id).bind(user).bind(token).bind(client).bind(hash)
}

pub(in super::super) fn find_media_operation(
    user: Uuid,
    token: Option<Uuid>,
    client: Uuid,
) -> Rows<'static, (Uuid, Vec<u8>)> {
    sqlx::query_as("SELECT id,request_hash FROM media_operations WHERE user_id=? AND token_id<=>? AND client_id=?").bind(user).bind(token).bind(client)
}

pub(in super::super) fn save_media_operation(id: Uuid, outcome: &Outcome) -> Statement<'_> {
    sqlx::query("UPDATE media_operations SET result=? WHERE id=? AND result IS NULL")
        .bind(sqlx::types::Json(outcome))
        .bind(id)
}

pub(in super::super) fn partial_audit(id: Id) -> Statement<'static> {
    sqlx::query(
        "UPDATE audit_events SET outcome='partial',finished_at=CURRENT_TIMESTAMP(6) WHERE id=?",
    )
    .bind(id)
}

pub(in super::super) fn set_stream_public(id: Id, public: bool) -> Statement<'static> {
    sqlx::query("UPDATE streams SET public_read=? WHERE id=?")
        .bind(public)
        .bind(id)
}

pub(in super::super) fn set_catalog_public(bucket: Uuid, key: &str, public: bool) -> Statement<'_> {
    sqlx::query("UPDATE objects SET catalog_public=? WHERE bucket_id=? AND object_key=?")
        .bind(public)
        .bind(bucket)
        .bind(key.as_bytes())
}

pub(in super::super) fn move_stream(id: Id, bucket: Uuid, key: &str) -> Statement<'_> {
    sqlx::query("UPDATE streams SET bucket_id=?,object_key=? WHERE id=?")
        .bind(bucket)
        .bind(key.as_bytes())
        .bind(id)
}

pub(in super::super) fn abandon_copy_stream(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_execution_token=NULL,touched_at=CURRENT_TIMESTAMP(6) WHERE id=? AND state='writing' AND NOT EXISTS(SELECT 1 FROM replica_publications rp WHERE rp.source_pin=streams.id AND rp.phase='preparing') AND NOT EXISTS(SELECT 1 FROM replica_targets rt WHERE rt.candidate=streams.id AND rt.phase IN ('preparing','prepared'))").bind(id)
}

pub(in super::super) fn publish_migration_stream(id: Id, task: Execution) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='ready',owner_task_id=NULL,owner_execution_token=NULL,touched_at=CURRENT_TIMESTAMP(6) WHERE id=? AND state='writing' AND owner_task_id=? AND owner_execution_token=?").bind(id).bind(task.id).bind(task.token)
}

pub(in super::super) fn complete_migration_task(task: Execution) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='completed',processed=processed+1,execution_token=NULL,updated_at=CURRENT_TIMESTAMP(6) WHERE id=? AND state='running' AND execution_token=?").bind(task.id).bind(task.token)
}

pub(in super::super) async fn lock_media_quota(
    connection: &mut sqlx::MySqlConnection,
    buckets: &[Uuid],
) -> Result<()> {
    if buckets.is_empty() {
        return Ok(());
    }
    let mut projects = sqlx::QueryBuilder::<MySql>::new(
        "SELECT scope_id FROM quota_scopes WHERE scope='project' AND scope_id IN (SELECT project_id FROM buckets WHERE id IN (",
    );
    let mut ids = projects.separated(",");
    for id in buckets {
        ids.push_bind(*id);
    }
    projects.push(")) ORDER BY scope_id FOR UPDATE");
    projects
        .build_query_scalar::<Uuid>()
        .fetch_all(&mut *connection)
        .await?;
    let mut scopes = sqlx::QueryBuilder::<MySql>::new(
        "SELECT scope_id FROM quota_scopes WHERE scope='bucket' AND scope_id IN (",
    );
    let mut ids = scopes.separated(",");
    for id in buckets {
        ids.push_bind(*id);
    }
    scopes.push(") ORDER BY scope_id FOR UPDATE");
    scopes
        .build_query_scalar::<Uuid>()
        .fetch_all(connection)
        .await?;
    Ok(())
}

pub(in super::super) async fn browse_catalog(
    connection: &mut sqlx::MySqlConnection,
    bucket: Uuid,
    filter: &mut CatalogFilter,
    cursor: Option<CatalogCursor>,
    (min, max): (Option<i64>, Option<i64>),
) -> Result<Vec<MediaItem>> {
    if filter.mode == "contains" && !filter.q.is_empty() {
        let mut run = 0;
        let usable = filter.q.chars().any(|c| {
            run = if c.is_alphanumeric() { run + 1 } else { 0 };
            run >= 3
        });
        if !usable {
            filter.mode = "prefix".into();
        }
    }
    let mut query = sqlx::QueryBuilder::<MySql>::new(
        "/*M!100603 SET STATEMENT max_statement_time=2 FOR */ SELECT /*+ MAX_EXECUTION_TIME(2000) */ stream_id AS id,object_key,CAST(catalog_size AS CHAR) size,catalog_type AS content_type,catalog_public AS public_read,catalog_modified AS modified_at FROM objects WHERE stream_id IS NOT NULL AND bucket_id=",
    );
    query.push_bind(bucket);
    if !filter.prefix.is_empty() {
        query
            .push(" AND object_key>=")
            .push_bind(filter.prefix.as_bytes());
        if let Some(end) = crate::app::listing::successor(&filter.prefix) {
            query.push(" AND object_key<").push_bind(end.into_bytes());
        }
    }
    if !filter.q.is_empty() {
        let prefix = format!("{}{}", filter.prefix, filter.q);
        match filter.mode.as_str() {
            "exact" => {
                query
                    .push(" AND object_key=")
                    .push_bind(prefix.into_bytes());
            }
            "prefix" => {
                query.push(" AND object_key>=").push_bind(prefix.as_bytes());
                if let Some(end) = crate::app::listing::successor(&prefix) {
                    query.push(" AND object_key<").push_bind(end.into_bytes());
                }
            }
            _ => {
                query
                    .push(" AND INSTR(LOWER(CONVERT(object_key USING utf8mb4)),LOWER(")
                    .push_bind(&filter.q)
                    .push("))>0");
                if filter.search_in == "name" {
                    query.push(" AND INSTR(LOWER(SUBSTRING_INDEX(CONVERT(object_key USING utf8mb4),'/',-1)),LOWER(").push_bind(&filter.q).push("))>0");
                }
            }
        }
    }
    if let Some(category) = &filter.category {
        query.push(" AND catalog_category=").push_bind(category);
    }
    if let Some(public) = filter.public {
        query.push(" AND catalog_public=").push_bind(public);
    }
    if let Some(min) = min {
        query.push(" AND catalog_size>=").push_bind(min);
    }
    if let Some(max) = max {
        query.push(" AND catalog_size<=").push_bind(max);
    }
    if let Some(since) = filter.since {
        query.push(" AND catalog_modified>=").push_bind(since);
    }
    if let Some(until) = filter.until {
        query.push(" AND catalog_modified<=").push_bind(until);
    }
    let column = match filter.sort.as_str() {
        "size" => "catalog_size",
        "modified" => "catalog_modified",
        _ => "object_key",
    };
    let comparison = if filter.order == "asc" { " > " } else { " < " };
    if let Some(cursor) = cursor {
        if column == "object_key" {
            query
                .push(" AND object_key")
                .push(comparison)
                .push_bind(cursor.key.into_bytes());
        } else {
            query
                .push(" AND (")
                .push(column)
                .push(",object_key)")
                .push(comparison)
                .push("(");
            if column == "catalog_size" {
                query.push_bind(
                    cursor
                        .size
                        .parse::<i64>()
                        .ok()
                        .filter(|n| *n >= 0)
                        .ok_or_else(|| s3s::s3_error!(InvalidArgument))?,
                );
            } else {
                query.push_bind(cursor.modified);
            }
            query.push(",").push_bind(cursor.key.into_bytes()).push(")");
        }
    }
    let direction = if filter.order == "asc" {
        " ASC"
    } else {
        " DESC"
    };
    query.push(" ORDER BY ").push(column).push(direction);
    if column != "object_key" {
        query.push(",object_key").push(direction);
    }
    query.push(" LIMIT ").push_bind(filter.limit as i64 + 1);
    Ok(query.build_query_as().fetch_all(connection).await?)
}

#[async_trait::async_trait]
impl ObjectStore for Backend {
    async fn browse(
        &self,
        authority: &crate::authorization::Permit,
        bucket: Uuid,
        filter: &mut CatalogFilter,
        cursor: Option<CatalogCursor>,
        sizes: (Option<i64>, Option<i64>),
    ) -> Result<Vec<MediaItem>> {
        transactions::browse_catalog(&self.pool, authority, bucket, filter, cursor, sizes).await
    }
    async fn chunks(&self, stream: Id, after: i64, limit: i64) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT JSON_OBJECT('id',CAST(c.id AS CHAR),'offset_bytes',CAST(e.offset_bytes AS CHAR),'length',e.length,'source_offset',e.source_offset,'raw_size',c.raw_size,'stored_size',l.standalone_size,'independent_size_hint',c.standalone_size,'payload_size',c.standalone_size-IF(c.encryption_algorithm='none',0,16),'compression',IF(c.standalone_compressed,'zstd','none'),'algorithm',CONVERT(c.encryption_algorithm USING utf8mb4),'block_key_id',LOWER(INSERT(INSERT(INSERT(INSERT(HEX(c.block_key_id),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-')),'pack_id',CAST(c.pack_id AS CHAR),'source',CASE WHEN l.encoding_id IS NOT NULL THEN 'chunk' WHEN c.pack_id IS NOT NULL THEN 'pack' ELSE 'pending' END,'reads',CAST(COALESCE(a.read_count,0) AS CHAR),'range_reads',CAST(COALESCE(a.range_read_count,0) AS CHAR)) FROM extents e JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' LEFT JOIN chunk_access_stats a ON a.chunk_id=c.id WHERE e.stream_id=? AND e.offset_bytes>? ORDER BY e.offset_bytes LIMIT ?").bind(stream).bind(after).bind(limit).fetch_all(&self.pool).await?)
    }
    async fn change(&self, input: ObjectChange) -> Result<bool> {
        transactions::change_object(&self.pool, input).await
    }
    async fn begin_operation(
        &self,
        principal: &Principal,
        user: Uuid,
        client: Uuid,
        hash: &[u8],
    ) -> Result<(Uuid, Vec<u8>)> {
        transactions::begin_media_operation(&self.pool, principal, user, client, hash).await
    }
    async fn operation(&self, id: Uuid) -> Result<Option<Outcome>> {
        let result: Option<sqlx::types::Json<Outcome>> =
            sqlx::query_scalar("SELECT result FROM media_operations WHERE id=?")
                .bind(id)
                .fetch_one(&self.pool)
                .await?;
        Ok(result.map(|r| r.0))
    }
    async fn fail_operation(&self, id: Uuid, outcome: &Outcome) -> Result<()> {
        save_media_operation(id, outcome)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn audit_batch(&self, bucket: Uuid, detail: Value, partial: bool) -> Result<()> {
        transactions::audit_object_batch(&self.pool, bucket, detail, partial).await
    }
    async fn storage(&self, bucket: Uuid) -> Result<Option<Uuid>> {
        Ok(
            sqlx::query_scalar("SELECT storage_id FROM buckets WHERE id=?")
                .bind(bucket)
                .fetch_optional(&self.pool)
                .await?,
        )
    }
    async fn move_object(
        &self,
        permit: &crate::authorization::Permit,
        input: ObjectTransfer<'_>,
    ) -> Result<()> {
        transactions::move_object(&self.pool, permit, input).await
    }
    async fn publish_copy(
        &self,
        permit: &crate::authorization::Permit,
        input: ObjectTransfer<'_>,
        stream: Id,
        public: bool,
        expected_public: Option<bool>,
    ) -> Result<()> {
        transactions::publish_copy(&self.pool, permit, input, stream, public, expected_public).await
    }
    async fn queue_migration(
        &self,
        permit: &crate::authorization::Permit,
        job: &StorageMigration,
    ) -> Result<()> {
        transactions::queue_storage_migration(&self.pool, permit, job).await
    }
    async fn resume_migration(
        &self,
        task: crate::model::task::Execution,
        job: &StorageMigration,
    ) -> Result<bool> {
        transactions::resume_migration(&self.pool, task, job).await
    }
    async fn complete_migration(
        &self,
        task: Uuid,
        token: Uuid,
        stream: Id,
        job: &StorageMigration,
    ) -> Result<()> {
        transactions::complete_storage_migration(
            &self.pool,
            Execution { id: task, token },
            stream,
            job,
        )
        .await
    }
    async fn abandon_copy(&self, stream: Id) -> Result<()> {
        transactions::abandon_copy(&self.pool, stream).await
    }
    async fn abandon_migration(&self, stream: Id) -> Result<()> {
        transactions::abandon_copy(&self.pool, stream).await
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for MediaItem {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            object_key: object_key(row)?,
            size: row.try_get("size")?,
            content_type: row.try_get("content_type")?,
            public_read: row.try_get("public_read")?,
            modified_at: row.try_get("modified_at")?,
        })
    }
}
