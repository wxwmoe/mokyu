use super::*;

pub(in super::super) fn save_media_operation<'q>(id: Uuid, outcome: &'q Outcome) -> Statement<'q> {
    sqlx::query("UPDATE media_operations SET result=$2 WHERE id=$1 AND result IS NULL")
        .bind(id)
        .bind(sqlx::types::Json(outcome))
}

pub(in super::super) fn insert_media_operation<'q>(
    user: Uuid,
    token: Option<Uuid>,
    client: Uuid,
    hash: &'q [u8],
) -> Statement<'q> {
    sqlx::query("INSERT INTO media_operations(user_id,token_id,client_id,request_hash) VALUES($1,$2,$3,$4) ON CONFLICT(user_id,token_id,client_id) DO NOTHING").bind(user).bind(token).bind(client).bind(hash)
}

pub(in super::super) fn find_media_operation(
    user: Uuid,
    token: Option<Uuid>,
    client: Uuid,
) -> Rows<'static, (Uuid, Vec<u8>)> {
    sqlx::query_as("SELECT id,request_hash FROM media_operations WHERE user_id=$1 AND token_id IS NOT DISTINCT FROM $2 AND client_id=$3").bind(user).bind(token).bind(client)
}

pub(in super::super) fn media_operation(id: Uuid) -> Scalar<'static, Option<Value>> {
    sqlx::query_scalar("SELECT result FROM media_operations WHERE id=$1").bind(id)
}

pub(in super::super) fn blocked_media_buckets<'q>(buckets: &'q [Uuid]) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM buckets WHERE id=ANY($1) AND state<>'active')")
        .bind(buckets)
}

pub(in super::super) fn lock_media_projects<'q>(buckets: &'q [Uuid]) -> Statement<'q> {
    sqlx::query("SELECT scope_id FROM quota_scopes WHERE scope='project' AND scope_id IN (SELECT project_id FROM buckets WHERE id=ANY($1)) ORDER BY scope_id FOR UPDATE").bind(buckets)
}

pub(in super::super) fn media_bucket_scopes<'q>(buckets: &'q [Uuid]) -> Statement<'q> {
    sqlx::query("SELECT scope_id FROM quota_scopes WHERE scope='bucket' AND scope_id=ANY($1) ORDER BY scope_id").bind(buckets)
}

pub(in super::super) fn object_storage(bucket: Uuid) -> Scalar<'static, Option<Uuid>> {
    sqlx::query_scalar("SELECT storage_id FROM buckets WHERE id=$1").bind(bucket)
}

pub(in super::super) fn stream_public(stream: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT public_read FROM streams WHERE id=$1").bind(stream)
}

pub(in super::super) fn tombstone_object<'q>(
    bucket: Uuid,
    key: &'q str,
    token: Uuid,
) -> Statement<'q> {
    sqlx::query(
        "UPDATE objects SET stream_id=NULL,write_token=$3 WHERE bucket_id=$1 AND object_key=$2",
    )
    .bind(bucket)
    .bind(key)
    .bind(token)
}

pub(in super::super) fn move_stream<'q>(stream: Id, bucket: Uuid, key: &'q str) -> Statement<'q> {
    sqlx::query("UPDATE streams SET bucket_id=$2,object_key=$3 WHERE id=$1")
        .bind(stream)
        .bind(bucket)
        .bind(key)
}

pub(in super::super) fn upsert_object<'q>(bucket: Uuid, key: &'q str, stream: Id) -> Statement<'q> {
    sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,$2,$3,$3) ON CONFLICT(bucket_id,object_key) DO UPDATE SET stream_id=excluded.stream_id,write_token=excluded.write_token").bind(bucket).bind(key).bind(stream)
}

pub(in super::super) fn publish_copy_stream(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='ready',touched_at=now() WHERE id=$1 AND state='writing'")
        .bind(stream)
}

pub(in super::super) fn abandon_copy_stream(stream: Id) -> Statement<'static> {
    sqlx::query(
        "UPDATE streams SET state='abandoned',touched_at=now() WHERE id=$1 AND state='writing' AND NOT EXISTS(SELECT 1 FROM replica_publications rp WHERE rp.source_pin=streams.id AND rp.phase='preparing') AND NOT EXISTS(SELECT 1 FROM replica_targets rt WHERE rt.candidate=streams.id AND rt.phase IN ('preparing','prepared'))",
    )
    .bind(stream)
}

pub(in super::super) fn lock_migration_task(task: Uuid, token: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()) FOR UPDATE").bind(task).bind(token)
}

pub(in super::super) fn publish_migration_stream(
    stream: Id,
    task: Uuid,
    token: Uuid,
) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='ready',owner_task_id=NULL,owner_lease_token=NULL,touched_at=now() WHERE id=$1 AND state='writing' AND owner_task_id=$2 AND owner_lease_token=$3").bind(stream).bind(task).bind(token)
}

pub(in super::super) fn complete_migration_task(task: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='completed',processed=processed+1,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())").bind(task).bind(token)
}

pub(in super::super) fn abandon_migration_stream(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_lease_token=NULL,touched_at=now() WHERE id=$1 AND state='writing' AND NOT EXISTS(SELECT 1 FROM replica_publications rp WHERE rp.source_pin=streams.id AND rp.phase='preparing') AND NOT EXISTS(SELECT 1 FROM replica_targets rt WHERE rt.candidate=streams.id AND rt.phase IN ('preparing','prepared'))").bind(stream)
}

#[async_trait::async_trait]
impl ObjectStore for Backend {
    async fn browse(
        &self,
        authority: &Permit,
        bucket: Uuid,
        filter: &mut CatalogFilter,
        cursor: Option<CatalogCursor>,
        sizes: (Option<i64>, Option<i64>),
    ) -> Result<Vec<MediaItem>> {
        transactions::browse_catalog(&self.pool, authority, bucket, filter, cursor, sizes).await
    }
    async fn chunks(&self, stream: Id, after: i64, limit: i64) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT jsonb_build_object('id',c.id::text,'offset_bytes',e.offset_bytes::text,'length',e.length,'source_offset',e.source_offset,'raw_size',c.raw_size,'stored_size',COALESCE(m.encoded_size,l.standalone_size),'independent_size_hint',c.standalone_size,'payload_size',c.standalone_size-CASE WHEN COALESCE(m.encryption_algorithm,l.encryption_algorithm,c.encryption_algorithm)='none' THEN 0 ELSE 16 END,'compression',CASE WHEN COALESCE(m.compressed,l.standalone_compressed,c.standalone_compressed) THEN 'zstd' ELSE 'none' END,'algorithm',COALESCE(m.encryption_algorithm,l.encryption_algorithm,c.encryption_algorithm),'block_key_id',CASE WHEN m.pack_id IS NULL THEN c.block_key_id ELSE m.block_key_id END,'pack_offset',m.offset_bytes,'pack_ordinal',m.ordinal,'pack_count',p.member_count,'pack_size',p.stored_size,'pack_id',c.pack_id::text,'source',CASE WHEN l.encoding_id IS NOT NULL THEN 'chunk' WHEN c.pack_id IS NOT NULL THEN 'pack' ELSE 'pending' END,'reads',COALESCE(a.reads,0)::text,'range_reads',COALESCE(a.range_reads,0)::text) FROM extents e JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' LEFT JOIN pack_members m ON l.encoding_id IS NULL AND m.pack_id=c.pack_id AND m.chunk_id=c.id LEFT JOIN packs p ON p.id=m.pack_id LEFT JOIN chunk_access_stats a ON a.chunk_id=c.id WHERE e.stream_id=$1 AND e.offset_bytes>$2 ORDER BY e.offset_bytes LIMIT $3").bind(stream).bind(after).bind(limit).fetch_all(&self.pool).await?)
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
        media_operation(id)
            .fetch_one(&self.pool)
            .await?
            .map(serde_json::from_value)
            .transpose()
            .map_err(Into::into)
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
        Ok(object_storage(bucket)
            .fetch_optional(&self.pool)
            .await?
            .flatten())
    }
    async fn move_object(&self, permit: &Permit, input: ObjectTransfer<'_>) -> Result<()> {
        transactions::move_object(&self.pool, permit, input).await
    }
    async fn publish_copy(
        &self,
        permit: &Permit,
        input: ObjectTransfer<'_>,
        stream: Id,
        public: bool,
        expected_public: Option<bool>,
    ) -> Result<()> {
        transactions::publish_copy(&self.pool, permit, input, stream, public, expected_public).await
    }
    async fn queue_migration(&self, permit: &Permit, job: &StorageMigration) -> Result<()> {
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
        transactions::complete_storage_migration(&self.pool, task, token, stream, job).await
    }
    async fn abandon_copy(&self, stream: Id) -> Result<()> {
        abandon_copy_stream(stream).execute(&self.pool).await?;
        Ok(())
    }
    async fn abandon_migration(&self, stream: Id) -> Result<()> {
        abandon_migration_stream(stream).execute(&self.pool).await?;
        Ok(())
    }
}

pub(in super::super) fn catalog_search_settings() -> sqlx::RawSql {
    sqlx::raw_sql("SET LOCAL statement_timeout='2s'; SET LOCAL work_mem='4MB'")
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for MediaItem {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            object_key: row.try_get("object_key")?,
            size: row.try_get("size")?,
            content_type: row.try_get("content_type")?,
            public_read: row.try_get("public_read")?,
            modified_at: row.try_get("modified_at")?,
        })
    }
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

pub(super) fn invalid() -> anyhow::Error {
    s3s::s3_error!(InvalidArgument).into()
}

pub(in super::super) async fn browse_catalog(
    db: &mut PgConnection,
    bucket: Uuid,
    q: &mut CatalogFilter,
    cursor: Option<CatalogCursor>,
    (min, max): (Option<i64>, Option<i64>),
) -> Result<Vec<MediaItem>> {
    if q.mode == "contains" && !q.q.is_empty() {
        let usable: bool = sqlx::query_scalar("SELECT $1 COLLATE \"default\" ~ '[[:alnum:]]{3}'")
            .bind(&q.q)
            .fetch_one(&mut *db)
            .await?;
        if !usable {
            q.mode = "prefix".into();
        }
    }
    let mut sql = QueryBuilder::<Postgres>::new(
        "SELECT stream_id id,object_key,catalog_size::text size,catalog_type content_type,catalog_public public_read,catalog_modified modified_at FROM objects WHERE stream_id IS NOT NULL AND bucket_id=",
    );
    sql.push_bind(bucket);
    if !q.prefix.is_empty() {
        sql.push(" AND object_key>=").push_bind(&q.prefix);
        if let Some(end) = crate::app::listing::successor(&q.prefix) {
            sql.push(" AND object_key<").push_bind(end);
        }
    }
    if !q.q.is_empty() {
        match q.mode.as_str() {
            "exact" => {
                sql.push(" AND object_key=")
                    .push_bind(format!("{}{}", q.prefix, q.q));
            }
            "prefix" => {
                let prefix = format!("{}{}", q.prefix, q.q);
                sql.push(" AND object_key>=").push_bind(prefix.clone());
                if let Some(end) = crate::app::listing::successor(&prefix) {
                    sql.push(" AND object_key<").push_bind(end);
                }
            }
            _ => {
                let pattern = format!("%{}%", escape(&q.q));
                sql.push(" AND object_key COLLATE \"default\" ILIKE ")
                    .push_bind(pattern.clone());
                if q.search_in == "name" {
                    sql.push(
                        " AND regexp_replace(object_key,'^.*/','') COLLATE \"default\" ILIKE ",
                    )
                    .push_bind(pattern);
                }
            }
        }
    }
    if let Some(category) = &q.category {
        sql.push(" AND catalog_category=").push_bind(category);
    }
    if let Some(public) = q.public {
        sql.push(" AND catalog_public=").push_bind(public);
    }
    if let Some(min) = min {
        sql.push(" AND catalog_size>=").push_bind(min);
    }
    if let Some(max) = max {
        sql.push(" AND catalog_size<=").push_bind(max);
    }
    if let Some(since) = q.since {
        sql.push(" AND catalog_modified>=").push_bind(since);
    }
    if let Some(until) = q.until {
        sql.push(" AND catalog_modified<=").push_bind(until);
    }
    let column = match q.sort.as_str() {
        "size" => "catalog_size",
        "modified" => "catalog_modified",
        _ => "object_key",
    };
    let comparison = if q.order == "asc" { " > " } else { " < " };
    if let Some(c) = cursor {
        if column == "object_key" {
            sql.push(" AND object_key")
                .push(comparison)
                .push_bind(c.key);
        } else {
            sql.push(" AND (")
                .push(column)
                .push(",object_key)")
                .push(comparison)
                .push("(");
            if column == "catalog_size" {
                sql.push_bind(
                    c.size
                        .parse::<i64>()
                        .ok()
                        .filter(|n| *n >= 0)
                        .ok_or_else(invalid)?,
                );
            } else {
                sql.push_bind(c.modified);
            }
            sql.push(",").push_bind(c.key).push(")");
        }
    }
    let direction = if q.order == "asc" { " ASC" } else { " DESC" };
    sql.push(" ORDER BY ").push(column).push(direction);
    if column != "object_key" {
        sql.push(",object_key").push(direction);
    }
    sql.push(" LIMIT ").push_bind(q.limit as i64 + 1);
    Ok(sql.build_query_as().fetch_all(db).await?)
}
