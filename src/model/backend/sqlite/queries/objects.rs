use super::*;

pub(in super::super) fn insert_media_operation(
    id: Uuid,
    user: Uuid,
    token: Option<Uuid>,
    client: Uuid,
    hash: &[u8],
) -> Statement<'_> {
    sqlx::query("INSERT INTO media_operations(id,user_id,token_id,client_id,request_hash) VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING").bind(id).bind(user).bind(token).bind(client).bind(hash)
}

pub(in super::super) fn find_media_operation(
    user: Uuid,
    token: Option<Uuid>,
    client: Uuid,
) -> Rows<'static, (Uuid, Vec<u8>)> {
    sqlx::query_as("SELECT id,request_hash FROM media_operations WHERE user_id=? AND token_id IS ? AND client_id=?").bind(user).bind(token).bind(client)
}

pub(in super::super) fn save_media_operation(id: Uuid, outcome: &Outcome) -> Statement<'_> {
    sqlx::query("UPDATE media_operations SET result=? WHERE id=? AND result IS NULL")
        .bind(sqlx::types::Json(outcome))
        .bind(id)
}

pub(in super::super) fn partial_audit(id: Id) -> Statement<'static> {
    sqlx::query(
        "UPDATE audit_events SET outcome='partial',finished_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?",
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
    sqlx::query("UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_execution_token=NULL,touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='writing' AND NOT EXISTS(SELECT 1 FROM replica_publications rp WHERE rp.source_pin=streams.id AND rp.phase='preparing') AND NOT EXISTS(SELECT 1 FROM replica_targets rt WHERE rt.candidate=streams.id AND rt.phase IN ('preparing','prepared'))").bind(id)
}

pub(in super::super) fn publish_migration_stream(id: Id, task: Execution) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='ready',owner_task_id=NULL,owner_execution_token=NULL,touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='writing' AND owner_task_id=? AND owner_execution_token=?").bind(id).bind(task.id).bind(task.token)
}

pub(in super::super) fn complete_migration_task(task: Execution) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='completed',processed=processed+1,execution_token=NULL,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='running' AND execution_token=?").bind(task.id).bind(task.token)
}

pub(in super::super) async fn browse_catalog(
    connection: &mut sqlx::SqliteConnection,
    bucket: Uuid,
    filter: &mut CatalogFilter,
    cursor: Option<CatalogCursor>,
    sizes: (Option<i64>, Option<i64>),
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
    let target = i64::try_from(filter.limit)
        .ok()
        .and_then(|limit| limit.checked_add(1))
        .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
    if filter.mode != "contains" || filter.q.is_empty() {
        return Ok(
            catalog_page(bucket, filter, cursor.as_ref(), sizes, target)?
                .build_query_as()
                .fetch_all(connection)
                .await?,
        );
    }
    let needle = filter.q.to_lowercase();
    let mut after = cursor;
    let mut result = Vec::new();
    loop {
        let rows: Vec<MediaItem> = catalog_page(bucket, filter, after.as_ref(), sizes, 128)?
            .build_query_as()
            .fetch_all(&mut *connection)
            .await?;
        let more = rows.len() == 128;
        if let Some(last) = rows.last() {
            after = Some(CatalogCursor {
                scope: String::new(),
                key: last.object_key.clone(),
                size: last.size.clone(),
                modified: last.modified_at,
                folder: None,
            });
        }
        for row in rows {
            let text = if filter.search_in == "name" {
                row.object_key.rsplit('/').next().unwrap_or("")
            } else {
                &row.object_key
            };
            if text.to_lowercase().contains(&needle) {
                result.push(row);
                if result.len() as i64 == target {
                    return Ok(result);
                }
            }
        }
        if !more {
            return Ok(result);
        }
    }
}

fn catalog_page(
    bucket: Uuid,
    filter: &CatalogFilter,
    cursor: Option<&CatalogCursor>,
    (min, max): (Option<i64>, Option<i64>),
    limit: i64,
) -> Result<sqlx::QueryBuilder<Sqlite>> {
    let mut query = sqlx::QueryBuilder::<Sqlite>::new(
        "SELECT stream_id AS id,object_key,CAST(catalog_size AS TEXT) size,catalog_type AS content_type,catalog_public AS public_read,catalog_modified AS modified_at FROM objects WHERE stream_id IS NOT NULL AND bucket_id=",
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
            _ => {}
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
        query
            .push(" AND catalog_modified>=")
            .push_bind(since.timestamp_micros());
    }
    if let Some(until) = filter.until {
        query
            .push(" AND catalog_modified<=")
            .push_bind(until.timestamp_micros());
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
                .push_bind(cursor.key.as_bytes());
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
                query.push_bind(cursor.modified.timestamp_micros());
            }
            query.push(",").push_bind(cursor.key.as_bytes()).push(")");
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
    query.push(" LIMIT ").push_bind(limit);
    Ok(query)
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
        transactions::browse_catalog(&self.read, authority, bucket, filter, cursor, sizes).await
    }
    async fn extent_count(&self, stream: Id, raw: bool) -> Result<i64> {
        let sql = if raw {
            "SELECT COUNT(*) FROM raw_ranges WHERE stream_id=?"
        } else {
            "SELECT COUNT(*) FROM extents WHERE stream_id=?"
        };
        Ok(sqlx::query_scalar(sql).bind(stream).fetch_one(&self.read).await?)
    }
    async fn chunks(&self, stream: Id, after: i64, limit: i64) -> Result<Vec<Value>> {
        let rows: Vec<(Value,Option<Uuid>)> = sqlx::query_as("SELECT JSON_OBJECT('id',CAST(c.id AS TEXT),'offset_bytes',CAST(e.offset_bytes AS TEXT),'length',e.length,'source_offset',e.source_offset,'raw_size',c.raw_size,'stored_size',COALESCE(m.encoded_size,l.standalone_size),'independent_size_hint',c.standalone_size,'payload_size',c.standalone_size-IIF(COALESCE(m.encryption_algorithm,l.encryption_algorithm,c.encryption_algorithm)='none',0,16),'compression',IIF(COALESCE(m.compressed,l.standalone_compressed,c.standalone_compressed),'zstd','none'),'algorithm',COALESCE(m.encryption_algorithm,l.encryption_algorithm,c.encryption_algorithm),'pack_offset',m.offset_bytes,'pack_ordinal',m.ordinal,'uuid',lower(hex(c.uuid)),'pack_uuid',CASE WHEN p.location_id IS NOT NULL THEN lower(hex(p.location_id)) END,'pack_count',p.member_count,'pack_size',p.stored_size,'pack_id',CAST(c.pack_id AS TEXT),'source',CASE WHEN l.encoding_id IS NOT NULL THEN 'chunk' WHEN c.pack_id IS NOT NULL THEN 'pack' ELSE 'pending' END,'reads',CAST(COALESCE(a.read_count,0) AS TEXT),'range_reads',CAST(COALESCE(a.range_read_count,0) AS TEXT)),CASE WHEN m.pack_id IS NULL THEN c.block_key_id ELSE m.block_key_id END FROM extents e JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' LEFT JOIN pack_members m ON l.encoding_id IS NULL AND m.pack_id=c.pack_id AND m.chunk_id=c.id LEFT JOIN packs p ON p.id=m.pack_id LEFT JOIN chunk_access_stats a ON a.chunk_id=c.id WHERE e.stream_id=? AND e.offset_bytes>? ORDER BY e.offset_bytes LIMIT ?").bind(stream).bind(after).bind(limit).fetch_all(&self.read).await?;
        rows.into_iter()
            .map(|(mut row, key)| {
                row["block_key_id"] = serde_json::to_value(key)?;
                Ok(row)
            })
            .collect()
    }
    async fn change(&self, input: ObjectChange) -> Result<bool> {
        transactions::change_object(&self.write, input).await
    }
    async fn begin_operation(
        &self,
        principal: &Principal,
        user: Uuid,
        client: Uuid,
        hash: &[u8],
    ) -> Result<(Uuid, Vec<u8>)> {
        transactions::begin_media_operation(&self.write, principal, user, client, hash).await
    }
    async fn operation(&self, id: Uuid) -> Result<Option<Outcome>> {
        let result: Option<sqlx::types::Json<Outcome>> =
            sqlx::query_scalar("SELECT result FROM media_operations WHERE id=?")
                .bind(id)
                .fetch_one(&self.read)
                .await?;
        Ok(result.map(|r| r.0))
    }
    async fn fail_operation(&self, id: Uuid, outcome: &Outcome) -> Result<()> {
        save_media_operation(id, outcome)
            .execute(&self.write)
            .await?;
        Ok(())
    }
    async fn audit_batch(&self, bucket: Uuid, detail: Value, partial: bool) -> Result<()> {
        transactions::audit_object_batch(&self.write, bucket, detail, partial).await
    }
    async fn storage(&self, bucket: Uuid) -> Result<Option<Uuid>> {
        Ok(
            sqlx::query_scalar("SELECT storage_id FROM buckets WHERE id=?")
                .bind(bucket)
                .fetch_optional(&self.read)
                .await?,
        )
    }
    async fn move_object(
        &self,
        permit: &crate::authorization::Permit,
        input: ObjectTransfer<'_>,
    ) -> Result<()> {
        transactions::move_object(&self.write, permit, input).await
    }
    async fn publish_copy(
        &self,
        permit: &crate::authorization::Permit,
        input: ObjectTransfer<'_>,
        stream: Id,
        public: bool,
        expected_public: Option<bool>,
    ) -> Result<()> {
        transactions::publish_copy(&self.write, permit, input, stream, public, expected_public)
            .await
    }
    async fn queue_migration(
        &self,
        permit: &crate::authorization::Permit,
        job: &StorageMigration,
    ) -> Result<()> {
        transactions::queue_storage_migration(&self.write, permit, job).await
    }
    async fn resume_migration(
        &self,
        task: crate::model::task::Execution,
        job: &StorageMigration,
    ) -> Result<bool> {
        transactions::resume_migration(&self.write, task, job).await
    }
    async fn complete_migration(
        &self,
        task: Uuid,
        token: Uuid,
        stream: Id,
        job: &StorageMigration,
    ) -> Result<()> {
        transactions::complete_storage_migration(
            &self.write,
            Execution { id: task, token },
            stream,
            job,
        )
        .await
    }
    async fn abandon_copy(&self, stream: Id) -> Result<()> {
        transactions::abandon_copy(&self.write, stream).await
    }
    async fn abandon_migration(&self, stream: Id) -> Result<()> {
        transactions::abandon_copy(&self.write, stream).await
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for MediaItem {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            object_key: object_key(row)?,
            size: row.try_get("size")?,
            content_type: row.try_get("content_type")?,
            public_read: row.try_get("public_read")?,
            modified_at: date(row, "modified_at")?,
        })
    }
}

pub(in super::super) fn managed_keys() -> Rows<'static, ManagedKeyMaterial> {
    sqlx::query_as(
        "SELECT id,name,purpose,key_algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE secret_encrypted IS NOT NULL ORDER BY id",
    )
}

pub(in super::super) fn required_keys() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT DISTINCT block_key_id FROM (SELECT block_key_id FROM chunk_encodings WHERE encryption_algorithm<>'none' AND state<>'deleted' UNION SELECT block_key_id FROM packs WHERE encryption_algorithm<>'none' AND state<>'deleted' UNION SELECT block_key_id FROM chunks WHERE encryption_algorithm<>'none' AND EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=chunks.id)) sources WHERE block_key_id IS NOT NULL",
    )
}
