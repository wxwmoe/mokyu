use super::*;

pub(super) fn pack_layout(pack: Id, referenced: bool) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new("SELECT ");
    query.push(CHUNK_COLUMNS).push(",m.offset_bytes,c.pack_id FROM pack_members m JOIN chunks c ON c.id=m.chunk_id AND c.pack_id=m.pack_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE m.pack_id=").push_bind(pack).push(" AND c.state='ready'");
    if referenced {
        query.push(" AND EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)");
    }
    query.push(" ORDER BY m.ordinal LIMIT 4096");
    query
}

#[derive(sqlx::FromRow)]
pub(super) struct PackCandidateRow {
    #[sqlx(flatten)]
    pub(super) row: Mapping,
    pub(super) eligible: bool,
    pub(super) unit_count: i64,
}

#[async_trait::async_trait]
impl PackingStore for Backend {
    async fn in_storage(&self, pack: Id, storage: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM packs WHERE id=? AND storage_id=? AND state='ready')",
        )
        .bind(pack)
        .bind(storage)
        .fetch_one(&self.pool)
        .await?)
    }
    async fn scoped_page(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        after: Id,
        limit: i64,
        state: Option<&str>,
    ) -> Result<Vec<PackSummary>> {
        transactions::scoped_pack_page(&self.pool, principal, scope, after, limit, state).await
    }
    async fn scoped_detail(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        id: Id,
        after: Id,
        limit: i64,
    ) -> Result<(PackSummary, Vec<PackMember>, bool)> {
        transactions::scoped_pack_detail(&self.pool, principal, scope, id, after, limit).await
    }
    async fn scoped_objects(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        id: Id,
        cursor: Option<(Uuid, String)>,
    ) -> Result<Vec<PackObject>> {
        transactions::scoped_pack_objects(&self.pool, principal, scope, id, cursor).await
    }
    async fn page(&self, after: Id, limit: i64) -> Result<Vec<Value>> {
        sqlx::query("SELECT * FROM packs WHERE id>? ORDER BY id LIMIT ?")
            .bind(after)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(pack_value)
            .collect()
    }
    async fn detail(&self, id: Id) -> Result<(Value, Vec<Value>)> {
        let pack = sqlx::query("SELECT * FROM packs WHERE id=?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        let members = sqlx::query_scalar("SELECT JSON_OBJECT('ordinal',m.ordinal,'chunk_id',CAST(m.chunk_id AS CHAR),'offset',m.offset_bytes,'raw_size',c.raw_size,'current',JSON_EXTRACT(IF(c.pack_id=m.pack_id,'true','false'),'$')) FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=? ORDER BY m.ordinal LIMIT 4096").bind(id).fetch_all(&self.pool).await?;
        Ok((pack_value(pack)?, members))
    }
    async fn rewrite_counts(
        &self,
        storage: Uuid,
        algorithm: &str,
        key: Option<Uuid>,
    ) -> Result<(i64, i64)> {
        Ok(rewrite_counts(storage, algorithm, key)
            .build_query_as()
            .fetch_one(&self.pool)
            .await?)
    }
    async fn start_rewrite(&self, job: &RewriteTask, node: Option<Uuid>) -> Result<(Uuid, bool)> {
        ensure!(node.is_none(), "MySQL does not support cluster nodes");
        transactions::start_rewrite(&self.pool, job).await
    }
    async fn rewrite_candidates(
        &self,
        after: Id,
        job: &RewriteTask,
    ) -> Result<(Option<Chunk>, Option<(Id, Id)>)> {
        let loose = rewrite_loose_candidate(after, job)
            .build_query_as()
            .fetch_optional(&self.pool)
            .await?;
        let pack = rewrite_pack_candidate(after, job)
            .build_query_as()
            .fetch_optional(&self.pool)
            .await?;
        Ok((loose, pack))
    }
    async fn rewrite_pack_inputs(&self, pack: Id) -> Result<(Pack, Vec<Chunk>, Vec<Id>)> {
        let old = pack_record(pack).fetch_one(&self.pool).await?;
        let members = rewrite_members(pack)
            .build_query_as::<Chunk>()
            .fetch_all(&self.pool)
            .await?;
        ensure!(
            members.len() == old.member_count as usize,
            "pack member count changed during rewrite"
        );
        let current = rewrite_current_members(pack).fetch_all(&self.pool).await?;
        Ok((old, members, current))
    }
    async fn commit_rewrite_chunk(
        &self,
        task: Uuid,
        token: Uuid,
        source: &Chunk,
        physical: &Chunk,
    ) -> Result<()> {
        transactions::commit_rewrite_chunk(
            &self.pool,
            Execution { id: task, token },
            source,
            physical,
        )
        .await
    }
    async fn commit_rewrite_pack(
        &self,
        task: Uuid,
        token: Uuid,
        old: &Pack,
        new: &Pack,
        current: &[Id],
        members: &[(Id, crate::content::pack::frame::Entry)],
        first: Id,
    ) -> Result<()> {
        transactions::commit_rewrite_pack(
            &self.pool,
            Execution { id: task, token },
            old,
            new,
            current,
            members,
            first,
        )
        .await
    }
    async fn complete_rewrite(&self, task: Uuid, token: Uuid, after: Id) -> Result<()> {
        sqlx::query("UPDATE tasks SET state='completed',progress_cursor=?,updated_at=CURRENT_TIMESTAMP(6) WHERE id=? AND execution_token=? AND state='running'").bind(after.to_string()).bind(task).bind(token).execute(&self.pool).await?;
        Ok(())
    }
    async fn pack_totals(&self) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT JSON_OBJECT('state',state,'count',COUNT(*),'raw_bytes',CAST(COALESCE(SUM(raw_size),0) AS SIGNED),'stored_bytes',CAST(COALESCE(SUM(stored_size),0) AS SIGNED)) FROM packs GROUP BY state").fetch_all(&self.pool).await?)
    }
    async fn unpack_totals(&self, pack: Option<Id>) -> Result<(i64, i64)> {
        Ok(sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(raw_size),0) AS SIGNED) FROM packs WHERE state='ready' AND (? IS NULL OR id=?)").bind(pack).bind(pack).fetch_one(&self.pool).await?)
    }
    async fn unpack_identity(&self, pack: Option<Id>) -> Result<(String, String)> {
        Ok(sqlx::query_as("SELECT CAST(COALESCE(MAX(id),0) AS CHAR),CAST(COALESCE(SUM(id),0) AS CHAR) FROM packs WHERE state='ready' AND (? IS NULL OR id=?)").bind(pack).bind(pack).fetch_one(&self.pool).await?)
    }
    async fn preparing_packs(&self) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM packs WHERE state='preparing')")
                .fetch_one(&self.pool)
                .await?,
        )
    }
    async fn ready_encoding(&self, chunk: Id) -> Result<Option<(Id, Uuid, i32)>> {
        super::pack_work::ready_encoding(&self.pool, chunk).await
    }
    async fn restore_encoding(&self, physical: &Chunk, task: Execution) -> Result<Id> {
        transactions::restore_member_encoding(&self.pool, physical, task).await
    }

    async fn prepare_encoding(&self, physical: &Chunk, task: Execution) -> Result<Id> {
        transactions::prepare_encoding(&self.pool, physical, task).await
    }
    async fn encoding_metadata(&self, physical: &Chunk) -> Result<()> {
        transactions::register_encoding(&self.pool, physical).await
    }
    async fn prepare_pack(
        &self,
        storage: Uuid,
        algorithm: &str,
        key: Option<Uuid>,
        bytes: i64,
        count: i32,
        task: Execution,
    ) -> Result<Pack> {
        transactions::prepare_pack(&self.pool, storage, algorithm, key, bytes, count, task).await
    }
    async fn discard_pack(&self, pack: Id) -> Result<()> {
        sqlx::query("UPDATE packs SET state='retired',unreferenced_at=COALESCE(unreferenced_at,CURRENT_TIMESTAMP(6)) WHERE id=? AND state='preparing'").bind(pack).execute(&self.pool).await?;
        Ok(())
    }
    async fn pack_metadata(&self, pack: &Pack) -> Result<()> {
        transactions::pack_metadata(&self.pool, pack).await
    }
    async fn pack_has_pending(&self, pack: Id) -> Result<bool> {
        super::pack_work::pack_has_pending(&self.pool, pack).await
    }
    async fn pack_layout(&self, pack: Id) -> Result<Vec<Mapping>> {
        super::pack_work::pack_layout(&self.pool, pack).await
    }
    async fn pack_has_writers(&self, pack: Id) -> Result<bool> {
        super::pack_work::pack_has_writers(&self.pool, pack).await
    }
    async fn pack_raw_size(&self, pack: Id) -> Result<i64> {
        super::pack_work::pack_raw_size(&self.pool, pack).await
    }
    async fn chunk_has_references(&self, chunk: Id) -> Result<bool> {
        super::pack_work::chunk_has_references(&self.pool, chunk).await
    }
    async fn pack_stored_size(&self, pack: Id) -> Result<i64> {
        super::pack_work::pack_stored_size(&self.pool, pack).await
    }
    async fn pack_candidates(
        &self,
        stream: Id,
        offset: i64,
        cooldown: f64,
    ) -> Result<Vec<PackCandidate>> {
        super::pack_work::pack_candidates(&self.pool, stream, offset, cooldown).await
    }
    async fn pack_member_count(&self, pack: Id) -> Result<i64> {
        super::pack_work::pack_member_count(&self.pool, pack).await
    }
    async fn pack_stream_metadata(&self, stream: Id) -> Result<(Value, String)> {
        let (metadata, key): (Value, Vec<u8>) =
            sqlx::query_as("SELECT metadata,object_key FROM streams WHERE id=?")
                .bind(stream)
                .fetch_one(&self.pool)
                .await?;
        Ok((metadata, String::from_utf8(key)?))
    }
    async fn physical_bytes(&self, ids: &[Id]) -> Result<i64> {
        super::pack_work::physical_bytes(&self.pool, ids).await
    }
    async fn pause_pack_task(&self, task: Execution) -> Result<()> {
        pause_owned_task(task, None).execute(&self.pool).await?;
        Ok(())
    }
    async fn next_pack(
        &self,
        after: Id,
        pack: Option<Id>,
        upper: Option<DateTime<Utc>>,
        storage: Option<Uuid>,
    ) -> Result<Option<Id>> {
        super::pack_work::next_pack(&self.pool, after, pack, upper, storage).await
    }
    async fn next_pack_stream(
        &self,
        after: Id,
        offset: i64,
        upper: DateTime<Utc>,
        storage: Option<Uuid>,
    ) -> Result<Option<Id>> {
        super::pack_work::next_pack_stream(&self.pool, after, offset, upper, storage).await
    }
    async fn advance_pack_task(
        &self,
        task: Execution,
        next: &Option<String>,
        processed: i64,
    ) -> Result<()> {
        if let Some(next) = next {
            sqlx::query("UPDATE tasks SET progress_cursor=?,processed=processed+?,updated_at=CURRENT_TIMESTAMP(6) WHERE id=? AND execution_token=? AND state='running'").bind(next).bind(processed).bind(task.id).bind(task.token).execute(&self.pool).await?;
            Ok(())
        } else {
            transactions::advance_pack_storage(&self.pool, task, processed).await
        }
    }
    async fn next_pack_change(
        &self,
        cutoff: DateTime<Utc>,
        policy: &str,
        storage: Option<Uuid>,
    ) -> Result<Option<(Id, i64)>> {
        super::pack_work::next_pack_change(&self.pool, cutoff, policy, storage).await
    }
    async fn finish_pack_change(&self, task: Execution, pack: Id, generation: i64) -> Result<()> {
        transactions::finish_pack_change(&self.pool, task, pack, generation).await
    }
    async fn next_pack_queue(
        &self,
        cutoff: DateTime<Utc>,
        policy: &str,
        storage: Option<Uuid>,
    ) -> Result<Option<(Id, i64, i64)>> {
        super::pack_work::next_pack_queue(&self.pool, cutoff, policy, storage).await
    }
    async fn stream_repack_after(
        &self,
        stream: Id,
        cooldown: f64,
    ) -> Result<Option<DateTime<Utc>>> {
        super::pack_work::stream_repack_after(&self.pool, stream, cooldown).await
    }
    async fn defer_pack_queue(
        &self,
        task: Execution,
        stream: Id,
        generation: i64,
        later: DateTime<Utc>,
    ) -> Result<()> {
        transactions::defer_pack_queue(&self.pool, task, stream, generation, later).await
    }
    async fn finish_pack_queue(&self, task: Execution, stream: Id, generation: i64) -> Result<()> {
        transactions::finish_pack_queue(&self.pool, task, stream, generation).await
    }
    async fn advance_pack_queue(
        &self,
        task: Execution,
        stream: Id,
        generation: i64,
        cursor: i64,
    ) -> Result<()> {
        transactions::advance_pack_queue(&self.pool, task, stream, generation, cursor).await
    }
    async fn pin_pack_inputs(&self, task: Execution, rows: &[Mapping]) -> Result<()> {
        transactions::pin_pack_inputs(&self.pool, task, rows).await
    }
    async fn start_pack(&self, policy: &str, automatic: bool, node: Option<Uuid>) -> Result<Value> {
        ensure!(node.is_none(), "MySQL does not support cluster nodes");
        transactions::start_pack(&self.pool, policy, automatic).await
    }
    async fn start_unpack(&self, pack: Option<Id>, all: bool, node: Option<Uuid>) -> Result<Value> {
        ensure!(node.is_none(), "MySQL does not support cluster nodes");
        transactions::start_unpack(&self.pool, pack, all).await
    }
    async fn insert_pack_members(
        &self,
        pack: Id,
        rows: &[Mapping],
        entries: &[(Id, crate::content::pack::frame::Entry)],
    ) -> Result<()> {
        transactions::insert_pack_members(&self.pool, pack, rows, entries).await
    }
    async fn commit_pack_outputs(
        &self,
        task: Execution,
        rows: &[Mapping],
        outputs: &[PackOutput],
        split: bool,
        settings: &crate::content::pack::Config,
        flush_seconds: f64,
    ) -> Result<()> {
        transactions::commit_pack_outputs(
            &self.pool,
            task,
            rows,
            outputs,
            split,
            settings,
            flush_seconds,
        )
        .await
    }
    async fn same_references(&self, a: Id, b: Id, distance: i64) -> Result<bool> {
        Ok(same_references(a, b, distance)
            .fetch_one(&self.pool)
            .await?)
    }
    async fn next_range_pack(
        &self,
        storage: Uuid,
        cutoff: DateTime<Utc>,
        window: f64,
        minimum: i64,
    ) -> Result<Option<Id>> {
        super::pack_work::next_range_pack(&self.pool, storage, cutoff, window, minimum).await
    }
    async fn complete_range_task(&self, task: Execution) -> Result<()> {
        transactions::advance_pack_storage(&self.pool, task, 0).await
    }
    async fn range_pack_blocked(&self, pack: Id) -> Result<bool> {
        super::pack_work::range_pack_blocked(&self.pool, pack).await
    }
    async fn pack_range_totals(&self, pack: Id, window: f64) -> Result<(i64, i64, i64)> {
        super::pack_work::pack_range_totals(&self.pool, pack, window).await
    }
    async fn pack_member_hits(&self, pack: Id, window: f64) -> Result<Vec<(Id, i64)>> {
        super::pack_work::pack_member_hits(&self.pool, pack, window).await
    }
    async fn pack_dimensions(&self, pack: Id) -> Result<(i64, i64, i32)> {
        super::pack_work::pack_dimensions(&self.pool, pack).await
    }
    async fn range_pack_layout(&self, pack: Id) -> Result<Vec<Mapping>> {
        super::pack_work::range_pack_layout(&self.pool, pack).await
    }
    async fn defer_range_repack(
        &self,
        stream: Id,
        window: f64,
        flush: f64,
        retry: f64,
        offset: i64,
    ) -> Result<()> {
        super::pack_work::defer_range_repack(&self.pool, stream, window, flush, retry, offset).await
    }
    async fn record_range(
        &self,
        task: Execution,
        id: Id,
        processed: usize,
        decision: Value,
    ) -> Result<()> {
        transactions::record_range(&self.pool, task, id, processed, decision).await
    }
}

pub(in super::super) fn usage_scope_buckets(
    user: Option<Uuid>,
    admin: bool,
    token: Option<Uuid>,
    bucket: Option<Uuid>,
    project: Option<Uuid>,
) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("/*M!100603 SET STATEMENT max_statement_time=5 FOR */ SELECT /*+ MAX_EXECUTION_TIME(5000) */ b.id FROM buckets b LEFT JOIN user_bucket_access a ON a.bucket_id=b.id AND a.user_id=? LEFT JOIN token_bucket_access t ON t.bucket_id=b.id AND t.token_id=? WHERE (? OR (IF(? IS NULL,a.actions,t.actions)&64)<>0) AND (? IS NULL OR b.id=?) AND (? IS NULL OR b.project_id=?) ORDER BY b.id").bind(user).bind(token).bind(admin).bind(token).bind(bucket).bind(bucket).bind(project).bind(project)
}

pub(super) fn uuid_ids(query: &mut sqlx::QueryBuilder<MySql>, ids: &[Uuid]) {
    query.push("(");
    if ids.is_empty() {
        query.push("NULL");
    } else {
        let mut values = query.separated(",");
        for id in ids {
            values.push_bind(*id);
        }
    }
    query.push(")");
}

fn scoped_packs(ids: &[Uuid], admin: bool, all: bool) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "/*M!100603 SET STATEMENT max_statement_time=5 FOR */ SELECT /*+ MAX_EXECUTION_TIME(5000) */ CAST(p.id AS CHAR) AS id,p.state,p.pack_compressed AS compressed,",
    );
    query.push(if admin { "CAST(p.raw_size AS CHAR) AS raw_size,CAST(p.stored_size AS CHAR) AS stored_size,p.member_count" } else { "NULL AS raw_size,NULL AS stored_size,NULL AS member_count" });
    query.push(",p.created_at FROM packs p WHERE TRUE");
    if !admin {
        query.push(" AND p.state='ready'");
    }
    if !all {
        query.push(" AND EXISTS(SELECT 1 FROM pack_members m JOIN chunks c ON c.id=m.chunk_id AND c.pack_id=m.pack_id JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.stream_id=s.id WHERE m.pack_id=p.id AND o.bucket_id IN ");
        uuid_ids(&mut query, ids);
        query.push(")");
    }
    query
}

pub(in super::super) fn scoped_pack_page(
    ids: &[Uuid],
    admin: bool,
    all: bool,
    after: Id,
    limit: i64,
    state: Option<&str>,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = scoped_packs(ids, admin, all);
    query.push(" AND p.id>").push_bind(after);
    if let Some(state) = state {
        query.push(" AND p.state=").push_bind(state.to_owned());
    }
    query.push(" ORDER BY p.id LIMIT ").push_bind(limit);
    query
}

pub(in super::super) fn scoped_pack(
    ids: &[Uuid],
    admin: bool,
    all: bool,
    id: Id,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = scoped_packs(ids, admin, all);
    query.push(" AND p.id=").push_bind(id);
    query
}

pub(in super::super) fn scoped_pack_members(
    id: Id,
    ids: &[Uuid],
    admin: bool,
    all: bool,
    after: Id,
    limit: i64,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "/*M!100603 SET STATEMENT max_statement_time=5 FOR */ WITH selected_members AS (SELECT c.id,c.raw_size,c.pack_id,m.ordinal FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=",
    );
    query.push_bind(id).push(" AND c.id>").push_bind(after);
    if !all {
        query.push(" AND c.pack_id=m.pack_id AND EXISTS(SELECT 1 FROM extents e JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.stream_id=s.id WHERE e.chunk_id=c.id AND o.bucket_id IN ");
        uuid_ids(&mut query, ids);
        query.push(")");
    }
    query.push(" ORDER BY c.id LIMIT ").push_bind(limit).push("),visible_ranges AS (SELECT DISTINCT m.id,e.source_offset AS lo,e.source_offset+e.length AS hi FROM selected_members m JOIN extents e ON e.chunk_id=m.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.stream_id=s.id WHERE o.bucket_id IN ");
    uuid_ids(&mut query, ids);
    // Running maximum, rather than the preceding endpoint, also merges nested intervals.
    query.push("),range_order AS (SELECT id,lo,hi,MAX(hi) OVER(PARTITION BY id ORDER BY lo,hi ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_end FROM visible_ranges),range_groups AS (SELECT id,lo,hi,SUM(previous_end IS NULL OR lo>previous_end) OVER(PARTITION BY id ORDER BY lo,hi ROWS UNBOUNDED PRECEDING) AS span_id FROM range_order),range_spans AS (SELECT id,span_id,MAX(hi)-MIN(lo) AS bytes FROM range_groups GROUP BY id,span_id),visible AS (SELECT id,SUM(bytes) AS bytes FROM range_spans GROUP BY id) SELECT /*+ MAX_EXECUTION_TIME(5000) */ CAST(m.id AS CHAR) AS chunk_id,");
    query.push(if admin {
        "m.ordinal,m.raw_size"
    } else {
        "NULL AS ordinal,NULL AS raw_size"
    });
    query.push(",CAST(COALESCE(v.bytes,0) AS CHAR) AS visible_bytes,COALESCE(m.pack_id=").push_bind(id).push(" AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=m.id AND state='ready'),FALSE) AS current_source FROM selected_members m LEFT JOIN visible v ON v.id=m.id ORDER BY m.id");
    query
}

pub(in super::super) fn scoped_pack_objects(
    id: Id,
    ids: &[Uuid],
    principal: &Principal,
    admin: bool,
    cursor: Option<(Uuid, String)>,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "/*M!100603 SET STATEMENT max_statement_time=5 FOR */ SELECT /*+ MAX_EXECUTION_TIME(5000) */ DISTINCT o.bucket_id,b.name AS bucket_name,o.object_key AS object_key,s.id AS version,CAST(s.size AS CHAR) AS size,COALESCE(JSON_UNQUOTE(JSON_EXTRACT(s.metadata,'$.content_type')),'application/octet-stream') AS content_type,s.public_read FROM pack_members m JOIN chunks c ON c.id=m.chunk_id JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.stream_id=s.id JOIN buckets b ON b.id=o.bucket_id LEFT JOIN user_bucket_access a ON a.bucket_id=b.id AND a.user_id=",
    );
    query
        .push_bind(principal.user_id())
        .push(" LEFT JOIN token_bucket_access t ON t.bucket_id=b.id AND t.token_id=")
        .push_bind(principal.token_id())
        .push(" WHERE m.pack_id=")
        .push_bind(id)
        .push(" AND o.bucket_id IN ");
    uuid_ids(&mut query, ids);
    if !admin {
        query
            .push(" AND c.pack_id=m.pack_id AND (IF(")
            .push_bind(principal.token_id())
            .push(" IS NULL,a.actions,t.actions)&1)<>0");
    }
    if let Some((bucket, key)) = cursor {
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
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for PackSummary {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            state: row.try_get("state")?,
            compressed: row.try_get("compressed")?,
            raw_size: row.try_get("raw_size")?,
            stored_size: row.try_get("stored_size")?,
            member_count: row.try_get("member_count")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for PackMember {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        Ok(Self {
            chunk_id: row.try_get("chunk_id")?,
            ordinal: row.try_get("ordinal")?,
            raw_size: row.try_get("raw_size")?,
            visible_bytes: row.try_get("visible_bytes")?,
            current_source: row.try_get("current_source")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for PackObject {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        let key = String::from_utf8(row.try_get("object_key")?).map_err(|e| {
            sqlx::Error::ColumnDecode {
                index: "object_key".into(),
                source: Box::new(e),
            }
        })?;
        Ok(Self {
            bucket_id: row.try_get("bucket_id")?,
            bucket_name: row.try_get("bucket_name")?,
            key,
            version: row.try_get("version")?,
            size: row.try_get("size")?,
            content_type: row.try_get("content_type")?,
            public_read: row.try_get("public_read")?,
        })
    }
}

const PACK_FINGERPRINT: &str = "UNHEX(SHA2(CONCAT_WS(':',HEX(k.block_key_id),k.encryption_algorithm,k.pack_compressed,k.member_count,p.compression_enabled,p.compression_strategy,p.compression_level,p.compression_sample_level,p.compression_min_savings_percent,p.compression_min_savings_bytes,p.pack_max_size,p.pack_range_enabled,p.pack_range_min_downloads,p.pack_range_min_savings_percent,p.pack_range_min_savings_bytes,'pack'),256))";

pub(in super::super) fn lock_pack_layout(ids: &[Id]) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT id,pack_id,storage_id FROM chunks WHERE state='ready' AND id IN ",
    );
    chunk_ids(&mut query, ids);
    query.push(" ORDER BY id FOR UPDATE");
    query
}

pub(in super::super) fn pack_input_owners(ids: &[Id]) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new("SELECT task_id FROM pack_inputs WHERE chunk_id IN ");
    chunk_ids(&mut query, ids);
    query
}

pub(in super::super) fn lock_cache_inputs(
    ids: &[Id],
    task: Uuid,
    flush: bool,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT c.id,c.current_encoding_id,c.pack_id,p.stream_id,p.offset_bytes,p.source_pack_id FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE c.id IN ",
    );
    chunk_ids(&mut query, ids);
    query.push(" AND (p.owner_task IS NULL OR p.owner_task=").push_bind(task)
        .push(" OR (").push_bind(flush)
        .push(" AND p.owner_task IN (SELECT id FROM tasks WHERE state IN ('paused','failed')))) ORDER BY c.id FOR UPDATE");
    query
}

pub(in super::super) fn insert_pack_inputs(task: Uuid, ids: &[Id]) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new("INSERT INTO pack_inputs(task_id,chunk_id) SELECT ");
    query
        .push_bind(task)
        .push(",c.id FROM chunks c WHERE c.id IN ");
    chunk_ids(&mut query, ids);
    query.push(" AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=c.id)");
    query
}

pub(in super::super) fn prepare_encoding(physical: &Chunk, task: Execution) -> Statement<'_> {
    sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,state,created_at,unreferenced_at,owner_task,encryption_algorithm,block_key_id,encoding_fingerprint) VALUES(?,?,?,'uploading',?,CURRENT_TIMESTAMP(6),?,?,?,UNHEX(REPEAT('00',32)))")
        .bind(physical.id).bind(physical.location_id).bind(physical.storage_id).bind(physical.created_at).bind(task.id).bind(&physical.algorithm).bind(physical.block_key_id)
}

pub(in super::super) fn prepare_pack(
    storage: Uuid,
    algorithm: &str,
    key: Option<Uuid>,
    bytes: i64,
    count: i32,
    task: Execution,
) -> Statement<'_> {
    sqlx::query("INSERT INTO packs(location_id,storage_id,encryption_algorithm,block_key_id,raw_size,member_count,state,unreferenced_at,owner_task,pack_fingerprint) VALUES(?,?,?,?,?,?,'preparing',CURRENT_TIMESTAMP(6),?,UNHEX(REPEAT('00',32)))")
        .bind(Uuid::now_v7()).bind(storage).bind(algorithm).bind(key).bind(bytes).bind(count).bind(task.id)
}

pub(in super::super) async fn refresh_pack_fingerprint(
    connection: &mut sqlx::MySqlConnection,
    pack: Id,
) -> Result<()> {
    let mut query = sqlx::QueryBuilder::new(
        "UPDATE packs k JOIN storage_profiles p ON p.storage_id=k.storage_id SET k.pack_fingerprint=",
    );
    query
        .push(PACK_FINGERPRINT)
        .push(" WHERE k.id=")
        .push_bind(pack);
    query.build().execute(connection).await?;
    Ok(())
}

pub(in super::super) fn pack_record(id: Id) -> Rows<'static, Pack> {
    sqlx::query_as("SELECT (SELECT h.material FROM tier_locations h WHERE h.unit=1 AND h.physical_id=p.location_id) AS tier_material,p.* FROM packs p WHERE id=?").bind(id)
}

pub(in super::super) fn lock_prepared_pack(id: Id) -> Rows<'static, Pack> {
    sqlx::query_as("SELECT * FROM packs WHERE id=? AND state='preparing' FOR UPDATE").bind(id)
}

pub(in super::super) fn pack_metadata(pack: &Pack) -> Statement<'_> {
    sqlx::query("UPDATE packs SET stored_size=?,pack_compressed=?,nonce=?,nonce_id=?,digest=? WHERE id=? AND location_id=? AND storage_id=? AND encryption_algorithm=? AND block_key_id<=>? AND state='preparing'")
        .bind(pack.stored_size).bind(pack.compressed).bind(&pack.nonce).bind(crate::content::nonce::id(pack.nonce.as_deref())).bind(&pack.digest).bind(pack.id).bind(pack.location_id).bind(pack.storage_id).bind(&pack.algorithm).bind(pack.block_key_id)
}

pub(in super::super) async fn reserve_member_encoding(
    db: &mut sqlx::MySqlConnection,
    c: &Chunk,
    task: Uuid,
) -> Result<Id> {
    crate::content::pack::frame::Encoding::capture(c)?;
    let previous: Option<(Id, String, Option<Uuid>, Option<i32>, bool, String, Option<Uuid>, Option<Vec<u8>>)> = sqlx::query_as("SELECT encoding_id,state,owner_task,standalone_size,standalone_compressed,encryption_algorithm,block_key_id,nonce FROM chunk_encodings WHERE location_id=? AND chunk_id=? AND storage_id=? AND quarantined_at IS NULL FOR UPDATE")
        .bind(c.location_id).bind(c.id).bind(c.storage_id).fetch_optional(&mut *db).await?;
    let counter = crate::content::nonce::id(c.nonce.as_deref());
    let id = if let Some((id, state, owner, size, compressed, algorithm, key, nonce)) = previous {
        ensure!(
            algorithm == c.algorithm
                && key == c.block_key_id
                && nonce.as_ref().is_none_or(|n| Some(n) == c.nonce.as_ref())
                && size.is_none_or(|n| Some(n) == c.stored_size && compressed == c.compressed),
            "member encoding identity changed"
        );
        ensure!(
            matches!(state.as_str(), "retired" | "deleted")
                || state == "uploading" && owner == Some(task),
            "member encoding is in use"
        );
        sqlx::query("UPDATE chunk_encodings SET state='uploading',owner_task=?,standalone_size=?,standalone_compressed=?,encryption_algorithm=?,block_key_id=?,nonce=?,nonce_id=?,deleted_at=NULL,stored_at=NULL,unreferenced_at=UTC_TIMESTAMP(6) WHERE encoding_id=?").bind(task).bind(c.stored_size).bind(c.compressed).bind(&c.algorithm)
            .bind(c.block_key_id).bind(&c.nonce).bind(counter).bind(id).execute(&mut *db).await?;
        id
    } else {
        let inserted = sqlx::query("INSERT INTO chunk_encodings(location_id,chunk_id,storage_id,owner_task,standalone_size,standalone_compressed,encryption_algorithm,block_key_id,nonce,nonce_id,state,unreferenced_at,encoding_fingerprint) VALUES (?,?,?,?,?,?,?,?,?,?,'uploading',UTC_TIMESTAMP(6),UNHEX(REPEAT('00',32)))")
            .bind(c.location_id).bind(c.id).bind(c.storage_id).bind(task).bind(c.stored_size)
            .bind(c.compressed).bind(&c.algorithm).bind(c.block_key_id).bind(&c.nonce).bind(counter)
            .execute(&mut *db).await?;
        let id = i64::try_from(inserted.last_insert_id())?.into();
        id
    };
    refresh_encoding_fingerprint(db, id).await?;
    Ok(id)
}

pub(in super::super) async fn write_pack_index(
    db: &mut sqlx::MySqlConnection,
    pack: Id,
    members: &[(Id, crate::content::pack::frame::Entry)],
) -> Result<()> {
    for batch in members.chunks(512) {
        let mut query = sqlx::QueryBuilder::<sqlx::MySql>::new(
            "INSERT INTO pack_members(pack_id,ordinal,chunk_id,offset_bytes,encoding_uuid,encoded_size,compressed,encryption_algorithm,block_key_id,nonce) ",
        );
        query.push_values(batch, |mut row, (id, member)| {
            let position = &member.position;
            let encoding = &position.encoding;
            row.push_bind(pack)
                .push_bind(position.ordinal as i32)
                .push_bind(*id)
                .push_bind(position.offset as i64)
                .push_bind(encoding.location_id)
                .push_bind(encoding.stored_size)
                .push_bind(encoding.compressed)
                .push_bind(&encoding.algorithm)
                .push_bind(encoding.block_key_id)
                .push_bind(&encoding.nonce);
        });
        query.build().execute(&mut *db).await?;
    }
    Ok(())
}

pub(in super::super) fn pack_member_ids(pack: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT chunk_id FROM pack_members WHERE pack_id=? ORDER BY ordinal")
        .bind(pack)
}

pub(in super::super) fn pack_layout_bytes(ids: &[Id]) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT CAST(COALESCE(SUM(bytes),0) AS SIGNED) FROM (SELECT standalone_size+8 bytes FROM chunk_encodings WHERE state='ready' AND chunk_id IN ",
    );
    chunk_ids(&mut query, ids);
    query.push(" UNION ALL SELECT stored_size FROM packs WHERE id IN (SELECT pack_id FROM chunks WHERE id IN ");
    chunk_ids(&mut query, ids);
    query.push(")) physical");
    query
}

pub(in super::super) fn pack_added_bytes(task: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT CAST(COALESCE(SUM(bytes),0) AS SIGNED) FROM (SELECT stored_size bytes FROM packs WHERE owner_task=? AND state='preparing' UNION ALL SELECT standalone_size+8 FROM chunk_encodings WHERE owner_task=? AND state='uploading') physical").bind(task).bind(task)
}

pub(in super::super) fn pack_task_policy(task: Uuid) -> Rows<'static, (bool, bool)> {
    sqlx::query_as("SELECT task_type IN ('upload','cache_flush'),COALESCE(task_type='pack' AND CAST(JSON_UNQUOTE(JSON_EXTRACT(detail,'$.policy')) AS BINARY)='range',FALSE) FROM tasks WHERE id=?").bind(task)
}

pub(in super::super) fn pack_exclusive(
    ids: &[Id],
    cooldown: f64,
    window: f64,
    flush: f64,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT NOT EXISTS(SELECT 1 FROM chunks c CROSS JOIN access_state a WHERE c.id IN ",
    );
    chunk_ids(&mut query, ids);
    query.push(" AND ((SELECT COUNT(*) FROM extents WHERE chunk_id=c.id)<>1 OR NOT COALESCE(c.repack_after<=CURRENT_TIMESTAMP(6),TRUE) OR (c.split_at IS NOT NULL AND c.reference_changed_at>TIMESTAMPADD(SECOND,-").push_bind(cooldown).push(",CURRENT_TIMESTAMP(6))) OR (c.range_split_at IS NOT NULL AND (a.coverage_since IS NULL OR a.coverage_since>CAST(DATE_FORMAT(TIMESTAMPADD(SECOND,-").push_bind(window).push(",CURRENT_TIMESTAMP(6)),'%Y-%m-%d %H:00:00') AS DATETIME) OR a.flushed_at IS NULL OR a.flushed_at<TIMESTAMPADD(SECOND,-").push_bind(flush).push(",CURRENT_TIMESTAMP(6)) OR EXISTS(SELECT 1 FROM chunk_access_windows w WHERE w.chunk_id=c.id AND w.range_read_count>0 AND w.window_start>=CAST(DATE_FORMAT(TIMESTAMPADD(SECOND,-").push_bind(window).push(",CURRENT_TIMESTAMP(6)),'%Y-%m-%d %H:00:00') AS DATETIME))))))");
    query
}

pub(in super::super) fn publish_prepared_pack(pack: Id, task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE packs SET state='ready',unreferenced_at=NULL,stored_at=CURRENT_TIMESTAMP(6),owner_task=NULL WHERE id=? AND owner_task=? AND state='preparing' AND stored_size IS NOT NULL AND digest IS NOT NULL").bind(pack).bind(task)
}

pub(in super::super) fn ready_pack_encodings(
    ids: &[Id],
    keep: Option<Id>,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT encoding_id FROM chunk_encodings WHERE state='ready' AND chunk_id IN ",
    );
    chunk_ids(&mut query, ids);
    if let Some(keep) = keep {
        query.push(" AND encoding_id<>").push_bind(keep);
    }
    query
}

pub(in super::super) fn retire_pack_encodings(
    chunks: &[Id],
    encodings: &[Id],
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "UPDATE chunk_encodings FORCE INDEX (PRIMARY) SET state='retired',unreferenced_at=CURRENT_TIMESTAMP(6) WHERE encoding_id IN ",
    );
    chunk_ids(&mut query, encodings);
    query.push(" AND state='ready' AND chunk_id IN ");
    chunk_ids(&mut query, chunks);
    query
}

pub(in super::super) fn lock_output_encoding(
    encoding: Id,
    chunk: Id,
) -> Rows<'static, (String, Option<Uuid>)> {
    sqlx::query_as("SELECT state,owner_task FROM chunk_encodings WHERE encoding_id=? AND chunk_id=? AND standalone_size IS NOT NULL FOR UPDATE").bind(encoding).bind(chunk)
}

pub(in super::super) fn publish_chunk_encoding(chunk: Id, encoding: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks c JOIN chunk_encodings e ON e.chunk_id=c.id AND e.encoding_id=? AND e.state='ready' SET c.current_encoding_id=e.encoding_id,c.standalone_size=e.standalone_size,c.standalone_compressed=e.standalone_compressed,c.nonce=e.nonce,c.encryption_algorithm=e.encryption_algorithm,c.block_key_id=e.block_key_id WHERE c.id=? AND c.state='ready'").bind(encoding).bind(chunk)
}

pub(in super::super) fn publish_pack_layout(
    ids: &[Id],
    pack: Option<Id>,
    split: bool,
    cooldown: f64,
    range: bool,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new("UPDATE chunks SET pack_id=");
    query
        .push_bind(pack)
        .push(",split_at=IF(")
        .push_bind(split)
        .push(",CURRENT_TIMESTAMP(6),split_at),repack_after=IF(")
        .push_bind(split)
        .push(",GREATEST(COALESCE(repack_after,CURRENT_TIMESTAMP(6)),TIMESTAMPADD(SECOND,")
        .push_bind(cooldown)
        .push(",CURRENT_TIMESTAMP(6))),repack_after),range_split_at=CASE WHEN ")
        .push_bind(range)
        .push(" THEN CURRENT_TIMESTAMP(6) WHEN ")
        .push_bind(!split && pack.is_some())
        .push(" THEN NULL ELSE range_split_at END WHERE id IN ");
    chunk_ids(&mut query, ids);
    query
}

pub(in super::super) fn pack_has_references(ids: &[Id]) -> sqlx::QueryBuilder<MySql> {
    let mut query =
        sqlx::QueryBuilder::new("SELECT EXISTS(SELECT 1 FROM extents WHERE chunk_id IN ");
    chunk_ids(&mut query, ids);
    query.push(")");
    query
}

pub(in super::super) fn fail_dead_pack_chunks(ids: &[Id]) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "UPDATE chunks SET state='failed',pack_id=NULL,unreferenced_at=COALESCE(unreferenced_at,CURRENT_TIMESTAMP(6)) WHERE id IN ",
    );
    chunk_ids(&mut query, ids);
    query
}

pub(in super::super) fn schedule_repacked_streams(
    ids: &[Id],
    cooldown: f64,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "INSERT INTO pack_maintenance(stream_id,reason,next_check_at) SELECT DISTINCT e.stream_id,'repack',TIMESTAMPADD(SECOND,",
    );
    query.push_bind(cooldown).push(",CURRENT_TIMESTAMP(6)) FROM extents e JOIN streams s ON s.id=e.stream_id WHERE s.state='ready' AND s.stream_type='object' AND e.chunk_id IN ");
    chunk_ids(&mut query, ids);
    query.push(" ON DUPLICATE KEY UPDATE generation=pack_maintenance.generation+1,progress_cursor=0,reason='repack',updated_at=CURRENT_TIMESTAMP(6),next_check_at=TIMESTAMPADD(SECOND,").push_bind(cooldown).push(",CURRENT_TIMESTAMP(6))");
    query
}

pub(in super::super) fn retire_replaced_packs(ids: &[Id]) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "UPDATE packs p SET state='retired',unreferenced_at=CURRENT_TIMESTAMP(6) WHERE state='ready' AND id IN ",
    );
    chunk_ids(&mut query, ids);
    query.push(" AND NOT EXISTS(SELECT 1 FROM chunks WHERE pack_id=p.id AND state='ready')");
    query
}

pub(in super::super) fn record_pack_rewrite(task: Uuid, detail: Value) -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET detail=JSON_SET(detail,'$.last_rewrite',JSON_EXTRACT(?,'$')) WHERE id=?",
    )
    .bind(detail)
    .bind(task)
}

pub(in super::super) fn lock_pack_policy(policy: &str) -> Scalar<'_, String> {
    sqlx::query_scalar("SELECT policy FROM maintenance_controls WHERE policy=? FOR UPDATE")
        .bind(policy)
}

pub(in super::super) fn active_pack_task(policy: &str) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='pack' AND CAST(JSON_UNQUOTE(JSON_EXTRACT(detail,'$.policy')) AS BINARY)=? AND state IN ('queued','running') LIMIT 1").bind(policy)
}

pub(in super::super) fn pack_storages(policy: Option<&str>) -> Rows<'_, (Uuid, i64)> {
    sqlx::query_as("SELECT s.id,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.enabled AND CASE WHEN ? IN ('pack','repack') THEN p.pack_enabled WHEN ?='range' THEN p.pack_range_enabled ELSE TRUE END ORDER BY s.created_at,s.id").bind(policy).bind(policy)
}

pub(in super::super) fn unpack_storage(pack: Id) -> Rows<'static, (Uuid, i64)> {
    sqlx::query_as("SELECT s.id,p.revision FROM packs k JOIN storages s ON s.id=k.storage_id JOIN storage_profiles p ON p.storage_id=s.id WHERE k.id=?").bind(pack)
}

pub(in super::super) fn active_unpack_task(all: bool, pack: Option<Id>) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='unpack' AND state IN ('queued','running','paused','failed') AND CAST(JSON_UNQUOTE(JSON_EXTRACT(detail,'$.all')) AS BINARY)=? AND COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(detail,'$.pack_id')) AS BINARY),'null')=? LIMIT 1").bind(all.to_string()).bind(pack.map(|id| id.to_string()).unwrap_or_else(||"null".into()))
}

pub(in super::super) fn lock_task_detail(task: Execution) -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT detail FROM tasks WHERE id=? AND state='running' AND execution_token=? FOR UPDATE",
    )
    .bind(task.id)
    .bind(task.token)
}

pub(in super::super) fn advance_storage_task(
    task: Uuid,
    detail: Value,
    processed: i64,
    done: bool,
) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET progress_cursor=NULL,detail=?,processed=processed+?,state=IF(?,'completed',state),updated_at=CURRENT_TIMESTAMP(6) WHERE id=?").bind(detail).bind(processed).bind(done).bind(task)
}

pub(in super::super) fn finish_pack_change(pack: Id, generation: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM pack_changes WHERE pack_id=? AND generation=?")
        .bind(pack)
        .bind(generation)
}

pub(in super::super) fn defer_pack_queue(
    stream: Id,
    generation: i64,
    later: DateTime<Utc>,
) -> Statement<'static> {
    sqlx::query("UPDATE pack_maintenance SET progress_cursor=0,next_check_at=?,reason='repack' WHERE stream_id=? AND generation=?").bind(later).bind(stream).bind(generation)
}

pub(in super::super) fn finish_pack_queue(stream: Id, generation: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM pack_maintenance WHERE stream_id=? AND generation=?")
        .bind(stream)
        .bind(generation)
}

pub(in super::super) fn advance_pack_queue(
    stream: Id,
    generation: i64,
    cursor: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE pack_maintenance SET progress_cursor=? WHERE stream_id=? AND generation=?")
        .bind(cursor)
        .bind(stream)
        .bind(generation)
}

pub(in super::super) fn mark_range_checked(pack: Id) -> Statement<'static> {
    sqlx::query("UPDATE packs SET range_checked_at=CURRENT_TIMESTAMP(6) WHERE id=?").bind(pack)
}

pub(in super::super) fn record_range_result(
    task: Uuid,
    cursor: String,
    processed: i64,
    decision: Value,
) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET progress_cursor=?,processed=processed+?,detail=JSON_SET(detail,'$.last_range',JSON_EXTRACT(?,'$'),'$.evaluated',COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(detail,'$.evaluated')) AS SIGNED),0)+1),updated_at=CURRENT_TIMESTAMP(6) WHERE id=?").bind(cursor).bind(processed).bind(decision).bind(task)
}

fn rewrite_needed(
    query: &mut sqlx::QueryBuilder<MySql>,
    pack: bool,
    algorithm: &str,
    key: Option<Uuid>,
) {
    query.push(if pack {
        "(NOT(k.pack_fingerprint<=>"
    } else {
        "(l.encoding_id IS NULL OR NOT(l.encoding_fingerprint<=>"
    });
    query
        .push("UNHEX(SHA2(CONCAT_WS(':',HEX(")
        .push_bind(if pack { None } else { key })
        .push("),CAST(")
        .push_bind(if pack { "none" } else { algorithm }.to_owned())
        .push(" AS BINARY)");
    query.push(if pack {
        ",k.pack_compressed,k.member_count"
    } else {
        ",l.standalone_compressed"
    });
    query.push(",p.compression_enabled,p.compression_strategy,p.compression_level,p.compression_sample_level,p.compression_min_savings_percent,p.compression_min_savings_bytes");
    if pack {
        query.push(",p.pack_max_size,p.pack_range_enabled,p.pack_range_min_downloads,p.pack_range_min_savings_percent,p.pack_range_min_savings_bytes,'pack'");
    } else {
        query.push(",'standalone'");
    }
    query
        .push("),256))) OR ((NOT p.compression_enabled OR p.compression_strategy='never') AND ")
        .push(if pack {
            "k.pack_compressed) OR EXISTS (SELECT 1 FROM pack_members m WHERE m.pack_id=k.id AND (m.encryption_algorithm<>"
        } else {
            "l.standalone_compressed))"
        });
    if pack {
        query.push_bind(algorithm.to_owned()).push(" OR NOT(m.block_key_id<=>").push_bind(key)
            .push(") OR ((NOT p.compression_enabled OR p.compression_strategy='never') AND m.compressed))))");
    }
}

pub(in super::super) fn rewrite_counts(
    storage: Uuid,
    algorithm: &str,
    key: Option<Uuid>,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new("SELECT CAST(COALESCE(SUM(c.pack_id IS NULL AND ");
    rewrite_needed(&mut query, false, algorithm, key);
    query.push("),0) AS SIGNED),CAST(COALESCE(SUM(c.pack_id IS NOT NULL AND ");
    rewrite_needed(&mut query, true, algorithm, key);
    query.push("),0) AS SIGNED) FROM chunks c JOIN storage_profiles p ON p.storage_id=c.storage_id LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' LEFT JOIN packs k ON k.id=c.pack_id AND k.state='ready' WHERE c.state='ready' AND c.storage_id=").push_bind(storage);
    query
}

pub(in super::super) fn rewrite_loose_candidate(
    after: Id,
    job: &RewriteTask,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = chunk_query(true);
    query.push(" JOIN storage_profiles p ON p.storage_id=c.storage_id WHERE c.state='ready' AND c.pack_id IS NULL AND l.encoding_id IS NOT NULL AND c.id>").push_bind(after).push(" AND c.id<=").push_bind(job.upper_chunk_id).push(" AND c.storage_id=").push_bind(job.storage_id).push(" AND ");
    rewrite_needed(&mut query, false, &job.target_algorithm, job.target_key_id);
    query.push(" ORDER BY c.id LIMIT 1");
    query
}

pub(in super::super) fn rewrite_pack_candidate(
    after: Id,
    job: &RewriteTask,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT MIN(c.id),k.id FROM chunks c JOIN packs k ON k.id=c.pack_id AND k.state='ready' JOIN storage_profiles p ON p.storage_id=c.storage_id WHERE c.state='ready' AND c.storage_id=",
    );
    query.push_bind(job.storage_id).push(" AND ");
    rewrite_needed(&mut query, true, &job.target_algorithm, job.target_key_id);
    query
        .push(" GROUP BY k.id HAVING MIN(c.id)>")
        .push_bind(after)
        .push(" AND MIN(c.id)<=")
        .push_bind(job.upper_chunk_id)
        .push(" ORDER BY MIN(c.id) LIMIT 1");
    query
}

pub(in super::super) fn lock_rewrite_storage(storage: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM storages WHERE id=? AND enabled FOR UPDATE").bind(storage)
}

pub(in super::super) fn active_rewrite(storage: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='rewrite' AND CAST(JSON_UNQUOTE(JSON_EXTRACT(detail,'$.storage_id')) AS BINARY)=? AND state IN ('queued','running','paused') LIMIT 1").bind(storage.to_string())
}

pub(in super::super) fn rewrite_upper(storage: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT COALESCE(MAX(id),0) FROM chunks WHERE storage_id=?").bind(storage)
}

pub(in super::super) fn lock_rewrite_chunk(chunk: Id) -> Rows<'static, (Option<Id>, Id)> {
    sqlx::query_as(
        "SELECT pack_id,current_encoding_id FROM chunks WHERE id=? AND state='ready' FOR UPDATE",
    )
    .bind(chunk)
}

pub(in super::super) fn retire_rewrite_encoding(encoding: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=CURRENT_TIMESTAMP(6) WHERE encoding_id=? AND state='ready'").bind(encoding)
}

pub(in super::super) fn rewrite_members(pack: Id) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new("SELECT ");
    query.push(CHUNK_COLUMNS).push(" FROM pack_members m JOIN chunks c ON c.id=m.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE m.pack_id=").push_bind(pack).push(" ORDER BY m.ordinal LIMIT 4097");
    query
}

pub(in super::super) fn rewrite_current_members(pack: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM chunks WHERE pack_id=? AND state='ready' ORDER BY id")
        .bind(pack)
}

pub(in super::super) fn lock_rewrite_members(pack: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT id FROM chunks WHERE pack_id=? AND state='ready' ORDER BY id FOR UPDATE",
    )
    .bind(pack)
}

pub(in super::super) fn replace_rewrite_pack(old: Id, new: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET pack_id=? WHERE pack_id=? AND state='ready'")
        .bind(new)
        .bind(old)
}
