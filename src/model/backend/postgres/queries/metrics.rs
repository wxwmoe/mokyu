use super::*;

#[derive(Default, sqlx::FromRow)]
pub(in super::super) struct SavedStorage {
    pub(in super::super) as_of: Option<DateTime<Utc>>,
    pub(in super::super) data: Option<sqlx::types::Json<StorageUsage>>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for WorkSummary {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            task_type: row.try_get("task_type")?,
            state: row.try_get("state")?,
            processed: row.try_get("processed")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

pub(in super::super) fn inventory_isolation() -> Statement<'static> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
}

pub(in super::super) fn inventory_limits() -> sqlx::RawSql {
    sqlx::raw_sql(
        "SET LOCAL work_mem='16MB'; SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL lock_timeout='1s'",
    )
}

pub(in super::super) fn inventory_timeout(timeout: String) -> Statement<'static> {
    sqlx::query("SELECT set_config('statement_timeout',$1,true)").bind(timeout)
}

pub(in super::super) fn inventory_time() -> Scalar<'static, DateTime<Utc>> {
    sqlx::query_scalar("SELECT transaction_timestamp()")
}

pub(in super::super) fn inventory_buckets() -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT jsonb_build_object('id',b.id,'name',b.name,'objects',count(o.stream_id),'logical_bytes',COALESCE(sum(s.size),0))
         FROM buckets b LEFT JOIN objects o ON o.bucket_id=b.id AND o.stream_id IS NOT NULL
         LEFT JOIN streams s ON s.id=o.stream_id
         GROUP BY GROUPING SETS ((b.id,b.name),()) ORDER BY b.id NULLS FIRST LIMIT 1002")
}

pub(in super::super) fn inventory_chunks() -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT jsonb_build_object(
            'states',COALESCE(jsonb_object_agg(state,n),'{}'),
            'stored_bytes',COALESCE(sum(bytes) FILTER(WHERE state IN ('ready','deleting')),0),
            'unconfirmed_bytes',COALESCE(sum(bytes) FILTER(WHERE state IN ('uploading','failed')),0))
         FROM (SELECT state,count(*) n,COALESCE(sum(standalone_size),0) bytes FROM chunks GROUP BY state) c")
}

pub(in super::super) fn inventory_physical() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(to_jsonb(p)),'[]') FROM (SELECT source_type,state,count(*) objects,COALESCE(sum(stored_size),0) stored_bytes FROM (SELECT 'chunk' source_type,state,standalone_size AS stored_size FROM chunk_encodings UNION ALL SELECT 'pack',state,stored_size FROM packs UNION ALL SELECT 'raw',state,size FROM raw_sources) sources GROUP BY source_type,state ORDER BY source_type,state) p",
    )
}

pub(in super::super) fn inventory_live() -> Scalar<'static, Value> {
    sqlx::query_scalar("WITH refs AS (
            SELECT e.chunk_id,sum(e.length) bytes FROM objects o JOIN extents e ON e.stream_id=o.stream_id
            WHERE e.chunk_id IS NOT NULL GROUP BY e.chunk_id),
         raw_live AS (SELECT id,size FROM raw_sources r WHERE EXISTS(SELECT 1 FROM raw_ranges e JOIN objects o ON o.stream_id=e.stream_id WHERE e.source_id=r.id)), physical AS (
            SELECT l.standalone_size bytes,l.standalone_size-CASE WHEN l.encryption_algorithm='none' THEN 0 ELSE 16 END payload
            FROM refs r JOIN chunks c ON c.id=r.chunk_id JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready'
            UNION ALL SELECT p.stored_size,p.stored_size-CASE WHEN p.encryption_algorithm='none' THEN 0 ELSE 16 END
            FROM packs p WHERE state='ready' AND EXISTS(SELECT 1 FROM refs r JOIN chunks c ON c.id=r.chunk_id WHERE c.pack_id=p.id) UNION ALL SELECT size,size FROM raw_live)
         SELECT jsonb_build_object('raw_objects',(SELECT COUNT(*) FROM raw_live),'chunks',count(*),'reference_bytes',(COALESCE(sum(r.bytes),0)+(SELECT COALESCE(SUM(e.length),0) FROM raw_ranges e JOIN objects o ON o.stream_id=e.stream_id)),
            'raw_bytes',(COALESCE(sum(c.raw_size),0)+(SELECT COALESCE(SUM(size),0) FROM raw_live)),'pending_raw_bytes',COALESCE(sum(c.raw_size) FILTER(WHERE c.pack_id IS NULL AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=c.id AND state='ready')),0),'stored_bytes',(SELECT COALESCE(sum(bytes),0) FROM physical),
            'payload_bytes',(SELECT COALESCE(sum(payload),0) FROM physical))
         FROM refs r JOIN chunks c ON c.id=r.chunk_id")
}

pub(in super::super) fn inventory_unreferenced() -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT jsonb_build_object('chunks',count(*),'stored_bytes',COALESCE(sum(standalone_size),0),
            'eligible_chunks',count(*) FILTER(WHERE unreferenced_at<now()-((SELECT (left(sp.unreferenced_grace,-1)::bigint*CASE right(sp.unreferenced_grace,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=c.storage_id))*interval '1 second' AND owner_stream IS NULL),
            'eligible_bytes',COALESCE(sum(standalone_size) FILTER(WHERE unreferenced_at<now()-((SELECT (left(sp.unreferenced_grace,-1)::bigint*CASE right(sp.unreferenced_grace,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=c.storage_id))*interval '1 second' AND owner_stream IS NULL),0))
         FROM chunks c WHERE state IN ('ready','failed','deleting') AND unreferenced_at IS NOT NULL
            AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)")
}

pub(in super::super) fn inventory_reclaimable() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT jsonb_build_object('bytes',COALESCE(sum(bytes),0),'eligible',COALESCE(sum(bytes) FILTER(WHERE since<now()-((SELECT (left(sp.unreferenced_grace,-1)::bigint*CASE right(sp.unreferenced_grace,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=p.storage_id))*interval '1 second'),0)) FROM (SELECT l.standalone_size bytes,COALESCE(l.unreferenced_at,c.unreferenced_at) since,l.storage_id FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE l.state IN ('retired','deleting') OR (l.state='ready' AND c.unreferenced_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)) UNION ALL SELECT stored_size,unreferenced_at,storage_id FROM packs WHERE state IN ('retired','deleting') UNION ALL SELECT size,unreferenced_at,storage_id FROM raw_sources r WHERE r.owner_stream IS NULL AND NOT EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.kind='raw' AND n.location_id=r.id AND l.expires_at>clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=r.id) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=r.id) AND NOT EXISTS(SELECT 1 FROM uploads WHERE raw_source_id=r.id)) p",
    )
}

pub(in super::super) fn inventory_tasks() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT COALESCE(jsonb_object_agg(state,n),'{}') FROM (SELECT state,count(*) n FROM tasks GROUP BY state) t",
    )
}

pub(in super::super) fn inventory_uploads() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT COALESCE(jsonb_object_agg(state,n),'{}') FROM (SELECT state,count(*) n FROM uploads WHERE state IN ('active','completing') GROUP BY state) u",
    )
}

pub(in super::super) fn inventory_cleanup(tasks: f64) -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT jsonb_object_agg(source_type,jsonb_build_object('eligible',n,'oldest_at',oldest)) FROM (
            SELECT 'chunks' source_type,count(*) n,min(deleted_at) oldest FROM chunks c
                WHERE state='deleted' AND deleted_at<now()-((SELECT (left(sp.deleted_chunk_retention,-1)::bigint*CASE right(sp.deleted_chunk_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=c.storage_id))*interval '1 second'
                AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)
            UNION ALL SELECT 'uploads',count(*),min(touched_at) FROM uploads u
                WHERE state IN ('completed','aborted') AND touched_at<now()-((SELECT (left(sp.upload_retention,-1)::bigint*CASE right(sp.upload_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=(SELECT storage_id FROM buckets WHERE id=u.bucket_id)))*interval '1 second'
                AND NOT EXISTS(SELECT 1 FROM parts WHERE upload_id=u.id)
            UNION ALL SELECT 'tasks',count(*),min(updated_at) FROM tasks
                WHERE state='completed' AND updated_at<now()-$1*interval '1 second'
            UNION ALL SELECT 'integrity_issues',count(*),min(t.updated_at) FROM integrity_issues i JOIN tasks t ON t.id=i.task_id
                WHERE t.state='completed' AND t.updated_at<now()-$1*interval '1 second'
            UNION ALL SELECT 'sessions',count(*),min(expires_at) FROM sessions WHERE expires_at<now()
            UNION ALL SELECT 'packs',count(*),min(deleted_at) FROM packs p WHERE state='deleted' AND deleted_at<now()-((SELECT (left(sp.deleted_chunk_retention,-1)::bigint*CASE right(sp.deleted_chunk_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=p.storage_id))*interval '1 second'
            UNION ALL SELECT 'chunk_encodings',count(*),min(deleted_at) FROM chunk_encodings ce WHERE state='deleted' AND deleted_at<now()-((SELECT (left(sp.deleted_chunk_retention,-1)::bigint*CASE right(sp.deleted_chunk_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=ce.storage_id))*interval '1 second'
        ) backlog").bind(tasks)
}

pub(in super::super) fn inventory_database() -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT jsonb_build_object('table',relname,'total_bytes',pg_total_relation_size(relid),
            'index_bytes',pg_indexes_size(relid),'live_rows_estimate',n_live_tup,
            'dead_rows_estimate',n_dead_tup,'last_autovacuum',last_autovacuum,'last_autoanalyze',last_autoanalyze)
         FROM pg_stat_user_tables WHERE schemaname='public'
            AND relname IN ('chunks','extents','raw_sources','raw_ranges','streams','objects','uploads','parts','fragments','multipart_files','tasks','sessions','session_refresh_tokens','integrity_issues','chunk_encodings','packs','pack_members','pack_maintenance','pack_changes','pack_inputs','chunk_access_stats','chunk_access_windows','pack_access_windows','pack_member_access_windows','integrity_packs','pending_uploads','cache_pins')
         ORDER BY relname")
}

pub(in super::super) fn storage_inventory_limits() -> sqlx::RawSql {
    sqlx::raw_sql(
        "SET LOCAL work_mem='8MB'; SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL statement_timeout='5s'",
    )
}

pub(in super::super) fn storage_inventory_buckets(storage: Uuid) -> Rows<'static, (i64, i64, i64)> {
    sqlx::query_as("SELECT count(DISTINCT b.id),count(o.stream_id),coalesce(sum(s.size),0)::bigint FROM buckets b LEFT JOIN objects o ON o.bucket_id=b.id AND o.stream_id IS NOT NULL LEFT JOIN streams s ON s.id=o.stream_id WHERE b.storage_id=$1").bind(storage)
}

pub(in super::super) fn storage_inventory_chunks(storage: Uuid) -> Rows<'static, (i64, i64)> {
    sqlx::query_as("SELECT count(*),coalesce(sum(standalone_size),0)::bigint FROM chunks WHERE storage_id=$1 AND state IN ('ready','deleting')").bind(storage)
}

pub(in super::super) fn storage_inventory_physical(storage: Uuid) -> Rows<'static, (i64, i64)> {
    sqlx::query_as("SELECT count(*),coalesce(sum(bytes),0)::bigint FROM (SELECT standalone_size::bigint bytes FROM chunk_encodings WHERE storage_id=$1 AND state IN ('ready','retired','deleting') UNION ALL SELECT stored_size::bigint FROM packs WHERE storage_id=$1 AND state IN ('ready','retired','deleting') UNION ALL SELECT size FROM raw_sources WHERE storage_id=$1 AND state<>'writing') p").bind(storage)
}

pub(in super::super) fn active_usage_task() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM tasks WHERE task_type='usage' AND state IN ('queued','running','paused') ORDER BY created_at LIMIT 1",
    )
}

pub(super) async fn complete_usage(pool: &PgPool, id: Execution) -> Result<bool> {
    let changed = sqlx::query("UPDATE tasks SET state='completed',processed=processed+1,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())")
            .bind(id.id)
            .bind(id.token).execute(pool)
            .await?.rows_affected();
    Ok(changed == 1)
}

pub(in super::super) fn usage_scope_buckets(
    user: Option<Uuid>,
    admin: bool,
    token: Option<Uuid>,
    bucket: Option<Uuid>,
    project: Option<Uuid>,
) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT b.id FROM buckets b LEFT JOIN user_bucket_access a ON a.bucket_id=b.id AND a.user_id=$1 LEFT JOIN token_bucket_access t ON t.bucket_id=b.id AND t.token_id=$3 WHERE ($2 OR 'storage.inspect'=ANY(CASE WHEN $3::uuid IS NOT NULL THEN t.actions ELSE a.actions END)) AND ($4::uuid IS NULL OR b.id=$4) AND ($5::uuid IS NULL OR b.project_id=$5) ORDER BY b.id").bind(user).bind(admin).bind(token).bind(bucket).bind(project)
}

pub(in super::super) fn usage_exists<'q>(key: &'q str) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM usage_summary WHERE id=$1)").bind(key)
}

pub(in super::super) fn usage_scope_lock() -> Statement<'static> {
    sqlx::query("SELECT pg_advisory_xact_lock(734922709851030)")
}

pub(in super::super) fn usage_scope_evict(keep: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM usage_summary WHERE id IN (SELECT id FROM usage_summary WHERE id<>'global' ORDER BY requested_at DESC,id OFFSET $1)").bind(keep)
}

pub(in super::super) fn usage_scope_create<'q>(key: &'q str, ids: &'q [Uuid]) -> Statement<'q> {
    sqlx::query("INSERT INTO usage_summary(id,bucket_ids) VALUES($1,$2) ON CONFLICT DO NOTHING")
        .bind(key)
        .bind(ids)
}

pub(in super::super) fn usage_scope_touch<'q>(key: &'q str) -> Statement<'q> {
    sqlx::query("UPDATE usage_summary SET requested_at=now() WHERE id=$1 AND requested_at<now()-interval '1 hour'").bind(key)
}

pub(in super::super) fn usage_saved<'q>(key: &'q str) -> Rows<'q, SavedStorage> {
    sqlx::query_as("SELECT as_of,data FROM usage_summary WHERE id=$1").bind(key)
}

pub(in super::super) fn usage_history<'q>(
    key: &'q str,
) -> Rows<'q, (DateTime<Utc>, sqlx::types::Json<StorageUsage>)> {
    sqlx::query_as("SELECT snapshot_at,data FROM (SELECT snapshot_at,data FROM usage_history WHERE scope_id=$1 ORDER BY snapshot_at DESC LIMIT 2048) p ORDER BY snapshot_at").bind(key)
}

// Range attribution joins entire inventories, where repeated inner scans cost more than hashing.
pub(in super::super) const USAGE_SUMMARY: &str = r#"SET LOCAL jit=off;
SET LOCAL enable_nestloop=off;
WITH ranges AS MATERIALIZED (
    SELECT o.bucket_id,b.project_id,e.chunk_id,
           range_agg(int8range(e.source_offset::bigint,e.source_offset::bigint+e.length)) AS spans
    FROM objects o JOIN buckets b ON b.id=o.bucket_id JOIN extents e ON e.stream_id=o.stream_id
    WHERE e.chunk_id IS NOT NULL GROUP BY o.bucket_id,b.project_id,e.chunk_id
), boundaries AS (
    SELECT chunk_id,lower(span) AS point FROM ranges CROSS JOIN LATERAL unnest(spans) span
    UNION SELECT chunk_id,upper(span) FROM ranges CROSS JOIN LATERAL unnest(spans) span
), segments AS (
    SELECT chunk_id,point AS lo,lead(point) OVER(PARTITION BY chunk_id ORDER BY point) AS hi FROM boundaries
), coverage AS MATERIALIZED (
    SELECT s.chunk_id,s.lo,s.hi,r.bucket_id,r.project_id
    FROM segments s JOIN ranges r ON r.chunk_id=s.chunk_id AND r.spans @> int8range(s.lo,s.hi)
    WHERE s.hi IS NOT NULL
), project_counts AS (
    SELECT chunk_id,lo,count(DISTINCT project_id) AS n FROM coverage GROUP BY chunk_id,lo
), bucket_counts AS (
    SELECT chunk_id,lo,project_id,count(*) AS n FROM coverage GROUP BY chunk_id,lo,project_id
), allocated AS (
    SELECT c.bucket_id,c.chunk_id,sum((hi-lo)::numeric/p.n/b.n) AS raw_bytes
    FROM coverage c JOIN project_counts p USING(chunk_id,lo)
    JOIN bucket_counts b USING(chunk_id,lo,project_id) GROUP BY c.bucket_id,c.chunk_id
), chunk_unique AS (
    SELECT chunk_id,sum(hi-lo) AS bytes FROM (SELECT DISTINCT chunk_id,lo,hi FROM coverage) u GROUP BY chunk_id
), sources AS MATERIALIZED (
    SELECT c.id AS chunk_id,CASE WHEN l.encoding_id IS NOT NULL THEN 'chunk' WHEN p.id IS NOT NULL THEN 'pack' ELSE 'pending' END AS source_type,
           coalesce(l.encoding_id,p.id,c.id) AS source_id,coalesce(l.standalone_size,p.stored_size) AS stored_size,u.bytes
    FROM chunk_unique u JOIN chunks c ON c.id=u.chunk_id
    LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready'
    LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready'
), source_totals AS MATERIALIZED (
    SELECT source_type,source_id,sum(bytes) AS raw_bytes,max(stored_size) AS stored_size FROM sources GROUP BY source_type,source_id
), attribution AS MATERIALIZED (
    SELECT a.bucket_id,a.chunk_id,a.raw_bytes,s.source_type,s.source_id,
           a.raw_bytes/t.raw_bytes*t.stored_size AS encoded_bytes
    FROM allocated a JOIN sources s USING(chunk_id) JOIN source_totals t USING(source_type,source_id)
), scopes AS MATERIALIZED (
    SELECT id,bucket_ids FROM usage_summary
), scope_ranges AS (
    SELECT s.id,r.chunk_id,range_agg(r.spans) AS spans FROM scopes s JOIN ranges r ON s.bucket_ids IS NULL OR r.bucket_id=ANY(s.bucket_ids)
    GROUP BY s.id,r.chunk_id
), scope_unique AS (
    SELECT r.id,count(DISTINCT r.chunk_id) AS chunks,
           count(DISTINCT s.source_id) FILTER(WHERE s.source_type='pack') AS packs,
           sum(upper(span)-lower(span)) AS bytes,
           coalesce(sum(upper(span)-lower(span)) FILTER(WHERE s.source_type='pack'),0) AS packed_bytes,
           coalesce(sum(upper(span)-lower(span)) FILTER(WHERE s.source_type='pending'),0) AS pending_bytes
    FROM scope_ranges r JOIN sources s USING(chunk_id) CROSS JOIN LATERAL unnest(r.spans) span GROUP BY r.id
), scope_allocated AS (
    SELECT s.id,sum(a.raw_bytes) AS raw_bytes,coalesce(sum(a.encoded_bytes),0) AS encoded_bytes,
           bool_and(a.encoded_bytes IS NOT NULL) AS complete
    FROM scopes s JOIN attribution a ON s.bucket_ids IS NULL OR a.bucket_id=ANY(s.bucket_ids) GROUP BY s.id
), scope_logical AS (
    SELECT s.id,count(b.id) AS buckets,coalesce(sum(q.used_bytes),0) AS bytes,coalesce(sum(q.object_count),0) AS objects
    FROM scopes s LEFT JOIN buckets b ON s.bucket_ids IS NULL OR b.id=ANY(s.bucket_ids)
    LEFT JOIN quota_scopes q ON q.scope='bucket' AND q.scope_id=b.id GROUP BY s.id
), raw_references AS (
    SELECT DISTINCT o.bucket_id,b.project_id,r.id AS source_id,r.size
    FROM objects o JOIN buckets b ON b.id=o.bucket_id JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id
), raw_project_counts AS (
    SELECT source_id,COUNT(DISTINCT project_id) AS n FROM raw_references GROUP BY source_id
), raw_allocated AS (
    SELECT r.*,r.size::numeric/p.n/COUNT(*) OVER(PARTITION BY r.source_id,r.project_id) AS attributed
    FROM raw_references r JOIN raw_project_counts p USING(source_id)
), raw_scopes AS (
    SELECT id,SUM(size) AS bytes,SUM(attributed) AS attributed FROM (
        SELECT s.id AS id,r.source_id,MAX(r.size) AS size,SUM(r.attributed) AS attributed
        FROM scopes s JOIN raw_allocated r ON s.bucket_ids IS NULL OR r.bucket_id=ANY(s.bucket_ids) GROUP BY s.id,r.source_id
    ) scoped_raw GROUP BY id
), raw_inventory AS MATERIALIZED (
    SELECT r.size,r.state,
        EXISTS(SELECT 1 FROM raw_ranges e JOIN objects o ON o.stream_id=e.stream_id WHERE e.source_id=r.id) AS selected,
        r.owner_stream IS NULL AND NOT EXISTS(SELECT 1 FROM raw_ranges e WHERE e.source_id=r.id) AS garbage
    FROM raw_sources r
), raw_physical AS (
    SELECT COALESCE(SUM(CASE WHEN state<>'writing' THEN size ELSE 0 END),0) AS indexed_bytes,
        COALESCE(SUM(CASE WHEN selected THEN size ELSE 0 END),0) AS selected_bytes,
        COALESCE(SUM(CASE WHEN NOT selected AND NOT garbage AND state<>'writing' THEN size ELSE 0 END),0) AS retained_bytes,
        COALESCE(SUM(CASE WHEN NOT selected AND garbage THEN size ELSE 0 END),0) AS gc_bytes,
        COALESCE(SUM(CASE WHEN state='writing' THEN size ELSE 0 END),0) AS unconfirmed_bytes,
        COUNT(*) AS raw_objects FROM raw_inventory
), physical AS (
    SELECT 'chunk' AS source_type,l.encoding_id AS source_id,l.state,l.standalone_size AS stored_size,
           l.state IN ('retired','deleting') OR (c.unreferenced_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)) AS garbage
    FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE l.state IN ('ready','retired','deleting','uploading')
    UNION ALL SELECT 'pack',id,state,stored_size,state IN ('retired','deleting') FROM packs WHERE state<>'deleted'
), bridge AS (
    SELECT jsonb_build_object(
        'indexed_bytes',(coalesce(sum(p.stored_size) FILTER(WHERE state IN ('ready','retired','deleting')),0)+(SELECT indexed_bytes FROM raw_physical))::text,
        'selected_bytes',(coalesce(sum(p.stored_size) FILTER(WHERE t.source_id IS NOT NULL),0)+(SELECT selected_bytes FROM raw_physical))::text,
        'retained_bytes',(coalesce(sum(p.stored_size) FILTER(WHERE t.source_id IS NULL AND state IN ('ready','retired','deleting') AND NOT garbage),0)+(SELECT retained_bytes FROM raw_physical))::text,
        'gc_bytes',(coalesce(sum(p.stored_size) FILTER(WHERE t.source_id IS NULL AND garbage),0)+(SELECT gc_bytes FROM raw_physical))::text,
        'unconfirmed_bytes',(coalesce(sum(p.stored_size) FILTER(WHERE state IN ('preparing','uploading')),0)+(SELECT unconfirmed_bytes FROM raw_physical))::text,
        'chunk_objects',count(*) FILTER(WHERE p.source_type='chunk' AND state IN ('ready','retired','deleting'))::text,
        'raw_objects',(SELECT raw_objects FROM raw_physical)::text,
        'pack_objects',count(*) FILTER(WHERE p.source_type='pack' AND state IN ('ready','retired','deleting'))::text
    ) AS data FROM physical p LEFT JOIN source_totals t ON t.source_type=p.source_type AND t.source_id=p.source_id AND t.source_type<>'pending'
), summaries AS (
    SELECT l.id,jsonb_build_object(
        'buckets',l.buckets::text,'objects',l.objects::text,'logical_bytes',l.bytes::text,
        'unique_bytes',(coalesce(u.bytes,0)+COALESCE(r.bytes,0))::text,'attributed_raw_bytes',round((coalesce(a.raw_bytes,0)+COALESCE(r.attributed,0)),6)::text,
        'encoded_bytes',CASE WHEN coalesce(a.complete,true) THEN round((coalesce(a.encoded_bytes,0)+COALESCE(r.attributed,0)),6)::text END,
        'encoded_known_bytes',round((coalesce(a.encoded_bytes,0)+COALESCE(r.attributed,0)),6)::text,
        'local_savings', (l.bytes-(coalesce(u.bytes,0)+COALESCE(r.bytes,0)))::text,
        'shared_savings',round((coalesce(u.bytes,0)+COALESCE(r.bytes,0))-(coalesce(a.raw_bytes,0)+COALESCE(r.attributed,0)),6)::text,
        'encoding_savings',CASE WHEN coalesce(a.complete,true) THEN round((coalesce(a.raw_bytes,0)+COALESCE(r.attributed,0))-(coalesce(a.encoded_bytes,0)+COALESCE(r.attributed,0)),6)::text END,
        'chunks',coalesce(u.chunks,0)::text,'packs',coalesce(u.packs,0)::text,
        'packed_bytes',coalesce(u.packed_bytes,0)::text,'pending_bytes',coalesce(u.pending_bytes,0)::text,
        'physical',CASE WHEN l.id='global' THEN (SELECT data FROM bridge) END
    ) AS data FROM scope_logical l LEFT JOIN scope_unique u USING(id) LEFT JOIN scope_allocated a USING(id) LEFT JOIN raw_scopes r ON r.id=l.id
)
UPDATE usage_summary i SET as_of=transaction_timestamp(),data=s.data FROM summaries s WHERE s.id=i.id;
"#;

pub(in super::super) fn usage_scope_expire(retention: f64) -> Statement<'static> {
    sqlx::query("DELETE FROM usage_summary WHERE id<>'global' AND requested_at<now()-$1*interval '1 second'").bind(retention)
}

pub(in super::super) fn collect_usage() -> sqlx::RawSql {
    sqlx::raw_sql(USAGE_SUMMARY)
}

pub(in super::super) fn usage_history_append(interval: f64) -> Statement<'static> {
    sqlx::query("INSERT INTO usage_history(scope_id,snapshot_at,data) SELECT i.id,i.as_of,i.data FROM usage_summary i WHERE i.as_of IS NOT NULL AND NOT EXISTS(SELECT 1 FROM usage_history h WHERE h.scope_id=i.id AND h.snapshot_at>i.as_of-$1*interval '1 second') ON CONFLICT DO NOTHING").bind(interval)
}

pub(in super::super) fn usage_history_expire(retention: f64) -> Statement<'static> {
    sqlx::query("DELETE FROM usage_history WHERE snapshot_at<now()-$1*interval '1 second'")
        .bind(retention)
}

pub(in super::super) fn runtime_history() -> Scalar<'static, sqlx::types::Json<RuntimePoint>> {
    sqlx::query_scalar(
        "SELECT data FROM (SELECT sampled_at,data FROM runtime_samples WHERE data->>'scope' IS NULL AND (SELECT deployment_mode FROM mokyu_meta)='single' ORDER BY sampled_at DESC LIMIT 2048) h ORDER BY sampled_at",
    )
}

pub(in super::super) fn runtime_work() -> Rows<'static, WorkSummary> {
    sqlx::query_as(
        "SELECT id,coalesce(detail->>'policy',task_type) task_type,state,processed::text,updated_at FROM tasks WHERE state IN ('queued','running','paused') ORDER BY CASE state WHEN 'running' THEN 0 WHEN 'queued' THEN 1 ELSE 2 END,updated_at DESC LIMIT 5",
    )
}

pub(in super::super) fn runtime_pending()
-> Rows<'static, (String, String, Option<DateTime<Utc>>, String)> {
    sqlx::query_as(
        "SELECT count(*)::text,coalesce(sum(staged_size),0)::text,min(created_at),count(*) FILTER(WHERE last_error IS NOT NULL)::text FROM pending_uploads",
    )
}

pub(in super::super) fn runtime_sample(
    at: DateTime<Utc>,
    data: sqlx::types::Json<RuntimePoint>,
) -> Statement<'static> {
    sqlx::query("INSERT INTO runtime_samples(sampled_at,data) VALUES($1,$2)")
        .bind(at)
        .bind(data)
}

pub(in super::super) fn runtime_expire(retention: f64) -> Statement<'static> {
    sqlx::query("DELETE FROM runtime_samples WHERE sampled_at<now()-$1*interval '1 second'")
        .bind(retention)
}

pub(in super::super) fn access_chunks<'q>(
    ids: &'q [Id],
    reads: &'q [i64],
    ranges: &'q [i64],
    bytes: &'q [i64],
) -> Statement<'q> {
    sqlx::query("INSERT INTO chunk_access_stats(chunk_id,reads,range_reads,bytes,last_read_at) SELECT c.id,sum(s.reads),sum(s.ranges),sum(s.bytes),now() FROM unnest($1::uuid[],$2::bigint[],$3::bigint[],$4::bigint[]) s(id,reads,ranges,bytes) JOIN chunks c ON c.id=s.id GROUP BY c.id ON CONFLICT(chunk_id) DO UPDATE SET reads=chunk_access_stats.reads+EXCLUDED.reads,range_reads=chunk_access_stats.range_reads+EXCLUDED.range_reads,bytes=chunk_access_stats.bytes+EXCLUDED.bytes,last_read_at=EXCLUDED.last_read_at").bind(ids).bind(reads).bind(ranges).bind(bytes)
}

pub(in super::super) fn access_chunk_windows<'q>(
    ids: &'q [Id],
    hours: Vec<DateTime<Utc>>,
    reads: Vec<i64>,
    ranges: Vec<i64>,
    bytes: Vec<i64>,
    origins: Vec<i64>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO chunk_access_windows(chunk_id,window_start,reads,range_reads,bytes,range_origin_reads) SELECT c.id,s.hour,s.reads,s.ranges,s.bytes,s.origins FROM unnest($1::uuid[],$2::timestamptz[],$3::bigint[],$4::bigint[],$5::bigint[],$6::bigint[]) s(id,hour,reads,ranges,bytes,origins) JOIN chunks c ON c.id=s.id ON CONFLICT(window_start,chunk_id) DO UPDATE SET reads=chunk_access_windows.reads+EXCLUDED.reads,range_reads=chunk_access_windows.range_reads+EXCLUDED.range_reads,bytes=chunk_access_windows.bytes+EXCLUDED.bytes,range_origin_reads=chunk_access_windows.range_origin_reads+EXCLUDED.range_origin_reads").bind(ids).bind(hours).bind(reads).bind(ranges).bind(bytes).bind(origins)
}

pub(in super::super) fn access_packs(
    ids: Vec<Id>,
    hours: Vec<DateTime<Utc>>,
    reads: Vec<i64>,
    bytes: Vec<i64>,
    partial: Vec<i64>,
    useful: Vec<i64>,
    partial_bytes: Vec<i64>,
) -> Statement<'static> {
    sqlx::query("INSERT INTO pack_access_windows(pack_id,window_start,downloads,downloaded_bytes,partial_downloads,useful_bytes,partial_bytes) SELECT p.id,s.hour,s.reads,s.bytes,s.partial,s.useful,s.partial_bytes FROM unnest($1::uuid[],$2::timestamptz[],$3::bigint[],$4::bigint[],$5::bigint[],$6::bigint[],$7::bigint[]) s(id,hour,reads,bytes,partial,useful,partial_bytes) JOIN packs p ON p.id=s.id ON CONFLICT(window_start,pack_id) DO UPDATE SET downloads=pack_access_windows.downloads+EXCLUDED.downloads,downloaded_bytes=pack_access_windows.downloaded_bytes+EXCLUDED.downloaded_bytes,partial_downloads=pack_access_windows.partial_downloads+EXCLUDED.partial_downloads,useful_bytes=pack_access_windows.useful_bytes+EXCLUDED.useful_bytes,partial_bytes=pack_access_windows.partial_bytes+EXCLUDED.partial_bytes,updated_at=now()").bind(ids).bind(hours).bind(reads).bind(bytes).bind(partial).bind(useful).bind(partial_bytes)
}

pub(in super::super) fn access_members(
    packs: Vec<Id>,
    chunks: Vec<Id>,
    hours: Vec<DateTime<Utc>>,
    counts: Vec<i64>,
) -> Statement<'static> {
    sqlx::query("INSERT INTO pack_member_access_windows(pack_id,chunk_id,window_start,downloads) SELECT p.id,c.id,s.hour,s.n FROM unnest($1::uuid[],$2::uuid[],$3::timestamptz[],$4::bigint[]) s(p,c,hour,n) JOIN packs p ON p.id=s.p JOIN chunks c ON c.id=s.c ON CONFLICT(window_start,pack_id,chunk_id) DO UPDATE SET downloads=pack_member_access_windows.downloads+EXCLUDED.downloads").bind(packs).bind(chunks).bind(hours).bind(counts)
}

pub(in super::super) fn observation_limits() -> sqlx::RawSql {
    sqlx::raw_sql("SET LOCAL lock_timeout='100ms'; SET LOCAL statement_timeout='1s'")
}

pub(in super::super) fn node_access(
    node: Uuid,
) -> Rows<'static, (Uuid, i64, Vec<u8>, DateTime<Utc>, DateTime<Utc>, String)> {
    sqlx::query_as("SELECT boot_id,sequence,request_hash,gap_since,received_at,COALESCE((SELECT value->>'access_flush_interval' FROM runtime_settings WHERE section='metrics'),'1m') FROM node_access WHERE node_id=$1 FOR UPDATE").bind(node)
}

pub(in super::super) fn node_access_saved<'a>(
    node: Uuid,
    boot: Uuid,
    sequence: i64,
    hash: &'a [u8],
    now: DateTime<Utc>,
    gap: DateTime<Utc>,
) -> Statement<'a> {
    sqlx::query("INSERT INTO node_access(node_id,boot_id,sequence,request_hash,received_at,gap_since) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(node_id) DO UPDATE SET boot_id=EXCLUDED.boot_id,sequence=EXCLUDED.sequence,request_hash=EXCLUDED.request_hash,received_at=EXCLUDED.received_at,gap_since=EXCLUDED.gap_since")
        .bind(node).bind(boot).bind(sequence).bind(hash).bind(now).bind(gap)
}

pub(in super::super) fn access_coverage(dropped: bool, stale: f64) -> Statement<'static> {
    sqlx::query("WITH coverage AS (SELECT count(*) expected,count(a.node_id) FILTER(WHERE a.boot_id=s.boot_id AND s.credential_generation=n.credential_generation AND s.expires_at>clock_timestamp() AND a.received_at>=now()-$2*interval '1 second') reported,min(a.received_at) flushed,max(a.gap_since) gap FROM nodes n LEFT JOIN node_sessions s ON s.node_id=n.id LEFT JOIN node_access a ON a.node_id=n.id WHERE n.enabled AND n.role IN ('data','node')) UPDATE mokyu_meta m SET access_coverage_since=CASE WHEN $1 OR m.access_flushed_at<now()-$2*interval '1 second' OR (m.deployment_mode='cluster' AND c.expected<>c.reported) THEN now() WHEN m.deployment_mode='cluster' THEN GREATEST(m.access_coverage_since,c.gap) ELSE m.access_coverage_since END,access_flushed_at=CASE WHEN m.deployment_mode<>'cluster' THEN now() WHEN c.expected=c.reported THEN COALESCE(c.flushed,now()) ELSE LEAST(m.access_flushed_at,now()-($2+1)*interval '1 second') END FROM coverage c WHERE m.singleton").bind(dropped).bind(stale)
}

async fn expire_access(pool: &PgPool, retention: f64, batch: i64) -> Result<()> {
    for sql in [
        "DELETE FROM pack_read_shapes WHERE (stream_id,start_bytes,end_bytes,observed_hour) IN (SELECT stream_id,start_bytes,end_bytes,observed_hour FROM pack_read_shapes WHERE observed_hour<EXTRACT(EPOCH FROM now())::double precision-$1::double precision ORDER BY observed_hour LIMIT $2)",
        super::super::queries::tier::EXPIRE,
        super::super::queries::tier::EXPIRE_TOTALS,
        "DELETE FROM chunk_access_windows WHERE (window_start,chunk_id) IN (SELECT window_start,chunk_id FROM chunk_access_windows WHERE window_start<now()-$1*interval '1 second' ORDER BY window_start LIMIT $2)",
        "DELETE FROM pack_access_windows WHERE (window_start,pack_id) IN (SELECT window_start,pack_id FROM pack_access_windows WHERE window_start<now()-$1*interval '1 second' ORDER BY window_start LIMIT $2)",
        "DELETE FROM pack_member_access_windows WHERE (window_start,pack_id,chunk_id) IN (SELECT window_start,pack_id,chunk_id FROM pack_member_access_windows WHERE window_start<now()-$1*interval '1 second' ORDER BY window_start LIMIT $2)",
    ] {
        if let Err(e) = sqlx::query(sql)
            .bind(retention)
            .bind(batch)
            .execute(pool)
            .await
        {
            tracing::warn!(error=%e,"access statistics cleanup failed");
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl MetricsStore for Backend {
    async fn inventory(
        &self,
        metrics: &crate::config::MetricsSettings,
        maintenance: &crate::config::Maintenance,
    ) -> Result<Value> {
        transactions::collect_inventory(&self.pool, metrics, maintenance).await
    }
    async fn storage_inventory(&self, storage: Uuid) -> Result<Value> {
        transactions::collect_storage(&self.pool, storage).await
    }
    async fn usage(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        limit: usize,
    ) -> Result<UsageSnapshot> {
        transactions::read_usage(&self.pool, principal, scope, limit).await
    }
    async fn runtime(&self, principal: &Principal) -> Result<RuntimeHistory> {
        transactions::read_runtime(&self.pool, principal).await
    }
    async fn sample_runtime(&self, point: RuntimePoint, retention: f64) -> Result<()> {
        transactions::sample_runtime(&self.pool, point, retention).await
    }
    async fn flush_access(&self, pending: AccessBatch, stale: f64) -> Result<()> {
        transactions::flush_access(&self.pool, pending, stale).await
    }
    async fn expire_access(&self, retention: f64, batch: i64) -> Result<()> {
        expire_access(&self.pool, retention, batch).await
    }
}
