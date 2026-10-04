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
    SELECT c.id AS chunk_id,CASE WHEN l.id IS NOT NULL THEN 'chunk' WHEN p.id IS NOT NULL THEN 'pack' ELSE 'pending' END AS kind,
           coalesce(l.id,p.id,c.id) AS source_id,coalesce(l.stored_size,p.stored_size) AS stored_size,u.bytes
    FROM chunk_unique u JOIN chunks c ON c.id=u.chunk_id
    LEFT JOIN chunk_locations l ON l.chunk_id=c.id AND l.state='ready'
    LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready'
), source_totals AS MATERIALIZED (
    SELECT kind,source_id,sum(bytes) AS raw_bytes,max(stored_size) AS stored_size FROM sources GROUP BY kind,source_id
), attribution AS MATERIALIZED (
    SELECT a.bucket_id,a.chunk_id,a.raw_bytes,s.kind,s.source_id,
           a.raw_bytes/t.raw_bytes*t.stored_size AS encoded_bytes
    FROM allocated a JOIN sources s USING(chunk_id) JOIN source_totals t USING(kind,source_id)
), scopes AS MATERIALIZED (
    SELECT id,bucket_ids FROM storage_insights
), scope_ranges AS (
    SELECT s.id,r.chunk_id,range_agg(r.spans) AS spans FROM scopes s JOIN ranges r ON s.bucket_ids IS NULL OR r.bucket_id=ANY(s.bucket_ids)
    GROUP BY s.id,r.chunk_id
), scope_unique AS (
    SELECT r.id,count(DISTINCT r.chunk_id) AS chunks,
           count(DISTINCT s.source_id) FILTER(WHERE s.kind='pack') AS packs,
           sum(upper(span)-lower(span)) AS bytes,
           coalesce(sum(upper(span)-lower(span)) FILTER(WHERE s.kind='pack'),0) AS packed_bytes,
           coalesce(sum(upper(span)-lower(span)) FILTER(WHERE s.kind='pending'),0) AS pending_bytes
    FROM scope_ranges r JOIN sources s USING(chunk_id) CROSS JOIN LATERAL unnest(r.spans) span GROUP BY r.id
), scope_allocated AS (
    SELECT s.id,sum(a.raw_bytes) AS raw_bytes,coalesce(sum(a.encoded_bytes),0) AS encoded_bytes,
           bool_and(a.encoded_bytes IS NOT NULL) AS complete
    FROM scopes s JOIN attribution a ON s.bucket_ids IS NULL OR a.bucket_id=ANY(s.bucket_ids) GROUP BY s.id
), scope_logical AS (
    SELECT s.id,count(b.id) AS buckets,coalesce(sum(q.used_bytes),0) AS bytes,coalesce(sum(q.object_count),0) AS objects
    FROM scopes s LEFT JOIN buckets b ON s.bucket_ids IS NULL OR b.id=ANY(s.bucket_ids)
    LEFT JOIN quota_accounts q ON q.kind='bucket' AND q.id=b.id GROUP BY s.id
), physical AS (
    SELECT 'chunk' AS kind,l.id AS source_id,l.state,l.stored_size,
           l.state IN ('retired','deleting') OR (c.unreferenced_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)) AS garbage
    FROM chunk_locations l JOIN chunks c ON c.id=l.chunk_id WHERE l.state IN ('ready','retired','deleting','uploading')
    UNION ALL SELECT 'pack',id,state,stored_size,state IN ('retired','deleting') FROM packs WHERE state<>'deleted'
), bridge AS (
    SELECT jsonb_build_object(
        'indexed_bytes',coalesce(sum(p.stored_size) FILTER(WHERE state IN ('ready','retired','deleting')),0)::text,
        'selected_bytes',coalesce(sum(p.stored_size) FILTER(WHERE t.source_id IS NOT NULL),0)::text,
        'retained_bytes',coalesce(sum(p.stored_size) FILTER(WHERE t.source_id IS NULL AND state IN ('ready','retired','deleting') AND NOT garbage),0)::text,
        'gc_bytes',coalesce(sum(p.stored_size) FILTER(WHERE t.source_id IS NULL AND garbage),0)::text,
        'unconfirmed_bytes',coalesce(sum(p.stored_size) FILTER(WHERE state IN ('preparing','uploading')),0)::text,
        'chunk_objects',count(*) FILTER(WHERE p.kind='chunk' AND state IN ('ready','retired','deleting'))::text,
        'pack_objects',count(*) FILTER(WHERE p.kind='pack' AND state IN ('ready','retired','deleting'))::text
    ) AS data FROM physical p LEFT JOIN source_totals t ON t.kind=p.kind AND t.source_id=p.source_id AND t.kind<>'pending'
), summaries AS (
    SELECT l.id,jsonb_build_object(
        'buckets',l.buckets::text,'objects',l.objects::text,'logical_bytes',l.bytes::text,
        'unique_bytes',coalesce(u.bytes,0)::text,'attributed_raw_bytes',round(coalesce(a.raw_bytes,0),6)::text,
        'encoded_bytes',CASE WHEN coalesce(a.complete,true) THEN round(coalesce(a.encoded_bytes,0),6)::text END,
        'encoded_known_bytes',round(coalesce(a.encoded_bytes,0),6)::text,
        'local_savings', (l.bytes-coalesce(u.bytes,0))::text,
        'shared_savings',round(coalesce(u.bytes,0)-coalesce(a.raw_bytes,0),6)::text,
        'encoding_savings',CASE WHEN coalesce(a.complete,true) THEN round(coalesce(a.raw_bytes,0)-coalesce(a.encoded_bytes,0),6)::text END,
        'chunks',coalesce(u.chunks,0)::text,'packs',coalesce(u.packs,0)::text,
        'packed_bytes',coalesce(u.packed_bytes,0)::text,'pending_bytes',coalesce(u.pending_bytes,0)::text,
        'physical',CASE WHEN l.id='global' THEN (SELECT data FROM bridge) END
    ) AS data FROM scope_logical l LEFT JOIN scope_unique u USING(id) LEFT JOIN scope_allocated a USING(id)
)
UPDATE storage_insights i SET as_of=transaction_timestamp(),data=s.data FROM summaries s WHERE s.id=i.id;
