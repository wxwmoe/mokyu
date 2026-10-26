use super::*;

pub(crate) async fn ready_encoding(
    db: impl sqlx::PgExecutor<'_>,
    chunk: Id,
) -> Result<Option<(Id, i32)>> {
    Ok(sqlx::query_as("SELECT encoding_id,standalone_size FROM chunk_encodings WHERE chunk_id=$1 AND state='ready'").bind(chunk).fetch_optional(db).await?)
}

pub(crate) async fn pack_has_pending(db: impl sqlx::PgExecutor<'_>, pack: Id) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE source_pack_id=$1)")
            .bind(pack)
            .fetch_one(db)
            .await?,
    )
}

pub(crate) async fn pack_layout(db: impl sqlx::PgExecutor<'_>, pack: Id) -> Result<Vec<Mapping>> {
    Ok(sqlx::query_as("SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at,m.offset_bytes,c.pack_id FROM pack_members m JOIN chunks c ON c.id=m.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE m.pack_id=$1 AND c.pack_id=$1 AND c.state='ready' ORDER BY m.ordinal LIMIT 4096").bind(pack).fetch_all(db).await?)
}

pub(crate) async fn pack_has_writers(db: impl sqlx::PgExecutor<'_>, pack: Id) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents e JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' JOIN streams s ON s.id=e.stream_id WHERE c.pack_id=$1 AND s.state='writing')").bind(pack).fetch_one(db).await?)
}

pub(crate) async fn pack_raw_size(db: impl sqlx::PgExecutor<'_>, pack: Id) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT raw_size FROM packs WHERE id=$1 AND state='ready'")
            .bind(pack)
            .fetch_one(db)
            .await?,
    )
}

pub(crate) async fn chunk_has_references(db: impl sqlx::PgExecutor<'_>, chunk: Id) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents WHERE chunk_id=$1)")
            .bind(chunk)
            .fetch_one(db)
            .await?,
    )
}

pub(crate) async fn pack_stored_size(db: impl sqlx::PgExecutor<'_>, pack: Id) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT stored_size FROM packs WHERE id=$1")
            .bind(pack)
            .fetch_one(db)
            .await?,
    )
}

pub(crate) async fn pack_candidates(
    db: impl sqlx::PgExecutor<'_>,
    stream: Id,
    offset: i64,
    cooldown: f64,
) -> Result<Vec<PackCandidate>> {
    Ok(sqlx::query_as::<_, PackCandidateRow>("SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at,e.offset_bytes,c.pack_id,(NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings q WHERE q.chunk_id=c.id AND q.state='ready' AND q.quarantined_at IS NOT NULL) AND NOT EXISTS(SELECT 1 FROM packs q WHERE q.id=c.pack_id AND q.quarantined_at IS NOT NULL) AND e.source_offset=0 AND e.length=c.raw_size AND (SELECT count(*) FROM extents WHERE chunk_id=c.id)=1 AND COALESCE(c.repack_after<=now(),true) AND (c.split_at IS NULL OR c.reference_changed_at<=now()-$3*interval '1 second')) eligible,CASE WHEN c.pack_id IS NULL THEN 1 ELSE (SELECT count(*) FROM chunks WHERE pack_id=c.pack_id AND state='ready') END unit_count FROM extents e JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' JOIN streams s ON s.id=e.stream_id WHERE e.stream_id=$1 AND e.offset_bytes>=$2 AND s.state='ready' AND c.state='ready' AND EXISTS(SELECT 1 FROM objects WHERE stream_id=s.id) ORDER BY e.offset_bytes LIMIT 1026").bind(stream).bind(offset).bind(cooldown).fetch_all(db).await?.into_iter().map(|r| PackCandidate {row:r.row,eligible:r.eligible,unit_count:r.unit_count}).collect())
}

pub(crate) async fn pack_member_count(db: impl sqlx::PgExecutor<'_>, pack: Id) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM chunks WHERE pack_id=$1 AND state='ready'")
            .bind(pack)
            .fetch_one(db)
            .await?,
    )
}

pub(crate) async fn physical_bytes(db: impl sqlx::PgExecutor<'_>, ids: &[Id]) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT COALESCE(sum(bytes),0)::bigint FROM (SELECT standalone_size bytes FROM chunk_encodings WHERE chunk_id=ANY($1) AND state='ready' UNION ALL SELECT stored_size FROM packs WHERE id IN (SELECT pack_id FROM chunks WHERE id=ANY($1))) physical").bind(ids).fetch_one(db).await?)
}

pub(crate) async fn next_pack(
    db: impl sqlx::PgExecutor<'_>,
    after: Id,
    pack: Option<Id>,
    upper: Option<DateTime<Utc>>,
    storage: Option<Uuid>,
) -> Result<Option<Id>> {
    Ok(sqlx::query_scalar("SELECT id FROM packs WHERE state='ready' AND id>$1 AND ($2::uuid IS NULL OR id=$2) AND ($4::uuid IS NULL OR storage_id=$4) AND created_at<=COALESCE($3::timestamptz,now()) ORDER BY id LIMIT 1").bind(after).bind(pack).bind(upper).bind(storage).fetch_optional(db).await?)
}

pub(crate) async fn next_pack_stream(
    db: impl sqlx::PgExecutor<'_>,
    after: Id,
    offset: i64,
    upper: DateTime<Utc>,
    storage: Option<Uuid>,
) -> Result<Option<Id>> {
    Ok(sqlx::query_scalar("SELECT s.id FROM streams s JOIN objects o ON o.stream_id=s.id JOIN buckets b ON b.id=o.bucket_id WHERE s.state='ready' AND (s.id>$1 OR(s.id=$1 AND $2>=0)) AND ($4::uuid IS NULL OR b.storage_id=$4) AND s.created_at<=$3 ORDER BY s.id LIMIT 1").bind(after).bind(offset).bind(upper).bind(storage).fetch_optional(db).await?)
}

pub(crate) async fn next_pack_change(
    db: impl sqlx::PgExecutor<'_>,
    cutoff: DateTime<Utc>,
    policy: &str,
    storage: Option<Uuid>,
) -> Result<Option<(Id, i64)>> {
    Ok(sqlx::query_as("SELECT pc.pack_id,pc.generation FROM pack_changes pc JOIN packs p ON p.id=pc.pack_id WHERE pc.next_check_at<=$1 AND pc.reason=$2 AND ($3::uuid IS NULL OR p.storage_id=$3) ORDER BY pc.next_check_at,pc.pack_id LIMIT 1").bind(cutoff).bind(policy).bind(storage).fetch_optional(db).await?)
}

pub(crate) async fn finish_pack_change(
    db: impl sqlx::PgExecutor<'_>,
    task: Execution,
    pack: Id,
    generation: i64,
) -> Result<()> {
    sqlx::query("WITH execution AS (SELECT id FROM tasks WHERE id=$3 AND state='running' AND lease_token=$4 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()) FOR UPDATE) DELETE FROM pack_changes WHERE pack_id=$1 AND generation=$2 AND EXISTS(SELECT 1 FROM execution)")
            .bind(pack)
            .bind(generation)
            .bind(task.id).bind(task.token).execute(db)
            .await?;
    Ok(())
}

pub(crate) async fn next_pack_queue(
    db: impl sqlx::PgExecutor<'_>,
    cutoff: DateTime<Utc>,
    policy: &str,
    storage: Option<Uuid>,
) -> Result<Option<(Id, i64, i64)>> {
    Ok(sqlx::query_as("SELECT pm.stream_id,pm.cursor,pm.generation FROM pack_maintenance pm JOIN streams s ON s.id=pm.stream_id JOIN objects o ON o.stream_id=s.id JOIN buckets b ON b.id=o.bucket_id WHERE pm.next_check_at<=$1 AND pm.reason=$2 AND ($3::uuid IS NULL OR b.storage_id=$3) ORDER BY pm.next_check_at,pm.stream_id LIMIT 1").bind(cutoff).bind(policy).bind(storage).fetch_optional(db).await?)
}

pub(crate) async fn stream_repack_after(
    db: impl sqlx::PgExecutor<'_>,
    stream: Id,
    cooldown: f64,
) -> Result<Option<DateTime<Utc>>> {
    Ok(sqlx::query_scalar("SELECT max(GREATEST(c.repack_after,c.reference_changed_at+$2*interval '1 second')) FROM extents e JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=$1 AND c.split_at IS NOT NULL AND GREATEST(c.repack_after,c.reference_changed_at+$2*interval '1 second')>now() AND (SELECT count(*) FROM extents WHERE chunk_id=c.id)=1").bind(stream).bind(cooldown).fetch_one(db).await?)
}

pub(crate) async fn defer_pack_queue(
    db: impl sqlx::PgExecutor<'_>,
    task: Execution,
    stream: Id,
    generation: i64,
    later: DateTime<Utc>,
) -> Result<()> {
    sqlx::query("WITH execution AS (SELECT id FROM tasks WHERE id=$4 AND state='running' AND lease_token=$5 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()) FOR UPDATE) UPDATE pack_maintenance SET cursor=0,next_check_at=$3,reason='repack' WHERE stream_id=$1 AND generation=$2 AND EXISTS(SELECT 1 FROM execution)").bind(stream).bind(generation).bind(later).bind(task.id).bind(task.token).execute(db).await?;
    Ok(())
}

pub(crate) async fn finish_pack_queue(
    db: impl sqlx::PgExecutor<'_>,
    task: Execution,
    stream: Id,
    generation: i64,
) -> Result<()> {
    sqlx::query("WITH execution AS (SELECT id FROM tasks WHERE id=$3 AND state='running' AND lease_token=$4 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()) FOR UPDATE) DELETE FROM pack_maintenance WHERE stream_id=$1 AND generation=$2 AND EXISTS(SELECT 1 FROM execution)")
            .bind(stream)
            .bind(generation)
            .bind(task.id).bind(task.token).execute(db)
            .await?;
    Ok(())
}

pub(crate) async fn advance_pack_queue(
    db: impl sqlx::PgExecutor<'_>,
    task: Execution,
    stream: Id,
    generation: i64,
    cursor: i64,
) -> Result<()> {
    sqlx::query("WITH execution AS (SELECT id FROM tasks WHERE id=$4 AND state='running' AND lease_token=$5 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()) FOR UPDATE) UPDATE pack_maintenance SET cursor=$3 WHERE stream_id=$1 AND generation=$2 AND EXISTS(SELECT 1 FROM execution)")
            .bind(stream)
            .bind(generation)
            .bind(cursor)
            .bind(task.id).bind(task.token).execute(db)
            .await?;
    Ok(())
}

pub(crate) async fn next_range_pack(
    db: impl sqlx::PgExecutor<'_>,
    storage: Uuid,
    cutoff: DateTime<Utc>,
    window: f64,
    minimum: i64,
) -> Result<Option<Id>> {
    Ok(sqlx::query_scalar("SELECT p.id FROM packs p JOIN pack_access_windows w ON w.pack_id=p.id WHERE p.storage_id=$4 AND p.state='ready' AND p.created_at<=$1 AND (p.range_checked_at IS NULL OR p.range_checked_at<$1) AND w.window_start>=date_trunc('hour',now()-$2*interval '1 second') GROUP BY p.id HAVING sum(w.partial_downloads)>=$3 AND max(w.updated_at)>COALESCE(p.range_checked_at,'-infinity'::timestamptz) ORDER BY sum(w.partial_bytes) DESC,p.id LIMIT 1").bind(cutoff).bind(window).bind(minimum).bind(storage).fetch_optional(db).await?)
}

pub(crate) async fn range_pack_blocked(db: impl sqlx::PgExecutor<'_>, pack: Id) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE source_pack_id=$1) OR EXISTS(SELECT 1 FROM chunks c JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id WHERE c.pack_id=$1 AND s.state='writing')").bind(pack).fetch_one(db).await?)
}

pub(crate) async fn pack_range_totals(
    db: impl sqlx::PgExecutor<'_>,
    pack: Id,
    window: f64,
) -> Result<(i64, i64, i64)> {
    Ok(sqlx::query_as("SELECT COALESCE(sum(partial_downloads),0)::bigint,COALESCE(sum(partial_bytes),0)::bigint,COALESCE(sum(downloads-partial_downloads),0)::bigint FROM pack_access_windows WHERE pack_id=$1 AND window_start>=date_trunc('hour',now()-$2*interval '1 second')").bind(pack).bind(window).fetch_one(db).await?)
}

pub(crate) async fn pack_member_hits(
    db: impl sqlx::PgExecutor<'_>,
    pack: Id,
    window: f64,
) -> Result<Vec<(Id, i64)>> {
    Ok(sqlx::query_as("SELECT chunk_id,sum(downloads)::bigint FROM pack_member_access_windows WHERE pack_id=$1 AND window_start>=date_trunc('hour',now()-$2*interval '1 second') GROUP BY chunk_id").bind(pack).bind(window).fetch_all(db).await?)
}

pub(crate) async fn pack_dimensions(
    db: impl sqlx::PgExecutor<'_>,
    pack: Id,
) -> Result<(i64, i64, i32)> {
    Ok(
        sqlx::query_as("SELECT stored_size,raw_size,member_count FROM packs WHERE id=$1")
            .bind(pack)
            .fetch_one(db)
            .await?,
    )
}

pub(crate) async fn range_pack_layout(
    db: impl sqlx::PgExecutor<'_>,
    pack: Id,
) -> Result<Vec<Mapping>> {
    Ok(sqlx::query_as("SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at,m.offset_bytes,c.pack_id FROM pack_members m JOIN chunks c ON c.id=m.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE m.pack_id=$1 AND c.pack_id=$1 AND c.state='ready' AND EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id) ORDER BY m.ordinal LIMIT 4096").bind(pack).fetch_all(db).await?)
}

pub(crate) async fn defer_range_repack(
    db: impl sqlx::PgExecutor<'_>,
    stream: Id,
    window: f64,
    flush: f64,
    retry: f64,
    offset: i64,
) -> Result<()> {
    sqlx::query("UPDATE chunks c SET repack_after=now()+$4*interval '1 second' FROM mokyu_meta g WHERE c.range_split_at IS NOT NULL AND c.repack_after<=now() AND c.id IN (SELECT chunk_id FROM extents WHERE stream_id=$1 AND offset_bytes>=$5 ORDER BY offset_bytes LIMIT 1026) AND (g.access_coverage_since IS NULL OR g.access_coverage_since>date_trunc('hour',now()-$2*interval '1 second') OR g.access_flushed_at IS NULL OR g.access_flushed_at<now()-$3*interval '1 second' OR EXISTS(SELECT 1 FROM chunk_access_windows w WHERE w.chunk_id=c.id AND w.range_reads>0 AND w.window_start>=date_trunc('hour',now()-$2*interval '1 second')))").bind(stream).bind(window).bind(flush).bind(retry).bind(offset).execute(db).await?;
    Ok(())
}

pub(crate) async fn prepare_outputs(
    db: &mut sqlx::PgConnection,
    task: Uuid,
    storage: Uuid,
    batch: &crate::model::node_pack::Batch,
) -> Result<()> {
    let chunks: Vec<_> = batch.chunks.iter().filter(|c| c.ready.is_none()).collect();
    if !chunks.is_empty() {
        let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO chunk_encodings(encoding_id,chunk_id,storage_id,standalone_compressed,state,created_at,unreferenced_at,owner_task,encryption_algorithm,block_key_id,nonce,nonce_id) ",
        );
        query.push_values(chunks, |mut row, c| {
            row.push_bind(c.location)
                .push_bind(c.id)
                .push_bind(storage)
                .push("false")
                .push("'uploading'")
                .push_bind(batch.created)
                .push("clock_timestamp()")
                .push_bind(task)
                .push_bind(&batch.algorithm)
                .push_bind(batch.key)
                .push_bind(&c.nonce)
                .push_bind(crate::content::nonce::id(c.nonce.as_deref()));
        });
        query.build().execute(&mut *db).await?;
    }
    if !batch.packs.is_empty() {
        let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO packs(id,storage_id,encryption_algorithm,block_key_id,raw_size,member_count,state,unreferenced_at,owner_task,created_at,nonce,nonce_id) ",
        );
        query.push_values(&batch.packs, |mut row, p| {
            let raw: i64 = batch.chunks[p.start..p.end]
                .iter()
                .map(|c| i64::from(c.size))
                .sum();
            row.push_bind(p.location)
                .push_bind(storage)
                .push_bind(&batch.algorithm)
                .push_bind(batch.key)
                .push_bind(raw)
                .push_bind((p.end - p.start) as i32)
                .push("'preparing'")
                .push("clock_timestamp()")
                .push_bind(task)
                .push_bind(batch.created)
                .push_bind(&p.nonce)
                .push_bind(crate::content::nonce::id(p.nonce.as_deref()));
        });
        query.build().execute(db).await?;
    }
    Ok(())
}

pub(crate) async fn record_outputs(
    db: &mut sqlx::PgConnection,
    task: Uuid,
    rows: &[Mapping],
    outputs: &[PackOutput],
) -> Result<()> {
    let packs: Vec<_> = outputs.iter().filter_map(|o| o.pack.as_ref()).collect();
    if !packs.is_empty() {
        let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "UPDATE packs p SET stored_size=v.size,pack_compressed=v.compressed,digest=v.digest FROM (",
        );
        query.push_values(packs, |mut row, p| {
            row.push_bind(p.id)
                .push_bind(p.stored_size)
                .push_bind(p.compressed)
                .push_bind(&p.digest);
        });
        query
            .push(") AS v(id,size,compressed,digest) WHERE p.id=v.id AND p.owner_task=")
            .push_bind(task)
            .push(" AND p.state='preparing'");
        query.build().execute(&mut *db).await?;
        let sizes: std::collections::BTreeMap<_, _> = rows
            .iter()
            .map(|r| (r.chunk.id, i64::from(r.chunk.raw_size)))
            .collect();
        let mut members = Vec::new();
        for output in outputs {
            if let Some(pack) = &output.pack {
                let mut offset = 0;
                for (ordinal, id) in output.chunks.iter().enumerate() {
                    members.push((pack.id, ordinal as i32, *id, offset));
                    offset += sizes.get(id).context("missing pack source")?;
                }
            }
        }
        let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO pack_members(pack_id,ordinal,chunk_id,offset_bytes) ",
        );
        query.push_values(members, |mut row, (pack, ordinal, id, offset)| {
            row.push_bind(pack)
                .push_bind(ordinal)
                .push_bind(id)
                .push_bind(offset);
        });
        query.build().execute(&mut *db).await?;
    }
    let chunks: Vec<_> = outputs
        .iter()
        .filter_map(|o| o.independent.as_ref())
        .collect();
    if !chunks.is_empty() {
        let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "UPDATE chunk_encodings c SET standalone_size=v.size,standalone_compressed=v.compressed FROM (",
        );
        query.push_values(chunks, |mut row, c| {
            row.push_bind(c.encoding_id)
                .push_bind(c.stored_size)
                .push_bind(c.compressed);
        });
        query
            .push(") AS v(id,size,compressed) WHERE c.encoding_id=v.id AND c.owner_task=")
            .push_bind(task)
            .push(" AND c.state='uploading'");
        query.build().execute(db).await?;
    }
    Ok(())
}

pub(crate) async fn publish_outputs(
    db: &mut sqlx::PgConnection,
    rows: &[Mapping],
    outputs: &[PackOutput],
    split: bool,
    range: bool,
    cooldown: f64,
) -> Result<()> {
    let positions: std::collections::BTreeMap<_, _> =
        rows.iter().map(|r| (r.chunk.id, r.offset_bytes)).collect();
    let mut first = Vec::new();
    let mut second = Vec::new();
    let mut distances = Vec::new();
    for output in outputs.iter().filter(|o| o.pack.is_some()) {
        for pair in output.chunks.windows(2) {
            first.push(pair[0]);
            second.push(pair[1]);
            distances.push(
                positions.get(&pair[1]).context("missing pack source")?
                    - positions.get(&pair[0]).context("missing pack source")?,
            );
        }
    }
    anyhow::ensure!(
        matching_references(&mut *db, &first, &second, &distances)
            .await?
            .into_iter()
            .all(|same| same),
        "pack reference boundaries changed; retry maintenance"
    );
    let packs: Vec<_> = outputs
        .iter()
        .filter_map(|o| o.pack.as_ref().map(|p| p.id))
        .collect();
    if !packs.is_empty() {
        sqlx::query("UPDATE packs SET state='ready',unreferenced_at=NULL,stored_at=clock_timestamp() WHERE id=ANY($1) AND state='preparing'").bind(packs).execute(&mut *db).await?;
        let ids: Vec<_> = outputs
            .iter()
            .filter(|o| o.pack.is_some())
            .flat_map(|o| o.chunks.iter().copied())
            .collect();
        retire_pack_encodings(&ids).execute(&mut *db).await?;
    }
    let chunks: Vec<_> = outputs
        .iter()
        .filter_map(|o| o.independent.as_ref())
        .collect();
    if !chunks.is_empty() {
        let encodings: Vec<_> = chunks.iter().map(|c| c.encoding_id).collect();
        sqlx::query("UPDATE chunk_encodings SET sync_pending=CASE WHEN boot_id IS NOT NULL THEN FALSE ELSE sync_pending END,boot_id=NULL,state='ready',unreferenced_at=NULL,stored_at=clock_timestamp() WHERE encoding_id=ANY($1) AND state='uploading'").bind(encodings).execute(&mut *db).await?;
        let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "UPDATE chunks c SET current_encoding_id=v.encoding,standalone_size=v.size,standalone_compressed=v.compressed,nonce=v.nonce,encryption_algorithm=v.algorithm,block_key_id=v.key FROM (",
        );
        query.push_values(chunks, |mut row, c| {
            row.push_bind(c.id)
                .push_bind(c.encoding_id)
                .push_bind(c.stored_size)
                .push_bind(c.compressed)
                .push_bind(&c.nonce)
                .push_bind(&c.algorithm)
                .push_bind(c.block_key_id);
        });
        query.push(") AS v(id,encoding,size,compressed,nonce,algorithm,key) WHERE c.id=v.id AND c.state='ready'");
        query.build().execute(&mut *db).await?;
    }
    let ids: Vec<_> = outputs
        .iter()
        .flat_map(|o| o.chunks.iter().copied())
        .collect();
    let packs: Vec<_> = outputs
        .iter()
        .flat_map(|o| o.chunks.iter().map(|_| o.pack.as_ref().map(|p| p.id)))
        .collect();
    sqlx::query("UPDATE chunks c SET pack_id=v.pack,split_at=CASE WHEN $3 THEN now() ELSE split_at END,repack_after=CASE WHEN $3 THEN GREATEST(repack_after,now()+$4*interval '1 second') ELSE repack_after END,range_split_at=CASE WHEN $5 THEN now() WHEN NOT $3 AND v.pack IS NOT NULL THEN NULL ELSE range_split_at END FROM unnest($1::uuid[],$2::uuid[]) v(id,pack) WHERE c.id=v.id")
        .bind(ids).bind(packs).bind(split).bind(cooldown).bind(range).execute(db).await?;
    Ok(())
}

pub(crate) async fn matching_references(
    db: impl sqlx::PgExecutor<'_>,
    first: &[Id],
    second: &[Id],
    distances: &[i64],
) -> Result<Vec<bool>> {
    Ok(sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM extents a JOIN chunks c ON c.id=a.chunk_id WHERE a.chunk_id=v.a AND (a.source_offset<>0 OR a.length<>c.raw_size OR NOT EXISTS(SELECT 1 FROM extents b JOIN chunks d ON d.id=b.chunk_id WHERE b.chunk_id=v.b AND b.stream_id=a.stream_id AND b.offset_bytes=a.offset_bytes+v.distance AND b.source_offset=0 AND b.length=d.raw_size))) AND NOT EXISTS(SELECT 1 FROM extents b JOIN chunks c ON c.id=b.chunk_id WHERE b.chunk_id=v.b AND (b.source_offset<>0 OR b.length<>c.raw_size OR NOT EXISTS(SELECT 1 FROM extents a JOIN chunks d ON d.id=a.chunk_id WHERE a.chunk_id=v.a AND a.stream_id=b.stream_id AND a.offset_bytes=b.offset_bytes-v.distance AND a.source_offset=0 AND a.length=d.raw_size))) FROM unnest($1::uuid[],$2::uuid[],$3::bigint[]) WITH ORDINALITY v(a,b,distance,position) ORDER BY position")
        .bind(first).bind(second).bind(distances).fetch_all(db).await?)
}

pub(crate) async fn reference_boundaries(
    db: impl sqlx::PgExecutor<'_>,
    rows: &[&Mapping],
) -> Result<Vec<bool>> {
    let first: Vec<_> = rows.windows(2).map(|r| r[0].chunk.id).collect();
    let second: Vec<_> = rows.windows(2).map(|r| r[1].chunk.id).collect();
    let distances: Vec<_> = rows
        .windows(2)
        .map(|r| r[1].offset_bytes - r[0].offset_bytes)
        .collect();
    matching_references(db, &first, &second, &distances).await
}
