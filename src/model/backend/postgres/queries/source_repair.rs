use super::*;

pub(in super::super) fn lock_task_detail(task: Execution) -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT detail FROM tasks WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()) FOR UPDATE")
        .bind(task.id).bind(task.token)
}

pub(in super::super) fn advance_storage_task(
    id: Uuid,
    detail: Value,
    processed: i64,
    done: bool,
) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET cursor=NULL,detail=$2,processed=processed+$3,state=CASE WHEN $4 THEN 'completed' ELSE state END,updated_at=now() WHERE id=$1")
        .bind(id).bind(detail).bind(processed).bind(done)
}

pub(in super::super) fn lock_task_family(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM tasks WHERE id=$1 OR parent_task_id=$1 ORDER BY (id=$1),id FOR UPDATE",
    )
    .bind(id)
}

pub(in super::super) fn runtime_diagnostic_limits() -> sqlx::RawSql {
    sqlx::raw_sql(
        "SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='100ms'; SET LOCAL work_mem='4MB'",
    )
}

pub(in super::super) fn runtime_aggregate_lock() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(734922709851032)")
}

pub(in super::super) fn runtime_aggregate_due(
    interval: f64,
) -> Rows<'static, (DateTime<Utc>, i64, bool)> {
    sqlx::query_as("SELECT now(),(SELECT count(*) FROM nodes WHERE enabled),NOT EXISTS(SELECT 1 FROM runtime_samples WHERE data->>'scope'='cluster' AND sampled_at>now()-$1*interval '1 second')").bind(interval)
}

pub(in super::super) fn node_runtime_sample<'a>(
    id: Uuid,
    sample: &'a crate::model::metrics::NodeSample,
) -> Statement<'a> {
    sqlx::query("INSERT INTO node_runtime_samples(node_id,boot_id,sequence,uptime_seconds,data) SELECT $1,$2,$3,$4,$5 FROM node_reports r WHERE r.node_id=$1 AND r.boot_id=$2 AND r.sequence>=$3 AND NOT EXISTS(SELECT 1 FROM node_runtime_samples s WHERE s.node_id=$1 AND s.boot_id=$2 AND (s.sequence>=$3 OR s.uptime_seconds>=$4)) ON CONFLICT DO NOTHING")
        .bind(id).bind(sample.boot_id).bind(sample.sequence).bind(sample.uptime_seconds as i64).bind(sqlx::types::Json(sample))
}

type NodeRuntimeRow = (
    Uuid,
    DateTime<Utc>,
    sqlx::types::Json<crate::model::metrics::NodeSample>,
);

pub(in super::super) fn runtime_node_window(
    seconds: f64,
) -> Rows<
    'static,
    (
        Option<Uuid>,
        Option<DateTime<Utc>>,
        Option<sqlx::types::Json<crate::model::metrics::NodeSample>>,
        i64,
    ),
> {
    // The cap bounds coordinator memory; omitted nodes remain in expected coverage.
    sqlx::query_as("WITH members AS (SELECT id FROM nodes WHERE enabled AND last_seen_at>=now()-$2*interval '1 second' ORDER BY id LIMIT 4096), samples AS (SELECT n.id,s.received_at,s.data,s.sequence FROM members n JOIN node_reports r ON r.node_id=n.id CROSS JOIN LATERAL (SELECT received_at,data,sequence FROM node_runtime_samples s WHERE s.node_id=n.id AND s.boot_id=r.boot_id AND s.received_at>=now()-$1*interval '1 second' ORDER BY received_at DESC,sequence DESC LIMIT 2) s) SELECT s.id,s.received_at,s.data,e.expected FROM (SELECT count(*) AS expected FROM nodes WHERE enabled) e LEFT JOIN samples s ON true ORDER BY s.id,s.received_at DESC,s.sequence DESC").bind(seconds).bind(CONTACT_SECONDS)
}

pub(in super::super) fn node_runtime_history(
    id: Uuid,
    query: &crate::model::metrics::RuntimeQuery,
) -> Rows<'static, NodeRuntimeRow> {
    sqlx::query_as("SELECT node_id,received_at,data FROM (SELECT node_id,received_at,sequence,data FROM node_runtime_samples WHERE node_id=$1 AND received_at>=now()-$2*interval '1 hour' ORDER BY received_at DESC,sequence DESC LIMIT $3) s ORDER BY received_at,sequence").bind(id).bind(f64::from(query.hours)).bind(query.limit)
}

pub(in super::super) fn cluster_runtime_history(
    query: &crate::model::metrics::RuntimeQuery,
) -> Scalar<'static, sqlx::types::Json<crate::model::metrics::ClusterRuntimePoint>> {
    sqlx::query_scalar("SELECT data FROM (SELECT sampled_at,data FROM runtime_samples WHERE data->>'scope'='cluster' AND sampled_at>=now()-$1*interval '1 hour' ORDER BY sampled_at DESC LIMIT $2) s ORDER BY sampled_at").bind(f64::from(query.hours)).bind(query.limit)
}

pub(in super::super) fn cluster_runtime_sample(
    point: &crate::model::metrics::ClusterRuntimePoint,
) -> Statement<'_> {
    sqlx::query("INSERT INTO runtime_samples(sampled_at,data) VALUES($1,$2) ON CONFLICT DO NOTHING")
        .bind(point.at)
        .bind(sqlx::types::Json(point))
}

pub(in super::super) fn node_runtime_expire(retention: f64) -> Statement<'static> {
    sqlx::query("DELETE FROM node_runtime_samples WHERE (node_id,boot_id,sequence) IN (SELECT node_id,boot_id,sequence FROM node_runtime_samples WHERE received_at<now()-$1*interval '1 second' ORDER BY received_at LIMIT 1000)").bind(retention)
}

pub(in super::super) fn cluster_runtime_expire(retention: f64) -> Statement<'static> {
    sqlx::query("DELETE FROM runtime_samples WHERE sampled_at IN (SELECT sampled_at FROM runtime_samples WHERE data->>'scope'='cluster' AND sampled_at<now()-$1*interval '1 second' ORDER BY sampled_at LIMIT 1000)").bind(retention)
}

fn source_scope(query: &mut sqlx::QueryBuilder<Postgres>, stream: Option<Id>, chunks: &[Id]) {
    query.push("c.id IN (");
    if let Some(stream) = stream {
        query
            .push("SELECT chunk_id FROM extents WHERE stream_id=")
            .push_bind(stream);
    } else if chunks.is_empty() {
        query.push("NULL");
    } else {
        let mut values = query.separated(",");
        for id in chunks {
            values.push_bind(*id);
        }
    }
    query.push(")");
}

pub(in super::super) fn source_locks(
    stream: Option<Id>,
    chunks: &[Id],
    phase: u8,
) -> sqlx::QueryBuilder<Postgres> {
    let mut query = sqlx::QueryBuilder::new(match phase {
        // Stream generic plans may scan unrelated chunks; avoid fetching their pack column.
        0 if stream.is_some() => "SELECT TRUE FROM chunks c WHERE ",
        0 => "SELECT c.pack_id IS NOT NULL FROM chunks c WHERE ",
        1 => {
            "SELECT TRUE FROM chunk_encodings l WHERE l.state='ready' AND l.chunk_id IN (SELECT c.id FROM chunks c WHERE "
        }
        2 => {
            "SELECT TRUE FROM packs p WHERE p.state='ready' AND p.id IN (SELECT c.pack_id FROM chunks c WHERE "
        }
        _ => unreachable!(),
    });
    source_scope(&mut query, stream, chunks);
    query.push(match phase {
        0 => " ORDER BY c.id FOR KEY SHARE",
        1 => ") ORDER BY l.encoding_id FOR SHARE",
        _ => ") ORDER BY p.id FOR SHARE",
    });
    query
}

pub(in super::super) fn unavailable_sources(
    stream: Option<Id>,
    chunks: &[Id],
) -> sqlx::QueryBuilder<Postgres> {
    let mut query = sqlx::QueryBuilder::new("SELECT EXISTS(SELECT 1 FROM chunks c WHERE ");
    source_scope(&mut query, stream, chunks);
    query.push(" AND (c.state<>'ready' OR (NOT EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=c.id AND l.state='ready' AND l.quarantined_at IS NULL) AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.state='ready' AND p.quarantined_at IS NULL) AND NOT (EXISTS(SELECT 1 FROM pending_uploads u WHERE u.chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=c.id AND l.quarantined_at IS NOT NULL) AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.quarantined_at IS NOT NULL)))))");
    query
}

pub(in super::super) fn source_writing_stream(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=$1 AND state='writing' FOR UPDATE")
        .bind(stream)
}

pub(in super::super) async fn source_snapshot(
    db: &mut sqlx::PgConnection,
    scope: &crate::model::source::SourceScope,
    lock: bool,
) -> Result<crate::model::source::SourceSnapshot> {
    use crate::model::source::{SourceSnapshot, manifest_digest};
    let suffix = if lock { " FOR SHARE" } else { "" };
    let (storage_revision, marker): (i64, sqlx::types::Json<Value>) = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT p.revision,s.identity_marker FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=$1 AND s.enabled{suffix}")))
        .bind(scope.storage_id).fetch_one(&mut *db).await?;
    let suffix = if lock { " FOR UPDATE" } else { "" };
    if scope.source == crate::model::source::SourceType::Raw {
        let raw: RawSource = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=2 AND tl.physical_id=r.id) AS tier_material,r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id=$1 AND r.storage_id=$2{suffix}")))
            .bind(scope.identity()?).bind(scope.storage_id).fetch_one(&mut *db).await?;
        return Ok(SourceSnapshot {
            scope: scope.clone(),
            state: raw.state.clone(),
            revision: raw.revision,
            quarantine_code: raw.quarantine_code.clone(),
            tier_revision: super::super::tier_transactions::placement_revision(
                db,
                scope.storage_id,
                raw.storage_id,
            )
            .await?,
            physical: PhysicalSource::Raw(raw),
            storage_revision,
            identity_marker: marker.0,
            manifest_digest: None,
        });
    }
    let id = scope.identity()?;
    let (table, column) = if scope.packed() {
        ("packs", "id")
    } else {
        ("chunk_encodings", "encoding_id")
    };
    let (state, revision, quarantine_code): (String, i64, Option<String>) = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT state,quarantine_revision,quarantine_code FROM {table} WHERE {column}=$1 AND storage_id=$2{suffix}")))
        .bind(id).bind(scope.storage_id).fetch_optional(&mut *db).await?.context(crate::model::Error::OperationAborted)?;
    let physical = if scope.packed() {
        PhysicalSource::Pack(
            sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=1 AND tl.physical_id=p.id) AS tier_material,p.* FROM packs p WHERE id=$1 AND storage_id=$2")
                .bind(id)
                .bind(scope.storage_id)
                .fetch_one(&mut *db)
                .await?,
        )
    } else {
        PhysicalSource::Chunk(sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=0 AND tl.physical_id=l.encoding_id) AS tier_material,c.id,l.encoding_id AS current_encoding_id,l.encoding_id AS location_id,c.storage_id,c.hash,c.raw_size,l.standalone_size,l.encryption_algorithm,l.block_key_id,l.standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE l.encoding_id=$1 AND l.storage_id=$2").bind(id).bind(scope.storage_id).fetch_one(&mut *db).await?)
    };
    let manifest_digest = if let PhysicalSource::Pack(pack) = &physical {
        let members = super::pack_work::physical_members(db, id).await?;
        Some(manifest_digest(pack, &members)?)
    } else {
        None
    };
    Ok(SourceSnapshot {
        scope: scope.clone(),
        tier_revision: super::super::tier_transactions::placement_revision(
            db,
            scope.storage_id,
            physical.storage()?,
        )
        .await?,
        physical,
        state,
        revision,
        quarantine_code,
        storage_revision,
        identity_marker: marker.0,
        manifest_digest,
    })
}

pub(in super::super) async fn quarantines(
    db: &mut sqlx::PgConnection,
    storage: Uuid,
    packed: bool,
    after: Id,
    limit: i64,
) -> Result<Vec<crate::model::source::QuarantinedSource>> {
    use crate::model::source::{QuarantinedSource, SourceScope, SourceType};
    let (table, id) = if packed {
        ("packs", "id")
    } else {
        ("chunk_encodings", "encoding_id")
    };
    let rows: Vec<(Id, Uuid, String, String, String, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT {id},{id},state,to_char(quarantined_at AT TIME ZONE 'UTC','YYYY-MM-DD') || 'T' || to_char(quarantined_at AT TIME ZONE 'UTC','HH24:MI:SS.US') || 'Z',quarantine_code,quarantine_revision FROM {table} WHERE storage_id=$1 AND quarantined_at IS NOT NULL AND {id}>$2 ORDER BY {id} LIMIT $3")))
        .bind(storage).bind(after).bind(limit).fetch_all(db).await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, location_id, state, quarantined_at, code, revision)| QuarantinedSource {
                scope: SourceScope {
                    storage_id: storage,
                    source: if packed {
                        SourceType::Pack
                    } else {
                        SourceType::Chunk
                    },
                    id: id.to_string(),
                },
                location_id,
                state,
                quarantined_at,
                code,
                revision: revision.to_string(),
            },
        )
        .collect())
}

pub(in super::super) fn change_source<'a>(
    snapshot: &crate::model::source::SourceSnapshot,
    code: Option<&'a str>,
) -> Result<Statement<'a>> {
    if snapshot.scope.source == crate::model::source::SourceType::Raw {
        return Ok(sqlx::query("UPDATE raw_sources SET quarantined_at=CASE WHEN $1 IS NULL THEN NULL ELSE now() END,quarantine_code=$2,revision=revision+1 WHERE id=$3 AND storage_id=$4 AND revision=$5 AND state='ready'")
            .bind(code).bind(code).bind(snapshot.scope.identity()?).bind(snapshot.scope.storage_id).bind(snapshot.revision));
    }
    let (table, id) = if snapshot.scope.packed() {
        ("packs", "id")
    } else {
        ("chunk_encodings", "encoding_id")
    };
    Ok(sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {table} SET quarantined_at=CASE WHEN $1::text IS NULL THEN NULL ELSE now() END,quarantine_code=$1,quarantine_revision=quarantine_revision+1 WHERE {id}=$2 AND storage_id=$3 AND quarantine_revision=$4 AND state='ready'")))
        .bind(code).bind(snapshot.scope.identity()?).bind(snapshot.scope.storage_id).bind(snapshot.revision))
}

pub(in super::super) fn source_mode(mode: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT deployment_mode=$1 FROM mokyu_meta FOR SHARE").bind(mode)
}

pub(in super::super) fn source_result(task: Uuid, result: Value) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET detail=detail||$2::jsonb,processed=processed+1 WHERE id=$1")
        .bind(task)
        .bind(result)
}

pub(in super::super) fn quarantined_inputs(chunks: &[Id]) -> sqlx::QueryBuilder<Postgres> {
    let mut query = sqlx::QueryBuilder::new("SELECT EXISTS(SELECT 1 FROM chunks c WHERE ");
    source_scope(&mut query, None, chunks);
    query.push(" AND (EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=c.id AND l.state='ready' AND l.quarantined_at IS NOT NULL) OR EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.state='ready' AND p.quarantined_at IS NOT NULL)))");
    query
}

pub(in super::super) fn splice_locks(
    streams: &[Id],
    chunk: Id,
    physical: bool,
) -> sqlx::QueryBuilder<Postgres> {
    let mut query = sqlx::QueryBuilder::new(if physical {
        "SELECT c.id FROM chunks c WHERE c.id="
    } else {
        "SELECT id FROM streams WHERE id IN ("
    });
    if physical {
        query
            .push_bind(chunk)
            .push(" OR c.id IN (SELECT chunk_id FROM extents WHERE stream_id IN (");
    }
    let mut values = query.separated(",");
    for stream in streams {
        values.push_bind(*stream);
    }
    query.push(if physical {
        ")) ORDER BY c.id FOR UPDATE"
    } else {
        ") ORDER BY id FOR UPDATE"
    });
    query
}

pub(in super::super) fn allocated_source(
    stream: Id,
    chunk: &Chunk,
) -> sqlx::QueryBuilder<Postgres> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT l.encoding_id FROM chunks c JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.chunk_id=c.id WHERE c.state='uploading' AND l.state='uploading' AND l.quarantined_at IS NULL AND l.owner_task IS NULL AND c.id=",
    );
    query
        .push_bind(chunk.id)
        .push(" AND c.owner_stream=")
        .push_bind(stream)
        .push(" AND l.encoding_id=")
        .push_bind(chunk.encoding_id)
        .push(" AND l.encoding_id=")
        .push_bind(chunk.location_id)
        .push(" FOR UPDATE");
    query
}

pub(in super::super) fn source_executor(task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT enabled AND NOT recovery_required AND status NOT IN ('released','draining') FROM nodes WHERE id=(SELECT lease_node_id FROM tasks WHERE id=$1) FOR SHARE").bind(task)
}

pub(in super::super) fn source_input_versions(chunks: &[Id]) -> sqlx::QueryBuilder<Postgres> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT c.id,l.encoding_id,l.quarantine_revision,p.id,p.quarantine_revision FROM chunks c LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND (l.state='ready' OR (l.state='uploading' AND l.boot_id IS NOT NULL AND l.quarantined_at IS NOT NULL)) LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready' WHERE ",
    );
    source_scope(&mut query, None, chunks);
    query.push(" ORDER BY c.id,l.encoding_id,p.id");
    query
}

pub(in super::super) fn save_source_inputs(task: Uuid, stamp: &Value) -> Statement<'_> {
    sqlx::query("UPDATE tasks SET detail=jsonb_set(detail,'{source_inputs}',$2::jsonb) WHERE id=$1")
        .bind(task)
        .bind(stamp)
}

pub(in super::super) fn saved_source_inputs(task: Uuid) -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT detail->'source_inputs' FROM tasks WHERE id=$1").bind(task)
}

// Recovery candidates are private to their writing stream until full verification.
pub(in super::super) fn repair_mode() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT deployment_mode IN ('single','single_control') AND NOT maintenance FROM mokyu_meta FOR SHARE",
    )
}

pub(in super::super) fn repair_chunk(input: &ChunkCreate) -> Rows<'_, Chunk> {
    sqlx::query_as("SELECT * FROM chunks WHERE storage_id=$1 AND hash=$2 AND raw_size=$3 AND state='ready' ORDER BY id LIMIT 1 FOR UPDATE")
        .bind(input.storage).bind(input.hash.as_slice()).bind(input.raw_size)
}

pub(in super::super) fn repair_cluster_mode() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT deployment_mode='cluster' AND NOT maintenance FROM mokyu_meta FOR SHARE",
    )
}

pub(in super::super) fn repair_node(node: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM nodes WHERE id=$1 AND enabled AND NOT recovery_required AND status NOT IN ('released','draining') FOR SHARE").bind(node)
}

pub(in super::super) fn repair_lease(stream: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM stream_leases WHERE stream_id=$1 AND lease_type='write' AND expires_at>clock_timestamp() FOR SHARE").bind(stream)
}

pub(in super::super) fn repair_stream(
    stream: Id,
) -> Rows<'static, (Uuid, String, Option<Uuid>, Option<Uuid>)> {
    sqlx::query_as("SELECT bucket_id,stream_type,owner_task_id,owner_lease_token FROM streams WHERE id=$1 AND state='writing' FOR UPDATE").bind(stream)
}

pub(in super::super) fn repair_object(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT o.write_token FROM objects o JOIN streams s ON s.bucket_id=o.bucket_id AND s.object_key=o.object_key WHERE s.id=$1 FOR UPDATE").bind(stream)
}

pub(in super::super) fn repair_part(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT write_token FROM parts WHERE write_token=$1 FOR UPDATE").bind(stream)
}

pub(in super::super) fn repair_target(
    storage: Uuid,
    key: Option<Uuid>,
    algorithm: &str,
) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=$1 AND s.enabled AND s.block_key_id IS NOT DISTINCT FROM $2 AND ((s.block_key_id IS NULL AND $3='none') OR EXISTS(SELECT 1 FROM managed_keys k WHERE k.id=s.block_key_id AND k.key_algorithm=$4 AND k.revoked_at IS NULL)) FOR SHARE").bind(storage).bind(key).bind(algorithm).bind(algorithm)
}

pub(in super::super) fn repair_pending(chunk: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=$1)").bind(chunk)
}

pub(in super::super) fn insert_repair(chunk: &Chunk, stream: Id) -> Scalar<'_, Id> {
    sqlx::query_scalar("INSERT INTO chunk_encodings(chunk_id,storage_id,state,created_at,unreferenced_at,owner_stream,recovery_pending,encryption_algorithm,block_key_id,standalone_compressed) VALUES($1,$2,'uploading',$3,clock_timestamp(),$4,TRUE,$5,$6,FALSE) RETURNING encoding_id").bind(chunk.id).bind(chunk.storage_id).bind(chunk.created_at).bind(stream).bind(&chunk.algorithm).bind(chunk.block_key_id)
}

pub(in super::super) fn repair_candidate(encoding: Id, stream: Id) -> Rows<'static, Chunk> {
    sqlx::query_as("SELECT c.id,l.encoding_id AS current_encoding_id,l.encoding_id AS location_id,c.storage_id,c.hash,c.raw_size,l.standalone_size,l.encryption_algorithm,l.block_key_id,l.standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE l.encoding_id=$1 AND l.owner_stream=$2 AND l.owner_task IS NULL AND l.state='uploading' AND l.recovery_pending AND l.quarantined_at IS NULL FOR UPDATE").bind(encoding).bind(stream)
}

pub(in super::super) fn save_repair_metadata(chunk: &Chunk, stream: Id) -> Statement<'_> {
    sqlx::query("UPDATE chunk_encodings SET standalone_size=$1,standalone_compressed=$2,nonce=$3,nonce_id=$6 WHERE encoding_id=$4 AND owner_stream=$5 AND state='uploading'").bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(chunk.encoding_id).bind(stream).bind(crate::content::nonce::id(chunk.nonce.as_deref()))
}

pub(in super::super) fn retire_repair_sources(chunk: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',sync_pending=FALSE,boot_id=NULL,unreferenced_at=clock_timestamp() WHERE chunk_id=$1 AND (state='ready' OR (state='uploading' AND boot_id IS NOT NULL)) AND quarantined_at IS NOT NULL").bind(chunk)
}

pub(in super::super) fn publish_repair(encoding: Id, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET sync_pending=CASE WHEN boot_id IS NOT NULL THEN FALSE ELSE sync_pending END,boot_id=NULL,state='ready',owner_stream=NULL,recovery_pending=FALSE,unreferenced_at=NULL,stored_at=clock_timestamp() WHERE encoding_id=$1 AND owner_stream=$2 AND state='uploading' AND quarantined_at IS NULL").bind(encoding).bind(stream)
}

pub(in super::super) fn discard_verified_repair(encoding: Id, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',owner_stream=NULL,recovery_pending=FALSE,unreferenced_at=clock_timestamp(),stored_at=clock_timestamp() WHERE encoding_id=$1 AND owner_stream=$2 AND state='uploading'").bind(encoding).bind(stream)
}

pub(in super::super) fn publish_repaired_chunk(chunk: &Chunk) -> Statement<'_> {
    sqlx::query("UPDATE chunks SET current_encoding_id=$1,pack_id=NULL,standalone_size=$2,standalone_compressed=$3,nonce=$4,encryption_algorithm=$5,block_key_id=$6,unreferenced_at=NULL WHERE id=$7 AND state='ready'").bind(chunk.encoding_id).bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(&chunk.algorithm).bind(chunk.block_key_id).bind(chunk.id)
}

pub(in super::super) fn retire_stream_encodings(stream: Id, batch: i64) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET owner_stream=NULL,state='retired',unreferenced_at=clock_timestamp() WHERE encoding_id IN (SELECT encoding_id FROM chunk_encodings WHERE owner_stream=$1 AND state='uploading' ORDER BY encoding_id LIMIT $2 FOR UPDATE)").bind(stream).bind(batch)
}

pub(in super::super) fn repair_owner(
    stream: Id,
) -> Rows<'static, (Uuid, String, Option<Uuid>, Option<Uuid>)> {
    sqlx::query_as("SELECT bucket_id,stream_type,owner_task_id,owner_lease_token FROM streams WHERE id=$1 AND state='writing'").bind(stream)
}

pub(in super::super) fn repair_upload(stream: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM uploads WHERE id=(SELECT r.upload_id FROM quota_writes w JOIN quota_reservations r ON r.id=w.reservation_id WHERE w.stream_id=$1) AND state='active' FOR UPDATE").bind(stream)
}

pub(in super::super) fn source_audit<'a>(
    principal: &'a Principal,
    action: &'a str,
    target: &'a str,
    mut detail: Value,
) -> Result<Statement<'a>> {
    let (source, fallback, access_key) = match principal {
        Principal::Service { access_key, .. } => {
            detail["access_key_id"] = serde_json::json!(access_key);
            ("s3", "S3 credential", Some(access_key.as_str()))
        }
        Principal::User { .. } => ("web", "Deleted account", None),
        Principal::Token { .. } => ("token", "Deleted account", None),
        Principal::Local => ("cli", "Local CLI", None),
        _ => ("cli", "Internal operation", None),
    };
    if let Some(id) = crate::model::audit::current() {
        detail["request_audit_id"] = serde_json::json!(id.to_string());
    }
    ensure!(
        serde_json::to_vec(&detail)?.len() <= 262144,
        "audit detail exceeds its bound"
    );
    Ok(sqlx::query("INSERT INTO audit_events(actor_id,actor_label,token_id,source,action,target,outcome,finished_at,detail) VALUES($1,COALESCE((SELECT username FROM web_users WHERE id=$1),NULLIF(substr((SELECT label FROM s3_credentials WHERE access_key_id=$2),1,64),''),$3),$4,$5,$6,$7,'succeeded',now(),$8)")
        .bind(principal.user_id()).bind(access_key).bind(fallback).bind(principal.token_id()).bind(source).bind(action).bind(target).bind(detail))
}
