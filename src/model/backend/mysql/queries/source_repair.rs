use super::*;

pub(in super::super) fn lock_repair_chunk(storage: Uuid, hash: &[u8], size: i32) -> Scalar<'_, Id> {
    sqlx::query_scalar("SELECT id FROM chunks FORCE INDEX (chunks_dedup) WHERE storage_id=? AND active_hash=? AND raw_size=? FOR UPDATE")
        .bind(storage).bind(hash).bind(size)
}

fn source_scope(query: &mut sqlx::QueryBuilder<MySql>, stream: Option<Id>, chunks: &[Id]) {
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
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(match phase {
        0 => "SELECT c.id FROM (",
        1 => "SELECT l.encoding_id FROM (",
        2 => "SELECT p.id FROM (",
        _ => unreachable!(),
    });
    // Materialize only the requested IDs before locking rows; secondary scans can lock other chunks.
    if let Some(stream) = stream {
        query
            .push("SELECT DISTINCT chunk_id AS id FROM extents WHERE stream_id=")
            .push_bind(stream);
    } else {
        query
            .push("SELECT DISTINCT id FROM JSON_TABLE(")
            .push_bind(sqlx::types::Json(chunks.to_vec()))
            .push(", '$[*]' COLUMNS(id BIGINT PATH '$')) ids");
    }
    query.push(") target STRAIGHT_JOIN chunks c FORCE INDEX (PRIMARY) ON c.id=target.id");
    query.push(match phase {
        0 => "",
        1 => " STRAIGHT_JOIN chunk_encodings l FORCE INDEX (encodings_ready) ON l.ready_chunk=c.id",
        _ => " STRAIGHT_JOIN packs p FORCE INDEX (PRIMARY) ON p.id=c.pack_id AND p.state='ready'",
    });
    query.push(match phase {
        0 => " ORDER BY c.id LOCK IN SHARE MODE",
        1 => " ORDER BY l.encoding_id LOCK IN SHARE MODE",
        _ => " ORDER BY p.id LOCK IN SHARE MODE",
    });
    query
}

pub(in super::super) fn unavailable_sources(
    stream: Option<Id>,
    chunks: &[Id],
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new("SELECT EXISTS(SELECT 1 FROM chunks c WHERE ");
    source_scope(&mut query, stream, chunks);
    query.push(" AND (c.state<>'ready' OR (NOT EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=c.id AND l.state='ready' AND l.quarantined_at IS NULL) AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.state='ready' AND p.quarantined_at IS NULL) AND NOT (EXISTS(SELECT 1 FROM pending_uploads u WHERE u.chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=c.id AND l.quarantined_at IS NOT NULL) AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.quarantined_at IS NOT NULL)))))");
    query
}

pub(in super::super) async fn source_snapshot(
    db: &mut sqlx::MySqlConnection,
    scope: &crate::model::source::SourceScope,
    lock: bool,
) -> Result<crate::model::source::SourceSnapshot> {
    use crate::model::source::{SourceSnapshot, manifest_digest};
    let suffix = if lock { " LOCK IN SHARE MODE" } else { "" };
    let (storage_revision, marker): (i64, sqlx::types::Json<Value>) = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT p.revision,s.identity_marker FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=? AND s.enabled{suffix}")))
        .bind(scope.storage_id).fetch_one(&mut *db).await?;
    let suffix = if lock { " FOR UPDATE" } else { "" };
    if scope.source == crate::model::source::SourceType::Raw {
        let raw: RawSource = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=2 AND tl.physical_id=r.location_id) AS tier_material,r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id=? AND r.storage_id=?{suffix}")))
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
    let (state, revision, quarantine_code): (String, i64, Option<String>) = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT state,quarantine_revision,quarantine_code FROM {table} WHERE {column}=? AND storage_id=?{suffix}")))
        .bind(id).bind(scope.storage_id).fetch_optional(&mut *db).await?.context(crate::model::Error::OperationAborted)?;
    let physical = if scope.packed() {
        PhysicalSource::Pack(
            sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=1 AND tl.physical_id=p.location_id) AS tier_material,p.* FROM packs p WHERE id=? AND storage_id=?")
                .bind(id)
                .bind(scope.storage_id)
                .fetch_one(&mut *db)
                .await?,
        )
    } else {
        PhysicalSource::Chunk(sqlx::query_as("SELECT (SELECT tl.material FROM tier_locations tl WHERE tl.unit=0 AND tl.physical_id=l.location_id) AS tier_material,c.id,c.uuid,l.encoding_id AS current_encoding_id,l.location_id,c.storage_id,c.hash,c.raw_size,l.standalone_size,l.encryption_algorithm,l.block_key_id,l.standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE l.encoding_id=? AND l.storage_id=?").bind(id).bind(scope.storage_id).fetch_one(&mut *db).await?)
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
    db: &mut sqlx::MySqlConnection,
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
    let rows: Vec<(i64, Uuid, String, String, String, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT {id},location_id,state,DATE_FORMAT(quarantined_at,'%Y-%m-%dT%H:%i:%s.%fZ'),quarantine_code,quarantine_revision FROM {table} WHERE storage_id=? AND quarantined_at IS NOT NULL AND {id}>? ORDER BY {id} LIMIT ?")))
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
        return Ok(sqlx::query("UPDATE raw_sources SET quarantined_at=CASE WHEN ? IS NULL THEN NULL ELSE UTC_TIMESTAMP(6) END,quarantine_code=?,revision=revision+1 WHERE id=? AND storage_id=? AND revision=? AND state='ready'")
            .bind(code).bind(code).bind(snapshot.scope.identity()?).bind(snapshot.scope.storage_id).bind(snapshot.revision));
    }
    let (table, id) = if snapshot.scope.packed() {
        ("packs", "id")
    } else {
        ("chunk_encodings", "encoding_id")
    };
    Ok(sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {table} SET quarantined_at=CASE WHEN ? IS NULL THEN NULL ELSE UTC_TIMESTAMP(6) END,quarantine_code=?,quarantine_revision=quarantine_revision+1 WHERE {id}=? AND storage_id=? AND quarantine_revision=? AND state='ready'")))
        .bind(code).bind(code).bind(snapshot.scope.identity()?).bind(snapshot.scope.storage_id).bind(snapshot.revision))
}

pub(in super::super) fn source_mode(mode: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT deployment_mode=? FROM mokyu_meta WHERE id=1").bind(mode)
}

pub(in super::super) fn source_result(task: Uuid, result: Value) -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET detail=JSON_MERGE_PATCH(detail,?),processed=processed+1 WHERE id=?",
    )
    .bind(result)
    .bind(task)
}

pub(in super::super) fn quarantined_inputs(chunks: &[Id]) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new("SELECT EXISTS(SELECT 1 FROM chunks c WHERE ");
    source_scope(&mut query, None, chunks);
    query.push(" AND (EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=c.id AND l.state='ready' AND l.quarantined_at IS NOT NULL) OR EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.state='ready' AND p.quarantined_at IS NOT NULL)))");
    query
}

pub(in super::super) fn splice_locks(
    streams: &[Id],
    chunk: Id,
    physical: bool,
) -> sqlx::QueryBuilder<MySql> {
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

pub(in super::super) fn allocated_source(stream: Id, chunk: &Chunk) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT l.encoding_id FROM chunks c JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.chunk_id=c.id WHERE c.state='uploading' AND l.state='uploading' AND l.quarantined_at IS NULL AND l.owner_task IS NULL AND c.id=",
    );
    query
        .push_bind(chunk.id)
        .push(" AND c.owner_stream=")
        .push_bind(stream)
        .push(" AND l.encoding_id=")
        .push_bind(chunk.encoding_id)
        .push(" AND l.location_id=")
        .push_bind(chunk.location_id)
        .push(" FOR UPDATE");
    query
}

pub(in super::super) fn source_input_versions(chunks: &[Id]) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT c.id,l.encoding_id,l.quarantine_revision,p.id,p.quarantine_revision FROM chunks c LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND (l.state='ready' OR (l.state='uploading' AND l.boot_id IS NOT NULL AND l.quarantined_at IS NOT NULL)) LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready' WHERE ",
    );
    source_scope(&mut query, None, chunks);
    query.push(" ORDER BY c.id,l.encoding_id,p.id");
    query
}

pub(in super::super) fn save_source_inputs(task: Uuid, stamp: &Value) -> Statement<'_> {
    sqlx::query(
        "UPDATE tasks SET detail=JSON_SET(detail,'$.source_inputs',JSON_EXTRACT(?,'$')) WHERE id=?",
    )
    .bind(stamp)
    .bind(task)
}

pub(in super::super) fn saved_source_inputs(task: Uuid) -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT JSON_EXTRACT(detail,'$.source_inputs') FROM tasks WHERE id=?")
        .bind(task)
}

// Recovery candidates are private to their writing stream until full verification.
pub(in super::super) fn repair_mode() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT maintenance FROM mokyu_meta LOCK IN SHARE MODE")
}

pub(in super::super) fn repair_stream(
    stream: Id,
) -> Rows<'static, (Uuid, String, Option<Uuid>, Option<Uuid>)> {
    sqlx::query_as("SELECT bucket_id,stream_type,owner_task_id,owner_execution_token FROM streams WHERE id=? AND state='writing' FOR UPDATE").bind(stream)
}

pub(in super::super) fn repair_object(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT o.write_token FROM objects o JOIN streams s ON s.bucket_id=o.bucket_id AND s.object_key=o.object_key WHERE s.id=? FOR UPDATE").bind(stream)
}
