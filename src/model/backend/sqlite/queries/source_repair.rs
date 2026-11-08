use super::*;

pub(in super::super) fn source_locks(
    stream: Option<Id>,
    chunks: &[Id],
    phase: u8,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(match phase {
        0 => "SELECT c.id FROM chunks c WHERE ",
        1 => {
            "SELECT l.encoding_id FROM chunk_encodings l WHERE l.state='ready' AND l.chunk_id IN (SELECT c.id FROM chunks c WHERE "
        }
        2 => {
            "SELECT p.id FROM packs p WHERE p.state='ready' AND p.id IN (SELECT c.pack_id FROM chunks c WHERE "
        }
        _ => unreachable!(),
    });
    source_scope(&mut query, stream, chunks);
    query.push(match phase {
        0 => " ORDER BY c.id",
        1 => ") ORDER BY l.encoding_id",
        _ => ") ORDER BY p.id",
    });
    query
}

pub(in super::super) fn unavailable_sources(
    stream: Option<Id>,
    chunks: &[Id],
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new("SELECT EXISTS(SELECT 1 FROM chunks c WHERE ");
    source_scope(&mut query, stream, chunks);
    query.push(" AND (c.state<>'ready' OR (NOT EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=c.id AND l.state='ready' AND l.quarantined_at IS NULL) AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.state='ready' AND p.quarantined_at IS NULL) AND NOT (EXISTS(SELECT 1 FROM pending_uploads u WHERE u.chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=c.id AND l.quarantined_at IS NOT NULL) AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.quarantined_at IS NOT NULL)))))");
    query
}

pub(in super::super) async fn source_snapshot(
    db: &mut sqlx::SqliteConnection,
    scope: &crate::model::source::SourceScope,
    lock: bool,
) -> Result<crate::model::source::SourceSnapshot> {
    use crate::model::source::{SourceSnapshot, manifest_digest};
    let suffix = if lock { "" } else { "" };
    let (storage_revision, marker): (i64, sqlx::types::Json<Value>) = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT p.revision,s.identity_marker FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=? AND s.enabled{suffix}")))
        .bind(scope.storage_id).fetch_one(&mut *db).await?;
    let suffix = if lock { "" } else { "" };
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
        let members = super::pack_work::physical_members(db, pack).await?;
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
    db: &mut sqlx::SqliteConnection,
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
    let rows: Vec<(i64, Uuid, String, String, String, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT {id},location_id,state,strftime('%Y-%m-%dT%H:%M:%fZ',quarantined_at/1000000.0,'unixepoch'),quarantine_code,quarantine_revision FROM {table} WHERE storage_id=? AND quarantined_at IS NOT NULL AND {id}>? ORDER BY {id} LIMIT ?")))
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
        return Ok(sqlx::query("UPDATE raw_sources SET quarantined_at=CASE WHEN ? IS NULL THEN NULL ELSE CAST(unixepoch('subsec')*1000000 AS INTEGER) END,quarantine_code=?,revision=revision+1 WHERE id=? AND storage_id=? AND revision=? AND state='ready'")
            .bind(code).bind(code).bind(snapshot.scope.identity()?).bind(snapshot.scope.storage_id).bind(snapshot.revision));
    }
    let (table, id) = if snapshot.scope.packed() {
        ("packs", "id")
    } else {
        ("chunk_encodings", "encoding_id")
    };
    Ok(sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {table} SET quarantined_at=CASE WHEN ? IS NULL THEN NULL ELSE CAST(unixepoch('subsec')*1000000 AS INTEGER) END,quarantine_code=?,quarantine_revision=quarantine_revision+1 WHERE {id}=? AND storage_id=? AND quarantine_revision=? AND state='ready'")))
        .bind(code).bind(code).bind(snapshot.scope.identity()?).bind(snapshot.scope.storage_id).bind(snapshot.revision))
}

pub(in super::super) fn source_mode(mode: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT deployment_mode=? FROM mokyu_meta WHERE id=1").bind(mode)
}

pub(in super::super) fn source_result(task: Uuid, result: Value) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET detail=json_patch(detail,?),processed=processed+1 WHERE id=?")
        .bind(result)
        .bind(task)
}

pub(in super::super) fn quarantined_inputs(chunks: &[Id]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new("SELECT EXISTS(SELECT 1 FROM chunks c WHERE ");
    source_scope(&mut query, None, chunks);
    query.push(" AND (EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=c.id AND l.state='ready' AND l.quarantined_at IS NOT NULL) OR EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.state='ready' AND p.quarantined_at IS NOT NULL)))");
    query
}

pub(in super::super) fn splice_locks(
    streams: &[Id],
    chunk: Id,
    physical: bool,
) -> sqlx::QueryBuilder<Sqlite> {
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
        ")) ORDER BY c.id"
    } else {
        ") ORDER BY id"
    });
    query
}

pub(in super::super) fn allocated_source(stream: Id, chunk: &Chunk) -> sqlx::QueryBuilder<Sqlite> {
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
        .push("");
    query
}

pub(in super::super) fn source_input_versions(chunks: &[Id]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT c.id,l.encoding_id,l.quarantine_revision,p.id,p.quarantine_revision FROM chunks c LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND (l.state='ready' OR (l.state='uploading' AND l.boot_id IS NOT NULL AND l.quarantined_at IS NOT NULL)) LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready' WHERE ",
    );
    source_scope(&mut query, None, chunks);
    query.push(" ORDER BY c.id,l.encoding_id,p.id");
    query
}

pub(in super::super) fn save_source_inputs(task: Uuid, stamp: &Value) -> Statement<'_> {
    sqlx::query("UPDATE tasks SET detail=json_set(detail,'$.source_inputs',json(?)) WHERE id=?")
        .bind(stamp)
        .bind(task)
}

pub(in super::super) fn saved_source_inputs(task: Uuid) -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT json_extract(detail,'$.source_inputs') FROM tasks WHERE id=?")
        .bind(task)
}

// Recovery candidates are private to their writing stream until full verification.
pub(in super::super) fn repair_mode() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT maintenance FROM mokyu_meta")
}

pub(in super::super) fn repair_stream(
    stream: Id,
) -> Rows<'static, (Uuid, String, Option<Uuid>, Option<Uuid>)> {
    sqlx::query_as("SELECT bucket_id,stream_type,owner_task_id,owner_execution_token FROM streams WHERE id=? AND state='writing'").bind(stream)
}

pub(in super::super) fn repair_object(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT o.write_token FROM objects o JOIN streams s ON s.bucket_id=o.bucket_id AND s.object_key=o.object_key WHERE s.id=?").bind(stream)
}

pub(in super::super) fn repair_part(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT write_token FROM parts WHERE write_token=?").bind(stream)
}

pub(in super::super) fn repair_target(
    storage: Uuid,
    key: Option<Uuid>,
    algorithm: &str,
) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=? AND s.enabled AND s.block_key_id IS ? AND ((s.block_key_id IS NULL AND ?='none') OR EXISTS(SELECT 1 FROM managed_keys k WHERE k.id=s.block_key_id AND k.key_algorithm=? AND k.revoked_at IS NULL))").bind(storage).bind(key).bind(algorithm).bind(algorithm)
}

pub(in super::super) fn repair_pending(chunk: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=?)").bind(chunk)
}

pub(in super::super) fn insert_repair(chunk: &Chunk, stream: Id) -> Statement<'_> {
    sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,state,created_at,unreferenced_at,owner_stream,recovery_pending,encryption_algorithm,block_key_id,standalone_compressed,encoding_fingerprint) VALUES(?,?,?,'uploading',?,CAST(unixepoch('subsec')*1000000 AS INTEGER),?,TRUE,?,?,FALSE,'{}')").bind(chunk.id).bind(chunk.location_id).bind(chunk.storage_id).bind(chunk.created_at.timestamp_micros()).bind(stream).bind(&chunk.algorithm).bind(chunk.block_key_id)
}

pub(in super::super) fn repair_candidate(encoding: Id, stream: Id) -> Rows<'static, Chunk> {
    sqlx::query_as("SELECT c.id,c.uuid,l.encoding_id AS current_encoding_id,l.location_id,c.storage_id,c.hash,c.raw_size,l.standalone_size,l.encryption_algorithm,l.block_key_id,l.standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE l.encoding_id=? AND l.owner_stream=? AND l.owner_task IS NULL AND l.state='uploading' AND l.recovery_pending AND l.quarantined_at IS NULL").bind(encoding).bind(stream)
}

pub(in super::super) fn save_repair_metadata(chunk: &Chunk, stream: Id) -> Statement<'_> {
    sqlx::query("UPDATE chunk_encodings SET standalone_size=?,standalone_compressed=?,nonce=?,nonce_id=? WHERE encoding_id=? AND owner_stream=? AND state='uploading'").bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(crate::content::nonce::id(chunk.nonce.as_deref())).bind(chunk.encoding_id).bind(stream)
}

pub(in super::super) fn retire_repair_sources(chunk: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',sync_pending=FALSE,boot_id=NULL,unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE chunk_id=? AND (state='ready' OR (state='uploading' AND boot_id IS NOT NULL)) AND quarantined_at IS NOT NULL").bind(chunk)
}

pub(in super::super) fn publish_repair(encoding: Id, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET sync_pending=CASE WHEN boot_id IS NOT NULL THEN FALSE ELSE sync_pending END,boot_id=NULL,state='ready',owner_stream=NULL,recovery_pending=FALSE,unreferenced_at=NULL,stored_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE encoding_id=? AND owner_stream=? AND state='uploading' AND quarantined_at IS NULL").bind(encoding).bind(stream)
}

pub(in super::super) fn discard_verified_repair(encoding: Id, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',owner_stream=NULL,recovery_pending=FALSE,unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),stored_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE encoding_id=? AND owner_stream=? AND state='uploading'").bind(encoding).bind(stream)
}

pub(in super::super) fn publish_repaired_chunk(chunk: &Chunk) -> Statement<'_> {
    sqlx::query("UPDATE chunks SET current_encoding_id=?,pack_id=NULL,standalone_size=?,standalone_compressed=?,nonce=?,encryption_algorithm=?,block_key_id=?,unreferenced_at=NULL WHERE id=? AND state='ready'").bind(chunk.encoding_id).bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(&chunk.algorithm).bind(chunk.block_key_id).bind(chunk.id)
}

pub(in super::super) fn retire_stream_encodings(stream: Id, batch: i64) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET owner_stream=NULL,state='retired',unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE encoding_id IN (SELECT encoding_id FROM chunk_encodings WHERE owner_stream=? AND state='uploading' ORDER BY encoding_id LIMIT ?)").bind(stream).bind(batch)
}

pub(in super::super) fn repair_owner(
    stream: Id,
) -> Rows<'static, (Uuid, String, Option<Uuid>, Option<Uuid>)> {
    sqlx::query_as("SELECT bucket_id,stream_type,owner_task_id,owner_execution_token FROM streams WHERE id=? AND state='writing'").bind(stream)
}

pub(in super::super) fn repair_upload(stream: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM uploads WHERE id=(SELECT r.upload_id FROM quota_writes w JOIN quota_reservations r ON r.id=w.reservation_id WHERE w.stream_id=?) AND state='active'").bind(stream)
}
