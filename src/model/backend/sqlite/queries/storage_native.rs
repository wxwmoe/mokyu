pub(in super::super) use super::super::native_parts::storage_completion as multipart_completion;
pub(in super::super) use super::super::native_parts::storage_work as multipart_work;
pub(in super::super) use super::super::node_work::storage_plan;
pub(in super::super) use super::super::node_writes::authorize_storage_marker as authorize_marker;
pub(in super::super) use super::super::node_writes::authorize_storage_upload as authorize_upload;
use super::{Rows, Scalar, Statement};
use crate::model::storage_native::Condition;
use crate::model::storage_payload::Intent;
use sqlx::types::Json;
use uuid::Uuid;

pub(in super::super) fn part_carrier(
    upload: Uuid,
    stream: crate::model::Id,
    storage: Uuid,
) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT location_id FROM multipart_carriers WHERE upload_id=? AND stream_id=? AND storage_id=?").bind(upload).bind(stream).bind(storage)
}
pub(in super::super) fn add_part_carrier(
    location: Uuid,
    upload: Uuid,
    stream: crate::model::Id,
    storage: Uuid,
    now: i64,
) -> Statement<'static> {
    sqlx::query("INSERT INTO multipart_carriers(location_id,upload_id,stream_id,storage_id,created_at) VALUES(?,?,?,?,?) ON CONFLICT(upload_id,stream_id) DO NOTHING").bind(location).bind(upload).bind(stream).bind(storage).bind(now)
}
pub(in super::super) fn unused_parts(
    storage: Uuid,
    before: i64,
    limit: i64,
) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT c.location_id FROM multipart_carriers c LEFT JOIN uploads u ON u.id=c.upload_id WHERE c.storage_id=? AND c.created_at<? AND (u.id IS NULL OR u.state NOT IN ('active','completing') OR NOT EXISTS(SELECT 1 FROM parts p WHERE p.upload_id=c.upload_id AND p.stream_id=c.stream_id)) ORDER BY c.created_at,c.location_id LIMIT ?").bind(storage).bind(before).bind(limit)
}
pub(in super::super) fn remove_part_carrier(location: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM multipart_carriers WHERE location_id=?").bind(location)
}

#[cfg(test)]
pub(in super::super) async fn test_execution(
    tx: &mut super::storage_payload::Tx<'_>,
    kind: &str,
) -> anyhow::Result<crate::model::task::Execution> {
    let task = crate::model::task::Execution {
        id: Uuid::now_v7(),
        token: Uuid::now_v7(),
    };
    sqlx::query(
        "INSERT INTO tasks(id,task_type,state,execution_token,detail) VALUES(?,?,'running',?,?)",
    )
    .bind(task.id)
    .bind(kind)
    .bind(task.token)
    .bind(Json(serde_json::json!({"policy":kind})))
    .execute(&mut **tx)
    .await?;
    Ok(task)
}

#[cfg(test)]
pub(in super::super) async fn test_work(
    tx: &mut super::storage_payload::Tx<'_>,
    node: &crate::cluster::NodeState,
    run: crate::model::session::Run,
    plan: &crate::model::node_work::Plan,
) -> anyhow::Result<(Uuid, Uuid)> {
    let task = Uuid::now_v7();
    let token = Uuid::now_v7();
    let operation = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO tasks(id,task_type,state,execution_token) VALUES(?,'integrity','running',?)",
    )
    .bind(task)
    .bind(token)
    .execute(&mut **tx)
    .await?;
    let input = crate::model::node_work::Exchange {
        protected: vec![],
        run,
        operation,
        completion: None,
        class: 0,
        max_items: 1,
        cached: Vec::new(),
    };
    super::work::insert(
        node.node_id,
        &input,
        &[0; 32],
        Some(task),
        token,
        Some(plan),
        None,
    )
    .execute(&mut **tx)
    .await?;
    Ok((operation, task))
}

#[cfg(test)]
pub(in super::super) fn test_pause_task(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='paused' WHERE id=?").bind(task)
}

pub(in super::super) type ObjectRow = (Uuid, String, Option<Uuid>, Option<Uuid>, i64, i64);
pub(in super::super) type WriteRow = (Uuid, Json<Condition>, Option<Uuid>);
pub(in super::super) type ListRow = (String, Json<Intent>, String, i64, i64, i64);

pub(in super::super) fn prefix(namespace: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT prefix FROM storages WHERE id=? AND type='mokyu'").bind(namespace)
}

#[cfg(test)]
pub(in super::super) async fn test_bucket(
    tx: &mut super::storage_payload::Tx<'_>,
    project: Uuid,
    storage: Uuid,
) -> anyhow::Result<Uuid> {
    let id = Uuid::now_v7();
    super::insert_bucket(id, &format!("read-{id}"), project, storage)
        .execute(&mut **tx)
        .await?;
    Ok(id)
}

#[cfg(test)]
pub(in super::super) fn test_raw_profile(storage: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_profiles(storage_id,compression_enabled,pack_enabled,cdc_enabled) VALUES(?,FALSE,FALSE,FALSE)").bind(storage)
}

#[cfg(test)]
pub(in super::super) fn test_expire_protection(operation: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_payloads SET protected_until=0 WHERE operation=?").bind(operation)
}

#[cfg(test)]
pub(in super::super) fn test_expire(read: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE node_reads SET expires_at=0 WHERE read_id=?").bind(read)
}

pub(in super::super) fn window(
    node: &crate::cluster::NodeState,
    run: crate::model::session::Run,
    id: Uuid,
) -> Rows<'static, (Json<crate::model::node_read::Mapping>, i64)> {
    sqlx::query_as("SELECT w.mapping,r.expires_at FROM node_read_windows w JOIN node_reads r ON r.read_id=w.read_id WHERE w.id=? AND r.node_id=? AND r.credential_generation=? AND r.boot_id=? AND r.fence=? AND r.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)")
        .bind(id).bind(node.node_id).bind(node.credential_generation).bind(run.boot).bind(run.fence)
}

#[cfg(test)]
pub(in super::super) fn test_enable(namespace: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storages SET enabled=TRUE WHERE id=?").bind(namespace)
}

pub(in super::super) fn unregistered(namespace: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM storages WHERE id=?)").bind(namespace)
}

pub(in super::super) fn cluster(namespace: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT cluster_id FROM storages WHERE id=? AND type='mokyu'")
        .bind(namespace)
}

pub(in super::super) fn insert(
    namespace: Uuid,
    payload: Uuid,
    hash: Vec<u8>,
    path: &str,
) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_objects(namespace_id,payload_id,path_hash,path) VALUES(?,?,?,?) ON CONFLICT(namespace_id,path_hash) DO NOTHING")
        .bind(namespace).bind(payload).bind(hash).bind(path)
}

pub(in super::super) fn object(namespace: Uuid, hash: Vec<u8>) -> Rows<'static, ObjectRow> {
    sqlx::query_as("SELECT payload_id,path,head,pending,revision,modified FROM storage_objects WHERE namespace_id=? AND path_hash=?")
        .bind(namespace).bind(hash)
}

pub(in super::super) fn identified(namespace: Uuid, payload: Uuid) -> Rows<'static, ObjectRow> {
    sqlx::query_as("SELECT payload_id,path,head,pending,revision,modified FROM storage_objects WHERE namespace_id=? AND payload_id=?")
        .bind(namespace).bind(payload)
}

pub(in super::super) fn binding(operation: Uuid) -> Rows<'static, WriteRow> {
    sqlx::query_as("SELECT owner,condition,previous FROM storage_object_writes WHERE operation=?")
        .bind(operation)
}

pub(in super::super) fn bind(
    operation: Uuid,
    owner: Uuid,
    condition: &Condition,
    previous: Option<Uuid>,
) -> Statement<'_> {
    sqlx::query(
        "INSERT INTO storage_object_writes(operation,owner,condition,previous) VALUES(?,?,?,?)",
    )
    .bind(operation)
    .bind(owner)
    .bind(Json(condition))
    .bind(previous)
}

pub(in super::super) fn reserve(
    namespace: Uuid,
    payload: Uuid,
    revision: i64,
    operation: Uuid,
) -> Statement<'static> {
    sqlx::query(
        "UPDATE storage_objects SET pending=?,revision=? WHERE namespace_id=? AND payload_id=?",
    )
    .bind(operation)
    .bind(revision)
    .bind(namespace)
    .bind(payload)
}

pub(in super::super) fn publish(
    namespace: Uuid,
    payload: Uuid,
    operation: Uuid,
    modified: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE storage_objects SET head=?,pending=NULL,modified=? WHERE namespace_id=? AND payload_id=? AND pending=?")
        .bind(operation).bind(modified).bind(namespace).bind(payload).bind(operation)
}

pub(in super::super) fn clear(namespace: Uuid, payload: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE storage_objects SET head=NULL,pending=NULL WHERE namespace_id=? AND payload_id=?",
    )
    .bind(namespace)
    .bind(payload)
}

pub(in super::super) fn page(namespace: Uuid, after: String) -> Rows<'static, ListRow> {
    sqlx::query_as("SELECT o.path,p.intent,p.state,p.protected_until,p.write_until,o.modified FROM storage_objects o JOIN storage_payloads p ON p.operation=o.head WHERE o.head IS NOT NULL AND o.namespace_id=? AND o.path>? ORDER BY o.path LIMIT 256")
        .bind(namespace).bind(after)
}

pub(in super::super) fn raw_carrier(id: crate::model::Id) -> Rows<'static, (Uuid, Uuid)> {
    sqlx::query_as("SELECT r.storage_id,r.location_id FROM raw_sources r JOIN storages s ON s.id=r.storage_id WHERE r.id=? AND s.type='mokyu'").bind(id)
}

pub(in super::super) fn native_stream(stream: crate::model::Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents e JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=? AND (EXISTS(SELECT 1 FROM chunk_encodings l JOIN storages s ON s.id=l.storage_id WHERE l.chunk_id=c.id AND l.state='ready' AND s.type='mokyu') OR EXISTS(SELECT 1 FROM packs p JOIN storages s ON s.id=p.storage_id WHERE p.id=c.pack_id AND p.state='ready' AND s.type='mokyu'))) OR EXISTS(SELECT 1 FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id JOIN storages s ON s.id=r.storage_id WHERE e.stream_id=? AND s.type='mokyu')").bind(stream).bind(stream)
}

pub(in super::super) fn protection_policy(
    operation: Uuid,
) -> Rows<
    'static,
    (
        Json<crate::model::storage_cluster::Policy>,
        Json<crate::model::storage_cluster::Manifest>,
    ),
> {
    sqlx::query_as("SELECT c.policy,c.manifest FROM storage_clusters c JOIN storage_payloads p ON p.cluster_id=c.id WHERE p.operation=?").bind(operation)
}

pub(in super::super) fn confirmed_copies(
    operation: Uuid,
    all_copies: bool,
    data_shards: u16,
    parity: bool,
) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM storage_fragments f WHERE f.operation=? AND (? OR f.copy_index=0) AND (? OR f.shard_index<?) AND f.root IS NOT NULL AND NOT f.deleted AND NOT EXISTS(SELECT 1 FROM storage_repairs r WHERE r.id=f.id AND r.state='writing')")
        .bind(operation).bind(all_copies).bind(parity).bind(i32::from(data_shards))
}
