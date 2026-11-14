pub(in super::super) use super::super::transactions::authorize_storage_marker as authorize_marker;
pub(in super::super) use super::super::transactions::authorize_storage_upload as authorize_upload;
pub(in super::super) use super::super::transactions::work::storage_plan;
use super::{Rows, Scalar, Statement};
use crate::model::storage_native::Condition;
use crate::model::storage_payload::Intent;
use sqlx::types::Json;
use uuid::Uuid;

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
        "INSERT INTO tasks(id,task_type,state,lease_token,detail) VALUES($1,$2,'running',$3,$4)",
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
    let maintenance = Uuid::now_v7();
    let operation = Uuid::now_v7();
    sqlx::query("INSERT INTO tasks(id,task_type,state,lease_node_id,lease_token,lease_expires_at) VALUES($1,'integrity','running',$2,$3,clock_timestamp()+interval '60 seconds')")
        .bind(task).bind(node.node_id).bind(token).execute(&mut **tx).await?;
    for permit in [token, maintenance] {
        sqlx::query("INSERT INTO storage_permits(token,storage_id,direction,holder_id,expires_at) VALUES($1,$2,'maintenance',$3,clock_timestamp()+interval '60 seconds')")
            .bind(permit).bind(plan.storage().unwrap()).bind(node.node_id).execute(&mut **tx).await?;
    }
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
        node,
        &input,
        &[0; 32],
        Some(task),
        token,
        maintenance,
        Some(plan),
        Some(b"scope-fixture"),
    )
    .execute(&mut **tx)
    .await?;
    Ok((operation, task))
}

#[cfg(test)]
pub(in super::super) fn test_pause_task(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='paused' WHERE id=$1").bind(task)
}

pub(in super::super) type ObjectRow = (Uuid, String, Option<Uuid>, Option<Uuid>, i64, i64);
pub(in super::super) type WriteRow = (Uuid, Json<Condition>, Option<Uuid>);
pub(in super::super) type ListRow = (String, Json<Intent>, String, i64, i64, i64);

pub(in super::super) fn prefix(namespace: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT prefix FROM storages WHERE id=$1 AND type='mokyu'").bind(namespace)
}

#[cfg(test)]
pub(in super::super) async fn test_bucket(
    tx: &mut super::storage_payload::Tx<'_>,
    project: Uuid,
    storage: Uuid,
) -> anyhow::Result<Uuid> {
    Ok(
        super::insert_bucket(&format!("read-{}", Uuid::now_v7()), project, storage)
            .fetch_one(&mut **tx)
            .await?
            .id,
    )
}

#[cfg(test)]
pub(in super::super) fn test_raw_profile(storage: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_profiles(storage_id,compression_enabled,pack_enabled,cdc_enabled) VALUES($1,FALSE,FALSE,FALSE)").bind(storage)
}

#[cfg(test)]
pub(in super::super) fn test_expire_protection(operation: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_payloads SET protected_until=0 WHERE operation=$1").bind(operation)
}

#[cfg(test)]
pub(in super::super) fn test_expire(read: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE stream_leases SET expires_at='2000-01-01' WHERE id=$1").bind(read)
}

pub(in super::super) fn window(
    node: &crate::cluster::NodeState,
    run: crate::model::session::Run,
    id: Uuid,
) -> Rows<'static, (Json<crate::model::node_read::Mapping>, i64)> {
    sqlx::query_as("SELECT w.mapping,(extract(epoch FROM l.expires_at)*1000000)::bigint FROM node_read_windows w JOIN node_reads r ON r.read_id=w.read_id JOIN stream_leases l ON l.id=r.read_id WHERE w.id=$1 AND r.node_id=$2 AND r.credential_generation=$3 AND r.boot_id=$4 AND r.fence=$5 AND l.expires_at>clock_timestamp() FOR SHARE OF w,r,l")
        .bind(id).bind(node.node_id).bind(node.credential_generation).bind(run.boot).bind(run.fence)
}

#[cfg(test)]
pub(in super::super) fn test_enable(namespace: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storages SET enabled=TRUE WHERE id=$1").bind(namespace)
}

pub(in super::super) fn unregistered(namespace: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM storages WHERE id=$1)").bind(namespace)
}

pub(in super::super) fn cluster(namespace: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT cluster_id FROM storages WHERE id=$1 AND type='mokyu'")
        .bind(namespace)
}

pub(in super::super) fn insert(
    namespace: Uuid,
    payload: Uuid,
    hash: Vec<u8>,
    path: &str,
) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_objects(namespace_id,payload_id,path_hash,path) VALUES($1,$2,$3,$4) ON CONFLICT(namespace_id,path_hash) DO NOTHING")
        .bind(namespace).bind(payload).bind(hash).bind(path)
}

pub(in super::super) fn object(namespace: Uuid, hash: Vec<u8>) -> Rows<'static, ObjectRow> {
    sqlx::query_as("SELECT payload_id,path,head,pending,revision,modified FROM storage_objects WHERE namespace_id=$1 AND path_hash=$2 FOR UPDATE")
        .bind(namespace).bind(hash)
}

pub(in super::super) fn identified(namespace: Uuid, payload: Uuid) -> Rows<'static, ObjectRow> {
    sqlx::query_as("SELECT payload_id,path,head,pending,revision,modified FROM storage_objects WHERE namespace_id=$1 AND payload_id=$2 FOR UPDATE")
        .bind(namespace).bind(payload)
}

pub(in super::super) fn binding(operation: Uuid) -> Rows<'static, WriteRow> {
    sqlx::query_as("SELECT owner,condition,previous FROM storage_object_writes WHERE operation=$1")
        .bind(operation)
}

pub(in super::super) fn bind(
    operation: Uuid,
    owner: Uuid,
    condition: &Condition,
    previous: Option<Uuid>,
) -> Statement<'_> {
    sqlx::query(
        "INSERT INTO storage_object_writes(operation,owner,condition,previous) VALUES($1,$2,$3,$4)",
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
        "UPDATE storage_objects SET pending=$1,revision=$2 WHERE namespace_id=$3 AND payload_id=$4",
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
    sqlx::query("UPDATE storage_objects SET head=$1,pending=NULL,modified=$2 WHERE namespace_id=$3 AND payload_id=$4 AND pending=$5")
        .bind(operation).bind(modified).bind(namespace).bind(payload).bind(operation)
}

pub(in super::super) fn clear(namespace: Uuid, payload: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE storage_objects SET head=NULL,pending=NULL WHERE namespace_id=$1 AND payload_id=$2",
    )
    .bind(namespace)
    .bind(payload)
}

pub(in super::super) fn page(namespace: Uuid, after: String) -> Rows<'static, ListRow> {
    sqlx::query_as("SELECT o.path,p.intent,p.state,p.protected_until,p.write_until,o.modified FROM storage_objects o JOIN storage_payloads p ON p.operation=o.head WHERE o.head IS NOT NULL AND o.namespace_id=$1 AND o.path COLLATE \"C\">$2 COLLATE \"C\" ORDER BY o.path COLLATE \"C\" LIMIT 256")
        .bind(namespace).bind(after)
}

pub(in super::super) fn raw_carrier(id: crate::model::Id) -> Rows<'static, (Uuid, Uuid)> {
    sqlx::query_as("SELECT r.storage_id,r.id FROM raw_sources r JOIN storages s ON s.id=r.storage_id WHERE r.id=$1 AND s.type='mokyu'").bind(id)
}

pub(in super::super) fn native_stream(stream: crate::model::Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents e JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=$1 AND (EXISTS(SELECT 1 FROM chunk_encodings l JOIN storages s ON s.id=l.storage_id WHERE l.chunk_id=c.id AND l.state='ready' AND s.type='mokyu') OR EXISTS(SELECT 1 FROM packs p JOIN storages s ON s.id=p.storage_id WHERE p.id=c.pack_id AND p.state='ready' AND s.type='mokyu'))) OR EXISTS(SELECT 1 FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id JOIN storages s ON s.id=r.storage_id WHERE e.stream_id=$1 AND s.type='mokyu')").bind(stream)
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
    sqlx::query_as("SELECT c.policy,c.manifest FROM storage_clusters c JOIN storage_payloads p ON p.cluster_id=c.id WHERE p.operation=$1 FOR SHARE OF c").bind(operation)
}

pub(in super::super) fn confirmed_copies(
    operation: Uuid,
    all_copies: bool,
    data_shards: u16,
    parity: bool,
) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM storage_fragments f WHERE f.operation=$1 AND ($2 OR f.copy_index=0) AND ($4 OR f.shard_index<$3) AND f.root IS NOT NULL AND NOT f.deleted AND NOT EXISTS(SELECT 1 FROM storage_repairs r WHERE r.id=f.id AND r.state='writing')")
        .bind(operation).bind(all_copies).bind(i32::from(data_shards)).bind(parity)
}
