use super::super::{Backend, transactions};
pub(in super::super) use super::node_requests::{Connection, begin};
use crate::{
    app::{Extent, StoredStream},
    cluster::NodeState,
    content::codec::Key,
    model::{Error, node_read as read, node_refresh, session::Run},
};
use anyhow::{Result, ensure};
use std::collections::BTreeSet;
use uuid::Uuid;
pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, sqlx::Postgres>;
pub(in super::super) use super::{
    node_read_allowed, node_read_count, node_read_existing, node_read_owned, node_read_permit,
    node_read_permit_count, node_read_stream, node_read_window, node_read_window_count,
    read_capacity, read_pool,
};
pub(in super::super) async fn current_stream(
    tx: &mut Connection,
    bucket: Uuid,
    key: &str,
) -> Result<Option<StoredStream>> {
    transactions::locked_current_stream(tx, bucket, key).await
}
pub(in super::super) async fn validate_run(
    tx: &mut Connection,
    node: &NodeState,
    run: Run,
) -> Result<()> {
    transactions::require_node_run(tx, node, run, false).await?;
    Ok(())
}
pub(in super::super) async fn access(
    _b: &Backend,
    tx: &mut Connection,
    node: &NodeState,
    key: &Key,
    run: Run,
) -> Result<()> {
    transactions::authenticate_rpc_node(tx, node, key, false).await?;
    ensure!(
        matches!(node.role.as_str(), "data" | "worker" | "node"),
        Error::NodeAuthentication
    );
    validate_run(tx, node, run).await
}
pub(in super::super) async fn lock_node(tx: &mut Connection, id: Uuid) -> Result<()> {
    super::lock_staging(format!("mokyu:node-read:{id}"))
        .execute(tx)
        .await?;
    Ok(())
}
pub(in super::super) async fn lock_storage(
    tx: &mut Connection,
    id: Uuid,
    direction: &str,
) -> Result<()> {
    super::lock_storage_permits(format!("mokyu:store:{id}:{direction}"))
        .execute(tx)
        .await?;
    Ok(())
}
pub(in super::super) async fn allocate(
    db: &mut Connection,
    read: Uuid,
    node: Uuid,
    storage: Uuid,
    limit: i64,
    want: i64,
) -> Result<Vec<Uuid>> {
    Ok(
        super::allocate_read_permits(read, node, storage, limit, want)
            .fetch_all(db)
            .await?,
    )
}
pub(in super::super) async fn own(db: &mut Connection, read: Uuid, tokens: &[Uuid]) -> Result<()> {
    super::own_read_permits(read, tokens).execute(db).await?;
    Ok(())
}
pub(in super::super) async fn insert_window(
    db: &mut Connection,
    read: Uuid,
    mapping: &read::Mapping,
) -> Result<Uuid> {
    Ok(super::node_read_insert_window(read, mapping)
        .fetch_one(db)
        .await?)
}
pub(in super::super) async fn pin(
    db: &mut Connection,
    window: Uuid,
    pins: &BTreeSet<(&str, Uuid)>,
) -> Result<()> {
    let types = pins
        .iter()
        .map(|(t, _)| (*t).to_string())
        .collect::<Vec<_>>();
    let ids = pins.iter().map(|(_, id)| *id).collect::<Vec<_>>();
    super::node_read_pin(window, &types, &ids)
        .execute(db)
        .await?;
    Ok(())
}
pub(in super::super) async fn create(
    tx: &mut Tx<'_>,
    state: &NodeState,
    input: &read::Query,
    stream: &StoredStream,
    start: i64,
    end: i64,
    hash: &[u8],
) -> Result<Uuid> {
    let id = transactions::create_read_lease(tx, stream.id, read::READ_SECONDS).await?;
    super::node_read_insert(state, input, id, stream, start, end, hash)
        .execute(&mut **tx)
        .await?;
    Ok(id)
}
pub(in super::super) async fn release_held(
    db: &mut Connection,
    node: &NodeState,
    run: Run,
    ids: &[Uuid],
) -> Result<()> {
    if !ids.is_empty() {
        super::node_read_release_permits(node, run, &[], ids)
            .execute(db)
            .await?;
    }
    Ok(())
}
pub(in super::super) async fn replica_extents(
    b: &Backend,
    owned: &super::NodeReadLease,
    stream: &StoredStream,
    w: &read::Window,
) -> Option<Vec<Extent>> {
    if owned.replica_allowed
        && stream.payload_type == "chunks"
        && let Some(replica) = &b.replica
    {
        replica.node_extents(&w.cursor, w.max_items).await
    } else {
        None
    }
}
pub(in super::super) async fn refresh(
    b: &Backend,
    node: &NodeState,
    key: &Key,
    input: node_refresh::Request,
) -> Result<node_refresh::Reply> {
    super::super::refresh::request(&b.pool, b.refresh.clone(), node, key, input).await
}
pub(in super::super) fn refresh_status(b: &Backend) -> serde_json::Value {
    b.refresh.snapshot()
}
pub(in super::super) async fn report(
    b: &Backend,
    node: &NodeState,
    key: &Key,
    input: read::Report,
) -> Result<Vec<Uuid>> {
    transactions::report_node_read(b, node, key, input).await
}
