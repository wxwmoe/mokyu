use super::super::Backend;
pub(in super::super) use super::super::node_registry::validate_run;
pub(in super::super) use super::node_requests::{Connection, begin};
use super::{Rows, Scalar};
use crate::{
    app::{Extent, StoredStream},
    cluster::NodeState,
    content::codec::Key,
    model::{Error, Id, node_read as read, session::Run},
};
use anyhow::{Result, ensure};
use sqlx::types::Json;
use std::collections::BTreeSet;
use uuid::Uuid;
pub(in super::super) type Tx<'a> = super::node_registry::Tx<'a>;
pub(in super::super) use super::super::read_reports::{refresh, refresh_status, report};
pub(in super::super) fn epoch() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT control_epoch FROM mokyu_meta WHERE id=1")
}
pub(in super::super) async fn access(
    b: &Backend,
    tx: &mut Tx<'_>,
    node: &NodeState,
    key: &Key,
    run: Run,
) -> Result<()> {
    ensure!(
        matches!(node.role.as_str(), "data" | "node" | "worker"),
        Error::NodeAuthentication
    );
    super::super::node_registry::require_work(b, tx, node, key, run).await?;
    Ok(())
}
pub(in super::super) async fn lock_node(_tx: &mut Connection, _node: Uuid) -> Result<()> {
    // Session authentication holds the node row through commit.
    Ok(())
}
pub(in super::super) async fn lock_storage(
    db: &mut Connection,
    storage: Uuid,
    direction: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO storage_gates(storage_id,direction) VALUES(?,?) ON CONFLICT DO NOTHING",
    )
    .bind(storage)
    .bind(direction)
    .execute(&mut *db)
    .await?;
    Ok(())
}
pub(in super::super) fn node_read_allowed(
    access: &str,
    project: Uuid,
    bucket: Uuid,
) -> Scalar<'static, bool> {
    super::node_requests::node_request_allowed(access, project, bucket, "object.read")
}
pub(in super::super) async fn current_stream(
    tx: &mut Connection,
    bucket: Uuid,
    key: &str,
) -> Result<Option<StoredStream>> {
    let id: Option<Option<Id>> =
        sqlx::query_scalar("SELECT stream_id FROM objects WHERE bucket_id=? AND object_key=?")
            .bind(bucket)
            .bind(key.as_bytes())
            .fetch_optional(&mut *tx)
            .await?;
    let Some(id) = id.flatten() else {
        return Ok(None);
    };
    Ok(
        sqlx::query_as("SELECT * FROM streams WHERE id=? AND state='ready'")
            .bind(id)
            .fetch_optional(tx)
            .await?,
    )
}
#[derive(sqlx::FromRow)]
pub(in super::super) struct Lease {
    pub read_id: Uuid,
    pub stream_id: Id,
    pub payload_revision: i64,
    pub offset_bytes: i64,
    pub end_bytes: i64,
    pub request_hash: Vec<u8>,
    pub valid_for_ms: i64,
}
pub(in super::super) fn node_read_existing(
    node: &NodeState,
    run: Run,
    operation: Uuid,
) -> Rows<'static, Lease> {
    sqlx::query_as("SELECT r.*,MAX(0,(r.expires_at-CAST(unixepoch('subsec')*1000000 AS INTEGER))/1000) AS valid_for_ms FROM node_reads r WHERE r.node_id=? AND r.credential_generation=? AND r.boot_id=? AND r.fence=? AND operation_id=?").bind(node.node_id).bind(node.credential_generation).bind(run.boot).bind(run.fence).bind(operation)
}
pub(in super::super) fn node_read_owned(
    node: &NodeState,
    run: Run,
    id: Uuid,
) -> Rows<'static, Lease> {
    sqlx::query_as("SELECT r.*,MAX(0,(r.expires_at-CAST(unixepoch('subsec')*1000000 AS INTEGER))/1000) AS valid_for_ms FROM node_reads r WHERE r.node_id=? AND r.credential_generation=? AND r.boot_id=? AND r.fence=? AND read_id=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(node.node_id).bind(node.credential_generation).bind(run.boot).bind(run.fence).bind(id)
}
pub(in super::super) fn node_read_count(node: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT COUNT(*) FROM node_reads WHERE node_id=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(node)
}
pub(in super::super) async fn create(
    tx: &mut Tx<'_>,
    node: &NodeState,
    input: &read::Query,
    s: &StoredStream,
    start: i64,
    end: i64,
    hash: &[u8],
) -> Result<Uuid> {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO node_reads(read_id,node_id,boot_id,fence,credential_generation,operation_id,request_hash,stream_id,payload_revision,offset_bytes,end_bytes,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,CAST(unixepoch('subsec')*1000000 AS INTEGER)+90000000)")
        .bind(id).bind(node.node_id).bind(input.run.boot).bind(input.run.fence).bind(node.credential_generation).bind(input.operation).bind(hash).bind(s.id).bind(s.payload_revision).bind(start).bind(end).execute(&mut **tx).await?;
    Ok(id)
}
pub(in super::super) fn node_read_stream(id: Id) -> Rows<'static, StoredStream> {
    sqlx::query_as("SELECT * FROM streams WHERE id=? AND state IN ('ready','retired')").bind(id)
}
pub(in super::super) fn node_read_window(
    id: Uuid,
    offset: i64,
) -> Rows<'static, (Uuid, Json<read::Mapping>)> {
    sqlx::query_as("SELECT id,mapping FROM node_read_windows WHERE read_id=? AND offset_bytes=?")
        .bind(id)
        .bind(offset)
}
pub(in super::super) fn node_read_window_count(id: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_read_windows WHERE read_id=?").bind(id)
}
pub(in super::super) async fn insert_window(
    db: &mut Connection,
    id: Uuid,
    m: &read::Mapping,
) -> Result<Uuid> {
    let window = Uuid::new_v4();
    sqlx::query("INSERT INTO node_read_windows(id,read_id,offset_bytes,mapping) VALUES(?,?,?,?)")
        .bind(window)
        .bind(id)
        .bind(m.start)
        .bind(Json(m))
        .execute(db)
        .await?;
    Ok(window)
}
pub(in super::super) async fn pin(
    db: &mut Connection,
    window: Uuid,
    pins: &BTreeSet<(&str, Uuid)>,
) -> Result<()> {
    if pins.is_empty() {
        return Ok(());
    }
    let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
        "INSERT INTO node_read_sources(window_id,source_type,location_id) ",
    );
    query
        .push_values(pins, |mut row, (scope, id)| {
            row.push_bind(window).push_bind(*scope).push_bind(*id);
        })
        .build()
        .execute(db)
        .await?;
    Ok(())
}
fn hex_ids(ids: &[Uuid]) -> Json<Vec<String>> {
    Json(ids.iter().map(|id| id.simple().to_string()).collect())
}
pub(in super::super) fn node_read_permit(id: Uuid, storage: Uuid) -> Rows<'static, (Uuid, i64)> {
    sqlx::query_as("SELECT p.token,MAX(0,(p.expires_at-CAST(unixepoch('subsec')*1000000 AS INTEGER))/1000) FROM storage_permits p JOIN node_read_permits n ON n.token=p.token WHERE n.read_id=? AND p.storage_id=? AND p.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER) ORDER BY p.token").bind(id).bind(storage)
}
pub(in super::super) fn node_read_permit_count(id: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_read_permits n JOIN storage_permits p ON p.token=n.token WHERE n.read_id=? AND p.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(id)
}
pub(in super::super) fn read_capacity(
    node: &NodeState,
    run: Run,
    storage: Uuid,
) -> Rows<'static, (i64, bool)> {
    sqlx::query_as("SELECT (SELECT count(*) FROM storage_permits WHERE storage_id=? AND direction='read' AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)),EXISTS(SELECT 1 FROM node_read_permits n JOIN node_reads r ON r.read_id=n.read_id JOIN storage_permits s ON s.token=n.token WHERE s.storage_id=? AND s.direction='read' AND s.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER) AND r.node_id=? AND r.boot_id=? AND r.fence=? AND r.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))")
        .bind(storage).bind(storage).bind(node.node_id).bind(run.boot).bind(run.fence)
}
pub(in super::super) fn read_pool(
    read: Uuid,
    storage: Uuid,
    limit: i64,
    held: &[Uuid],
) -> Rows<'static, (Uuid, i64)> {
    sqlx::query_as("SELECT p.token,MAX(0,(p.expires_at-CAST(unixepoch('subsec')*1000000 AS INTEGER))/1000) FROM node_reads c JOIN storage_permits p ON p.holder=c.node_id WHERE c.read_id=? AND p.storage_id=? AND p.direction='read' AND p.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER) AND p.token IN (SELECT unhex(value) FROM json_each(?)) AND EXISTS(SELECT 1 FROM node_read_permits n JOIN node_reads r ON r.read_id=n.read_id WHERE n.token=p.token AND r.node_id=c.node_id AND r.credential_generation=c.credential_generation AND r.boot_id=c.boot_id AND r.fence=c.fence AND r.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)) ORDER BY p.token LIMIT ?").bind(read).bind(storage).bind(hex_ids(held)).bind(limit)
}
pub(in super::super) async fn allocate(
    db: &mut Connection,
    read: Uuid,
    node: Uuid,
    storage: Uuid,
    limit: i64,
    wanted: i64,
) -> Result<Vec<Uuid>> {
    let count:i64=sqlx::query_scalar("SELECT count(*) FROM storage_permits WHERE storage_id=? AND direction='read' AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(storage).fetch_one(&mut *db).await?;
    let tokens: Vec<_> = (0..wanted.min((limit - count).max(0)))
        .map(|_| Uuid::new_v4())
        .collect();
    if tokens.is_empty() {
        return Ok(tokens);
    }
    let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
        "INSERT INTO storage_permits(token,storage_id,direction,holder,expires_at) ",
    );
    query
        .push_values(&tokens, |mut row, token| {
            row.push_bind(*token)
                .push_bind(storage)
                .push_bind("read")
                .push_bind(node)
                .push("CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000");
        })
        .build()
        .execute(&mut *db)
        .await?;
    own(db, read, &tokens).await?;
    Ok(tokens)
}
pub(in super::super) async fn own(db: &mut Connection, read: Uuid, tokens: &[Uuid]) -> Result<()> {
    if tokens.is_empty() {
        return Ok(());
    }
    let mut query =
        sqlx::QueryBuilder::<sqlx::Sqlite>::new("INSERT INTO node_read_permits(read_id,token) ");
    query
        .push_values(tokens, |mut row, token| {
            row.push_bind(read).push_bind(*token);
        })
        .push(" ON CONFLICT DO NOTHING")
        .build()
        .execute(db)
        .await?;
    Ok(())
}
pub(in super::super) async fn release_held(
    db: &mut Connection,
    node: &NodeState,
    run: Run,
    tokens: &[Uuid],
) -> Result<()> {
    if tokens.is_empty() {
        return Ok(());
    }
    sqlx::query("DELETE FROM storage_permits WHERE token IN (SELECT unhex(value) FROM json_each(?)) AND EXISTS(SELECT 1 FROM node_read_permits n JOIN node_reads r ON r.read_id=n.read_id WHERE n.token=storage_permits.token AND r.node_id=? AND r.boot_id=? AND r.fence=? AND r.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(hex_ids(tokens)).bind(node.node_id).bind(run.boot).bind(run.fence).execute(db).await?;
    Ok(())
}
pub(in super::super) async fn replica_extents(
    _b: &Backend,
    _owned: &Lease,
    _stream: &StoredStream,
    _w: &read::Window,
) -> Option<Vec<Extent>> {
    None
}

pub(in super::super) fn report_window(
    read: Uuid,
    window: Uuid,
) -> Scalar<'static, Json<read::Mapping>> {
    sqlx::query_scalar("SELECT mapping FROM node_read_windows WHERE read_id=? AND id=?")
        .bind(read)
        .bind(window)
}
pub(in super::super) fn verification_count() -> Scalar<'static, i64> {
    sqlx::query_scalar(
        "SELECT count(*) FROM tasks WHERE state IN ('queued','running','paused') AND task_type='integrity' AND json_extract(detail,'$.operation')='verify'",
    )
}
pub(in super::super) fn reported(task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?)").bind(task)
}

#[async_trait::async_trait]
impl crate::model::cluster::StoragePermits for Backend {
    async fn acquire_storage_permit(
        &self,
        storage: Uuid,
        direction: &str,
        limit: i32,
        holder: Uuid,
    ) -> Result<Option<Uuid>> {
        let mut tx = begin(self).await?;
        lock_storage(&mut tx, storage, direction).await?;
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM storage_permits WHERE storage_id=? AND direction=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(storage).bind(direction).fetch_one(&mut *tx).await?;
        if count >= i64::from(limit) {
            return Ok(None);
        }
        let token = Uuid::new_v4();
        sqlx::query("INSERT INTO storage_permits(token,storage_id,direction,holder,expires_at) VALUES(?,?,?,?,CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000)").bind(token).bind(storage).bind(direction).bind(holder).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some(token))
    }
    async fn renew_storage_permit(&self, id: Uuid) -> Result<bool> {
        let mut tx = begin(self).await?;
        let held=sqlx::query("UPDATE storage_permits SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000 WHERE token=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(id).execute(&mut *tx).await?.rows_affected()>0;
        tx.commit().await?;
        Ok(held)
    }
    async fn release_storage_permit(&self, id: Uuid) -> Result<()> {
        let mut tx = begin(self).await?;
        sqlx::query("DELETE FROM storage_permits WHERE token=?")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}
pub(in super::super) async fn touch_read(
    tx: &mut Tx<'_>,
    node: Uuid,
    run: Run,
    grant: read::GrantRef,
    renew: bool,
) -> Result<Option<u64>> {
    use read::GrantType;
    let held: bool = match (grant.r#type, renew) {
        (GrantType::Read, true) => sqlx::query("UPDATE node_reads SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+90000000 WHERE read_id=? AND node_id=? AND boot_id=? AND fence=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(grant.id).bind(node).bind(run.boot).bind(run.fence).execute(&mut **tx).await?.rows_affected()>0,
        (GrantType::Read, false) => {
            sqlx::query("DELETE FROM storage_permits WHERE EXISTS(SELECT 1 FROM node_read_permits n JOIN node_reads r ON r.read_id=n.read_id WHERE n.token=storage_permits.token AND n.read_id=? AND r.node_id=? AND r.boot_id=? AND r.fence=?) AND NOT EXISTS(SELECT 1 FROM node_read_permits o WHERE o.token=storage_permits.token AND o.read_id<>?)").bind(grant.id).bind(node).bind(run.boot).bind(run.fence).bind(grant.id).execute(&mut **tx).await?;
            sqlx::query("DELETE FROM node_reads WHERE read_id=? AND node_id=? AND boot_id=? AND fence=?").bind(grant.id).bind(node).bind(run.boot).bind(run.fence).execute(&mut **tx).await?;true
        }
        (GrantType::Window,true) => {
            let left:Option<i64> = sqlx::query_scalar("SELECT (r.expires_at-CAST(unixepoch('subsec')*1000000 AS INTEGER))/1000 FROM node_read_windows w JOIN node_reads r ON r.read_id=w.read_id WHERE w.id=? AND r.node_id=? AND r.boot_id=? AND r.fence=? AND r.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)")
                .bind(grant.id).bind(node).bind(run.boot).bind(run.fence).fetch_optional(&mut **tx).await?;
            return Ok(left.and_then(|ms| u64::try_from(ms).ok()).filter(|ms| *ms>0));
        },
        (GrantType::Window, false) => {
            sqlx::query("DELETE FROM node_read_windows WHERE id=? AND EXISTS(SELECT 1 FROM node_reads r WHERE r.read_id=node_read_windows.read_id AND r.node_id=? AND r.boot_id=? AND r.fence=?)").bind(grant.id).bind(node).bind(run.boot).bind(run.fence).execute(&mut **tx).await?;true
        }
        (GrantType::StorageRead, true) => sqlx::query("UPDATE storage_permits SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000 WHERE token=? AND direction='read' AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER) AND EXISTS(SELECT 1 FROM node_read_permits n JOIN node_reads r ON r.read_id=n.read_id WHERE n.token=storage_permits.token AND r.node_id=? AND r.boot_id=? AND r.fence=? AND r.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(grant.id).bind(node).bind(run.boot).bind(run.fence).execute(&mut **tx).await?.rows_affected()>0,
        (GrantType::StorageRead, false) => {
            sqlx::query("DELETE FROM storage_permits WHERE token=? AND direction='read' AND EXISTS(SELECT 1 FROM node_read_permits n JOIN node_reads r ON r.read_id=n.read_id WHERE n.token=storage_permits.token AND r.node_id=? AND r.boot_id=? AND r.fence=?)").bind(grant.id).bind(node).bind(run.boot).bind(run.fence).execute(&mut **tx).await?;true
        }
        _=>return Ok(None)
    };
    Ok(held.then_some(if !renew {
        0
    } else if grant.r#type == GrantType::StorageRead {
        60_000
    } else {
        90_000
    }))
}
pub(in super::super) async fn expire(tx: &mut Tx<'_>) -> Result<()> {
    sqlx::query("DELETE FROM storage_permits WHERE expires_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER) AND token IN (SELECT token FROM storage_permits WHERE expires_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER) LIMIT 512)").execute(&mut **tx).await?;
    sqlx::query("DELETE FROM node_reads WHERE expires_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER) AND read_id IN (SELECT read_id FROM node_reads WHERE expires_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER) LIMIT 512)").execute(&mut **tx).await?;
    Ok(())
}
