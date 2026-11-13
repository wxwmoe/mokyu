use super::super::Backend;
use super::{Rows, Statement};
use crate::model::{
    storage_cluster::Manifest,
    storage_recovery::{CatalogEntry, Cursor},
};
use anyhow::Result;
use sqlx::types::Json;
use uuid::Uuid;

pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, sqlx::Sqlite>;
pub(in super::super) type CursorRow = (i64, Json<Cursor>, Vec<u8>);

pub(in super::super) async fn begin(backend: &Backend) -> Result<Tx<'_>> {
    super::node_registry::begin(backend).await
}

pub(in super::super) fn create(id: Uuid, namespace: Uuid, anchor: &Manifest) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_recovery_catalogs(id,namespace_id,anchor) VALUES(?,?,?) ON CONFLICT DO NOTHING")
        .bind(id).bind(namespace).bind(Json(anchor))
}

pub(in super::super) fn catalog(id: Uuid) -> Rows<'static, (Uuid, Json<Manifest>)> {
    sqlx::query_as("SELECT namespace_id,anchor FROM storage_recovery_catalogs WHERE id=?").bind(id)
}

pub(in super::super) fn cursor(id: Uuid, media: Uuid, scope: &str) -> Rows<'_, CursorRow> {
    sqlx::query_as("SELECT revision,cursor_value,batch_hash FROM storage_recovery_cursors WHERE catalog=? AND media=? AND scope=?")
        .bind(id).bind(media).bind(scope)
}

pub(in super::super) fn insert_cursor(id: Uuid, value: &Cursor, hash: Vec<u8>) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_recovery_cursors(catalog,media,scope,revision,cursor_value,batch_hash) VALUES(?,?,?,1,?,?)")
        .bind(id).bind(value.media).bind(&value.scope).bind(Json(value)).bind(hash)
}

pub(in super::super) fn update_cursor(
    id: Uuid,
    value: &Cursor,
    hash: Vec<u8>,
    revision: i64,
) -> Statement<'_> {
    sqlx::query("UPDATE storage_recovery_cursors SET revision=?,cursor_value=?,batch_hash=? WHERE catalog=? AND media=? AND scope=? AND revision=?")
        .bind(revision+1).bind(Json(value)).bind(hash).bind(id).bind(value.media).bind(&value.scope).bind(revision)
}

pub(in super::super) fn cursors(id: Uuid, media: Uuid, scope: String) -> Rows<'static, CursorRow> {
    sqlx::query_as("SELECT revision,cursor_value,batch_hash FROM storage_recovery_cursors WHERE catalog=? AND (media>? OR (media=? AND scope>?)) ORDER BY media,scope LIMIT 128")
        .bind(id).bind(media).bind(media).bind(scope)
}

pub(in super::super) fn entry(
    id: Uuid,
    category: i16,
    key: Uuid,
    position: u64,
) -> Rows<'static, (Vec<u8>, Json<CatalogEntry>)> {
    sqlx::query_as("SELECT content_hash,content FROM storage_recovery_entries WHERE catalog=? AND category=? AND id=? AND position=?")
        .bind(id).bind(category).bind(key).bind(position as i64)
}

pub(in super::super) async fn hashes(
    tx: &mut Tx<'_>,
    id: Uuid,
    keys: &[(i16, Uuid, u64)],
) -> Result<Vec<(i16, Uuid, i64, Vec<u8>)>> {
    let mut query =
        sqlx::QueryBuilder::<sqlx::Sqlite>::new("WITH wanted(category,id,position) AS (");
    query.push_values(keys, |mut row, (category, key, position)| {
        row.push_bind(*category)
            .push_bind(*key)
            .push_bind(*position as i64);
    });
    // Probe supplied keys instead of scanning a growing catalog.
    query.push(") SELECT e.category,e.id,e.position,e.content_hash FROM wanted CROSS JOIN storage_recovery_entries e WHERE e.catalog=")
        .push_bind(id)
        .push(" AND e.category=wanted.category AND e.id=wanted.id AND e.position=wanted.position");
    Ok(query.build_query_as().fetch_all(&mut **tx).await?)
}

pub(in super::super) async fn insert(
    tx: &mut Tx<'_>,
    id: Uuid,
    entries: &[(&CatalogEntry, [u8; 32], usize)],
) -> Result<()> {
    let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
        "INSERT INTO storage_recovery_entries(catalog,category,owner_id,id,position,content_hash,encoded_bytes,content) ",
    );
    query.push_values(entries, |mut row, (value, hash, bytes)| {
        let (category, owner, key, position) = value.identity();
        row.push_bind(id)
            .push_bind(category)
            .push_bind(owner)
            .push_bind(key)
            .push_bind(position as i64)
            .push_bind(hash.as_slice())
            .push_bind(*bytes as i32)
            .push_bind(Json(*value));
    });
    query.build().execute(&mut **tx).await?;
    Ok(())
}

pub(in super::super) fn page(
    id: Uuid,
    category: i16,
    owner: Option<Uuid>,
    after: (Uuid, i64),
) -> Rows<'static, (Uuid, i64, i32)> {
    sqlx::query_as("SELECT id,position,encoded_bytes FROM storage_recovery_entries WHERE catalog=? AND category=? AND (owner_id=? OR ? IS NULL) AND (id>? OR (id=? AND position>?)) ORDER BY id,position LIMIT 128")
        .bind(id).bind(category).bind(owner).bind(owner).bind(after.0).bind(after.0).bind(after.1)
}
