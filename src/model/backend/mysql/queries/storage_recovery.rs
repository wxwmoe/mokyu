use super::super::Backend;
use super::{Rows, Statement};
use crate::model::{
    storage_cluster::Manifest,
    storage_recovery::{CatalogEntry, Cursor},
};
use anyhow::Result;
use sqlx::types::Json;
use uuid::Uuid;

pub(in super::super) type FileRow = (
    Json<crate::model::storage_recovery::Part>,
    i64,
    i64,
    bool,
    Option<Json<crate::model::storage_recovery::DecodedFile>>,
);

pub(in super::super) fn root(
    id: Uuid,
) -> Rows<'static, (Option<Json<crate::model::storage_recovery::Proof>>, i64)> {
    sqlx::query_as("SELECT publication,indexed_revision FROM storage_recovery_catalogs WHERE id=?")
        .bind(id)
}

pub(in super::super) fn bind_head(
    id: Uuid,
    proof: &crate::model::storage_recovery::Proof,
) -> Statement<'_> {
    sqlx::query("UPDATE storage_recovery_catalogs SET publication=? WHERE id=?")
        .bind(Json(proof))
        .bind(id)
}

pub(in super::super) fn proofs(
    id: Uuid,
    after: u64,
    through: u64,
) -> Rows<'static, (Vec<u8>, Json<CatalogEntry>)> {
    sqlx::query_as("SELECT content_hash,content FROM storage_recovery_entries WHERE catalog=? AND category=1 AND position>? AND position<=? ORDER BY position,id LIMIT 32").bind(id).bind(after as i64).bind(through as i64)
}

pub(in super::super) fn file(id: Uuid, operation: Uuid) -> Rows<'static, FileRow> {
    sqlx::query_as("SELECT part,through_sequence,publication_revision,retired,decoded FROM storage_recovery_files WHERE catalog=? AND operation=?").bind(id).bind(operation)
}

pub(in super::super) fn file_path<'a>(
    id: Uuid,
    hash: &'a [u8],
    path: &'a str,
) -> Rows<'a, FileRow> {
    sqlx::query_as("SELECT part,through_sequence,publication_revision,retired,decoded FROM storage_recovery_files WHERE catalog=? AND path_hash=? AND path=? ORDER BY publication_revision DESC LIMIT 1").bind(id).bind(hash).bind(path)
}

pub(in super::super) fn insert_file(
    id: Uuid,
    part: &crate::model::storage_recovery::Part,
    hash: Vec<u8>,
    revision: u64,
    through: u64,
) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_recovery_files(catalog,operation,path_hash,path,publication_revision,through_sequence,part) VALUES(?,?,?,?,?,?,?)").bind(id).bind(part.intent.operation).bind(hash).bind(&part.path).bind(revision as i64).bind(through as i64).bind(Json(part))
}

pub(in super::super) fn reference_file(
    id: Uuid,
    operation: Uuid,
    revision: u64,
) -> Statement<'static> {
    sqlx::query(
        "UPDATE storage_recovery_files SET publication_revision=? WHERE catalog=? AND operation=?",
    )
    .bind(revision as i64)
    .bind(id)
    .bind(operation)
}

pub(in super::super) fn retire_file(id: Uuid, operation: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_recovery_files SET retired=TRUE WHERE catalog=? AND operation=?")
        .bind(id)
        .bind(operation)
}

pub(in super::super) fn indexed(id: Uuid, revision: u64) -> Statement<'static> {
    sqlx::query("UPDATE storage_recovery_catalogs SET indexed_revision=? WHERE id=?")
        .bind(revision as i64)
        .bind(id)
}

pub(in super::super) fn decoded(
    id: Uuid,
    operation: Uuid,
    summary: &crate::model::storage_recovery::DecodedFile,
) -> Statement<'_> {
    sqlx::query("UPDATE storage_recovery_files SET decoded=? WHERE catalog=? AND operation=?")
        .bind(Json(summary))
        .bind(id)
        .bind(operation)
}

pub(in super::super) fn files(id: Uuid, after: Uuid) -> Rows<'static, FileRow> {
    sqlx::query_as("SELECT f.part,f.through_sequence,f.publication_revision,f.retired,f.decoded FROM storage_recovery_files f WHERE f.catalog=? AND f.operation>? AND NOT f.retired AND NOT EXISTS(SELECT 1 FROM storage_recovery_files n WHERE n.catalog=f.catalog AND n.path_hash=f.path_hash AND n.path=f.path AND n.publication_revision>f.publication_revision) ORDER BY f.operation LIMIT 128").bind(id).bind(after)
}

pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, sqlx::MySql>;
pub(in super::super) type CursorRow = (i64, Json<Cursor>, Vec<u8>);

pub(in super::super) async fn begin(backend: &Backend) -> Result<Tx<'_>> {
    super::node_registry::begin(backend).await
}

pub(in super::super) fn create(id: Uuid, namespace: Uuid, anchor: &Manifest) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_recovery_catalogs(id,namespace_id,anchor) VALUES(?,?,?) ON DUPLICATE KEY UPDATE id=id")
        .bind(id).bind(namespace).bind(Json(anchor))
}

pub(in super::super) fn catalog(id: Uuid) -> Rows<'static, (Uuid, Json<Manifest>)> {
    sqlx::query_as(
        "SELECT namespace_id,anchor FROM storage_recovery_catalogs WHERE id=? FOR UPDATE",
    )
    .bind(id)
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
    let mut query = sqlx::QueryBuilder::<sqlx::MySql>::new(
        "SELECT category,id,position,content_hash FROM storage_recovery_entries WHERE catalog=",
    );
    query.push_bind(id).push(" AND (category,id,position) IN (");
    for (index, (category, key, position)) in keys.iter().enumerate() {
        if index != 0 {
            query.push(",");
        }
        query
            .push("(")
            .push_bind(*category)
            .push(",")
            .push_bind(*key)
            .push(",")
            .push_bind(*position as i64)
            .push(")");
    }
    query.push(") LOCK IN SHARE MODE");
    Ok(query.build_query_as().fetch_all(&mut **tx).await?)
}

pub(in super::super) async fn insert(
    tx: &mut Tx<'_>,
    id: Uuid,
    entries: &[(&CatalogEntry, [u8; 32], usize)],
) -> Result<()> {
    let mut query = sqlx::QueryBuilder::<sqlx::MySql>::new(
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
pub(in super::super) fn covered(operation: Uuid, data: u16) -> Rows<'static, (i64,)> {
    sqlx::query_as("SELECT COUNT(*) FROM (SELECT sequence FROM storage_fragments WHERE operation=? AND root IS NOT NULL AND NOT deleted GROUP BY sequence HAVING COUNT(DISTINCT shard_index)>=?) covered").bind(operation).bind(i32::from(data))
}
