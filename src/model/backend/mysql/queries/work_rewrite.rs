use super::*;
pub(in super::super) fn key(id: Uuid) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT name,key_algorithm FROM managed_keys WHERE id=? AND purpose='block' AND NOT recovery_read_only AND revoked_at IS NULL").bind(id)
}
pub(in super::super) fn chunk_nonce<'a>(
    id: Id,
    task: Uuid,
    nonce: Option<&'a [u8]>,
) -> Statement<'a> {
    sqlx::query("UPDATE chunk_encodings SET nonce=?,nonce_id=? WHERE encoding_id=? AND owner_task=? AND state='uploading'").bind(nonce).bind(crate::content::nonce::id(nonce)).bind(id).bind(task)
}
