use super::*;

pub(in super::super) fn reserve_quota(
    stream: Id,
    size: i64,
    padding: bool,
    upload: Option<Uuid>,
    part: Option<i32>,
) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT quota_reserve($1,$2,$3,$4,$5)")
        .bind(stream)
        .bind(size)
        .bind(padding)
        .bind(upload)
        .bind(part)
}

pub(in super::super) fn lock_completing_upload(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT bucket_id FROM uploads WHERE id=$1 AND state='completing' FOR SHARE")
        .bind(id)
}

pub(in super::super) fn lock_stream(id: Id) -> Statement<'static> {
    sqlx::query("SELECT id FROM streams WHERE id=$1 FOR SHARE").bind(id)
}

pub(in super::super) fn lock_quota(bucket: Uuid) -> Statement<'static> {
    sqlx::query("SELECT quota_lock($1)").bind(bucket)
}

pub(in super::super) fn complete_quota(upload: Uuid, stream: Id, size: i64) -> Statement<'static> {
    sqlx::query(
        "UPDATE quota_reservations SET result_stream_id=$2 WHERE id=$1 AND logical_bytes>=$3",
    )
    .bind(upload)
    .bind(stream)
    .bind(size)
}

pub(in super::super) fn write_authorization(id: Id) -> Scalar<'static, sqlx::types::Json<Permit>> {
    sqlx::query_scalar("SELECT write_authorization FROM streams WHERE id=$1").bind(id)
}

pub(in super::super) fn lock_writing_stream(
    id: Id,
) -> Rows<'static, (Uuid, String, String, i64, String, bool, Option<Uuid>)> {
    sqlx::query_as(
        "SELECT bucket_id,object_key,state,size,payload_type,COALESCE((metadata->'user') ? 'mokyu-replica-origin',false),publication_id FROM streams WHERE id=$1 FOR UPDATE",
    )
    .bind(id)
}

pub(in super::super) fn quota_membership(
    user: Option<Uuid>,
    project: Uuid,
) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT scope FROM project_members WHERE user_id=$1 AND project_id=$2")
        .bind(user)
        .bind(project)
}

pub(in super::super) fn quota_scope<'q>(
    scope: &'q str,
    id: Uuid,
    visible: bool,
) -> Rows<'q, Quota> {
    sqlx::query_as("SELECT scope,scope_id,CASE WHEN $3 THEN used_bytes::text END used_bytes,CASE WHEN $3 THEN reserved_bytes::text END reserved_bytes,CASE WHEN $3 THEN inflight_bytes::text END inflight_bytes,CASE WHEN $3 THEN object_count::text END object_count,CASE WHEN $3 THEN bucket_count::text END bucket_count,logical_bytes_limit::text logical_bytes_limit,inflight_bytes_limit::text inflight_bytes_limit,bucket_count_limit::text bucket_count_limit FROM quota_scopes WHERE scope=$1 AND scope_id=$2").bind(scope).bind(id).bind(visible)
}

pub(in super::super) fn update_quota<'q>(
    scope: &'q str,
    id: Uuid,
    bytes: Option<i64>,
    inflight: Option<i64>,
    buckets: Option<i64>,
) -> Statement<'q> {
    sqlx::query("UPDATE quota_scopes SET logical_bytes_limit=$3,inflight_bytes_limit=$4,bucket_count_limit=$5 WHERE scope=$1 AND scope_id=$2").bind(scope).bind(id).bind(bytes).bind(inflight).bind(buckets)
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for Quota {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            scope: row.try_get("scope")?,
            scope_id: row.try_get("scope_id")?,
            used_bytes: row.try_get("used_bytes")?,
            reserved_bytes: row.try_get("reserved_bytes")?,
            inflight_bytes: row.try_get("inflight_bytes")?,
            object_count: row.try_get("object_count")?,
            bucket_count: row.try_get("bucket_count")?,
            logical_bytes_limit: row.try_get("logical_bytes_limit")?,
            inflight_bytes_limit: row.try_get("inflight_bytes_limit")?,
            bucket_count_limit: row.try_get("bucket_count_limit")?,
        })
    }
}
