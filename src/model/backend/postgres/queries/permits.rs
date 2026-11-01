use super::*;

pub(in super::super) fn expire_storage_permits<'q>(
    storage: Uuid,
    direction: &'q str,
) -> Statement<'q> {
    sqlx::query(
        "DELETE FROM storage_permits WHERE storage_id=$1 AND direction=$2 AND expires_at<=now()",
    )
    .bind(storage)
    .bind(direction)
}

pub(in super::super) fn storage_permit_count<'q>(
    storage: Uuid,
    direction: &'q str,
) -> Scalar<'q, i64> {
    sqlx::query_scalar("SELECT count(*) FROM storage_permits WHERE storage_id=$1 AND direction=$2")
        .bind(storage)
        .bind(direction)
}

pub(in super::super) fn insert_storage_permit<'q>(
    token: Uuid,
    storage: Uuid,
    direction: &'q str,
    holder: Uuid,
) -> Statement<'q> {
    sqlx::query("INSERT INTO storage_permits(token,storage_id,direction,holder_id,expires_at) VALUES($1,$2,$3,$4,now()+interval '1 minute')")
        .bind(token)
        .bind(storage)
        .bind(direction)
        .bind(holder)
}

pub(in super::super) fn renew_storage_permit(token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_permits SET expires_at=now()+interval '1 minute' WHERE token=$1")
        .bind(token)
}

pub(in super::super) fn release_storage_permit(token: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM storage_permits WHERE token=$1").bind(token)
}

pub(in super::super) fn storage_profile(
    id: Uuid,
) -> Rows<'static, crate::storage::profile::StorageProfile> {
    sqlx::query_as("SELECT storage_id,revision,cdc_enabled,deduplication_enabled,write_processing,CASE WHEN (SELECT deployment_mode FROM mokyu_meta WHERE singleton)='single' THEN write_durability END AS write_durability,write_flush_interval,write_backlog_max_age,background_scheduling,public_cache_validation,cors_cache_enabled,public_cache_refresh_interval,public_cache_max_age,read_source_validation,source_validation_max_age,read_integrity,file_delivery,file_delivery_prefix,connect_timeout,request_timeout,max_retries,retry_timeout,priority_aging,min_storage_duration,unreferenced_grace,deleted_chunk_retention,upload_retention,multipart_idle_timeout,min_free_space,cache_enabled,cache_soft_limit,cache_entries,upload_cache,upload_cache_size,upload_cache_overflow,compression_enabled,compression_strategy,compression_level,compression_sample_level,compression_min_savings_percent,compression_min_savings_bytes,pack_enabled,pack_max_size,pack_upload_flush_timeout,pack_interval,pack_reuse_interval,pack_reclaim_interval,pack_repack_interval,pack_repack_cooldown,pack_reclaim_min_savings_bytes,pack_range_enabled,pack_range_interval,pack_range_window,pack_range_min_downloads,pack_range_min_savings_percent,pack_range_min_savings_bytes,pack_range_repack_after,pack_range_repack_window,pack_range_repack_retry_interval,pack_concurrency,integrity_concurrency,integrity_rps,integrity_bandwidth,integrity_batch_timeout,read_concurrency,upload_concurrency,control_concurrency,pack_byte_budget,integrity_byte_budget,maintenance_concurrency,maintenance_byte_budget,migration_concurrency,migration_byte_budget FROM storage_profiles WHERE storage_id=$1")
        .bind(id)
}

pub(in super::super) fn insert_task<'q>(
    id: Uuid,
    task_type: &'q str,
    bucket: Option<Uuid>,
    detail: Value,
    audit: Option<Id>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO tasks(id,task_type,bucket_id,state,detail,created_by_label,source,execution_scope,target_node_id) VALUES($1,$2,$3,'queued',$4,coalesce((SELECT actor_label FROM audit_events WHERE id=$5),'Scheduler'),coalesce((SELECT source FROM audit_events WHERE id=$5),'system'),CASE WHEN $4->>'source_node' IS NULL THEN 'cluster' ELSE 'node' END,($4->>'source_node')::uuid)")
        .bind(id)
        .bind(task_type)
        .bind(bucket)
        .bind(detail)
        .bind(audit)
}

pub(in super::super) fn notify_task(id: String) -> Statement<'static> {
    sqlx::query("SELECT pg_notify('mokyu_tasks',$1) FROM mokyu_meta WHERE singleton AND deployment_mode='cluster'").bind(id)
}

pub(in super::super) fn notify_work(id: Uuid) -> Statement<'static> {
    notify_task(format!("task:{id}"))
}

pub(in super::super) fn finish_stream<'q>(
    id: Id,
    size: i64,
    etag: &'q str,
    checksums: Value,
) -> Statement<'q> {
    sqlx::query("UPDATE streams SET size=$2,etag=$3,checksums=$4,touched_at=now() WHERE id=$1 AND state='writing'")
        .bind(id)
        .bind(size)
        .bind(etag)
        .bind(checksums)
}
