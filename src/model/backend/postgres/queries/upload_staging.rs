use super::{Rows, Scalar, Statement};
use crate::{config::UploadSettings, model::multipart::Staging};
use sqlx::types::Json;
use uuid::Uuid;

pub(in super::super) fn configuration(
    bucket: Uuid,
) -> Rows<'static, (String, Json<UploadSettings>, i64, Uuid)> {
    sqlx::query_as("SELECT m.deployment_mode,r.value,r.revision,b.storage_id FROM buckets b CROSS JOIN mokyu_meta m JOIN runtime_settings r ON r.section='uploads' WHERE b.id=$1").bind(bucket)
}

pub(in super::super) fn valid(storage: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM storages WHERE id=$1 AND enabled AND type IN ('s3','mokyu'))",
    )
    .bind(storage)
}

pub(in super::super) fn insert(upload: Uuid, value: &Staging) -> Statement<'_> {
    sqlx::query("INSERT INTO upload_staging(upload_id,storage_id,policy_revision,tail_upload,tail_wait_seconds) VALUES($1,$2,$3,$4,$5)").bind(upload).bind(value.storage_id).bind(value.policy_revision).bind(&value.tail_upload).bind(value.tail_wait_seconds)
}

pub(in super::super) fn load(upload: Uuid) -> Rows<'static, Staging> {
    sqlx::query_as("SELECT storage_id,policy_revision,tail_upload,tail_wait_seconds FROM upload_staging WHERE upload_id=$1").bind(upload)
}

pub(in super::super) fn add_tail(
    fragment: crate::model::Id,
    upload: Uuid,
    node: Uuid,
    path: &str,
    now: i64,
) -> Statement<'_> {
    sqlx::query("INSERT INTO multipart_tails(fragment_id,upload_id,node_id,object_key,created_at) VALUES($1,$2,$3,$4,$5)").bind(fragment).bind(upload).bind(node).bind(path).bind(now)
}

pub(in super::super) fn due(
    node: Uuid,
    now: i64,
) -> Rows<'static, (crate::model::Id, Uuid, Uuid, Uuid, i32, Vec<u8>)> {
    sqlx::query_as("SELECT t.fragment_id,t.upload_id,j.storage_id,f.id,f.size,f.hash FROM multipart_tails t JOIN upload_staging j ON j.upload_id=t.upload_id JOIN uploads u ON u.id=t.upload_id JOIN fragments f ON f.id=t.fragment_id WHERE t.node_id=$1 AND f.sealed AND f.staging_storage_id IS NULL AND t.source_error IS NULL AND t.retry_at<=$2 AND u.state IN ('active','completing') AND EXISTS(SELECT 1 FROM extents e JOIN parts p ON p.stream_id=e.stream_id WHERE e.fragment_id=t.fragment_id AND p.upload_id=t.upload_id) AND NOT EXISTS(SELECT 1 FROM node_native_work w WHERE w.token=t.token AND w.result_hash IS NULL AND w.expires_at>clock_timestamp()) AND (t.requested OR (j.tail_upload='on_demand_timeout' AND t.created_at+j.tail_wait_seconds*1000000<=$2 AND NOT EXISTS(SELECT 1 FROM parts a JOIN streams s ON s.id=a.write_token JOIN stream_leases l ON l.stream_id=s.id AND l.lease_type='write' WHERE a.upload_id=t.upload_id AND s.state='writing' AND s.stream_type='part' AND l.expires_at>clock_timestamp() AND s.touched_at>clock_timestamp()-interval '2 minutes'))) ORDER BY t.requested DESC,t.created_at,t.fragment_id LIMIT 1 FOR UPDATE OF t SKIP LOCKED").bind(node).bind(now)
}

pub(in super::super) fn claim(
    fragment: crate::model::Id,
    token: Uuid,
    until: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE multipart_tails SET token=$1,retry_at=$2 WHERE fragment_id=$3")
        .bind(token)
        .bind(until)
        .bind(fragment)
}

pub(in super::super) fn owners<'a>(
    upload: Uuid,
    node: Option<Uuid>,
    numbers: &'a [i32],
) -> Rows<'a, (crate::model::Id, Uuid)> {
    sqlx::query_as("SELECT DISTINCT t.fragment_id,t.node_id FROM multipart_tails t JOIN extents e ON e.fragment_id=t.fragment_id JOIN parts p ON p.stream_id=e.stream_id AND p.upload_id=t.upload_id WHERE t.upload_id=$1 AND t.node_id IS DISTINCT FROM $2 AND p.part_number=ANY($3)").bind(upload).bind(node).bind(numbers)
}

pub(in super::super) fn request(id: crate::model::Id) -> Statement<'static> {
    sqlx::query("UPDATE multipart_tails SET requested=TRUE WHERE fragment_id=$1 AND NOT requested")
        .bind(id)
}

pub(in super::super) fn synced(
    storage: Uuid,
    path: &str,
    id: crate::model::Id,
    token: Uuid,
) -> Statement<'_> {
    sqlx::query("UPDATE fragments SET staging_storage_id=$1,staging_object_key=$2 WHERE id=$3 AND sealed AND staging_storage_id IS NULL AND EXISTS(SELECT 1 FROM multipart_tails t JOIN uploads u ON u.id=t.upload_id WHERE t.fragment_id=fragments.id AND t.token=$4 AND u.state IN ('active','completing') AND EXISTS(SELECT 1 FROM extents e JOIN parts p ON p.stream_id=e.stream_id WHERE e.fragment_id=t.fragment_id AND p.upload_id=t.upload_id))").bind(storage).bind(path).bind(id).bind(token)
}

pub(in super::super) fn finish_tail(id: crate::model::Id, token: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM multipart_tails WHERE fragment_id=$1 AND token=$2")
        .bind(id)
        .bind(token)
}

pub(in super::super) fn delay_tail<'a>(
    error: Option<&'a str>,
    next: i64,
    id: crate::model::Id,
    token: Uuid,
) -> Statement<'a> {
    sqlx::query("UPDATE multipart_tails SET token=NULL,source_error=$1,retry_at=$2 WHERE fragment_id=$3 AND token=$4").bind(error).bind(next).bind(id).bind(token)
}

#[cfg(test)]
pub(in super::super) async fn fixture(
    tx: &mut super::storage_cluster::Tx<'_>,
) -> anyhow::Result<(Uuid, Uuid, Uuid, Uuid)> {
    let a = Uuid::now_v7();
    let b = Uuid::now_v7();
    for id in [a, b] {
        sqlx::query("INSERT INTO storages(id,name,type,endpoint_url,remote_bucket_name,region,access_key_encrypted,secret_key_encrypted,identity_marker) VALUES($1,$2,'s3','http://fixture.invalid','stage','region',$3,$4,$5)")
            .bind(id).bind(format!("staging-{id}")).bind(vec![1u8;32]).bind(vec![2u8;32]).bind(Json(serde_json::json!({}))).execute(&mut **tx).await?;
    }
    let bucket =
        super::storage_native::test_bucket(tx, crate::authorization::DEFAULT_PROJECT, a).await?;
    let other =
        super::storage_native::test_bucket(tx, crate::authorization::DEFAULT_PROJECT, b).await?;
    Ok((bucket, other, a, b))
}

#[cfg(test)]
pub(in super::super) fn configure(settings: &UploadSettings) -> Statement<'_> {
    sqlx::query("UPDATE runtime_settings SET value=$1,revision=revision+1 WHERE section='uploads'")
        .bind(Json(settings))
}

#[cfg(test)]
pub(in super::super) fn upload(id: Uuid, bucket: Uuid) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO uploads(id,bucket_id,object_key,access_key) VALUES($1,$2,'fixture','fixture')",
    )
    .bind(id)
    .bind(bucket)
}
