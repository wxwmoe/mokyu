use super::super::Backend;
pub(in super::super) use super::node_registry::begin;
pub(in super::super) use super::storage_profile;
use super::{Rows, Scalar};
use crate::{
    app::Bucket,
    cluster::NodeState,
    content::codec::Key,
    model::{
        Error, ReadRoute,
        multipart::{Upload, UploadPart, UploadSummary},
        node_request,
        session::Run,
    },
};
use anyhow::{Result, ensure};
use uuid::Uuid;
pub(in super::super) type Connection = sqlx::MySqlConnection;
pub(in super::super) async fn admit(
    b: &Backend,
    tx: &mut super::node_registry::Tx<'_>,
    node: &NodeState,
    key: &Key,
    run: Run,
) -> Result<()> {
    ensure!(
        matches!(node.role.as_str(), "data" | "node"),
        Error::NodeAuthentication
    );
    let ready = super::super::node_registry::require_work(b, tx, node, key, run).await?;
    ensure!(ready && sqlx::query_scalar::<_,bool>("SELECT NOT maintenance AND deployment_mode='single_control' FROM mokyu_meta WHERE id=1").fetch_one(&mut **tx).await?,Error::NodeInactive);
    Ok(())
}
pub(in super::super) fn node_read_credential(access: &str) -> Rows<'_, (i64, Vec<u8>)> {
    sqlx::query_as("SELECT authorization_revision,secret_encrypted FROM s3_credentials WHERE access_key_id=? AND enabled AND (expires_at IS NULL OR expires_at>CURRENT_TIMESTAMP(6)) LOCK IN SHARE MODE").bind(access)
}
pub(in super::super) fn node_read_bucket(route: &ReadRoute) -> Rows<'_, Bucket> {
    match route.base() {
        ReadRoute::Image {..}=>unreachable!(),
        ReadRoute::Host{host,..}=>sqlx::query_as("SELECT b.* FROM buckets b JOIN bucket_domains d ON b.id=d.bucket_id WHERE d.host=? LOCK IN SHARE MODE").bind(host),
        ReadRoute::Bucket{name,alias,..}=>{
            let alias=alias.as_ref().map(|(name,_)|name);
            sqlx::query_as("SELECT * FROM buckets WHERE name IN (?,?) ORDER BY CASE WHEN name=? THEN 0 ELSE 1 END LIMIT 1 LOCK IN SHARE MODE").bind(name).bind(alias).bind(alias)
        }
    }
}
pub(in super::super) fn node_request_allowed(
    access: &str,
    project: Uuid,
    bucket: Uuid,
    action: &str,
) -> Scalar<'static, bool> {
    let mask = match action {
        "object.read" => 2,
        "bucket.list" => 1,
        _ => 0,
    };
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM s3_credential_grants g JOIN s3_credentials c ON c.access_key_id=g.access_key WHERE g.access_key=? AND c.project_id=? AND g.bucket_id=? AND c.enabled AND (c.expires_at IS NULL OR c.expires_at>CURRENT_TIMESTAMP(6)) AND (g.actions & ?)<>0)").bind(access.to_owned()).bind(project).bind(bucket).bind(mask)
}
pub(in super::super) fn node_object_acl(bucket: Uuid, key: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT s.public_read FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=? AND o.object_key=? AND s.state='ready'").bind(bucket).bind(key.as_bytes())
}
pub(in super::super) fn node_catalog_buckets<'a>(
    access: &'a str,
    prefix: &'a str,
    after: &'a str,
    region: Option<&'a str>,
    expected: &'a str,
    limit: i64,
) -> Rows<'a, node_request::Bucket> {
    sqlx::query_as("SELECT b.name,b.created_at AS created_at FROM buckets b JOIN s3_credential_grants g ON g.bucket_id=b.id JOIN s3_credentials c ON c.access_key_id=g.access_key WHERE g.access_key=? AND c.enabled AND (c.expires_at IS NULL OR c.expires_at>CURRENT_TIMESTAMP(6)) AND c.project_id=b.project_id AND (g.actions&1)<>0 AND LEFT(b.name,OCTET_LENGTH(?))=? AND b.name>? AND (? IS NULL OR ?=?) ORDER BY b.name LIMIT ?").bind(access).bind(prefix).bind(prefix).bind(after).bind(region).bind(region).bind(expected).bind(limit)
}
pub(in super::super) fn node_catalog_objects<'a>(
    bucket: Uuid,
    prefix: &'a str,
    key: &'a str,
    inclusive: bool,
) -> Rows<'a, node_request::Object> {
    sqlx::query_as("SELECT CAST(o.object_key AS CHAR CHARACTER SET utf8mb4) AS `key`,s.size,s.etag,s.touched_at AS modified_at FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=? AND s.state='ready' AND LEFT(o.object_key,OCTET_LENGTH(?))=? AND (o.object_key>? OR (? AND o.object_key=?)) ORDER BY o.object_key LIMIT 64").bind(bucket).bind(prefix.as_bytes()).bind(prefix.as_bytes()).bind(key.as_bytes()).bind(inclusive).bind(key.as_bytes())
}
pub(in super::super) fn node_upload_page<'a>(
    bucket: Uuid,
    access: &'a str,
    prefix: &'a str,
    after: &'a str,
    inclusive: bool,
    id: Option<Uuid>,
) -> Rows<'a, UploadSummary> {
    sqlx::query_as("SELECT id,CAST(object_key AS CHAR CHARACTER SET utf8mb4) AS object_key,created_at AS created_at,checksum_algorithm,checksum_type FROM uploads WHERE bucket_id=? AND access_key=? AND state IN ('active','completing') AND LEFT(object_key,OCTET_LENGTH(?))=? AND (object_key>? OR (object_key=? AND (? OR id>?))) ORDER BY object_key,id LIMIT 64").bind(bucket).bind(access).bind(prefix.as_bytes()).bind(prefix.as_bytes()).bind(after.as_bytes()).bind(after.as_bytes()).bind(inclusive).bind(id)
}
pub(in super::super) fn multipart_upload<'a>(
    id: Uuid,
    bucket: Uuid,
    key: &'a str,
    access: &'a str,
) -> Rows<'a, Upload> {
    sqlx::query_as("SELECT * FROM uploads WHERE id=? AND bucket_id=? AND object_key=? AND access_key=? AND state<>'aborted'").bind(id).bind(bucket).bind(key.as_bytes()).bind(access)
}
pub(in super::super) fn upload_parts_page(
    id: Uuid,
    after: i32,
    limit: i64,
) -> Rows<'static, UploadPart> {
    sqlx::query_as("SELECT p.part_number,s.size,s.etag,s.touched_at AS touched_at,s.checksums FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=? AND p.part_number>? ORDER BY p.part_number LIMIT ?").bind(id).bind(after).bind(limit)
}

pub(in super::super) use super::super::node_mutations::{change_objects, multipart_operation};
