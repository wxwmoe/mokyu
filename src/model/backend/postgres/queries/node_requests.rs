use super::super::{Backend, transactions};
pub(in super::super) use super::{
    multipart_upload, node_catalog_buckets, node_catalog_objects, node_object_acl,
    node_read_bucket, node_read_credential, node_request_allowed, node_upload_page,
    storage_profile, upload_parts_page,
};
use crate::{
    cluster::NodeState,
    content::codec::Key,
    model::{Error, session::Run},
};
use anyhow::{Result, ensure};
pub(in super::super) type Connection = sqlx::PgConnection;
pub(in super::super) async fn begin(b: &Backend) -> Result<sqlx::Transaction<'_, sqlx::Postgres>> {
    let mut tx = b.pool.begin().await?;
    b.fence_control(&mut tx).await?;
    Ok(tx)
}
pub(in super::super) async fn admit(
    _b: &Backend,
    tx: &mut Connection,
    node: &NodeState,
    key: &Key,
    run: Run,
) -> Result<()> {
    transactions::authenticate_rpc_node(tx, node, key, false).await?;
    ensure!(
        matches!(node.role.as_str(), "data" | "node"),
        Error::NodeAuthentication
    );
    let session = transactions::require_node_run(tx, node, run, false).await?;
    ensure!(
        !session.draining && super::node_read_admission().fetch_one(tx).await?,
        Error::NodeInactive
    );
    Ok(())
}
