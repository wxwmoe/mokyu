use super::super::{Backend, transactions};
use crate::model::{key::ManagedKeyMaterial, source::SourceUpdate, task::Execution};
use anyhow::Result;
use uuid::Uuid;

pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, sqlx::Postgres>;
pub(in super::super) async fn apply(tx: &mut Tx<'_>, update: SourceUpdate<'_>) -> Result<()> {
    transactions::source_update_in(tx, update).await
}
pub(in super::super) async fn fail(tx: &mut Tx<'_>, task: Execution, error: &str) -> Result<()> {
    super::work::finish(task.id, task.token, 0, true, Some(error))
        .execute(&mut **tx)
        .await?;
    Ok(())
}
pub(in super::super) async fn block(tx: &mut Tx<'_>, id: Uuid) -> Result<ManagedKeyMaterial> {
    Ok(super::node_read_keys(&[id]).fetch_one(&mut **tx).await?)
}
pub(in super::super) async fn rate(
    _backend: &Backend,
    tx: &mut Tx<'_>,
    storage: Uuid,
    rates: &[(&str, f64, f64)],
) -> Result<bool> {
    transactions::integrity_rate_in(tx, storage, rates).await
}
