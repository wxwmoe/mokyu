use super::super::{Backend, transactions};
use crate::model::{
    key::ManagedKeyMaterial,
    node_work::Member,
    source::{SourceSnapshot, SourceUpdate},
    task::Execution,
};
use anyhow::{Result, ensure};
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
pub(in super::super) async fn members(
    tx: &mut Tx<'_>,
    pack: &crate::content::pack::Pack,
    snapshot: &SourceSnapshot,
) -> Result<Vec<Member>> {
    let members = super::work::members(pack.id).fetch_all(&mut **tx).await?;
    ensure!(
        Some(crate::model::source::manifest_digest(pack, &members)?) == snapshot.manifest_digest,
        crate::model::Error::OperationAborted
    );
    Ok(members)
}
