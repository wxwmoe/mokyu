use super::super::{Backend, transactions};
use crate::model::{key::ManagedKeyMaterial, source::SourceUpdate, task::Execution};
use anyhow::Result;
use uuid::Uuid;

pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, sqlx::Sqlite>;
pub(in super::super) async fn apply(tx: &mut Tx<'_>, update: SourceUpdate<'_>) -> Result<()> {
    transactions::source_update_in(tx, update).await
}
pub(in super::super) async fn fail(tx: &mut Tx<'_>, task: Execution, error: &str) -> Result<()> {
    super::finish_task(task, "failed", Some(error))
        .execute(&mut **tx)
        .await?;
    Ok(())
}
pub(in super::super) async fn block(tx: &mut Tx<'_>, id: Uuid) -> Result<ManagedKeyMaterial> {
    Ok(super::work::block_key(id).fetch_one(&mut **tx).await?)
}
pub(in super::super) async fn rate(
    backend: &Backend,
    _tx: &mut Tx<'_>,
    storage: Uuid,
    rates: &[(&str, f64, f64)],
) -> Result<bool> {
    let mut requests = None;
    let mut bytes = None;
    let mut count = 0.;
    let mut size = 0.;
    for &(metric, rate, amount) in rates {
        match metric {
            "integrity_rps" => {
                requests = Some(rate);
                count = amount;
            }
            "integrity_bandwidth" => {
                bytes = Some(rate);
                size = amount;
            }
            _ => anyhow::bail!("invalid integrity rate"),
        }
    }
    Ok(backend
        .integrity_rates
        .acquire(storage, requests, bytes, count, size))
}
