use super::super::{Backend, transactions};
use crate::model::{
    Id,
    key::ManagedKeyMaterial,
    node_work::Member,
    source::{SourceSnapshot, SourceUpdate},
    task::Execution,
};
use anyhow::{Result, ensure};
use uuid::Uuid;

pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, sqlx::MySql>;
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
pub(in super::super) async fn members(
    tx: &mut Tx<'_>,
    pack: &crate::content::pack::Pack,
    snapshot: &SourceSnapshot,
) -> Result<Vec<Member>> {
    let rows:Vec<(Id,Uuid,i64,i32,Vec<u8>)>=sqlx::query_as("SELECT m.chunk_id,c.uuid,m.offset_bytes,c.raw_size,c.hash FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=? ORDER BY m.ordinal LIMIT 4097").bind(pack.id).fetch_all(&mut **tx).await?;
    let logical = rows
        .iter()
        .map(|(id, _, offset, size, hash)| (*id, *offset, *size, hash.clone()))
        .collect::<Vec<_>>();
    ensure!(
        Some(crate::model::source::manifest_digest(pack, &logical)?) == snapshot.manifest_digest,
        crate::model::Error::OperationAborted
    );
    Ok(rows
        .into_iter()
        .map(|(_, uuid, offset, size, hash)| (uuid.into(), offset, size, hash))
        .collect())
}
