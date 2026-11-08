use super::{
    Backend,
    queries::{node_registry as registry, staging as q, work},
};
use anyhow::{Result, ensure};
use uuid::Uuid;
pub(super) async fn reserve(
    tx: &mut registry::Tx<'_>,
    storage: Uuid,
    key: &str,
    bytes: u64,
    limit: u64,
) -> Result<()> {
    ensure!(
        crate::storage::is_fragment_key(key),
        s3s::s3_error!(InvalidArgument)
    );
    super::queries::storage_profile(storage)
        .fetch_one(&mut **tx)
        .await?;
    work::storage(storage)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(crate::model::Error::SourceUnavailable)?;
    if let Some(old) = q::size(storage, key).fetch_optional(&mut **tx).await? {
        ensure!(
            old == i64::try_from(bytes)?,
            crate::model::Error::NodeOperationConflict
        );
        return Ok(());
    }
    let used = q::used(storage).fetch_one(&mut **tx).await?;
    ensure!(
        u64::try_from(used)?
            .checked_add(bytes)
            .is_some_and(|n| n <= limit),
        s3s::s3_error!(SlowDown, "shared staging quota exhausted")
    );
    q::reserve(storage, key, i64::try_from(bytes)?)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
#[async_trait::async_trait]
impl crate::model::cluster::StagingStore for Backend {
    async fn claim_staging(&self, storage: Uuid, key: &str, bytes: u64, limit: u64) -> Result<()> {
        let mut tx = registry::begin(self).await?;
        reserve(&mut tx, storage, key, bytes, limit).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn release_staging(&self, storage: Uuid, key: &str) -> Result<()> {
        let mut tx = registry::begin(self).await?;
        q::release(storage, key).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn stale_staging(&self, storage: Option<Uuid>) -> Result<Vec<(Uuid, String)>> {
        let mut tx = registry::begin(self).await?;
        Ok(q::old(storage).fetch_all(&mut *tx).await?)
    }
}
