use super::Backend;
pub(super) type Db = sqlx::MySql;
pub(super) const LOCK: &str = " FOR UPDATE";
pub(super) async fn read(b: &Backend) -> anyhow::Result<sqlx::Transaction<'_, Db>> {
    Ok(b.pool.begin().await?)
}
pub(super) async fn write(b: &Backend) -> anyhow::Result<sqlx::Transaction<'_, Db>> {
    Ok(b.pool.begin().await?)
}
