use super::Backend;
pub(super) type Db = sqlx::Sqlite;
pub(super) const LOCK: &str = "";
pub(super) async fn read(b: &Backend) -> anyhow::Result<sqlx::Transaction<'_, Db>> {
    Ok(b.read.begin().await?)
}
pub(super) async fn write(b: &Backend) -> anyhow::Result<sqlx::Transaction<'_, Db>> {
    Ok(b.write.begin_with(super::queries::BEGIN_WRITE).await?)
}
