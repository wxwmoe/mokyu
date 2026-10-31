use super::Backend;
pub(super) type Db = sqlx::Sqlite;
pub(super) const LOCK: &str = "";
pub(super) async fn read(b: &Backend) -> anyhow::Result<sqlx::Transaction<'_, Db>> {
    Ok(b.read.begin().await?)
}
pub(super) async fn write(b: &Backend) -> anyhow::Result<sqlx::Transaction<'_, Db>> {
    Ok(b.write.begin_with(super::queries::BEGIN_WRITE).await?)
}

pub(super) fn pool(b: &Backend) -> &sqlx::Pool<Db> {
    &b.read
}

pub(super) async fn collision(
    t: &mut sqlx::Transaction<'_, Db>,
    bucket: uuid::Uuid,
    policy: &crate::model::image_styles::Policy,
    edits: &[crate::model::image_styles::Edit],
) -> anyhow::Result<bool> {
    use crate::model::image_styles::Edit;
    if !policy.enabled || policy.delimiters.is_empty() {
        return Ok(false);
    }
    let mut query = sqlx::QueryBuilder::<Db>::new(
        "SELECT o.object_key FROM objects o JOIN (SELECT name FROM image_styles WHERE enabled AND bucket_id=",
    );
    query.push_bind(bucket);
    if !edits.is_empty() {
        query.push(" AND name NOT IN (");
        let mut p = query.separated(",");
        for edit in edits {
            p.push_bind(edit.name());
        }
        query.push(")");
    }
    for edit in edits {
        if let Edit::Put { style } = edit
            && style.enabled
        {
            query.push(" UNION ALL SELECT ").push_bind(&style.name);
        }
    }
    query.push(") s ON (");
    for (i, delimiter) in policy.delimiters.iter().enumerate() {
        if i > 0 {
            query.push(" OR ");
        }
        query
            .push("substr(CAST(o.object_key AS TEXT),-length(s.name)-1)=")
            .push("")
            .push_bind(delimiter)
            .push(" || s.name");
    }
    query
        .push(") WHERE o.bucket_id=")
        .push_bind(bucket)
        .push(" LIMIT 1");
    Ok(query.build().fetch_optional(&mut **t).await?.is_some())
}
