use super::super::Backend;
pub(in super::super) type Db = sqlx::MySql;
pub(in super::super) const LOCK: &str = " FOR UPDATE";
pub(in super::super) async fn read(b: &Backend) -> anyhow::Result<sqlx::Transaction<'_, Db>> {
    Ok(b.pool.begin().await?)
}
pub(in super::super) async fn write(b: &Backend) -> anyhow::Result<sqlx::Transaction<'_, Db>> {
    Ok(b.pool.begin().await?)
}

pub(in super::super) fn pool(b: &Backend) -> &sqlx::Pool<Db> {
    &b.pool
}

pub(in super::super) async fn collision(
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
            .push("RIGHT(o.object_key,OCTET_LENGTH(s.name)+1)=")
            .push("CONCAT(")
            .push_bind(delimiter)
            .push(",CAST(s.name AS BINARY))");
    }
    query
        .push(") WHERE o.bucket_id=")
        .push_bind(bucket)
        .push(" LIMIT 1");
    query.push(" LOCK IN SHARE MODE");
    Ok(query.build().fetch_optional(&mut **t).await?.is_some())
}
