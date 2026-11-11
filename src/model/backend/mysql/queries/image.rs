use super::super::Backend;
pub(in super::super) type Db = sqlx::MySql;
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
    delimiters: &[&String],
    stored: bool,
    edits: &[crate::model::image_styles::Edit],
) -> anyhow::Result<bool> {
    use crate::model::image_styles::Edit;
    if delimiters.is_empty() || !stored && edits.is_empty() {
        return Ok(false);
    }
    let mut query = sqlx::QueryBuilder::<Db>::new(
        "SELECT o.object_key FROM objects o JOIN (SELECT name FROM image_styles WHERE enabled AND bucket_id=",
    );
    query.push_bind(bucket);
    if !stored {
        query.push(" AND 1=0");
    } else if !edits.is_empty() {
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
            query
                .push(" UNION ALL SELECT CAST(")
                .push_bind(&style.name)
                .push(" AS CHAR CHARACTER SET ascii) COLLATE ascii_bin");
        }
    }
    query.push(") s ON (");
    for (i, delimiter) in delimiters.iter().copied().enumerate() {
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
        .push(" AND OCTET_LENGTH(o.object_key)>OCTET_LENGTH(s.name)+1 AND (o.stream_id IS NOT NULL OR EXISTS(SELECT 1 FROM streams w WHERE w.id=o.write_token AND w.bucket_id=o.bucket_id AND w.object_key=o.object_key AND w.state='writing')) LIMIT 1");
    query.push(" LOCK IN SHARE MODE");
    Ok(query.build().fetch_optional(&mut **t).await?.is_some())
}

pub(in super::super) const IMPORTED: &str =
    "SELECT image_revision FROM buckets WHERE id=? AND image_import_token=?";

pub(in super::super) const DELETE: &str = "DELETE FROM image_styles WHERE bucket_id=? AND name=?";

pub(in super::super) const INSERT: &str =
    "INSERT INTO image_styles(bucket_id,name,enabled,body) VALUES(?,?,?,?)";

pub(in super::super) const UPDATE: &str = "UPDATE buckets SET image_revision=?,image_families=?,image_policy=?,image_import_token=? WHERE id=? AND image_revision=?";

type Tx<'a> = sqlx::Transaction<'a, Db>;
type StyleRow = (String, bool, String);

pub(in super::super) async fn settings(
    db: &mut Tx<'_>,
    bucket: uuid::Uuid,
    lock: bool,
) -> sqlx::Result<Option<(i64, Option<String>, i64)>> {
    let statement = if lock {
        "SELECT image_revision,image_policy,image_families FROM buckets WHERE id=? FOR UPDATE"
    } else {
        "SELECT image_revision,image_policy,image_families FROM buckets WHERE id=?"
    };
    sqlx::query_as(statement)
        .bind(bucket)
        .fetch_optional(&mut **db)
        .await
}

pub(in super::super) async fn named(
    db: &mut Tx<'_>,
    bucket: uuid::Uuid,
    names: &[String],
) -> sqlx::Result<Vec<StyleRow>> {
    let mut query = sqlx::QueryBuilder::<Db>::new(
        "SELECT name,enabled,body FROM image_styles WHERE bucket_id=",
    );
    query.push_bind(bucket).push(" AND name IN (");
    let mut args = query.separated(",");
    for name in names {
        args.push_bind(name);
    }
    query
        .push(") ORDER BY name")
        .build_query_as()
        .fetch_all(&mut **db)
        .await
}

pub(in super::super) async fn page(
    db: &mut Tx<'_>,
    bucket: uuid::Uuid,
    after: &str,
    prefix: &str,
    limit: u16,
) -> sqlx::Result<Vec<StyleRow>> {
    let mut query = sqlx::QueryBuilder::<Db>::new(
        "SELECT name,enabled,body FROM image_styles WHERE bucket_id=",
    );
    query.push_bind(bucket).push(" AND name>").push_bind(after);
    if !prefix.is_empty() {
        query
            .push(" AND name>=")
            .push_bind(prefix)
            .push(" AND name<")
            .push_bind(format!("{prefix}\u{7f}"));
    }
    query
        .push(" ORDER BY name LIMIT ")
        .push_bind(i64::from(limit))
        .build_query_as()
        .fetch_all(&mut **db)
        .await
}
