use super::super::{Backend, transactions};
use super::{Scalar, Statement};
use crate::model::{
    Id,
    history::{Selection, Version},
};
use anyhow::Result;
use uuid::Uuid;

pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, sqlx::Sqlite>;
pub(in super::super) async fn begin(b: &Backend, write: bool) -> Result<Tx<'_>> {
    let mut tx = if write {
        b.write.begin_with(super::BEGIN_WRITE).await?
    } else {
        b.read.begin().await?
    };
    if write {
        b.fence_control(&mut tx).await?;
    }
    Ok(tx)
}
pub(in super::super) fn mode(b: Uuid, _lock: bool) -> Scalar<'static, i64> {
    let query = "SELECT CAST(version_mode AS BIGINT) FROM published_buckets WHERE id=?";
    sqlx::query_scalar(query).bind(b)
}
pub(in super::super) fn clock(b: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO object_version_clocks(bucket_id) VALUES(?)").bind(b)
}
pub(in super::super) fn baseline(b: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO object_versions(bucket_id,object_key,seq,version_id,stream_id,modified_us) SELECT o.bucket_id,o.object_key,0,'null',s.id,s.touched_at FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=?").bind(b)
}
pub(in super::super) fn set_mode(b: Uuid, mode: i16) -> Statement<'static> {
    sqlx::query(
        "UPDATE buckets SET settings_revision=settings_revision+1,version_mode=? WHERE id=? AND restore_job IS NULL",
    )
    .bind(mode)
    .bind(b)
}
pub(in super::super) fn remove<'a>(b: Uuid, key: &'a str, id: &'a str) -> Statement<'a> {
    sqlx::query("DELETE FROM object_versions WHERE bucket_id=? AND object_key=? AND version_id=?")
        .bind(b)
        .bind(key.as_bytes())
        .bind(id)
}
pub(in super::super) async fn published(b: &Backend, stream: Id) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT version_id FROM object_write_receipts WHERE stream_id=?")
            .bind(stream)
            .fetch_optional(&b.write)
            .await?,
    )
}
pub(in super::super) async fn receipt(tx: &mut Tx<'_>, stream: Id) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT version_id FROM object_write_receipts WHERE stream_id=?")
            .bind(stream)
            .fetch_optional(&mut **tx)
            .await?,
    )
}
const BASE: &str = r#"SELECT CAST(v.object_key AS TEXT) AS "key", v.version_id,NOT EXISTS(SELECT 1 FROM object_versions later WHERE later.bucket_id=v.bucket_id AND later.object_key=v.object_key AND later.seq>v.seq) AS latest,v.modified_us,s.size,s.etag,v.stream_id AS stream FROM object_versions v LEFT JOIN streams s ON s.id=v.stream_id WHERE v.bucket_id="#;
const CURRENT: &str = r#"SELECT CAST(v.object_key AS TEXT) AS "key", 'null' AS version_id, TRUE AS latest,s.touched_at AS modified_us,s.size,s.etag,v.stream_id AS stream FROM objects v JOIN streams s ON s.id=v.stream_id WHERE v.bucket_id="#;
pub(in super::super) async fn one(
    tx: &mut Tx<'_>,
    b: Uuid,
    key: &str,
    id: Option<&str>,
) -> Result<Option<Version>> {
    let mut q = sqlx::QueryBuilder::<sqlx::Sqlite>::new(BASE);
    q.push_bind(b)
        .push(" AND v.object_key=")
        .push_bind(key.as_bytes());
    if let Some(id) = id {
        q.push(" AND v.version_id=").push_bind(id);
    }
    q.push(" ORDER BY v.seq DESC LIMIT 1");
    Ok(q.build_query_as().fetch_optional(&mut **tx).await?)
}
pub(in super::super) async fn current(
    tx: &mut Tx<'_>,
    b: Uuid,
    key: &str,
) -> Result<Option<Version>> {
    let mut q = sqlx::QueryBuilder::<sqlx::Sqlite>::new(CURRENT);
    q.push_bind(b)
        .push(" AND v.object_key=")
        .push_bind(key.as_bytes());
    Ok(q.build_query_as().fetch_optional(&mut **tx).await?)
}
pub(in super::super) async fn list(
    tx: &mut Tx<'_>,
    b: Uuid,
    f: &Selection,
    n: i64,
    enabled: bool,
) -> Result<Vec<Version>> {
    let mut q = sqlx::QueryBuilder::<sqlx::Sqlite>::new(if enabled { BASE } else { CURRENT });
    q.push_bind(b);
    if !f.prefix.is_empty() {
        q.push(" AND v.object_key>=").push_bind(f.prefix.as_bytes());
        q.push(" AND substr(v.object_key,1,")
            .push_bind(f.prefix.len() as i64)
            .push(")=")
            .push_bind(f.prefix.as_bytes());
    }
    if let Some(k) = f.key_marker.as_deref() {
        q.push(" AND (v.object_key>").push_bind(k.as_bytes());
        if enabled && let Some(id) = &f.version_marker {
            q.push(" OR (v.object_key=")
                .push_bind(k.as_bytes())
                .push(" AND v.seq<(SELECT seq FROM object_versions WHERE bucket_id=")
                .push_bind(b)
                .push(" AND object_key=")
                .push_bind(k.as_bytes())
                .push(" AND version_id=")
                .push_bind(id)
                .push("))");
        }
        q.push(")");
    }
    q.push(if enabled {
        " ORDER BY v.object_key,v.seq DESC LIMIT "
    } else {
        " ORDER BY v.object_key LIMIT "
    })
    .push_bind(n);
    Ok(q.build_query_as().fetch_all(&mut **tx).await?)
}
pub(in super::super) async fn lease(tx: &mut Tx<'_>, s: Id, ttl: i64) -> Result<Option<Uuid>> {
    let _ = (tx, s, ttl);
    Ok(None)
}
pub(in super::super) async fn select_head(
    tx: &mut Tx<'_>,
    b: Uuid,
    key: &str,
    previous: &Version,
    next: Option<&Version>,
) -> Result<()> {
    let id = next.and_then(|v| v.stream);
    let mut accounts = transactions::lock_quota(tx, b).await?;
    transactions::replace_object_quota(
        tx,
        &mut accounts,
        b,
        key,
        previous.stream.zip(previous.size),
        next.and_then(|v| v.stream.zip(v.size)),
    )
    .await?;
    if let Some(s) = id {
        sqlx::query("UPDATE streams SET state='ready' WHERE id=?")
            .bind(s)
            .execute(&mut **tx)
            .await?;
    }
    let attr = if let Some(s) = id {
        Some(super::journal::stream(s).fetch_one(&mut **tx).await?)
    } else {
        None
    };
    let mime = attr
        .as_ref()
        .and_then(|v| v.metadata.get("content_type"))
        .and_then(|v| v.as_str())
        .unwrap_or("application/octet-stream");
    let category = attr.as_ref().map(|_| super::catalog_category(key, mime));
    sqlx::query("UPDATE objects SET version_switch=NOT version_switch,stream_id=?,write_token=0,catalog_size=?,catalog_modified=?,catalog_type=?,catalog_category=?,catalog_public=? WHERE bucket_id=? AND object_key=?").bind(id).bind(next.and_then(|v|v.size)).bind(attr.as_ref().map(|v|v.created_at.timestamp_micros())).bind(attr.as_ref().map(|_|mime)).bind(category).bind(attr.as_ref().map(|v|v.public_read)).bind(b).bind(key.as_bytes()).execute(&mut **tx).await?;
    if let Some(old) = previous.stream
        && Some(old) != id
    {
        super::retire_stream(old).execute(&mut **tx).await?;
    }
    Ok(())
}

pub(in super::super) async fn deleted(
    tx: &mut Tx<'_>,
    b: Uuid,
    key: &str,
    absent: bool,
) -> Result<()> {
    let mode = mode(b, false).fetch_one(&mut **tx).await?;
    if mode == 0 {
        return Ok(());
    }
    if absent {
        sqlx::query("UPDATE object_version_clocks SET version_seq=version_seq+1 WHERE bucket_id=?")
            .bind(b)
            .execute(&mut **tx)
            .await?;
        if mode == 2 {
            remove(b, key, "null").execute(&mut **tx).await?;
        }
        let id = if mode == 2 {
            "null".to_owned()
        } else {
            Uuid::new_v4().simple().to_string()
        };
        sqlx::query("INSERT INTO object_versions(bucket_id,object_key,seq,version_id,stream_id,modified_us) SELECT bucket_id,?,version_seq,?,NULL,CAST(unixepoch('subsec')*1000000 AS INTEGER) FROM object_version_clocks WHERE bucket_id=?").bind(key.as_bytes()).bind(id).bind(b).execute(&mut **tx).await?;
    }
    if crate::model::history::DELETE_RECEIPT
        .try_with(|_| ())
        .is_ok()
    {
        crate::model::history::delete_result(one(tx, b, key, None).await?);
    }
    Ok(())
}

pub(in super::super) async fn retention_preview(
    tx: &mut Tx<'_>,
    bucket: Uuid,
    cutoff: i64,
    keep: i64,
) -> Result<crate::model::history::RetentionPreview> {
    Ok(sqlx::query_as(r#"WITH successors AS (
    SELECT v.bucket_id,v.object_key,v.seq,v.stream_id,
           LEAD(v.modified_us) OVER (PARTITION BY v.object_key ORDER BY v.seq) AS superseded
    FROM object_versions v WHERE v.bucket_id=?
), historical AS (
    SELECT successors.*,ROW_NUMBER() OVER (PARTITION BY object_key ORDER BY seq DESC) AS newer_rank
    FROM successors WHERE superseded IS NOT NULL AND stream_id IS NOT NULL
), candidates AS (
    SELECT * FROM historical WHERE superseded<=? AND newer_rank>?
), sizes AS (
    SELECT c.stream_id,s.size,EXISTS (SELECT 1 FROM object_time_points t WHERE t.stream_id=c.stream_id) AS timed
    FROM candidates c JOIN streams s ON s.id=c.stream_id
), measured AS (
    SELECT s.size,s.timed,CASE WHEN s.timed THEN 0
        WHEN (EXISTS (SELECT 1 FROM objects o WHERE o.stream_id=s.stream_id)
                OR EXISTS (SELECT 1 FROM object_time_points t WHERE t.stream_id=s.stream_id)
                OR EXISTS (SELECT 1 FROM object_versions v WHERE v.stream_id=s.stream_id AND NOT EXISTS (
                    SELECT 1 FROM candidates x WHERE x.bucket_id=v.bucket_id AND x.object_key=v.object_key AND x.seq=v.seq))) THEN s.size ELSE
            COALESCE((SELECT SUM(e.length) FROM extents e WHERE e.stream_id=s.stream_id AND EXISTS (
                SELECT 1 FROM (SELECT stream_id FROM extents WHERE chunk_id=e.chunk_id
                               UNION ALL SELECT stream_id FROM extents WHERE fragment_id=e.fragment_id) other
                WHERE other.stream_id<>s.stream_id AND (EXISTS (SELECT 1 FROM objects o WHERE o.stream_id=other.stream_id)
                OR EXISTS (SELECT 1 FROM object_time_points t WHERE t.stream_id=other.stream_id)
                OR EXISTS (SELECT 1 FROM object_versions v WHERE v.stream_id=other.stream_id AND NOT EXISTS (
                    SELECT 1 FROM candidates x WHERE x.bucket_id=v.bucket_id AND x.object_key=v.object_key AND x.seq=v.seq))))),0)
          + COALESCE((SELECT SUM(r.length) FROM raw_ranges r WHERE r.stream_id=s.stream_id AND EXISTS (
                SELECT 1 FROM raw_ranges other WHERE other.source_id=r.source_id AND other.stream_id<>s.stream_id AND (EXISTS (SELECT 1 FROM objects o WHERE o.stream_id=other.stream_id)
                OR EXISTS (SELECT 1 FROM object_time_points t WHERE t.stream_id=other.stream_id)
                OR EXISTS (SELECT 1 FROM object_versions v WHERE v.stream_id=other.stream_id AND NOT EXISTS (
                    SELECT 1 FROM candidates x WHERE x.bucket_id=v.bucket_id AND x.object_key=v.object_key AND x.seq=v.seq))))),0)
        END AS shared_size FROM sizes s
)
SELECT CAST(COUNT(*) AS BIGINT) AS versions,CAST(COALESCE(SUM(size),0) AS BIGINT) AS logical_bytes,
       CAST(COALESCE(SUM(CASE WHEN timed THEN size ELSE 0 END),0) AS BIGINT) AS time_protected_bytes,
       CAST(COALESCE(SUM(shared_size),0) AS BIGINT) AS shared_bytes FROM measured"#).bind(bucket).bind(cutoff).bind(keep).fetch_one(&mut **tx).await?)
}
