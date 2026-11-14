use super::super::Backend;
use super::{Scalar, Statement};
use crate::model::{
    Id,
    history::{Selection, Version},
};
use anyhow::Result;
use uuid::Uuid;

pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, sqlx::Postgres>;
pub(in super::super) async fn begin(b: &Backend, write: bool) -> Result<Tx<'_>> {
    let mut tx = b.pool.begin().await?;
    if write {
        b.fence_control(&mut tx).await?;
    }
    Ok(tx)
}
pub(in super::super) fn mode(b: Uuid, lock: bool) -> Scalar<'static, i64> {
    let query = "SELECT CAST(version_mode AS BIGINT) FROM buckets WHERE id=$1";
    sqlx::query_scalar(if lock {
        "SELECT CAST(version_mode AS BIGINT) FROM buckets WHERE id=$1 FOR UPDATE"
    } else {
        query
    })
    .bind(b)
}
pub(in super::super) fn clock(b: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO object_version_clocks(bucket_id) VALUES($1)").bind(b)
}
pub(in super::super) fn baseline(b: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO object_versions(bucket_id,object_key,seq,version_id,stream_id,modified_us) SELECT o.bucket_id,o.object_key,0,'null',s.id,(extract(epoch FROM s.touched_at)*1000000)::bigint FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=$1").bind(b)
}
pub(in super::super) fn set_mode(b: Uuid, mode: i16) -> Statement<'static> {
    sqlx::query(
        "UPDATE buckets SET settings_revision=settings_revision+1,version_mode=$1 WHERE id=$2",
    )
    .bind(mode)
    .bind(b)
}
pub(in super::super) fn remove<'a>(b: Uuid, key: &'a str, id: &'a str) -> Statement<'a> {
    sqlx::query(
        "DELETE FROM object_versions WHERE bucket_id=$1 AND object_key=$2 AND version_id=$3",
    )
    .bind(b)
    .bind(key)
    .bind(id)
}
pub(in super::super) async fn published(b: &Backend, stream: Id) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT version_id FROM object_write_receipts WHERE stream_id=$1")
            .bind(stream)
            .fetch_optional(&b.pool)
            .await?,
    )
}
pub(in super::super) async fn receipt(tx: &mut Tx<'_>, stream: Id) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT version_id FROM object_write_receipts WHERE stream_id=$1")
            .bind(stream)
            .fetch_optional(&mut **tx)
            .await?,
    )
}
const BASE: &str = r#"SELECT v.object_key AS "key", v.version_id,NOT EXISTS(SELECT 1 FROM object_versions later WHERE later.bucket_id=v.bucket_id AND later.object_key=v.object_key AND later.seq>v.seq) AS latest,v.modified_us,s.size,s.etag,v.stream_id AS stream FROM object_versions v LEFT JOIN streams s ON s.id=v.stream_id WHERE v.bucket_id="#;
const CURRENT: &str = r#"SELECT v.object_key AS "key", 'null' AS version_id, TRUE AS latest,(extract(epoch FROM s.touched_at)*1000000)::bigint AS modified_us,s.size,s.etag,v.stream_id AS stream FROM objects v JOIN streams s ON s.id=v.stream_id WHERE v.bucket_id="#;
pub(in super::super) async fn one(
    tx: &mut Tx<'_>,
    b: Uuid,
    key: &str,
    id: Option<&str>,
) -> Result<Option<Version>> {
    let mut q = sqlx::QueryBuilder::<sqlx::Postgres>::new(BASE);
    q.push_bind(b).push(" AND v.object_key=").push_bind(key);
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
    let mut q = sqlx::QueryBuilder::<sqlx::Postgres>::new(CURRENT);
    q.push_bind(b).push(" AND v.object_key=").push_bind(key);
    Ok(q.build_query_as().fetch_optional(&mut **tx).await?)
}
pub(in super::super) async fn list(
    tx: &mut Tx<'_>,
    b: Uuid,
    f: &Selection,
    n: i64,
    enabled: bool,
) -> Result<Vec<Version>> {
    let mut q = sqlx::QueryBuilder::<sqlx::Postgres>::new(if enabled { BASE } else { CURRENT });
    q.push_bind(b);
    if !f.prefix.is_empty() {
        q.push(" AND v.object_key>=").push_bind(f.prefix.as_str());
        q.push(" AND starts_with(v.object_key,")
            .push_bind(f.prefix.as_str())
            .push(")");
    }
    if let Some(k) = f.key_marker.as_deref() {
        q.push(" AND (v.object_key>").push_bind(k);
        if enabled && let Some(id) = &f.version_marker {
            q.push(" OR (v.object_key=")
                .push_bind(k)
                .push(" AND v.seq<(SELECT seq FROM object_versions WHERE bucket_id=")
                .push_bind(b)
                .push(" AND object_key=")
                .push_bind(k)
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
    Ok(Some(
        super::create_read_lease(s, ttl)
            .fetch_one(&mut **tx)
            .await?,
    ))
}
pub(in super::super) async fn select_head(
    tx: &mut Tx<'_>,
    b: Uuid,
    key: &str,
    previous: &Version,
    next: Option<&Version>,
) -> Result<()> {
    let id = next.and_then(|v| v.stream);
    let _ = previous;
    if let Some(s) = id {
        sqlx::query("UPDATE streams SET state='ready' WHERE id=$1")
            .bind(s)
            .execute(&mut **tx)
            .await?;
    }
    sqlx::query("UPDATE objects SET version_switch=NOT version_switch,stream_id=$3,write_token=gen_random_uuid() WHERE bucket_id=$1 AND object_key=$2").bind(b).bind(key).bind(id).execute(&mut **tx).await?;
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
        sqlx::query(
            "UPDATE object_version_clocks SET version_seq=version_seq+1 WHERE bucket_id=$1",
        )
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
        sqlx::query("INSERT INTO object_versions(bucket_id,object_key,seq,version_id,stream_id,modified_us) SELECT bucket_id,$1,version_seq,$2,NULL,(extract(epoch FROM clock_timestamp())*1000000)::bigint FROM object_version_clocks WHERE bucket_id=$3").bind(key).bind(id).bind(b).execute(&mut **tx).await?;
    }
    if crate::model::history::DELETE_RECEIPT
        .try_with(|_| ())
        .is_ok()
    {
        crate::model::history::delete_result(one(tx, b, key, None).await?);
    }
    Ok(())
}
