use super::queries::raw_pages as q;
use crate::{
    content::raw_pages::{self, GROUP, Group, HASH},
    model::Id,
};
use anyhow::{Result, ensure};

type Source = (String, i64, Option<Vec<u8>>, Option<i64>);

pub(super) async fn put(db: &mut q::Connection, source: Id, group: &Group) -> Result<()> {
    group.validate()?;
    let (_, _, _, sealed): Source = sqlx::query_as(q::LOCK)
        .bind(source)
        .fetch_one(&mut *db)
        .await?;
    if sealed.is_none() {
        sqlx::query(q::INSERT)
            .bind(source)
            .bind(group.index)
            .bind(&group.hashes)
            .execute(&mut *db)
            .await?;
    }
    let saved: Vec<u8> = sqlx::query_scalar(q::GROUP_HASHES)
        .bind(source)
        .bind(group.index)
        .fetch_one(db)
        .await?;
    ensure!(saved == group.hashes, "conflicting raw page group");
    Ok(())
}

pub(super) async fn seal(
    db: &mut q::Connection,
    source: Id,
    size: i64,
    digest: &[u8],
) -> Result<()> {
    let count = raw_pages::count(size)?;
    let (state, current_size, current_digest, sealed): Source = sqlx::query_as(q::LOCK)
        .bind(source)
        .fetch_one(&mut *db)
        .await?;
    ensure!(
        digest.len() == 16
            && (state == "writing"
                || (state == "ready"
                    && current_size == size
                    && current_digest.as_deref() == Some(digest))),
        "raw page source changed"
    );
    if let Some(existing) = sealed {
        ensure!(existing == count, "raw page count changed");
        return Ok(());
    }
    let groups = (count + GROUP as i64 - 1) / GROUP as i64;
    let last = groups - 1;
    let length = if groups == 0 {
        0
    } else {
        (count - last * GROUP as i64) * HASH as i64
    };
    let (total, valid): (i64, i64) = sqlx::query_as(q::COUNTS)
        .bind(last)
        .bind(last)
        .bind(length)
        .bind(source)
        .fetch_one(&mut *db)
        .await?;
    ensure!(
        total == groups && valid == total,
        "incomplete raw page manifest"
    );
    sqlx::query(q::SEAL)
        .bind(count)
        .bind(source)
        .execute(db)
        .await?;
    Ok(())
}

pub(super) async fn inherit(
    db: &mut q::Connection,
    source: Id,
    target: &crate::model::raw::RawSource,
) -> Result<()> {
    let (_, _, _, existing): Source = sqlx::query_as(q::LOCK)
        .bind(target.id)
        .fetch_one(&mut *db)
        .await?;
    if existing.is_some() {
        return Ok(());
    }
    let (_, _, _, pages): Source = sqlx::query_as(q::LOCK)
        .bind(source)
        .fetch_one(&mut *db)
        .await?;
    if pages == Some(raw_pages::count(target.size)?) {
        sqlx::query(q::CLEAR)
            .bind(target.id)
            .execute(&mut *db)
            .await?;
        sqlx::query(q::COPY_GROUPS)
            .bind(target.id)
            .bind(source)
            .execute(&mut *db)
            .await?;
        seal(
            db,
            target.id,
            target.size,
            target.digest.as_deref().unwrap_or_default(),
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn read(
    db: &mut q::Connection,
    source: Id,
    size: i64,
    first: i64,
    count: usize,
) -> Result<Option<Vec<u8>>> {
    let pages = raw_pages::count(size)?;
    let end = first
        .checked_add(i64::try_from(count)?)
        .ok_or_else(|| anyhow::anyhow!("raw page overflow"))?;
    ensure!(
        first >= 0 && end <= pages && count <= 16384,
        "invalid raw page window"
    );
    let rows: Vec<(Option<i64>, Option<i64>, Option<Vec<u8>>)> = sqlx::query_as(q::READ)
        .bind(first / GROUP as i64)
        .bind(if count == 0 {
            -1
        } else {
            (end - 1) / GROUP as i64
        })
        .bind(source)
        .bind(size)
        .fetch_all(db)
        .await?;
    ensure!(!rows.is_empty(), "raw page source missing");
    if rows[0].0.is_none() {
        return Ok(None);
    }
    if count == 0 {
        ensure!(rows[0].0 == Some(pages), "raw page count changed");
        return Ok(Some(Vec::new()));
    }
    let mut out = Vec::with_capacity(count * HASH);
    let mut next = first;
    for (sealed, index, data) in rows {
        ensure!(
            sealed == Some(pages) && index == Some(next / GROUP as i64),
            "raw page group missing"
        );
        let data = data.ok_or_else(|| anyhow::anyhow!("raw page group missing"))?;
        let index = next / GROUP as i64;
        let offset = index * GROUP as i64;
        let group_pages = (pages - offset).min(GROUP as i64);
        ensure!(
            data.len() == group_pages as usize * HASH,
            "raw page group length changed"
        );
        let stop = end.min(offset + group_pages);
        out.extend_from_slice(
            &data[(next - offset) as usize * HASH..(stop - offset) as usize * HASH],
        );
        next = stop;
    }
    ensure!(next == end, "incomplete raw page window");
    Ok(Some(out))
}
