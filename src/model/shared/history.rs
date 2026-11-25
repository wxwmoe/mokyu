use super::{Backend, queries::history as q, transactions};
use crate::{
    app::StoredStream,
    authorization::{Action, Permit, Principal},
    model::{Id, history::*},
};
use anyhow::{Result, ensure};
use uuid::Uuid;

pub(super) async fn set_mode(
    tx: &mut q::Tx<'_>,
    bucket: Uuid,
    change: Option<Versioning>,
) -> Result<Versioning> {
    let current = Versioning::from_code(
        q::mode(bucket, change.is_some())
            .fetch_one(&mut **tx)
            .await?,
    )?;
    if let Some(mode) = change {
        ensure!(mode != Versioning::Off, s3s::s3_error!(InvalidArgument));
        if current != mode {
            if current == Versioning::Off {
                q::clock(bucket).execute(&mut **tx).await?;
                q::baseline(bucket).execute(&mut **tx).await?;
            }
            q::set_mode(bucket, mode.code()).execute(&mut **tx).await?;
        }
    }
    Ok(change.unwrap_or(current))
}
pub(super) async fn page(tx: &mut q::Tx<'_>, bucket: Uuid, selection: &Selection) -> Result<Page> {
    let limit = selection.limit()?;
    let mut page = Page::default();
    if limit == 0 {
        return Ok(page);
    }
    let mut cursor = selection.clone();
    let mode = q::mode(bucket, false).fetch_one(&mut **tx).await?;
    let mut last = None;
    let mut next = None;
    loop {
        let rows = q::list(tx, bucket, &cursor, limit as i64 + 1, mode != 0).await?;
        let full = rows.len() > limit;
        for row in rows {
            cursor.key_marker = Some(row.key.clone());
            cursor.version_marker = Some(row.version_id.clone());
            let prefix = selection
                .delimiter
                .as_deref()
                .filter(|d| !d.is_empty())
                .and_then(|d| {
                    row.key[selection.prefix.len()..]
                        .find(d)
                        .map(|n| row.key[..selection.prefix.len() + n + d.len()].to_owned())
                });
            if let Some(prefix) = &prefix
                && (last.as_ref() == Some(prefix)
                    || selection.key_marker.as_ref().is_some_and(|k| k >= prefix))
            {
                continue;
            }
            if page.rows.len() + page.prefixes.len() == limit {
                (page.next_key, page.next_version) = next.unwrap();
                return Ok(page);
            }
            if let Some(prefix) = prefix {
                next = Some((Some(prefix.clone()), None));
                last = Some(prefix.clone());
                page.prefixes.push(prefix);
            } else {
                next = Some((Some(row.key.clone()), Some(row.version_id.clone())));
                page.rows.push(row);
            }
        }
        if !full {
            return Ok(page);
        }
    }
}
#[async_trait::async_trait]
impl History for Backend {
    async fn versioning(
        &self,
        principal: &Principal,
        bucket: Uuid,
        change: Option<Versioning>,
    ) -> Result<Versioning> {
        let permit = Permit::for_action(principal.clone(), bucket, Action::Settings);
        let mut tx = q::begin(self, change.is_some()).await?;
        transactions::lock_media_scope(&mut tx, &permit).await?;
        let result = set_mode(&mut tx, bucket, change).await?;
        tx.commit().await?;
        Ok(result)
    }
    async fn versions(
        &self,
        authority: &Permit,
        bucket: Uuid,
        selection: &Selection,
    ) -> Result<Page> {
        let mut tx = q::begin(self, false).await?;
        let mut p = authority.clone();
        p.add(bucket, Action::ListVersions);
        transactions::lock_permit(&mut tx, &p).await?;
        let rows = page(&mut tx, bucket, selection).await?;
        tx.commit().await?;
        Ok(rows)
    }
    async fn version(
        &self,
        authority: &Permit,
        bucket: Uuid,
        key: &str,
        id: Option<&str>,
        ttl: Option<i64>,
    ) -> Result<Option<(Version, Option<StoredStream>, Option<Uuid>)>> {
        ensure!(id.is_none_or(valid_id), s3s::s3_error!(NoSuchVersion));
        let mut p = authority.clone();
        p.add(
            bucket,
            if id.is_some() {
                Action::ReadVersion
            } else {
                Action::Read
            },
        );
        let mut tx = q::begin(self, ttl.is_some()).await?;
        transactions::lock_permit(&mut tx, &p).await?;
        let row = if id == Some("null") && q::mode(bucket, false).fetch_one(&mut *tx).await? == 0 {
            q::current(&mut tx, bucket, key).await?
        } else {
            q::one(&mut tx, bucket, key, id).await?
        };
        let result = if let Some(v) = row {
            let stream = if let Some(s) = v.stream {
                let mut stream = super::queries::journal::stream(s)
                    .fetch_one(&mut *tx)
                    .await?;
                stream.object_key = key.to_owned();
                stream.bucket_id = bucket;
                stream.touched_at = chrono::DateTime::from_timestamp_micros(v.modified_us)
                    .ok_or_else(|| anyhow::anyhow!("invalid version timestamp"))?;
                Some(stream)
            } else {
                None
            };
            let lease = if let (Some(s), Some(ttl)) = (v.stream, ttl) {
                q::lease(&mut tx, s, ttl).await?
            } else {
                None
            };
            Some((v, stream, lease))
        } else {
            None
        };
        tx.commit().await?;
        Ok(result)
    }
    async fn retention_preview(
        &self,
        principal: &Principal,
        bucket: Uuid,
        filter: RetentionFilter,
    ) -> Result<RetentionPreview> {
        let now = chrono::Utc::now().timestamp_micros();
        let age = i64::from(filter.days)
            .checked_mul(86_400_000_000)
            .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
        let cutoff = now
            .checked_sub(age)
            .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
        let mut p = Permit::for_action(principal.clone(), bucket, Action::Settings);
        p.add(bucket, Action::ListVersions);
        let mut tx = q::begin(self, false).await?;
        transactions::lock_permit(&mut tx, &p).await?;
        let result = q::retention_preview(&mut tx, bucket, cutoff, i64::from(filter.keep)).await?;
        tx.commit().await?;
        Ok(result)
    }
    async fn delete_version(
        &self,
        authority: &Permit,
        bucket: Uuid,
        key: &str,
        id: &str,
    ) -> Result<Option<Version>> {
        ensure!(valid_id(id), s3s::s3_error!(InvalidArgument));
        let mut p = authority.clone();
        p.add(bucket, Action::DeleteVersion);
        let mut tx = q::begin(self, true).await?;
        transactions::lock_media_scope(&mut tx, &p).await?;
        let row = delete(&mut tx, bucket, key, id).await?;
        tx.commit().await?;
        Ok(row)
    }
}
pub(super) async fn delete(
    tx: &mut q::Tx<'_>,
    bucket: Uuid,
    key: &str,
    id: &str,
) -> Result<Option<Version>> {
    transactions::redundancy::writable(tx, bucket, key, None).await?;
    let mode = q::mode(bucket, true).fetch_one(&mut **tx).await?;
    let row = if mode == 0 && id == "null" {
        q::current(tx, bucket, key).await?
    } else {
        q::one(tx, bucket, key, Some(id)).await?
    };
    if let Some(row) = &row {
        q::remove(bucket, key, id).execute(&mut **tx).await?;
        if row.latest {
            let next = q::one(tx, bucket, key, None).await?;
            if row.stream.is_none() && next.as_ref().is_some_and(|v| v.stream.is_some()) {
                // GC may have removed the empty current slot while the delete marker was retained.
                super::queries::claim_object(bucket, key, Id::default())
                    .execute(&mut **tx)
                    .await?;
            }
            q::select_head(tx, bucket, key, row, next.as_ref()).await?;
        }
    }
    Ok(row)
}
