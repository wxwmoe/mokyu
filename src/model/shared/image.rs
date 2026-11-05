use super::{Backend, queries::image as q, transactions};
use crate::{
    authorization::{Action, Permit, Principal},
    model::{Error, image_styles::*},
    processing::image::{Format, Pipeline},
};
use anyhow::{Result, ensure};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

type Tx<'a> = sqlx::Transaction<'a, q::Db>;
pub(super) async fn writable(
    t: &mut Tx<'_>,
    bucket: Uuid,
    key: &str,
    policy: Option<&str>,
) -> Result<()> {
    let Some(policy) = policy else {
        return Ok(());
    };
    let policy: Policy = serde_json::from_str(policy)?;
    let request = crate::processing::image::request::Request::default();
    let names: Vec<_> = request
        .candidates(key, &policy)
        .into_iter()
        .map(|(_, name, _)| name)
        .collect();
    if names.is_empty() {
        return Ok(());
    }
    let styles = named(t, bucket, &names).await?;
    ensure!(
        !styles.values().any(|s| s.enabled),
        s3s::s3_error!(
            InvalidArgument,
            "object key conflicts with an image style route"
        )
    );
    Ok(())
}
pub(super) async fn snapshot(
    b: &Backend,
    route: &crate::model::ReadRoute,
    access: Option<&str>,
) -> Result<Option<crate::model::ReadSnapshot>> {
    let initial = super::queries::read_snapshot(q::pool(b), route, access).await?;
    let Some(first) = initial.as_ref() else {
        return Ok(None);
    };
    let Some(request) = route
        .image()
        .filter(|r| access.is_none() && (first.bucket.image_revision > 0 || r.explicit()))
    else {
        return Ok(initial);
    };
    let mut tx = q::read(b).await?;
    let image = resolve(&mut tx, first.bucket.id, route.key(&first.bucket), request).await?;
    let selected = crate::model::ReadRoute::Bucket {
        name: first.bucket.name.clone(),
        key: image.key.clone(),
        alias: None,
    };
    let Some(mut snapshot) = super::queries::read_snapshot(&mut *tx, &selected, None).await? else {
        return Ok(None);
    };
    ensure!(
        snapshot.bucket.id == first.bucket.id
            && snapshot.bucket.image_revision == image.settings.revision,
        Error::OperationAborted
    );
    snapshot.image = Some(image);
    tx.commit().await?;
    Ok(Some(snapshot))
}
async fn permit(t: &mut Tx<'_>, principal: &Principal, bucket: Uuid) -> Result<()> {
    transactions::lock_permit(
        t,
        &Permit::for_action(principal.clone(), bucket, Action::Settings),
    )
    .await
}
async fn settings(t: &mut Tx<'_>, bucket: Uuid, lock: bool) -> Result<(Settings, i64)> {
    let (revision, body, families) = q::settings(t, bucket, lock)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    Ok((
        Settings {
            revision,
            policy: body
                .map(|s| serde_json::from_str(&s))
                .transpose()?
                .unwrap_or_default(),
        },
        families,
    ))
}
fn style((name, enabled, body): (String, bool, String)) -> Result<Style> {
    Ok(Style {
        name,
        enabled,
        rule: Pipeline::parse_native(&body)?,
    })
}
async fn named(t: &mut Tx<'_>, bucket: Uuid, names: &[String]) -> Result<BTreeMap<String, Style>> {
    let mut rows = BTreeMap::new();
    for chunk in names.chunks(PAGE) {
        for row in q::named(t, bucket, chunk).await? {
            let s = style(row)?;
            rows.insert(s.name.clone(), s);
        }
    }
    Ok(rows)
}
async fn page_rows(
    t: &mut Tx<'_>,
    bucket: Uuid,
    after: &str,
    prefix: &str,
    limit: u16,
) -> Result<Vec<Style>> {
    q::page(t, bucket, after, prefix, limit)
        .await?
        .into_iter()
        .map(style)
        .collect()
}
fn count_families(
    map: &Mapping,
    groups: &BTreeSet<String>,
    styles: &BTreeMap<String, Style>,
    validate: bool,
) -> Result<i64> {
    let mut count = 0;
    for family in groups {
        let Some(auto) = styles
            .get(&format!("{family}{}", map.auto))
            .filter(|s| s.enabled)
        else {
            continue;
        };
        ensure!(
            !validate || auto.rule.format() == Format::Auto,
            s3s::s3_error!(
                InvalidArgument,
                "mapped source {} must select auto",
                auto.name
            )
        );
        if auto.rule.format() != Format::Auto {
            continue;
        }
        if validate {
            for (n, format) in map.members(family) {
                ensure!(
                    styles
                        .get(&n)
                        .is_some_and(|s| s.enabled && s.rule.format() == format),
                    s3s::s3_error!(
                        InvalidArgument,
                        "missing or invalid mapped image style: {n}"
                    )
                );
            }
        }
        count += 1;
    }
    Ok(count)
}
fn members(map: &Mapping, groups: &BTreeSet<String>) -> Vec<String> {
    groups
        .iter()
        .flat_map(|f| map.members(f).map(|(n, _)| n))
        .collect()
}
async fn mapping_scan(
    backend: &Backend,
    principal: &Principal,
    bucket: Uuid,
    map: &Mapping,
    revision: i64,
) -> Result<i64> {
    let mut after = String::new();
    let mut total = 0;
    loop {
        let mut tx = q::read(backend).await?;
        permit(&mut tx, principal, bucket).await?;
        ensure!(
            settings(&mut tx, bucket, false).await?.0.revision == revision,
            Error::OperationAborted
        );
        let rows = page_rows(&mut tx, bucket, &after, "", PAGE as u16).await?;
        let groups = rows
            .iter()
            .filter(|s| s.enabled)
            .filter_map(|s| Mapping::family(&s.name, &map.auto).map(str::to_owned))
            .collect();
        let styles = named(&mut tx, bucket, &members(map, &groups)).await?;
        total += count_families(map, &groups, &styles, true)?;
        tx.commit().await?;
        if rows.len() < PAGE {
            break;
        }
        after = rows.last().unwrap().name.clone();
    }
    ensure!(
        total > 0,
        s3s::s3_error!(InvalidArgument, "no enabled auto style matches the mapping")
    );
    Ok(total)
}
#[async_trait::async_trait]
impl ImageStore for Backend {
    async fn resolve(
        &self,
        bucket: Uuid,
        key: &str,
        request: &crate::processing::image::request::Request,
    ) -> Result<crate::processing::image::request::Selection> {
        let mut tx = q::read(self).await?;
        let selected = resolve(&mut tx, bucket, key, request).await?;
        ensure!(
            settings(&mut tx, bucket, false).await?.0.revision == selected.settings.revision,
            Error::OperationAborted
        );
        tx.commit().await?;
        Ok(selected)
    }
    async fn page(
        &self,
        principal: &Principal,
        bucket: Uuid,
        after: &str,
        prefix: &str,
        limit: u16,
    ) -> Result<Page> {
        ensure!(
            (1..=PAGE as u16).contains(&limit)
                && after.len() <= 128
                && prefix.len() <= 128
                && prefix.is_ascii(),
            s3s::s3_error!(InvalidArgument, "invalid image page")
        );
        let mut tx = q::read(self).await?;
        permit(&mut tx, principal, bucket).await?;
        let (state, _) = settings(&mut tx, bucket, false).await?;
        let rows = page_rows(&mut tx, bucket, after, prefix, limit).await?;
        ensure!(
            settings(&mut tx, bucket, false).await?.0.revision == state.revision,
            Error::OperationAborted
        );
        tx.commit().await?;
        Ok(Page {
            settings: state,
            styles: rows,
        })
    }
    async fn selected(
        &self,
        principal: &Principal,
        bucket: Uuid,
        names: &[String],
    ) -> Result<Page> {
        ensure!(
            names.len() <= PAGE,
            s3s::s3_error!(InvalidArgument, "too many image styles")
        );
        let mut tx = q::read(self).await?;
        permit(&mut tx, principal, bucket).await?;
        let (state, _) = settings(&mut tx, bucket, false).await?;
        let rows = named(&mut tx, bucket, names).await?;
        ensure!(
            settings(&mut tx, bucket, false).await?.0.revision == state.revision,
            Error::OperationAborted
        );
        tx.commit().await?;
        Ok(Page {
            settings: state,
            styles: rows.into_values().collect(),
        })
    }
    async fn imported(
        &self,
        principal: &Principal,
        bucket: Uuid,
        digest: &str,
    ) -> Result<Option<i64>> {
        let mut tx = q::read(self).await?;
        permit(&mut tx, principal, bucket).await?;
        let rev = sqlx::query_scalar(q::IMPORTED)
            .bind(bucket)
            .bind(digest)
            .fetch_optional(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rev)
    }
    async fn update(
        &self,
        principal: &Principal,
        bucket: Uuid,
        update: &Update,
        import: Option<&str>,
        preview: bool,
    ) -> Result<Settings> {
        update
            .validate()
            .map_err(|e| s3s::s3_error!(InvalidArgument, "{e}"))?;
        let (old, _) = {
            let mut tx = q::read(self).await?;
            permit(&mut tx, principal, bucket).await?;
            let s = settings(&mut tx, bucket, false).await?;
            tx.commit().await?;
            s
        };
        ensure!(old.revision == update.revision, Error::OperationAborted);
        let policy = update.policy.as_ref().unwrap_or(&old.policy);
        let mapped = if policy.mapping != old.policy.mapping {
            Some(match &policy.mapping {
                Some(map) => mapping_scan(self, principal, bucket, map, old.revision).await?,
                None => 0,
            })
        } else {
            None
        };
        let mut tx = q::write(self).await?;
        transactions::lock_bucket_settings(&mut tx, principal, bucket).await?;
        let (current, prior) = settings(&mut tx, bucket, true).await?;
        ensure!(current.revision == update.revision, Error::OperationAborted);
        let mut families = mapped.unwrap_or(prior);
        let names: Vec<_> = update.edits.iter().map(|e| e.name().to_owned()).collect();
        let existing = named(&mut tx, bucket, &names).await?;
        if policy.enabled {
            let (added, kept): (Vec<_>, Vec<_>) = policy
                .delimiters
                .iter()
                .partition(|d| !current.policy.enabled || !current.policy.delimiters.contains(d));
            let added_styles: Vec<_> = update
                .edits
                .iter()
                .filter(|e| {
                    matches!(e, Edit::Put { style }
                if style.enabled && !existing.get(&style.name).is_some_and(|old| old.enabled))
                })
                .cloned()
                .collect();
            ensure!(
                !q::collision(&mut tx, bucket, &added, true, &update.edits).await?
                    && !q::collision(&mut tx, bucket, &kept, false, &added_styles).await?,
                s3s::s3_error!(
                    InvalidArgument,
                    "image style route conflicts with an existing object key"
                )
            );
        }
        if let Some(map) = &policy.mapping {
            let groups = names
                .iter()
                .filter_map(|n| map.related(n).map(str::to_owned))
                .collect();
            let mut rows = named(&mut tx, bucket, &members(map, &groups)).await?;
            let before = count_families(map, &groups, &rows, false)?;
            for e in &update.edits {
                match e {
                    Edit::Put { style } => {
                        rows.insert(style.name.clone(), style.clone());
                    }
                    Edit::Remove { name } => {
                        rows.remove(name);
                    }
                }
            }
            families += count_families(map, &groups, &rows, true)? - before;
            ensure!(
                families > 0,
                s3s::s3_error!(
                    InvalidArgument,
                    "at least one mapped auto style must remain enabled"
                )
            );
        }
        let mut modified = policy != &old.policy;
        for e in &update.edits {
            let n = e.name();
            let prev = existing.get(n);
            if let Edit::Put { style } = e {
                if prev == Some(style) {
                    continue;
                }
            }
            ensure!(
                prev.is_some() || !matches!(e, Edit::Remove { .. }),
                s3s::s3_error!(InvalidArgument, "image style does not exist: {n}")
            );
            if preview {
                continue;
            }
            if prev.is_some() {
                sqlx::query(q::DELETE)
                    .bind(bucket)
                    .bind(n)
                    .execute(&mut *tx)
                    .await?;
            }
            if let Edit::Put { style } = e {
                sqlx::query(q::INSERT)
                    .bind(bucket)
                    .bind(n)
                    .bind(style.enabled)
                    .bind(style.rule.text())
                    .execute(&mut *tx)
                    .await?;
            }
            modified = true;
        }
        let revision = if modified && !preview {
            let next = old.revision.checked_add(1).ok_or(Error::OperationAborted)?;
            let updated = sqlx::query(q::UPDATE)
                .bind(next)
                .bind(families)
                .bind(serde_json::to_string(policy)?)
                .bind(import)
                .bind(bucket)
                .bind(old.revision)
                .execute(&mut *tx)
                .await?;
            ensure!(updated.rows_affected() == 1, Error::OperationAborted);
            transactions::checkpoint_event(
                &mut tx,
                "bucket.images",
                &bucket.to_string(),
                serde_json::json!({"bucket_id":bucket,"revision":next}),
            )
            .await?;
            next
        } else {
            current.revision
        };
        tx.commit().await?;
        Ok(Settings {
            revision,
            policy: policy.clone(),
        })
    }
}

pub(super) async fn resolve(
    t: &mut Tx<'_>,
    bucket: Uuid,
    key: &str,
    request: &crate::processing::image::request::Request,
) -> Result<crate::processing::image::request::Selection> {
    let (settings, _) = settings(t, bucket, false).await?;
    let names = request
        .candidates(key, &settings.policy)
        .into_iter()
        .map(|(_, name, _)| name)
        .collect::<Vec<_>>();
    let styles = named(t, bucket, &names).await?;
    request.select(key, settings, &styles)
}
