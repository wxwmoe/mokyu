use super::{Backend, image_queries as q, transactions};
use crate::{
    authorization::{Action, Permit, Principal},
    model::{Error, image_styles::*},
    processing::image::{Format, Pipeline},
};
use anyhow::{Result, ensure};
use sqlx::QueryBuilder;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

type Tx<'a> = sqlx::Transaction<'a, q::Db>;
async fn permit(t: &mut Tx<'_>, principal: &Principal, bucket: Uuid) -> Result<()> {
    transactions::lock_permit(
        t,
        &Permit::for_action(principal.clone(), bucket, Action::Settings),
    )
    .await
}
async fn settings(t: &mut Tx<'_>, bucket: Uuid, lock: bool) -> Result<(Settings, i64)> {
    let mut query = QueryBuilder::<q::Db>::new(
        "SELECT image_revision,image_policy,image_families FROM buckets WHERE id=",
    );
    query.push_bind(bucket);
    if lock {
        query.push(q::LOCK);
    }
    let (revision, body, families) = query
        .build_query_as::<(i64, Option<String>, i64)>()
        .fetch_optional(&mut **t)
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
        let mut query = QueryBuilder::<q::Db>::new(
            "SELECT name,enabled,body FROM image_styles WHERE bucket_id=",
        );
        query.push_bind(bucket).push(" AND name IN (");
        let mut params = query.separated(",");
        for n in chunk {
            params.push_bind(n);
        }
        query.push(") ORDER BY name");
        for row in query.build_query_as().fetch_all(&mut **t).await? {
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
    let mut query =
        QueryBuilder::<q::Db>::new("SELECT name,enabled,body FROM image_styles WHERE bucket_id=");
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
        .push_bind(i64::from(limit));
    query
        .build_query_as()
        .fetch_all(&mut **t)
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
        let mut q = QueryBuilder::<q::Db>::new("SELECT image_revision FROM buckets WHERE id=");
        let rev = q
            .push_bind(bucket)
            .push(" AND image_import_token=")
            .push_bind(digest)
            .build_query_scalar()
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
                let mut q = QueryBuilder::<q::Db>::new("DELETE FROM image_styles WHERE bucket_id=");
                q.push_bind(bucket)
                    .push(" AND name=")
                    .push_bind(n)
                    .build()
                    .execute(&mut *tx)
                    .await?;
            }
            if let Edit::Put { style } = e {
                let mut q = QueryBuilder::<q::Db>::new(
                    "INSERT INTO image_styles(bucket_id,name,enabled,body) VALUES(",
                );
                q.push_bind(bucket)
                    .push(",")
                    .push_bind(n)
                    .push(",")
                    .push_bind(style.enabled)
                    .push(",")
                    .push_bind(style.rule.text())
                    .push(")")
                    .build()
                    .execute(&mut *tx)
                    .await?;
            }
            modified = true;
        }
        let revision = if modified && !preview {
            let next = old.revision.checked_add(1).ok_or(Error::OperationAborted)?;
            let mut q = QueryBuilder::<q::Db>::new("UPDATE buckets SET image_revision=");
            q.push_bind(next)
                .push(",image_families=")
                .push_bind(families)
                .push(",image_policy=")
                .push_bind(serde_json::to_string(policy)?)
                .push(",image_import_token=")
                .push_bind(import)
                .push(" WHERE id=")
                .push_bind(bucket)
                .push(" AND image_revision=")
                .push_bind(old.revision);
            ensure!(
                q.build().execute(&mut *tx).await?.rows_affected() == 1,
                Error::OperationAborted
            );
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
