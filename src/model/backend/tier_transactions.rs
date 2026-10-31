use super::tier_queries as q;
use super::{Backend, queries, redundancy_queries, replication_queries, transactions};
use crate::model::tiering::Unit;
use crate::{
    authorization::{Action, Permit, Principal},
    model::{
        Error,
        redundancy::{Location, Placement, Space},
        tiering::{Locations, TieringStore, Update, View},
    },
};
use anyhow::Result;
use anyhow::ensure;
use futures_util::TryStreamExt;
use sqlx::types::Json;
use std::collections::BTreeMap;
use uuid::Uuid;

async fn hot(db: &mut q::Tx<'_>, principal: &Principal, bucket: Uuid) -> Result<Uuid> {
    transactions::lock_permit(
        db,
        &Permit::for_action(principal.clone(), bucket, Action::Settings),
    )
    .await?;
    Ok(replication_queries::profile(bucket)
        .fetch_optional(&mut **db)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?
        .0)
}

async fn locations(
    db: &mut q::Tx<'_>,
    principal: &Principal,
    bucket: Uuid,
    cold: Uuid,
) -> Result<Locations> {
    let home = hot(db, principal, bucket).await?;
    ensure!(
        home != cold,
        s3s::s3_error!(InvalidArgument, "cold storage must differ from hot storage")
    );
    async fn read(db: &mut q::Tx<'_>, id: Uuid) -> Result<(Uuid, i64, Location)> {
        let row = queries::enabled_storage(id)
            .fetch_optional(&mut **db)
            .await?
            .ok_or_else(|| {
                s3s::s3_error!(InvalidArgument, "an enabled internal storage is required")
            })?;
        Ok((id, row.revision, Location::storage(&row)?))
    }
    Ok(Locations {
        hot: read(db, home).await?,
        cold: read(db, cold).await?,
    })
}

async fn view(db: &mut q::Tx<'_>, bucket: Uuid, hot_storage: Uuid) -> Result<View> {
    let row = q::policy(bucket).fetch_optional(&mut **db).await?;
    let (policy, revision, state) = if let Some((p, r, s)) = row {
        (Some(p.0), r, s)
    } else {
        let mut query =
            sqlx::QueryBuilder::<q::Db>::new("SELECT revision FROM tier_scans WHERE bucket_id=");
        (
            None,
            query
                .push_bind(bucket)
                .build_query_scalar::<i64>()
                .fetch_optional(&mut **db)
                .await?
                .unwrap_or_default(),
            "disabled".into(),
        )
    };
    Ok(View {
        bucket,
        hot_storage,
        policy,
        revision,
        state,
    })
}

async fn independent(db: &mut q::Tx<'_>, bucket: Uuid, space: &Space) -> Result<()> {
    let mut rows = q::family(bucket).fetch(&mut **db);
    while let Some((domain, path)) = rows.try_next().await? {
        ensure!(
            space.independent(&domain, &path),
            s3s::s3_error!(InvalidArgument, "cold storage overlaps a redundant copy")
        );
    }
    Ok(())
}

async fn cold_dependencies(
    db: &mut q::Tx<'_>,
    bucket: Uuid,
    home: Uuid,
    cold: Uuid,
) -> Result<bool> {
    let mut moves =
        sqlx::QueryBuilder::<q::Db>::new("SELECT EXISTS(SELECT 1 FROM tier_moves WHERE bucket_id=");
    if moves
        .push_bind(bucket)
        .push(")")
        .build_query_scalar::<bool>()
        .fetch_one(&mut **db)
        .await?
    {
        return Ok(true);
    }
    for (unit, refs) in [
        (
            Unit::Chunk,
            "JOIN chunks c ON c.current_encoding_id=u.encoding_id JOIN extents e ON e.chunk_id=c.id",
        ),
        (
            Unit::Pack,
            "JOIN pack_members m ON m.pack_id=u.id JOIN chunks c ON c.id=m.chunk_id AND c.pack_id=u.id JOIN extents e ON e.chunk_id=c.id",
        ),
        (Unit::Raw, "JOIN raw_ranges e ON e.source_id=u.id"),
    ] {
        let (table, column) = q::physical(unit);
        let mut query = sqlx::QueryBuilder::<q::Db>::new(format!(
            "SELECT EXISTS(SELECT 1 FROM tier_locations h JOIN {table} u ON u.{column}=h.physical_id {refs} JOIN objects o ON o.stream_id=e.stream_id WHERE h.unit="
        ));
        if query
            .push_bind(unit as i16)
            .push(" AND h.home_storage=")
            .push_bind(home)
            .push(" AND h.storage_id=")
            .push_bind(cold)
            .push(" AND o.bucket_id=")
            .push_bind(bucket)
            .push(")")
            .build_query_scalar::<bool>()
            .fetch_one(&mut **db)
            .await?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) async fn check_link(
    db: &mut q::Tx<'_>,
    source: Uuid,
    target: Option<Uuid>,
    placement: &Placement,
) -> Result<()> {
    if let Some((domain, path)) = q::cold(source).fetch_optional(&mut **db).await? {
        ensure!(
            placement.target.independent(&domain, &path),
            s3s::s3_error!(InvalidArgument, "redundancy target overlaps cold storage")
        );
    }
    // A new sibling must remain independent of existing siblings' cold locations.
    if let Some(bucket) = target {
        if let Some((domain, path)) = q::cold(bucket).fetch_optional(&mut **db).await? {
            let space = Space { domain, path };
            ensure!(
                placement.source.independent(&space.domain, &space.path),
                s3s::s3_error!(InvalidArgument, "redundancy source overlaps cold storage")
            );
            if let Some((domain, path)) = q::cold(source).fetch_optional(&mut **db).await? {
                ensure!(
                    space.independent(&domain, &path),
                    s3s::s3_error!(InvalidArgument, "redundant cold locations overlap")
                );
            }
            independent(db, source, &space).await?;
        }
    }
    let mut rows = q::siblings(source).fetch(&mut **db);
    while let Some((domain, path)) = rows.try_next().await? {
        ensure!(
            placement.target.independent(&domain, &path),
            s3s::s3_error!(InvalidArgument, "redundancy target overlaps cold storage")
        );
    }
    Ok(())
}

#[async_trait::async_trait]
impl TieringStore for Backend {
    async fn transition(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: &crate::model::tiering::Change,
    ) -> Result<View> {
        use crate::model::tiering::Transition;
        let mut db = q::begin(self).await?;
        redundancy_queries::lock_catalog().execute(&mut *db).await?;
        let home = hot(&mut db, principal, bucket).await?;
        let mut result = view(&mut db, bucket, home).await?;
        ensure!(result.revision == input.revision, Error::OperationAborted);
        let policy = result
            .policy
            .as_ref()
            .ok_or_else(|| s3s::s3_error!(InvalidArgument, "tiering is disabled"))?;
        let state = match input.action {
            Transition::Pause => "paused",
            Transition::Resume => "active",
            Transition::Drain => "draining",
            Transition::Disable => "disabled",
        };
        if result.state != state {
            let revision = result
                .revision
                .checked_add(1)
                .ok_or(Error::OperationAborted)?;
            if state == "disabled" {
                ensure!(
                    !cold_dependencies(&mut db, bucket, home, policy.cold_storage).await?,
                    s3s::s3_error!(InvalidArgument, "return cold data before disabling tiering")
                );
                let mut query =
                    sqlx::QueryBuilder::<q::Db>::new("DELETE FROM bucket_tiering WHERE bucket_id=");
                ensure!(
                    query
                        .push_bind(bucket)
                        .push(" AND revision=")
                        .push_bind(result.revision)
                        .build()
                        .execute(&mut *db)
                        .await?
                        .rows_affected()
                        == 1,
                    Error::OperationAborted
                );
                result.policy = None;
            } else {
                let mut query =
                    sqlx::QueryBuilder::<q::Db>::new("UPDATE bucket_tiering SET state=");
                ensure!(
                    query
                        .push_bind(state)
                        .push(",revision=revision+1 WHERE bucket_id=")
                        .push_bind(bucket)
                        .push(" AND revision=")
                        .push_bind(result.revision)
                        .build()
                        .execute(&mut *db)
                        .await?
                        .rows_affected()
                        == 1,
                    Error::OperationAborted
                );
            }
            sqlx::query(q::CURSOR)
                .bind(bucket)
                .bind(revision)
                .bind(0i16)
                .bind(Uuid::nil())
                .bind(0i64)
                .execute(&mut *db)
                .await?;
            result.state = state.into();
            result.revision = revision;
        }
        db.commit().await?;
        Ok(result)
    }
    async fn schedule(&self, stale: i64) -> Result<usize> {
        super::tier_move_transactions::schedule(self, stale).await
    }
    async fn step(
        &self,
        task: crate::model::task::Execution,
        stale: i64,
        busy: Option<Uuid>,
    ) -> Result<crate::model::tiering::Step> {
        let mut db = q::begin(self).await?;
        let step = super::tier_move_transactions::step(&mut db, task, stale, busy).await?;
        db.commit().await?;
        Ok(step)
    }
    async fn finish(
        &self,
        task: crate::model::task::Execution,
        outcome: &crate::model::tiering::Outcome,
    ) -> Result<()> {
        let mut db = q::begin(self).await?;
        super::tier_move_transactions::finish(&mut db, task, outcome).await?;
        db.commit().await?;
        Ok(())
    }
    async fn active(&self) -> Result<bool> {
        q::active(self).await
    }
    async fn candidates(
        &self,
        principal: &Principal,
        bucket: Uuid,
        unit: Unit,
        after: Uuid,
        limit: u16,
        stale: i64,
    ) -> Result<Vec<crate::model::tiering::Candidate>> {
        use crate::model::tiering::{Reference, candidate};
        ensure!(
            (1..=128).contains(&limit) && stale > 0,
            s3s::s3_error!(InvalidArgument)
        );
        let mut db = q::begin(self).await?;
        hot(&mut db, principal, bucket).await?;
        let (ids, rows, clock) =
            super::tier_read_queries::references(&mut db, bucket, unit, after, limit, false)
                .await?;
        let limited = rows.len() > 1024;
        let mut groups = BTreeMap::<Uuid, Vec<&Reference>>::new();
        for row in &rows {
            groups.entry(row.physical).or_default().push(row);
        }
        let result = ids
            .into_iter()
            .map(|id| {
                let rows = groups.get(&id).map_or(&[][..], Vec::as_slice);
                candidate(
                    unit,
                    id,
                    rows,
                    clock,
                    stale,
                    limited || !rows.iter().any(|r| r.owner == bucket && r.visible),
                )
            })
            .collect();
        db.commit().await?;
        Ok(result)
    }
    async fn tiering(&self, principal: &Principal, bucket: Uuid) -> Result<View> {
        let mut db = q::begin(self).await?;
        let storage = hot(&mut db, principal, bucket).await?;
        let result = view(&mut db, bucket, storage).await?;
        db.commit().await?;
        Ok(result)
    }
    async fn locations(
        &self,
        principal: &Principal,
        bucket: Uuid,
        cold: Uuid,
    ) -> Result<Locations> {
        let mut db = q::begin(self).await?;
        let result = locations(&mut db, principal, bucket, cold).await?;
        db.commit().await?;
        Ok(result)
    }
    async fn update(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: &Update,
        checked: &Locations,
        placement: &Placement,
        preview: bool,
    ) -> Result<View> {
        input.policy.validate()?;
        ensure!(input.revision >= 0, s3s::s3_error!(InvalidArgument));
        let mut db = q::begin(self).await?;
        redundancy_queries::lock_catalog().execute(&mut *db).await?;
        ensure!(
            *checked == locations(&mut db, principal, bucket, input.policy.cold_storage).await?,
            Error::OperationAborted
        );
        placement.validate()?;
        independent(&mut db, bucket, &placement.target).await?;
        let mut current = view(&mut db, bucket, checked.hot.0).await?;
        ensure!(current.revision == input.revision, Error::OperationAborted);
        if current.policy.as_ref() != Some(&input.policy) {
            if current
                .policy
                .as_ref()
                .is_some_and(|p| p.cold_storage != input.policy.cold_storage)
            {
                ensure!(
                    !cold_dependencies(
                        &mut db,
                        bucket,
                        checked.hot.0,
                        current.policy.as_ref().unwrap().cold_storage
                    )
                    .await?,
                    s3s::s3_error!(
                        InvalidArgument,
                        "return cold data before changing the backend"
                    )
                );
            }
            if !preview {
                let changed = if current.policy.is_none() {
                    sqlx::query(q::INSERT_POLICY)
                        .bind(bucket)
                        .bind(checked.hot.0)
                        .bind(checked.cold.0)
                        .bind(&placement.target.domain)
                        .bind(&placement.target.path)
                        .bind(Json(&input.policy))
                        .bind(
                            current
                                .revision
                                .checked_add(1)
                                .ok_or(Error::OperationAborted)?,
                        )
                        .execute(&mut *db)
                        .await?
                } else {
                    sqlx::query(q::UPDATE_POLICY)
                        .bind(checked.cold.0)
                        .bind(&placement.target.domain)
                        .bind(&placement.target.path)
                        .bind(Json(&input.policy))
                        .bind(bucket)
                        .bind(input.revision)
                        .execute(&mut *db)
                        .await?
                };
                ensure!(changed.rows_affected() == 1, Error::OperationAborted);
                current.revision += 1;
            }
            if current.state == "disabled" {
                current.state = "active".into();
            }
            current.policy = Some(input.policy.clone());
        }
        db.commit().await?;
        Ok(current)
    }
}
