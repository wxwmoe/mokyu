use super::{
    Backend, queries, queries::redundancy as redundancy_queries,
    queries::replication as replication_queries, queries::tier as q,
};
use crate::model::{
    Error, PhysicalSource,
    source::{SourceScope, SourceSnapshot},
    task::Execution,
    tiering::{Head, Movement, Outcome, Policy, Reference, Step, Unit, Work, candidate},
};
use anyhow::{Context, Result, ensure};
use serde_json::json;
use sqlx::types::Json;
use uuid::Uuid;

type Due = (
    Uuid,
    Uuid,
    Uuid,
    i64,
    String,
    Json<Policy>,
    Option<i16>,
    Option<Uuid>,
);
type Destination = (String, Option<Uuid>, Option<String>, bool, String, i64);

async fn head(db: &mut q::Tx<'_>, unit: Unit, id: Uuid) -> Result<Option<Head>> {
    Ok(sqlx::query_as(q::HEAD)
        .bind(unit as i16)
        .bind(id)
        .fetch_optional(&mut **db)
        .await?)
}

async fn snapshot(
    db: &mut q::Tx<'_>,
    unit: Unit,
    id: Uuid,
    home: Uuid,
) -> Result<Option<SourceSnapshot>> {
    let Some(native) = q::identity(db, unit, id, home).await? else {
        return Ok(None);
    };
    let scope = SourceScope {
        storage_id: home,
        source: unit.source()?,
        id: native.to_string(),
    };
    Ok(Some(
        queries::source_snapshot(&mut **db, &scope, true).await?,
    ))
}

fn destination(
    state: &str,
    policy: &Policy,
    home: Uuid,
    head: Option<&Head>,
    rows: &[&Reference],
    reason: &str,
    clock: (i64, i64, i64),
) -> Option<Uuid> {
    if state == "paused" {
        return None;
    }
    if let Some(head) = head.filter(|h| h.storage_id != home) {
        if rows.is_empty() || rows.iter().any(|r| !r.ready || !r.visible) {
            return None;
        }
        if state == "draining" || reason == "shared_policy" {
            return Some(home);
        }
        if state != "active" {
            return None;
        }
        return rows
            .iter()
            .filter_map(|r| r.policy.as_ref())
            .any(|p| {
                clock.0 - head.activated_at >= i64::from(p.residency_seconds)
                    && head.origin_reads >= i64::from(p.promote_reads)
                    && head.pressure_since > 0
                    && clock.0 - head.pressure_since < i64::from(p.observation_seconds)
            })
            .then_some(home);
    }
    (state == "active"
        && reason == "eligible"
        && head.is_none_or(|h| clock.0 - h.activated_at >= i64::from(policy.residency_seconds)))
    .then_some(policy.cold_storage)
}

fn stamp(s: &SourceSnapshot, target: &Destination) -> Result<Vec<u8>> {
    Ok(
        blake3::hash(&serde_json::to_vec(&(s.fingerprint()?, target))?)
            .as_bytes()
            .to_vec(),
    )
}

async fn pack_index(
    db: &mut q::Tx<'_>,
    pack: &crate::content::pack::Pack,
) -> Result<crate::content::pack::TierIndex> {
    if let Some(index) = &pack.tier_index {
        return Ok(*index.clone());
    }
    let entries = queries::pack_work::physical_members(db, pack).await?;
    crate::model::source::manifest_digest(pack, &entries)?;
    Ok(crate::content::pack::TierIndex {
        home_digest: pack.digest.clone().context("missing pack digest")?,
        entries,
    })
}

async fn template(
    db: &mut q::Tx<'_>,
    s: &PhysicalSource,
    storage: Uuid,
    addr: Uuid,
    cfg: &Destination,
) -> Result<Option<PhysicalSource>> {
    let mut out = s.clone();
    out.relocate(storage, addr, cfg.0 == "local");
    let alg = cfg.2.as_deref().unwrap_or("none");
    ensure!(cfg.1.is_some() == cfg.2.is_some(), Error::SourceUnavailable);
    if matches!(s, PhysicalSource::Raw(_)) {
        return Ok(cfg.1.is_none().then_some(out));
    }
    let allow_compression = cfg.3 && cfg.4 != "never";
    if let PhysicalSource::Pack(p) = &mut out {
        let mut index = pack_index(db, p).await?;
        if index.entries.iter().any(|e| {
            let c = &e.position.encoding;
            c.block_key_id != cfg.1 || c.algorithm != alg || c.compressed && !allow_compression
        }) {
            let first = if let Some(key) = cfg.1 {
                let tag = crate::content::nonce::CHUNK;
                let previous = queries::nonce_counter(key, tag)
                    .fetch_one(&mut **db)
                    .await?;
                let units = u32::try_from(
                    index
                        .entries
                        .len()
                        .div_ceil(crate::content::nonce::UNIT as usize),
                )?;
                let (first, last) = crate::model::key::nonce_reservation(previous, units)?;
                ensure!(
                    queries::advance_nonce(key, tag, previous, last)
                        .execute(&mut **db)
                        .await?
                        .rows_affected()
                        == 1,
                    Error::OperationAborted
                );
                Some(first)
            } else {
                None
            };
            index.entries = crate::content::pack::frame::rewrite_entries(
                &index.entries,
                alg,
                cfg.1,
                p.created_at,
                first,
                allow_compression,
            )?;
        }
        p.tier_index = Some(Box::new(index));
        return Ok(Some(out));
    }
    let PhysicalSource::Chunk(c) = &mut out else {
        unreachable!()
    };
    let (key, algorithm, nonce, compressed, date, tag) = (
        &mut c.block_key_id,
        &mut c.algorithm,
        &mut c.nonce,
        &mut c.compressed,
        c.created_at,
        crate::content::nonce::CHUNK,
    );

    if *key != cfg.1 || algorithm != alg || *compressed && !allow_compression {
        *key = cfg.1;
        *algorithm = alg.into();
        *compressed &= allow_compression;
        *nonce = if let Some(key) = *key {
            let prev = queries::nonce_counter(key, tag)
                .fetch_one(&mut **db)
                .await?;
            let (first, last) = crate::model::key::nonce_reservation(prev, 1)?;
            ensure!(
                queries::advance_nonce(key, tag, prev, last)
                    .execute(&mut **db)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
            Some(crate::content::codec::typed_nonce(date, first, tag)?.to_vec())
        } else {
            None
        };
    }
    Ok(Some(out))
}

fn recode(a: &PhysicalSource, b: &PhysicalSource) -> bool {
    match (a, b) {
        (PhysicalSource::Chunk(a), PhysicalSource::Chunk(b)) => {
            a.algorithm != b.algorithm
                || a.block_key_id != b.block_key_id
                || a.nonce != b.nonce
                || a.compressed != b.compressed
        }
        (PhysicalSource::Pack(a), PhysicalSource::Pack(b)) => {
            a.tier_index.as_ref().map(|i| &i.entries) != b.tier_index.as_ref().map(|i| &i.entries)
        }
        _ => false,
    }
}

pub(super) async fn schedule(backend: &Backend, stale: i64) -> Result<usize> {
    let mut db = q::begin(backend).await?;
    let now = sqlx::query_scalar::<_, i64>(q::CLOCK)
        .fetch_one(&mut *db)
        .await?;
    let due = sqlx::query_as::<_, Due>(q::DUE)
        .bind(now)
        .fetch_optional(&mut *db)
        .await?;
    if due.is_none() {
        return Ok(0);
    }
    redundancy_queries::lock_catalog().execute(&mut *db).await?;
    let Some((bucket, home, _, revision, state, policy, category, after)) =
        sqlx::query_as::<_, Due>(q::DUE)
            .bind(now)
            .fetch_optional(&mut *db)
            .await?
    else {
        return Ok(0);
    };
    let category = category.unwrap_or(0);
    let unit = Unit::movable(category)?;
    let woken = sqlx::query(q::WAKE)
        .bind(bucket)
        .bind(now)
        .execute(&mut *db)
        .await?
        .rows_affected();
    let in_progress = sqlx::query_scalar::<_, i64>(q::RUNNING)
        .bind(bucket)
        .fetch_one(&mut *db)
        .await?;
    let backlog = sqlx::query_scalar::<_, i64>(q::BACKLOG)
        .bind(bucket)
        .fetch_one(&mut *db)
        .await?;
    let slots = i64::from(policy.parallelism) - in_progress;
    let (mut next, mut cursor, mut delay) = (category, after.unwrap_or_default(), 1);
    let mut scheduled = 0;
    if state == "paused" {
        delay = 5;
    } else if slots > 0 && backlog < 16 * i64::from(policy.parallelism) {
        let (ids, owners, clock) = q::references(&mut db, bucket, unit, cursor, 16, false).await?;
        for id in ids.iter().copied() {
            cursor = id;
            if sqlx::query_scalar::<_, Uuid>(q::HAS_MOVEMENT)
                .bind(category)
                .bind(id)
                .fetch_optional(&mut *db)
                .await?
                .is_some()
            {
                continue;
            }
            let rows: Vec<_> = owners.iter().filter(|r| r.physical == id).collect();
            let current = head(&mut db, unit, id).await?;
            let reason = candidate(
                unit,
                id,
                &rows,
                clock,
                stale,
                owners.len() > 1024 || !rows.iter().any(|r| r.owner == bucket && r.visible),
            )
            .reason;
            let Some(target) = destination(
                &state,
                &policy,
                home,
                current.as_ref(),
                &rows,
                &reason,
                clock,
            ) else {
                continue;
            };
            let Some(source) = snapshot(&mut db, unit, id, home).await? else {
                continue;
            };
            if source.state != "ready" || source.quarantine_code.is_some() {
                continue;
            }
            let cfg: Destination = sqlx::query_as(q::TARGET)
                .bind(target)
                .fetch_one(&mut *db)
                .await?;
            let address = Uuid::now_v7();
            let Some(to) = template(&mut db, &source.physical, target, address, &cfg).await? else {
                continue;
            };
            let task = Uuid::now_v7();
            let from = source.physical.storage()?;
            let detail = json!({"operation":"tiering","storage_id":target,"source_storage":from,"physical_id":id,"unit":unit,"stale_seconds":stale});
            queries::insert_task(task, "storage_migration", Some(bucket), detail, None)
                .execute(&mut *db)
                .await?;
            sqlx::query(q::PREPARE)
                .bind(task)
                .bind(bucket)
                .bind(category)
                .bind(id)
                .bind(revision)
                .bind(current.map_or(0, |h| h.generation))
                .bind(from)
                .bind(source.physical.address())
                .bind(Json(&source.physical))
                .bind(stamp(&source, &cfg)?)
                .bind(source.physical.block_key())
                .bind(source.physical.bytes()?)
                .bind(target)
                .bind(address)
                .bind(Json(&to))
                .bind(to.block_key())
                .bind(now)
                .execute(&mut *db)
                .await?;
            scheduled += 1;
            if i64::from(scheduled) >= slots {
                break;
            }
        }
        if ids.len() < 16 && ids.last().is_none_or(|id| *id == cursor) {
            next = (category + 1) % 3;
            cursor = Uuid::nil();
            delay = if next == 0 { 5 } else { 0 };
        }
    }
    sqlx::query(q::CURSOR)
        .bind(bucket)
        .bind(revision)
        .bind(next)
        .bind(cursor)
        .bind(now + delay)
        .execute(&mut *db)
        .await?;
    db.commit().await?;
    Ok(scheduled as usize + woken as usize)
}

async fn owned(db: &mut q::Tx<'_>, task: Execution) -> Result<Movement> {
    replication_queries::owned(task)
        .fetch_optional(&mut **db)
        .await?
        .ok_or(Error::OperationAborted)?;
    Ok(sqlx::query_as(q::MOVEMENT)
        .bind(task.id)
        .fetch_optional(&mut **db)
        .await?
        .ok_or(Error::OperationAborted)?)
}

async fn phase(db: &mut q::Tx<'_>, id: Uuid, phase: &str, after: i64) -> Result<()> {
    ensure!(
        sqlx::query(q::RETIRE)
            .bind(phase)
            .bind(after)
            .bind(id)
            .execute(&mut **db)
            .await?
            .rows_affected()
            == 1,
        Error::OperationAborted
    );
    Ok(())
}

async fn retention(db: &mut q::Tx<'_>, storage: Uuid, start: i64, now: i64) -> Result<i64> {
    let minimum = queries::storage_profile(storage)
        .fetch_one(&mut **db)
        .await?
        .min_storage_seconds()?;
    Ok((now + crate::model::node_read::READ_SECONDS)
        .max(start.saturating_add(i64::try_from(minimum)?)))
}

pub(super) async fn step(
    db: &mut q::Tx<'_>,
    task: Execution,
    stale: i64,
    busy: Option<Uuid>,
) -> Result<Step> {
    redundancy_queries::lock_catalog()
        .execute(&mut **db)
        .await?;
    let movement = owned(db, task).await?;
    let now = sqlx::query_scalar::<_, i64>(q::CLOCK)
        .fetch_one(&mut **db)
        .await?;
    let unit = Unit::movable(movement.unit)?;
    let idle = Step {
        work: None,
        switched: false,
    };
    let available: i64 = sqlx::query_scalar(q::S3_PAIR)
        .bind(movement.source_material.storage()?)
        .bind(movement.target_storage)
        .fetch_one(&mut **db)
        .await?;
    if available != 2 {
        sqlx::query(q::PARK)
            .bind(task.id)
            .execute(&mut **db)
            .await?;
        return Ok(idle);
    }
    let target = movement
        .target_material
        .as_ref()
        .context("tier move has no target")?;
    let source = &*movement.source_material;
    let current = head(db, unit, movement.physical_id).await?;
    if matches!(movement.phase.as_str(), "retiring" | "discarding") {
        let old = if movement.phase == "retiring" {
            source
        } else {
            &target.0
        };
        ensure!(
            current.as_ref().is_none_or(|h| h.address != old.address())
                && !(current.is_none() && old.address() == movement.physical_id),
            Error::OperationAborted
        );
        if now < movement.retire_after
            || busy == Some(old.address())
            || q::protected(&mut **db, old.address()).await?
            || movement.phase == "retiring" && q::pinned(db, unit, movement.physical_id).await?
        {
            if now >= movement.retire_after {
                phase(db, task.id, &movement.phase, now + 5).await?;
            }
            sqlx::query(q::PARK)
                .bind(task.id)
                .execute(&mut **db)
                .await?;
            return Ok(idle);
        }
        return Ok(Step {
            work: Some(Work::Remove {
                source: old.clone(),
            }),
            switched: false,
        });
    }
    let (policy, revision, state) = q::policy(movement.bucket_id)
        .fetch_optional(&mut **db)
        .await?
        .ok_or(Error::OperationAborted)?;
    let home = replication_queries::profile(movement.bucket_id)
        .fetch_one(&mut **db)
        .await?
        .0;
    let cfg: Option<Destination> = sqlx::query_as(q::TARGET)
        .bind(movement.target_storage)
        .fetch_optional(&mut **db)
        .await?;
    let snapshot = snapshot(db, unit, movement.physical_id, home).await?;
    let valid = if let (Some(cfg), Some(snapshot)) = (&cfg, &snapshot) {
        revision == movement.policy_revision
            && current.as_ref().map_or(0, |h| h.generation) == movement.source_generation
            && snapshot.state == "ready"
            && snapshot.quarantine_code.is_none()
            && stamp(snapshot, cfg)? == movement.source_stamp
    } else {
        false
    };
    let wanted = if valid {
        let (_, refs, clock) =
            q::references(db, movement.bucket_id, unit, movement.physical_id, 1, true).await?;
        let rows: Vec<_> = refs.iter().collect();
        let reason = candidate(
            unit,
            movement.physical_id,
            &rows,
            clock,
            stale,
            refs.len() > 1024
                || !rows
                    .iter()
                    .any(|r| r.owner == movement.bucket_id && r.visible),
        )
        .reason;
        if state == "active"
            && movement.target_storage != home
            && matches!(reason.as_str(), "observation_gap" | "source_busy")
        {
            // A coverage gap after restart cannot authorize a switch or invalidate verified bytes.
            phase(db, task.id, &movement.phase, now + 5).await?;
            sqlx::query(q::PARK)
                .bind(task.id)
                .execute(&mut **db)
                .await?;
            return Ok(idle);
        }
        destination(
            &state,
            &policy,
            home,
            current.as_ref(),
            &rows,
            &reason,
            clock,
        )
    } else {
        None
    };
    if wanted != Some(movement.target_storage) {
        let after = retention(db, movement.target_storage, now, now).await?;
        phase(db, task.id, "discarding", after).await?;
        return Ok(idle);
    }
    if movement.phase == "copying" {
        let mut source = (*source).clone();
        if let PhysicalSource::Pack(pack) = &mut source {
            pack.tier_index = Some(Box::new(pack_index(db, pack).await?));
        }
        return Ok(Step {
            work: Some(Work::Copy {
                source: source.clone(),
                target: target.0.clone(),
                rate: (policy.bytes_per_second / u64::from(policy.parallelism)).max(1),
                recode: recode(&source, target),
                progress: movement.checkpoint.map(|p| p.0).unwrap_or_default(),
            }),
            switched: false,
        });
    }
    ensure!(
        movement.phase == "verified" && movement.receipt.as_ref().is_some_and(|v| v.len() == 32),
        Error::OperationAborted
    );
    let affected = if current.is_none() {
        sqlx::query(q::INITIAL)
            .bind(movement.unit)
            .bind(movement.physical_id)
            .bind(home)
            .bind(movement.target_storage)
            .bind(movement.target_address)
            .bind(target)
            .bind(target.block_key())
            .bind(target.bytes()?)
            .bind(now)
            .bind(i64::from(policy.observation_seconds))
            .execute(&mut **db)
            .await?
    } else {
        sqlx::query(q::SWITCH)
            .bind(movement.target_storage)
            .bind(movement.target_address)
            .bind(target)
            .bind(target.block_key())
            .bind(target.bytes()?)
            .bind(now)
            .bind(i64::from(policy.observation_seconds))
            .bind(movement.unit)
            .bind(movement.physical_id)
            .bind(movement.source_generation)
            .bind(source.address())
            .execute(&mut **db)
            .await?
    };
    ensure!(affected.rows_affected() == 1, Error::OperationAborted);
    // An original with no confirmed placement time gets the full minimum retention.
    let placed = if let Some(h) = &current {
        h.activated_at
    } else if unit == Unit::Raw {
        now
    } else {
        q::stored_at(db, unit, movement.physical_id)
            .await?
            .unwrap_or(now)
    };
    let after = retention(db, source.storage()?, placed, now).await?;
    phase(db, task.id, "retiring", after).await?;
    q::changed(&mut **db, home, movement.target_storage).await?;
    Ok(Step {
        work: None,
        switched: true,
    })
}

pub(super) async fn finish(db: &mut q::Tx<'_>, task: Execution, outcome: &Outcome) -> Result<()> {
    let m = owned(db, task).await?;
    match outcome {
        Outcome::Discard => {
            ensure!(m.phase == "copying", Error::NodeOperationConflict);
            let now = sqlx::query_scalar::<_, i64>(q::CLOCK)
                .fetch_one(&mut **db)
                .await?;
            let after = retention(db, m.target_storage, now, now).await?;
            phase(db, task.id, "discarding", after).await?;
        }
        Outcome::Retry => {
            let now = sqlx::query_scalar::<_, i64>(q::CLOCK)
                .fetch_one(&mut **db)
                .await?;
            phase(db, task.id, &m.phase, m.retire_after.max(now + 15)).await?;
            sqlx::query(q::PARK)
                .bind(task.id)
                .execute(&mut **db)
                .await?;
        }
        Outcome::Checkpoint { progress } => {
            let previous = m
                .checkpoint
                .as_ref()
                .map(|v| v.0.clone())
                .unwrap_or_default();
            ensure!(
                m.phase == "copying"
                    && m.unit == Unit::Raw as i16
                    && progress.parts <= 10000
                    && progress.upload.as_ref().is_none_or(|v| !v.is_empty()
                        && v.len() <= 2048
                        && !v.chars().any(char::is_control))
                    && (progress == &previous
                        || progress.parts == 0
                        || progress.upload == previous.upload
                            && progress.parts == previous.parts + 1),
                Error::NodeOperationConflict
            );
            sqlx::query(q::CHECKPOINT)
                .bind(Json(progress))
                .bind(task.id)
                .execute(&mut **db)
                .await?;
        }
        Outcome::Verified { target, digest } => {
            let prepared = m.target_material.as_ref().context("tier target missing")?;
            ensure!(
                m.phase == "copying"
                    && m.source_material.same_content(target)
                    && target.unit() == Unit::movable(m.unit)?
                    && target.address() == m.target_address
                    && target.storage()? == m.target_storage
                    && match (&prepared.0, target) {
                        (PhysicalSource::Chunk(a), PhysicalSource::Chunk(b)) =>
                            a.algorithm == b.algorithm
                                && a.nonce == b.nonce
                                && (!b.compressed || a.compressed),
                        (PhysicalSource::Pack(a), PhysicalSource::Pack(b)) =>
                            a.tier_index.as_ref().map(|i| &i.entries)
                                == b.tier_index.as_ref().map(|i| &i.entries)
                                && b.digest.as_deref() == Some(digest.as_slice()),
                        (PhysicalSource::Raw(_), PhysicalSource::Raw(_)) => true,
                        _ => false,
                    }
                    && target.block_key() == prepared.block_key(),
                Error::NodeOperationConflict
            );
            ensure!(
                sqlx::query(q::PROOF)
                    .bind(Json(target))
                    .bind(target.block_key())
                    .bind(target.bytes()?)
                    .bind(digest.as_slice())
                    .bind(task.id)
                    .execute(&mut **db)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
        }
        Outcome::Removed => {
            ensure!(
                sqlx::query(q::ERASE)
                    .bind(task.id)
                    .execute(&mut **db)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
            replication_queries::task_state(task.id, "completed", None)
                .execute(&mut **db)
                .await?;
        }
    }
    Ok(())
}
