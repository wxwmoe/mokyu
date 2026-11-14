use super::{
    Backend,
    queries::{self, history as h, journal_restore as auth, time_recovery as q},
};
use crate::{
    authorization::Principal,
    model::{Id, time_recovery::*},
};
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

async fn view(tx: &mut q::Tx<'_>, id: Uuid, after: Option<&str>) -> Result<Plan> {
    let (source_bucket, _, req, _, digest, watermark, expires_us, phase) =
        q::plan(id).fetch_one(&mut **tx).await?;
    let mut items = q::items(id, after.unwrap_or(""))
        .fetch_all(&mut **tx)
        .await?;
    let next = if items.len() > 50 {
        items.pop();
        items.last().map(|v| v.key.clone())
    } else {
        None
    };
    let counts = q::counts(id)
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .collect();
    Ok(Plan {
        id,
        source_bucket,
        request: serde_json::from_str(&req)?,
        watermark,
        digest,
        expires_us,
        phase,
        counts,
        items,
        next,
    })
}
fn floor(first: i64, seconds: i64, now: i64) -> i64 {
    first.max(now.saturating_sub(seconds.saturating_mul(1_000_000)))
}
#[async_trait::async_trait]
impl TimeRecovery for Backend {
    async fn time_window(&self, p: &Principal, b: Uuid, change: Option<i64>) -> Result<Window> {
        let mut tx = auth::admin(self, p).await?;
        self.fence_control(&mut tx).await?;
        h::mode(b, change.is_some()).fetch_one(&mut *tx).await?;
        let raw = q::lock_window(b).fetch_optional(&mut *tx).await?;
        let now = q::now().fetch_one(&mut *tx).await?;
        if let Some(seconds) = change {
            ensure!(
                (0..=315_360_000).contains(&seconds),
                s3s::s3_error!(InvalidArgument)
            );
            match (seconds, raw) {
                (0, Some(_)) => {
                    q::clear_points(b).execute(&mut *tx).await?;
                    q::close(b).execute(&mut *tx).await?;
                    q::free_retained(b).execute(&mut *tx).await?;
                }
                (0, None) => {}
                (s, Some((old, first, _, _))) => {
                    q::resize(s, floor(floor(first, old, now), s, now), b)
                        .execute(&mut *tx)
                        .await?;
                }
                (s, None) => {
                    q::start(b, s, now, now).execute(&mut *tx).await?;
                    q::baseline(now, b).execute(&mut *tx).await?;
                }
            }
            if raw.is_some() || seconds > 0 {
                let n = q::journal_window(b, seconds == 0)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected();
                if n != 0 {
                    queries::journal::advanced_history(i64::try_from(n)?, b)
                        .execute(&mut *tx)
                        .await?;
                }
                q::dirty_window(b).execute(&mut *tx).await?;
            }
        }
        let mut v = Window {
            latest_us: now,
            retained_bytes: q::retained(b).fetch_one(&mut *tx).await?,
            ..Default::default()
        };
        if let Some((seconds, first, last, seq)) = q::window(b).fetch_optional(&mut *tx).await? {
            v.seconds = seconds;
            v.latest_us = now.max(last);
            v.earliest_us = Some(floor(first, seconds, v.latest_us));
            v.watermark = seq;
            v.gaps = serde_json::from_str(&q::gaps(b).fetch_one(&mut *tx).await?)?;
            v.gaps.retain(|g| Some(g.before_us) > v.earliest_us);
            // A later commit in the same database clock tick must fall after the returned cut.
            if v.latest_us > last {
                q::seal_read(v.latest_us, b).execute(&mut *tx).await?;
            }
        }
        tx.commit().await?;
        Ok(v)
    }
    async fn time_preview(&self, p: &Principal, b: Uuid, req: &Request) -> Result<Plan> {
        req.validate(b)?;
        let mut tx = auth::admin(self, p).await?;
        self.fence_control(&mut tx).await?;
        let mut buckets = vec![b, req.target_bucket];
        buckets.sort_unstable();
        buckets.dedup();
        for x in buckets {
            h::mode(x, true).fetch_one(&mut *tx).await?;
        }
        let (sec, first, last, _) = q::lock_window(b)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(InvalidRequest, "recovery window is disabled"))?;
        let now = q::now().fetch_one(&mut *tx).await?.max(last);
        let at = req.at.timestamp_micros();
        ensure!(
            (floor(first, sec, now)..=now).contains(&at),
            s3s::s3_error!(InvalidArgument, "time is outside recovery window")
        );
        let gaps: Vec<Gap> = serde_json::from_str(&q::gaps(b).fetch_one(&mut *tx).await?)?;
        ensure!(
            !gaps.iter().any(|g| g.contains(at)),
            s3s::s3_error!(
                InvalidArgument,
                "the selected time is in an unverified backup interval"
            )
        );
        let cut = q::cut(b, at).fetch_one(&mut *tx).await?;
        let id = Uuid::new_v4();
        let spec = serde_json::to_string(req)?;
        let owner = serde_json::to_string(p)?;
        q::insert_plan(
            id,
            b,
            req.target_bucket,
            &spec,
            &owner,
            cut,
            at,
            now + 900_000_000,
        )
        .execute(&mut *tx)
        .await?;
        let mut hash = blake3::Hasher::new();
        hash.update(id.as_bytes());
        hash.update(b.as_bytes());
        hash.update(&cut.to_be_bytes());
        hash.update(spec.as_bytes());
        let mut after = String::new();
        loop {
            let page = q::scan(cut, b, &after, &req.key, &req.key)
                .fetch_all(&mut *tx)
                .await?;
            if page.is_empty() {
                break;
            }
            for (key, source) in page {
                after.clone_from(&key);
                if matches!(req.scope, Scope::Object) && key != req.key {
                    break;
                }
                let target = req.target(&key)?;
                let expected = q::target(req.target_bucket, &target)
                    .fetch_optional(&mut *tx)
                    .await?
                    .flatten();
                let unavailable = if let Some(id) = source {
                    q::missing(id).fetch_one(&mut *tx).await?
                } else {
                    false
                };
                let state = match (source, expected) {
                    _ if unavailable => "missing",
                    (None, None) => continue,
                    (None, Some(_)) if req.in_place => "remove",
                    (None, _) => continue,
                    (Some(_), None) => "add",
                    (Some(old), Some(cur)) if req.in_place && old != cur => "replace",
                    _ => "skip",
                };
                let image = serde_json::to_vec(&(&key, &target, source, expected, state))?;
                hash.update(&(image.len() as u64).to_be_bytes());
                hash.update(&image);
                q::insert_item(id, &key, &target, source, expected, state)
                    .execute(&mut *tx)
                    .await?;
            }
            if matches!(req.scope, Scope::Object) {
                break;
            }
        }
        q::digest(&hash.finalize().to_hex(), id)
            .execute(&mut *tx)
            .await?;
        let v = view(&mut tx, id, None).await?;
        tx.commit().await?;
        Ok(v)
    }
    async fn time_plan(
        &self,
        p: &Principal,
        id: Uuid,
        digest: Option<&str>,
        after: Option<&str>,
    ) -> Result<Plan> {
        let mut tx = auth::admin(self, p).await?;
        self.fence_control(&mut tx).await?;
        let (b, _, req, owner, stored, _, expires, phase) = q::plan(id).fetch_one(&mut *tx).await?;
        ensure!(
            owner == serde_json::to_string(p)?,
            s3s::s3_error!(AccessDenied)
        );
        if let Some(d) = digest {
            ensure!(
                d == stored && matches!(phase.as_str(), "preview" | "running" | "done"),
                s3s::s3_error!(PreconditionFailed)
            );
            if phase == "preview" {
                let total = q::counts(id).fetch_all(&mut *tx).await?;
                ensure!(
                    !total.iter().any(|(s, n)| s == "missing" && *n > 0),
                    s3s::s3_error!(PreconditionFailed, "the plan contains missing payloads")
                );
                let spec: Request = serde_json::from_str(&req)?;
                let now = q::now().fetch_one(&mut *tx).await?;
                let (seconds, first, last, _) =
                    q::lock_window(b)
                        .fetch_optional(&mut *tx)
                        .await?
                        .ok_or_else(|| s3s::s3_error!(PreconditionFailed))?;
                ensure!(
                    now <= expires
                        && spec.at.timestamp_micros() >= floor(first, seconds, now.max(last)),
                    s3s::s3_error!(PreconditionFailed)
                );
                q::run(id).execute(&mut *tx).await?;
            }
        }
        let v = view(&mut tx, id, after).await?;
        tx.commit().await?;
        Ok(v)
    }
    async fn time_work(&self) -> Result<Option<Work>> {
        let mut tx = h::begin(self, true).await?;
        let Some(id) = q::active().fetch_optional(&mut *tx).await? else {
            return Ok(None);
        };
        let (source, target, _, owner, _, _, _, phase) = q::plan(id).fetch_one(&mut *tx).await?;
        if phase != "running" {
            return Ok(None);
        }
        let Some(item) = q::next(id).fetch_optional(&mut *tx).await? else {
            q::complete(id).execute(&mut *tx).await?;
            q::release_plan(id).execute(&mut *tx).await?;
            q::free_retained(source).execute(&mut *tx).await?;
            tx.commit().await?;
            return Ok(None);
        };
        let actor: Principal = serde_json::from_str(&owner)?;
        if let Err(e) = q::admin_check(&mut tx, &actor).await {
            if !e
                .downcast_ref::<s3s::S3Error>()
                .is_some_and(|v| v.code() == &s3s::S3ErrorCode::AccessDenied)
            {
                return Err(e);
            }
            q::expire(id).execute(&mut *tx).await?;
            q::release_plan(id).execute(&mut *tx).await?;
            q::free_retained(source).execute(&mut *tx).await?;
            tx.commit().await?;
            return Ok(None);
        }
        let token = Uuid::new_v4();
        if q::lease(token, id).execute(&mut *tx).await?.rows_affected() != 1 {
            return Ok(None);
        }
        let stream = match item.source {
            Some(s) => Some(queries::journal::stream(s).fetch_one(&mut *tx).await?),
            None => None,
        };
        tx.commit().await?;
        Ok(Some(Work {
            step: Step {
                plan: id,
                token,
                key: item.key.clone(),
            },
            actor,
            source,
            target,
            item,
            stream,
        }))
    }
    async fn time_renew(&self, step: &Step) -> Result<bool> {
        let mut tx = h::begin(self, true).await?;
        let ok = q::renew(step.plan, step.token)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1;
        tx.commit().await?;
        Ok(ok)
    }
    async fn time_conflict(&self, step: &Step) -> Result<()> {
        let mut tx = h::begin(self, true).await?;
        if q::release(step.plan, step.token)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            != 1
        {
            return Ok(());
        }
        q::finish_item("conflict", step.plan, &step.key)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    async fn time_expire(&self) -> Result<()> {
        let mut tx = h::begin(self, true).await?;
        for (id, b) in q::expired().fetch_all(&mut *tx).await? {
            q::expire(id).execute(&mut *tx).await?;
            q::release_plan(id).execute(&mut *tx).await?;
            q::free_retained(b).execute(&mut *tx).await?;
        }
        if let Some(b) = q::due_window().fetch_optional(&mut *tx).await?
            && let Some((s, f, last, _)) = q::lock_window(b).fetch_optional(&mut *tx).await?
        {
            let now = q::now().fetch_one(&mut *tx).await?.max(last);
            let boundary = floor(f, s, now);
            for (k, seq) in q::stale_points(b, boundary, boundary)
                .fetch_all(&mut *tx)
                .await?
            {
                q::erase(b, &k, seq).execute(&mut *tx).await?;
            }
            q::resize(s, boundary, b).execute(&mut *tx).await?;
            q::free_retained(b).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
pub(super) async fn applied(
    tx: &mut q::Tx<'_>,
    step: &Step,
    target: Uuid,
    key: &str,
    old: Option<Id>,
    remove: bool,
) -> Result<()> {
    let (_, dest, _, owner, _, _, _, phase) = q::plan(step.plan).fetch_one(&mut **tx).await?;
    let item = q::item(step.plan, &step.key)
        .fetch_optional(&mut **tx)
        .await?
        .context("missing recovery plan item")?;
    ensure!(
        phase == "running"
            && dest == target
            && item.target == key
            && !item.target_changed
            && item.expected == old
            && q::owns(step.plan, step.token).fetch_one(&mut **tx).await?
            && (if remove {
                item.state == "remove"
            } else {
                matches!(item.state.as_str(), "add" | "replace")
            }),
        s3s::s3_error!(PreconditionFailed)
    );
    q::admin_check(tx, &serde_json::from_str(&owner)?).await?;
    q::release(step.plan, step.token).execute(&mut **tx).await?;
    ensure!(
        q::finish_item("done", step.plan, &step.key)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 1,
        s3s::s3_error!(PreconditionFailed)
    );
    Ok(())
}
