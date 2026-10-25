use super::RunLease;
use crate::rpc::{client::lease_deadline, wire};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{
    sync::{Notify, Semaphore},
    time::Instant,
};
use tonic::Status;
use uuid::Uuid;

const LIMIT: usize = 8192;
const BATCH: usize = 256;
const INTERVAL: Duration = Duration::from_secs(10);

pub(crate) struct Grant {
    pub(crate) reference: wire::GrantRef,
    pub(crate) storage: Option<Uuid>,
    run: Arc<RunLease>,
    expires: Mutex<Instant>,
    resources: Weak<Resources>,
    pub(crate) io: Arc<Semaphore>,
}

impl Grant {
    pub(crate) fn deadline(&self) -> Instant {
        *self.expires.lock().unwrap()
    }
    pub(crate) fn check(&self) -> Result<(), Status> {
        self.run.check()?;
        if Instant::now() >= *self.expires.lock().unwrap() {
            Err(Status::failed_precondition("node resource has expired"))
        } else {
            Ok(())
        }
    }

    fn renew(&self, deadline: Instant) -> Result<Instant, Status> {
        self.run.check()?;
        let mut expires = self.expires.lock().unwrap();
        if Instant::now() >= *expires {
            return Err(Status::failed_precondition(
                "node resource cannot be revived",
            ));
        }
        *expires = (*expires).max(deadline);
        Ok(*expires)
    }

    fn invalidate(&self) {
        *self.expires.lock().unwrap() = Instant::now();
    }
}

impl Drop for Grant {
    fn drop(&mut self) {
        if let Some(resources) = self.resources.upgrade() {
            resources.changed.notify_one();
        }
    }
}

struct Entry {
    reference: wire::GrantRef,
    run: Arc<RunLease>,
    held: Weak<Grant>,
    expires: Instant,
    next: Instant,
    retired: bool,
    released: bool,
}

#[derive(Default)]
pub(crate) struct Resources {
    entries: Mutex<HashMap<Uuid, Entry>>,
    pub(super) changed: Notify,
}

pub(super) struct Batch {
    pub(super) renew: Vec<wire::GrantRef>,
    pub(super) release: Vec<wire::GrantRef>,
}

fn prune(entries: &mut HashMap<Uuid, Entry>) {
    let now = Instant::now();
    // Keep expired handles indexed while callers hold them, so late replies cannot revive them.
    entries.retain(|_, entry| {
        entry.held.strong_count() > 0
            || (!entry.released && entry.expires > now && entry.run.check().is_ok())
    });
}

impl Resources {
    pub(crate) fn released(&self, id: Uuid) -> bool {
        self.entries
            .lock()
            .unwrap()
            .get(&id)
            .is_none_or(|entry| entry.released || entry.expires <= Instant::now())
    }
    pub(crate) fn adopt(
        self: &Arc<Self>,
        run: &Arc<RunLease>,
        grants: &[wire::Grant],
        started: Instant,
    ) -> Result<Vec<Arc<Grant>>, Status> {
        run.check()?;
        if grants.len() > BATCH {
            return Err(Status::data_loss("too many resource grants"));
        }
        let invalid = || Status::data_loss("invalid resource grant");
        let mut seen = HashSet::new();
        let mut parsed = Vec::with_capacity(grants.len());
        for grant in grants {
            let id = Uuid::parse_str(&grant.id).map_err(|_| invalid())?;
            if id.is_nil() || grant.id.len() != 36 || !seen.insert(id) {
                return Err(invalid());
            }
            let storage = match wire::GrantType::try_from(grant.r#type) {
                Ok(
                    wire::GrantType::StorageRead
                    | wire::GrantType::StorageWrite
                    | wire::GrantType::NativeWork,
                ) => {
                    let id = Uuid::parse_str(&grant.storage_id).map_err(|_| invalid())?;
                    if id.is_nil() || grant.storage_id.len() != 36 {
                        return Err(invalid());
                    }
                    Some(id)
                }
                Ok(
                    wire::GrantType::ReadProtection
                    | wire::GrantType::ReadWindow
                    | wire::GrantType::WriteProtection,
                ) if grant.storage_id.is_empty() => None,
                _ => return Err(invalid()),
            };
            parsed.push((
                id,
                grant,
                storage,
                lease_deadline(started, grant.valid_for_ms)?,
            ));
        }
        let mut entries = self.entries.lock().unwrap();
        prune(&mut entries);
        let mut additional = 0;
        for (id, grant, storage, _) in &parsed {
            if let Some(entry) = entries.get(id) {
                let held = entry.held.upgrade().ok_or_else(|| {
                    Status::failed_precondition("node resource is being released")
                })?;
                held.check()?;
                if entry.released
                    || !Arc::ptr_eq(&entry.run, run)
                    || entry.reference.r#type != grant.r#type
                    || held.storage != *storage
                {
                    return Err(invalid());
                }
            } else {
                additional += 1;
            }
        }
        if entries.len() + additional > LIMIT {
            return Err(Status::resource_exhausted("node resource registry is full"));
        }
        let now = Instant::now();
        let mut held = Vec::with_capacity(grants.len());
        for (id, grant, storage, deadline) in parsed {
            if let Some(entry) = entries.get_mut(&id) {
                let resource = entry
                    .held
                    .upgrade()
                    .ok_or_else(|| Status::failed_precondition("node resource was released"))?;
                entry.expires = resource.renew(deadline)?;
                held.push(resource);
            } else {
                let reference = wire::GrantRef {
                    r#type: grant.r#type,
                    id: grant.id.clone(),
                };
                let resource = Arc::new(Grant {
                    reference: reference.clone(),
                    storage,
                    run: run.clone(),
                    expires: Mutex::new(deadline),
                    resources: Arc::downgrade(self),
                    io: Arc::new(Semaphore::new(1)),
                });
                entries.insert(
                    id,
                    Entry {
                        reference,
                        run: run.clone(),
                        held: Arc::downgrade(&resource),
                        expires: deadline,
                        next: now + INTERVAL,
                        retired: false,
                        released: false,
                    },
                );
                held.push(resource);
            }
        }
        if held
            .iter()
            .any(|grant| *grant.expires.lock().unwrap() <= now + INTERVAL)
        {
            self.changed.notify_one();
        }
        Ok(held)
    }

    pub(super) fn batch(&self, run: &Arc<RunLease>) -> Batch {
        let mut entries = self.entries.lock().unwrap();
        prune(&mut entries);
        let now = Instant::now();
        let mut pending = entries
            .values()
            .filter_map(|entry| {
                if entry.released || !Arc::ptr_eq(&entry.run, run) {
                    return None;
                }
                let release =
                    entry.retired || entry.held.strong_count() == 0 || entry.expires <= now;
                (release || entry.next <= now || entry.expires <= now + INTERVAL).then_some((
                    release,
                    entry.expires,
                    entry.reference.clone(),
                ))
            })
            .collect::<Vec<_>>();
        pending.sort_by_key(|(release, expires, reference)| (*expires, !release, reference.r#type));
        let mut batch = Batch {
            renew: Vec::new(),
            release: Vec::new(),
        };
        for (release, _, reference) in pending.into_iter().take(BATCH) {
            if release {
                batch.release.push(reference);
            } else {
                batch.renew.push(reference);
            }
        }
        batch
    }

    pub(super) fn acknowledge(
        &self,
        run: &Arc<RunLease>,
        batch: &Batch,
        started: Instant,
        results: &[wire::GrantResult],
    ) -> Result<(), Status> {
        let expected = batch.renew.iter().chain(&batch.release);
        if results.len() != expected.clone().count()
            || results
                .iter()
                .zip(expected)
                .any(|(result, expected)| result.grant.as_ref() != Some(expected))
        {
            return Err(Status::data_loss("resource acknowledgement mismatch"));
        }
        let mut entries = self.entries.lock().unwrap();
        for (index, result) in results.iter().enumerate() {
            let reference = result.grant.as_ref().unwrap();
            let id = Uuid::parse_str(&reference.id)
                .map_err(|_| Status::data_loss("invalid resource identity"))?;
            let Some(entry) = entries
                .get_mut(&id)
                .filter(|entry| Arc::ptr_eq(&entry.run, run))
            else {
                continue;
            };
            if index >= batch.renew.len() || !result.accepted {
                entry.released = true;
                if let Some(held) = entry.held.upgrade() {
                    held.invalidate();
                }
                continue;
            }
            let deadline = lease_deadline(started, result.valid_for_ms)?;
            entry.next = Instant::now() + INTERVAL;
            if let Some(held) = entry.held.upgrade() {
                match held.renew(deadline) {
                    Ok(deadline) => entry.expires = deadline,
                    Err(_) => {
                        held.invalidate();
                        entry.expires = deadline;
                        entry.retired = true;
                    }
                }
            } else {
                entry.expires = entry.expires.max(deadline);
            }
        }
        prune(&mut entries);
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/rpc/resources.rs"]
mod tests;
