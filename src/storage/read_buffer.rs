use anyhow::{Result, ensure};
use bytes::Bytes;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) const UNIT: usize = 64 * 1024;
pub(crate) const UNITS: usize = 16 * 1024 * 1024 / UNIT;

struct Slab {
    credits: Arc<Semaphore>,
    _slot: OwnedSemaphorePermit,
}

pub(crate) struct Spares {
    slots: Arc<Semaphore>,
    slabs: Mutex<Vec<Weak<Slab>>>,
}

impl Spares {
    pub(crate) fn new(slots: Arc<Semaphore>) -> Arc<Self> {
        Arc::new(Self {
            slots,
            slabs: Default::default(),
        })
    }

    fn borrow(&self, units: u32) -> Option<(Arc<Slab>, OwnedSemaphorePermit)> {
        let mut slabs = self.slabs.lock().unwrap();
        slabs.retain(|slab| slab.strong_count() > 0);
        for slab in slabs.iter().filter_map(Weak::upgrade) {
            if let Ok(credit) = slab.credits.clone().try_acquire_many_owned(units) {
                return Some((slab, credit));
            }
        }
        // A reader can borrow an idle slot while leaving an admission slot for another request.
        let mut slots = self.slots.clone().try_acquire_many_owned(2).ok()?;
        let slab = Arc::new(Slab {
            credits: Arc::new(Semaphore::new(UNITS)),
            _slot: slots.split(1)?,
        });
        let credit = slab.credits.clone().try_acquire_many_owned(units).ok()?;
        slabs.push(Arc::downgrade(&slab));
        Some((slab, credit))
    }
}

tokio::task_local! { pub(crate) static WORK: Arc<Charge>; }

pub(crate) struct Charge {
    credit: OwnedSemaphorePermit,
    _slot: Arc<OwnedSemaphorePermit>,
    _borrowed: Option<Arc<Slab>>,
}

pub(crate) async fn reserve(
    reserved: &Arc<Semaphore>,
    slot: &Arc<OwnedSemaphorePermit>,
    spare: Option<&Arc<Spares>>,
    bytes: usize,
) -> Result<Charge> {
    let units = bytes.max(1).div_ceil(UNIT);
    ensure!(units <= UNITS, "read exceeds buffer budget");
    let (borrowed, credit) =
        if let Ok(credit) = reserved.clone().try_acquire_many_owned(units as u32) {
            (None, credit)
        } else if let Some((slab, credit)) = spare.and_then(|spare| spare.borrow(units as u32)) {
            (Some(slab), credit)
        } else {
            (
                None,
                reserved.clone().acquire_many_owned(units as u32).await?,
            )
        };
    Ok(Charge {
        credit,
        _slot: slot.clone(),
        _borrowed: borrowed,
    })
}

struct Output<T> {
    bytes: Bytes,
    _charge: T,
}
impl<T> AsRef<[u8]> for Output<T> {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Charge {
    pub(crate) fn hold_shared(self: Arc<Self>, bytes: Bytes) -> Bytes {
        match Arc::try_unwrap(self) {
            Ok(charge) => charge.hold(bytes),
            Err(charge) => Bytes::from_owner(Output {
                bytes,
                _charge: charge,
            }),
        }
    }
    pub(crate) fn hold(mut self, bytes: Bytes) -> Bytes {
        let release = self
            .credit
            .num_permits()
            .saturating_sub(bytes.len().max(1).div_ceil(UNIT));
        if release > 0 {
            drop(self.credit.split(release));
        }
        Bytes::from_owner(Output {
            bytes,
            _charge: self,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn retained_output_returns_workspace_and_preserves_admission() {
        let reserved = Arc::new(Semaphore::new(4));
        let slots = Arc::new(Semaphore::new(3));
        let base = Arc::new(slots.clone().acquire_owned().await.unwrap());
        let spare = Spares::new(slots.clone());
        let first = reserve(&reserved, &base, Some(&spare), 4 * UNIT)
            .await
            .unwrap();
        let borrowed = reserve(&reserved, &base, Some(&spare), UNIT).await.unwrap();
        assert_eq!(slots.available_permits(), 1);
        let data = first.hold(Bytes::from_static(b"x"));
        assert_eq!(reserved.available_permits(), 3);
        drop(data);
        drop(borrowed);
        assert_eq!(reserved.available_permits(), 4);
        assert_eq!(slots.available_permits(), 2);
    }
}
