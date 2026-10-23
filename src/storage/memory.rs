use anyhow::{Result, ensure};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const UNIT: usize = 4096;

pub(crate) struct Pool {
    units: u32,
    permits: Arc<Semaphore>,
}

pub(crate) struct Buffer {
    data: Vec<u8>,
    limit: usize,
    _permit: OwnedSemaphorePermit,
}

struct Payload {
    data: Bytes,
    _permit: OwnedSemaphorePermit,
}

impl Pool {
    pub fn new(bytes: u64) -> Self {
        let units = (bytes / UNIT as u64) as u32;
        Self {
            units,
            permits: Arc::new(Semaphore::new(units as usize)),
        }
    }

    fn claim(&self, bytes: usize) -> Option<OwnedSemaphorePermit> {
        let units = u32::try_from(bytes.max(1).div_ceil(UNIT)).ok()?;
        self.permits.clone().try_acquire_many_owned(units).ok()
    }

    pub fn buffer(&self, size: usize) -> Option<Buffer> {
        let permit = self.claim(size)?;
        Some(Buffer {
            data: Vec::with_capacity(size),
            limit: size,
            _permit: permit,
        })
    }

    pub fn retain(&self, data: Bytes) -> Option<Bytes> {
        let permit = self.claim(data.len())?;
        Some(Bytes::from_owner(Payload {
            data,
            _permit: permit,
        }))
    }

    pub fn status(&self) -> serde_json::Value {
        serde_json::json!({
            "capacity": u64::from(self.units) * UNIT as u64,
            "used": (self.units as usize - self.permits.available_permits()) as u64 * UNIT as u64,
        })
    }
}

impl Buffer {
    pub fn receive_external(&mut self) {
        self.data = Vec::new();
    }

    pub fn retain(self, data: Bytes) -> Result<Bytes> {
        ensure!(data.len() == self.limit, "write memory length mismatch");
        Ok(Bytes::from_owner(Payload {
            data,
            _permit: self._permit,
        }))
    }

    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        ensure!(
            data.len() <= self.limit - self.data.len(),
            "write memory length exceeded"
        );
        self.data.extend_from_slice(data);
        Ok(())
    }

    pub fn finish(self) -> Bytes {
        Bytes::from_owner(self)
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn received_read_memory_does_not_hold_the_write_charge_after_drain() {
        let reads = Pool::new(UNIT as u64);
        let writes = Pool::new(UNIT as u64);
        let mut incoming = reads.buffer(3).unwrap();
        incoming.append(b"raw").unwrap();
        let cached = incoming.finish();
        let mut reservation = writes.buffer(3).unwrap();
        reservation.receive_external();
        assert_eq!(reservation.data.capacity(), 0);
        let pending = reservation.retain(cached.clone()).unwrap();
        assert_eq!(pending.as_ptr(), cached.as_ptr());
        let reader = pending.slice(1..);
        drop(pending);
        assert_eq!(writes.status()["used"], UNIT);
        drop(reader);
        assert_eq!(writes.status()["used"], 0);
        assert_eq!(reads.status()["used"], UNIT);
        drop(cached);
        assert_eq!(reads.status()["used"], 0);
        assert!(
            writes
                .buffer(3)
                .unwrap()
                .retain(Bytes::from_static(b"bad size"))
                .is_err()
        );
    }

    #[test]
    fn write_memory_stays_charged_until_the_last_reader_finishes() {
        let pool = Pool::new(UNIT as u64);
        let mut buffer = pool.buffer(3).unwrap();
        buffer.append(b"raw").unwrap();
        assert!(buffer.append(b"x").is_err());
        assert!(pool.buffer(0).is_none());
        let data = buffer.finish();
        let reader = data.slice(1..);
        drop(data);
        assert_eq!(reader, "aw");
        assert!(pool.buffer(1).is_none());
        drop(reader);
        assert_eq!(pool.status()["used"], 0);
        let data = pool.retain(Bytes::from_static(b"chunk")).unwrap();
        assert!(pool.retain(Bytes::new()).is_none());
        drop(data);
        assert!(pool.buffer(UNIT + 1).is_none());
        assert!(Pool::new(0).buffer(0).is_none());
        assert!(pool.buffer(0).is_some());
    }
}
