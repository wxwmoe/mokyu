use anyhow::Result;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use uuid::Uuid;

pub(crate) struct SharedStorePermit {
    pub(crate) db: sqlx::PgPool,
    pub(crate) token: Uuid,
    pub(crate) alive: Arc<AtomicBool>,
}

impl Drop for SharedStorePermit {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
        let db = self.db.clone();
        let token = self.token;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = sqlx::query("DELETE FROM storage_permits WHERE token=$1")
                    .bind(token)
                    .execute(&db)
                    .await;
            });
        }
    }
}

pub(crate) struct StorePermit {
    pub(crate) _local: crate::storage::resource_gate::Permit,
    pub(crate) _shared: Option<SharedStorePermit>,
}

pub(crate) struct StoreGate {
    pub(crate) local: Arc<crate::storage::resource_gate::Gate>,
    pub(crate) db: sqlx::PgPool,
    pub(crate) storage_id: Uuid,
    pub(crate) direction: &'static str,
    pub(crate) cluster_limit: Option<i32>,
    pub(crate) holder: Uuid,
}

impl StoreGate {
    pub(crate) fn new(
        db: &sqlx::PgPool,
        storage_id: Uuid,
        direction: &'static str,
        local_limit: usize,
        cluster_limit: Option<i32>,
        aging: Duration,
        holder: Uuid,
    ) -> Arc<Self> {
        Arc::new(Self {
            local: crate::storage::resource_gate::Gate::new(local_limit, aging),
            db: db.clone(),
            storage_id,
            direction,
            cluster_limit,
            holder,
        })
    }

    pub(crate) async fn acquire_shared(&self, limit: i32) -> Result<SharedStorePermit> {
        loop {
            let token = Uuid::new_v4();
            let mut tx = self.db.begin().await?;
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                .bind(format!(
                    "mokyu:store:{}:{}",
                    self.storage_id, self.direction
                ))
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM storage_permits WHERE storage_id=$1 AND direction=$2 AND expires_at<=now()")
                .bind(self.storage_id)
                .bind(self.direction)
                .execute(&mut *tx)
                .await?;
            let active: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM storage_permits WHERE storage_id=$1 AND direction=$2",
            )
            .bind(self.storage_id)
            .bind(self.direction)
            .fetch_one(&mut *tx)
            .await?;
            if active < i64::from(limit) {
                sqlx::query("INSERT INTO storage_permits(token,storage_id,direction,holder_id,expires_at) VALUES($1,$2,$3,$4,now()+interval '1 minute')")
                    .bind(token)
                    .bind(self.storage_id)
                    .bind(self.direction)
                    .bind(self.holder)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
                let alive = Arc::new(AtomicBool::new(true));
                let renew_alive = alive.clone();
                let db = self.db.clone();
                tokio::spawn(async move {
                    let mut failures = 0u8;
                    while renew_alive.load(Ordering::Acquire) {
                        tokio::time::sleep(Duration::from_secs(20)).await;
                        if !renew_alive.load(Ordering::Acquire) {
                            break;
                        }
                        match sqlx::query(
                            "UPDATE storage_permits SET expires_at=now()+interval '1 minute' WHERE token=$1",
                        )
                        .bind(token)
                        .execute(&db)
                        .await
                        {
                            Ok(result) if result.rows_affected() == 1 => failures = 0,
                            Ok(_) => {
                                renew_alive.store(false, Ordering::Release);
                                break;
                            }
                            Err(error) => {
                                failures = failures.saturating_add(1);
                                if failures >= 3 {
                                    tracing::warn!(%error, %token, "storage permit renewal stopped");
                                    let _ = sqlx::query("DELETE FROM storage_permits WHERE token=$1")
                                        .bind(token)
                                        .execute(&db)
                                        .await;
                                    renew_alive.store(false, Ordering::Release);
                                    break;
                                }
                            }
                        }
                    }
                });
                return Ok(SharedStorePermit {
                    db: self.db.clone(),
                    token,
                    alive,
                });
            }
            tx.rollback().await?;
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub(crate) async fn acquire(self: &Arc<Self>) -> Result<StorePermit> {
        let local = self.local.acquire().await?;
        let shared = match self.cluster_limit {
            Some(limit) => Some(self.acquire_shared(limit).await?),
            None => None,
        };
        Ok(StorePermit {
            _local: local,
            _shared: shared,
        })
    }
}
