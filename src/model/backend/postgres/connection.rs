use super::{Backend, migration, queries, transactions};
use crate::config::{Budget, Config, Secrets};
use crate::model::Id;
use anyhow::{Context, Result, ensure};
use sqlx::{Connection, PgConnection, PgPool, postgres::PgPoolOptions};
use std::{
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

#[derive(Default)]
struct ReplicaCounters {
    attempts: AtomicU64,
    pages: AtomicU64,
    lagging: AtomicU64,
    errors: AtomicU64,
    connect_errors: AtomicU64,
    query_errors: AtomicU64,
    busy: AtomicU64,
    fallbacks: AtomicU64,
}

struct ReplicaHealth {
    next: Instant,
    failures: u32,
    state: &'static str,
}

pub(super) struct Replica {
    pool: PgPool,
    identity: queries::ReplicaIdentity,
    health: Mutex<ReplicaHealth>,
    probe: tokio::sync::Semaphore,
    slots: tokio::sync::Semaphore,
    limit: u32,
    counters: ReplicaCounters,
    database_info: std::sync::Arc<Mutex<Option<crate::model::runtime::DatabaseInfo>>>,
}

impl Replica {
    fn new(
        options: sqlx::postgres::PgConnectOptions,
        limit: u32,
        identity: queries::ReplicaIdentity,
    ) -> Self {
        let expected = identity.clone();
        let database_info = std::sync::Arc::new(Mutex::new(None));
        let observed = database_info.clone();
        let pool = PgPoolOptions::new()
            .min_connections(0)
            .max_connections(limit)
            .acquire_timeout(Duration::from_millis(200))
            .test_before_acquire(false)
            .idle_timeout(Duration::from_secs(60))
            .after_connect(move |connection, _| {
                let expected = expected.clone();
                let observed = observed.clone();
                Box::pin(async move {
                    if !queries::check_replica(&expected)
                        .fetch_one(&mut *connection)
                        .await?
                    {
                        return Err(sqlx::Error::Protocol(
                            "replica identity, version or role mismatch".into(),
                        ));
                    }
                    let (_, version) = queries::server_version().fetch_one(connection).await?;
                    *observed.lock().unwrap() = Some(crate::model::runtime::DatabaseInfo {
                        program: "PostgreSQL",
                        version,
                        schema_version: i64::from(expected.schema_version),
                        observed_at: chrono::Utc::now(),
                        journal_mode: None,
                        synchronous: None,
                    });
                    Ok(())
                })
            })
            .connect_lazy_with(
                options
                    .options([
                        ("default_transaction_read_only", "on"),
                        ("statement_timeout", "100ms"),
                    ])
                    .application_name("mokyu-replica"),
            );
        Self {
            pool,
            identity,
            limit,
            health: Mutex::new(ReplicaHealth {
                next: Instant::now(),
                failures: 0,
                state: "idle",
            }),
            probe: tokio::sync::Semaphore::new(1),
            slots: tokio::sync::Semaphore::new(limit as usize),
            counters: ReplicaCounters::default(),
            database_info,
        }
    }

    pub(super) fn snapshot(&self) -> serde_json::Value {
        let health = self.health.lock().unwrap();
        serde_json::json!({"database":*self.database_info.lock().unwrap(),"state":health.state,"retry_after_ms":health.next.saturating_duration_since(Instant::now()).as_millis(),"pool_size":self.pool.size(),"pool_idle":self.pool.num_idle(),"connections":self.limit,"attempts":self.counters.attempts.load(Relaxed),"pages":self.counters.pages.load(Relaxed),"lagging":self.counters.lagging.load(Relaxed),"errors":self.counters.errors.load(Relaxed),"connect_errors":self.counters.connect_errors.load(Relaxed),"query_errors":self.counters.query_errors.load(Relaxed),"busy":self.counters.busy.load(Relaxed),"fallbacks":self.counters.fallbacks.load(Relaxed)})
    }

    pub(super) async fn read(
        &self,
        stream: Id,
        lease: uuid::Uuid,
        from: i64,
        until: i64,
    ) -> Option<Vec<crate::model::ReadExtent>> {
        let result = self.try_read(stream, lease, from, until).await;
        if result.is_none() {
            self.counters.fallbacks.fetch_add(1, Relaxed);
        }
        result
    }

    async fn try_read(
        &self,
        stream: Id,
        lease: uuid::Uuid,
        from: i64,
        until: i64,
    ) -> Option<Vec<crate::model::ReadExtent>> {
        let probe = {
            let health = self.health.lock().unwrap();
            if Instant::now() < health.next {
                return None;
            }
            if health.failures > 0 {
                Some(self.probe.try_acquire().ok()?)
            } else {
                None
            }
        };
        let Ok(_slot) = self.slots.try_acquire() else {
            self.counters.busy.fetch_add(1, Relaxed);
            return None;
        };
        if self.pool.num_idle() == 0 && self.pool.size() >= self.limit {
            self.counters.busy.fetch_add(1, Relaxed);
            return None;
        }
        self.counters.attempts.fetch_add(1, Relaxed);
        let mut connected = false;
        let result = async {
            let mut connection =
                tokio::time::timeout(Duration::from_millis(200), self.pool.acquire()).await??;
            connected = true;
            let result = tokio::time::timeout(Duration::from_millis(100), async {
                #[cfg(feature = "fault-injection")]
                crate::faults::point("read-replica-query").await;
                queries::replica_page(&mut connection, &self.identity, stream, lease, from, until)
                    .await
            })
            .await;
            if !matches!(result, Ok(Ok(_))) {
                connection.close_on_drop();
            }
            result?
        }
        .await;
        match result {
            Ok(Some(page)) => {
                self.counters.pages.fetch_add(1, Relaxed);
                let mut health = self.health.lock().unwrap();
                if probe.is_some() || health.failures == 0 {
                    health.failures = 0;
                    health.state = "available";
                }
                Some(page)
            }
            result => {
                let mut health = self.health.lock().unwrap();
                health.failures = (health.failures + 1).min(6);
                let delay = if result.is_ok() {
                    self.counters.lagging.fetch_add(1, Relaxed);
                    health.state = "lagging";
                    Duration::from_millis(250)
                } else {
                    self.counters.errors.fetch_add(1, Relaxed);
                    if connected {
                        self.counters.query_errors.fetch_add(1, Relaxed);
                    } else {
                        self.counters.connect_errors.fetch_add(1, Relaxed);
                    }
                    health.state = "backoff";
                    Duration::from_secs((1u64 << (health.failures - 1)).min(30))
                };
                health.next = Instant::now() + delay;
                None
            }
        }
    }
}

struct RuntimeOwner(std::sync::Arc<Backend>);

#[async_trait::async_trait]
impl crate::model::runtime::Owner for RuntimeOwner {
    async fn check(&mut self) -> Result<()> {
        queries::health()
            .execute(&mut *self.0.owner.lock().await)
            .await?;
        Ok(())
    }
}

pub(super) async fn set_mode(
    backend: &Backend,
    principal: &crate::authorization::Principal,
    mode: &str,
) -> Result<()> {
    let mut owner = backend.owner.lock().await;
    let exclusive = queries::claim_runtime(true).fetch_one(&mut *owner).await?;
    ensure!(
        exclusive,
        "stop every other runtime before changing deployment mode"
    );
    let result = transactions::set_deployment_mode(&backend.pool, principal, mode).await;
    if result.is_err() {
        queries::release_runtime().fetch_one(&mut *owner).await?;
    }
    // Keep exclusive ownership until restart, including the control process.
    result
}

struct ProfileListener(sqlx::postgres::PgListener);

#[async_trait::async_trait]
impl crate::model::cluster::ProfileChanges for ProfileListener {
    async fn next(&mut self) -> Result<uuid::Uuid> {
        loop {
            let notification = self.0.recv().await?;
            match notification.payload().parse() {
                Ok(id) => return Ok(id),
                Err(_) => tracing::warn!(
                    payload = notification.payload(),
                    "invalid storage profile notification"
                ),
            }
        }
    }
}

pub(super) async fn profile_changes(
    pool: &PgPool,
) -> Result<Box<dyn crate::model::cluster::ProfileChanges>> {
    let mut listener = sqlx::postgres::PgListener::connect_with(pool).await?;
    listener
        .listen("mokyu_storage_profiles")
        .await
        .context("storage profile subscription failed")?;
    Ok(Box::new(ProfileListener(listener)))
}

pub(super) async fn listen_tasks(
    pool: &PgPool,
    wake: std::sync::Arc<tokio::sync::Notify>,
) -> Result<()> {
    let mut listener = sqlx::postgres::PgListener::connect_with(pool).await?;
    listener.listen("mokyu_tasks").await?;
    wake.notify_waiters();
    loop {
        listener.recv().await?;
        wake.notify_waiters();
    }
}

pub async fn connect(
    c: &Config,
    secrets: &mut Secrets,
    budget: &Budget,
    role: &str,
) -> Result<(crate::model::Stores, Box<dyn crate::model::runtime::Owner>)> {
    let options = sqlx::postgres::PgConnectOptions::new()
        .host(&c.database.host)
        .port(
            c.database
                .effective_port()
                .context("missing database port")?,
        )
        .database(&c.database.name)
        .username(&c.database.user)
        .password(&secrets.database_password)
        .ssl_mode(c.database.ssl_mode.parse()?);
    let mut owner = PgConnection::connect_with(&options)
        .await
        .map_err(|_| anyhow::anyhow!("cannot connect to PostgreSQL"))?;
    let (version_number, version) = queries::server_version().fetch_one(&mut owner).await?;
    ensure!(
        version_number >= 180_000,
        "PostgreSQL 18 or newer is required"
    );
    let pool = PgPoolOptions::new()
        .min_connections(0)
        .max_connections(budget.database_connections)
        .acquire_timeout(std::time::Duration::from_secs(15))
        .acquire_time_level(log::LevelFilter::Debug)
        .acquire_slow_threshold(std::time::Duration::from_millis(200))
        .idle_timeout(std::time::Duration::from_secs(60))
        .after_connect(|connection, _| {
            Box::pin(async move {
                queries::synchronous_commit()
                    .execute(&mut *connection)
                    .await?;
                queries::statement_timeout().execute(connection).await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await
        .map_err(|_| anyhow::anyhow!("cannot open PostgreSQL pool"))?;
    let fsync: String = queries::fsync().fetch_one(&pool).await?;
    ensure!(fsync == "on", "PostgreSQL fsync must be enabled");
    let locked = queries::claim_runtime(role == "single")
        .fetch_one(&mut owner)
        .await?;
    ensure!(
        locked,
        "another runtime owns this database in an incompatible mode"
    );
    migration::run(&mut owner).await?;
    let database_info = crate::model::runtime::DatabaseInfo {
        program: "PostgreSQL",
        version,
        schema_version: i64::from(queries::schema_version().fetch_one(&mut owner).await?),
        observed_at: chrono::Utc::now(),
        journal_mode: None,
        synchronous: None,
    };
    crate::model::key::load(secrets, queries::managed_keys().fetch_all(&pool).await?)?;
    let deployment_fingerprint = blake3::hash(&secrets.credential_key.material);
    let saved: Option<Vec<u8>> = queries::deployment_fingerprint().fetch_one(&pool).await?;
    if let Some(saved) = saved {
        ensure!(
            saved == deployment_fingerprint.as_bytes(),
            "deployment key does not match this database"
        );
    } else {
        queries::set_deployment_fingerprint(deployment_fingerprint.as_bytes().as_slice())
            .execute(&pool)
            .await?;
    }
    for (id, alg, material) in secrets
        .keys
        .iter()
        .map(|(id, key)| (id.as_str(), key.algorithm.as_str(), &key.material))
        .chain(std::iter::once((
            "__credentials",
            "credentials-aes-256-gcm",
            &secrets.credential_key.material,
        )))
    {
        let hash = blake3::hash(material);
        queries::register_key_fingerprint(id, alg, hash.as_bytes().as_slice())
            .execute(&pool)
            .await?;
        let (saved_alg, saved): (String, Vec<u8>) =
            queries::key_fingerprint(id).fetch_one(&pool).await?;
        ensure!(
            saved_alg == alg && saved == hash.as_bytes(),
            "key ID changed algorithm or material"
        );
    }
    let required: Vec<uuid::Uuid> = queries::required_keys().fetch_all(&pool).await?;
    ensure!(
        required
            .iter()
            .all(|id| secrets.key_for_id(Some(*id)).is_some()),
        "database is missing a historical chunk key"
    );
    let mut backend = Backend::new(pool, owner, role == "single");
    backend.database_info = Some(database_info);
    if role == "data"
        && let Some(replica) = &c.replica_database
    {
        let identity = queries::replica_identity().fetch_one(&backend.pool).await?;
        if identity.deployment_mode == "cluster" {
            let replica = replica.resolve(&c.database)?;
            let options = sqlx::postgres::PgConnectOptions::new()
                .host(&replica.host)
                .port(replica.effective_port().unwrap())
                .database(&replica.name)
                .username(&replica.user)
                .password(
                    secrets
                        .replica_password
                        .as_deref()
                        .context("missing replica database password")?,
                )
                .ssl_mode(replica.ssl_mode.parse()?);
            backend.replica = Some(Replica::new(
                options,
                budget.database_read_connections,
                identity,
            ));
        }
    }
    let backend = std::sync::Arc::new(backend);
    Ok((
        super::stores(backend.clone()),
        Box::new(RuntimeOwner(backend)),
    ))
}
