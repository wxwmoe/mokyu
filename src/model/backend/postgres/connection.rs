use super::{Backend, migration, queries, transactions};
use crate::config::{Budget, Config, Secrets};
use anyhow::{Context, Result, ensure};
use sqlx::{Connection, PgConnection, PgPool, postgres::PgPoolOptions};

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
    ensure!(
        queries::server_version().fetch_one(&mut owner).await? >= 180_000,
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
    let exists: Option<String> = queries::metadata_table().fetch_one(&pool).await?;
    let locked = queries::claim_runtime(role == "single")
        .fetch_one(&mut owner)
        .await?;
    ensure!(
        locked,
        "another runtime owns this database in an incompatible mode"
    );
    migration::run(&pool, &mut owner, exists.is_some()).await?;
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
    let backend = std::sync::Arc::new(Backend::new(pool, owner, role == "single"));
    Ok((
        super::stores(backend.clone()),
        Box::new(RuntimeOwner(backend)),
    ))
}
