use super::{Backend, migration, queries, transactions};
use crate::config::{Budget, Config, Secrets};
use anyhow::{Context, Result, ensure};
use sqlx::{
    Connection, SqliteConnection,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    time::Duration,
};

struct RuntimeOwner(std::sync::Arc<Backend>);

#[async_trait::async_trait]
impl crate::model::runtime::Owner for RuntimeOwner {
    async fn check(&mut self) -> Result<()> {
        self.0.check_owner()
    }
}

pub(crate) async fn connect(
    config: &Config,
    secrets: &mut Secrets,
    budget: &Budget,
    role: &str,
) -> Result<(crate::model::Stores, Box<dyn crate::model::runtime::Owner>)> {
    let backend = std::sync::Arc::new(open(config, secrets, budget, role).await?);
    crate::model::key::load(
        secrets,
        queries::managed_keys().fetch_all(&backend.read).await?,
    )?;
    let required = queries::required_keys().fetch_all(&backend.read).await?;
    ensure!(
        required
            .iter()
            .all(|id| secrets.key_for_id(Some(*id)).is_some()),
        "database is missing a historical chunk key"
    );
    let stores = crate::model::Stores {
        runtime: backend.clone(),
        cluster: None,
        metrics: backend.clone(),
        integrity: backend.clone(),
        maintenance: backend.clone(),
        packing: backend.clone(),
        physical: backend.clone(),
        upload_cache: backend.clone(),
        multipart: backend.clone(),
        buckets: backend.clone(),
        credentials: backend.clone(),
        administration: backend.clone(),
        accounts: backend.clone(),
        authorization: backend.clone(),
        audit: backend.clone(),
        objects: backend.clone(),
        publish: backend.clone(),
        quota: backend.clone(),
        streams: backend.clone(),
        raw: backend.clone(),
        task_control: backend.clone(),
        task_execution: backend.clone(),
        storage_profiles: backend.clone(),
        runtime_settings: backend.clone(),
        managed_keys: backend.clone(),
        storage_records: backend.clone(),
        external_buckets: backend.clone(),
        replications: backend.clone(),
        chunks: backend.clone(),
    };
    Ok((stores, Box::new(RuntimeOwner(backend))))
}

pub(super) struct DeadlineConnection {
    connection: Option<sqlx::pool::PoolConnection<sqlx::Sqlite>>,
    cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl DeadlineConnection {
    pub(super) async fn acquire(
        pool: &sqlx::SqlitePool,
        deadline: tokio::time::Instant,
    ) -> Result<Self> {
        let connection = tokio::time::timeout_at(deadline, pool.acquire())
            .await
            .map_err(|_| crate::model::Error::QueryCancelled)??;
        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut result = Self {
            connection: Some(connection),
            cancelled: cancelled.clone(),
        };
        result
            .lock_handle()
            .await?
            .set_progress_handler(1000, move || {
                !cancelled.load(std::sync::atomic::Ordering::Relaxed)
                    && tokio::time::Instant::now() < deadline
            });
        Ok(result)
    }

    pub(super) async fn release(mut self) -> Result<()> {
        self.lock_handle().await?.remove_progress_handler();
        self.connection.take();
        Ok(())
    }
}

impl std::ops::Deref for DeadlineConnection {
    type Target = SqliteConnection;
    fn deref(&self) -> &Self::Target {
        self.connection.as_deref().unwrap()
    }
}

impl std::ops::DerefMut for DeadlineConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection.as_deref_mut().unwrap()
    }
}

impl Drop for DeadlineConnection {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            self.cancelled
                .store(true, std::sync::atomic::Ordering::Relaxed);
            // Stop the worker and free pool capacity when the caller cancels the query.
            drop(connection.detach());
        }
    }
}

fn claim(path: &Path) -> Result<(PathBuf, File, PathBuf)> {
    let path = std::path::absolute(path)?;
    let parent = path
        .parent()
        .context("SQLite database requires a parent directory")?;
    let mut directories = std::fs::DirBuilder::new();
    directories.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        directories.mode(0o755);
    }
    directories.create(parent)?;
    let path = match std::fs::symlink_metadata(&path) {
        Ok(_) => path.canonicalize()?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => parent.canonicalize()?.join(
            path.file_name()
                .context("SQLite database requires a filename")?,
        ),
        Err(error) => return Err(error.into()),
    };
    let mut lock_name = path.as_os_str().to_owned();
    lock_name.push(".lock");
    let lock_path = PathBuf::from(lock_name);
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let owner = options.open(&lock_path)?;
    fs2::FileExt::try_lock_exclusive(&owner)
        .context("another runtime owns this SQLite database")?;
    if !path.exists() {
        options.create_new(true).open(&path)?;
    }
    let metadata = path.metadata()?;
    ensure!(
        metadata.is_file(),
        "SQLite requires a regular database file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        ensure!(
            metadata.nlink() == 1,
            "SQLite database must not be hard-linked"
        );
    }
    Ok((path, owner, lock_path))
}

pub(super) async fn open(
    config: &Config,
    secrets: &Secrets,
    budget: &Budget,
    role: &str,
) -> Result<Backend> {
    ensure!(
        role == "single",
        "SQLite only supports single-container mode"
    );
    let path = config
        .database
        .sqlite_path(&config.instance.data_path)
        .context("SQLite database type is required")?;
    ensure!(
        path != Path::new(":memory:"),
        "SQLite requires a persistent database file"
    );
    let (path, owner, lock_path) = claim(&path)?;
    // Inspect the path without opening another database descriptor and disturbing POSIX locks.
    let database_metadata = path.metadata()?;
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(false)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Full)
        .busy_timeout(Duration::from_secs(15))
        .pragma("wal_autocheckpoint", "1000")
        .pragma("journal_size_limit", "67108864");
    let mut connection = SqliteConnection::connect_with(&options)
        .await
        .context("cannot open SQLite database")?;
    let mut synchronous = SqliteSynchronous::Full;
    let initialized: Result<crate::model::runtime::DatabaseInfo> = async {
        let version = queries::version().fetch_one(&mut connection).await?;
        let numbers: Vec<u64> = version
            .split('.')
            .map(str::parse)
            .collect::<std::result::Result<_, _>>()?;
        ensure!(
            numbers.as_slice() >= [3, 51, 3].as_slice(),
            "SQLite 3.51.3 or newer is required"
        );
        ensure!(
            queries::journal_mode().fetch_one(&mut connection).await? == "wal",
            "SQLite requires WAL support on a local filesystem"
        );
        #[cfg(any(test, feature = "fault-injection"))]
        ensure!(
            queries::synchronous().fetch_one(&mut connection).await? == 2,
            "SQLite initialization requires FULL synchronization"
        );
        migration::run(&mut connection).await?;
        transactions::identify(
            &mut connection,
            blake3::hash(&secrets.credential_key.material).as_bytes(),
        )
        .await?;
        let settings = queries::runtime_setting("io")
            .fetch_optional(&mut connection)
            .await?
            .map(serde_json::from_value::<crate::config::runtime::IoSettings>)
            .transpose()?
            .unwrap_or_else(|| config.io.clone());
        settings.validate()?;
        ensure!(
            settings.sqlite_synchronous != "off"
                || !queries::nonces_reserved()
                    .fetch_one(&mut connection)
                    .await?,
            "SQLite OFF synchronization is unavailable after encryption nonce reservations"
        );
        synchronous = match settings.sqlite_synchronous.as_str() {
            "normal" => SqliteSynchronous::Normal,
            "off" => SqliteSynchronous::Off,
            _ => SqliteSynchronous::Full,
        };
        Ok(crate::model::runtime::DatabaseInfo {
            program: "SQLite",
            version,
            schema_version: queries::schema_version().fetch_one(&mut connection).await?,
            observed_at: chrono::Utc::now(),
            journal_mode: None,
            synchronous: None,
        })
    }
    .await;
    connection.close().await?;
    let mut database_info = initialized?;
    let options = options.synchronous(synchronous);
    let pools = || {
        SqlitePoolOptions::new()
            .min_connections(0)
            .acquire_timeout(Duration::from_secs(15))
            .acquire_time_level(log::LevelFilter::Debug)
            .acquire_slow_threshold(Duration::from_millis(200))
            .idle_timeout(Duration::from_secs(60))
    };
    // One write connection queues transactions before SQLite's busy handler is involved.
    let write = pools()
        .max_connections(1)
        .connect_with(options.clone())
        .await?;
    let read = if budget.database_connections == 1 {
        write.clone()
    } else {
        pools()
            .max_connections(budget.database_connections - 1)
            .connect_with(options.read_only(true).pragma("query_only", "ON"))
            .await?
    };
    database_info.journal_mode = Some(queries::journal_mode().fetch_one(&write).await?);
    database_info.synchronous = Some(
        match queries::synchronous().fetch_one(&write).await? {
            0 => "off",
            1 => "normal",
            2 => "full",
            _ => anyhow::bail!("unexpected SQLite synchronous mode"),
        }
        .into(),
    );
    let backend = Backend {
        database_info,
        authorization: Default::default(),
        previews: Default::default(),
        shared_pool: budget.database_connections == 1,
        read,
        write,
        owner,
        lock_path,
        database_path: path,
        database_metadata,
    };
    backend.check_owner()?;
    Ok(backend)
}
