use super::{Backend, migration, queries, transactions};
use crate::config::{Budget, Config, Secrets};
use anyhow::{Context, Result, ensure};
use sqlx::{
    Connection, MySqlConnection,
    mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlSslMode},
};

struct RuntimeOwner(std::sync::Arc<Backend>);

#[async_trait::async_trait]
impl crate::model::runtime::Owner for RuntimeOwner {
    async fn check(&mut self) -> Result<()> {
        self.0.check_owner().await
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
        queries::managed_keys().fetch_all(&backend.pool).await?,
    )?;
    let required = queries::required_keys().fetch_all(&backend.pool).await?;
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
        chunks: backend.clone(),
    };
    Ok((stores, Box::new(RuntimeOwner(backend))))
}

pub(super) struct DeadlineConnection(Option<sqlx::pool::PoolConnection<sqlx::MySql>>);

impl DeadlineConnection {
    pub(super) async fn acquire(
        pool: &sqlx::MySqlPool,
        deadline: tokio::time::Instant,
    ) -> Result<Self> {
        Ok(Self(Some(
            tokio::time::timeout_at(deadline, pool.acquire())
                .await
                .map_err(|_| crate::model::Error::QueryCancelled)??,
        )))
    }

    pub(super) fn release(mut self) {
        self.0.take();
    }
}

impl std::ops::Deref for DeadlineConnection {
    type Target = MySqlConnection;
    fn deref(&self) -> &Self::Target {
        self.0.as_deref().unwrap()
    }
}

impl std::ops::DerefMut for DeadlineConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_deref_mut().unwrap()
    }
}

impl Drop for DeadlineConnection {
    fn drop(&mut self) {
        if let Some(connection) = self.0.take() {
            // Cancellation must free pool capacity without waiting for the blocked SQL to finish.
            drop(connection.detach());
        }
    }
}

fn ssl_mode(value: &str) -> Result<MySqlSslMode> {
    Ok(match value.to_ascii_lowercase().as_str() {
        "disable" | "disabled" => MySqlSslMode::Disabled,
        "prefer" | "preferred" => MySqlSslMode::Preferred,
        "require" | "required" => MySqlSslMode::Required,
        "verify-ca" | "verify_ca" => MySqlSslMode::VerifyCa,
        "verify-full" | "verify_identity" => MySqlSslMode::VerifyIdentity,
        _ => anyhow::bail!("invalid MySQL database.ssl_mode"),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Server {
    MySql,
    MariaDb,
}

impl Server {
    fn detect(version: &str) -> Result<Self> {
        let (server, number, minimum) =
            if let Some(end) = version.to_ascii_lowercase().find("-mariadb") {
                (
                    Self::MariaDb,
                    version[..end].rsplit('-').next().unwrap_or(""),
                    [10, 6, 3],
                )
            } else {
                (
                    Self::MySql,
                    version.split('-').next().unwrap_or(""),
                    [8, 0, 19],
                )
            };
        let numbers = number
            .split('.')
            .map(str::parse::<u32>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        ensure!(
            numbers.len() == 3 && numbers.as_slice() >= minimum.as_slice(),
            "MySQL 8.0.19 or MariaDB 10.6.3 or newer is required"
        );
        Ok(server)
    }

    fn collation(self) -> &'static str {
        match self {
            Self::MySql => queries::MYSQL_COLLATION,
            Self::MariaDb => queries::MARIADB_COLLATION,
        }
    }
}

pub(super) async fn open(
    config: &Config,
    secrets: &Secrets,
    budget: &Budget,
    role: &str,
) -> Result<Backend> {
    ensure!(
        role == "single",
        "MySQL only supports single-container mode"
    );
    let database = &config.database;
    let options = MySqlConnectOptions::new()
        .host(&database.host)
        .port(database.effective_port().context("missing MySQL port")?)
        .database(&database.name)
        .username(&database.user)
        .password(&secrets.database_password)
        .ssl_mode(ssl_mode(&database.ssl_mode)?)
        .charset("utf8mb4")
        .collation("utf8mb4_bin");
    let mut owner = MySqlConnection::connect_with(&options)
        .await
        .map_err(|_| anyhow::anyhow!("cannot connect to MySQL"))?;
    let (version, durable) = queries::server().fetch_one(&mut owner).await?;
    let server = Server::detect(&version)?;
    sqlx::raw_sql(server.collation())
        .execute(&mut owner)
        .await?;
    ensure!(
        durable == 1,
        "MySQL innodb_flush_log_at_trx_commit must be 1"
    );
    ensure!(
        queries::claim_runtime().fetch_one(&mut owner).await? == Some(1),
        "another runtime owns this MySQL database"
    );
    sqlx::raw_sql(queries::SESSION_SETUP)
        .execute(&mut owner)
        .await?;
    migration::run(&mut owner).await?;
    let database_info = crate::model::runtime::DatabaseInfo {
        program: match server {
            Server::MySql => "MySQL",
            Server::MariaDb => "MariaDB",
        },
        version,
        schema_version: i64::from(queries::schema_version().fetch_one(&mut owner).await?),
        observed_at: chrono::Utc::now(),
        journal_mode: None,
        synchronous: None,
    };
    transactions::identify(
        &mut owner,
        blake3::hash(&secrets.credential_key.material).as_bytes(),
    )
    .await?;
    let pool = MySqlPoolOptions::new()
        .min_connections(0)
        .max_connections(budget.database_connections)
        .acquire_timeout(std::time::Duration::from_secs(15))
        .acquire_time_level(log::LevelFilter::Debug)
        .acquire_slow_threshold(std::time::Duration::from_millis(200))
        .idle_timeout(std::time::Duration::from_secs(60))
        .after_connect(move |connection, _| {
            Box::pin(async move {
                sqlx::raw_sql(server.collation())
                    .execute(&mut *connection)
                    .await?;
                sqlx::raw_sql(queries::SESSION_SETUP)
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await
        .map_err(|_| anyhow::anyhow!("cannot open MySQL pool"))?;
    Ok(Backend {
        database_info,
        pool,
        owner: tokio::sync::Mutex::new(owner),
        upload_admission: tokio::sync::Mutex::new(()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_versions_preserve_patch_and_product_boundaries() {
        for version in ["8.0.19", "8.0.46-ubuntu", "8.4.11", "9.7.1"] {
            assert_eq!(Server::detect(version).unwrap(), Server::MySql);
        }
        for version in [
            "10.6.3-MariaDB",
            "5.5.5-10.6.3-MariaDB-1:10.6.3",
            "12.3.3-mariadb",
        ] {
            assert_eq!(Server::detect(version).unwrap(), Server::MariaDb);
        }
        for version in [
            "8.0.18",
            "5.7.44",
            "10.6.2-MariaDB",
            "5.5.5-10.5.29-MariaDB",
            "8.0",
            "unknown",
        ] {
            assert!(Server::detect(version).is_err(), "{version}");
        }
    }

    #[test]
    fn mysql_tls_keeps_shared_configuration_names() {
        assert!(matches!(
            ssl_mode("prefer").unwrap(),
            MySqlSslMode::Preferred
        ));
        assert!(matches!(
            ssl_mode("require").unwrap(),
            MySqlSslMode::Required
        ));
        assert!(matches!(
            ssl_mode("verify-full").unwrap(),
            MySqlSslMode::VerifyIdentity
        ));
        assert!(ssl_mode("allow").is_err());
        assert!(ssl_mode("typo").is_err());
    }
}
