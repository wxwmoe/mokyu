use crate::config::{Budget, Config, Secrets};
use anyhow::{Context, Result, ensure};
use sqlx::{Connection, PgConnection, PgPool, postgres::PgPoolOptions};

static MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub async fn connect(
    c: &Config,
    secrets: &Secrets,
    budget: &Budget,
) -> Result<(PgPool, PgConnection)> {
    let options = sqlx::postgres::PgConnectOptions::new()
        .host(&c.database.host)
        .port(c.database.port)
        .database(&c.database.name)
        .username(&c.database.user)
        .password(&secrets.database_password)
        .ssl_mode(c.database.ssl_mode.parse()?);
    let mut owner = PgConnection::connect_with(&options)
        .await
        .map_err(|_| anyhow::anyhow!("cannot connect to PostgreSQL"))?;
    let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(734922709851001)")
        .fetch_one(&mut owner)
        .await?;
    ensure!(locked, "another gateway owns this database");
    let pool = PgPoolOptions::new()
        .min_connections(0)
        .max_connections(budget.db_connections)
        .acquire_timeout(std::time::Duration::from_secs(15))
        .acquire_time_level(log::LevelFilter::Debug)
        .acquire_slow_threshold(std::time::Duration::from_millis(200))
        .idle_timeout(std::time::Duration::from_secs(60))
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET synchronous_commit = on")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SET statement_timeout = '30s'")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await
        .map_err(|_| anyhow::anyhow!("cannot open PostgreSQL pool"))?;
    let fsync: String = sqlx::query_scalar("SHOW fsync").fetch_one(&pool).await?;
    ensure!(fsync == "on", "PostgreSQL fsync must be enabled");
    let exists: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('public.mokyu_meta')::text")
            .fetch_one(&pool)
            .await?;
    let backend = format!(
        "{}|{}|{}",
        c.backend.endpoint,
        c.backend.bucket,
        c.backend.prefix.trim_end_matches('/')
    );
    let latest = MIGRATIONS
        .iter()
        .last()
        .context("no embedded database migrations")?
        .version;
    if exists.is_some() {
        let (version, identity): (i32, String) = sqlx::query_as(
            "SELECT schema_version,backend_identity FROM mokyu_meta WHERE singleton",
        )
        .fetch_one(&pool)
        .await?;
        ensure!(
            i64::from(version) <= latest,
            "database schema is newer than this binary; downgrade is unsupported"
        );
        ensure!(version >= 1, "unsupported database schema version");
        ensure!(
            identity == backend,
            "backend identity changed; restore its configuration before startup"
        );
    }
    // Migrate on the database owner connection, before listeners or background work start.
    sqlx::raw_sql("SET synchronous_commit = on; SET lock_timeout = '30s'")
        .execute(&mut owner)
        .await?;
    // The immutable baseline reads this historical migration parameter.
    sqlx::query("SELECT set_config('media_gateway.backend_identity',$1,false)")
        .bind(&backend)
        .execute(&mut owner)
        .await?;
    MIGRATIONS
        .run(&mut owner)
        .await
        .context("apply database migrations")?;
    let version: i32 = sqlx::query_scalar("SELECT schema_version FROM mokyu_meta WHERE singleton")
        .fetch_one(&pool)
        .await?;
    ensure!(
        i64::from(version) == latest,
        "schema version does not match the migration history"
    );
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
        sqlx::query("INSERT INTO key_fingerprints(key_id,algorithm,fingerprint) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
            .bind(id).bind(alg).bind(hash.as_bytes().as_slice()).execute(&pool).await?;
        let (saved_alg, saved): (String, Vec<u8>) =
            sqlx::query_as("SELECT algorithm,fingerprint FROM key_fingerprints WHERE key_id=$1")
                .bind(id)
                .fetch_one(&pool)
                .await?;
        ensure!(
            saved_alg == alg && saved == hash.as_bytes(),
            "key ID changed algorithm or material"
        );
    }
    let required: Vec<String> = sqlx::query_scalar("SELECT DISTINCT key_id FROM chunks WHERE algorithm <> 'none' AND state NOT IN ('deleted','failed')").fetch_all(&pool).await?;
    ensure!(
        required.iter().all(|id| secrets.keys.contains_key(id)),
        "keyring is missing a historical chunk key"
    );
    Ok((pool, owner))
}
