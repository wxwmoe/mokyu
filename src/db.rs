use crate::config::{Budget, Config, Secrets};
use anyhow::{Context, Result, ensure};
use sqlx::{Connection, PgConnection, PgPool, postgres::PgPoolOptions};
use std::collections::{BTreeMap, HashSet};

static MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

async fn load_database_keys(pool: &PgPool, secrets: &mut Secrets, algorithm: &str) -> Result<()> {
    let rows: Vec<(uuid::Uuid, String, String, String, Vec<u8>, Vec<u8>, bool)> = sqlx::query_as(
        "SELECT id,name,purpose,algorithm,secret_encrypted,fingerprint,active FROM managed_keys ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    let mut keys = BTreeMap::new();
    let mut materials = HashSet::new();
    let mut active = None;
    for (id, name, purpose, key_algorithm, encrypted, fingerprint, enabled) in rows {
        let material = crate::codec::unprotect_bytes(
            &encrypted,
            &secrets.credential_key,
            format!("managed-key-{id}").as_bytes(),
        )?;
        let material: [u8; 32] = material
            .try_into()
            .map_err(|_| anyhow::anyhow!("managed key material must be 32 bytes"))?;
        ensure!(
            fingerprint == blake3::hash(&material).as_bytes(),
            "managed key fingerprint mismatch"
        );
        ensure!(materials.insert(material), "managed keys must be unique");
        if purpose != "encryption" {
            ensure!(!enabled, "only encryption keys can be active");
            continue;
        }
        ensure!(
            matches!(key_algorithm.as_str(), "aes-256-gcm" | "chacha20-poly1305"),
            "invalid managed encryption key algorithm"
        );
        let key = crate::codec::Key::new(&key_algorithm, material)?;
        if enabled {
            ensure!(active.is_none(), "multiple active encryption keys");
            active = Some(name.clone());
        }
        keys.insert(name, key);
    }
    if let Some(active_key) = active.as_deref() {
        ensure!(
            algorithm == "none"
                || keys
                    .get(active_key)
                    .is_some_and(|key| key.algorithm == algorithm),
            "active database key algorithm does not match encryption.algorithm"
        );
    }
    secrets.active_key = active.unwrap_or_default();
    secrets.keys = keys;
    Ok(())
}

pub async fn connect(
    c: &Config,
    secrets: &mut Secrets,
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
    let previous_version: Option<i32> = if exists.is_some() {
        Some(
            sqlx::query_scalar("SELECT schema_version FROM mokyu_meta WHERE singleton")
                .fetch_one(&pool)
                .await?,
        )
    } else {
        None
    };
    let cluster_mode = if previous_version.is_some_and(|version| version >= 22) {
        sqlx::query_scalar::<_, String>("SELECT deployment_mode FROM mokyu_meta WHERE singleton")
            .fetch_optional(&pool)
            .await?
            .as_deref()
            == Some("cluster")
    } else {
        false
    };
    if cluster_mode {
        sqlx::query("SELECT pg_advisory_lock(734922709851001)")
            .execute(&mut owner)
            .await?;
    } else {
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(734922709851001)")
            .fetch_one(&mut owner)
            .await?;
        ensure!(locked, "another gateway owns this database");
    }
    let storage_identity = "storage-registry";
    let latest = MIGRATIONS
        .iter()
        .last()
        .context("no embedded database migrations")?
        .version;
    if exists.is_some() {
        let version: i32 =
            sqlx::query_scalar("SELECT schema_version FROM mokyu_meta WHERE singleton")
                .fetch_one(&pool)
                .await?;
        ensure!(
            i64::from(version) <= latest,
            "database schema is newer than this binary; downgrade is unsupported"
        );
        ensure!(version >= 1, "unsupported database schema version");
        if version < 22 {
            ensure!(
                version == 21,
                "0.0.4 requires a fresh database or an empty 0.0.3 schema"
            );
            let used: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM buckets) OR EXISTS(SELECT 1 FROM chunks) OR EXISTS(SELECT 1 FROM objects) OR EXISTS(SELECT 1 FROM streams) OR EXISTS(SELECT 1 FROM projects WHERE id <> '00000000-0000-0000-0000-000000000001') OR EXISTS(SELECT 1 FROM credentials) OR EXISTS(SELECT 1 FROM uploads) OR EXISTS(SELECT 1 FROM sessions) OR EXISTS(SELECT 1 FROM tasks) OR EXISTS(SELECT 1 FROM web_users) OR EXISTS(SELECT 1 FROM manage_setup)",
            )
            .fetch_one(&pool)
            .await?;
            ensure!(
                !used,
                "0.0.3 data is not importable; start with a fresh database and migrate files manually"
            );
        }
    }
    // Migrate on the database owner connection, before listeners or background work start.
    sqlx::raw_sql("SET synchronous_commit = on; SET lock_timeout = '30s'")
        .execute(&mut owner)
        .await?;
    // The immutable baseline reads this historical migration parameter.
    sqlx::query("SELECT set_config('media_gateway.backend_identity',$1,false)")
        .bind(storage_identity)
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
    load_database_keys(&pool, secrets, &c.encryption.algorithm).await?;
    sqlx::query("UPDATE mokyu_meta SET storage_identity=$1 WHERE singleton")
        .bind(storage_identity)
        .execute(&pool)
        .await?;
    let deployment_fingerprint = blake3::hash(&secrets.credential_key.material);
    let saved: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT deployment_key_fingerprint FROM mokyu_meta WHERE singleton")
            .fetch_one(&pool)
            .await?;
    if let Some(saved) = saved {
        ensure!(
            saved == deployment_fingerprint.as_bytes(),
            "deployment key does not match this database"
        );
    } else {
        sqlx::query("UPDATE mokyu_meta SET deployment_key_fingerprint=$1 WHERE singleton")
            .bind(deployment_fingerprint.as_bytes().as_slice())
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
        "database is missing a historical chunk key"
    );
    if cluster_mode {
        sqlx::query("SELECT pg_advisory_unlock(734922709851001)")
            .execute(&mut owner)
            .await?;
    }
    Ok((pool, owner))
}
