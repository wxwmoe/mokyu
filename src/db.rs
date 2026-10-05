use crate::config::{Budget, Config, Secrets};
use anyhow::{Context, Result, ensure};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::{Connection, PgConnection, PgPool, postgres::PgPoolOptions};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};

static MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

fn without_fields(mut value: Value, fields: &[&str]) -> Value {
    if let Some(object) = value.as_object_mut() {
        for field in fields {
            object.remove(*field);
        }
    }
    value
}

fn overlay<T>(current: &T, patch: &Value) -> Result<T>
where
    T: Serialize + DeserializeOwned,
{
    let mut value = serde_json::to_value(current)?;
    let object = value
        .as_object_mut()
        .context("runtime settings target must be an object")?;
    let patch = patch
        .as_object()
        .context("runtime settings value must be an object")?;
    for (key, value) in patch {
        object.insert(key.clone(), value.clone());
    }
    Ok(serde_json::from_value(value)?)
}

fn rename_fields(value: &mut Value, names: &[(&str, &str)]) {
    if let Some(object) = value.as_object_mut() {
        for (from, to) in names {
            if let Some(value) = object.remove(*from) {
                object.insert((*to).into(), value);
            }
        }
    }
}

pub fn runtime_defaults(configured: &Config) -> Vec<(&'static str, Value)> {
    vec![
        (
            "web",
            json!({
                "origin": configured.control.origin.clone(),
                "secure_cookie": configured.control.secure_cookie,
                "session_lifetime": configured.control.session_lifetime.clone(),
                "avatar_url": configured.control.gravatar_base_url.clone(),
            }),
        ),
        (
            "s3",
            json!({
                "domain": configured.listen.s3_domain.clone(),
                "signature_region": configured.listen.region.clone(),
                "max_stream_chunk_size": configured.listen.aws_chunk_limit.clone(),
            }),
        ),
        (
            "storage",
            json!({
                "connect_timeout": configured.storage.connect_timeout.clone(),
                "request_timeout": configured.storage.request_timeout.clone(),
                "max_retries": configured.storage.max_retries,
                "retry_timeout": configured.storage.retry_timeout.clone(),
                "priority_aging": configured.storage.priority_aging.clone(),
                "min_retention": configured.storage.min_storage_duration.clone(),
            }),
        ),
        (
            "requests",
            json!({
                "read_idle_timeout": configured.processing.read_idle_timeout.clone(),
                "upload_idle_timeout": configured.processing.upload_idle_timeout.clone(),
            }),
        ),
        (
            "uploads",
            json!({
                "idle_timeout": configured.multipart.idle_timeout.clone(),
                "sweep_interval": configured.multipart.sweep_interval.clone(),
                "client_idle_timeout": configured.multipart.client_idle_timeout.clone(),
                "max_active": configured.multipart.max_active_uploads,
            }),
        ),
        (
            "cache",
            json!({
                "upload_enabled": configured.cache.upload_cache,
                "min_savings_percent": configured.cache.min_compression_savings_percent,
            }),
        ),
        (
            "compression",
            without_fields(
                serde_json::to_value(&configured.compression).unwrap_or_default(),
                &["context_idle_timeout"],
            ),
        ),
        (
            "packing",
            without_fields(
                {
                    let mut value = serde_json::to_value(&configured.pack).unwrap_or_default();
                    rename_fields(&mut value, &[("range_optimization", "range_enabled")]);
                    value
                },
                &["maintenance_concurrency"],
            ),
        ),
        (
            "garbage_collection",
            {
                let mut value = serde_json::to_value(&configured.gc).unwrap_or_default();
                rename_fields(&mut value, &[("unreferenced_grace", "grace_period")]);
                value
            },
        ),
        (
            "retention",
            {
                let mut value = serde_json::to_value(&configured.cleanup).unwrap_or_default();
                rename_fields(&mut value, &[("deleted_chunk_retention", "deleted_chunks"), ("upload_retention", "uploads"), ("task_retention", "tasks"), ("audit_retention", "audit")]);
                value
            },
        ),
        (
            "statistics",
            {
                let mut value = serde_json::to_value(&configured.statistics).unwrap_or_default();
                rename_fields(&mut value, &[("storage_sample_interval", "storage_interval"), ("runtime_sample_interval", "runtime_interval")]);
                value
            },
        ),
        (
            "integrity",
            without_fields(
                serde_json::to_value(&configured.integrity).unwrap_or_default(),
                &["concurrency", "requests_per_second", "bandwidth"],
            ),
        ),
    ]
}

pub async fn ensure_runtime_settings(pool: &PgPool, configured: &Config) -> Result<()> {
    for (section, value) in runtime_defaults(configured) {
        sqlx::query(
            "INSERT INTO runtime_settings(section,value) VALUES($1,$2) ON CONFLICT (section) DO NOTHING",
        )
        .bind(section)
        .bind(value)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub async fn load_runtime_settings(pool: &PgPool, config: &mut Config) -> Result<HashMap<String, i64>> {
    let rows: Vec<(String, Value, i64)> =
        sqlx::query_as("SELECT section,value,revision FROM runtime_settings ORDER BY section")
            .fetch_all(pool)
            .await?;
    let mut revisions = HashMap::new();
    for (section, value, revision) in rows {
        apply_runtime_section(config, &section, value)?;
        revisions.insert(section, revision);
    }
    config.validate()?;
    Ok(revisions)
}

pub(crate) fn apply_runtime_section(config: &mut Config, section: &str, mut value: Value) -> Result<()> {
        match section {
            "web" => {
                value = without_fields(value, &["thumbnail_cache_size"]);
                rename_fields(&mut value, &[("avatar_url", "gravatar_base_url")]);
                config.control = overlay(&config.control, &value)?;
            }
            "s3" => {
                rename_fields(&mut value, &[("domain", "s3_domain"), ("signature_region", "region"), ("max_stream_chunk_size", "aws_chunk_limit")]);
                config.listen = overlay(&config.listen, &value)?;
            }
            "storage" => {
                rename_fields(&mut value, &[("min_retention", "min_storage_duration")]);
                config.storage = overlay(&config.storage, &value)?;
            }
            "requests" => config.processing = overlay(&config.processing, &value)?,
            "uploads" => {
                rename_fields(&mut value, &[("max_active", "max_active_uploads")]);
                config.multipart = overlay(&config.multipart, &value)?;
            }
            "cache" => {
                rename_fields(&mut value, &[("upload_enabled", "upload_cache"), ("min_savings_percent", "min_compression_savings_percent")]);
                config.cache = overlay(&config.cache, &value)?;
            }
            "compression" => {
                value = without_fields(value, &["context_idle_timeout"]);
                config.compression = overlay(&config.compression, &value)?;
            }
            "packing" => {
                value = without_fields(value, &["maintenance_concurrency"]);
                rename_fields(&mut value, &[("range_enabled", "range_optimization")]);
                config.pack = overlay(&config.pack, &value)?;
            }
            "garbage_collection" => {
                rename_fields(&mut value, &[("grace_period", "unreferenced_grace")]);
                config.gc = overlay(&config.gc, &value)?;
            }
            "retention" => {
                rename_fields(&mut value, &[("deleted_chunks", "deleted_chunk_retention"), ("uploads", "upload_retention"), ("tasks", "task_retention"), ("audit", "audit_retention")]);
                config.cleanup = overlay(&config.cleanup, &value)?;
            }
            "statistics" => {
                rename_fields(&mut value, &[("storage_interval", "storage_sample_interval"), ("runtime_interval", "runtime_sample_interval")]);
                config.statistics = overlay(&config.statistics, &value)?;
            }
            "integrity" => {
                value = without_fields(value, &["concurrency", "requests_per_second", "bandwidth"]);
                config.integrity = overlay(&config.integrity, &value)?;
            }
            _ => anyhow::bail!("unknown runtime settings section {section}"),
        }
    Ok(())
}

async fn load_database_keys(pool: &PgPool, secrets: &mut Secrets) -> Result<()> {
    let rows: Vec<(uuid::Uuid, String, String, String, Vec<u8>, Vec<u8>)> = sqlx::query_as(
        "SELECT id,name,purpose,algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE secret_encrypted IS NOT NULL ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    let mut keys = BTreeMap::new();
    let mut materials = HashSet::new();
    for (id, name, purpose, key_algorithm, encrypted, fingerprint) in rows {
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
        if purpose != "block" {
            continue;
        }
        ensure!(
            matches!(key_algorithm.as_str(), "aes-256-gcm" | "chacha20-poly1305"),
            "invalid managed encryption key algorithm"
        );
        let key = crate::codec::Key::new(&key_algorithm, material)?;
        keys.insert(name, key);
    }
    secrets.keys = keys;
    secrets
        .runtime_keys
        .write()
        .map_err(|_| anyhow::anyhow!("key registry is unavailable"))?
        .clear();
    Ok(())
}

pub(crate) async fn load_runtime_key(pool: &PgPool, secrets: &Secrets, name: &str) -> Result<()> {
    if secrets.key(name).is_some() {
        return Ok(());
    }
    let row: Option<(uuid::Uuid, String, String, String, Vec<u8>, Vec<u8>)> = sqlx::query_as(
        "SELECT id,name,purpose,algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE name=$1 AND secret_encrypted IS NOT NULL",
    )
    .bind(name)
    .fetch_optional(pool)
    .await?;
    let (id, stored_name, purpose, algorithm, encrypted, fingerprint) =
        row.context("managed key was not found")?;
    ensure!(purpose == "block", "managed key is not a block key");
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
    secrets.publish_key(stored_name, crate::codec::Key::new(&algorithm, material)?)
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
                "SELECT EXISTS(SELECT 1 FROM buckets) OR EXISTS(SELECT 1 FROM chunks) OR EXISTS(SELECT 1 FROM objects) OR EXISTS(SELECT 1 FROM streams) OR EXISTS(SELECT 1 FROM projects WHERE id <> '00000000-0000-0000-0000-000000000001') OR EXISTS(SELECT 1 FROM credentials) OR EXISTS(SELECT 1 FROM uploads) OR EXISTS(SELECT 1 FROM sessions) OR EXISTS(SELECT 1 FROM tasks) OR EXISTS(SELECT 1 FROM web_users) OR EXISTS(SELECT 1 FROM admin_setup)",
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
    load_database_keys(&pool, secrets).await?;
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
    let required: Vec<String> = sqlx::query_scalar("SELECT DISTINCT key_id FROM (SELECT key_id FROM chunk_locations WHERE algorithm<>'none' AND state<>'deleted' UNION SELECT key_id FROM packs WHERE algorithm<>'none' AND state<>'deleted' UNION SELECT key_id FROM chunks WHERE algorithm<>'none' AND EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=chunks.id)) keys").fetch_all(&pool).await?;
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
