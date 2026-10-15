use super::queries;
use anyhow::{Context, Result, ensure};
use sqlx::{PgConnection, PgPool};

static MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
const LEGACY_SCHEMA_VERSION: i32 = 33;

pub(super) async fn run(pool: &PgPool, owner: &mut PgConnection, existing: bool) -> Result<()> {
    let latest = MIGRATIONS
        .iter()
        .last()
        .context("no embedded database migrations")?
        .version;
    if existing {
        let version: i32 = queries::schema_version().fetch_one(pool).await?;
        ensure!(
            i64::from(version) <= latest,
            "database schema is newer than this binary; downgrade is unsupported"
        );
        ensure!(version >= 1, "unsupported database schema version");
        ensure!(
            version >= LEGACY_SCHEMA_VERSION,
            "0.0.5 requires a fresh database or an empty 0.0.4 schema at migration version 33; migrate logical objects through S3"
        );
    }
    // Migrate before listeners or background work start. The migrator serializes
    // concurrent cluster upgrades; cluster roles do not hold a runtime session lock.
    sqlx::raw_sql(queries::MIGRATION_SETUP)
        .execute(&mut *owner)
        .await?;
    // The immutable baseline reads this historical migration parameter.
    queries::migration_identity("storage-registry")
        .execute(&mut *owner)
        .await?;
    MIGRATIONS
        .run(&mut *owner)
        .await
        .context("apply database migrations")?;
    let version: i32 = queries::schema_version().fetch_one(pool).await?;
    ensure!(
        i64::from(version) == latest,
        "schema version does not match the migration history"
    );
    Ok(())
}
