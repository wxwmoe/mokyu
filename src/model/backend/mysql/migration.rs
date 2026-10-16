use super::queries;
use anyhow::{Context, Result, ensure};
use sqlx::{MySqlConnection, migrate::Migrate};

static MIGRATIONS: sqlx::migrate::Migrator = {
    let mut migrator = sqlx::migrate!("./migrations/mysql");
    migrator.locking = false;
    migrator
};

pub(super) async fn run(owner: &mut MySqlConnection) -> Result<()> {
    let latest = MIGRATIONS
        .iter()
        .last()
        .context("no embedded MySQL migrations")?
        .version;
    let tables = queries::tables().fetch_all(&mut *owner).await?;
    ensure!(
        tables.is_empty() || tables.iter().any(|name| name == "_sqlx_migrations"),
        "MySQL initialization requires an empty database"
    );
    if tables.iter().any(|name| name == "mokyu_meta") {
        let version = queries::schema_version()
            .fetch_optional(&mut *owner)
            .await?;
        ensure!(
            version.is_none_or(|v| i64::from(v) <= latest),
            "database schema is newer than this binary; downgrade is unsupported"
        );
    }
    owner.ensure_migrations_table("_sqlx_migrations").await?;
    let applied = queries::migrations().fetch_all(&mut *owner).await?;
    ensure!(
        !applied.is_empty() || tables.iter().all(|name| name == "_sqlx_migrations"),
        "MySQL migration history is missing"
    );
    for (index, (version, success, checksum)) in applied.iter().enumerate() {
        let migration = MIGRATIONS
            .iter()
            .nth(index)
            .context("applied MySQL migration is missing from this binary")?;
        ensure!(
            *version == migration.version,
            "MySQL migration history has a missing or unknown version"
        );
        ensure!(
            *checksum == migration.checksum.as_ref(),
            "MySQL migration checksum changed"
        );
        if !success {
            // Only explicitly idempotent migrations may resume after MySQL's implicit DDL commits.
            ensure!(
                matches!(*version, 1..=16) && index + 1 == applied.len(),
                "MySQL migration requires manual recovery"
            );
            queries::forget_failed_migration(*version)
                .execute(&mut *owner)
                .await?;
        }
    }
    // The dedicated connection already owns the database for this single runtime.
    MIGRATIONS
        .run(&mut *owner)
        .await
        .context("apply MySQL migrations")?;
    ensure!(
        i64::from(queries::schema_version().fetch_one(&mut *owner).await?) == latest,
        "schema version does not match the MySQL migration history"
    );
    Ok(())
}
