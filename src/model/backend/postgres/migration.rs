use super::queries;
use anyhow::{Context, Result, ensure};
use sqlx::{Connection, PgConnection, migrate::Migrate};

static MIGRATIONS: sqlx::migrate::Migrator = {
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.locking = false;
    migrator
};
const SCHEMA_VERSION: i64 = 72;
// Only the 0.0.6 format boundary requires an empty store.
const EMPTY_UPGRADE_VERSION: i64 = 71;

pub(super) async fn run(owner: &mut PgConnection) -> Result<()> {
    sqlx::raw_sql(queries::MIGRATION_SETUP)
        .execute(&mut *owner)
        .await?;
    // Serialize empty/current decisions across control processes.
    owner.lock().await?;
    let result = run_locked(owner).await;
    let unlock = owner.unlock().await;
    result?;
    unlock?;
    Ok(())
}

async fn run_locked(owner: &mut PgConnection) -> Result<()> {
    ensure!(
        MIGRATIONS.iter().last().map(|m| m.version) == Some(SCHEMA_VERSION),
        "PostgreSQL schema snapshot is out of date"
    );
    if queries::schema_empty().fetch_one(&mut *owner).await? {
        return initialize(owner, queries::SCHEMA).await;
    }
    let version = queries::schema_version().fetch_one(&mut *owner).await
        .context("nonempty database requires valid mokyu_meta.schema_version; initialization is incomplete or the database is unsupported")?;
    ensure!(
        i64::from(version) <= SCHEMA_VERSION,
        "database schema is newer than this binary; downgrade is unsupported"
    );
    ensure!(version >= 33, "unsupported database schema version");
    ensure!(
        owner.dirty_version("_sqlx_migrations").await?.is_none(),
        "PostgreSQL migration history contains a failed transaction"
    );
    let applied = owner.list_applied_migrations("_sqlx_migrations").await?;
    ensure!(
        applied.last().map(|m| m.version) == Some(i64::from(version)),
        "schema version does not match the migration history"
    );
    for (index, applied) in applied.iter().enumerate() {
        let migration = MIGRATIONS
            .iter()
            .nth(index)
            .context("applied PostgreSQL migration is missing from this binary")?;
        ensure!(
            applied.version == migration.version,
            "PostgreSQL migration history has a missing or unknown version"
        );
        ensure!(
            applied.checksum == migration.checksum,
            "PostgreSQL migration checksum changed"
        );
    }
    if i64::from(version) < SCHEMA_VERSION {
        let mut tx = owner.begin().await?;
        if i64::from(version) < EMPTY_UPGRADE_VERSION {
            for table in queries::upgrade_tables().fetch_all(&mut *tx).await? {
                queries::lock_upgrade_table(&table)
                    .execute(&mut *tx)
                    .await?;
                ensure!(
                    !queries::upgrade_has_data(&table)
                        .fetch_one(&mut *tx)
                        .await?,
                    "database upgrade requires an empty store (table {table})"
                );
            }
        }
        MIGRATIONS
            .run(&mut *tx)
            .await
            .context("apply database migrations")?;
        ensure!(
            i64::from(queries::schema_version().fetch_one(&mut *tx).await?) == SCHEMA_VERSION,
            "schema version does not match the migration history"
        );
        tx.commit().await?;
    }
    Ok(())
}

pub(super) async fn initialize(owner: &mut PgConnection, schema: &str) -> Result<()> {
    let mut tx = owner.begin().await?;
    sqlx::raw_sql(sqlx::AssertSqlSafe(schema))
        .execute(&mut *tx)
        .await
        .context("initialize PostgreSQL schema")?;
    tx.ensure_migrations_table("_sqlx_migrations").await?;
    for migration in MIGRATIONS.iter() {
        tx.skip("_sqlx_migrations", migration).await?;
    }
    queries::initialize_meta(SCHEMA_VERSION as i32)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
