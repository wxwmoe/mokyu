use super::queries;
use anyhow::{Context, Result, ensure};
use sqlx::{Connection, MySqlConnection, migrate::Migrate};

static MIGRATIONS: sqlx::migrate::Migrator = {
    let mut migrator = sqlx::migrate!("./migrations/mysql");
    migrator.locking = false;
    migrator
};
const SCHEMA_VERSION: i64 = 47;
// Only the 0.0.6 format boundary requires an empty store.
const EMPTY_UPGRADE_VERSION: i64 = 42;

pub(super) async fn run(owner: &mut MySqlConnection) -> Result<()> {
    ensure!(
        MIGRATIONS.iter().last().map(|m| m.version) == Some(SCHEMA_VERSION),
        "MySQL schema snapshot is out of date"
    );
    if queries::schema_empty().fetch_one(&mut *owner).await? {
        return initialize(owner, queries::SCHEMA).await;
    }
    let version = i64::from(queries::schema_version().fetch_one(&mut *owner).await
        .context("nonempty database requires valid mokyu_meta.schema_version; initialization is incomplete or the database is unsupported")?);
    ensure!(
        version <= SCHEMA_VERSION,
        "database schema is newer than this binary; downgrade is unsupported"
    );
    ensure!(version >= 1, "unsupported database schema version");
    let applied = queries::migrations().fetch_all(&mut *owner).await?;
    ensure!(
        applied.last().map(|m| m.0) == Some(version),
        "schema version does not match the MySQL migration history"
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
        ensure!(
            *success,
            "MySQL migration history contains a failed transaction"
        );
    }
    if version < SCHEMA_VERSION {
        if version < EMPTY_UPGRADE_VERSION {
            for table in queries::upgrade_tables().fetch_all(&mut *owner).await? {
                ensure!(
                    !queries::upgrade_has_data(&table)
                        .fetch_one(&mut *owner)
                        .await?,
                    "database upgrade requires an empty store (table {table})"
                );
            }
        }
        MIGRATIONS
            .run(&mut *owner)
            .await
            .context("apply database migrations")?;
        ensure!(
            i64::from(queries::schema_version().fetch_one(&mut *owner).await?) == SCHEMA_VERSION,
            "schema version does not match the migration history"
        );
    }
    Ok(())
}

pub(super) async fn initialize(owner: &mut MySqlConnection, schema: &str) -> Result<()> {
    // DDL commits implicitly; an interrupted bootstrap stays unversioned and fails closed.
    sqlx::raw_sql(sqlx::AssertSqlSafe(schema))
        .execute(&mut *owner)
        .await
        .context("initialize MySQL schema")?;
    owner.ensure_migrations_table("_sqlx_migrations").await?;
    let mut tx = owner.begin().await?;
    for migration in MIGRATIONS.iter() {
        tx.skip("_sqlx_migrations", migration).await?;
    }
    queries::initialize_meta(SCHEMA_VERSION as i32)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
