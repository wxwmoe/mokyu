use super::queries;
use anyhow::{Context, Result, ensure};
use sqlx::{Connection, MySqlConnection, migrate::Migrate};

static MIGRATIONS: sqlx::migrate::Migrator = {
    let mut migrator = sqlx::migrate!("./migrations/mysql");
    migrator.locking = false;
    migrator
};
const SCHEMA_VERSION: i64 = 31;

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
    if i64::from(version) < 25 {
        ensure!(
            !queries::incompatible_payload(version)
                .fetch_one(&mut *owner)
                .await?,
            "file identity format requires an empty store; migrate logical objects through S3"
        );
    }
    ensure!(version >= 1, "unsupported database schema version");
    let applied = queries::migrations().fetch_all(&mut *owner).await?;
    let last = applied
        .last()
        .context("MySQL migration history is missing")?;
    ensure!(
        version == last.0 || (!last.1 && version + 1 == last.0),
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
        if !success {
            // Only explicitly idempotent migrations may resume after MySQL's implicit DDL commits.
            ensure!(
                matches!(*version, 1..=20 | 24..=29) && index + 1 == applied.len(),
                "MySQL migration requires manual recovery"
            );
        }
    }
    if !last.1 {
        queries::forget_failed_migration(last.0)
            .execute(&mut *owner)
            .await?;
    }
    // The dedicated connection already owns the database for this single runtime.
    MIGRATIONS
        .run(&mut *owner)
        .await
        .context("apply MySQL migrations")?;
    ensure!(
        i64::from(queries::schema_version().fetch_one(&mut *owner).await?) == SCHEMA_VERSION,
        "schema version does not match the MySQL migration history"
    );
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
