use super::queries;
use anyhow::{Context, Result, ensure};
use sqlx::{Connection, SqliteConnection, migrate::Migrate};

static MIGRATIONS: sqlx::migrate::Migrator =
    sqlx::migrate!("./src/model/backend/sqlite/migrations");
const BASELINE_VERSION: i64 = 20261102022127;
const SCHEMA_VERSION: i64 = 20261102203720;

pub(super) async fn run(connection: &mut SqliteConnection) -> Result<()> {
    ensure!(
        MIGRATIONS.iter().last().map(|m| m.version) == Some(SCHEMA_VERSION),
        "SQLite schema snapshot is out of date"
    );
    if queries::schema_empty().fetch_one(&mut *connection).await? {
        return initialize(connection, queries::SCHEMA).await;
    }
    upgrade(connection, &MIGRATIONS).await
}

pub(super) async fn upgrade(
    connection: &mut SqliteConnection,
    migrations: &sqlx::migrate::Migrator,
) -> Result<()> {
    let latest = migrations
        .iter()
        .last()
        .context("empty migration chain")?
        .version;
    let version = queries::schema_version().fetch_one(&mut *connection).await
        .context("nonempty database requires valid mokyu_meta.schema_version; initialization is incomplete or the database is unsupported")?;
    ensure!(
        version <= latest,
        "database schema is newer than this binary; downgrade is unsupported"
    );
    ensure!(
        version >= BASELINE_VERSION,
        "unsupported database schema version"
    );
    let applied = queries::migrations().fetch_all(&mut *connection).await?;
    ensure!(
        applied.last().map(|m| m.0) == Some(version),
        "schema version does not match the SQLite migration history"
    );
    ensure!(
        applied.len() <= migrations.iter().len(),
        "migration history exceeds this binary's migration chain"
    );
    for ((version, success, checksum), migration) in applied.iter().zip(migrations.iter()) {
        ensure!(
            *version == migration.version,
            "SQLite migration history has a missing or unknown version"
        );
        ensure!(
            *success,
            "SQLite migration history contains a failed transaction"
        );
        ensure!(
            *checksum == migration.checksum.as_ref(),
            "SQLite migration checksum changed"
        );
    }
    if version < latest {
        let mut tx = connection.begin_with(queries::BEGIN_WRITE).await?;
        migrations
            .run(&mut *tx)
            .await
            .context("apply database migrations")?;
        ensure!(
            queries::schema_version().fetch_one(&mut *tx).await? == latest,
            "schema version does not match the migration history"
        );
        tx.commit().await?;
    }
    Ok(())
}

pub(super) async fn initialize(connection: &mut SqliteConnection, schema: &str) -> Result<()> {
    let mut tx = connection.begin_with(queries::BEGIN_WRITE).await?;
    sqlx::raw_sql(sqlx::AssertSqlSafe(schema))
        .execute(&mut *tx)
        .await
        .context("initialize SQLite schema")?;
    tx.ensure_migrations_table("_sqlx_migrations").await?;
    for migration in MIGRATIONS.iter() {
        tx.skip("_sqlx_migrations", migration).await?;
    }
    queries::initialize_meta(SCHEMA_VERSION)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
