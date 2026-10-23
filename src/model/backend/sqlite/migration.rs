use super::queries;
use anyhow::{Context, Result, ensure};
use sqlx::{Connection, SqliteConnection, migrate::Migrate};

static MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/sqlite");
const SCHEMA_VERSION: i64 = 40;

pub(super) async fn run(connection: &mut SqliteConnection) -> Result<()> {
    ensure!(
        MIGRATIONS.iter().last().map(|m| m.version) == Some(SCHEMA_VERSION),
        "SQLite schema snapshot is out of date"
    );
    if queries::schema_empty().fetch_one(&mut *connection).await? {
        return initialize(connection, queries::SCHEMA).await;
    }
    let version = queries::schema_version().fetch_one(&mut *connection).await
        .context("nonempty database requires valid mokyu_meta.schema_version; initialization is incomplete or the database is unsupported")?;
    ensure!(
        version <= SCHEMA_VERSION,
        "database schema is newer than this binary; downgrade is unsupported"
    );
    if i64::from(version) < 26 {
        ensure!(
            !queries::incompatible_payload(version)
                .fetch_one(&mut *connection)
                .await?,
            "file identity format requires an empty store; migrate logical objects through S3"
        );
    }
    ensure!(version >= 1, "unsupported database schema version");
    let applied = queries::migrations().fetch_all(&mut *connection).await?;
    ensure!(
        applied.last().map(|m| m.0) == Some(version),
        "schema version does not match the SQLite migration history"
    );
    for (index, (version, success, checksum)) in applied.iter().enumerate() {
        let migration = MIGRATIONS
            .iter()
            .nth(index)
            .context("applied SQLite migration is missing from this binary")?;
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
    MIGRATIONS
        .run(&mut *connection)
        .await
        .context("apply SQLite migrations")?;
    ensure!(
        queries::schema_version()
            .fetch_one(&mut *connection)
            .await?
            == SCHEMA_VERSION,
        "schema version does not match the SQLite migration history"
    );
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
