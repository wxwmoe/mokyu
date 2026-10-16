use super::queries;
use anyhow::{Context, Result, ensure};
use sqlx::{SqliteConnection, migrate::Migrate};

static MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/sqlite");

pub(super) async fn run(connection: &mut SqliteConnection) -> Result<()> {
    let latest = MIGRATIONS
        .iter()
        .last()
        .context("no embedded SQLite migrations")?
        .version;
    let tables = queries::tables().fetch_all(&mut *connection).await?;
    ensure!(
        tables.is_empty() || tables.iter().any(|name| name == "_sqlx_migrations"),
        "SQLite initialization requires an empty database"
    );
    if tables.iter().any(|name| name == "mokyu_meta") {
        ensure!(
            queries::schema_version()
                .fetch_optional(&mut *connection)
                .await?
                .is_none_or(|version| version <= latest),
            "database schema is newer than this binary; downgrade is unsupported"
        );
    }
    connection
        .ensure_migrations_table("_sqlx_migrations")
        .await?;
    let applied = queries::migrations().fetch_all(&mut *connection).await?;
    ensure!(
        !applied.is_empty() || tables.iter().all(|name| name == "_sqlx_migrations"),
        "SQLite migration history is missing"
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
            == latest,
        "schema version does not match the SQLite migration history"
    );
    Ok(())
}
