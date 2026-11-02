use super::queries;
use anyhow::{Context, Result, ensure};
use sqlx::{Connection, MySqlConnection, migrate::Migrate};

static MIGRATIONS: sqlx::migrate::Migrator = {
    let mut migrator = sqlx::migrate!("./src/model/backend/mysql/migrations");
    migrator.locking = false;
    migrator
};
const BASELINE_VERSION: i64 = 20261102022127;
const SCHEMA_VERSION: i64 = 20261102213026;

pub(super) async fn run(owner: &mut MySqlConnection) -> Result<()> {
    ensure!(
        MIGRATIONS.iter().last().map(|m| m.version) == Some(SCHEMA_VERSION),
        "MySQL schema snapshot is out of date"
    );
    if queries::schema_empty().fetch_one(&mut *owner).await? {
        return initialize(owner, queries::SCHEMA).await;
    }
    upgrade(owner, &MIGRATIONS).await
}

pub(super) async fn upgrade(
    owner: &mut MySqlConnection,
    migrations: &sqlx::migrate::Migrator,
) -> Result<()> {
    let latest = migrations
        .iter()
        .last()
        .context("empty migration chain")?
        .version;
    let version = queries::schema_version().fetch_one(&mut *owner).await
        .context("nonempty database requires valid mokyu_meta.schema_version; initialization is incomplete or the database is unsupported")?;
    ensure!(
        version <= latest,
        "database schema is newer than this binary; downgrade is unsupported"
    );
    ensure!(
        version >= BASELINE_VERSION,
        "unsupported database schema version"
    );
    let applied = queries::migrations().fetch_all(&mut *owner).await?;
    ensure!(
        applied.last().map(|m| m.0) == Some(version),
        "schema version does not match the MySQL migration history"
    );
    ensure!(
        applied.len() <= migrations.iter().len(),
        "migration history exceeds this binary's migration chain"
    );
    for ((version, success, checksum), migration) in applied.iter().zip(migrations.iter()) {
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
    if version < latest {
        migrations
            .run(&mut *owner)
            .await
            .context("apply database migrations")?;
        ensure!(
            queries::schema_version().fetch_one(&mut *owner).await? == latest,
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
    queries::initialize_meta(SCHEMA_VERSION)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
