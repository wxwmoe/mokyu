use super::{migration, queries};
use anyhow::Result;
use sqlx::{Connection, Row, SqliteConnection, migrate::Migrate};
use std::collections::BTreeMap;

async fn contents(db: &mut SqliteConnection) -> Result<BTreeMap<String, Vec<String>>> {
    read_contents(db, true).await
}

async fn read_contents(
    db: &mut SqliteConnection,
    normalize: bool,
) -> Result<BTreeMap<String, Vec<String>>> {
    let mut data = BTreeMap::new();
    for table in queries::tables().fetch_all(&mut *db).await? {
        if normalize && table == "_sqlx_migrations" {
            continue;
        }
        let columns = sqlx::query(sqlx::AssertSqlSafe(format!(
            "PRAGMA table_info(\"{table}\")"
        )))
        .fetch_all(&mut *db)
        .await?;
        let mut fields = Vec::new();
        for column in columns {
            let name: String = column.try_get("name")?;
            let column_type: String = column.try_get("type")?;
            let value = if normalize && table == "mokyu_meta" && name == "deployment_id" {
                "'deployment'".to_owned()
            } else if normalize
                && matches!(
                    name.as_str(),
                    "created_at"
                        | "updated_at"
                        | "last_scheduled_at"
                        | "coverage_since"
                        | "flushed_at"
                        | "requested_at"
                )
            {
                format!("CASE WHEN \"{name}\">0 THEN 'timestamp' ELSE \"{name}\" END")
            } else if column_type == "BLOB" {
                format!("CASE WHEN \"{name}\" IS NOT NULL THEN hex(\"{name}\") END")
            } else {
                format!("\"{name}\"")
            };
            fields.push(format!("'{name}',{value}"));
        }
        let mut rows: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT json_object({}) FROM \"{table}\"",
            fields.join(",")
        )))
        .fetch_all(&mut *db)
        .await?;
        rows.sort();
        data.insert(table, rows);
    }
    Ok(data)
}

async fn structure(db: &mut SqliteConnection) -> Result<Vec<(String, String, String)>> {
    Ok(sqlx::query_as("SELECT type,name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' AND sql IS NOT NULL ORDER BY type,name").fetch_all(db).await?)
}

async fn same_structure(left: &mut SqliteConnection, right: &mut SqliteConnection) -> Result<()> {
    let left = structure(left).await?;
    let right = structure(right).await?;
    assert_eq!(left.len(), right.len());
    for (actual, expected) in left.iter().zip(&right) {
        assert_eq!(actual, expected, "{} {}", expected.0, expected.1);
    }
    Ok(())
}

async fn rejects_unchanged(db: &mut SqliteConnection, message: &str) -> Result<()> {
    let schema = structure(db).await?;
    let data = read_contents(db, false).await?;
    let error = migration::run(db).await.unwrap_err();
    assert!(format!("{error:#}").contains(message), "{error:#}");
    assert_eq!(structure(db).await?, schema);
    assert_eq!(read_contents(db, false).await?, data);
    Ok(())
}

#[tokio::test]
async fn sqlite_schema_bootstrap_contract() -> Result<()> {
    let mut fresh = SqliteConnection::connect("sqlite::memory:").await?;
    let mut history = SqliteConnection::connect("sqlite::memory:").await?;
    let migrations = sqlx::migrate!("./migrations/sqlite");
    migrations.run(&mut history).await?;
    migration::run(&mut fresh).await?;
    same_structure(&mut fresh, &mut history).await?;
    let plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "EXPLAIN QUERY PLAN {}",
        queries::WRITE_BACKLOG
    )))
    .fetch_all(&mut fresh)
    .await?;
    assert!(
        plan.iter()
            .all(|row| !matches!(row.3.as_str(), "SCAN r" | "SCAN c" | "SCAN l")),
        "backlog scans durable inventory: {plan:?}"
    );
    assert_eq!(contents(&mut fresh).await?, contents(&mut history).await?);
    assert_eq!(
        queries::migrations().fetch_all(&mut fresh).await?,
        queries::migrations().fetch_all(&mut history).await?
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM _sqlx_migrations WHERE execution_time<>-1"
        )
        .fetch_one(&mut fresh)
        .await?,
        0
    );
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&mut fresh)
            .await?
            .is_empty()
    );
    let before = contents(&mut fresh).await?;
    migration::run(&mut fresh).await?;
    assert_eq!(contents(&mut fresh).await?, before);

    // Validate history before upgrading an empty schema; preserve populated schemas unchanged.
    for version in [17, 22, 25, 42].into_iter().chain(
        migrations
            .iter()
            .map(|entry| entry.version)
            .filter(|version| *version >= 43),
    ) {
        let mut old = SqliteConnection::connect("sqlite::memory:").await?;
        old.ensure_migrations_table("_sqlx_migrations").await?;
        for entry in migrations
            .iter()
            .take_while(|entry| entry.version <= version)
        {
            old.apply("_sqlx_migrations", entry).await?;
        }
        let mut invalid_history = old.begin().await?;
        sqlx::query("DELETE FROM _sqlx_migrations WHERE version=1")
            .execute(&mut *invalid_history)
            .await?;
        rejects_unchanged(&mut invalid_history, "history").await?;
        invalid_history.rollback().await?;
        sqlx::raw_sql("INSERT INTO storages(id,name,type,path,identity_marker) VALUES(X'00000000000000000000000000000001','preserved','local','/data/preserved','{}'); INSERT INTO buckets(id,name,storage_id,project_id) VALUES(X'00000000000000000000000000000001','preserved',X'00000000000000000000000000000001',X'00000000000000000000000000000001');")
            .execute(&mut old).await?;
        if version >= 43 {
            migration::run(&mut old).await?;
            assert_eq!(
                sqlx::query_scalar::<_, String>("SELECT name FROM buckets")
                    .fetch_one(&mut old)
                    .await?,
                "preserved"
            );
            assert_eq!(
                sqlx::query_scalar::<_, String>("SELECT name FROM storages")
                    .fetch_one(&mut old)
                    .await?,
                "preserved"
            );
            migration::run(&mut old).await?;
            continue;
        }
        rejects_unchanged(&mut old, "empty store").await?;
        sqlx::raw_sql("DELETE FROM buckets; DELETE FROM storages;")
            .execute(&mut old)
            .await?;
        sqlx::query("UPDATE quota_scopes SET used_bytes=1")
            .execute(&mut old)
            .await?;
        rejects_unchanged(&mut old, "empty store").await?;
        sqlx::query("UPDATE quota_scopes SET used_bytes=0")
            .execute(&mut old)
            .await?;
        migration::run(&mut old).await?;
        same_structure(&mut old, &mut fresh).await?;
        assert_eq!(contents(&mut old).await?, contents(&mut fresh).await?);
    }

    for sql in [
        "CREATE TABLE unrelated(id INTEGER)",
        "CREATE VIEW unrelated AS SELECT 1",
        "CREATE TABLE mokyu_meta(id INTEGER,schema_version INTEGER)",
        "CREATE TABLE mokyu_meta(id INTEGER,schema_version INTEGER); INSERT INTO mokyu_meta VALUES(1,0)",
        "CREATE TABLE mokyu_meta(id INTEGER,schema_version INTEGER); INSERT INTO mokyu_meta VALUES(1,999)",
    ] {
        let mut invalid = SqliteConnection::connect("sqlite::memory:").await?;
        sqlx::raw_sql(sql).execute(&mut invalid).await?;
        rejects_unchanged(&mut invalid, "").await?;
    }
    for sql in [
        "UPDATE mokyu_meta SET schema_version=17",
        "DELETE FROM _sqlx_migrations WHERE version=1",
        "UPDATE _sqlx_migrations SET checksum=zeroblob(48) WHERE version=1",
        "UPDATE _sqlx_migrations SET success=0 WHERE version=18",
        "DELETE FROM mokyu_meta",
    ] {
        let mut invalid = SqliteConnection::connect("sqlite::memory:").await?;
        migration::run(&mut invalid).await?;
        sqlx::raw_sql(sql).execute(&mut invalid).await?;
        rejects_unchanged(&mut invalid, "").await?;
    }

    // Both DDL failure and failure at final metadata insertion roll back everything.
    for suffix in [
        "SELECT * FROM missing_bootstrap_table;",
        "CREATE TRIGGER interrupt_meta BEFORE INSERT ON mokyu_meta BEGIN SELECT RAISE(ABORT,'interrupted'); END;",
    ] {
        let mut interrupted = SqliteConnection::connect("sqlite::memory:").await?;
        let schema = format!("{}\n{suffix}", queries::SCHEMA);
        assert!(
            migration::initialize(&mut interrupted, &schema)
                .await
                .is_err()
        );
        assert!(queries::schema_empty().fetch_one(&mut interrupted).await?);
        migration::run(&mut interrupted).await?;
        assert_eq!(contents(&mut interrupted).await?, before);
    }
    Ok(())
}
