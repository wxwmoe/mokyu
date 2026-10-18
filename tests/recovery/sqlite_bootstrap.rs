use super::{migration, queries};
use anyhow::Result;
use sqlx::{Connection, Row, SqliteConnection, migrate::Migrate};
use std::collections::BTreeMap;

async fn contents(db: &mut SqliteConnection) -> Result<BTreeMap<String, Vec<String>>> {
    let mut data = BTreeMap::new();
    for table in queries::tables().fetch_all(&mut *db).await? {
        if table == "_sqlx_migrations" {
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
            let value = if table == "mokyu_meta" && name == "deployment_id" {
                "'deployment'".to_owned()
            } else if matches!(
                name.as_str(),
                "created_at"
                    | "updated_at"
                    | "last_scheduled_at"
                    | "coverage_since"
                    | "flushed_at"
                    | "requested_at"
            ) {
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

#[tokio::test]
async fn sqlite_schema_bootstrap_contract() -> Result<()> {
    let mut fresh = SqliteConnection::connect("sqlite::memory:").await?;
    let mut history = SqliteConnection::connect("sqlite::memory:").await?;
    let migrations = sqlx::migrate!("./migrations/sqlite");
    migrations.run(&mut history).await?;
    migration::run(&mut fresh).await?;
    assert_eq!(structure(&mut fresh).await?, structure(&mut history).await?);
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

    // Upgrade an actual old schema with data; preserve identity and user values.
    let mut old = SqliteConnection::connect("sqlite::memory:").await?;
    old.ensure_migrations_table("_sqlx_migrations").await?;
    for migration in migrations.iter().take(17) {
        old.apply("_sqlx_migrations", migration).await?;
    }
    sqlx::query("INSERT INTO runtime_settings(section,value) VALUES('preserved','{}')")
        .execute(&mut old)
        .await?;
    sqlx::raw_sql("INSERT INTO audit_events(id,actor_label,source,action) VALUES(41,'operator','cli','preserved'),(99,'operator','cli','removed'); DELETE FROM audit_events WHERE id=99;")
        .execute(&mut old).await?;
    let id: Vec<u8> = sqlx::query_scalar("SELECT deployment_id FROM mokyu_meta")
        .fetch_one(&mut old)
        .await?;
    migration::run(&mut old).await?;
    assert_eq!(structure(&mut old).await?, structure(&mut fresh).await?);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT action FROM audit_events WHERE id=41")
            .fetch_one(&mut old)
            .await?,
        "preserved"
    );
    assert_eq!(sqlx::query("INSERT INTO audit_events(actor_label,source,action) VALUES('credential','s3','source.recover')").execute(&mut old).await?.last_insert_rowid(), 100, "table rebuild must preserve deleted audit identifiers");
    assert_eq!(
        sqlx::query_scalar::<_, Vec<u8>>("SELECT deployment_id FROM mokyu_meta")
            .fetch_one(&mut old)
            .await?,
        id
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT value FROM runtime_settings WHERE section='preserved'"
        )
        .fetch_one(&mut old)
        .await?,
        "{}"
    );

    for sql in [
        "CREATE TABLE unrelated(id INTEGER)",
        "CREATE VIEW unrelated AS SELECT 1",
        "CREATE TABLE mokyu_meta(id INTEGER,schema_version INTEGER)",
        "CREATE TABLE mokyu_meta(id INTEGER,schema_version INTEGER); INSERT INTO mokyu_meta VALUES(1,0)",
        "CREATE TABLE mokyu_meta(id INTEGER,schema_version INTEGER); INSERT INTO mokyu_meta VALUES(1,999)",
    ] {
        let mut invalid = SqliteConnection::connect("sqlite::memory:").await?;
        sqlx::raw_sql(sql).execute(&mut invalid).await?;
        let before = structure(&mut invalid).await?;
        assert!(migration::run(&mut invalid).await.is_err());
        assert_eq!(structure(&mut invalid).await?, before);
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
        let before = queries::migrations().fetch_all(&mut invalid).await?;
        assert!(migration::run(&mut invalid).await.is_err());
        assert_eq!(queries::migrations().fetch_all(&mut invalid).await?, before);
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
