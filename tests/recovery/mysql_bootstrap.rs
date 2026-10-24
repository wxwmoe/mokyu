use super::{migration, queries};
use crate::config::Config;
use anyhow::Result;
use serde_json::Value;
use sqlx::{Connection, MySqlConnection, Row, migrate::Migrate, mysql::MySqlConnectOptions};
use std::collections::BTreeMap;

async fn database(
    admin: &mut MySqlConnection,
    options: &MySqlConnectOptions,
) -> Result<MySqlConnection> {
    let name = format!("mokyu_bootstrap_{}", uuid::Uuid::new_v4().simple());
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE DATABASE {name} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
    )))
    .execute(admin)
    .await?;
    let mut db = MySqlConnection::connect_with(&options.clone().database(&name)).await?;
    sqlx::raw_sql(queries::SESSION_SETUP)
        .execute(&mut db)
        .await?;
    Ok(db)
}

fn constraint(line: &str, table: &str, maria: bool) -> String {
    let line = line.trim_end_matches(',');
    if let Some((name, definition)) = line
        .trim_start()
        .strip_prefix("CONSTRAINT `")
        .and_then(|rest| rest.split_once('`'))
    {
        let numbered =
            |name: &str| !name.is_empty() && name.bytes().all(|byte| byte.is_ascii_digit());
        // MariaDB can assign numeric names to anonymous foreign keys during ALTER TABLE.
        let anonymous = name
            .strip_prefix(&format!("{table}_ibfk_"))
            .is_some_and(numbered)
            || (maria && numbered(name));
        if anonymous && definition.starts_with(" FOREIGN KEY ") {
            return format!("  {}", definition.trim_start());
        }
    }
    line.to_owned()
}

#[test]
fn automatic_foreign_key_names_do_not_hide_structural_differences() {
    let definition =
        " FOREIGN KEY (`file_id`) REFERENCES `multipart_files` (`id`) ON DELETE CASCADE";
    let old = format!("  CONSTRAINT `fragments_ibfk_1`{definition},");
    let new = format!("  CONSTRAINT `1`{definition},");
    assert_eq!(
        constraint(&old, "fragments", true),
        constraint(&new, "fragments", true)
    );
    assert_ne!(
        constraint(&old, "fragments", false),
        constraint(&new, "fragments", false)
    );
    for name in ["fragments_file", "other_ibfk_1", "fragments_ibfk_one"] {
        let line = format!("  CONSTRAINT `{name}`{definition}");
        assert_eq!(constraint(&line, "fragments", true), line);
    }
    assert_ne!(
        constraint(&old, "fragments", true),
        constraint(&new.replace("CASCADE", "RESTRICT"), "fragments", true)
    );
    let check = "  CONSTRAINT `1` CHECK (`size` >= 0)";
    assert_eq!(constraint(check, "fragments", true), check);
}

async fn structure(db: &mut MySqlConnection) -> Result<BTreeMap<String, String>> {
    let name: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(&mut *db)
        .await?;
    let tables: Vec<String> = sqlx::query_scalar("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() ORDER BY TABLE_NAME").fetch_all(&mut *db).await?;
    let mut definitions = BTreeMap::new();
    let maria: bool = sqlx::query_scalar("SELECT VERSION() LIKE '%MariaDB%'")
        .fetch_one(&mut *db)
        .await?;
    let checks: Vec<(String, String, String, String)> = if maria {
        sqlx::query_as("SELECT TABLE_NAME,CONSTRAINT_NAME,CHECK_CLAUSE,'YES' FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=DATABASE()")
            .fetch_all(&mut *db).await?
    } else {
        sqlx::query_as("SELECT t.TABLE_NAME,c.CONSTRAINT_NAME,c.CHECK_CLAUSE,t.ENFORCED FROM information_schema.CHECK_CONSTRAINTS c JOIN information_schema.TABLE_CONSTRAINTS t ON t.CONSTRAINT_SCHEMA=c.CONSTRAINT_SCHEMA AND t.CONSTRAINT_NAME=c.CONSTRAINT_NAME WHERE c.CONSTRAINT_SCHEMA=DATABASE() AND t.TABLE_SCHEMA=DATABASE()")
            .fetch_all(&mut *db).await?
    };
    let mut table_checks = BTreeMap::<_, Vec<_>>::new();
    for (table, name, clause, enforced) in checks {
        table_checks
            .entry(table)
            .or_default()
            .push((name, clause, enforced));
    }
    for table in tables {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!("SHOW CREATE TABLE `{table}`")))
            .fetch_one(&mut *db)
            .await?;
        let sql: String = row.try_get(1)?;
        // Compare effective collations; the server may add redundant charset markers to ASCII literals.
        let sql = sql
            .replace(&format!("`{name}`."), "")
            .replace("CHARACTER SET utf8mb4 COLLATE", "COLLATE")
            .replace("CHARACTER SET ascii COLLATE", "COLLATE")
            .replace("_utf8mb4'", "'")
            .replace("_ascii'", "'");
        let checks = table_checks.remove(&table).unwrap_or_default();
        let inline_names: Vec<_> = sql
            .lines()
            .filter_map(|line| {
                let column = line.trim_start().strip_prefix('`')?.split('`').next()?;
                line.contains(" CHECK (").then_some(column.to_owned())
            })
            .collect();
        let mut sql_lines: Vec<String> = sql
            .lines()
            .filter(|line| {
                !(line.trim_start().starts_with("CONSTRAINT ") && line.contains(" CHECK ("))
            })
            .map(str::to_owned)
            .collect();
        let mut check_definitions = Vec::new();
        for (name, clause, enforced) in checks {
            let clause = clause.replace("_utf8mb4'", "'").replace("_ascii'", "'");
            for line in &mut sql_lines {
                *line = line.replace(&format!(" CHECK ({clause})"), "");
            }
            // Native exports can move anonymous column checks into table checks.
            let numbered = |prefix: &str| {
                name.strip_prefix(prefix).is_some_and(|suffix| {
                    !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit())
                })
            };
            let anonymous = inline_names.contains(&name)
                || numbered("CONSTRAINT_")
                || numbered(&format!("{table}_chk_"));
            let name = if anonymous { "" } else { &name };
            let clause = clause.replace("0 <> `sync_pending`", "`sync_pending` <> 0");
            check_definitions.push(format!("CHECK {name} {enforced} ({clause})"));
        }
        check_definitions.sort();
        let sql = sql_lines.join("\n");
        // Index and constraint declaration order does not change the effective structure.
        let unordered = |line: &str| {
            let line = line.trim_start();
            line.starts_with("CONSTRAINT ")
                || line.starts_with("KEY ")
                || line.starts_with("UNIQUE KEY ")
        };
        let mut lines: Vec<_> = sql
            .lines()
            .filter(|line| !unordered(line))
            .map(|line| {
                // The next allocated value is data, not a schema difference.
                if line.starts_with(") ") {
                    line.split_whitespace()
                        .filter(|part| !part.starts_with("AUTO_INCREMENT="))
                        .collect::<Vec<_>>()
                        .join(" ")
                } else {
                    line.to_owned()
                }
            })
            .collect();
        let mut constraints: Vec<_> = sql
            .lines()
            .filter(|line| unordered(line))
            .map(|line| constraint(line, &table, maria))
            .collect();
        constraints.sort();
        lines.extend(constraints);
        lines.extend(check_definitions);
        definitions.insert(table, lines.join("\n"));
    }
    let triggers: Vec<(String, String, String, String, String)> = sqlx::query_as("SELECT TRIGGER_NAME,ACTION_TIMING,EVENT_MANIPULATION,EVENT_OBJECT_TABLE,ACTION_STATEMENT FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=DATABASE() ORDER BY TRIGGER_NAME").fetch_all(&mut *db).await?;
    for (name, timing, event, table, action) in triggers {
        definitions.insert(
            format!("trigger:{name}"),
            format!("{timing} {event} {table}\n{action}"),
        );
    }
    Ok(definitions)
}

async fn same_structure(left: &mut MySqlConnection, right: &mut MySqlConnection) -> Result<()> {
    let left = structure(left).await?;
    let right = structure(right).await?;
    assert_eq!(left.len(), right.len());
    for (table, sql) in &left {
        let expected = &right[table];
        assert_eq!(sql.lines().count(), expected.lines().count(), "{table}");
        for (line, expected) in sql.lines().zip(expected.lines()) {
            assert_eq!(line, expected, "{table}");
        }
    }
    Ok(())
}

async fn contents(db: &mut MySqlConnection) -> Result<BTreeMap<String, Vec<String>>> {
    read_contents(db, true).await
}

async fn read_contents(
    db: &mut MySqlConnection,
    normalize: bool,
) -> Result<BTreeMap<String, Vec<String>>> {
    let mut data = BTreeMap::new();
    for table in queries::tables().fetch_all(&mut *db).await? {
        if normalize && table == "_sqlx_migrations" {
            continue;
        }
        let columns: Vec<(String,String)> = sqlx::query_as("SELECT COLUMN_NAME,DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? ORDER BY ORDINAL_POSITION").bind(&table).fetch_all(&mut *db).await?;
        let fields: Vec<_> = columns
            .into_iter()
            .map(|(name, column_type)| {
                let value = if normalize && table == "mokyu_meta" && name == "deployment_id" {
                    "'deployment'".to_owned()
                } else if normalize && column_type == "datetime" {
                    format!("IF(`{name}` IS NULL,NULL,'timestamp')")
                } else if matches!(column_type.as_str(), "binary" | "varbinary" | "blob") {
                    format!("HEX(`{name}`)")
                } else if matches!(
                    column_type.as_str(),
                    "char" | "varchar" | "text" | "longtext" | "json"
                ) {
                    format!("CONVERT(`{name}` USING utf8mb4) COLLATE utf8mb4_bin")
                } else {
                    format!("`{name}`")
                };
                format!("'{name}',{value}")
            })
            .collect();
        let values: Vec<Value> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT JSON_OBJECT({}) FROM `{table}`",
            fields.join(",")
        )))
        .fetch_all(&mut *db)
        .await?;
        let mut rows: Vec<_> = values.iter().map(Value::to_string).collect();
        rows.sort();
        data.insert(table, rows);
    }
    Ok(data)
}

async fn rejects_unchanged(db: &mut MySqlConnection, message: &str) -> Result<()> {
    let schema = structure(db).await?;
    let data = read_contents(db, false).await?;
    let error = migration::run(db).await.unwrap_err();
    assert!(format!("{error:#}").contains(message), "{error:#}");
    assert_eq!(structure(db).await?, schema);
    assert_eq!(read_contents(db, false).await?, data);
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated MySQL or MariaDB database and permission to create test databases"]
async fn mysql_schema_bootstrap_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, _) = Config::load(std::path::Path::new(&path))?;
    let options = MySqlConnectOptions::new()
        .host(&config.database.host)
        .port(config.database.effective_port().unwrap())
        .database(&config.database.name)
        .username(&config.database.user)
        .password(&secrets.database_password);
    let mut fresh = MySqlConnection::connect_with(&options).await?;
    assert!(queries::schema_empty().fetch_one(&mut fresh).await?);
    sqlx::raw_sql(queries::SESSION_SETUP)
        .execute(&mut fresh)
        .await?;
    let mut history = database(&mut fresh, &options).await?;
    let migrations = sqlx::migrate!("./migrations/mysql");
    migrations.run(&mut history).await?;
    migration::run(&mut fresh).await?;
    same_structure(&mut fresh, &mut history).await?;
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
    let before = contents(&mut fresh).await?;
    let id: uuid::Uuid = sqlx::query_scalar("SELECT deployment_id FROM mokyu_meta")
        .fetch_one(&mut fresh)
        .await?;
    migration::run(&mut fresh).await?;
    assert_eq!(contents(&mut fresh).await?, before);
    assert_eq!(
        sqlx::query_scalar::<_, uuid::Uuid>("SELECT deployment_id FROM mokyu_meta")
            .fetch_one(&mut fresh)
            .await?,
        id
    );

    // Validate history before upgrading an empty schema; preserve populated schemas unchanged.
    for version in [16, 21, 24, 41].into_iter().chain(
        migrations
            .iter()
            .map(|entry| entry.version)
            .filter(|version| *version >= 42),
    ) {
        let mut old = database(&mut fresh, &options).await?;
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
        sqlx::raw_sql("INSERT INTO storages(id,name,type,path,identity_marker) VALUES(UNHEX('00000000000000000000000000000001'),'preserved','local','/data/preserved','{}'); INSERT INTO buckets(id,name,storage_id,project_id) VALUES(UNHEX('00000000000000000000000000000001'),'preserved',UNHEX('00000000000000000000000000000001'),UNHEX('00000000000000000000000000000001'));")
            .execute(&mut old).await?;
        if version >= 42 {
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
        "CREATE TABLE unrelated(id INT)",
        "CREATE VIEW unrelated AS SELECT 1 AS id",
        "CREATE PROCEDURE unrelated() SELECT 1",
        "CREATE TABLE mokyu_meta(id INT,schema_version INT)",
        "CREATE TABLE mokyu_meta(id INT,schema_version INT); INSERT INTO mokyu_meta VALUES(1,0)",
        "CREATE TABLE mokyu_meta(id INT,schema_version INT); INSERT INTO mokyu_meta VALUES(1,999)",
    ] {
        let mut invalid = database(&mut fresh, &options).await?;
        sqlx::raw_sql(sql).execute(&mut invalid).await?;
        rejects_unchanged(&mut invalid, "").await?;
    }
    for sql in [
        "UPDATE mokyu_meta SET schema_version=16",
        "DELETE FROM _sqlx_migrations WHERE version=1",
        "UPDATE _sqlx_migrations SET checksum=REPEAT(CHAR(0),48) WHERE version=1",
        "UPDATE _sqlx_migrations SET success=FALSE WHERE version=40",
        "DELETE FROM mokyu_meta",
    ] {
        let mut tx = fresh.begin().await?;
        sqlx::raw_sql(sql).execute(&mut *tx).await?;
        rejects_unchanged(&mut tx, "").await?;
        tx.rollback().await?;
    }

    // Implicit DDL commits survive, but neither a valid version nor successful history may survive failure.
    for suffix in [
        "SELECT * FROM missing_bootstrap_table;",
        "CREATE TRIGGER interrupt_meta BEFORE INSERT ON mokyu_meta FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='interrupted';",
    ] {
        let mut interrupted = database(&mut fresh, &options).await?;
        let schema = format!("{}\n{suffix}", queries::SCHEMA);
        assert!(
            migration::initialize(&mut interrupted, &schema)
                .await
                .is_err()
        );
        assert!(!queries::schema_empty().fetch_one(&mut interrupted).await?);
        assert!(
            queries::schema_version()
                .fetch_optional(&mut interrupted)
                .await?
                .is_none()
        );
        let before = structure(&mut interrupted).await?;
        assert!(migration::run(&mut interrupted).await.is_err());
        assert_eq!(structure(&mut interrupted).await?, before);
        if queries::tables()
            .fetch_all(&mut interrupted)
            .await?
            .iter()
            .any(|t| t == "_sqlx_migrations")
        {
            assert!(
                queries::migrations()
                    .fetch_all(&mut interrupted)
                    .await?
                    .is_empty()
            );
        }
    }
    let mut interrupted = database(&mut fresh, &options).await?;
    let name: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(&mut interrupted)
        .await?;
    let id: u64 = sqlx::query_scalar("SELECT CONNECTION_ID()")
        .fetch_one(&mut interrupted)
        .await?;
    let schema = format!("{}\nSELECT SLEEP(30);", queries::SCHEMA);
    let disconnect = async {
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            loop {
                let sleeping: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM information_schema.PROCESSLIST WHERE ID=? AND STATE='User sleep')").bind(id).fetch_one(&mut fresh).await?;
                if sleeping { break; }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            sqlx::raw_sql(sqlx::AssertSqlSafe(format!("KILL CONNECTION {id}"))).execute(&mut fresh).await?;
            Ok::<_,anyhow::Error>(())
        }).await??;
        Ok::<_, anyhow::Error>(())
    };
    let (initializing, killed) =
        tokio::join!(migration::initialize(&mut interrupted, &schema), disconnect);
    killed?;
    assert!(initializing.is_err());
    let mut retry = MySqlConnection::connect_with(&options.database(&name)).await?;
    assert!(
        queries::schema_version()
            .fetch_optional(&mut retry)
            .await?
            .is_none()
    );
    let before = structure(&mut retry).await?;
    assert!(migration::run(&mut retry).await.is_err());
    assert_eq!(structure(&mut retry).await?, before);
    Ok(())
}
