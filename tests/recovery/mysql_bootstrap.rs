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

async fn structure(db: &mut MySqlConnection) -> Result<BTreeMap<String, String>> {
    let name: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(&mut *db)
        .await?;
    let tables: Vec<String> = sqlx::query_scalar("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() ORDER BY TABLE_NAME").fetch_all(&mut *db).await?;
    let mut definitions = BTreeMap::new();
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
        // Constraint declaration order does not affect MariaDB's structure.
        let mut lines: Vec<_> = sql
            .lines()
            .filter(|line| !line.trim_start().starts_with("CONSTRAINT "))
            .map(str::to_owned)
            .collect();
        let mut constraints: Vec<_> = sql
            .lines()
            .filter(|line| line.trim_start().starts_with("CONSTRAINT "))
            .map(|line| line.trim_end_matches(',').to_owned())
            .collect();
        constraints.sort();
        lines.extend(constraints);
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
    let mut data = BTreeMap::new();
    for table in queries::tables().fetch_all(&mut *db).await? {
        if table == "_sqlx_migrations" {
            continue;
        }
        let columns: Vec<(String,String)> = sqlx::query_as("SELECT COLUMN_NAME,DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? ORDER BY ORDINAL_POSITION").bind(&table).fetch_all(&mut *db).await?;
        let fields: Vec<_> = columns
            .into_iter()
            .map(|(name, column_type)| {
                let value = if table == "mokyu_meta" && name == "deployment_id" {
                    "'deployment'".to_owned()
                } else if column_type == "datetime" {
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

    // Materialize legacy upload policy once, including disabled backends.
    let mut policy = database(&mut fresh, &options).await?;
    policy.ensure_migrations_table("_sqlx_migrations").await?;
    for entry in migrations.iter().take(21) {
        policy.apply("_sqlx_migrations", entry).await?;
    }
    sqlx::raw_sql("INSERT INTO storages(id,name,type,path,identity_marker,enabled) VALUES(UNHEX('00000000000000000000000000000001'),'policy-upgrade','local','/data/policy-upgrade','{}',false); INSERT INTO storage_profiles(storage_id) VALUES(UNHEX('00000000000000000000000000000001')); INSERT INTO runtime_settings(section,value) VALUES('uploads','{\"idle_timeout\":\"7d\",\"client_idle_timeout\":\"120s\"}');").execute(&mut policy).await?;
    migration::run(&mut policy).await?;
    let upgraded: (String, Option<String>) = sqlx::query_as("SELECT multipart_idle_timeout,min_free_space FROM storage_profiles WHERE storage_id=UNHEX('00000000000000000000000000000001')").fetch_one(&mut policy).await?;
    assert_eq!(upgraded, ("7d".into(), None));
    let settings: serde_json::Value =
        sqlx::query_scalar("SELECT value FROM runtime_settings WHERE section='uploads'")
            .fetch_one(&mut policy)
            .await?;
    assert_eq!(settings, serde_json::json!({"client_idle_timeout":"120s"}));
    let migrated = contents(&mut policy).await?;
    migration::run(&mut policy).await?;
    assert_eq!(contents(&mut policy).await?, migrated);

    let mut old = database(&mut fresh, &options).await?;
    old.ensure_migrations_table("_sqlx_migrations").await?;
    for migration in migrations.iter().take(16) {
        old.apply("_sqlx_migrations", migration).await?;
    }
    sqlx::query("INSERT INTO runtime_settings(section,value) VALUES('preserved',JSON_OBJECT())")
        .execute(&mut old)
        .await?;
    let id: uuid::Uuid = sqlx::query_scalar("SELECT deployment_id FROM mokyu_meta")
        .fetch_one(&mut old)
        .await?;
    migration::run(&mut old).await?;
    same_structure(&mut old, &mut fresh).await?;
    assert_eq!(
        sqlx::query_scalar::<_, uuid::Uuid>("SELECT deployment_id FROM mokyu_meta")
            .fetch_one(&mut old)
            .await?,
        id
    );
    assert_eq!(
        sqlx::query_scalar::<_, Value>(
            "SELECT value FROM runtime_settings WHERE section='preserved'"
        )
        .fetch_one(&mut old)
        .await?,
        serde_json::json!({})
    );

    for version in [19, 20] {
        let recovery = migrations
            .iter()
            .find(|migration| migration.version == version)
            .unwrap();
        let markers = if version == 19 {
            [
                "DROP TRIGGER IF EXISTS encodings_owner_insert",
                "DROP TRIGGER IF EXISTS encodings_owner_update",
                "UPDATE mokyu_meta SET schema_version=19",
            ]
        } else {
            [
                "EXECUTE mokyu_audit",
                "DEALLOCATE PREPARE mokyu_audit",
                "UPDATE mokyu_meta SET schema_version=20",
            ]
        };
        for marker in markers {
            let mut interrupted = database(&mut fresh, &options).await?;
            interrupted
                .ensure_migrations_table("_sqlx_migrations")
                .await?;
            for migration in migrations
                .iter()
                .take_while(|migration| migration.version < version)
            {
                interrupted.apply("_sqlx_migrations", migration).await?;
            }
            let sql = recovery.sql.as_str();
            let prefix = &sql[..sql.find(marker).unwrap()];
            sqlx::raw_sql(sqlx::AssertSqlSafe(prefix))
                .execute(&mut interrupted)
                .await?;
            sqlx::query("INSERT INTO _sqlx_migrations(version,description,success,checksum,execution_time) VALUES(?,?,FALSE,?,0)")
            .bind(recovery.version).bind(recovery.description.as_ref()).bind(recovery.checksum.as_ref())
            .execute(&mut interrupted).await?;
            migration::run(&mut interrupted).await?;
            same_structure(&mut interrupted, &mut fresh).await?;
        }
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
        let before = structure(&mut invalid).await?;
        assert!(migration::run(&mut invalid).await.is_err());
        assert_eq!(structure(&mut invalid).await?, before);
    }
    for sql in [
        "UPDATE mokyu_meta SET schema_version=16",
        "DELETE FROM _sqlx_migrations WHERE version=1",
        "UPDATE _sqlx_migrations SET checksum=REPEAT(CHAR(0),48) WHERE version=1",
        "DELETE FROM mokyu_meta",
    ] {
        let mut tx = fresh.begin().await?;
        sqlx::raw_sql(sql).execute(&mut *tx).await?;
        let before = queries::migrations().fetch_all(&mut *tx).await?;
        assert!(migration::run(&mut tx).await.is_err());
        assert_eq!(queries::migrations().fetch_all(&mut *tx).await?, before);
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
