use super::{migration, queries};
use anyhow::Result;
use serde_json::Value;
use sqlx::{Connection, PgConnection, migrate::Migrate, postgres::PgConnectOptions};
use std::collections::BTreeMap;

async fn database(admin: &mut PgConnection, options: &PgConnectOptions) -> Result<PgConnection> {
    let name = format!("mokyu_bootstrap_{}", uuid::Uuid::new_v4().simple());
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}")))
        .execute(admin)
        .await?;
    Ok(PgConnection::connect_with(&options.clone().database(&name)).await?)
}

async fn structure(db: &mut PgConnection) -> Result<Vec<(String, String, Value)>> {
    Ok(sqlx::query_as(r#"
        SELECT 'column',c.relname||'.'||a.attname,jsonb_build_array(format_type(a.atttypid,a.atttypmod),a.attnotnull,pg_get_expr(d.adbin,d.adrelid),a.attidentity,a.attgenerated,coll.collname)
        FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum LEFT JOIN pg_collation coll ON coll.oid=a.attcollation
        WHERE n.nspname='public' AND c.relkind IN ('r','p','v') AND a.attnum>0 AND NOT a.attisdropped
        UNION ALL SELECT 'constraint',c.relname||'.'||k.conname,to_jsonb(pg_get_constraintdef(k.oid)) FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public'
        UNION ALL SELECT 'index',indexname,to_jsonb(indexdef) FROM pg_indexes WHERE schemaname='public'
        UNION ALL SELECT 'view',viewname,to_jsonb(definition) FROM pg_views WHERE schemaname='public'
        UNION ALL SELECT 'trigger',c.relname||'.'||t.tgname,to_jsonb(pg_get_triggerdef(t.oid)) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND NOT t.tgisinternal
        UNION ALL SELECT 'function',p.proname||'('||pg_get_function_identity_arguments(p.oid)||')',to_jsonb(pg_get_functiondef(p.oid)) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.deptype='e')
        UNION ALL SELECT 'sequence',sequencename,to_jsonb(s)-'schemaname'-'sequenceowner'-'last_value' FROM pg_sequences s WHERE schemaname='public'
        UNION ALL SELECT 'extension',extname,to_jsonb(extversion) FROM pg_extension
        ORDER BY 1,2
    "#).fetch_all(db).await?)
}

async fn same_structure(left: &mut PgConnection, right: &mut PgConnection) -> Result<()> {
    let left = structure(left).await?;
    let right = structure(right).await?;
    assert_eq!(left.len(), right.len());
    for (left, right) in left.iter().zip(&right) {
        assert_eq!(left, right);
    }
    Ok(())
}

async fn contents(db: &mut PgConnection) -> Result<BTreeMap<String, Vec<String>>> {
    read_contents(db, true).await
}

async fn read_contents(
    db: &mut PgConnection,
    normalize: bool,
) -> Result<BTreeMap<String, Vec<String>>> {
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename",
    )
    .fetch_all(&mut *db)
    .await?;
    let mut data = BTreeMap::new();
    for table in tables {
        if normalize && table == "_sqlx_migrations" {
            continue;
        }
        let values: Vec<Value> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT to_jsonb(t) FROM \"{table}\" t"
        )))
        .fetch_all(&mut *db)
        .await?;
        let mut rows = Vec::new();
        for mut row in values {
            for (name, value) in row.as_object_mut().unwrap() {
                if !normalize {
                    continue;
                }
                if table == "mokyu_meta" && name == "deployment_id" {
                    uuid::Uuid::parse_str(value.as_str().unwrap())?;
                    *value = Value::from("deployment");
                } else if matches!(
                    name.as_str(),
                    "created_at"
                        | "updated_at"
                        | "last_scheduled_at"
                        | "access_coverage_since"
                        | "access_flushed_at"
                        | "requested_at"
                ) && !value.is_null()
                {
                    chrono::DateTime::parse_from_rfc3339(value.as_str().unwrap())?;
                    *value = Value::from("timestamp");
                }
            }
            rows.push(row.to_string());
        }
        rows.sort();
        data.insert(table, rows);
    }
    Ok(data)
}

async fn rejects_unchanged(db: &mut PgConnection, message: &str) -> Result<()> {
    let schema = structure(db).await?;
    let data = read_contents(db, false).await?;
    let error = migration::run(db).await.unwrap_err();
    assert!(format!("{error:#}").contains(message), "{error:#}");
    assert_eq!(structure(db).await?, schema);
    assert_eq!(read_contents(db, false).await?, data);
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL database and permission to create test databases"]
async fn postgres_schema_bootstrap_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let options: PgConnectOptions = url.trim().parse()?;
    let mut fresh = PgConnection::connect_with(&options).await?;
    assert!(queries::schema_empty().fetch_one(&mut fresh).await?);
    let mut history = database(&mut fresh, &options).await?;
    queries::migration_identity("storage-registry")
        .execute(&mut history)
        .await?;
    let migrations = sqlx::migrate!("./migrations");
    migrations.run(&mut history).await?;

    // Both cluster starters must decide empty/current while owning the migration lock.
    let mut concurrent = PgConnection::connect_with(&options).await?;
    let (first, second) = tokio::join!(migration::run(&mut fresh), migration::run(&mut concurrent));
    first?;
    second?;
    same_structure(&mut fresh, &mut history).await?;
    assert_eq!(contents(&mut fresh).await?, contents(&mut history).await?);
    let applied = fresh.list_applied_migrations("_sqlx_migrations").await?;
    for (applied, expected) in applied.iter().zip(migrations.iter()) {
        assert_eq!(applied.version, expected.version);
        assert_eq!(applied.checksum, expected.checksum);
    }
    assert_eq!(applied.len(), migrations.iter().count());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM _sqlx_migrations WHERE execution_time<>-1"
        )
        .fetch_one(&mut fresh)
        .await?,
        0
    );
    let id: uuid::Uuid = sqlx::query_scalar("SELECT deployment_id FROM mokyu_meta")
        .fetch_one(&mut fresh)
        .await?;
    migration::run(&mut fresh).await?;
    assert_eq!(
        sqlx::query_scalar::<_, uuid::Uuid>("SELECT deployment_id FROM mokyu_meta")
            .fetch_one(&mut fresh)
            .await?,
        id
    );

    // Validate history before upgrading an empty schema; preserve populated schemas unchanged.
    for version in [33, 47, 50, 70].into_iter().chain(
        migrations
            .iter()
            .map(|entry| entry.version)
            .filter(|version| *version >= 71),
    ) {
        let mut old = database(&mut fresh, &options).await?;
        old.ensure_migrations_table("_sqlx_migrations").await?;
        queries::migration_identity("storage-registry")
            .execute(&mut old)
            .await?;
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
        sqlx::raw_sql("INSERT INTO storages(id,name,type,path,identity_marker) VALUES('00000000-0000-0000-0000-000000000001','preserved','local','/data/preserved','{}'); INSERT INTO buckets(id,name,storage_id,project_id) VALUES('00000000-0000-0000-0000-000000000001','preserved','00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000001');")
            .execute(&mut old).await?;
        if version >= 71 {
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
        "CREATE TYPE unrelated AS ENUM('value')",
        "CREATE TABLE mokyu_meta(singleton BOOLEAN,schema_version INT)",
        "CREATE TABLE mokyu_meta(singleton BOOLEAN,schema_version INT); INSERT INTO mokyu_meta VALUES(true,0)",
        "CREATE TABLE mokyu_meta(singleton BOOLEAN,schema_version INT); INSERT INTO mokyu_meta VALUES(true,999)",
    ] {
        let mut invalid = database(&mut fresh, &options).await?;
        sqlx::raw_sql(sql).execute(&mut invalid).await?;
        rejects_unchanged(&mut invalid, "").await?;
    }
    for sql in [
        "UPDATE mokyu_meta SET schema_version=39",
        "DELETE FROM _sqlx_migrations WHERE version=1",
        "UPDATE _sqlx_migrations SET checksum='\\x00' WHERE version=1",
        "UPDATE _sqlx_migrations SET success=false WHERE version=40",
        "DELETE FROM mokyu_meta",
    ] {
        let mut tx = fresh.begin().await?;
        sqlx::raw_sql(sql).execute(&mut *tx).await?;
        rejects_unchanged(&mut tx, "").await?;
        tx.rollback().await?;
    }

    for suffix in [
        "SELECT * FROM missing_bootstrap_table;",
        "ALTER TABLE mokyu_meta ADD CONSTRAINT interrupt_meta CHECK(schema_version<0);",
    ] {
        let mut interrupted = database(&mut fresh, &options).await?;
        let schema = format!("{}\n{suffix}", queries::SCHEMA);
        assert!(
            migration::initialize(&mut interrupted, &schema)
                .await
                .is_err()
        );
        assert!(queries::schema_empty().fetch_one(&mut interrupted).await?);
        migration::run(&mut interrupted).await?;
        assert_eq!(
            contents(&mut interrupted).await?,
            contents(&mut fresh).await?
        );
    }
    let mut interrupted = database(&mut fresh, &options).await?;
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut interrupted)
        .await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut interrupted)
        .await?;
    let schema = format!("{}\nSELECT pg_sleep(30);", queries::SCHEMA);
    let disconnect = async {
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            loop {
                let sleeping: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='PgSleep')").bind(pid).fetch_one(&mut fresh).await?;
                if sleeping { break; }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            sqlx::query("SELECT pg_terminate_backend($1)").bind(pid).execute(&mut fresh).await?;
            Ok::<_,anyhow::Error>(())
        }).await??;
        Ok::<_, anyhow::Error>(())
    };
    let (initializing, killed) =
        tokio::join!(migration::initialize(&mut interrupted, &schema), disconnect);
    killed?;
    assert!(initializing.is_err());
    let mut retry = PgConnection::connect_with(&options.clone().database(&name)).await?;
    assert!(queries::schema_empty().fetch_one(&mut retry).await?);
    migration::run(&mut retry).await?;
    assert_eq!(contents(&mut retry).await?, contents(&mut fresh).await?);
    Ok(())
}
