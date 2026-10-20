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
    let tables: Vec<String> = sqlx::query_scalar("SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename<>'_sqlx_migrations' ORDER BY tablename").fetch_all(&mut *db).await?;
    let mut data = BTreeMap::new();
    for table in tables {
        let values: Vec<Value> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT to_jsonb(t) FROM \"{table}\" t"
        )))
        .fetch_all(&mut *db)
        .await?;
        let mut rows = Vec::new();
        for mut row in values {
            for (name, value) in row.as_object_mut().unwrap() {
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

    // Empty live tables must not reset nonce high-water marks consumed by old writers.
    let mut counters = database(&mut fresh, &options).await?;
    counters.ensure_migrations_table("_sqlx_migrations").await?;
    queries::migration_identity("storage-registry")
        .execute(&mut counters)
        .await?;
    for entry in migrations.iter().take(49) {
        counters.apply("_sqlx_migrations", entry).await?;
    }
    sqlx::raw_sql("INSERT INTO managed_keys(id,name,purpose,key_algorithm,fingerprint) VALUES('00000000-0000-0000-0000-000000000001','legacy-counter','block','aes-256-gcm',decode(repeat('01',32),'hex')); SELECT setval('chunk_locations_id_seq',1000000,true); SELECT setval('packs_id_seq',2000000,true);").execute(&mut counters).await?;
    migration::run(&mut counters).await?;
    let high: Vec<(i64, i64)> =
        sqlx::query_as("SELECT type_tag,reserved_id FROM nonce_counters ORDER BY type_tag")
            .fetch_all(&mut counters)
            .await?;
    assert_eq!(high, vec![(0, 1000000), (1, 2000000)]);
    migration::run(&mut counters).await?;
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT type_tag,reserved_id FROM nonce_counters ORDER BY type_tag"
        )
        .fetch_all(&mut counters)
        .await?,
        high
    );

    // Materialize legacy upload policy once, including disabled backends.
    let mut policy = database(&mut fresh, &options).await?;
    policy.ensure_migrations_table("_sqlx_migrations").await?;
    queries::migration_identity("storage-registry")
        .execute(&mut policy)
        .await?;
    for entry in migrations.iter().take(47) {
        policy.apply("_sqlx_migrations", entry).await?;
    }
    sqlx::raw_sql("INSERT INTO storages(id,name,type,path,identity_marker,enabled) VALUES('00000000-0000-0000-0000-000000000001','policy-upgrade','local','/data/policy-upgrade','{}',false); INSERT INTO storage_profiles(storage_id) VALUES('00000000-0000-0000-0000-000000000001'); INSERT INTO runtime_settings(section,value) VALUES('uploads','{\"idle_timeout\":\"7d\",\"client_idle_timeout\":\"120s\"}') ON CONFLICT(section) DO UPDATE SET value=excluded.value;").execute(&mut policy).await?;
    migration::run(&mut policy).await?;
    let upgraded: (String, Option<String>) = sqlx::query_as("SELECT multipart_idle_timeout,min_free_space FROM storage_profiles WHERE storage_id='00000000-0000-0000-0000-000000000001'").fetch_one(&mut policy).await?;
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
    queries::migration_identity("storage-registry")
        .execute(&mut old)
        .await?;
    for migration in migrations.iter().take(33) {
        old.apply("_sqlx_migrations", migration).await?;
    }
    // The legacy empty-only boundary still refuses real user data.
    sqlx::query("INSERT INTO projects(id,name) VALUES(gen_random_uuid(),'preserved')")
        .execute(&mut old)
        .await?;
    assert!(migration::run(&mut old).await.is_err());
    assert_eq!(queries::schema_version().fetch_one(&mut old).await?, 33);
    sqlx::query("DELETE FROM projects WHERE name='preserved'")
        .execute(&mut old)
        .await?;
    migration::run(&mut old).await?;
    same_structure(&mut old, &mut fresh).await?;
    assert_eq!(contents(&mut old).await?, contents(&mut fresh).await?);

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
        let before = structure(&mut invalid).await?;
        assert!(migration::run(&mut invalid).await.is_err());
        assert_eq!(structure(&mut invalid).await?, before);
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
        assert!(migration::run(&mut tx).await.is_err());
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
