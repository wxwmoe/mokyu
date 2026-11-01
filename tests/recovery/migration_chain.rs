fn next_migration(current: &sqlx::migrate::Migrator, ddl: &str) -> sqlx::migrate::Migrator {
    use sqlx::{
        SqlSafeStr,
        migrate::{Migration, MigrationType, Migrator},
    };
    let previous = current.iter().last().unwrap().version;
    let next = (previous / 10_000_000_000 + 1) * 10_000_000_000 + 101_000_000;
    let mut entries: Vec<_> = current.iter().cloned().collect();
    entries.push(Migration::new(
        next,
        "test upgrade".into(),
        MigrationType::Simple,
        sqlx::AssertSqlSafe(format!(
            "{ddl}; UPDATE mokyu_meta SET schema_version={next}"
        ))
        .into_sql_str(),
        false,
    ));
    let mut successor = Migrator::with_migrations(entries);
    successor.set_locking(false);
    successor
}

async fn verify_upgrade(
    db: &mut TestConnection,
    current: &sqlx::migrate::Migrator,
    rollback_fault: Option<(&'static str, &'static str)>,
) -> Result<()> {
    migration::run(db).await?;
    sqlx::query("UPDATE projects SET description='upgrade fixture'")
        .execute(&mut *db)
        .await?;
    let identity = sqlx::query_scalar::<_, uuid::Uuid>("SELECT deployment_id FROM mokyu_meta")
        .fetch_one(&mut *db)
        .await?;
    let history = db.list_applied_migrations("_sqlx_migrations").await?;
    let successor = next_migration(
        current,
        "CREATE TABLE upgrade_probe(id INTEGER PRIMARY KEY)",
    );
    let next = successor.iter().last().unwrap().version;
    if let Some((install, remove)) = rollback_fault {
        sqlx::raw_sql(install).execute(&mut *db).await?;
        let structure_before = structure(db).await?;
        let rows_before = read_contents(db, false).await?;
        assert!(migration::upgrade(db, &successor).await.is_err());
        assert_eq!(structure(db).await?, structure_before);
        assert_eq!(read_contents(db, false).await?, rows_before);
        sqlx::raw_sql(remove).execute(&mut *db).await?;
    }
    migration::upgrade(db, &successor).await?;
    migration::upgrade(db, &successor).await?;
    assert_eq!(queries::schema_version().fetch_one(&mut *db).await?, next);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT description FROM projects")
            .fetch_one(&mut *db)
            .await?,
        "upgrade fixture"
    );
    assert_eq!(
        sqlx::query_scalar::<_, uuid::Uuid>("SELECT deployment_id FROM mokyu_meta")
            .fetch_one(&mut *db)
            .await?,
        identity
    );
    let applied = db.list_applied_migrations("_sqlx_migrations").await?;
    assert_eq!(applied.len(), history.len() + 1);
    for (stored, original) in applied.iter().zip(history) {
        assert_eq!(
            (stored.version, &stored.checksum),
            (original.version, &original.checksum)
        );
    }
    assert_eq!(applied.last().unwrap().version, next);
    assert_eq!(
        applied.last().unwrap().checksum,
        successor.iter().last().unwrap().checksum
    );
    Ok(())
}
