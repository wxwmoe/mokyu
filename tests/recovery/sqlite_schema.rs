use super::{connection, queries};
use crate::authorization::DEFAULT_PROJECT;
use crate::config::Config;
use crate::model::Id;
use crate::model::{AccountStore, StorageProfileStore};
use anyhow::Result;
use uuid::Uuid;

async fn chunk(db: &sqlx::SqlitePool, storage: Uuid) -> Result<i64> {
    Ok(sqlx::query("INSERT INTO chunks(uuid,storage_id,hash,raw_size,encryption_algorithm,state) VALUES(?,?,?,32,'none','preparing')").bind(Uuid::now_v7())
        .bind(storage).bind([1u8;32].as_slice()).execute(db).await?.last_insert_rowid())
}

async fn encoding(db: &sqlx::SqlitePool, storage: Uuid, chunk: Id) -> Result<i64> {
    Ok(sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,standalone_size,state,encryption_algorithm,encoding_fingerprint) VALUES(?,?,?,32,'ready','none','[]')")
        .bind(chunk).bind(Uuid::new_v4()).bind(storage).execute(db).await?.last_insert_rowid())
}

async fn pack(db: &sqlx::SqlitePool, storage: Uuid) -> Result<i64> {
    Ok(sqlx::query("INSERT INTO packs(location_id,storage_id,encryption_algorithm,raw_size,stored_size,member_count,state,pack_fingerprint) VALUES(?,?,'none',64,64,2,'ready','[]')")
        .bind(Uuid::new_v4()).bind(storage).execute(db).await?.last_insert_rowid())
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_SQLITE_CONFIG pointing to a fresh isolated SQLite database"]
async fn sqlite_data_schema_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.write;
    let storage = super::storage_tests::fixture(&backend).await?;
    let other = super::storage_tests::fixture_named(&backend, "other").await?;
    assert_eq!(
        queries::schema_version().fetch_one(&backend.read).await?,
        sqlx::migrate!("./migrations/sqlite")
            .iter()
            .last()
            .unwrap()
            .version
    );
    let profile = backend.load(storage).await?;
    assert!(profile.cdc_enabled);
    assert!(!profile.deduplication_enabled);
    let mut legacy = serde_json::to_value(profile)?;
    legacy.as_object_mut().unwrap().remove("cdc_enabled");
    legacy
        .as_object_mut()
        .unwrap()
        .remove("deduplication_enabled");
    assert!(serde_json::from_value::<crate::storage::profile::StorageProfile>(legacy)?.cdc_enabled);
    let raw = sqlx::query(
        "INSERT INTO raw_sources(location_id,storage_id,profile_revision) VALUES(?,?,1)",
    )
    .bind(Uuid::now_v7())
    .bind(storage)
    .execute(db)
    .await?
    .last_insert_rowid();
    for invalid in [
        "UPDATE raw_sources SET state='ready' WHERE id=?",
        "UPDATE raw_sources SET size=-1 WHERE id=?",
        "UPDATE raw_sources SET revision=0 WHERE id=?",
        "UPDATE raw_sources SET medium='memory' WHERE id=?",
        "UPDATE raw_sources SET boot_id=randomblob(16) WHERE id=?",
        "UPDATE raw_sources SET digest=x'00' WHERE id=?",
    ] {
        assert!(
            sqlx::query(invalid).bind(raw).execute(db).await.is_err(),
            "{invalid}"
        );
    }
    sqlx::query("UPDATE raw_sources SET state='ready',digest=? WHERE id=?")
        .bind([0u8; 16].as_slice())
        .bind(raw)
        .execute(db)
        .await?;
    assert!(
        sqlx::query("DELETE FROM storages WHERE id=?")
            .bind(storage)
            .execute(db)
            .await
            .is_err()
    );
    sqlx::query("DELETE FROM raw_sources WHERE id=?")
        .bind(raw)
        .execute(db)
        .await?;
    let first = chunk(db, storage).await?;
    assert!(chunk(db, storage).await.is_err());
    let encoding_one = encoding(db, storage, first.into()).await?;
    assert!(encoding(db, storage, first.into()).await.is_err());
    sqlx::query("UPDATE chunk_encodings SET state='retired' WHERE encoding_id=?")
        .bind(encoding_one)
        .execute(db)
        .await?;
    let encoding_two = encoding(db, storage, first.into()).await?;
    assert!(encoding_two > encoding_one);
    sqlx::query("UPDATE chunks SET state='failed',current_encoding_id=? WHERE id=?")
        .bind(encoding_two)
        .bind(first)
        .execute(db)
        .await?;
    let second = chunk(db, storage).await?;
    assert!(second > first);
    sqlx::query("DELETE FROM chunks WHERE storage_id=?")
        .bind(storage)
        .execute(db)
        .await?;
    let third = chunk(db, storage).await?;
    let encoding_three = encoding(db, storage, third.into()).await?;
    assert!(third > second && encoding_three > encoding_two);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chunk_encodings")
            .fetch_one(db)
            .await?,
        1
    );
    let created: i64 = sqlx::query_scalar("SELECT created_at FROM chunks WHERE id=?")
        .bind(third)
        .fetch_one(db)
        .await?;
    assert!(created > 1_500_000_000_000_000);

    let foreign = pack(db, other).await?;
    assert!(
        sqlx::query("UPDATE chunks SET pack_id=? WHERE id=?")
            .bind(foreign)
            .bind(third)
            .execute(db)
            .await
            .is_err()
    );
    let own = pack(db, storage).await?;
    sqlx::query("UPDATE chunks SET pack_id=? WHERE id=?")
        .bind(own)
        .bind(third)
        .execute(db)
        .await?;
    assert!(
        sqlx::query("DELETE FROM packs WHERE id=?")
            .bind(own)
            .execute(db)
            .await
            .is_err()
    );
    sqlx::query("DELETE FROM chunks WHERE id=?")
        .bind(third)
        .execute(db)
        .await?;
    sqlx::query("DELETE FROM packs").execute(db).await?;
    assert!(pack(db, storage).await? > own);

    let admin = backend.initialize_admin("admin", "test", false).await?;
    let client = Uuid::new_v4();
    for token in [None, Some(Uuid::nil()), Some(Uuid::new_v4())] {
        sqlx::query("INSERT INTO media_operations(id,user_id,token_id,client_id,request_hash) VALUES(?,?,?,?,?)")
            .bind(Uuid::new_v4()).bind(admin).bind(token).bind(client).bind([0u8;32].as_slice()).execute(db).await?;
    }
    for token in [None, Some(Uuid::nil())] {
        assert!(sqlx::query("INSERT INTO media_operations(id,user_id,token_id,client_id,request_hash) VALUES(?,?,?,?,?)")
            .bind(Uuid::new_v4()).bind(admin).bind(token).bind(client).bind([0u8;32].as_slice()).execute(db).await.is_err());
    }
    let task = Uuid::new_v4();
    sqlx::query("INSERT INTO tasks(id,task_type,state,detail) VALUES(?,'pack','queued','{\"policy\":\"reuse\"}')").bind(task).execute(db).await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT policy FROM tasks WHERE id=?")
            .bind(task)
            .fetch_one(db)
            .await?,
        "reuse"
    );
    sqlx::query("UPDATE tasks SET detail='{\"policy\":null}' WHERE id=?")
        .bind(task)
        .execute(db)
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT policy FROM tasks WHERE id=?")
            .bind(task)
            .fetch_one(db)
            .await?,
        "pack"
    );
    assert!(
        sqlx::query("UPDATE tasks SET detail='invalid' WHERE id=?")
            .bind(task)
            .execute(db)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM quota_scopes WHERE scope='project' AND scope_id=?"
        )
        .bind(DEFAULT_PROJECT)
        .fetch_one(db)
        .await?,
        1
    );
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(db)
            .await?
            .is_empty()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("PRAGMA integrity_check")
            .fetch_one(db)
            .await?,
        "ok"
    );
    backend.read.close().await;
    backend.write.close().await;
    Ok(())
}
