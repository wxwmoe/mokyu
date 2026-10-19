use super::connection;
use crate::authorization::DEFAULT_PROJECT;
use crate::config::Config;
use crate::model::{StorageProfileStore, StorageRecordStore};
use crate::storage::model::Record;
use anyhow::Result;
use serde_json::json;
use uuid::Uuid;

fn record(name: &str, deployment: Uuid) -> Record {
    let id = Uuid::new_v4();
    Record {
        id,
        name: name.into(),
        r#type: "local".into(),
        path: Some(format!("/data/{id}")),
        endpoint_url: None,
        remote_bucket_name: None,
        region: None,
        prefix: "".into(),
        access_key_encrypted: None,
        secret_key_encrypted: None,
        identity_marker: crate::storage::model::identity_marker(deployment, id).unwrap(),
        enabled: true,
    }
}

pub(super) async fn fixture(backend: &super::Backend) -> Result<Uuid> {
    fixture_named(backend, "fixture").await
}

pub(super) async fn fixture_named(backend: &super::Backend, name: &str) -> Result<Uuid> {
    let mut creation = backend.creation().await?;
    let record = record(name, creation.deployment);
    creation
        .writer
        .prepare(&record, None, None, None, None, None)
        .await?;
    creation.writer.finish().await?;
    Ok(record.id)
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_SQLITE_CONFIG pointing to a fresh isolated SQLite database"]
async fn sqlite_storage_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.write;
    assert!(backend.list().await?.is_empty());
    assert!(backend.creation().await?.writer.finish().await.is_err());
    let mut creation = backend.creation().await?;
    assert_eq!(creation.mode, "single");
    let deployment = creation.deployment;
    let local = record("local", deployment);
    creation
        .writer
        .prepare(&local, None, Some(2), Some(3), Some(1), None)
        .await?;
    assert!(backend.named("local").await?.is_none());
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sqlx::query("INSERT INTO runtime_settings(section,value) VALUES('prepare','{}')")
            .execute(db),
    )
    .await??;
    assert!(
        creation
            .writer
            .prepare(&local, None, None, None, None, None)
            .await
            .is_err()
    );
    creation.writer.finish().await?;
    assert_eq!(backend.list().await?.len(), 1);
    let view = backend.view(local.id).await?;
    assert_eq!(
        (
            view.read_concurrency,
            view.upload_concurrency,
            view.control_concurrency
        ),
        (Some(2), Some(3), Some(1))
    );
    assert_eq!(view.profile_revision, 1);
    assert_eq!(backend.id("local", None).await?, Some(local.id));
    assert_eq!(backend.id("missing", Some(local.id)).await?, Some(local.id));
    assert!(backend.named("LOCAL").await?.is_none());
    assert!(backend.named("local ").await?.is_none());
    let row = backend.record("local").await?.unwrap();
    assert_eq!(row.identity_marker, local.identity_marker);
    assert_eq!(backend.enabled().await?.len(), 1);
    assert!(backend.load(local.id).await.is_ok());
    assert!(backend.load(Uuid::new_v4()).await.is_err());
    let profile = backend.load(local.id).await?;
    assert_eq!(profile.multipart_idle_timeout, "24h");
    assert_eq!(profile.min_free_space.as_deref(), Some("1GiB"));
    sqlx::query("UPDATE storage_profiles SET min_free_space=NULL WHERE storage_id=?")
        .bind(local.id)
        .execute(db)
        .await?;
    StorageProfileStore::initialize(&backend, "64MiB").await?;
    let migrated = backend.load(local.id).await?;
    assert_eq!(migrated.min_free_space.as_deref(), Some("64MiB"));
    StorageProfileStore::initialize(&backend, "2GiB").await?;
    assert_eq!(backend.load(local.id).await?.revision, migrated.revision);
    let mut invalid = migrated.clone();
    invalid.multipart_idle_timeout = "18446744073709551615d".into();
    assert!(crate::storage::profile::validate(&invalid, "local").is_err());
    invalid = migrated.clone();
    invalid.min_free_space = None;
    assert!(crate::storage::profile::validate(&invalid, "local").is_err());
    let mut restore = migrated;
    restore.min_free_space = profile.min_free_space;
    backend.update("local".into(), restore).await?;
    let profile = backend.load(local.id).await?;
    assert_eq!(profile.min_storage_seconds()?, 0);
    let mut changed = profile.clone();
    changed.min_storage_duration = "30d".into();
    assert_eq!(changed.min_storage_seconds()?, 30 * 86400);
    for invalid in ["", "-1d", "30", "30days", "18446744073709551615d"] {
        changed.min_storage_duration = invalid.into();
        assert!(changed.min_storage_seconds().is_err());
    }
    assert!(
        !profile.cache_enabled
            && !profile.upload_cache
            && !profile.pack_enabled
            && !profile.pack_range_enabled
    );
    assert!(!profile.compression_enabled);
    assert_eq!(profile.request_timeout, "30s");

    let mut creation = backend.creation().await?;
    let aborted = record("aborted", deployment);
    creation
        .writer
        .prepare(&aborted, None, None, None, None, None)
        .await?;
    drop(creation);
    assert!(backend.named("aborted").await?.is_none());
    let mut creation = backend.creation().await?;
    let invalid = record("invalid", deployment);
    assert!(
        creation
            .writer
            .prepare(&invalid, None, Some(0), None, None, None)
            .await
            .is_err()
    );
    assert!(
        creation
            .writer
            .prepare(&invalid, None, None, None, None, None)
            .await
            .is_err()
    );
    assert!(creation.writer.finish().await.is_err());
    assert!(backend.named("invalid").await?.is_none());
    assert!(backend.load(invalid.id).await.is_err());
    let mut creation = backend.creation().await?;
    assert!(
        creation
            .writer
            .prepare(&record("local", deployment), None, None, None, None, None)
            .await
            .is_err()
    );
    assert!(creation.writer.finish().await.is_err());
    assert_eq!(backend.list().await?.len(), 1);
    let mut invalid_profile = profile.clone();
    invalid_profile.pack_enabled = true;
    let error = backend
        .update("local".into(), invalid_profile.clone())
        .await
        .err()
        .unwrap();
    assert_eq!(
        crate::model::error(&error),
        Some(crate::model::Error::LocalStorageFeatures)
    );
    let mut creation = backend.creation().await?;
    assert!(
        creation
            .writer
            .prepare(
                &record("bad-profile", deployment),
                None,
                None,
                None,
                None,
                Some(invalid_profile)
            )
            .await
            .is_err()
    );
    assert!(creation.writer.finish().await.is_err());
    assert!(backend.named("bad-profile").await?.is_none());

    let mut creation = backend.creation().await?;
    let mut remote = record("remote", deployment);
    remote.r#type = "s3".into();
    remote.path = None;
    remote.endpoint_url = Some("http://test:8333".into());
    remote.remote_bucket_name = Some("test".into());
    remote.region = Some("test".into());
    remote.access_key_encrypted = Some(vec![1, 2]);
    remote.secret_key_encrypted = Some(vec![3, 4]);
    creation
        .writer
        .prepare(&remote, None, None, None, None, None)
        .await?;
    creation.writer.finish().await?;
    let profile = backend.load(remote.id).await?;
    assert!(
        profile.cache_enabled
            && profile.upload_cache
            && profile.compression_enabled
            && profile.pack_enabled
            && profile.pack_range_enabled
    );
    let mut one = profile.clone();
    one.request_timeout = "45s".into();
    let mut two = profile.clone();
    two.request_timeout = "60s".into();
    let (one, two) = tokio::join!(
        backend.update("remote".into(), one),
        backend.update("remote".into(), two)
    );
    assert_ne!(one.is_ok(), two.is_ok());
    let failure = if let Err(error) = one {
        error
    } else {
        two.unwrap_err()
    };
    assert_eq!(
        crate::model::error(&failure),
        Some(crate::model::Error::StorageProfileConflict)
    );
    let mut changed = backend.load(remote.id).await?;
    changed.connect_timeout = "6s".into();
    changed.cache_entries = Some(31);
    changed.cache_soft_limit = Some("0B".into());
    changed.upload_cache_size = Some("12MiB".into());
    changed.compression_strategy = "sample".into();
    changed.compression_level = 7;
    changed.compression_min_savings_percent = 11;
    changed.compression_min_savings_bytes = 512;
    changed.integrity_rps = Some(5);
    changed.integrity_bandwidth = Some("2MiB".into());
    changed.read_concurrency = Some(4);
    changed.upload_concurrency = Some(5);
    changed.control_concurrency = Some(6);
    changed.pack_byte_budget = Some("64MiB".into());
    changed.integrity_byte_budget = Some("32MiB".into());
    changed.maintenance_concurrency = Some(2);
    changed.maintenance_byte_budget = Some("16MiB".into());
    changed.migration_concurrency = Some(3);
    changed.migration_byte_budget = Some("8MiB".into());
    backend.update("remote".into(), changed.clone()).await?;
    changed.revision += 1;
    assert_eq!(
        serde_json::to_value(backend.load(remote.id).await?)?,
        serde_json::to_value(&changed)?
    );
    assert_eq!(
        backend.enabled_by_id(remote.id).await?.unwrap().revision,
        changed.revision
    );
    let mut wrong = changed.clone();
    wrong.storage_id = local.id;
    assert!(backend.update("remote".into(), wrong).await.is_err());
    assert!(
        crate::model::audit::scope(-1, backend.update("remote".into(), changed.clone()))
            .await
            .is_err()
    );
    assert_eq!(backend.load(remote.id).await?.revision, changed.revision);
    let intent = sqlx::query("INSERT INTO audit_events(actor_label,source,action,target) VALUES('test','cli','storage.profile.update','')").execute(db).await?.last_insert_rowid();
    crate::model::audit::scope(intent, backend.update("remote".into(), changed.clone())).await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT outcome FROM audit_events WHERE id=?")
            .bind(intent)
            .fetch_one(db)
            .await?,
        "succeeded"
    );

    let stale = backend.edit("remote").await?;
    backend
        .update("remote".into(), stale.profile.clone())
        .await?;
    assert!(
        stale
            .update
            .finish(&remote, None, &stale.profile)
            .await
            .is_err()
    );
    let edit = backend.edit("remote").await?;
    assert_eq!(edit.mode, "single");
    let mut disabled = remote.clone();
    disabled.enabled = false;
    let marker = edit.row.identity_marker;
    disabled.identity_marker = json!({"cannot":"replace"});
    edit.update.finish(&disabled, None, &edit.profile).await?;
    assert!(backend.enabled_by_id(remote.id).await?.is_none());
    assert_eq!(
        backend.record("remote").await?.unwrap().identity_marker,
        marker
    );
    assert_eq!(backend.enabled().await?.len(), 1);
    let edit = backend.edit("remote").await?;
    assert!(
        edit.update
            .finish(&local, None, &edit.profile)
            .await
            .is_err()
    );
    assert!(!backend.view(remote.id).await?.enabled);

    let key = Uuid::new_v4();
    sqlx::query("INSERT INTO managed_keys(id,name,purpose,key_algorithm,secret_encrypted,fingerprint) VALUES(?,'key','block','aes-256-gcm',?,?)").bind(key).bind(vec![1u8;32]).bind(vec![2u8;32]).execute(db).await?;
    assert_eq!(backend.current_key("local", Some("key")).await?, local.id);
    assert_eq!(backend.view(local.id).await?.block_key_id, Some(key));
    assert!(
        crate::model::audit::scope(-1, backend.current_key("local", None))
            .await
            .is_err()
    );
    assert_eq!(backend.view(local.id).await?.block_key_id, Some(key));
    backend.current_key("local", Some(&key.to_string())).await?;
    sqlx::query("UPDATE managed_keys SET revoked_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?")
        .bind(key)
        .execute(db)
        .await?;
    assert!(backend.current_key("local", Some("key")).await.is_err());
    let mut creation = backend.creation().await?;
    assert!(
        creation
            .writer
            .prepare(
                &record("revoked", deployment),
                Some(key),
                None,
                None,
                None,
                None
            )
            .await
            .is_err()
    );
    assert!(creation.writer.finish().await.is_err());
    assert!(backend.named("revoked").await?.is_none());
    backend.current_key("local", None).await?;
    assert!(backend.view(local.id).await?.block_key_id.is_none());
    assert!(
        sqlx::query("INSERT INTO buckets(id,name,project_id,storage_id) VALUES(?,'invalid',?,?)")
            .bind(Uuid::new_v4())
            .bind(DEFAULT_PROJECT)
            .bind(Uuid::new_v4())
            .execute(db)
            .await
            .is_err()
    );
    let bucket = Uuid::new_v4();
    sqlx::query("INSERT INTO buckets(id,name,project_id,storage_id) VALUES(?,'valid',?,?)")
        .bind(bucket)
        .bind(DEFAULT_PROJECT)
        .bind(local.id)
        .execute(db)
        .await?;
    assert!(
        sqlx::query("DELETE FROM storages WHERE id=?")
            .bind(local.id)
            .execute(db)
            .await
            .is_err()
    );
    backend.read.close().await;
    backend.write.close().await;
    drop(backend);
    let mut one = budget.clone();
    one.database_connections = 1;
    let backend = connection::open(&config, &secrets, &one, "single").await?;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut creation = backend.creation().await?;
        creation
            .writer
            .prepare(&record("one", deployment), None, None, None, None, None)
            .await?;
        assert!(backend.named("one").await?.is_none());
        sqlx::query("INSERT INTO runtime_settings(section,value) VALUES('one','{}')")
            .execute(&backend.write)
            .await?;
        creation.writer.finish().await?;
        assert!(backend.named("one").await?.is_some());
        Result::<()>::Ok(())
    })
    .await??;
    backend.read.close().await;
    backend.write.close().await;
    Ok(())
}
