use super::{Backend, cache_tests, connection, queries, transactions};
use crate::authorization::{Action, Permit, Principal};
use crate::config::Config;
use crate::model::bucket::CreateBucket;
use crate::model::metrics::{ScopeQuery, StorageUsage};
use crate::model::{AccountStore, BucketStore, TaskControlStore};
use anyhow::Result;
use chrono::{Duration, Utc};
use serde_json::{Value, json};
use std::collections::HashMap;
use uuid::Uuid;

async fn collect(backend: &Backend, config: &Config) -> Result<()> {
    crate::model::metrics::MetricsStore::inventory(backend, &config.metrics, &config.maintenance)
        .await?;
    Ok(())
}
fn number(value: &Value, key: &str) -> f64 {
    value[key].as_str().unwrap().parse().unwrap()
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_SQLITE_CONFIG pointing to a fresh isolated SQLite database"]
async fn sqlite_usage_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.write;
    backend.initialize_admin("admin", "password", false).await?;
    let global = ScopeQuery::default();
    let empty = transactions::read_usage(&backend, &Principal::Local, &global, 64).await?;
    assert!(empty.global && !empty.created && empty.usage.is_none() && empty.history.is_empty());
    collect(&backend, &config).await?;
    let empty = transactions::read_usage(&backend, &Principal::Local, &global, 64).await?;
    let usage = empty.usage.unwrap();
    assert_eq!(
        (
            usage.buckets,
            usage.objects,
            usage.logical_bytes,
            usage.unique_bytes
        ),
        ("0".into(), "0".into(), "0".into(), "0".into())
    );
    assert_eq!(usage.physical.unwrap().indexed_bytes, "0");
    assert_eq!(empty.history.len(), 1);
    assert!(
        transactions::read_usage(&backend, &Principal::Local, &global, 0)
            .await
            .is_err()
    );
    let storage = super::storage_tests::fixture(&backend).await?;
    let project_a = Uuid::new_v4();
    sqlx::query("INSERT INTO projects(id,name) VALUES(?,'project-a')")
        .bind(project_a)
        .execute(db)
        .await?;
    let mut buckets = HashMap::new();
    for name in ["alpha", "beta", "gamma", "delta", "epsilon"] {
        let project = if matches!(name, "alpha" | "gamma") {
            project_a
        } else {
            let id = Uuid::new_v4();
            sqlx::query("INSERT INTO projects(id,name) VALUES(?,?)")
                .bind(id)
                .bind(name)
                .execute(db)
                .await?;
            id
        };
        if name != "gamma" {
            queries::insert_quota_scope("project", project)
                .execute(db)
                .await?;
        }
        let bucket = BucketStore::create(
            &backend,
            &Principal::Local,
            CreateBucket {
                name: name.into(),
                project_id: Some(project),
                storage_id: Some(storage),
            },
        )
        .await?
        .id;
        buckets.insert(name, bucket);
        transactions::read_usage(
            &backend,
            &Principal::Local,
            &ScopeQuery {
                bucket: Some(bucket),
                project: None,
            },
            64,
        )
        .await?;
    }
    let project_scope = ScopeQuery {
        project: Some(project_a),
        bucket: None,
    };
    transactions::read_usage(&backend, &Principal::Local, &project_scope, 64).await?;
    let task = TaskControlStore::enqueue(&backend, "pack", None, json!({})).await?;
    let unit = 131072i64;
    let mut packs = Vec::new();
    for _ in 0..2 {
        let pack = cache_tests::pack(&backend, storage, task).await?;
        sqlx::query("UPDATE packs SET state='ready',raw_size=?,stored_size=? WHERE id=?")
            .bind(32 * unit)
            .bind(8 * unit)
            .bind(pack)
            .execute(db)
            .await?;
        packs.push(pack);
    }
    let mut chunks = Vec::new();
    for n in 0..6 {
        let pack = match n {
            0 | 1 => Some(packs[0]),
            2 | 5 => Some(packs[1]),
            _ => None,
        };
        let id=sqlx::query("INSERT INTO chunks(storage_id,hash,raw_size,encryption_algorithm,state,current_encoding_id,pack_id,unreferenced_at) VALUES(?,?,?,'none','ready',1,?,IIF(?,CAST(unixepoch('subsec')*1000000 AS INTEGER),NULL))").bind(storage).bind([n as u8;32].as_slice()).bind(16*unit).bind(pack).bind(n==5).execute(db).await?.last_insert_rowid();
        chunks.push(id);
    }
    for (chunk, state, size) in [
        (chunks[4], "ready", 2 * unit),
        (chunks[5], "retired", 5 * unit),
    ] {
        let encoding=sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,standalone_size,state,encryption_algorithm,encoding_fingerprint) VALUES(?,?,?,?,?,'none',?)").bind(chunk).bind(Uuid::new_v4()).bind(storage).bind(size).bind(state).bind("[]").execute(db).await?.last_insert_rowid();
        sqlx::query("UPDATE chunks SET current_encoding_id=? WHERE id=?")
            .bind(encoding)
            .bind(chunk)
            .execute(db)
            .await?;
    }
    let refs = [
        ("alpha", 0, 0, 16),
        ("alpha", 0, 0, 16),
        ("alpha", 4, 0, 4),
        ("beta", 0, 0, 16),
        ("beta", 1, 0, 16),
        ("gamma", 0, 0, 4),
        ("gamma", 0, 2, 4),
        ("delta", 2, 0, 2),
        ("epsilon", 3, 0, 4),
    ];
    for (n, (name, chunk, offset, length)) in refs.into_iter().enumerate() {
        let bucket = buckets[name];
        let stream = Uuid::new_v4();
        let key = n.to_string();
        sqlx::query("INSERT INTO streams(id,bucket_id,object_key,stream_type,state,size,write_authorization) VALUES(?,?,?,'object','ready',?,?)").bind(stream).bind(bucket).bind(key.as_bytes()).bind(length*unit).bind(sqlx::types::Json(Permit::for_action(Principal::Local,bucket,Action::Write))).execute(db).await?;
        sqlx::query(
            "INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES(?,?,?,?)",
        )
        .bind(bucket)
        .bind(key.as_bytes())
        .bind(stream)
        .bind(stream)
        .execute(db)
        .await?;
        sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) VALUES(?,0,?,?,?)").bind(stream).bind(length*unit).bind(chunks[chunk]).bind(offset*unit).execute(db).await?;
        sqlx::query("UPDATE quota_scopes SET used_bytes=used_bytes+?,object_count=object_count+1 WHERE scope='bucket' AND scope_id=?").bind(length*unit).bind(bucket).execute(db).await?;
    }
    collect(&backend, &config).await?;
    let mut values = HashMap::new();
    for (name, bucket) in &buckets {
        let row = transactions::read_usage(
            &backend,
            &Principal::Local,
            &ScopeQuery {
                bucket: Some(*bucket),
                project: None,
            },
            64,
        )
        .await?;
        assert!(!row.global && row.bucket_count == 1 && !row.created);
        let value = serde_json::to_value(row.usage.unwrap())?;
        assert!(value["physical"].is_null());
        values.insert(*name, value);
    }
    let project = transactions::read_usage(&backend, &Principal::Local, &project_scope, 64).await?;
    assert_eq!(project.bucket_count, 2);
    values.insert("project-a", serde_json::to_value(project.usage.unwrap())?);
    let global_usage = transactions::read_usage(&backend, &Principal::Local, &global, 64).await?;
    assert!(global_usage.global && global_usage.bucket_count == 5);
    values.insert("global", serde_json::to_value(global_usage.usage.unwrap())?);
    let scaled = |name, key| number(&values[name], key) / unit as f64;
    assert_eq!(scaled("alpha", "unique_bytes"), 20.);
    assert_eq!(scaled("alpha", "local_savings"), 16.);
    assert_eq!(scaled("alpha", "encoded_bytes"), 3.625);
    assert_eq!(scaled("beta", "encoded_bytes"), 6.);
    assert_eq!(scaled("gamma", "unique_bytes"), 6.);
    assert_eq!(scaled("gamma", "encoded_bytes"), 0.375);
    assert_eq!(scaled("delta", "encoding_savings"), -6.);
    assert!(
        values["epsilon"]["encoded_bytes"].is_null()
            && values["epsilon"]["encoding_savings"].is_null()
    );
    assert_eq!(scaled("epsilon", "pending_bytes"), 4.);
    assert_eq!(scaled("project-a", "unique_bytes"), 20.);
    assert_eq!(scaled("project-a", "encoded_bytes"), 4.);
    assert_eq!(scaled("global", "unique_bytes"), 42.);
    assert_eq!(scaled("global", "encoded_known_bytes"), 18.);
    let physical = &values["global"]["physical"];
    assert_eq!(number(physical, "indexed_bytes"), 23. * unit as f64);
    assert_eq!(number(physical, "selected_bytes"), 18. * unit as f64);
    assert_eq!(number(physical, "gc_bytes"), 5. * unit as f64);
    for value in values.values().filter(|v| !v["encoded_bytes"].is_null()) {
        let savings = ["local_savings", "shared_savings", "encoding_savings"]
            .iter()
            .map(|key| number(value, key))
            .sum::<f64>();
        assert!(
            (number(value, "logical_bytes") - number(value, "encoded_bytes") - savings).abs()
                < 0.00001
        );
    }
    // A repeat within the sampling interval changes the current snapshot without adding history.
    let previous: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM usage_history")
        .fetch_one(db)
        .await?;
    collect(&backend, &config).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM usage_history")
            .fetch_one(db)
            .await?,
        previous
    );
    sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,standalone_size,state,encryption_algorithm,encoding_fingerprint) VALUES(?,?,?,?,'retired','none',?)").bind(chunks[0]).bind(Uuid::new_v4()).bind(storage).bind(4*unit).bind("[]").execute(db).await?;
    collect(&backend, &config).await?;
    let physical = transactions::read_usage(&backend, &Principal::Local, &global, 64)
        .await?
        .usage
        .unwrap()
        .physical
        .unwrap();
    assert_eq!(physical.indexed_bytes.parse::<i64>()?, 27 * unit);
    assert_eq!(
        physical.selected_bytes.parse::<i64>()?
            + physical.retained_bytes.parse::<i64>()?
            + physical.gc_bytes.parse::<i64>()?,
        27 * unit
    );

    let user = Uuid::new_v4();
    let token = Uuid::new_v4();
    sqlx::query("INSERT INTO web_users(id,username,password_hash) VALUES(?,'inspector','fixture')")
        .bind(user)
        .execute(db)
        .await?;
    sqlx::query("INSERT INTO project_members(user_id,project_id,role,scope) VALUES(?,?,'reader','selected')").bind(user).bind(project_a).execute(db).await?;
    sqlx::query(
        "INSERT INTO member_bucket_grants(user_id,project_id,bucket_id,actions) VALUES(?,?,?,67)",
    )
    .bind(user)
    .bind(project_a)
    .bind(buckets["alpha"])
    .execute(db)
    .await?;
    sqlx::query("INSERT INTO api_tokens(id,user_id,label,token_hash,prefix,system_access,auth_revision) VALUES(?,?,'inspect',?,'inspect',FALSE,0)").bind(token).bind(user).bind([8u8;32].as_slice()).execute(db).await?;
    sqlx::query("INSERT INTO token_bucket_grants(token_id,bucket_id,actions) VALUES(?,?,64)")
        .bind(token)
        .bind(buckets["alpha"])
        .execute(db)
        .await?;
    let principal = Principal::Token {
        id: token,
        user,
        revision: 0,
        user_revision: 0,
    };
    let visible = transactions::read_usage(&backend, &principal, &global, 64).await?;
    assert!(!visible.global && visible.bucket_count == 1);
    assert_eq!(
        visible.usage.unwrap().unique_bytes,
        values["alpha"]["unique_bytes"].as_str().unwrap()
    );
    assert!(
        transactions::read_usage(
            &backend,
            &principal,
            &ScopeQuery {
                bucket: Some(buckets["beta"]),
                project: None
            },
            64
        )
        .await
        .is_err()
    );
    let lock = backend.write.begin_with(queries::BEGIN_WRITE).await?;
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            transactions::read_usage(&backend, &principal, &global, 64)
        )
        .await??
        .usage
        .is_some()
    );
    let blocked = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        collect(&backend, &config),
    );
    let cached = async {
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(200),
                transactions::read_usage(&backend, &principal, &global, 64)
            )
            .await??
            .usage
            .is_some()
        );
        Ok::<_, anyhow::Error>(())
    };
    let (blocked, cached) = tokio::join!(blocked, cached);
    assert!(blocked.is_err());
    cached?;
    lock.rollback().await?;
    sqlx::query("DELETE FROM token_bucket_grants WHERE token_id=?")
        .bind(token)
        .execute(db)
        .await?;
    let visible = transactions::read_usage(&backend, &principal, &global, 64).await?;
    assert!(visible.created && visible.usage.is_none() && visible.bucket_count == 0);
    collect(&backend, &config).await?;
    let empty = transactions::read_usage(&backend, &principal, &global, 64)
        .await?
        .usage
        .unwrap();
    assert_eq!(empty.logical_bytes, "0");
    assert!(empty.physical.is_none());
    sqlx::query("UPDATE api_tokens SET auth_revision=auth_revision+1 WHERE id=?")
        .bind(token)
        .execute(db)
        .await?;
    assert!(
        transactions::read_usage(&backend, &principal, &global, 64)
            .await
            .is_err()
    );
    let new_scope = ScopeQuery {
        project: Some(Uuid::new_v4()),
        bucket: None,
    };
    // Expire the empty scope before racing two requests for the same permissions.
    let key = blake3::hash(&serde_json::to_vec(&Vec::<Uuid>::new())?)
        .to_hex()
        .to_string();
    sqlx::query("DELETE FROM usage_summary WHERE id=?")
        .bind(&key)
        .execute(db)
        .await?;
    let (a, b) = tokio::join!(
        transactions::read_usage(&backend, &Principal::Local, &new_scope, 2),
        transactions::read_usage(&backend, &Principal::Local, &new_scope, 2)
    );
    let (a, b) = (a?, b?);
    assert_ne!(a.created, b.created);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM usage_summary WHERE id<>'global'")
            .fetch_one(db)
            .await?,
        2
    );
    assert!(
        transactions::read_usage(&backend, &Principal::Local, &global, 2)
            .await?
            .usage
            .is_some()
    );
    sqlx::query("UPDATE usage_summary SET requested_at=946684800000000 WHERE id<>'global'")
        .execute(db)
        .await?;
    sqlx::query("UPDATE usage_history SET snapshot_at=946684800000000")
        .execute(db)
        .await?;
    collect(&backend, &config).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM usage_summary")
            .fetch_one(db)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM usage_buckets")
            .fetch_one(db)
            .await?,
        0
    );
    let history = transactions::read_usage(&backend, &Principal::Local, &global, 2)
        .await?
        .history;
    assert_eq!(history.len(), 1);
    assert!(history[0].at > Utc::now() - Duration::minutes(1));
    let saved: sqlx::types::Json<StorageUsage> =
        sqlx::query_scalar("SELECT data FROM usage_summary WHERE id='global'")
            .fetch_one(db)
            .await?;
    let at = Utc::now();
    let mut insert =
        sqlx::QueryBuilder::new("INSERT INTO usage_history(scope_id,snapshot_at,data) ");
    insert.push_values(1..=2050, |mut row, n| {
        row.push_bind("global")
            .push_bind((at - Duration::seconds(n)).timestamp_micros())
            .push_bind(serde_json::to_value(&saved.0).unwrap());
    });
    insert.build().execute(db).await?;
    let history = transactions::read_usage(&backend, &Principal::Local, &global, 2)
        .await?
        .history;
    assert_eq!(history.len(), 2048);
    assert!(history.windows(2).all(|p| p[0].at < p[1].at));

    let held = backend.write.begin_with(queries::BEGIN_WRITE).await?;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        assert!(
            transactions::read_usage(&backend, &Principal::Local, &global, 2)
                .await?
                .usage
                .is_some()
        );
        transactions::collect_storage(&backend.read, storage).await?;
        anyhow::Ok(())
    })
    .await??;
    held.rollback().await?;
    let before: Option<i64> =
        sqlx::query_scalar("SELECT as_of FROM usage_summary WHERE id='global'")
            .fetch_one(db)
            .await?;
    let rows = queries::collect_usage().fetch_all(&backend.read).await?;
    let mut tx = backend.write.begin_with(queries::BEGIN_WRITE).await?;
    queries::save_usage(&mut tx, Utc::now() - Duration::days(1), rows).await?;
    tx.commit().await?;
    assert_eq!(
        sqlx::query_scalar::<_, Option<i64>>("SELECT as_of FROM usage_summary WHERE id='global'")
            .fetch_one(db)
            .await?,
        before
    );
    backend.check_owner()?;
    backend.read.close().await;
    backend.write.close().await;
    Ok(())
}
