use super::{Backend, cache_tests, connection, transactions};
use crate::authorization::Principal;
use crate::config::Config;
use crate::model::account::SessionLogin;
use crate::model::bucket::CreateBucket;
use crate::model::metrics::{AccessBatch, ChunkCounts, PackCounts, RuntimePoint};
use crate::model::{AccountStore, BucketStore, TaskControlStore};
use anyhow::Result;
use chrono::{DateTime, Duration, Timelike, Utc};
use serde_json::json;

fn point(at: DateTime<Utc>) -> RuntimePoint {
    RuntimePoint {
        at,
        epoch: at - Duration::days(1),
        requests: u64::MAX.to_string(),
        failed: "2".into(),
        backend_read_bytes: "3".into(),
        backend_write_bytes: "4".into(),
        cache_hits: "5".into(),
        cache_lookups: "6".into(),
        cache_bytes: "7".into(),
        cache_limit: "8".into(),
        upload_bytes: "9".into(),
        upload_limit: "10".into(),
        multipart_bytes: "11".into(),
        thumbnail_bytes: "12".into(),
        rss_bytes: None,
    }
}
fn batch(chunk: i64, pack: i64, at: DateTime<Utc>) -> AccessBatch {
    let mut batch = AccessBatch::default();
    batch.chunks.insert(
        (chunk, at),
        ChunkCounts {
            reads: 2,
            ranges: 1,
            bytes: 50,
            origins: 1,
        },
    );
    batch.chunks.insert(
        (chunk, at - Duration::hours(1)),
        ChunkCounts {
            reads: 3,
            ranges: 2,
            bytes: 70,
            origins: 2,
        },
    );
    batch.chunks.insert(
        (i64::MAX, at),
        ChunkCounts {
            reads: 100,
            ..Default::default()
        },
    );
    batch.packs.insert(
        (pack, at),
        PackCounts {
            downloads: 4,
            bytes: 200,
            partial: 2,
            partial_bytes: 40,
            useful: 100,
        },
    );
    batch.packs.insert(
        (i64::MAX, at),
        PackCounts {
            downloads: 100,
            ..Default::default()
        },
    );
    batch.members.insert((pack, chunk, at), 2);
    batch.members.insert((pack, i64::MAX, at), 100);
    batch.members.insert((i64::MAX, chunk, at), 100);
    batch
}
async fn stats(backend: &Backend, chunk: i64) -> Result<(i64, i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT read_count,range_read_count,read_bytes FROM chunk_access_stats WHERE chunk_id=?",
    )
    .bind(chunk)
    .fetch_one(&backend.pool)
    .await?)
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_access_metrics_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    let admin = backend.initialize_admin("admin", "password", false).await?;
    let verified = backend.login_user("admin").await?.unwrap();
    backend
        .login(SessionLogin {
            verified: &verified,
            access_hash: &[1; 32],
            csrf_hash: &[2; 32],
            refresh_hash: &[3; 32],
            seconds: 3600,
            user_agent: "fixture",
            request_id: "fixture",
        })
        .await?;
    let session = backend.authenticate_session(&[1; 32]).await?.unwrap();
    let principal = Principal::User {
        id: admin,
        session: session.session_id,
        revision: session.authorization_revision,
    };
    let empty = transactions::read_runtime(db, &principal).await?;
    assert!(empty.history.is_empty() && empty.work.is_empty());
    assert_eq!(
        (
            empty.pending_entries,
            empty.pending_bytes,
            empty.failed_uploads
        ),
        ("0".into(), "0".into(), "0".into())
    );
    assert!(empty.oldest_pending.is_none());
    let storage = super::storage_tests::fixture(&backend).await?;
    let bucket = BucketStore::create(
        &backend,
        &Principal::Local,
        CreateBucket {
            name: "metrics".into(),
            project_id: None,
            storage_id: Some(storage),
        },
    )
    .await?
    .id;
    let stream = cache_tests::stream(&backend, bucket, "pending").await?;
    let chunk = cache_tests::chunk(&backend, storage, stream, 1).await?;
    cache_tests::stage(&backend, &chunk, stream, 0, true).await?;
    sqlx::query("UPDATE pending_uploads SET last_error='fixture'")
        .execute(db)
        .await?;
    let task = TaskControlStore::enqueue(&backend, "pack", None, json!({"policy":"range"})).await?;
    let pack = cache_tests::pack(&backend, storage, task).await?;
    let now = Utc::now().with_nanosecond(123456000).unwrap();
    transactions::sample_runtime(db, point(now - Duration::days(3)), 10. * 86400.).await?;
    transactions::sample_runtime(db, point(now), 86400.).await?;
    assert!(
        transactions::sample_runtime(db, point(now), 86400.)
            .await
            .is_err()
    );
    let runtime = transactions::read_runtime(db, &principal).await?;
    assert_eq!(runtime.history.len(), 1);
    assert_eq!(runtime.history[0].at, now);
    assert_eq!(runtime.history[0].requests, u64::MAX.to_string());
    assert!(runtime.history[0].rss_bytes.is_none());
    assert_eq!(
        (
            runtime.pending_entries,
            runtime.pending_bytes,
            runtime.failed_uploads
        ),
        ("1".into(), "32".into(), "1".into())
    );
    assert!(runtime.oldest_pending.is_some());
    assert_eq!(
        (
            runtime.work[0].id,
            runtime.work[0].task_type.as_str(),
            runtime.work[0].processed.as_str()
        ),
        (task, "range", "0")
    );
    for n in 0..7 {
        TaskControlStore::enqueue(&backend, "integrity", None, json!({"n":n})).await?;
    }
    sqlx::query("UPDATE tasks SET state='running',processed=42 WHERE id=?")
        .bind(task)
        .execute(db)
        .await?;
    let runtime = transactions::read_runtime(db, &principal).await?;
    assert_eq!(runtime.work.len(), 5);
    assert_eq!(
        (runtime.work[0].id, runtime.work[0].processed.as_str()),
        (task, "42")
    );
    let points = (0..2050)
        .map(|n| point(now - Duration::seconds(n + 1)))
        .collect::<Vec<_>>();
    let mut insert = sqlx::QueryBuilder::new("INSERT INTO runtime_samples(sampled_at,data) ");
    insert.push_values(points, |mut row, p| {
        row.push_bind(p.at).push_bind(sqlx::types::Json(p));
    });
    insert.build().execute(db).await?;
    let history = transactions::read_runtime(db, &principal).await?.history;
    assert_eq!(history.len(), 2048);
    assert!(history.windows(2).all(|p| p[0].at < p[1].at));
    assert_eq!(history.first().unwrap().at, now - Duration::seconds(2047));
    assert_eq!(history.last().unwrap().at, now);
    sqlx::query("UPDATE web_users SET enabled=FALSE WHERE id=?")
        .bind(admin)
        .execute(db)
        .await?;
    assert!(transactions::read_runtime(db, &principal).await.is_err());
    assert_eq!(
        transactions::read_runtime(db, &Principal::Local)
            .await?
            .history
            .len(),
        2048
    );
    sqlx::query("UPDATE web_users SET enabled=TRUE WHERE id=?")
        .bind(admin)
        .execute(db)
        .await?;

    let hour = now
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    sqlx::query(
        "UPDATE access_state SET coverage_since='2000-01-01',flushed_at=CURRENT_TIMESTAMP(6)",
    )
    .execute(db)
    .await?;
    let baseline: DateTime<Utc> = sqlx::query_scalar("SELECT coverage_since FROM access_state")
        .fetch_one(db)
        .await?;
    transactions::flush_access(db, batch(chunk.id, pack, hour), 60.).await?;
    assert_eq!(stats(&backend, chunk.id).await?, (5, 3, 120));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chunk_access_stats")
            .fetch_one(db)
            .await?,
        1
    );
    let windows:Vec<(i64,i64,i64,i64)>=sqlx::query_as("SELECT read_count,range_read_count,read_bytes,range_origin_reads FROM chunk_access_windows ORDER BY window_start").fetch_all(db).await?;
    assert_eq!(windows, vec![(3, 2, 70, 2), (2, 1, 50, 1)]);
    assert_eq!(
        sqlx::query_scalar::<_, DateTime<Utc>>("SELECT coverage_since FROM access_state")
            .fetch_one(db)
            .await?,
        baseline
    );
    let (a, b) = tokio::join!(
        transactions::flush_access(db, batch(chunk.id, pack, hour), 60.),
        transactions::flush_access(db, batch(chunk.id, pack, hour), 60.)
    );
    a?;
    b?;
    assert_eq!(stats(&backend, chunk.id).await?, (15, 9, 360));
    let packed:(i64,i64,i64,i64,i64)=sqlx::query_as("SELECT downloads,downloaded_bytes,partial_downloads,useful_bytes,partial_bytes FROM pack_access_windows WHERE pack_id=?").bind(pack).fetch_one(db).await?;
    assert_eq!(packed, (12, 600, 6, 300, 120));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT downloads FROM pack_member_access_windows")
            .fetch_one(db)
            .await?,
        6
    );
    sqlx::query("UPDATE pack_access_windows SET downloads=? WHERE pack_id=?")
        .bind(i64::MAX)
        .bind(pack)
        .execute(db)
        .await?;
    assert!(
        transactions::flush_access(db, batch(chunk.id, pack, hour), 60.)
            .await
            .is_err()
    );
    assert_eq!(stats(&backend, chunk.id).await?, (15, 9, 360));
    sqlx::query("UPDATE pack_access_windows SET downloads=12 WHERE pack_id=?")
        .bind(pack)
        .execute(db)
        .await?;
    let dropped = AccessBatch {
        dropped: true,
        ..Default::default()
    };
    transactions::flush_access(db, dropped, 60.).await?;
    assert!(
        sqlx::query_scalar::<_, DateTime<Utc>>("SELECT coverage_since FROM access_state")
            .fetch_one(db)
            .await?
            > baseline
    );
    sqlx::query("UPDATE access_state SET coverage_since='2000-01-01',flushed_at='2000-01-01'")
        .execute(db)
        .await?;
    transactions::flush_access(db, AccessBatch::default(), 60.).await?;
    assert!(
        sqlx::query_scalar::<_, DateTime<Utc>>("SELECT coverage_since FROM access_state")
            .fetch_one(db)
            .await?
            > baseline
    );
    transactions::expire_access(db, 0., 1).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chunk_access_windows")
            .fetch_one(db)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pack_access_windows")
            .fetch_one(db)
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pack_member_access_windows")
            .fetch_one(db)
            .await?,
        0
    );
    transactions::expire_access(db, 0., 10).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chunk_access_windows")
            .fetch_one(db)
            .await?,
        0
    );
    assert_eq!(stats(&backend, chunk.id).await?, (15, 9, 360));
    backend.pool.close().await;
    Ok(())
}
