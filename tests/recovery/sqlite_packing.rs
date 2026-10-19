use super::{Backend, cache_tests, connection, pack_publication_tests};
use crate::authorization::{DEFAULT_PROJECT, Principal};
use crate::config::Config;
use crate::model::bucket::CreateBucket;
use crate::model::metrics::ScopeQuery;
use crate::model::packing::{PackOutput, PackingStore};
use crate::model::{
    AccountStore, BucketStore, ChunkStore, Mapping, PhysicalStore, PublishStore, QuotaStore,
    StreamStore, TaskControlStore,
};
use anyhow::Result;
use chrono::{Duration, Utc};
use serde_json::json;
use uuid::Uuid;

async fn publish(backend: &Backend, stream: Uuid, size: i64) -> Result<()> {
    backend.reserve(stream, size, true, None).await?;
    StreamStore::finish(backend, stream, size, "fixture", json!({})).await?;
    assert!(backend.publish(stream, None, None, None).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_SQLITE_CONFIG pointing to a fresh isolated SQLite database"]
async fn sqlite_packing_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_SQLITE_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.write;
    backend.initialize_admin("admin", "password", false).await?;
    let storage = super::storage_tests::fixture(&backend).await?;
    let mut buckets = Vec::new();
    for name in ["private", "visible"] {
        buckets.push(
            BucketStore::create(
                &backend,
                &Principal::Local,
                CreateBucket {
                    name: name.into(),
                    project_id: None,
                    storage_id: Some(storage),
                },
            )
            .await?
            .id,
        );
    }
    let stream = cache_tests::stream(&backend, buckets[0], "source").await?;
    let mut rows = Vec::new();
    for n in 0..3u8 {
        let chunk = cache_tests::chunk(&backend, storage, stream, n + 1).await?;
        backend.register_encoding(&chunk).await?;
        ChunkStore::complete(&backend, stream, i64::from(n) * 32, &chunk).await?;
        rows.push(Mapping {
            chunk,
            offset_bytes: i64::from(n) * 32,
            pack_id: None,
        });
    }
    publish(&backend, stream, 96).await?;
    let candidates = backend.pack_candidates(stream, 0, 60.).await?;
    assert_eq!(candidates.len(), 3);
    assert!(candidates.iter().all(|c| c.eligible && c.unit_count == 1));
    assert!(
        backend
            .same_references(rows[0].chunk.id, rows[1].chunk.id, 32)
            .await?
    );
    let task = pack_publication_tests::execution(&backend, "pack").await?;
    backend.pin_pack_inputs(task, &rows[..2]).await?;
    let pack = pack_publication_tests::prepared(&backend, task, storage, &rows[..2]).await?;
    assert!(backend.preparing_packs().await?);
    backend
        .commit_pack_outputs(
            task,
            &rows[..2],
            &[PackOutput {
                chunks: rows[..2].iter().map(|r| r.chunk.id).collect(),
                pack: Some(pack.clone()),
                independent: None,
            }],
            false,
            &Default::default(),
            60.,
        )
        .await?;
    pack_publication_tests::finish(&backend, task).await?;
    assert!(!backend.preparing_packs().await?);
    assert!(backend.in_storage(pack.id, storage).await?);
    assert!(!backend.in_storage(pack.id, Uuid::new_v4()).await?);
    assert_eq!(backend.pack_layout(pack.id).await?.len(), 2);
    assert_eq!(backend.range_pack_layout(pack.id).await?.len(), 2);
    assert_eq!(backend.pack_dimensions(pack.id).await?, (40, 64, 2));
    assert_eq!(backend.pack_raw_size(pack.id).await?, 64);
    assert_eq!(backend.pack_stored_size(pack.id).await?, 40);
    assert_eq!(backend.pack_member_count(pack.id).await?, 2);
    assert!(backend.chunk_has_references(rows[0].chunk.id).await?);
    assert_eq!(backend.physical_bytes(&[]).await?, 0);
    assert_eq!(
        backend
            .physical_bytes(&rows.iter().map(|r| r.chunk.id).collect::<Vec<_>>())
            .await?,
        72
    );
    assert_eq!(backend.ready_encoding(rows[0].chunk.id).await?, None);
    assert_eq!(
        backend.ready_encoding(rows[2].chunk.id).await?,
        Some((rows[2].chunk.encoding_id, 32))
    );
    assert_eq!(backend.pack_stream_metadata(stream).await?.1, "source");
    assert_eq!(backend.unpack_totals(None).await?, (1, 64));
    assert_eq!(
        backend.unpack_identity(None).await?,
        (pack.id.to_string(), pack.id.to_string())
    );
    assert_eq!(backend.pack_totals().await?[0]["stored_bytes"], 40);
    assert_eq!(
        PackingStore::page(&backend, 0, 2).await?[0]["id"],
        pack.id.to_string()
    );
    let (detail, members) = PackingStore::detail(&backend, pack.id).await?;
    assert_eq!(members.len(), 2);
    assert_eq!(members[0]["current"], true);
    assert!(detail["created_at"].as_str().unwrap().ends_with('Z'));
    assert_eq!(detail["digest"], format!("\\x{}", "07".repeat(32)));
    assert_eq!(
        backend.pack_candidates(stream, 32, 60.).await?[0].unit_count,
        2
    );
    let cutoff = Utc::now();
    assert_eq!(
        backend
            .next_pack(0, None, Some(cutoff), Some(storage))
            .await?,
        Some(pack.id)
    );
    assert_eq!(backend.next_pack(pack.id, None, None, None).await?, None);
    assert_eq!(
        backend
            .next_pack_stream(Uuid::nil(), -1, cutoff, Some(storage))
            .await?,
        Some(stream)
    );
    assert_eq!(
        backend.next_pack_stream(stream, -1, cutoff, None).await?,
        None
    );
    assert_eq!(
        backend.next_pack_stream(stream, 0, cutoff, None).await?,
        Some(stream)
    );
    assert!(
        backend
            .next_pack_queue(cutoff, "pack", Some(storage))
            .await?
            .is_some()
    );
    assert!(!backend.pack_has_pending(pack.id).await?);
    assert!(!backend.pack_has_writers(pack.id).await?);
    assert!(!backend.range_pack_blocked(pack.id).await?);

    // Only the union of live, authorized source intervals is visible to an inspector.
    for (name, lo, hi) in [
        ("a", 0, 16),
        ("b", 4, 10),
        ("c", 8, 24),
        ("d", 28, 32),
        ("e", 0, 16),
    ] {
        let copy = cache_tests::stream(&backend, buckets[1], name).await?;
        backend.reference(copy, 0, rows[0].chunk.id, 32).await?;
        assert!(backend.pack_has_writers(pack.id).await?);
        assert!(backend.range_pack_blocked(pack.id).await?);
        sqlx::query("UPDATE extents SET source_offset=?,length=? WHERE stream_id=?")
            .bind(lo)
            .bind(hi - lo)
            .bind(copy)
            .execute(db)
            .await?;
        publish(&backend, copy, i64::from(hi - lo)).await?;
    }
    assert!(
        backend
            .next_pack_change(Utc::now(), "reuse", Some(storage))
            .await?
            .is_some()
    );
    assert!(!backend.pack_candidates(stream, 0, 60.).await?[0].eligible);
    let user = Uuid::new_v4();
    let token = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO web_users(id,username,password_hash) VALUES(?,'inspector','password')",
    )
    .bind(user)
    .execute(db)
    .await?;
    sqlx::query("INSERT INTO project_members(user_id,project_id,role,scope) VALUES(?,?,'reader','selected')").bind(user).bind(DEFAULT_PROJECT).execute(db).await?;
    sqlx::query(
        "INSERT INTO member_bucket_grants(user_id,project_id,bucket_id,actions) VALUES(?,?,?,67)",
    )
    .bind(user)
    .bind(DEFAULT_PROJECT)
    .bind(buckets[1])
    .execute(db)
    .await?;
    sqlx::query("INSERT INTO api_tokens(id,user_id,label,token_hash,prefix,system_access,auth_revision) VALUES(?,?,'inspect',?,'inspect',FALSE,0)").bind(token).bind(user).bind([8u8;32].as_slice()).execute(db).await?;
    sqlx::query("INSERT INTO token_bucket_grants(token_id,bucket_id,actions) VALUES(?,?,64)")
        .bind(token)
        .bind(buckets[1])
        .execute(db)
        .await?;
    let principal = Principal::Token {
        id: token,
        user,
        revision: 0,
        user_revision: 0,
    };
    let scope = ScopeQuery::default();
    let page = backend.scoped_page(&principal, &scope, 0, 10, None).await?;
    assert_eq!(page.len(), 1);
    assert!(
        page[0].raw_size.is_none()
            && page[0].stored_size.is_none()
            && page[0].member_count.is_none()
    );
    assert!(
        backend
            .scoped_page(&principal, &scope, 0, 10, Some("retired"))
            .await?
            .is_empty()
    );
    let (_, members, limited) = backend
        .scoped_detail(&principal, &scope, pack.id, 0, 1)
        .await?;
    assert!(limited);
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].chunk_id, rows[0].chunk.id.to_string());
    assert_eq!(members[0].visible_bytes, "28");
    assert!(
        members[0].ordinal.is_none() && members[0].raw_size.is_none() && members[0].current_source
    );
    assert!(
        backend
            .scoped_detail(&principal, &scope, pack.id, rows[0].chunk.id, 10)
            .await?
            .1
            .is_empty()
    );
    assert!(
        backend
            .scoped_objects(&principal, &scope, pack.id, None)
            .await?
            .is_empty()
    );
    assert!(
        backend
            .scoped_page(
                &principal,
                &ScopeQuery {
                    bucket: Some(buckets[0]),
                    project: None
                },
                0,
                10,
                None
            )
            .await
            .is_err()
    );
    sqlx::query("UPDATE token_bucket_grants SET actions=65 WHERE token_id=?")
        .bind(token)
        .execute(db)
        .await?;
    let visible = backend
        .scoped_objects(&principal, &scope, pack.id, None)
        .await?;
    assert_eq!(visible.len(), 5);
    assert!(visible.iter().all(|o| o.bucket_id == buckets[1]
        && o.bucket_name == "visible"
        && o.content_type == "text/plain"
        && !o.public_read));
    assert_eq!(
        backend
            .scoped_objects(&principal, &scope, pack.id, Some((buckets[1], "c".into())))
            .await?
            .iter()
            .map(|o| o.key.as_str())
            .collect::<Vec<_>>(),
        vec!["d", "e"]
    );
    let (summary, members, limited) = backend
        .scoped_detail(&Principal::Local, &scope, pack.id, 0, 10)
        .await?;
    assert!(!limited);
    assert_eq!(summary.raw_size.as_deref(), Some("64"));
    assert_eq!(members.len(), 2);
    assert!(
        members
            .iter()
            .all(|m| m.visible_bytes == "32" && m.ordinal.is_some())
    );
    let scoped_admin = ScopeQuery {
        bucket: Some(buckets[1]),
        project: None,
    };
    assert_eq!(
        backend
            .scoped_detail(&Principal::Local, &scoped_admin, pack.id, 0, 10)
            .await?
            .1
            .len(),
        1
    );
    sqlx::query("UPDATE api_tokens SET authorization_revision=1 WHERE id=?")
        .bind(token)
        .execute(db)
        .await?;
    assert!(
        backend
            .scoped_page(&principal, &scope, 0, 10, None)
            .await
            .is_err()
    );
    sqlx::query("UPDATE api_tokens SET authorization_revision=0 WHERE id=?")
        .bind(token)
        .execute(db)
        .await?;

    sqlx::query("INSERT INTO pack_access_windows(pack_id,window_start,downloads,partial_downloads,partial_bytes) VALUES(?,(CAST(unixepoch('subsec')*1000000 AS INTEGER)/3600000000)*3600000000,12,10,400),(?,CAST(unixepoch('subsec')*1000000 AS INTEGER)-172800000000,100,90,9000)").bind(pack.id).bind(pack.id).execute(db).await?;
    sqlx::query("INSERT INTO pack_member_access_windows(pack_id,window_start,chunk_id,downloads) VALUES(?,CAST(unixepoch('subsec')*1000000 AS INTEGER),?,7)").bind(pack.id).bind(rows[0].chunk.id).execute(db).await?;
    assert_eq!(
        backend.pack_range_totals(pack.id, 3600.).await?,
        (10, 400, 2)
    );
    assert_eq!(
        backend.pack_member_hits(pack.id, 3600.).await?,
        vec![(rows[0].chunk.id, 7)]
    );
    assert_eq!(
        backend
            .next_range_pack(storage, Utc::now(), 3600., 10)
            .await?,
        Some(pack.id)
    );
    assert_eq!(
        backend
            .next_range_pack(storage, Utc::now(), 3600., 11)
            .await?,
        None
    );
    sqlx::query("UPDATE storage_profiles SET pack_range_enabled=TRUE WHERE storage_id=?")
        .bind(storage)
        .execute(db)
        .await?;
    let range = backend.start_pack("range", false, None).await?;
    let range = crate::model::task::Execution {
        id: range["task_id"].as_str().unwrap().parse()?,
        token: Uuid::new_v4(),
    };
    assert_eq!(
        super::queries::claim_task(range.id, range.token)
            .execute(db)
            .await?
            .rows_affected(),
        1
    );
    backend
        .record_range(range, pack.id, 2, json!({"result":"keep"}))
        .await?;
    assert_eq!(
        backend
            .next_range_pack(storage, Utc::now() + Duration::seconds(1), 3600., 1)
            .await?,
        None
    );
    backend.complete_range_task(range).await?;
    assert_eq!(backend.raw(range.id).await?.unwrap()["state"], "completed");
    let chunk = rows[2].chunk.id;
    sqlx::query("UPDATE chunks SET split_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),range_split_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),repack_after=NULL WHERE id=?").bind(chunk).execute(db).await?;
    assert!(backend.stream_repack_after(stream, 3600.).await?.is_some());
    sqlx::query("UPDATE chunks SET repack_after=CAST(unixepoch('subsec')*1000000 AS INTEGER)-1000000 WHERE id=?")
        .bind(chunk)
        .execute(db)
        .await?;
    backend
        .defer_range_repack(stream, 3600., 60., 600., 64)
        .await?;
    assert!(
        backend.stream_repack_after(stream, 0.).await?.unwrap()
            > Utc::now() + Duration::seconds(500)
    );
    sqlx::query("UPDATE chunks SET repack_after=CAST(unixepoch('subsec')*1000000 AS INTEGER)-1000000 WHERE id=?")
        .bind(chunk)
        .execute(db)
        .await?;
    sqlx::query("UPDATE access_state SET coverage_since=CAST(unixepoch('subsec')*1000000 AS INTEGER)-172800000000,flushed_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)").execute(db).await?;
    backend
        .defer_range_repack(stream, 3600., 60., 600., 64)
        .await?;
    let deferred: bool = sqlx::query_scalar(
        "SELECT repack_after>CAST(unixepoch('subsec')*1000000 AS INTEGER) FROM chunks WHERE id=?",
    )
    .bind(chunk)
    .fetch_one(db)
    .await?;
    assert!(!deferred);
    sqlx::query("INSERT INTO chunk_access_windows(chunk_id,window_start,range_read_count) VALUES(?,CAST(unixepoch('subsec')*1000000 AS INTEGER),1)").bind(chunk).execute(db).await?;
    backend
        .defer_range_repack(stream, 3600., 60., 600., 64)
        .await?;
    assert!(backend.stream_repack_after(stream, 0.).await?.is_some());

    // A historical member is no longer visible through the pack after it moves out.
    sqlx::query("UPDATE chunk_encodings SET state='ready' WHERE encoding_id=?")
        .bind(rows[0].chunk.encoding_id)
        .execute(db)
        .await?;
    assert!(
        !backend
            .scoped_detail(&principal, &scope, pack.id, 0, 10)
            .await?
            .1[0]
            .current_source
    );
    sqlx::query("UPDATE chunks SET pack_id=NULL WHERE id=?")
        .bind(rows[0].chunk.id)
        .execute(db)
        .await?;
    assert!(
        backend
            .scoped_page(&principal, &scope, 0, 10, None)
            .await?
            .is_empty()
    );
    assert!(
        backend
            .scoped_detail(&principal, &scope, pack.id, 0, 10)
            .await
            .is_err()
    );
    assert!(
        backend
            .scoped_objects(&principal, &scope, pack.id, None)
            .await?
            .is_empty()
    );
    assert_eq!(
        backend
            .scoped_detail(&Principal::Local, &scope, pack.id, 0, 10)
            .await?
            .1
            .len(),
        2
    );
    let copy = cache_tests::stream(&backend, buckets[1], "remaining").await?;
    backend.reference(copy, 0, rows[1].chunk.id, 32).await?;
    publish(&backend, copy, 32).await?;
    let (_, visible, _) = backend
        .scoped_detail(&principal, &scope, pack.id, 0, 1)
        .await?;
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].chunk_id, rows[1].chunk.id.to_string());
    assert!(
        backend
            .start_pack("pack", false, Some(Uuid::new_v4()))
            .await
            .is_err()
    );
    let held = backend
        .write
        .begin_with(super::queries::BEGIN_WRITE)
        .await?;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        assert!(
            !backend
                .scoped_page(&Principal::Local, &scope, 0, 10, None)
                .await?
                .is_empty()
        );
        assert_eq!(
            backend
                .scoped_detail(&Principal::Local, &scope, pack.id, 0, 10)
                .await?
                .1
                .len(),
            2
        );
        assert_eq!(backend.detail(pack.id).await?.1.len(), 2);
        anyhow::Ok(())
    })
    .await??;
    held.rollback().await?;
    backend.check_owner()?;
    backend.read.close().await;
    backend.write.close().await;
    Ok(())
}
