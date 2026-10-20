use super::{cache_tests, connection, pack_publication_tests as packing, queries, transactions};
use crate::authorization::Principal;
use crate::config::Config;
use crate::content::codec::Chunk;
use crate::model::bucket::CreateBucket;
use crate::model::packing::{PackOutput, RewriteTask};
use crate::model::task::Execution;
use crate::model::{
    AccountStore, BucketStore, ChunkStore, Mapping, PhysicalSource, PhysicalStore, PublishStore,
    QuotaStore, StreamStore, TaskControlStore,
};
use anyhow::Result;
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_SQLITE_CONFIG pointing to a fresh isolated SQLite database"]
async fn sqlite_rewrite_contract() -> Result<()> {
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
    sqlx::query("UPDATE storage_profiles SET compression_enabled=FALSE WHERE storage_id=?")
        .bind(storage)
        .execute(db)
        .await?;
    let bucket = BucketStore::create(
        &backend,
        &Principal::Local,
        CreateBucket {
            name: "rewrite".into(),
            project_id: None,
            storage_id: Some(storage),
        },
    )
    .await?
    .id;
    let a = cache_tests::stream(&backend, bucket, "a").await?;
    let b = cache_tests::stream(&backend, bucket, "b").await?;
    let loose = cache_tests::stream(&backend, bucket, "loose").await?;
    let mut rows = Vec::new();
    for (n, stream, offset) in [(1, a, 0), (2, b, 0), (3, a, 32), (4, b, 32), (5, loose, 0)] {
        let chunk = cache_tests::chunk(&backend, storage, stream, n).await?;
        backend.register_encoding(&chunk).await?;
        ChunkStore::complete(&backend, stream, offset, &chunk).await?;
        rows.push(Mapping {
            chunk,
            offset_bytes: offset,
            pack_id: None,
        });
    }
    for (stream, size) in [(a, 64), (b, 64), (loose, 32)] {
        backend.reserve(stream, size, true, None).await?;
        backend.finish(stream, size, "fixture", json!({})).await?;
        assert!(backend.publish(stream, None, None, None).await?);
    }
    let task = packing::execution(&backend, "pack").await?;
    transactions::pin_pack_inputs(db, task, &rows[..4]).await?;
    let members_a = [rows[0].clone(), rows[2].clone()];
    let members_b = [rows[1].clone(), rows[3].clone()];
    let mut pack_a = packing::prepared(&backend, task, storage, &members_a).await?;
    let mut pack_b = packing::prepared(&backend, task, storage, &members_b).await?;
    for pack in [&mut pack_a, &mut pack_b] {
        pack.compressed = false;
        pack.stored_size = Some(64);
        transactions::pack_metadata(db, pack).await?;
    }
    let output = |pack: crate::content::pack::Pack, members: &[Mapping]| PackOutput {
        chunks: members.iter().map(|r| r.chunk.id).collect(),
        pack: Some(pack),
        independent: None,
    };
    transactions::commit_pack_outputs(
        db,
        task,
        &rows[..4],
        &[
            output(pack_a.clone(), &members_a),
            output(pack_b.clone(), &members_b),
        ],
        false,
        &Default::default(),
        60.,
    )
    .await?;
    packing::finish(&backend, task).await?;
    assert_eq!(
        queries::rewrite_counts(storage, "none", None)
            .build_query_as::<(i64, i64)>()
            .fetch_one(db)
            .await?,
        (0, 0)
    );
    assert_eq!(
        queries::rewrite_counts(storage, "aes-256-gcm", Some(Uuid::new_v4()))
            .build_query_as::<(i64, i64)>()
            .fetch_one(db)
            .await?,
        (1, 4)
    );
    sqlx::query("UPDATE storage_profiles SET compression_enabled=TRUE WHERE storage_id=?")
        .bind(storage)
        .execute(db)
        .await?;
    assert_eq!(
        queries::rewrite_counts(storage, "none", None)
            .build_query_as::<(i64, i64)>()
            .fetch_one(db)
            .await?,
        (1, 4)
    );
    sqlx::query("UPDATE storage_profiles SET compression_level=compression_level+1,revision=revision+1 WHERE storage_id=?").bind(storage).execute(db).await?;
    assert_eq!(
        queries::rewrite_counts(storage, "none", None)
            .build_query_as::<(i64, i64)>()
            .fetch_one(db)
            .await?,
        (1, 4)
    );
    let input = RewriteTask {
        storage_id: storage,
        upper_chunk_id: 0.into(),
        shard: 0,
        shards: 1,
        profile_revision: 2,
        target_key_id: None,
        target_key: "".into(),
        target_algorithm: "none".into(),
    };
    let (one, two) = tokio::join!(
        transactions::start_rewrite(db, &input),
        transactions::start_rewrite(db, &input)
    );
    let one = one?;
    let two = two?;
    assert_eq!(one.0, two.0);
    assert_ne!(one.1, two.1);
    let detail = backend.raw(one.0).await?.unwrap()["detail"].clone();
    assert!(detail.get("role").is_none() && detail.get("shards").is_none());
    let job: RewriteTask = serde_json::from_value(detail)?;
    assert_eq!(job.upper_chunk_id, rows[4].chunk.id);
    let task = Execution {
        id: one.0,
        token: Uuid::new_v4(),
    };
    queries::claim_task(task.id, task.token).execute(db).await?;
    let stale = Execution {
        id: task.id,
        token: Uuid::new_v4(),
    };
    assert_eq!(
        queries::rewrite_pack_candidate(0.into(), &job)
            .build_query_as::<(crate::model::Id, crate::model::Id)>()
            .fetch_optional(db)
            .await?,
        Some((rows[0].chunk.id, pack_a.id))
    );

    // An old physical pack retains all members, but only current members switch to its replacement.
    sqlx::query("UPDATE chunk_encodings SET state='ready' WHERE encoding_id=?")
        .bind(rows[2].chunk.encoding_id)
        .execute(db)
        .await?;
    sqlx::query("UPDATE chunks SET pack_id=NULL WHERE id=?")
        .bind(rows[2].chunk.id)
        .execute(db)
        .await?;
    assert_eq!(
        queries::rewrite_current_members(pack_a.id)
            .fetch_all(db)
            .await?,
        vec![rows[0].chunk.id]
    );
    let current = vec![rows[0].chunk.id];
    let members = queries::rewrite_members(pack_a.id)
        .build_query_as::<Chunk>()
        .fetch_all(db)
        .await?;
    let inputs = [Mapping {
        chunk: members[0].clone(),
        offset_bytes: 0,
        pack_id: Some(pack_a.id),
    }];
    transactions::pin_pack_inputs(db, task, &inputs).await?;
    let mut replacement =
        transactions::prepare_pack(db, storage, "none", None, 64, 2, task).await?;
    replacement.compressed = true;
    replacement.stored_size = Some(36);
    replacement.digest = Some(vec![8; 32]);
    assert!(
        transactions::commit_rewrite_pack(
            db,
            stale,
            &pack_a,
            &replacement,
            &current,
            &members,
            current[0]
        )
        .await
        .is_err()
    );
    assert!(
        transactions::commit_rewrite_pack(
            db,
            task,
            &pack_a,
            &replacement,
            &[rows[0].chunk.id, rows[2].chunk.id],
            &members,
            current[0]
        )
        .await
        .is_err()
    );
    assert_eq!(
        queries::pack_record(pack_a.id).fetch_one(db).await?.state,
        "ready"
    );
    assert!(
        queries::pack_member_ids(replacement.id)
            .fetch_all(db)
            .await?
            .is_empty()
    );
    transactions::commit_rewrite_pack(
        db,
        task,
        &pack_a,
        &replacement,
        &current,
        &members,
        current[0],
    )
    .await?;
    assert_eq!(
        queries::pack_record(pack_a.id).fetch_one(db).await?.state,
        "retired"
    );
    assert!(
        matches!(backend.source(rows[0].chunk.id, false).await?,PhysicalSource::Pack(p) if p.id==replacement.id)
    );
    assert!(matches!(
        backend.source(rows[2].chunk.id, false).await?,
        PhysicalSource::Chunk(_)
    ));
    assert_eq!(
        backend.raw(task.id).await?.unwrap()["cursor"],
        rows[0].chunk.id.to_string()
    );
    // Advancing past the maximum member ID would skip the interleaved second pack.
    assert_eq!(
        queries::rewrite_pack_candidate(rows[0].chunk.id, &job)
            .build_query_as::<(crate::model::Id, crate::model::Id)>()
            .fetch_optional(db)
            .await?,
        Some((rows[1].chunk.id, pack_b.id))
    );
    let members = queries::rewrite_members(pack_b.id)
        .build_query_as::<Chunk>()
        .fetch_all(db)
        .await?;
    let current = queries::rewrite_current_members(pack_b.id)
        .fetch_all(db)
        .await?;
    let inputs: Vec<_> = members
        .iter()
        .map(|chunk| Mapping {
            chunk: chunk.clone(),
            offset_bytes: 0,
            pack_id: Some(pack_b.id),
        })
        .collect();
    transactions::pin_pack_inputs(db, task, &inputs).await?;
    let mut replacement =
        transactions::prepare_pack(db, storage, "none", None, 64, 2, task).await?;
    replacement.compressed = true;
    replacement.stored_size = Some(36);
    replacement.digest = Some(vec![8; 32]);
    transactions::commit_rewrite_pack(
        db,
        task,
        &pack_b,
        &replacement,
        &current,
        &members,
        current[0],
    )
    .await?;
    assert!(
        queries::rewrite_pack_candidate(0.into(), &job)
            .build_query_as::<(crate::model::Id, crate::model::Id)>()
            .fetch_optional(db)
            .await?
            .is_none()
    );

    let mut after = crate::model::Id::default();
    let mut changed = Vec::new();
    while let Some(source) = queries::rewrite_loose_candidate(after, &job)
        .build_query_as::<Chunk>()
        .fetch_optional(db)
        .await?
    {
        transactions::pin_pack_inputs(
            db,
            task,
            &[Mapping {
                chunk: source.clone(),
                offset_bytes: 0,
                pack_id: None,
            }],
        )
        .await?;
        let mut physical = source.clone();
        physical.location_id = Uuid::new_v4();
        physical.encoding_id = transactions::prepare_encoding(db, &physical, task).await?;
        physical.compressed = true;
        physical.stored_size = Some(16);
        assert!(
            transactions::commit_rewrite_chunk(db, stale, &source, &physical)
                .await
                .is_err()
        );
        let mut changed_source = source.clone();
        changed_source.encoding_id = i64::MAX.into();
        assert!(
            transactions::commit_rewrite_chunk(db, task, &changed_source, &physical)
                .await
                .is_err()
        );
        transactions::commit_rewrite_chunk(db, task, &source, &physical).await?;
        assert!(
            transactions::commit_rewrite_chunk(db, task, &source, &physical)
                .await
                .is_err()
        );
        assert!(
            matches!(backend.source(source.id, false).await?,PhysicalSource::Chunk(c) if c.encoding_id==physical.encoding_id && c.compressed)
        );
        changed.push(source.id);
        after = source.id;
    }
    assert_eq!(changed, vec![rows[2].chunk.id, rows[4].chunk.id]);
    assert_eq!(
        queries::rewrite_counts(storage, "none", None)
            .build_query_as::<(i64, i64)>()
            .fetch_one(db)
            .await?,
        (0, 0)
    );
    packing::finish(&backend, task).await?;
    let empty = super::storage_tests::fixture_named(&backend, "empty").await?;
    let mut input = input.clone();
    input.storage_id = empty;
    let completed = transactions::start_rewrite(db, &input).await?.0;
    assert_eq!(backend.raw(completed).await?.unwrap()["state"], "completed");
    input.storage_id = Uuid::new_v4();
    assert!(transactions::start_rewrite(db, &input).await.is_err());

    backend.check_owner()?;
    backend.read.close().await;
    backend.write.close().await;
    Ok(())
}
