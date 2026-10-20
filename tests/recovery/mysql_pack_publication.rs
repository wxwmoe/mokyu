use super::{Backend, cache_tests, connection, queries, transactions};
use crate::authorization::Principal;
use crate::config::Config;
use crate::content::pack::{Config as PackConfig, Pack};
use crate::model::bucket::CreateBucket;
use crate::model::packing::PackOutput;
use crate::model::task::{Execution, TaskFinish};
use crate::model::{
    AccountStore, BucketStore, ChunkStore, Mapping, PhysicalSource, PhysicalStore, PublishStore,
    QuotaStore, StreamStore, TaskControlStore, TaskExecutionStore,
};
use anyhow::Result;
use serde_json::json;
use uuid::Uuid;

pub(super) async fn execution(backend: &Backend, policy: &str) -> Result<Execution> {
    let id = TaskControlStore::enqueue(backend, "pack", None, json!({"policy":policy})).await?;
    let token = Uuid::new_v4();
    assert_eq!(
        queries::claim_task(id, token)
            .execute(&backend.pool)
            .await?
            .rows_affected(),
        1
    );
    Ok(Execution { id, token })
}
pub(super) async fn finish(backend: &Backend, task: Execution) -> Result<()> {
    TaskControlStore::complete(backend, task).await?;
    TaskExecutionStore::finish(
        backend,
        TaskFinish {
            execution: task,
            error: None,
        },
    )
    .await
}
pub(super) async fn prepared(
    backend: &Backend,
    task: Execution,
    storage: Uuid,
    rows: &[Mapping],
) -> Result<Pack> {
    let mut pack =
        transactions::prepare_pack(&backend.pool, storage, "none", None, 64, 2, task).await?;
    pack.stored_size = Some(40);
    pack.compressed = true;
    pack.digest = Some(vec![7; 32]);
    transactions::pack_metadata(&backend.pool, &pack).await?;
    transactions::insert_pack_members(&backend.pool, pack.id, rows).await?;
    Ok(pack)
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_MYSQL_CONFIG pointing to a fresh isolated MySQL or MariaDB database"]
async fn mysql_pack_publication_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_MYSQL_CONFIG")?;
    let (config, secrets, budget) = Config::load(std::path::Path::new(&path))?;
    let backend = connection::open(&config, &secrets, &budget, "single").await?;
    let db = &backend.pool;
    backend.initialize_admin("admin", "password", false).await?;
    let storage = super::storage_tests::fixture(&backend).await?;
    let bucket = BucketStore::create(
        &backend,
        &Principal::Local,
        CreateBucket {
            name: "packing".into(),
            project_id: None,
            storage_id: Some(storage),
        },
    )
    .await?
    .id;
    let stream = cache_tests::stream(&backend, bucket, "source").await?;
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
    backend.reserve(stream, 96, true, None).await?;
    StreamStore::finish(&backend, stream, 96, "fixture", json!({})).await?;
    assert!(backend.publish(stream, None, None, None).await?);
    let task = execution(&backend, "pack").await?;
    transactions::pin_pack_inputs(db, task, &rows).await?;
    transactions::pin_pack_inputs(db, task, &rows).await?;
    let other = execution(&backend, "pack").await?;
    assert!(
        transactions::pin_pack_inputs(db, other, &rows)
            .await
            .is_err()
    );
    let stale = Execution {
        id: task.id,
        token: Uuid::new_v4(),
    };
    assert!(
        transactions::prepare_encoding(db, &rows[0].chunk, stale)
            .await
            .is_err()
    );
    assert!(
        transactions::prepare_pack(db, storage, "none", None, 64, 2, stale)
            .await
            .is_err()
    );
    assert!(
        transactions::prepare_pack(
            db,
            storage,
            "aes-256-gcm",
            Some(Uuid::new_v4()),
            64,
            2,
            task
        )
        .await
        .is_err()
    );
    let pack = prepared(&backend, task, storage, &rows[..2]).await?;
    assert!(
        transactions::insert_pack_members(db, pack.id, &rows[..2])
            .await
            .is_err()
    );
    let outputs = [PackOutput {
        chunks: rows[..2].iter().map(|r| r.chunk.id).collect(),
        pack: Some(pack.clone()),
        independent: None,
    }];
    let settings = PackConfig::default();
    assert!(
        transactions::commit_pack_outputs(db, stale, &rows[..2], &outputs, false, &settings, 60.)
            .await
            .is_err()
    );
    assert!(
        transactions::commit_pack_outputs(db, other, &rows[..2], &outputs, false, &settings, 60.)
            .await
            .is_err()
    );
    // Omitted live inputs are detected after preparing publication; the entire transaction rolls back.
    assert!(
        transactions::commit_pack_outputs(db, task, &rows, &outputs, false, &settings, 60.)
            .await
            .is_err()
    );
    assert_eq!(
        queries::pack_record(pack.id).fetch_one(db).await?.state,
        "preparing"
    );
    assert!(matches!(
        backend.source(rows[0].chunk.id, false).await?,
        PhysicalSource::Chunk(_)
    ));
    let before_fingerprint: Vec<u8> =
        sqlx::query_scalar("SELECT pack_fingerprint FROM packs WHERE id=?")
            .bind(pack.id)
            .fetch_one(db)
            .await?;
    assert_ne!(before_fingerprint, vec![0; 32]);
    transactions::commit_pack_outputs(db, task, &rows[..2], &outputs, false, &settings, 60.)
        .await?;
    assert!(
        matches!(backend.source(rows[0].chunk.id, false).await?,PhysicalSource::Pack(p) if p.id==pack.id)
    );
    assert_eq!(
        queries::pack_layout_bytes(&[rows[0].chunk.id, rows[1].chunk.id])
            .build_query_scalar::<i64>()
            .fetch_one(db)
            .await?,
        40
    );
    assert_eq!(
        backend.raw(task.id).await?.unwrap()["detail"]["last_rewrite"],
        json!({"before_bytes":64,"output_bytes":40,"temporary_added_bytes":40,"transition_bytes":104})
    );
    assert!(transactions::pack_metadata(db, &pack).await.is_err());
    finish(&backend, task).await?;
    finish(&backend, other).await?;

    // Unpacking can reuse an existing encoding and prepare only the missing one.
    let task = execution(&backend, "range").await?;
    rows.truncate(2);
    for row in &mut rows {
        row.pack_id = Some(pack.id);
    }
    sqlx::query("UPDATE chunk_encodings SET state='ready',standalone_compressed=TRUE,standalone_size=16 WHERE encoding_id=?").bind(rows[0].chunk.encoding_id).execute(db).await?;
    transactions::pin_pack_inputs(db, task, &rows).await?;
    // Quarantine follows the physical source, so a second healthy representation remains usable.
    use crate::model::source::{SourceOperation, SourceScope, SourceType, SourceUpdate};
    for (source, id) in [
        (SourceType::Pack, pack.id),
        (SourceType::Chunk, rows[0].chunk.encoding_id),
    ] {
        let scope = SourceScope {
            storage_id: storage,
            source,
            id: id.to_string(),
        };
        let snapshot = backend.source_snapshot(&scope).await?;
        let operation = SourceOperation {
            principal: Principal::Local,
            mode: "single".into(),
            snapshot,
            code: Some("hash_mismatch".into()),
            node: None,
        };
        let quarantine = execution(&backend, "quarantine").await?;
        backend
            .source_update(SourceUpdate {
                execution: quarantine,
                operation: &operation,
                apply: true,
            })
            .await?;
        let chosen = backend.source(rows[0].chunk.id, false).await?;
        assert!(matches!(
            (&chosen, source),
            (PhysicalSource::Chunk(_), SourceType::Pack)
                | (PhysicalSource::Pack(_), SourceType::Chunk)
        ));
        if source == SourceType::Pack {
            assert!(backend.source(rows[1].chunk.id, false).await.is_err());
        }
        let operation = SourceOperation {
            snapshot: backend.source_snapshot(&scope).await?,
            code: None,
            ..operation
        };
        let recheck = execution(&backend, "recheck").await?;
        backend
            .source_update(SourceUpdate {
                execution: recheck,
                operation: &operation,
                apply: true,
            })
            .await?;
    }
    transactions::pin_pack_inputs(db, task, &rows).await?;
    let existing = rows[0].chunk.clone();
    let mut physical = rows[1].chunk.clone();
    physical.location_id = Uuid::new_v4();
    physical.encoding_id = transactions::prepare_encoding(db, &physical, task).await?;
    assert!(physical.encoding_id > rows[1].chunk.encoding_id);
    physical.compressed = true;
    physical.stored_size = Some(16);
    backend.register_encoding(&physical).await?;
    let unpacked = [
        PackOutput {
            chunks: vec![existing.id],
            pack: None,
            independent: Some(existing),
        },
        PackOutput {
            chunks: vec![physical.id],
            pack: None,
            independent: Some(physical.clone()),
        },
    ];
    transactions::commit_pack_outputs(db, task, &rows, &unpacked, true, &settings, 60.).await?;
    assert_eq!(
        queries::pack_record(pack.id).fetch_one(db).await?.state,
        "retired"
    );
    let PhysicalSource::Chunk(current) = backend.source(rows[0].chunk.id, false).await? else {
        panic!("missing reused encoding")
    };
    assert!(current.compressed);
    assert_eq!(current.stored_size, Some(16));
    let PhysicalSource::Chunk(current) = backend.source(rows[1].chunk.id, false).await? else {
        panic!("missing prepared encoding")
    };
    assert_eq!(current.encoding_id, physical.encoding_id);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT reason FROM pack_maintenance WHERE stream_id=?")
            .bind(stream)
            .fetch_one(db)
            .await?,
        "repack"
    );
    assert!(sqlx::query_scalar::<_,bool>("SELECT split_at IS NOT NULL AND range_split_at IS NOT NULL AND repack_after>CURRENT_TIMESTAMP(6) FROM chunks WHERE id=?").bind(physical.id).fetch_one(db).await?);
    assert!(
        !queries::pack_exclusive(&[physical.id], 0., 3600., 60.)
            .build_query_scalar::<bool>()
            .fetch_one(db)
            .await?
    );

    // A range split is eligible again only with full, fresh access coverage and no partial reads.
    sqlx::query("UPDATE chunks SET repack_after='2000-01-01',reference_changed_at='2000-01-01' WHERE id IN (?,?)").bind(rows[0].chunk.id).bind(rows[1].chunk.id).execute(db).await?;
    sqlx::query(
        "UPDATE access_state SET coverage_since='2000-01-01',flushed_at=CURRENT_TIMESTAMP(6)",
    )
    .execute(db)
    .await?;
    assert!(
        queries::pack_exclusive(&[physical.id], 0., 3600., 60.)
            .build_query_scalar::<bool>()
            .fetch_one(db)
            .await?
    );
    sqlx::query("INSERT INTO chunk_access_windows(chunk_id,window_start,range_read_count) VALUES(?,CURRENT_TIMESTAMP(6),1)").bind(physical.id).execute(db).await?;
    assert!(
        !queries::pack_exclusive(&[physical.id], 0., 3600., 60.)
            .build_query_scalar::<bool>()
            .fetch_one(db)
            .await?
    );
    sqlx::query("DELETE FROM chunk_access_windows WHERE chunk_id=?")
        .bind(physical.id)
        .execute(db)
        .await?;
    sqlx::query("UPDATE access_state SET coverage_since=NULL")
        .execute(db)
        .await?;
    assert!(
        !queries::pack_exclusive(&[physical.id], 0., 3600., 60.)
            .build_query_scalar::<bool>()
            .fetch_one(db)
            .await?
    );
    sqlx::query("UPDATE access_state SET coverage_since='2000-01-01',flushed_at='2000-01-01'")
        .execute(db)
        .await?;
    assert!(
        !queries::pack_exclusive(&[physical.id], 0., 3600., 60.)
            .build_query_scalar::<bool>()
            .fetch_one(db)
            .await?
    );
    finish(&backend, task).await?;

    // Failure retires only the task's prepared outputs and retains every readable source.
    let task = execution(&backend, "pack").await?;
    for row in &mut rows {
        row.pack_id = None;
    }
    transactions::pin_pack_inputs(db, task, &rows).await?;
    let pending = prepared(&backend, task, storage, &rows).await?;
    TaskExecutionStore::finish(
        &backend,
        TaskFinish {
            execution: task,
            error: Some("upload failed".into()),
        },
    )
    .await?;
    assert_eq!(
        queries::pack_record(pending.id).fetch_one(db).await?.state,
        "retired"
    );
    assert!(matches!(
        backend.source(rows[0].chunk.id, false).await?,
        PhysicalSource::Chunk(_)
    ));
    assert!(matches!(
        backend.source(rows[1].chunk.id, false).await?,
        PhysicalSource::Chunk(_)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pack_inputs WHERE task_id=?")
            .bind(task.id)
            .fetch_one(db)
            .await?,
        0
    );
    backend.pool.close().await;
    Ok(())
}
