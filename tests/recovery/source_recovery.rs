use super::*;
use crate::{
    authorization::{Action, Permit, Principal},
    model::{
        ChunkCreate, PhysicalSource, Stores, StreamCreate, TaskClaim,
        source::{SourceOperation, SourceScope, SourceType, SourceUpdate},
        task::Execution,
    },
};
use serde_json::json;

async fn stream(stores: &Stores, bucket: Uuid, key: &str) -> Result<Uuid> {
    let id = stores
        .streams
        .create(StreamCreate {
            authority: Permit::for_action(Principal::Local, bucket, Action::Write),
            bucket,
            key: key.into(),
            stream_type: "object".into(),
            metadata: json!({}),
            public: false,
            claim_object: true,
            completion: None,
            owner: None,
        })
        .await?;
    stores.quota.reserve(id, 32, false, None).await?;
    Ok(id)
}
fn input(storage: Uuid, stream: Uuid) -> ChunkCreate {
    ChunkCreate {
        storage,
        stream,
        hash: [7; 32],
        raw_size: 32,
        algorithm: "none".into(),
        key: None,
    }
}
async fn quarantine(stores: &Stores, storage: Uuid, encoding: i64, clear: bool) -> Result<()> {
    let scope = SourceScope {
        storage_id: storage,
        source: SourceType::Chunk,
        id: encoding.to_string(),
    };
    let operation = SourceOperation {
        principal: Principal::Local,
        mode: "single".into(),
        snapshot: stores.physical.source_snapshot(&scope).await?,
        code: if clear {
            None
        } else {
            Some("hash_mismatch".into())
        },
        node: None,
    };
    let id = stores
        .task_control
        .enqueue(
            "integrity",
            None,
            json!({"source_operation":{},"operation":"quarantine"}),
        )
        .await?;
    let token = Uuid::new_v4();
    assert_eq!(
        stores
            .task_execution
            .claim(TaskClaim {
                class: 0,
                active: &[],
                token
            })
            .await?,
        Some(id)
    );
    stores
        .physical
        .source_update(SourceUpdate {
            execution: Execution { id, token },
            operation: &operation,
            apply: true,
        })
        .await
}
async fn candidate(stores: &Stores, storage: Uuid, stream: Uuid) -> Result<Repair> {
    let RepairAllocation::Candidate(candidate) = stores
        .chunks
        .prepare_repair(input(storage, stream), None)
        .await?
    else {
        panic!("expected recovery candidate")
    };
    Ok(*candidate)
}
async fn metadata(stores: &Stores, repair: &mut Repair) -> Result<()> {
    repair.chunk.stored_size = Some(32);
    stores.chunks.repair_metadata(repair).await
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_CONFIG pointing to a fresh isolated single-mode database"]
async fn source_recovery_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_CONFIG")?;
    let (config, mut secrets, budget) = crate::config::Config::load(std::path::Path::new(&path))?;
    let (stores, mut owner) =
        crate::model::connect(&config, &mut secrets, &budget, "single").await?;
    stores.runtime.initialize(false).await?;
    let storage = Uuid::new_v4();
    let mut creation = stores.storage_records.creation().await?;
    creation
        .writer
        .prepare(
            &crate::storage::model::Record {
                id: storage,
                name: "recovery".into(),
                r#type: "local".into(),
                path: Some("/data/recovery".into()),
                endpoint_url: None,
                remote_bucket_name: None,
                region: None,
                prefix: String::new(),
                access_key_encrypted: None,
                secret_key_encrypted: None,
                identity_marker: crate::storage::model::identity_marker(
                    creation.deployment,
                    storage,
                )?,
                enabled: true,
            },
            None,
            None,
            None,
            None,
            None,
        )
        .await?;
    creation.writer.finish().await?;
    let bucket = stores
        .buckets
        .create(
            &Principal::Local,
            crate::model::bucket::CreateBucket {
                name: "recovery".into(),
                project_id: None,
                storage_id: Some(storage),
            },
        )
        .await?
        .id;
    let original = stream(&stores, bucket, "original").await?;
    let (mut chunk, _) = stores.chunks.allocate(input(storage, original)).await?;
    chunk.location_id = Uuid::new_v4();
    chunk.stored_size = Some(32);
    stores.chunks.mark_uploading(&chunk).await?;
    stores.physical.register_encoding(&chunk).await?;
    stores.chunks.complete(original, 0, &chunk).await?;
    stores
        .streams
        .finish(original, 32, "original", json!({}))
        .await?;
    assert!(stores.publish.publish(original, None, None, None).await?);
    quarantine(&stores, storage, chunk.encoding_id, false).await?;
    let first = stream(&stores, bucket, "first").await?;
    let mut a = candidate(&stores, storage, first).await?;
    let second = stream(&stores, bucket, "second").await?;
    let mut b = candidate(&stores, storage, second).await?;
    assert_eq!(a.chunk.id, chunk.id);
    assert_ne!(a.chunk.location_id, chunk.location_id);
    assert_ne!(a.chunk.encoding_id, b.chunk.encoding_id);
    assert!(
        stores.chunks.finish_repair(&a, 0).await.is_err(),
        "unverified metadata cannot publish"
    );
    let mut invalid = a.clone();
    invalid.chunk.hash[0] ^= 1;
    assert!(stores.chunks.repair_metadata(&invalid).await.is_err());
    metadata(&stores, &mut a).await?;
    metadata(&stores, &mut b).await?;
    let restored = stores.chunks.finish_repair(&a, 0).await?;
    assert_eq!(restored.id, chunk.id);
    assert_eq!(restored.encoding_id, a.chunk.encoding_id);
    let reused = stores.chunks.finish_repair(&b, 0).await?;
    assert_eq!(
        reused.encoding_id, restored.encoding_id,
        "loser must reuse the winning source"
    );
    assert!(
        matches!(stores.physical.source(chunk.id,false).await?, PhysicalSource::Chunk(c) if c.encoding_id==restored.encoding_id)
    );
    assert_eq!(
        stores.chunks.extents(original, 0, 32).await?[0].chunk_id,
        Some(chunk.id)
    );
    assert_eq!(
        stores.chunks.extents(second, 0, 32).await?[0].chunk_id,
        Some(chunk.id)
    );
    let old = stores.physical.quarantines(storage, false, 0, 10).await?;
    assert_eq!(old.len(), 1);
    assert_eq!(old[0].state, "retired");

    quarantine(&stores, storage, restored.encoding_id, false).await?;
    let stale = stream(&stores, bucket, "stale").await?;
    let mut repair = candidate(&stores, storage, stale).await?;
    metadata(&stores, &mut repair).await?;
    quarantine(&stores, storage, restored.encoding_id, true).await?;
    quarantine(&stores, storage, restored.encoding_id, false).await?;
    assert!(
        stores.chunks.finish_repair(&repair, 0).await.is_err(),
        "ABA must not replace a new quarantine"
    );
    let superseded = stream(&stores, bucket, "superseded").await?;
    let mut replaced = candidate(&stores, storage, superseded).await?;
    metadata(&stores, &mut replaced).await?;
    stream(&stores, bucket, "superseded").await?;
    assert!(
        stores.chunks.finish_repair(&replaced, 0).await.is_err(),
        "superseded write cannot repair shared content"
    );

    let mut claimed = false;
    for _ in 0..100 {
        if stores
            .maintenance
            .claim_stream(stale, Some(storage), Some(0.))
            .await?
        {
            claimed = true;
            break;
        }
        // SQLite cleanup intentionally skips a busy writer, including a pending rollback.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(claimed);
    stores.maintenance.cleanup_stream(stale, 100).await?;
    let encoding = repair.chunk.encoding_id;
    assert!(
        stores
            .maintenance
            .claim_physical(false, encoding, -1., 0.)
            .await?
    );
    stores
        .maintenance
        .delete_physical(false, encoding, 3600.)
        .await?;
    stores
        .maintenance
        .cleanup_history("chunk_encodings", -1., 100, &[], 1000)
        .await?;
    assert!(
        stores
            .maintenance
            .physical_candidates(false, -1., 100, 0., storage)
            .await?
            .iter()
            .any(|row| row.0 == encoding),
        "uncertain write must remain tracked after the first delete"
    );
    assert!(
        stores
            .maintenance
            .claim_physical(false, encoding, -1., 0.)
            .await?
    );
    stores
        .maintenance
        .delete_physical(false, encoding, 0.)
        .await?;
    assert!(
        !stores
            .maintenance
            .physical_candidates(false, -1., 100, 0., storage)
            .await?
            .iter()
            .any(|row| row.0 == encoding)
    );
    stores
        .maintenance
        .cleanup_history("chunk_encodings", -1., 100, &[], 1000)
        .await?;
    assert!(
        stores
            .physical
            .source_snapshot(&SourceScope {
                storage_id: storage,
                source: SourceType::Chunk,
                id: encoding.to_string()
            })
            .await
            .is_err()
    );
    assert!(stores.chunks.finish_repair(&repair, 0).await.is_err());
    if let Some(cluster) = stores.cluster.as_ref() {
        use sqlx::Connection;
        let options = sqlx::postgres::PgConnectOptions::new()
            .host(&config.database.host)
            .port(config.database.effective_port().unwrap())
            .database(&config.database.name)
            .username(&config.database.user)
            .password(&secrets.database_password);
        let mut db = sqlx::PgConnection::connect_with(&options).await?;
        sqlx::query("UPDATE mokyu_meta SET deployment_mode='cluster'")
            .execute(&mut db)
            .await?;
        let state = cluster
            .register(
                "repair-node",
                "data",
                Uuid::new_v4().to_string(),
                &secrets.credential_key,
            )
            .await?
            .unwrap();
        let node = RepairNode {
            state: state.clone(),
            role: "data".into(),
            key: secrets.credential_key.clone(),
        };
        let stream = cluster
            .create_stream(
                StreamCreate {
                    authority: Permit::for_action(Principal::System, bucket, Action::Write),
                    bucket,
                    key: "node-owned".into(),
                    stream_type: "object".into(),
                    metadata: json!({}),
                    public: false,
                    claim_object: true,
                    completion: None,
                    owner: None,
                },
                60,
            )
            .await?;
        stores.quota.reserve(stream, 32, false, None).await?;
        let RepairAllocation::Candidate(mut candidate) = stores
            .chunks
            .prepare_repair(input(storage, stream), Some(node.clone()))
            .await?
        else {
            panic!("expected node candidate")
        };
        metadata(&stores, &mut candidate).await?;
        cluster.release("repair-node").await?;
        let replacement = cluster
            .register(
                "repair-node",
                "data",
                Uuid::new_v4().to_string(),
                &secrets.credential_key,
            )
            .await?
            .unwrap();
        assert_eq!(state.node_id, replacement.node_id);
        assert!(
            stores.chunks.finish_repair(&candidate, 0).await.is_err(),
            "rejoined node must fence the old credential"
        );
        assert!(
            stores
                .chunks
                .prepare_repair(input(storage, stream), Some(node))
                .await
                .is_err(),
            "old process cannot start another recovery"
        );
        candidate.node.as_mut().unwrap().state = replacement;
        stores.chunks.repair_metadata(&candidate).await?;
        sqlx::query("UPDATE nodes SET status='draining' WHERE id=$1")
            .bind(state.node_id)
            .execute(&mut db)
            .await?;
        assert!(
            stores.chunks.finish_repair(&candidate, 0).await.is_err(),
            "draining node cannot commit"
        );
        sqlx::query("UPDATE nodes SET status='registered' WHERE id=$1")
            .bind(state.node_id)
            .execute(&mut db)
            .await?;
        sqlx::query(
            "UPDATE stream_leases SET expires_at=now()-interval '1 second' WHERE stream_id=$1",
        )
        .bind(stream)
        .execute(&mut db)
        .await?;
        assert!(
            stores.chunks.finish_repair(&candidate, 0).await.is_err(),
            "expired write lease cannot commit"
        );
    }
    owner.check().await?;
    Ok(())
}
