use super::*;
use crate::model::Id;
use crate::{
    authorization::{Action, Permit},
    model::{ChunkCreate, Stores, StreamCreate, TaskClaim},
};

async fn stream(stores: &Stores, bucket: Uuid, key: &str) -> Result<Id> {
    stores
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
        .await
}

async fn task(stores: &Stores) -> Result<Execution> {
    let id = stores
        .task_control
        .enqueue(
            "integrity",
            None,
            json!({"operation":"quarantine","source_operation":{}}),
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
    Ok(Execution { id, token })
}

fn allocation(storage: Uuid, stream: Id) -> ChunkCreate {
    ChunkCreate {
        storage,
        stream,
        hash: [7; 32],
        raw_size: 32,
        algorithm: "none".into(),
        key: None,
    }
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_CONFIG pointing to a fresh isolated single-mode database"]
async fn source_quarantine_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_CONFIG")?;
    let (config, mut secrets, budget) = crate::config::Config::load(std::path::Path::new(&path))?;
    let (stores, mut owner) =
        crate::model::connect(&config, &mut secrets, &budget, "single").await?;
    stores.runtime.initialize(false).await?;
    let mut creation = stores.storage_records.creation().await?;
    let storage = Uuid::new_v4();
    creation
        .writer
        .prepare(
            &crate::storage::model::Record {
                id: storage,
                name: "quarantine".into(),
                r#type: "local".into(),
                path: Some("/data/quarantine".into()),
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
                name: "quarantine".into(),
                project_id: None,
                storage_id: Some(storage),
            },
        )
        .await?
        .id;
    let first = stream(&stores, bucket, "first").await?;
    stores.quota.reserve(first, 32, false, None).await?;
    let (mut chunk, reused) = stores.chunks.allocate(allocation(storage, first)).await?;
    assert!(!reused);
    chunk.stored_size = Some(32);
    stores.chunks.mark_uploading(&chunk).await?;
    stores.physical.register_encoding(&chunk).await?;
    stores.chunks.complete(first, 0, &chunk).await?;
    stores.streams.finish(first, 32, "etag", json!({})).await?;
    assert!(stores.publish.publish(first, None, None, None).await?);
    let maintenance = task(&stores).await?;
    let rows = [crate::model::Mapping {
        chunk: chunk.clone(),
        offset_bytes: 0,
        pack_id: None,
    }];
    stores.packing.pin_pack_inputs(maintenance, &rows).await?;
    let mut candidate = chunk.clone();
    candidate.location_id = Uuid::new_v4();
    candidate.encoding_id = stores
        .packing
        .prepare_encoding(&candidate, maintenance)
        .await?;
    stores.packing.encoding_metadata(&candidate).await?;
    let next = stream(&stores, bucket, "next").await?;
    stores.quota.reserve(next, 32, false, None).await?;
    assert!(stores.chunks.allocate(allocation(storage, next)).await?.1);
    stores.chunks.reference(next, 0, chunk.id, 32).await?;
    stores.streams.finish(next, 32, "etag", json!({})).await?;
    let scope = SourceScope {
        storage_id: storage,
        source: SourceType::Chunk,
        id: chunk.encoding_id.to_string(),
    };
    let snapshot = stores.physical.source_snapshot(&scope).await?;
    assert_eq!(snapshot.revision, 0);
    assert!(snapshot.quarantine_code.is_none());
    let mut operation = SourceOperation {
        principal: Principal::Local,
        mode: "single".into(),
        snapshot: snapshot.clone(),
        code: Some("hash_mismatch".into()),
        node: None,
    };
    let execution = task(&stores).await?;
    assert!(
        stores
            .physical
            .source_update(SourceUpdate {
                execution: Execution {
                    id: execution.id,
                    token: Uuid::new_v4()
                },
                operation: &operation,
                apply: true
            })
            .await
            .is_err()
    );
    stores
        .physical
        .source_update(SourceUpdate {
            execution,
            operation: &operation,
            apply: false,
        })
        .await?;
    assert_eq!(stores.physical.source_snapshot(&scope).await?.revision, 0);
    operation.code = Some("missing_key".into());
    assert!(
        stores
            .physical
            .source_update(SourceUpdate {
                execution,
                operation: &operation,
                apply: true
            })
            .await
            .is_err()
    );
    operation.code = Some("hash_mismatch".into());
    stores
        .physical
        .source_update(SourceUpdate {
            execution,
            operation: &operation,
            apply: true,
        })
        .await?;
    assert!(
        stores
            .physical
            .source_update(SourceUpdate {
                execution,
                operation: &operation,
                apply: true
            })
            .await
            .is_err()
    );
    assert_eq!(stores.physical.source_snapshot(&scope).await?.revision, 1);
    let audit = stores
        .audit
        .events(
            true,
            None,
            None,
            crate::model::audit::Filter {
                action: Some("source.quarantine".into()),
                ..Default::default()
            },
            10,
            true,
        )
        .await?;
    assert_eq!(
        audit.len(),
        1,
        "background isolation must commit its own audit event"
    );
    assert_eq!(audit[0].detail["revision"], "1");
    assert_eq!(audit[0].outcome, "succeeded");
    assert!(stores.physical.source(chunk.id, false).await.is_err());
    assert!(stores.physical.source(chunk.id, true).await.is_ok());
    assert!(
        stores
            .chunks
            .allocate(allocation(storage, next))
            .await
            .is_err()
    );
    let rejected = stream(&stores, bucket, "rejected").await?;
    assert!(
        stores
            .chunks
            .reference(rejected, 0, chunk.id, 32)
            .await
            .is_err()
    );
    assert!(
        stores
            .chunks
            .copy_references(rejected, first, 0, 32, &[chunk.id])
            .await
            .is_err()
    );
    let error = stores
        .publish
        .publish(next, None, None, None)
        .await
        .unwrap_err();
    assert!(matches!(
        crate::model::error(&error),
        Some(crate::model::Error::SourceUnavailable)
    ));
    assert!(stores.streams.current(bucket, "next").await.is_err());
    assert_eq!(stores.streams.current(bucket, "first").await?.id, first);
    assert_eq!(
        stores
            .physical
            .quarantines(storage, false, 0.into(), 10)
            .await?
            .len(),
        1
    );
    assert!(
        stores
            .physical
            .quarantines(storage, false, chunk.encoding_id, 10)
            .await?
            .is_empty()
    );
    assert!(
        stores
            .physical
            .quarantines(Uuid::new_v4(), false, 0.into(), 10)
            .await?
            .is_empty()
    );

    let bad = stores.physical.source_snapshot(&scope).await?;
    let recheck = SourceOperation {
        snapshot: bad.clone(),
        code: None,
        ..operation.clone()
    };
    let execution = task(&stores).await?;
    let mut denied = recheck.clone();
    denied.principal = Principal::Service {
        access_key: "missing".into(),
        revision: 0,
    };
    assert!(
        stores
            .physical
            .source_update(SourceUpdate {
                execution,
                operation: &denied,
                apply: true
            })
            .await
            .is_err()
    );
    stores
        .physical
        .source_update(SourceUpdate {
            execution,
            operation: &recheck,
            apply: false,
        })
        .await?;
    stores
        .physical
        .source_update(SourceUpdate {
            execution,
            operation: &recheck,
            apply: true,
        })
        .await?;
    assert_eq!(stores.physical.source_snapshot(&scope).await?.revision, 2);
    assert_eq!(
        stores
            .audit
            .events(
                true,
                None,
                None,
                crate::model::audit::Filter {
                    action: Some("source.recheck".into()),
                    ..Default::default()
                },
                10,
                true
            )
            .await?
            .len(),
        1,
        "only a successful full recheck is audited as recovered"
    );
    let outputs = [crate::model::packing::PackOutput {
        chunks: vec![chunk.id],
        pack: None,
        independent: Some(candidate),
    }];
    let error = stores
        .packing
        .commit_pack_outputs(maintenance, &rows, &outputs, true, &Default::default(), 60.)
        .await
        .unwrap_err();
    assert!(
        matches!(
            crate::model::error(&error),
            Some(crate::model::Error::OperationAborted)
        ),
        "a maintenance input must retain its pre-quarantine revision: {error:#}"
    );
    stores.task_control.pause(maintenance).await?;
    assert!(stores.physical.source(chunk.id, false).await.is_ok());
    assert!(stores.publish.publish(next, None, None, None).await?);
    operation.snapshot = stores.physical.source_snapshot(&scope).await?;
    let execution = task(&stores).await?;
    stores
        .physical
        .source_update(SourceUpdate {
            execution,
            operation: &operation,
            apply: true,
        })
        .await?;
    let execution = task(&stores).await?;
    assert!(
        stores
            .physical
            .source_update(SourceUpdate {
                execution,
                operation: &recheck,
                apply: true
            })
            .await
            .is_err(),
        "stale pre-clear revision must not clear a later quarantine"
    );
    assert_eq!(stores.physical.source_snapshot(&scope).await?.revision, 3);
    stores.task_control.pause(execution).await?;
    owner.check().await?;
    Ok(())
}
