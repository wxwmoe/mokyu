use super::*;
use crate::{
    content::codec::Key,
    model::storage_cluster::{Member, Topology},
};

#[tokio::test]
#[ignore = "requires MOKYU_TEST_PAYLOAD_CONFIG and an isolated database"]
async fn storage_files_rebuild_recovery_catalog_without_the_original_database() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let backend = super::super::queries::storage_payload::test_backend().await?;
    crate::node::storage::recovery_tests::contract(&backend).await
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_PAYLOAD_CONFIG and an isolated database"]
async fn recovery_catalog_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let backend = super::super::queries::storage_payload::test_backend().await?;
    let cluster = Uuid::now_v7();
    let media = Uuid::now_v7();
    let anchor = Manifest {
        format_version: 1,
        record_id: Uuid::now_v7(),
        cluster_id: cluster,
        deployment_id: Uuid::now_v7(),
        publication_key: public_key(&Key::new("aes-256-gcm", [9; 32])?, cluster)?,
        takeover_epoch: 1,
        revision: 1,
        previous: None,
        members: vec![Member {
            media_id: media,
            node_id: Uuid::now_v7(),
            state: "available".into(),
            topology: Topology {
                url: "https://storage.example".into(),
                region: None,
                storage_group: "host".into(),
                purpose: "primary".into(),
            },
        }],
        groups: vec![],
    };
    let job = Uuid::now_v7();
    let namespace = Uuid::now_v7();
    backend.catalog_open(job, namespace, &anchor).await?;
    backend.catalog_open(job, namespace, &anchor).await?;
    assert!(
        backend
            .catalog_open(job, Uuid::now_v7(), &anchor)
            .await
            .is_err()
    );
    let mut cursor = Cursor {
        media,
        boot: Uuid::now_v7(),
        fence: Uuid::now_v7(),
        manifest: anchor.digest()?,
        scan: Uuid::now_v7(),
        scope: "recovery".into(),
        page: 0,
        prefix: 0x123,
        complete: false,
    };
    let entries: Vec<_> = (1..=150)
        .map(|sequence| {
            CatalogEntry::Physical(Physical::State {
                sequence,
                operation: Uuid::now_v7(),
                published: true,
            })
        })
        .collect();
    assert_eq!(
        backend
            .catalog_checkpoint(job, 0, &cursor, &entries)
            .await?,
        1
    );
    assert_eq!(
        backend
            .catalog_checkpoint(job, 0, &cursor, &entries)
            .await?,
        1
    );
    assert!(
        backend
            .catalog_checkpoint(job, 0, &cursor, &[])
            .await
            .is_err()
    );
    let page = backend.catalog_page(job, 5, None, None).await?;
    assert_eq!(page.entries.len(), 128);
    let second = backend.catalog_page(job, 5, None, page.after).await?;
    assert_eq!(second.entries.len(), 22);
    assert!(
        backend
            .catalog_page(job, 5, None, second.after)
            .await?
            .entries
            .is_empty()
    );
    let CatalogEntry::Physical(Physical::State {
        sequence,
        operation,
        ..
    }) = &entries[0]
    else {
        unreachable!()
    };
    let selected = backend.catalog_page(job, 5, Some(*operation), None).await?;
    assert_eq!(selected.entries.len(), 1);
    let failed = vec![
        CatalogEntry::Physical(Physical::State {
            sequence: 151,
            operation: Uuid::now_v7(),
            published: true,
        }),
        CatalogEntry::Physical(Physical::State {
            sequence: *sequence,
            operation: *operation,
            published: false,
        }),
    ];
    cursor.page = 1;
    assert!(
        backend
            .catalog_checkpoint(job, 1, &cursor, &failed)
            .await
            .is_err()
    );
    assert_eq!(backend.catalog_cursors(job, None).await?[0].0, 1);
    let page = backend.catalog_page(job, 5, None, None).await?;
    assert_eq!(
        backend
            .catalog_page(job, 5, None, page.after)
            .await?
            .entries
            .len(),
        22
    );
    let mut skipped = cursor.clone();
    skipped.scan = Uuid::now_v7();
    skipped.page = 0;
    skipped.prefix += 1;
    assert!(
        backend
            .catalog_checkpoint(job, 1, &skipped, &[])
            .await
            .is_err()
    );
    cursor.scan = Uuid::now_v7();
    cursor.boot = Uuid::now_v7();
    cursor.fence = Uuid::now_v7();
    cursor.page = 0;
    assert_eq!(
        backend
            .catalog_checkpoint(job, 1, &cursor, &entries)
            .await?,
        2
    );
    let mut foreign = cursor.clone();
    foreign.manifest = "0".repeat(64);
    foreign.page = 1;
    assert!(
        backend
            .catalog_checkpoint(job, 2, &foreign, &[])
            .await
            .is_err()
    );
    cursor.page = 1;
    cursor.complete = true;
    assert_eq!(backend.catalog_checkpoint(job, 2, &cursor, &[]).await?, 3);
    let cursors = backend.catalog_cursors(job, None).await?;
    assert_eq!(cursors.len(), 1);
    assert_eq!(cursors[0].1, cursor);
    assert!(
        backend
            .catalog_cursors(job, Some((media, "recovery".into())))
            .await?
            .is_empty()
    );
    assert!(backend.catalog_page(job, 7, None, None).await.is_err());

    let batch_job = Uuid::now_v7();
    backend.catalog_open(batch_job, namespace, &anchor).await?;
    let mut batch: Vec<_> = (1..=257)
        .map(|sequence| {
            CatalogEntry::Physical(Physical::State {
                sequence,
                operation: Uuid::now_v7(),
                published: true,
            })
        })
        .collect();
    assert!(backend.catalog_stage(batch_job, &batch).await.is_err());
    batch.pop();
    let (first, second) = tokio::join!(
        backend.catalog_stage(batch_job, &batch),
        backend.catalog_stage(batch_job, &batch),
    );
    first?;
    second?;
    let CatalogEntry::Physical(value) = &batch[0] else {
        unreachable!()
    };
    backend
        .catalog_stage(
            batch_job,
            &[
                CatalogEntry::Physical(value.clone()),
                CatalogEntry::Physical(value.clone()),
            ],
        )
        .await?;
    let first = backend.catalog_page(batch_job, 5, None, None).await?;
    assert_eq!(first.entries.len(), 128);
    let second = backend
        .catalog_page(batch_job, 5, None, first.after)
        .await?;
    assert_eq!(second.entries.len(), 128);
    assert!(
        backend
            .catalog_page(batch_job, 5, None, second.after)
            .await?
            .entries
            .is_empty()
    );

    let operation = Uuid::now_v7();
    let state = |published| {
        CatalogEntry::Physical(Physical::State {
            sequence: 257,
            operation,
            published,
        })
    };
    assert!(
        backend
            .catalog_stage(batch_job, &[state(true), state(false)])
            .await
            .is_err()
    );
    assert!(
        backend
            .catalog_entry(batch_job, 5, operation, 257)
            .await?
            .is_none()
    );
    let published = [state(true)];
    let deleted = [state(false)];
    let (first, second) = tokio::join!(
        backend.catalog_stage(batch_job, &published),
        backend.catalog_stage(batch_job, &deleted),
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let stored = backend
        .catalog_entry(batch_job, 5, operation, 257)
        .await?
        .unwrap();
    assert_eq!(
        serde_json::to_value(stored)?,
        serde_json::to_value(state(first.is_ok()))?
    );

    use crate::model::journal::record::{Kind, Reference, Span};
    let material = Reference {
        kind: Kind::Chunk,
        id: Uuid::now_v7(),
        member: None,
    };
    let large: Vec<_> = (0..2)
        .map(|position| CatalogEntry::Business {
            payload: Uuid::now_v7(),
            position,
            record: crate::storage::journal::batch::Record::Ranges {
                bucket: Uuid::now_v7(),
                generation: 1,
                seq: 1,
                offset: 0,
                ranges: (0..40_000)
                    .map(|offset| Span {
                        material,
                        offset,
                        source_offset: 0,
                        length: 1,
                    })
                    .collect(),
            },
        })
        .collect();
    let sizes = encoded(&large)?;
    assert!(sizes.iter().map(|entry| entry.1).sum::<usize>() > ENTRY_LIMIT);
    backend.catalog_stage(batch_job, &large).await?;
    for (entry, (hash, size)) in large.iter().zip(sizes) {
        let (category, _, key, position) = entry.identity();
        let saved = backend
            .catalog_entry(batch_job, category, key, position)
            .await?
            .unwrap();
        let saved = serde_json::to_vec(&saved)?;
        assert_eq!(saved.len(), size);
        assert_eq!(blake3::hash(&saved).as_bytes(), &hash);
    }
    Ok(())
}
