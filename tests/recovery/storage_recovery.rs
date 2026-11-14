use super::*;
use crate::{
    content::codec::Key,
    model::storage_cluster::{Member, Topology},
};

#[tokio::test]
#[ignore = "requires MOKYU_TEST_PAYLOAD_CONFIG and an isolated database"]
async fn physical_restore_contract() -> Result<()> {
    use crate::{
        content::fragment::Area,
        model::{storage_native::NativeObjects, storage_payload::Intent},
        storage::marker::{self, EpochCut, Journal},
    };
    let (backend, cluster) = super::super::storage_payload::tests::fixture().await?;
    let namespace = Uuid::now_v7();
    let catalog = Uuid::now_v7();
    let journal = Journal::create(namespace, &Key::new("aes-256-gcm", [29; 32])?)?;
    let mut identity = marker::create(cluster.manifest.deployment_id, namespace)?;
    identity["metadata_journal"] = serde_json::to_value(&journal)?;
    let intent = Intent {
        operation: Uuid::now_v7(),
        namespace,
        payload: Uuid::now_v7(),
        revision: 1,
        cluster: cluster.id,
        layout_revision: cluster.revision as u64,
        takeover_epoch: cluster.manifest.takeover_epoch,
        group: cluster.manifest.groups[0].id,
        layout: cluster.policy.layout.clone(),
        members: cluster.manifest.groups[0].members.clone(),
        area: Area::Blocks,
        total: 8,
        acknowledgement: crate::model::storage_cluster::Acknowledgement::All,
    };
    let mut marker_intent = intent.clone();
    marker_intent.operation = Uuid::now_v7();
    marker_intent.payload = Uuid::now_v7();
    let mut marker_root = Root {
        intent: marker_intent.clone(),
        receipts: Vec::new(),
    };
    for copy in 0..2 {
        let mut header = marker_intent.header(
            0,
            copy,
            0,
            marker_intent.members[copy as usize],
            Uuid::now_v7(),
        )?;
        header.digest = [7; 32];
        let root = [8; 32];
        marker_root
            .receipts
            .push(crate::model::storage_payload::Receipt {
                commitment: crate::content::fragment::commitment(&header, &root)?,
                header,
                root,
            });
    }
    marker_root.validate(namespace, cluster.id)?;
    let mut invalid = marker_root.clone();
    invalid.receipts[1] = invalid.receipts[0].clone();
    assert!(invalid.validate(namespace, cluster.id).is_err());
    invalid = marker_root.clone();
    invalid.receipts.pop();
    assert!(invalid.validate(namespace, cluster.id).is_err());
    let head = Proof {
        publication: Publication {
            format_version: 1,
            id: Uuid::now_v7(),
            namespace,
            cluster: cluster.id,
            manifest: cluster.manifest.digest()?,
            takeover_epoch: cluster.manifest.takeover_epoch,
            revision: 1,
            previous: None,
            through: 3,
            parts: vec![],
            retired: vec![],
            identity: Some(identity),
            root: Some(marker_root),
            prefix: String::new(),
        },
        signature: vec![0; 64],
    };
    backend
        .catalog_open(catalog, namespace, &cluster.manifest)
        .await?;
    backend
        .catalog_checkpoint(
            catalog,
            0,
            &Cursor {
                media: cluster.manifest.members[0].media_id,
                boot: Uuid::now_v7(),
                fence: Uuid::now_v7(),
                manifest: cluster.manifest.digest()?,
                scan: Uuid::now_v7(),
                scope: "history".into(),
                page: 0,
                prefix: 0,
                complete: true,
            },
            &[CatalogEntry::Proof(head.clone())],
        )
        .await?;
    backend.catalog_bind(catalog, &head).await?;
    backend.catalog_index(catalog, &head).await?;
    let old_epoch = Uuid::now_v7();
    let epochs = [
        EpochCut {
            epoch: old_epoch,
            sequence: 9,
            tail: [1; 32],
            salvaged: None,
        },
        EpochCut {
            epoch: journal.writer_epoch,
            sequence: 3,
            tail: [2; 32],
            salvaged: None,
        },
    ];
    backend
        .catalog_map(catalog, marker_intent.operation, &epochs, false)
        .await?;
    assert_eq!(
        backend
            .catalog_image(catalog, namespace, "mokyu.json")
            .await?,
        Some(marker_intent.clone())
    );
    assert!(
        backend
            .read_object(
                namespace,
                "mokyu.json",
                None,
                chrono::Utc::now().timestamp_micros() + 60_000_000
            )
            .await
            .is_err()
    );
    assert!(
        !backend
            .catalog_shards(catalog, marker_intent.operation, 0)
            .await?
            .is_empty()
    );
    backend
        .catalog_restore(catalog, marker_intent.operation, &epochs)
        .await?;
    backend
        .catalog_restore(catalog, marker_intent.operation, &epochs)
        .await?;
    let restored_root = backend
        .read_object(
            namespace,
            "mokyu.json",
            None,
            chrono::Utc::now().timestamp_micros() + 60_000_000,
        )
        .await?;
    assert_eq!(restored_root.payload.intent, marker_intent);
    assert!(
        backend
            .find_write(namespace, "mokyu.json", catalog)
            .await?
            .is_none(),
        "restored roots must not alias a future takeover write"
    );
    let path = "objects/restored";
    backend
        .catalog_stage(
            catalog,
            &[
                CatalogEntry::Physical(
                    old_epoch,
                    Physical::Payload {
                        sequence: 1,
                        intent: intent.clone(),
                        path: path.into(),
                    },
                ),
                CatalogEntry::Physical(
                    old_epoch,
                    Physical::State {
                        sequence: 9,
                        operation: intent.operation,
                        published: true,
                    },
                ),
            ],
        )
        .await?;
    assert!(
        backend
            .catalog_restore(catalog, intent.operation, &epochs)
            .await
            .is_err(),
        "missing source receipts must not publish"
    );
    let mut header = intent.header(0, 1, 0, intent.members[1], Uuid::now_v7())?;
    header.digest = [4; 32];
    let root = [5; 32];
    let receipt = crate::model::storage_payload::Receipt {
        commitment: crate::content::fragment::commitment(&header, &root)?,
        header,
        root,
    };
    let mut repaired = receipt.clone();
    repaired.header.fragment = Uuid::now_v7();
    repaired.header.generation = 2;
    repaired.commitment = crate::content::fragment::commitment(&repaired.header, &root)?;
    let mut intermediate = receipt.clone();
    intermediate.header.fragment = Uuid::now_v7();
    intermediate.header.generation = 1;
    intermediate.commitment = crate::content::fragment::commitment(&intermediate.header, &root)?;
    backend
        .catalog_stage(
            catalog,
            &[
                CatalogEntry::Physical(
                    old_epoch,
                    Physical::Fragment {
                        sequence: 4,
                        receipt: repaired.clone(),
                    },
                ),
                CatalogEntry::Physical(
                    old_epoch,
                    Physical::Fragment {
                        sequence: 3,
                        receipt: intermediate,
                    },
                ),
                CatalogEntry::Physical(
                    old_epoch,
                    Physical::Fragment {
                        sequence: 2,
                        receipt,
                    },
                ),
            ],
        )
        .await?;
    backend
        .catalog_restore(catalog, intent.operation, &epochs)
        .await?;
    backend
        .catalog_restore(catalog, intent.operation, &epochs)
        .await?;
    backend
        .catalog_map(catalog, intent.operation, &epochs, false)
        .await?;
    assert_eq!(
        backend.catalog_shards(catalog, intent.operation, 0).await?,
        vec![repaired.clone()]
    );
    let restored = backend
        .read_object(
            namespace,
            path,
            None,
            chrono::Utc::now().timestamp_micros() + 60_000_000,
        )
        .await?;
    assert_eq!(restored.payload.intent, intent);
    {
        use super::super::queries::storage_cluster as q;
        let mut tx = q::begin(&backend).await?;
        for (online, expected) in [
            (Vec::new(), (2, 0, 0, 16, Some(0))),
            (vec![intent.members[0]], (2, 1, 0, 16, Some(0))),
            (intent.members.clone(), (2, 2, 1, 8, Some(1))),
        ] {
            assert_eq!(
                q::protection(cluster.id, online)
                    .fetch_one(&mut *tx)
                    .await?,
                expected
            );
        }
        tx.rollback().await?;
    }
    let fragments = crate::model::storage_payload::StoragePayloads::read(
        &backend,
        intent.operation,
        0,
        1,
        chrono::Utc::now().timestamp_micros() + 60_000_000,
    )
    .await?;
    assert_eq!(fragments.len(), 1);
    assert_eq!(fragments[0].receipt.as_ref(), Some(&repaired));
    let mut replacement = intent.clone();
    replacement.operation = Uuid::now_v7();
    replacement.revision += 1;
    backend
        .catalog_stage(
            catalog,
            &[
                CatalogEntry::Physical(
                    old_epoch,
                    Physical::Payload {
                        sequence: 5,
                        intent: replacement.clone(),
                        path: path.into(),
                    },
                ),
                CatalogEntry::Physical(
                    journal.writer_epoch,
                    Physical::State {
                        sequence: 1,
                        operation: replacement.operation,
                        published: false,
                    },
                ),
                CatalogEntry::Physical(
                    old_epoch,
                    Physical::State {
                        sequence: 9,
                        operation: replacement.operation,
                        published: true,
                    },
                ),
            ],
        )
        .await?;
    backend
        .catalog_restore(catalog, replacement.operation, &epochs)
        .await?;
    backend
        .catalog_restore(catalog, intent.operation, &epochs)
        .await?;
    assert!(
        backend
            .read_object(
                namespace,
                path,
                None,
                chrono::Utc::now().timestamp_micros() + 60_000_000
            )
            .await
            .is_err(),
        "older payload or writer sequence must not resurrect a deletion"
    );
    assert_eq!(
        crate::model::storage_payload::StoragePayloads::get(&backend, intent.operation)
            .await?
            .state,
        "retired"
    );
    Ok(())
}

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
            CatalogEntry::Physical(
                Uuid::from_u128(1),
                Physical::State {
                    sequence,
                    operation: Uuid::now_v7(),
                    published: true,
                },
            )
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
    let CatalogEntry::Physical(
        _,
        Physical::State {
            sequence,
            operation,
            ..
        },
    ) = &entries[0]
    else {
        unreachable!()
    };
    let selected = backend.catalog_page(job, 5, Some(*operation), None).await?;
    assert_eq!(selected.entries.len(), 1);
    let failed = vec![
        CatalogEntry::Physical(
            Uuid::from_u128(1),
            Physical::State {
                sequence: 151,
                operation: Uuid::now_v7(),
                published: true,
            },
        ),
        CatalogEntry::Physical(
            Uuid::from_u128(1),
            Physical::State {
                sequence: *sequence,
                operation: *operation,
                published: false,
            },
        ),
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
            CatalogEntry::Physical(
                Uuid::from_u128(1),
                Physical::State {
                    sequence,
                    operation: Uuid::now_v7(),
                    published: true,
                },
            )
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
    let CatalogEntry::Physical(epoch, value) = &batch[0] else {
        unreachable!()
    };
    backend
        .catalog_stage(
            batch_job,
            &[
                CatalogEntry::Physical(*epoch, value.clone()),
                CatalogEntry::Physical(*epoch, value.clone()),
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
        CatalogEntry::Physical(
            Uuid::from_u128(1),
            Physical::State {
                sequence: 257,
                operation,
                published,
            },
        )
    };
    assert!(
        backend
            .catalog_stage(batch_job, &[state(true), state(false)])
            .await
            .is_err()
    );
    assert!(
        backend
            .catalog_entry(batch_job, 5, state(true).identity().2, 257)
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
        .catalog_entry(batch_job, 5, state(true).identity().2, 257)
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
    let old_epoch = Uuid::now_v7();
    let new_epoch = Uuid::now_v7();
    let operation = Uuid::now_v7();
    let entries = [
        CatalogEntry::Physical(
            old_epoch,
            Physical::State {
                sequence: 1,
                operation,
                published: true,
            },
        ),
        CatalogEntry::Physical(
            new_epoch,
            Physical::State {
                sequence: 1,
                operation,
                published: false,
            },
        ),
    ];
    backend.catalog_stage(batch_job, &entries).await?;
    backend.catalog_stage(batch_job, &entries).await?;
    let rows = backend
        .catalog_page(batch_job, 5, Some(operation), None)
        .await?;
    assert_eq!(
        rows.entries.len(),
        2,
        "sequence numbers from different writer epochs must not collide"
    );
    Ok(())
}
