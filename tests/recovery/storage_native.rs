use super::*;
use crate::{
    content::fragment::{self, Area, Header},
    model::storage_payload::{Intent, Receipt, StoragePayloads},
    model::storage_recovery::RecoveryCatalog,
};

#[tokio::test]
#[ignore = "requires an isolated database and MOKYU_TEST_STORAGE_TLS"]
async fn native_takeover_requires_complete_history_and_preserves_layout() -> Result<()> {
    use crate::{
        authorization::Principal,
        model::{
            cluster::NodeSessions,
            session::Open,
            storage_cluster::{Group, Manifest, Member, StorageClusters, Takeover},
        },
        node::storage::TestNode,
    };
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let config = std::env::var("MOKYU_TEST_PAYLOAD_CONFIG")?;
    let (_, secrets, _) = crate::config::Config::load(std::path::Path::new(&config))?;
    let backend = physical::test_backend().await?;
    let key = &secrets.credential_key;
    let principal = Principal::Local;
    let state = backend
        .provision_node(
            &principal,
            &format!("takeover-{}", Uuid::now_v7()),
            "storage",
            key,
        )
        .await?;
    let run = backend
        .open_session(
            &state,
            key,
            Open {
                boot: Uuid::now_v7(),
                connection: Uuid::now_v7(),
                previous_epoch: 0,
            },
        )
        .await?
        .run;
    let node = TestNode::new(state, run).await?;
    let report = node.announcement()?;
    let media = report.media.media_id;
    let second_state = backend
        .provision_node(
            &principal,
            &format!("late-{}", Uuid::now_v7()),
            "storage",
            key,
        )
        .await?;
    let second_run = backend
        .open_session(
            &second_state,
            key,
            Open {
                boot: Uuid::now_v7(),
                connection: Uuid::now_v7(),
                previous_epoch: 0,
            },
        )
        .await?
        .run;
    let second = TestNode::new(second_state, second_run).await?;
    let second_report = second.announcement()?;
    let second_media = second_report.media.media_id;
    let old_key = crate::content::codec::Key::new("aes-256-gcm", [74; 32])?;
    let id = Uuid::now_v7();
    let initial = Manifest {
        format_version: 1,
        record_id: Uuid::now_v7(),
        cluster_id: id,
        deployment_id: Uuid::now_v7(),
        publication_key: crate::model::storage_recovery::public_key(&old_key, id)?,
        takeover_epoch: 1,
        revision: 1,
        previous: None,
        members: vec![],
        groups: vec![],
    };
    let mut members = Manifest {
        revision: 2,
        record_id: Uuid::now_v7(),
        previous: Some(initial.digest()?),
        ..initial.clone()
    };
    members.members.push(Member {
        media_id: media,
        node_id: Uuid::now_v7(),
        state: "available".into(),
        topology: report.topology.clone(),
    });
    members.members.push(Member {
        media_id: second_media,
        node_id: Uuid::now_v7(),
        state: "available".into(),
        topology: crate::model::storage_cluster::Topology {
            url: "https://old-location.invalid:9443".into(),
            ..second_report.topology
        },
    });
    let mut head = Manifest {
        revision: 3,
        record_id: Uuid::now_v7(),
        previous: Some(members.digest()?),
        ..members.clone()
    };
    head.groups.push(Group {
        replacing: None,
        id: Uuid::now_v7(),
        layout: Default::default(),
        members: head.members.iter().map(|m| m.media_id).collect(),
    });
    second
        .historical_layout(
            head.clone(),
            vec![initial.clone(), members.clone(), head.clone()],
        )
        .await?;
    node.historical_layout(head.clone(), vec![initial.clone(), members, head.clone()])
        .await?;
    node.apply(
        backend
            .announce(&node.state, key, run, node.announcement()?)
            .await?,
    )
    .await?;
    let request = || Takeover {
        name: format!("recovered-{id}"),
        media_ids: vec![media],
    };
    node.layout_file(&initial, true).await?;
    assert!(
        crate::storage::native::recovery::takeover(&backend, &principal, key, request())
            .await
            .is_err()
    );
    assert!(
        backend.placement_snapshot(id).await.is_err(),
        "partial history must not create a cluster"
    );
    node.layout_file(&initial, false).await?;
    let adopted =
        crate::storage::native::recovery::takeover(&backend, &principal, key, request()).await?;
    assert_eq!(adopted.manifest.groups, head.groups);
    assert_eq!(adopted.manifest.members[0].media_id, media);
    assert_eq!(adopted.manifest.members[0].node_id, node.state.node_id);
    assert_eq!(adopted.manifest.members[1], head.members[1]);
    assert_eq!(adopted.manifest.takeover_epoch, 2);
    assert_eq!(adopted.manifest.deployment_id, node.state.deployment_id);
    adopted.manifest.follows(&head)?;
    assert_eq!(
        crate::storage::native::recovery::takeover(&backend, &principal, key, request())
            .await?
            .manifest,
        adopted.manifest,
        "retries must not create another takeover revision"
    );
    node.apply(
        backend
            .announce(&node.state, key, run, node.announcement()?)
            .await?,
    )
    .await?;
    assert_eq!(
        node.announcement()?.manifest,
        Some(adopted.manifest.clone())
    );
    node.apply(
        backend
            .announce(&node.state, key, run, node.announcement()?)
            .await?,
    )
    .await?;
    second
        .apply(
            backend
                .announce(&second.state, key, second_run, second.announcement()?)
                .await?,
        )
        .await?;
    let both = || Takeover {
        name: format!("recovered-{id}"),
        media_ids: vec![media, second_media],
    };
    let extended =
        crate::storage::native::recovery::takeover(&backend, &principal, key, both()).await?;
    assert_eq!(
        extended.manifest.takeover_epoch,
        adopted.manifest.takeover_epoch
    );
    assert_eq!(extended.revision, adopted.revision + 1);
    assert_eq!(extended.manifest.groups, head.groups);
    let late = &extended.manifest.members[1];
    assert_eq!(
        (late.media_id, late.node_id),
        (second_media, second.state.node_id)
    );
    assert_eq!(late.topology.url, second.url);
    assert_eq!(
        crate::storage::native::recovery::takeover(&backend, &principal, key, both())
            .await?
            .manifest,
        extended.manifest
    );
    Ok(())
}

async fn received(
    backend: &Backend,
    intent: &Intent,
    copies: std::ops::Range<u16>,
    value: u8,
) -> Result<()> {
    for copy in copies {
        let data = vec![value; intent.total as usize];
        let mut header = Header {
            area: intent.area,
            namespace: intent.namespace,
            payload: intent.payload,
            fragment: Uuid::now_v7(),
            media: intent.members[copy as usize],
            cluster: intent.cluster,
            group: intent.group,
            operation: intent.operation,
            layout_revision: intent.layout_revision,
            revision: intent.revision,
            generation: 0,
            takeover_epoch: intent.takeover_epoch,
            offset: 0,
            length: intent.total,
            total: intent.total,
            shard_bytes: intent.total,
            sequence: 0,
            layout: intent.layout.clone(),
            copy,
            shard: 0,
            digest: [0; 32],
        };
        backend.stage(intent.operation, &[header.clone()]).await?;
        header.digest = *blake3::hash(&data).as_bytes();
        let leaves: Vec<_> = data
            .chunks(fragment::UNIT)
            .enumerate()
            .map(|(i, bytes)| fragment::leaf(i, bytes))
            .collect();
        let root = fragment::tree(&header, &leaves)?[0];
        let receipt = Receipt {
            commitment: fragment::commitment(&header, &root)?,
            header,
            root,
        };
        backend.receipts(intent.operation, &[receipt]).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated database and MOKYU_TEST_STORAGE_TLS"]
async fn native_journal_tracks_only_dependencies_and_fences_changes() -> Result<()> {
    use crate::{
        model::{
            journal::{JournalStore, native::NativeJournal, publication, record::Hold},
            storage_recovery::Physical,
        },
        storage::{
            journal::Format,
            marker::Journal,
            native::{
                control::{Control, WriteRequest},
                transport::Pool,
                writer,
            },
        },
    };
    use futures_util::StreamExt;
    use std::{sync::Arc, time::Duration};
    let (backend, cluster, _nodes, secrets) = live_fixture().await?;
    let (namespace, _) = native_backend(&backend, &cluster, true).await?;
    let control = Arc::new(Control::new(
        backend.clone(),
        backend.clone(),
        secrets.clone(),
    ));
    let pool = Pool::new(Duration::from_secs(30));
    let owner = Uuid::now_v7();
    let path = crate::storage::location::Layout::S3
        .object(owner)
        .to_string();
    let object = control
        .reserve(WriteRequest {
            namespace,
            cluster: cluster.id,
            path: &path,
            owner,
            condition: Condition::Create,
            area: Area::Blocks,
            total: 8192,
            region: None,
        })
        .await?;
    writer::write(
        control.as_ref(),
        &pool,
        &object.payload.intent,
        futures_util::stream::iter([Ok(bytes::Bytes::from(vec![61; 8192]))]).boxed(),
        None,
    )
    .await?;
    control
        .publish(namespace, object.payload.intent.payload, owner)
        .await?;
    let token = Uuid::now_v7();
    let mut ledger = backend
        .claim(namespace, token)
        .await?
        .context("journal lease missing")?;
    let descriptor = Journal::create(namespace, &secrets.credential_key)?;
    backend.descriptor(namespace, token, &descriptor).await?;
    ledger.descriptor = Some(sqlx::types::Json(descriptor.clone()));
    let key = descriptor.unlock(namespace, &secrets.credential_key)?;
    let format = Format {
        storage: namespace,
        journal: descriptor.journal_id,
        epoch: descriptor.writer_epoch,
        key: &key,
        encrypted: true,
    };
    assert!(
        backend.native_capture(namespace, token).await?.is_none(),
        "unreferenced carriers must not produce recovery records"
    );
    let mut tx = physical::begin(backend.as_ref()).await?;
    let hold = Hold {
        unit: "raw",
        storage: namespace,
        address: owner,
        key: None,
    };
    super::super::storage_journal::track(&mut tx, &hold).await?;
    super::super::storage_journal::track(&mut tx, &hold).await?;
    tx.commit().await?;
    let capture = backend
        .native_capture(namespace, token)
        .await?
        .context("missing physical capture")?;
    assert_eq!(capture.revision, 1);
    assert_eq!(capture.records.len(), 4);
    assert!(capture.complete);
    assert!(
        matches!(&capture.records[0], Physical::Payload { path: actual, intent, .. }
        if actual == &path && intent == &object.payload.intent)
    );
    let plan =
        publication::allocate_native(&ledger, vec![], capture.records.clone(), vec![], &format)?;
    backend
        .native_allocate(namespace, token, &capture, &plan)
        .await?;
    assert!(backend.native_capture(namespace, token).await?.is_none());
    assert!(
        backend
            .native_allocate(namespace, token, &capture, &plan)
            .await
            .is_err()
    );
    let mut tx = physical::begin(backend.as_ref()).await?;
    super::super::queries::storage_journal::dirty(capture.operation)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    let mut tx = physical::begin(backend.as_ref()).await?;
    ledger = super::super::queries::journal::ledger(namespace)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    let stale = backend
        .native_capture(namespace, token)
        .await?
        .context("missing changed capture")?;
    let stale_plan =
        publication::allocate_native(&ledger, vec![], stale.records.clone(), vec![], &format)?;
    backend
        .remove_object(namespace, &path, Some(capture.operation))
        .await?;
    assert!(
        backend
            .native_allocate(namespace, token, &stale, &stale_plan)
            .await
            .is_err()
    );
    let mut tx = physical::begin(backend.as_ref()).await?;
    let unchanged = super::super::queries::journal::ledger(namespace)
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(
        unchanged.allocated_seq, ledger.allocated_seq,
        "stale capture must roll back its journal allocation"
    );
    tx.commit().await?;
    JournalStore::release(backend.as_ref(), namespace, token, None).await?;
    drop(control);
    drop(backend);
    let reopened = physical::test_backend().await?;
    let token = Uuid::now_v7();
    let ledger = reopened
        .claim(namespace, token)
        .await?
        .context("restart lost journal lease")?;
    let latest = reopened
        .native_capture(namespace, token)
        .await?
        .context("restart lost changed source")?;
    assert!(latest.revision > stale.revision && latest.after.is_nil());
    assert!(matches!(
        latest.records.last(),
        Some(Physical::State {
            published: false,
            ..
        })
    ));
    let plan =
        publication::allocate_native(&ledger, vec![], latest.records.clone(), vec![], &format)?;
    reopened
        .native_allocate(namespace, token, &latest, &plan)
        .await?;
    let mut tx = physical::begin(&reopened).await?;
    super::super::queries::storage_journal::test_expire(capture.operation)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    assert!(
        reopened.reclaim(capture.operation, None).await.is_err(),
        "retirement must preserve the old recovery location"
    );
    assert!(
        !reopened
            .collect_candidates(namespace, 64)
            .await?
            .contains(&capture.operation)
    );
    JournalStore::release(&reopened, namespace, token, None).await?;
    Ok(())
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires an isolated database, MOKYU_TEST_STORAGE_TLS and MOKYU_TEST_FAULT_DIR"]
async fn native_journal_publishes_evidence_before_watermarks() -> Result<()> {
    use crate::{
        model::{
            StorageProfileStore,
            journal::{JournalStore, native::NativeJournal, publication},
            storage_recovery::Physical,
        },
        storage::{
            journal::remote::Remote,
            model::{Options, Store},
            native::control::Control,
        },
    };
    use object_store::{ObjectStoreExt, PutMode, PutOptions, path::Path};
    use std::{sync::Arc, time::Duration};
    let (backend, cluster, nodes, secrets) = live_fixture().await?;
    let record = native_record(&cluster, true)?;
    let namespace = record.id;
    let mut tx = physical::begin(backend.as_ref()).await?;
    super::super::queries::insert_storage_record(&record, None)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    let control = Arc::new(Control::new(
        backend.clone(),
        backend.clone(),
        secrets.clone(),
    ));
    let mut tx = physical::begin(backend.as_ref()).await?;
    q::test_raw_profile(namespace).execute(&mut *tx).await?;
    tx.commit().await?;
    let profile = StorageProfileStore::load(backend.as_ref(), namespace)
        .await
        .context("load native journal profile")?;
    let store = Store::build_control(
        &record,
        Options::from_profile(&profile)?,
        &secrets,
        control,
        None,
    )?;
    store
        .backend
        .put_opts(
            &Path::from("mokyu.json"),
            bytes::Bytes::from(serde_json::to_vec(&record.identity_marker)?).into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await?;
    let token = Uuid::now_v7();
    let ledger = backend
        .claim(namespace, token)
        .await?
        .context("journal lease missing")?;
    let remote = Arc::new(
        Remote::connect(
            backend.as_ref(),
            backend.as_ref(),
            backend.as_ref(),
            &secrets,
            &ledger,
            token,
            store.native.clone(),
        )
        .await
        .context("connect native journal")?,
    );
    let mut tx = physical::begin(backend.as_ref()).await?;
    let ledger = super::super::queries::journal::ledger(namespace)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    let plan = publication::allocate_native(
        &ledger,
        vec![],
        vec![Physical::State {
            sequence: 1,
            operation: Uuid::now_v7(),
            published: false,
        }],
        vec![],
        &remote.format(),
    )?;
    backend.allocate(namespace, token, &plan).await?;
    let file = backend
        .next_file(namespace)
        .await?
        .context("native journal allocation missing")?;
    let bytes = bytes::Bytes::from(backend.file_frames(file.id).await?.concat());
    assert!(bytes.starts_with(b"MOKYU\x00\x00\x0a"));
    assert!(
        backend
            .acknowledge(namespace, token, &file, None, None)
            .await
            .is_err(),
        "generic acknowledgements must not skip native evidence"
    );
    let barrier = std::path::PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?)
        .join("storage-evidence-published");
    if barrier.with_extension("hit").exists() {
        tokio::fs::remove_file(barrier.with_extension("hit")).await?;
    }
    tokio::fs::write(&barrier, []).await?;
    let pending = {
        let (db, remote, file, bytes, secrets) = (
            backend.clone(),
            remote.clone(),
            file.clone(),
            bytes.clone(),
            secrets.clone(),
        );
        tokio::spawn(async move {
            remote
                .publish_native(db.as_ref(), token, &file, bytes, &secrets.credential_key)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        while !barrier.with_extension("hit").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let history = backend.native_history(namespace, 0, 1).await?;
    assert_eq!(history.len(), 1);
    let proof = &history[0].0;
    assert!(proof.publication.prefix.is_empty());
    let root = proof
        .publication
        .root
        .as_ref()
        .context("native proof has no root mapping")?;
    root.validate(namespace, cluster.id)?;
    assert_eq!(root.receipts.len(), 2);
    assert_eq!(
        store.backend.head(&Path::from("mokyu.json")).await?.version,
        Some(root.intent.operation.to_string())
    );

    assert!(
        proof
            .publication
            .identity
            .as_ref()
            .and_then(|m| crate::storage::marker::journal(m).ok())
            .flatten()
            == Some(remote.journal.clone())
    );
    assert!(
        backend
            .native_acknowledge(namespace, token, &file, proof)
            .await
            .is_err()
    );
    assert!(store.backend.head(&Path::from(file.path()?)).await.is_err());
    let mut tx = physical::begin(backend.as_ref()).await?;
    assert_eq!(
        super::super::queries::journal::ledger(namespace)
            .fetch_one(&mut *tx)
            .await?
            .emitted_seq,
        0
    );
    tx.commit().await?;
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    tokio::fs::remove_file(&barrier).await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        for attempt in 0..100 {
            match remote
                .publish_native(
                    backend.as_ref(),
                    token,
                    &file,
                    bytes.clone(),
                    &secrets.credential_key,
                )
                .await
            {
                Ok(true) => return Ok::<_, anyhow::Error>(()),
                Err(error) if attempt == 99 => return Err(error),
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                _ => anyhow::bail!("native journal made no progress"),
            }
        }
        unreachable!()
    })
    .await??;
    let restored = store
        .backend
        .get(&Path::from(file.path()?))
        .await?
        .bytes()
        .await?;
    assert_eq!(restored, bytes);
    for node in &nodes {
        assert_eq!(
            backend
                .native_evidence(namespace, node.announcement()?.media.media_id)
                .await?,
            1
        );
    }
    assert!(
        remote
            .publish_native(
                backend.as_ref(),
                token,
                &file,
                bytes,
                &secrets.credential_key
            )
            .await?
    );
    assert_eq!(backend.native_history(namespace, 0, 2).await?.len(), 1);
    assert!(backend.next_file(namespace).await?.is_none());
    use crate::model::storage_cluster::{StorageClusters, Update};
    let updated = StorageClusters::update(
        backend.as_ref(),
        &crate::authorization::Principal::Local,
        cluster.id,
        Update {
            revision: cluster.revision,
            name: format!("{}-next", cluster.name),
            policy: cluster.policy.clone(),
        },
    )
    .await?;
    assert!(updated.manifest.revision > history[0].1.revision);
    for node in &nodes {
        for _ in 0..2 {
            node.apply(
                backend
                    .announce(
                        &node.state,
                        &secrets.credential_key,
                        node.run,
                        node.announcement()?,
                    )
                    .await?,
            )
            .await?;
        }
    }
    let fresh = Arc::new(Control::new(
        backend.clone(),
        backend.clone(),
        secrets.clone(),
    ));
    let target = fresh
        .publication_target(
            proof,
            &history[0].1,
            nodes[0].announcement()?.media.media_id,
        )
        .await?;
    store.native.as_ref().unwrap().pool.publish(&target).await?;
    let scanner = crate::storage::native::recovery::scan::Scanner::open(
        backend.clone(),
        fresh.clone(),
        cluster.id,
        None,
    )
    .await?;
    let media = nodes[0].announcement()?.media.media_id;
    let isolated = crate::storage::native::recovery::scan::Scanner::open(
        backend.clone(),
        fresh.clone(),
        cluster.id,
        Some("missing-backup-group".into()),
    )
    .await?;
    assert!(isolated.namespaces(media).await.is_err());
    let mut discovered = scanner.namespaces(media).await?;
    use futures_util::TryStreamExt;
    let entry = discovered
        .try_next()
        .await?
        .context("recovery namespace missing")?;
    assert_eq!(entry.proof.as_ref(), Some(proof));
    assert!(discovered.try_next().await?.is_none());
    drop(discovered);
    let catalog = Uuid::now_v7();
    assert_eq!(scanner.collect(catalog, namespace, &[media]).await?, *proof);
    assert_eq!(scanner.collect(catalog, namespace, &[media]).await?, *proof);
    let part = &proof.publication.parts[0];
    let mut decoded = crate::model::storage_recovery::verify::Decoders::default();
    scanner
        .decode(
            catalog,
            part,
            proof.publication.through,
            Some(&remote.format()),
            &mut decoded,
            &nodes[0].disk(),
        )
        .await?;
    assert!(decoded.complete());
    assert_eq!(
        backend
            .catalog_page(catalog, 5, None, None)
            .await?
            .entries
            .len(),
        1
    );
    let wrong_key = crate::content::codec::Key::new("aes-256-gcm", [91; 32])?;
    let wrong_format = crate::storage::journal::Format {
        key: &wrong_key,
        ..remote.format()
    };
    assert!(
        scanner
            .decode(
                catalog,
                part,
                proof.publication.through,
                Some(&wrong_format),
                &mut Default::default(),
                &nodes[0].disk()
            )
            .await
            .is_err(),
        "cached decoded rows must still require the correct key"
    );
    let mut connection =
        crate::storage::model::Connection::unprotect(&record, &secrets.credential_key)?;
    connection.identity_marker = proof.publication.identity.clone().unwrap();
    let source = crate::storage::journal::source::Source {
        connection,
        journal: remote.journal.clone(),
        material: remote.format().key.material,
    };
    let (recovered, cut) = scanner
        .replay(catalog, &source, proof, &nodes[0].disk(), None)
        .await?;
    assert!(recovered.buckets.is_empty());
    assert_eq!(cut.point.sequence, proof.publication.through);
    let mirrored = Uuid::now_v7();
    let both = nodes
        .iter()
        .map(|node| Ok(node.announcement()?.media.media_id))
        .collect::<Result<Vec<_>>>()?;
    scanner.collect(mirrored, namespace, &both).await?;
    let candidates = crate::model::storage_recovery::verify::fragments(
        backend.as_ref(),
        mirrored,
        part,
        &history[0].1,
    )
    .await?;
    let damaged = &candidates
        .iter()
        .find(|r| r.header.media == media)
        .context("test recovery fragment missing")?
        .header;
    nodes[0].corrupt(damaged, 0).await?;
    scanner
        .decode(
            mirrored,
            part,
            proof.publication.through,
            Some(&remote.format()),
            &mut Default::default(),
            &nodes[0].disk(),
        )
        .await?;
    assert_eq!(
        backend
            .catalog_page(mirrored, 5, None, None)
            .await?
            .entries
            .len(),
        1
    );
    nodes[0].fragment_byte(damaged, 0, b'M').await?;
    let mut candidate = proof.publication.clone();
    candidate.id = Uuid::now_v7();
    candidate.revision += 1;
    candidate.previous = Some(proof.link()?);
    candidate.manifest = updated.manifest.digest()?;
    let candidate = crate::model::storage_recovery::Proof::sign(
        candidate,
        &secrets.credential_key,
        &updated.manifest,
    )?;
    nodes[0].evidence_file(&candidate, false).await?;
    assert_eq!(
        scanner.collect(Uuid::now_v7(), namespace, &[media]).await?,
        *proof,
        "a file outside the committed head must not advance recovery"
    );
    let missing = crate::model::storage_recovery::Link {
        id: Uuid::now_v7(),
        digest: [7; 32],
    };
    nodes[0].recovery_head(namespace, &missing).await?;
    assert!(
        scanner
            .collect(Uuid::now_v7(), namespace, &[media])
            .await
            .is_err()
    );
    nodes[0].recovery_head(namespace, &proof.link()?).await?;
    nodes[0].evidence_file(proof, true).await?;
    let incomplete = Uuid::now_v7();
    assert!(
        scanner
            .collect(incomplete, namespace, &[media])
            .await
            .is_err()
    );
    nodes[0].evidence_file(proof, false).await?;
    nodes[0].evidence_file(&candidate, true).await?;
    assert_eq!(
        scanner.collect(incomplete, namespace, &[media]).await?,
        *proof
    );
    JournalStore::release(backend.as_ref(), namespace, token, None).await?;
    let mut native = store.native.as_ref().unwrap().as_ref().clone();
    native.authority = crate::storage::native::store::Authority::Control(fresh);
    native_snapshots(&backend, &native, &remote, &record, &secrets).await?;
    let checkpoint_catalog = Uuid::now_v7();
    let checkpoint = scanner
        .collect(checkpoint_catalog, namespace, &both)
        .await?;
    let mut source = source;
    source.connection.identity_marker = checkpoint.publication.identity.clone().unwrap();
    let (expected, expected_cut) = source.read(&native, None).await?;
    let (restored, restored_cut) = scanner
        .replay(
            checkpoint_catalog,
            &source,
            &checkpoint,
            &nodes[0].disk(),
            None,
        )
        .await?;
    assert_eq!(restored_cut, expected_cut);
    assert_eq!(restored.buckets.len(), expected.buckets.len());
    assert_eq!(restored.materials.len(), expected.materials.len());
    assert!(
        scanner
            .recover(
                checkpoint_catalog,
                &source,
                &checkpoint,
                &nodes[0].disk(),
                None
            )
            .await
            .err()
            .expect("recovery must not replace the deliberately deleted online snapshot entry")
            .to_string()
            .contains("physical recovery revision conflicts")
    );
    assert!(
        !backend
            .catalog_page(checkpoint_catalog, 3, None, None)
            .await?
            .entries
            .is_empty()
    );
    let old_root = checkpoint
        .publication
        .root
        .as_ref()
        .unwrap()
        .intent
        .operation;
    for node in &nodes {
        node.apply(
            backend
                .announce(
                    &node.state,
                    &secrets.credential_key,
                    node.run,
                    node.announcement()?,
                )
                .await?,
        )
        .await?;
    }
    let document = crate::storage::marker::Document::read(&native)
        .await?
        .context("missing native root")?;
    document.set_journal(&native, &source.journal).await?;
    let mut tx = physical::begin(backend.as_ref()).await?;
    super::super::queries::storage_journal::test_expire(old_root)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    assert!(
        backend.reclaim(old_root, None).await.is_err(),
        "the last published recovery root must survive marker replacement"
    );
    assert!(
        !backend
            .collect_candidates(namespace, 64)
            .await?
            .contains(&old_root)
    );
    Ok(())
}

#[cfg(feature = "fault-injection")]
async fn native_snapshots(
    backend: &std::sync::Arc<Backend>,
    native: &crate::storage::native::store::Store,
    remote: &crate::storage::journal::remote::Remote,
    record: &crate::storage::model::Record,
    secrets: &crate::config::Secrets,
) -> Result<()> {
    use crate::{
        model::journal::{
            native::{NativeJournal, SnapshotFile},
            snapshots::{Action, SnapshotStore},
        },
        storage::{
            external::objects::Condition,
            journal::{FrameType, remote::publish_snapshot, snapshot, source::Source},
        },
    };
    use object_store::{ObjectStore, ObjectStoreExt, path::Path};
    use std::{sync::Arc, time::Duration};
    let namespace = record.id;
    let bucket = Uuid::now_v7();
    let raw = Uuid::now_v7();
    let path = crate::storage::location::Layout::S3.object(raw);
    let mut options = object_store::PutOptions {
        mode: object_store::PutMode::Create,
        ..Default::default()
    };
    options
        .extensions
        .insert(crate::storage::native::store::Publication {
            owner: raw,
            area: Area::Blocks,
        });
    use futures_util::StreamExt;
    let intent = native
        .write(
            &path,
            4096,
            futures_util::stream::iter([Ok(bytes::Bytes::from(vec![7; 4096]))]).boxed(),
            options,
        )
        .await?;
    backend
        .publish_object(namespace, intent.payload, raw)
        .await?;
    let hold = crate::model::journal::record::Hold {
        unit: "raw",
        storage: namespace,
        address: raw,
        key: None,
    };
    let mut tx = physical::begin(backend.as_ref()).await?;
    super::super::storage_journal::track(&mut tx, &hold).await?;
    tx.commit().await?;
    let mut last_index = None;
    for round in 0..2 {
        backend
            .snapshot_request(
                &crate::authorization::Principal::Local,
                namespace,
                Action::New,
            )
            .await?;
        let token = Uuid::now_v7();
        assert!(backend.snapshot_claim(namespace, token).await?);
        let job = Arc::new(
            backend
                .snapshot_begin(namespace, token)
                .await?
                .context("snapshot cut not ready")?,
        );
        let format = crate::storage::journal::Format {
            encrypted: job.encrypt_frames,
            ..remote.format()
        };
        let capture = backend
            .native_snapshot_material(&job, token, &hold, None)
            .await?;
        assert!(capture.complete && capture.records.len() == 4);
        let rows: Vec<_> = (1..=256)
            .map(|seq| snapshot::Row::Delete {
                bucket,
                key: Uuid::new_v4().to_string(),
                generation: 1,
                seq,
            })
            .collect();
        let parts = snapshot::encode_pages(
            &format,
            job.id,
            0,
            &rows,
            4096,
            Some(bucket),
            Some(bucket),
            Some(&capture.records),
        )?;
        assert!(parts.len() > 1);
        backend
            .snapshot_page(&job, token, 0, 0, bucket, &parts, &[])
            .await?;
        let manifest = snapshot::Manifest {
            format_version: 1,
            id: job.id,
            parent: job.parent,
            sequence: job.sequence_no.try_into()?,
            tail: job.digest.as_deref().unwrap().try_into()?,
            pages: parts.iter().map(|(p, _)| p.clone()).collect(),
        };
        let manifest_path = SnapshotFile::Manifest.path(&job)?;
        let manifest_bytes = snapshot::native_envelope(
            &format,
            native
                .next_recovery_identity(&Path::from(manifest_path.clone()))
                .await?,
            job.id,
            FrameType::Manifest,
            &manifest_path,
            job.revision.try_into()?,
            &manifest,
        )?;
        let index = snapshot::Index {
            format_version: 1,
            snapshot: job.id,
            operation: job.id,
            revision: job.revision.try_into()?,
            sequence: job.sequence_no.try_into()?,
            manifest_digest: *blake3::hash(&manifest_bytes).as_bytes(),
            buckets: vec![],
            bucket_count: 0,
            excluded: vec![],
        };
        let index_identity = native
            .next_recovery_identity(&Path::from(snapshot::index_path()))
            .await?;
        if let Some(previous) = last_index {
            assert_eq!(index_identity.payload, previous);
            assert_eq!(index_identity.revision, 2);
        }
        last_index = Some(index_identity.payload);
        let index_bytes = snapshot::native_envelope(
            &format,
            index_identity,
            job.id,
            FrameType::Index,
            &snapshot::index_path(),
            job.revision.try_into()?,
            &index,
        )?;
        let checkpoint_path = SnapshotFile::Checkpoint.path(&job)?;
        let checkpoint_bytes = snapshot::native_envelope(
            &format,
            native
                .next_recovery_identity(&Path::from(checkpoint_path.clone()))
                .await?,
            job.id,
            FrameType::Index,
            &checkpoint_path,
            job.revision.try_into()?,
            &index,
        )?;
        backend
            .snapshot_seal(
                &job,
                token,
                &manifest_bytes,
                &index_bytes,
                Some(&checkpoint_bytes),
            )
            .await?;
        let saved = backend.snapshot_blobs(job.id).await?;
        assert_eq!(
            saved,
            (
                manifest_bytes.clone(),
                index_bytes.clone(),
                checkpoint_bytes.clone()
            )
        );
        let fake = format!("\"{}\"", Uuid::now_v7());
        assert!(
            backend
                .snapshot_receipt(&job, token, Some(0), &fake, None)
                .await
                .is_err()
        );
        for (page, bytes) in parts {
            let file = SnapshotFile::Page(page.number);
            let data = bytes::Bytes::from(bytes);
            let receipt = publish_snapshot(
                native,
                backend.as_ref(),
                &secrets.credential_key,
                &job,
                token,
                file,
                data.clone(),
                Condition::Absent,
            )
            .await?;
            let retry = publish_snapshot(
                native,
                backend.as_ref(),
                &secrets.credential_key,
                &job,
                token,
                file,
                data,
                Condition::Absent,
            )
            .await?;
            assert_eq!(receipt.etag, retry.etag);
        }
        publish_snapshot(
            native,
            backend.as_ref(),
            &secrets.credential_key,
            &job,
            token,
            SnapshotFile::Manifest,
            manifest_bytes.into(),
            Condition::Absent,
        )
        .await?;
        let condition = if let Some(parent) = job.parent {
            Condition::Match(backend.snapshot_parent_etag(parent).await?)
        } else {
            Condition::Absent
        };
        let index_bytes = bytes::Bytes::from(index_bytes);
        if round == 0 {
            let barrier = std::path::PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?)
                .join("storage-evidence-published");
            let hit = barrier.with_extension("hit");
            if hit.exists() {
                tokio::fs::remove_file(&hit).await?;
            }
            tokio::fs::write(&barrier, b"hold").await?;
            let pending = {
                let backend = backend.clone();
                let native = native.clone();
                let credential = secrets.credential_key.clone();
                let job = job.clone();
                let data = index_bytes.clone();
                tokio::spawn(async move {
                    publish_snapshot(
                        &native,
                        backend.as_ref(),
                        &credential,
                        &job,
                        token,
                        SnapshotFile::Index,
                        data,
                        Condition::Absent,
                    )
                    .await
                })
            };
            tokio::time::timeout(Duration::from_secs(10), async {
                while !hit.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            assert!(
                backend
                    .snapshot_commit(&job, token, &fake, &fake, None, 0)
                    .await
                    .is_err()
            );
            assert!(backend.snapshot_status(namespace).await?.current.is_none());
            assert!(
                native
                    .head(&Path::from(snapshot::index_path()))
                    .await
                    .is_err()
            );
            pending.abort();
            assert!(pending.await.unwrap_err().is_cancelled());
            tokio::fs::remove_file(&barrier).await?;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let index_receipt = publish_snapshot(
            native,
            backend.as_ref(),
            &secrets.credential_key,
            &job,
            token,
            SnapshotFile::Index,
            index_bytes,
            condition,
        )
        .await?;
        assert!(
            backend
                .snapshot_commit(&job, token, &index_receipt.etag, &fake, None, 0)
                .await
                .is_err()
        );
        let checkpoint_receipt = publish_snapshot(
            native,
            backend.as_ref(),
            &secrets.credential_key,
            &job,
            token,
            SnapshotFile::Checkpoint,
            checkpoint_bytes.into(),
            Condition::Absent,
        )
        .await?;
        backend
            .snapshot_commit(
                &job,
                token,
                &index_receipt.etag,
                &checkpoint_receipt.etag,
                checkpoint_receipt.version.as_deref(),
                0,
            )
            .await?;
        assert_eq!(
            backend.snapshot_status(namespace).await?.current,
            Some(job.id)
        );
        backend.snapshot_release(namespace, token).await?;
        let source = Source {
            connection: crate::storage::model::Connection::unprotect(
                record,
                &secrets.credential_key,
            )?,
            journal: remote.journal.clone(),
            material: remote.key.material,
        };
        let (_, cut) = source.read(native, None).await?;
        assert_eq!(
            cut.checkpoint_revision,
            Some((remote.journal.writer_epoch, job.revision as u64))
        );
        if round == 1 {
            native_snapshot_gc(backend, native, namespace, secrets).await?;
            backend
                .remove_object(namespace, &snapshot::index_path(), None)
                .await?;
            let (_, recovered) = source.read(native, None).await?;
            assert_eq!(recovered.checkpoint_revision, cut.checkpoint_revision);
        }
    }
    Ok(())
}

#[cfg(feature = "fault-injection")]
async fn native_snapshot_gc(
    backend: &std::sync::Arc<Backend>,
    native: &crate::storage::native::store::Store,
    namespace: Uuid,
    secrets: &crate::config::Secrets,
) -> Result<()> {
    use crate::model::journal::{native::NativeJournal, snapshots::SnapshotStore};
    use object_store::{ObjectStoreExt, path::Path};
    use std::{path::PathBuf, time::Duration};
    let mut token = Uuid::now_v7();
    assert!(backend.snapshot_claim(namespace, token).await?);
    let batch = backend.snapshot_gc(namespace, token, 0).await?;
    assert!(!batch.is_empty());
    for (index, file) in batch.into_iter().enumerate() {
        assert!(backend.snapshot_reclaimed(token, &file).await.is_err());
        let proof = backend
            .native_gc_prepare(token, &file, &secrets.credential_key)
            .await?;
        assert_eq!(proof.publication.retired.len(), 1);
        let operation = proof.publication.retired[0];
        assert!(backend.native_gc_acknowledge(token, &proof).await.is_err());
        assert!(
            native
                .head(&Path::from(file.file_path.clone()))
                .await
                .is_ok()
        );
        let retry = backend
            .native_gc_prepare(token, &file, &secrets.credential_key)
            .await?;
        assert_eq!(retry, proof);
        if index == 0 {
            backend.snapshot_release(namespace, token).await?;
            token = Uuid::now_v7();
            assert!(backend.snapshot_claim(namespace, token).await?);
            assert_eq!(
                backend.native_gc_pending(namespace, token).await?,
                Some(proof.clone())
            );
        }
        native
            .publish_evidence(backend.as_ref(), token, &proof)
            .await?;
        if index == 0 {
            let pin = Uuid::now_v7();
            let mut tx = physical::begin(backend.as_ref()).await?;
            super::super::queries::journal_restore::create(
                pin,
                namespace,
                b"pinned",
                &Default::default(),
            )
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            assert!(backend.native_gc_acknowledge(token, &proof).await.is_err());
            let mut tx = physical::begin(backend.as_ref()).await?;
            super::super::queries::journal_restore::cancel(pin, 1)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }
        backend.native_gc_acknowledge(token, &proof).await?;
        backend.native_gc_acknowledge(token, &proof).await?;
        assert!(backend.snapshot_reclaimed(token, &file).await.is_err());
        assert!(
            native
                .head(&Path::from(file.file_path.clone()))
                .await
                .is_err()
        );
        assert!(!native.reclaim_recovery(operation).await?);
        let mut tx = physical::begin(backend.as_ref()).await?;
        super::super::queries::storage_journal::test_expire(operation)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        if index == 0 {
            let pin = Uuid::now_v7();
            let mut tx = physical::begin(backend.as_ref()).await?;
            super::super::queries::journal_restore::create(
                pin,
                namespace,
                b"pinned",
                &Default::default(),
            )
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            assert!(!native.reclaim_recovery(operation).await?);
            let mut tx = physical::begin(backend.as_ref()).await?;
            super::super::queries::journal_restore::cancel(pin, 1)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            let barrier = PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?)
                .join("storage-fragment-removed");
            let hit = barrier.with_extension("hit");
            if hit.exists() {
                tokio::fs::remove_file(&hit).await?;
            }
            tokio::fs::write(&barrier, b"hold").await?;
            let pending = {
                let native = native.clone();
                tokio::spawn(async move { native.reclaim_recovery(operation).await })
            };
            tokio::time::timeout(Duration::from_secs(10), async {
                while !hit.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            pending.abort();
            assert!(pending.await.unwrap_err().is_cancelled());
            tokio::fs::remove_file(&barrier).await?;
        }
        assert!(native.reclaim_recovery(operation).await?);
        assert!(native.reclaim_recovery(operation).await?);
        backend.snapshot_reclaimed(token, &file).await?;
    }
    assert!(backend.native_gc_pending(namespace, token).await?.is_none());
    backend.snapshot_release(namespace, token).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated database and MOKYU_TEST_STORAGE_TLS"]
async fn dashboard_reads_while_topology_is_locked() -> Result<()> {
    use crate::{authorization::Principal, model::storage_cluster::StorageClusters};
    let (backend, cluster, _nodes, _secrets) = live_fixture().await?;
    let lock = super::super::queries::storage_cluster::begin(&backend).await?;
    let read = async {
        let (health, space, overview) = tokio::try_join!(
            backend.health(&Principal::Local, cluster.id),
            backend.space(&Principal::Local, cluster.id),
            backend.overview()
        )?;
        ensure!(
            health.online_primary == 2
                && space.groups == 1
                && overview.clusters.iter().any(|c| c.id == cluster.id),
            "dashboard read inconsistent topology"
        );
        Ok::<_, anyhow::Error>(())
    };
    tokio::time::timeout(std::time::Duration::from_secs(4), read).await??;
    lock.rollback().await?;
    Ok(())
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires an isolated database, MOKYU_TEST_STORAGE_TLS and MOKYU_TEST_FAULT_DIR"]
async fn native_parallel_ranges_bound_prefetch_and_release_on_cancel() -> Result<()> {
    use crate::storage::native::{
        control::{Control, WriteRequest},
        reader,
        transport::Pool,
        writer,
    };
    use futures_util::{StreamExt, TryStreamExt};
    use std::{path::PathBuf, sync::Arc, time::Duration};
    let (backend, cluster, nodes, secrets) = live_fixture().await?;
    let (namespace, _) = native_backend(&backend, &cluster, true).await?;
    let control = Arc::new(Control::new(backend.clone(), backend, secrets));
    let pool = Arc::new(Pool::new(Duration::from_secs(30)));
    let owner = Uuid::now_v7();
    let path = format!("objects/{}", owner.simple());
    let body = bytes::Bytes::from(vec![47; reader::WINDOW as usize]);
    let object = control
        .reserve(WriteRequest {
            namespace,
            cluster: cluster.id,
            path: &path,
            owner,
            condition: Condition::Create,
            area: Area::Blocks,
            total: body.len() as u64,
            region: None,
        })
        .await?;
    writer::write(
        control.as_ref(),
        &pool,
        &object.payload.intent,
        futures_util::stream::iter([Ok(body.clone())]).boxed(),
        None,
    )
    .await?;
    control
        .publish(namespace, object.payload.intent.payload, owner)
        .await?;
    let plan = control
        .plan_read(
            namespace,
            &path,
            None,
            None,
            Some(0..body.len() as u64),
            None,
            chrono::Utc::now().timestamp_micros() + 60_000_000,
        )
        .await?;
    assert_eq!(plan.parts.len(), 16);
    let fault_dir = PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?);
    let mut barriers = Vec::new();
    for part in plan.parts.iter().take(3) {
        let grant = &part.copies[0].authorization.grant;
        let crate::model::storage_transfer::Action::Read { offset, .. } = grant.action else {
            unreachable!()
        };
        let barrier = fault_dir.join(format!("storage-read-{}-{offset}", grant.media));
        tokio::fs::write(&barrier, []).await?;
        barriers.push(barrier);
    }
    assert_ne!(
        plan.parts[0].copies[0].authorization.grant.media,
        plan.parts[1].copies[0].authorization.grant.media
    );
    let mut read = reader::read(pool.clone(), control.clone(), plan.clone(), 2)?;
    let pending = tokio::spawn(async move { read.try_next().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !barriers[..2]
            .iter()
            .all(|path| path.with_extension("hit").exists())
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(nodes.iter().all(|node| node.available_reads() == 1));
    tokio::fs::remove_file(&barriers[1]).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while nodes
            .iter()
            .filter(|node| node.available_reads() == 2)
            .count()
            != 1
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !barriers[2].with_extension("hit").exists(),
        "prefetch passed the blocked two-part window"
    );
    assert!(
        !pending.is_finished(),
        "out-of-order data was delivered before the first part"
    );
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), async {
        while nodes.iter().any(|node| node.available_reads() != 2) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    for barrier in barriers {
        if barrier.exists() {
            tokio::fs::remove_file(barrier).await?;
        }
    }
    let read = reader::read(pool, control, plan, 2)?
        .try_fold(Vec::new(), |mut data, part| async move {
            data.extend_from_slice(&part);
            Ok(data)
        })
        .await?;
    assert_eq!(read, body);
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated database and MOKYU_TEST_STORAGE_TLS"]
async fn native_early_ack_replicas_resume_with_authorized_receipts() -> Result<()> {
    use crate::{
        authorization::Principal,
        model::{
            cluster::NodeSessions,
            session::Open,
            storage_cluster::{Acknowledgement, StorageClusters, Update},
        },
        storage::native::{
            control::{Control, WriteRequest},
            replication::{Operation, Reply, Request},
            transport::Pool,
            writer,
        },
    };
    use futures_util::{StreamExt, TryStreamExt};
    use std::{sync::Arc, time::Duration};
    let (backend, mut cluster, nodes, secrets) = live_fixture().await?;
    let key = &secrets.credential_key;
    let (namespace, _) = native_backend(&backend, &cluster, true).await?;
    let worker = backend
        .provision_node(
            &Principal::Local,
            &format!("fill-{}", Uuid::now_v7()),
            "worker",
            key,
        )
        .await?;
    let run = backend
        .open_session(
            &worker,
            key,
            Open {
                boot: Uuid::now_v7(),
                connection: Uuid::now_v7(),
                previous_epoch: 0,
            },
        )
        .await?
        .run;
    let pool = Pool::new(Duration::from_secs(30));
    for acknowledgement in [Acknowledgement::Data, Acknowledgement::DataAndParity] {
        let mut policy = cluster.policy.clone();
        policy.acknowledgement = acknowledgement;
        cluster = backend
            .update(
                &Principal::Local,
                cluster.id,
                Update {
                    revision: cluster.revision,
                    name: cluster.name.clone(),
                    policy,
                },
            )
            .await?;
        for node in &nodes {
            for _ in 0..2 {
                node.apply(
                    backend
                        .announce(&node.state, key, node.run, node.announcement()?)
                        .await?,
                )
                .await?;
            }
        }
        let control = Arc::new(Control::new(
            backend.clone(),
            backend.clone(),
            secrets.clone(),
        ));
        let recovery_owner = Uuid::now_v7();
        let recovery_identity = crate::model::storage_native::RecoveryIdentity {
            payload: Uuid::now_v7(),
            revision: 1,
        };
        let recovery_path = format!("metadata/changes/{}", recovery_owner.simple());
        let recovery_request = || WriteRequest {
            namespace,
            cluster: cluster.id,
            path: &recovery_path,
            owner: recovery_owner,
            condition: Condition::Create,
            area: Area::Recovery,
            total: 16,
            region: None,
        };
        let recovery = control
            .reserve_recovery(recovery_request(), recovery_identity)
            .await?;
        assert_eq!(recovery.payload.intent.payload, recovery_identity.payload);
        assert_eq!(
            control
                .reserve_recovery(recovery_request(), recovery_identity)
                .await?
                .payload
                .intent,
            recovery.payload.intent
        );
        assert!(
            control
                .reserve_recovery(
                    recovery_request(),
                    crate::model::storage_native::RecoveryIdentity {
                        payload: Uuid::now_v7(),
                        ..recovery_identity
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(
            recovery.payload.intent.acknowledgement,
            Acknowledgement::All
        );
        let one = control
            .write_window(recovery.payload.intent.operation, 0, 1)
            .await?
            .into_iter()
            .find_map(|piece| piece.destination)
            .context("missing recovery target")?;
        let receipt = pool
            .write(
                one,
                futures_util::stream::iter([Ok(bytes::Bytes::from_static(b"recovery payload"))])
                    .boxed(),
                control.as_ref(),
            )
            .await?;
        control
            .accept(recovery.payload.intent.operation, &[receipt])
            .await?;
        assert!(
            control
                .publish(namespace, recovery.payload.intent.payload, recovery_owner)
                .await
                .is_err()
        );
        let owner = Uuid::now_v7();
        let path = format!("objects/{}", Uuid::now_v7().simple());
        let bytes = bytes::Bytes::from(vec![83; 2 * 1024 * 1024 + 17]);
        let object = control
            .reserve(WriteRequest {
                namespace,
                cluster: cluster.id,
                path: &path,
                owner,
                condition: Condition::Create,
                area: Area::Blocks,
                total: bytes.len() as u64,
                region: None,
            })
            .await?;
        let intent = &object.payload.intent;
        writer::write(
            control.as_ref(),
            &pool,
            intent,
            futures_util::stream::iter([Ok(bytes.clone())]).boxed(),
            None,
        )
        .await?;
        control.publish(namespace, intent.payload, owner).await?;
        let mappings = control.read_window(intent.operation, 0, 1).await?;
        assert_eq!(mappings.iter().filter(|p| p.receipt.is_some()).count(), 1);
        let mut tx = physical::begin(backend.as_ref()).await?;
        super::require_protection(&mut tx, intent.operation).await?;
        tx.commit().await?;
        let mut policy = cluster.policy.clone();
        policy.acknowledgement = Acknowledgement::All;
        cluster = backend
            .update(
                &Principal::Local,
                cluster.id,
                Update {
                    revision: cluster.revision,
                    name: cluster.name.clone(),
                    policy,
                },
            )
            .await?;
        for node in &nodes {
            for _ in 0..2 {
                node.apply(
                    backend
                        .announce(&node.state, key, node.run, node.announcement()?)
                        .await?,
                )
                .await?;
            }
        }
        let mut tx = physical::begin(backend.as_ref()).await?;
        let missing = super::require_protection(&mut tx, intent.operation)
            .await
            .unwrap_err();
        assert_eq!(
            crate::model::error(&missing),
            Some(crate::model::Error::StorageCopiesPending)
        );
        tx.rollback().await?;
        let task = crate::model::TaskControlStore::raw(backend.as_ref(), intent.operation)
            .await?
            .context("missing persistent replica task")?;
        assert_eq!(task["state"], "queued");
        let control = Arc::new(Control::new(
            backend.clone(),
            backend.clone(),
            secrets.clone(),
        ));
        let plan = control
            .next_fill(intent.operation)
            .await?
            .context("missing replica work after control restart")?;
        assert!(
            control
                .node_replicate(
                    &worker,
                    run,
                    Request {
                        work: Uuid::now_v7(),
                        action: Operation::Window
                    }
                )
                .await
                .is_err()
        );
        let mut tx = physical::begin(backend.as_ref()).await?;
        let (work, task) = q::test_work(
            &mut tx,
            &worker,
            run,
            &crate::model::node_work::Plan::Replicate(plan.clone()),
        )
        .await?;
        tx.commit().await?;
        let Reply::Window {
            sources, targets, ..
        } = control
            .node_replicate(
                &worker,
                run,
                Request {
                    work,
                    action: Operation::Window,
                },
            )
            .await?
        else {
            anyhow::bail!("replication window missing");
        };
        assert_eq!((sources.len(), targets.len()), (1, 1));
        let body = pool.read(sources[0].clone(), control.clone()).await?;
        let completed = pool
            .write(targets[0].clone(), body, control.as_ref())
            .await?;
        let mut forged = completed.clone();
        forged.receipt.tag[0] ^= 1;
        assert!(
            control
                .node_replicate(
                    &worker,
                    run,
                    Request {
                        work,
                        action: Operation::Accept {
                            receipts: vec![forged]
                        }
                    }
                )
                .await
                .is_err()
        );
        let mut tx = physical::begin(backend.as_ref()).await?;
        q::test_pause_task(task).execute(&mut *tx).await?;
        tx.commit().await?;
        assert!(
            control
                .node_replicate(
                    &worker,
                    run,
                    Request {
                        work,
                        action: Operation::Accept {
                            receipts: vec![completed.clone()]
                        }
                    }
                )
                .await
                .is_err()
        );
        assert!(control.next_fill(intent.operation).await?.is_some());
        let mut tx = physical::begin(backend.as_ref()).await?;
        let (work, _) = q::test_work(
            &mut tx,
            &worker,
            run,
            &crate::model::node_work::Plan::Replicate(plan.clone()),
        )
        .await?;
        tx.commit().await?;
        for _ in 0..2 {
            assert!(matches!(
                control
                    .node_replicate(
                        &worker,
                        run,
                        Request {
                            work,
                            action: Operation::Accept {
                                receipts: vec![completed.clone()]
                            }
                        }
                    )
                    .await?,
                Reply::Accepted
            ));
        }
        assert!(control.next_fill(intent.operation).await?.is_none());
        let mappings = control.read_window(intent.operation, 0, 1).await?;
        let mut tx = physical::begin(backend.as_ref()).await?;
        super::require_protection(&mut tx, intent.operation).await?;
        tx.commit().await?;
        for mapping in &mappings {
            assert!(mapping.receipt.is_some());
            let body = pool
                .read(
                    control.read(mapping, 0, bytes.len() as u64).await?,
                    control.clone(),
                )
                .await?
                .try_fold(Vec::new(), |mut body, part| async move {
                    body.extend_from_slice(&part);
                    Ok(body)
                })
                .await?;
            assert_eq!(body, bytes);
        }
        control.remove(namespace, &path).await?;
        assert!(control.next_fill(intent.operation).await?.is_none());
        assert!(
            control
                .node_replicate(
                    &worker,
                    run,
                    Request {
                        work,
                        action: Operation::Window
                    }
                )
                .await
                .is_err()
        );
    }
    drop(nodes);
    Ok(())
}

fn native_record(
    cluster: &crate::model::storage_cluster::Cluster,
    enabled: bool,
) -> Result<crate::storage::model::Record> {
    let namespace = Uuid::now_v7();
    Ok(crate::storage::model::Record {
        id: namespace,
        name: format!("native-{namespace}"),
        r#type: "mokyu".into(),
        cluster_id: Some(cluster.id),
        path: None,
        endpoint_url: None,
        remote_bucket_name: None,
        region: None,
        prefix: String::new(),
        access_key_encrypted: None,
        secret_key_encrypted: None,
        identity_marker: crate::storage::marker::create(cluster.manifest.deployment_id, namespace)?,
        enabled,
    })
}

async fn native_backend(
    backend: &Backend,
    cluster: &crate::model::storage_cluster::Cluster,
    enabled: bool,
) -> Result<(Uuid, crate::storage::model::Record)> {
    let record = native_record(cluster, enabled)?;
    let mut tx = physical::begin(backend).await?;
    super::super::queries::insert_storage_record(&record, None)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((record.id, record))
}

#[tokio::test]
#[ignore = "requires an isolated database and MOKYU_TEST_STORAGE_TLS"]
async fn native_repairs_replace_only_verified_generations() -> Result<()> {
    repair_generations(false).await
}

#[tokio::test]
#[ignore = "requires an isolated database and MOKYU_TEST_STORAGE_TLS"]
async fn native_repairs_wait_for_recovery_handoff() -> Result<()> {
    repair_generations(true).await
}

async fn repair_generations(logged: bool) -> Result<()> {
    use crate::{
        authorization::Principal,
        model::{storage_repair::Repairs, storage_transfer::Action},
        storage::native::{
            control::{Control, WriteRequest},
            replication::{Operation, Reply, Worker},
            transport::{Pool, Renewal},
            writer,
        },
    };
    use bytes::Bytes;
    use futures_util::{StreamExt, TryStreamExt};
    use std::{sync::Arc, time::Duration};
    let (backend, cluster, nodes, secrets) = live_fixture().await?;
    let (namespace, record) = native_backend(&backend, &cluster, true).await?;
    let control = Arc::new(Control::new(
        backend.clone(),
        backend.clone(),
        secrets.clone(),
    ));
    let pool = Pool::new(Duration::from_secs(30));
    let bytes = Bytes::from(vec![37; 2 * 1024 * 1024 + 31]);
    let owner = Uuid::now_v7();
    let object = control
        .reserve(WriteRequest {
            namespace,
            cluster: cluster.id,
            path: "objects/repair",
            owner,
            condition: Condition::Create,
            area: Area::Blocks,
            total: bytes.len() as u64,
            region: None,
        })
        .await?;
    let operation = object.payload.intent.operation;
    writer::write(
        control.as_ref(),
        &pool,
        &object.payload.intent,
        futures_util::stream::iter([Ok(bytes.clone())]).boxed(),
        None,
    )
    .await?;
    backend
        .publish_object(namespace, object.payload.intent.payload, owner)
        .await?;
    if logged {
        let mut tx = physical::begin(&backend).await?;
        super::super::queries::storage_journal::track(operation, namespace, "objects/repair")
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
    }
    let original = control.read_window(operation, 0, 1).await?;
    let first = original[0].receipt.as_ref().unwrap();
    assert!(
        backend
            .request_repair(&Principal::Local, Uuid::now_v7(), first.header.fragment)
            .await
            .is_err()
    );
    let id = backend
        .request_repair(&Principal::Local, cluster.id, first.header.fragment)
        .await?;
    let plan = backend
        .next_fill(id)
        .await?
        .context("missing repair plan")?;
    Arc::new(Worker::Local {
        control: control.clone(),
        plan: plan.clone(),
    })
    .execute(&pool, &plan)
    .await?;
    assert_eq!(backend.repair(id).await?.state, "healthy");
    assert!(backend.next_fill(id).await?.is_none());
    assert!(backend.repair(id).await?.candidate.is_none());

    for (index, source) in original.iter().enumerate() {
        let header = &source.header;
        let node = nodes
            .iter()
            .find(|node| node.announcement().unwrap().media.media_id == header.media)
            .unwrap();
        let old_read = control.read(source, 0, header.shard_bytes).await?;
        if index == 0 {
            node.corrupt(header, 1).await?;
        } else {
            node.remove_fragment(header).await?;
        }
        let id = backend
            .request_repair(&Principal::Local, cluster.id, header.fragment)
            .await?;
        assert_eq!(
            id,
            backend
                .request_repair(&Principal::Local, cluster.id, header.fragment)
                .await?
        );
        let plan = backend
            .next_fill(id)
            .await?
            .context("missing repair plan")?;
        if index == 1 {
            // A persisted candidate survives loss of the worker before accepting its receipt.
            let reply = control
                .replicate(&plan, Operation::Checked { damaged: true }, i64::MAX)
                .await?;
            let Reply::Window {
                sources, targets, ..
            } = reply
            else {
                anyhow::bail!("missing repair window")
            };
            let body = pool.read(sources[0].clone(), control.clone()).await?;
            let proof = pool
                .write(targets[0].clone(), body, control.as_ref())
                .await?;
            assert_eq!(backend.repair(id).await?.state, "writing");
            let mut changed = proof.clone();
            changed.receipt.tag[0] ^= 1;
            assert!(
                backend
                    .accept_repair(&secrets.credential_key, id, &changed)
                    .await
                    .is_err()
            );
            assert_eq!(
                backend.repair(id).await?.candidate.as_ref(),
                Some(&proof.receipt.receipt.header)
            );
        }
        let resumed = Arc::new(Control::new(
            backend.clone(),
            backend.clone(),
            secrets.clone(),
        ));
        Arc::new(Worker::Local {
            control: resumed.clone(),
            plan: plan.clone(),
        })
        .execute(&pool, &plan)
        .await?;
        let repair = backend.repair(id).await?;
        assert_eq!(repair.state, "installed");
        assert_eq!(repair.original.receipt(), source.receipt.as_ref());
        assert!(!repair.original_removed);
        assert!(backend.next_fill(id).await?.is_none());
        let mappings = resumed.read_window(operation, 0, 1).await?;
        assert_eq!(mappings.len(), 2);
        let current = mappings
            .iter()
            .find(|item| item.header.media == header.media)
            .unwrap();
        assert_ne!(current.header.fragment, header.fragment);
        assert_eq!(current.header.generation, header.generation + 1);
        assert!(
            current
                .receipt
                .as_ref()
                .unwrap()
                .newer_than(source.receipt.as_ref().unwrap())?
        );
        assert!(
            !source
                .receipt
                .as_ref()
                .unwrap()
                .newer_than(current.receipt.as_ref().unwrap())?
        );
        let mut competing = current.receipt.clone().unwrap();
        competing.header.fragment = Uuid::now_v7();
        competing.commitment = fragment::commitment(&competing.header, &competing.root)?;
        assert!(
            competing
                .newer_than(current.receipt.as_ref().unwrap())
                .is_err()
        );
        let renewed = resumed.renew(&old_read.authorization).await?;
        assert!(
            matches!(&renewed.grant.action, Action::Read { receipt, .. } if receipt.header == *header)
        );
        for mapping in &mappings {
            let destination = resumed.read(mapping, 11, bytes.len() as u64 - 22).await?;
            let actual = pool
                .read(destination, resumed.clone())
                .await?
                .try_fold(Vec::new(), |mut all, data| async move {
                    all.extend_from_slice(&data);
                    Ok(all)
                })
                .await?;
            assert_eq!(actual, bytes[11..bytes.len() - 11]);
        }
        if logged {
            assert!(backend.reclaim(operation, None).await.is_err());
        } else {
            assert!(backend.reclaim(operation, None).await?.is_empty());
        }
    }
    let mut tx = physical::begin(&backend).await?;
    super::super::queries::storage_repair::test_expire(operation)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    if logged {
        use crate::model::{
            StorageProfileStore,
            journal::{JournalStore, native::NativeJournal, publication},
        };
        use crate::storage::{
            journal::remote::Remote,
            model::{Options, Store},
        };
        use object_store::{ObjectStoreExt, PutMode, PutOptions, path::Path};
        assert!(backend.reclaim(operation, None).await.is_err());
        let mut tx = physical::begin(&backend).await?;
        q::test_raw_profile(namespace).execute(&mut *tx).await?;
        tx.commit().await?;
        let profile = StorageProfileStore::load(backend.as_ref(), namespace)
            .await
            .context("load repair journal profile")?;
        let store = Store::build_control(
            &record,
            Options::from_profile(&profile)?,
            &secrets,
            control.clone(),
            None,
        )?;
        store
            .backend
            .put_opts(
                &Path::from("mokyu.json"),
                Bytes::from(serde_json::to_vec(&record.identity_marker)?).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;
        let token = Uuid::now_v7();
        let ledger = backend
            .claim(namespace, token)
            .await?
            .context("journal lease missing")?;
        let remote = Remote::connect(
            backend.as_ref(),
            backend.as_ref(),
            backend.as_ref(),
            &secrets,
            &ledger,
            token,
            store.native.clone(),
        )
        .await?;
        let mut tx = physical::begin(&backend).await?;
        let ledger = super::super::queries::journal::ledger(namespace)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        let capture = backend
            .native_capture(namespace, token)
            .await?
            .context("repair was not queued for recovery")?;
        assert_eq!(capture.operation, operation);
        let plan = publication::allocate_native(
            &ledger,
            vec![],
            capture.records.clone(),
            vec![],
            &remote.format(),
        )?;
        backend
            .native_allocate(namespace, token, &capture, &plan)
            .await?;
        assert!(
            backend.reclaim(operation, None).await.is_err(),
            "allocation alone cannot release old fragments"
        );
        let file = backend
            .next_file(namespace)
            .await?
            .context("repair journal was not allocated")?;
        let bytes = Bytes::from(backend.file_frames(file.id).await?.concat());
        remote
            .publish_native(
                backend.as_ref(),
                token,
                &file,
                bytes,
                &secrets.credential_key,
            )
            .await?;
    }
    let garbage = backend.reclaim(operation, None).await?;
    assert_eq!(garbage.len(), 2);
    for entry in &garbage {
        assert!(
            backend
                .resume_repair(
                    entry.header.fragment,
                    chrono::Utc::now().timestamp_micros() + 60_000_000
                )
                .await
                .is_err()
        );
    }
    assert_eq!(backend.reclaim(operation, None).await?.len(), 2);
    assert!(
        control
            .reclaim(namespace, operation, Area::Blocks, &pool)
            .await?
    );
    assert!(backend.reclaim(operation, None).await?.is_empty());
    assert_eq!(control.read_window(operation, 0, 1).await?.len(), 2);
    if logged {
        return Ok(());
    }
    backend
        .remove_object(namespace, "objects/repair", Some(operation))
        .await?;
    assert!(
        backend
            .request_repair(&Principal::Local, cluster.id, first.header.fragment)
            .await
            .is_err()
    );
    assert!(backend.reclaim(operation, None).await.is_err());
    let mut tx = physical::begin(&backend).await?;
    super::super::queries::storage_journal::test_expire(operation)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    let garbage = backend.reclaim(operation, None).await?;
    assert_eq!(garbage.len(), 2);
    assert!(
        control
            .reclaim(namespace, operation, Area::Blocks, &pool)
            .await?
    );
    assert!(backend.reclaim(operation, None).await?.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated database and MOKYU_TEST_STORAGE_TLS"]
async fn native_namespace_initialization_releases_locks_and_rechecks_publication() -> Result<()> {
    use crate::{
        model::StorageRecordStore,
        storage::{
            model::{Options, Store},
            native::control::{Control, WriteRequest},
        },
    };
    use std::{sync::Arc, time::Duration};
    let (backend, cluster, _nodes, secrets) = live_fixture().await?;
    let control = Arc::new(Control::new(
        backend.clone(),
        backend.clone(),
        secrets.clone(),
    ));
    let record = native_record(&cluster, false)?;
    let request = || WriteRequest {
        namespace: record.id,
        cluster: cluster.id,
        path: "mokyu.json",
        owner: Uuid::now_v7(),
        condition: Condition::Create,
        area: Area::Blocks,
        total: 128,
        region: None,
    };
    for invalid in [
        WriteRequest {
            path: "chunks/unregistered",
            ..request()
        },
        WriteRequest {
            condition: Condition::Overwrite,
            ..request()
        },
        WriteRequest {
            area: Area::Recovery,
            ..request()
        },
        WriteRequest {
            total: crate::storage::marker::MAX_BYTES as u64 + 1,
            ..request()
        },
    ] {
        assert!(control.reserve(invalid).await.is_err());
    }
    let mut missing = backend.creation().await?.writer;
    missing
        .prepare(&record, None, None, None, None, None)
        .await?;
    assert!(missing.finish().await.is_err());
    assert!(backend.named(&record.name).await?.is_none());
    let mut pending = backend.creation().await?.writer;
    let profile = pending
        .prepare(&record, None, None, None, None, None)
        .await?;
    let store = Store::build_control(
        &record,
        Options::from_profile(&profile)?,
        &secrets,
        control.clone(),
        None,
    )?;
    tokio::time::timeout(Duration::from_secs(10), store.initialize()).await??;
    assert!(backend.named(&record.name).await?.is_none());
    // A cancellation after marker publication leaves no partially committed backend.
    drop(pending);
    let mut resumed = backend.creation().await?.writer;
    resumed
        .prepare(&record, None, None, None, None, None)
        .await?;
    store.verify().await?;
    resumed.finish().await?;
    assert_eq!(
        backend.named(&record.name).await?.map(|row| row.id),
        Some(record.id)
    );

    let stale = backend.edit(&record.name).await?;
    tokio::time::timeout(Duration::from_secs(5), store.verify()).await??;
    tokio::time::timeout(
        Duration::from_secs(5),
        crate::model::StorageProfileStore::update(
            backend.as_ref(),
            record.name.clone(),
            stale.profile.clone(),
        ),
    )
    .await??;
    assert!(matches!(
        crate::model::error(
            &stale
                .update
                .finish(&record, None, &stale.profile)
                .await
                .unwrap_err()
        ),
        Some(crate::model::Error::StorageProfileConflict)
    ));

    let changed = native_record(&cluster, false)?;
    let mut pending = backend.creation().await?.writer;
    let profile = pending
        .prepare(&changed, None, None, None, None, None)
        .await?;
    let store = Store::build_control(
        &changed,
        Options::from_profile(&profile)?,
        &secrets,
        control.clone(),
        None,
    )?;
    store.initialize().await?;
    backend
        .remove_object(changed.id, "mokyu.json", None)
        .await?;
    assert!(pending.finish().await.is_err());
    assert!(backend.named(&changed.name).await?.is_none());
    store.initialize().await?;
    let mut first = backend.creation().await?.writer;
    let mut second = backend.creation().await?.writer;
    first
        .prepare(&changed, None, None, None, None, None)
        .await?;
    second
        .prepare(&changed, None, None, None, None, None)
        .await?;
    first.finish().await?;
    assert!(second.finish().await.is_err());
    assert_eq!(
        backend.named(&changed.name).await?.map(|row| row.id),
        Some(changed.id)
    );
    let mut tx = physical::begin(&backend).await?;
    q::test_enable(record.id).execute(&mut *tx).await?;
    let storages = super::super::queries::node_read_storages(&[record.id, changed.id])
        .fetch_all(&mut *tx)
        .await?;
    assert_eq!(storages.len(), 1);
    assert_eq!(storages[0].id, record.id);
    assert_eq!(storages[0].r#type, "mokyu");
    assert_eq!(storages[0].cluster_id, Some(cluster.id));
    tx.commit().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated database and MOKYU_TEST_STORAGE_TLS"]
async fn storage_samples_preserve_freshness_across_replays() -> Result<()> {
    use crate::model::storage_cluster::{CapacitySample, StorageClusters};
    let (backend, cluster, nodes, secrets) = live_fixture().await?;
    let node = &nodes[0];
    let original = node.announcement()?;
    let media = original.media.media_id;
    let sampled = chrono::Utc::now().timestamp_micros();
    let sample = CapacitySample {
        media,
        observed_us: sampled,
        capacity: original.capacity,
        load: original.load.unwrap(),
        revision: Some(cluster.revision),
        pending: None,
        history_after: cluster.revision,
    };
    let key = &secrets.credential_key;
    backend
        .sample_capacity(&node.state, key, node.run, sample.clone())
        .await?;
    let mut old = sample.clone();
    old.capacity.available_bytes = 0;
    for delta in [0, 1] {
        old.observed_us = sampled - delta;
        backend
            .sample_capacity(&node.state, key, node.run, old.clone())
            .await?;
    }
    let (_, targets) = backend.placement_snapshot(cluster.id).await?;
    let target = targets.iter().find(|t| t.device.media_id == media).unwrap();
    assert_eq!(target.device.observed_at, sampled);
    assert_eq!(
        target.device.announcement.capacity.available_bytes,
        sample.capacity.available_bytes
    );
    let mut stale_run = node.run;
    stale_run.boot = Uuid::now_v7();
    assert!(
        backend
            .sample_capacity(&node.state, key, stale_run, sample.clone())
            .await
            .is_err()
    );
    let mut full = sample;
    full.observed_us = chrono::Utc::now().timestamp_micros();
    full.capacity.available_bytes = 0;
    full.pending = Some(cluster.revision);
    assert!(
        backend
            .sample_capacity(&node.state, key, node.run, full)
            .await?
    );
    let (_, targets) = backend.placement_snapshot(cluster.id).await?;
    assert_eq!(
        targets
            .iter()
            .find(|t| t.device.media_id == media)
            .unwrap()
            .device
            .announcement
            .capacity
            .writable(),
        0
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated database and MOKYU_TEST_STORAGE_TLS"]
async fn native_capacity_reservations_survive_retries_and_reports() -> Result<()> {
    use crate::{
        model::storage_cluster::StorageClusters,
        storage::native::{
            control::{Control, WriteRequest},
            transport::Pool,
        },
    };
    use futures_util::StreamExt;
    use std::{sync::Arc, time::Duration};
    let (backend, cluster, nodes, secrets) = live_fixture().await?;
    let (namespace, _) = native_backend(&backend, &cluster, true).await?;
    let control = Arc::new(Control::new(
        backend.clone(),
        backend.clone(),
        secrets.clone(),
    ));
    let key = &secrets.credential_key;
    for node in &nodes {
        let mut report = node.announcement()?;
        report.capacity.limit = Some(report.capacity.used_bytes + 200_000);
        node.apply(backend.announce(&node.state, key, node.run, report).await?)
            .await?;
    }
    let owners = [Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7()];
    let paths = [
        "objects/reserve-a",
        "objects/reserve-b",
        "objects/reserve-c",
    ];
    let request = |index: usize| WriteRequest {
        namespace,
        cluster: cluster.id,
        path: paths[index],
        owner: owners[index],
        condition: Condition::Create,
        area: Area::Blocks,
        total: 65536,
        region: None,
    };
    let (a, b) = tokio::join!(control.reserve(request(0)), control.reserve(request(1)));
    assert_eq!(
        usize::from(a.is_ok()) + usize::from(b.is_ok()),
        1,
        "two independent transactions must not reserve the same remaining space"
    );
    let (index, object) = if let Ok(value) = a {
        (0, value)
    } else {
        (1, b?)
    };
    let intent = &object.payload.intent;
    let amount = super::super::storage_capacity::bytes(intent, 0, 0)?;
    assert!(amount > intent.total);
    assert_eq!(
        control.reserve(request(index)).await?.payload.intent,
        *intent
    );
    let fresh_control = Control::new(backend.clone(), backend.clone(), secrets.clone());
    assert!(fresh_control.reserve(request(2)).await.is_err());
    let mut tx = physical::begin(&backend).await?;
    let totals = super::super::queries::storage_capacity::totals(cluster.id)
        .fetch_all(&mut *tx)
        .await?;
    assert_eq!(totals.len(), 2);
    assert!(totals.iter().all(|(_, bytes)| *bytes as u64 == amount));
    assert_eq!(
        super::super::queries::storage_capacity::allocation(cluster.id)
            .fetch_one(&mut *tx)
            .await?
            .2,
        Some(intent.group)
    );
    tx.commit().await?;

    let pool = Pool::new(Duration::from_secs(30));
    let mut proofs = Vec::new();
    for piece in control.write_window(intent.operation, 0, 1).await? {
        proofs.push(
            pool.write(
                piece.destination.context("reserved target missing")?,
                futures_util::stream::iter([Ok(bytes::Bytes::from(vec![51; 65536]))]).boxed(),
                control.as_ref(),
            )
            .await?,
        );
    }
    control.accept(intent.operation, &proofs).await?;
    control.accept(intent.operation, &proofs).await?;
    assert!(
        fresh_control.reserve(request(2)).await.is_err(),
        "receipts do not replace a capacity report"
    );
    for node in &nodes {
        let report = node.announcement()?;
        let latest = report.capacity.accounting.unwrap();
        node.apply(
            backend
                .announce(&node.state, key, node.run, report.clone())
                .await?,
        )
        .await?;
        let mut stale = report;
        stale.capacity.accounting.as_mut().unwrap().revision = latest.revision - 1;
        assert!(
            backend
                .announce(&node.state, key, node.run, stale)
                .await
                .is_err()
        );
    }
    let again = fresh_control.reserve(request(2)).await?;
    let mut tx = physical::begin(&backend).await?;
    for media in &intent.members {
        assert!(
            super::super::queries::storage_capacity::reserved(intent.operation, *media)
                .fetch_optional(&mut *tx)
                .await?
                .is_none()
        );
    }
    tx.commit().await?;
    control.accept(intent.operation, &proofs).await?;
    assert_eq!(
        control.reserve(request(2)).await?.payload.intent,
        again.payload.intent
    );
    // A staged, unconfirmed candidate retains its reservation after logical deletion.
    control
        .write_window(again.payload.intent.operation, 0, 1)
        .await?;
    control.remove(namespace, paths[2]).await?;
    let mut tx = physical::begin(&backend).await?;
    for media in &again.payload.intent.members {
        assert!(
            super::super::queries::storage_capacity::reserved(
                again.payload.intent.operation,
                *media
            )
            .fetch_optional(&mut *tx)
            .await?
            .is_some()
        );
    }
    tx.commit().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_PAYLOAD_CONFIG and an isolated database"]
async fn native_objects_publish_atomically_and_preserve_old_reads() -> Result<()> {
    let (backend, cluster, _nodes, _secrets) = live_fixture().await?;
    let (namespace, _) = native_backend(&backend, &cluster, false).await?;
    let group = &cluster.manifest.groups[0];
    let mut write = Write {
        recovery: None,
        region: None,
        path: "objects/test".into(),
        owner: Uuid::now_v7(),
        condition: Condition::Create,
        intent: Intent {
            operation: Uuid::now_v7(),
            namespace,
            payload: Uuid::now_v7(),
            revision: 1,
            cluster: cluster.id,
            layout_revision: cluster.manifest.revision as u64,
            takeover_epoch: cluster.manifest.takeover_epoch,
            group: group.id,
            layout: group.layout.clone(),
            members: group.members.clone(),
            area: Area::Blocks,
            total: 1024,
            acknowledgement: cluster.policy.acknowledgement,
        },
    };
    let until = chrono::Utc::now().timestamp_micros() + 80_000_000;
    let first = backend.reserve_object(&write, until).await?.payload;
    let retry = backend.reserve_object(&write, until).await?.payload;
    assert_eq!(first.intent, retry.intent);
    assert!(
        backend
            .read_object(namespace, &write.path, None, until)
            .await
            .is_err()
    );
    assert_eq!(
        backend
            .read_object(namespace, &write.path, Some(write.owner), until)
            .await?
            .payload
            .intent
            .operation,
        first.intent.operation
    );
    received(&backend, &first.intent, 0..1, 17).await?;
    assert!(
        backend
            .publish_object(namespace, first.intent.payload, write.owner)
            .await
            .is_err()
    );
    received(&backend, &first.intent, 1..2, 17).await?;
    let mut tx = physical::begin(&backend).await?;
    publish_in(&mut tx, namespace, first.intent.payload, write.owner).await?;
    tx.rollback().await?;
    assert!(
        backend
            .read_object(namespace, &write.path, None, until)
            .await
            .is_err()
    );
    backend
        .publish_object(namespace, first.intent.payload, write.owner)
        .await?;
    backend
        .publish_object(namespace, first.intent.payload, write.owner)
        .await?;
    assert_eq!(
        backend
            .read_object(namespace, &write.path, None, until)
            .await?
            .payload
            .intent
            .operation,
        first.intent.operation
    );
    write.owner = Uuid::now_v7();
    write.intent.operation = Uuid::now_v7();
    assert!(backend.reserve_object(&write, until).await.is_err());
    write.condition = Condition::Match(Uuid::now_v7());
    assert!(backend.reserve_object(&write, until).await.is_err());
    write.condition = Condition::Match(first.intent.operation);
    let second = backend.reserve_object(&write, until).await?.payload;
    assert_eq!(second.intent.revision, 2);
    assert_eq!(
        backend
            .read_object(namespace, &write.path, None, until)
            .await?
            .payload
            .intent
            .operation,
        first.intent.operation
    );
    received(&backend, &second.intent, 0..2, 18).await?;
    backend
        .publish_object(namespace, second.intent.payload, write.owner)
        .await?;
    assert!(backend.reclaim(first.intent.operation, None).await.is_err());
    assert_eq!(backend.get(first.intent.operation).await?.state, "retired");
    assert!(
        backend
            .remove_object(namespace, &write.path, Some(first.intent.operation))
            .await
            .is_err()
    );
    backend
        .remove_object(namespace, &write.path, Some(second.intent.operation))
        .await?;
    assert!(
        backend
            .read_object(namespace, &write.path, None, until)
            .await
            .is_err()
    );
    assert!(
        backend
            .reclaim(second.intent.operation, None)
            .await
            .is_err()
    );
    assert!(backend.list_objects(namespace, None).await?.is_empty());
    assert!(backend.collect_candidates(namespace, 64).await?.is_empty());
    let mut tx = physical::begin(&backend).await?;
    super::super::queries::storage_journal::test_expire(first.intent.operation)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    assert_eq!(
        backend.collect_candidates(namespace, 64).await?,
        [first.intent.operation]
    );
    assert!(
        backend
            .collect_candidates(Uuid::now_v7(), 64)
            .await?
            .is_empty()
    );
    listing_pages(&backend, &write.intent).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated database and MOKYU_TEST_STORAGE_TLS"]
async fn native_gc_reclaims_only_unprotected_retired_files() -> Result<()> {
    use crate::storage::native::{
        control::{Control, WriteRequest},
        transport::Pool,
        writer,
    };
    use futures_util::StreamExt;
    use std::{sync::Arc, time::Duration};
    let (backend, cluster, nodes, secrets) = live_fixture().await?;
    let (namespace, _) = native_backend(&backend, &cluster, true).await?;
    let control = Arc::new(Control::new(backend.clone(), backend.clone(), secrets));
    let pool = Pool::new(Duration::from_secs(30));
    for accepted in [true, false] {
        let owner = Uuid::now_v7();
        let path = format!("objects/{}", owner.simple());
        let body = bytes::Bytes::from(vec![53; 128 * 1024]);
        let object = control
            .reserve(WriteRequest {
                namespace,
                cluster: cluster.id,
                path: &path,
                owner,
                condition: Condition::Create,
                area: Area::Blocks,
                total: body.len() as u64,
                region: None,
            })
            .await?;
        let operation = object.payload.intent.operation;
        let pieces = control.write_window(operation, 0, 1).await?;
        if accepted {
            writer::write(
                control.as_ref(),
                &pool,
                &object.payload.intent,
                futures_util::stream::iter([Ok(body.clone())]).boxed(),
                None,
            )
            .await?;
            control
                .publish(namespace, object.payload.intent.payload, owner)
                .await?;
        } else {
            for piece in &pieces {
                pool.write(
                    piece.destination.clone().unwrap(),
                    futures_util::stream::iter([Ok(body.clone())]).boxed(),
                    control.as_ref(),
                )
                .await?;
            }
        }
        let files: Vec<_> = pieces
            .iter()
            .map(|piece| {
                let header = &piece.mapping.header;
                let node = nodes
                    .iter()
                    .find(|node| node.announcement().unwrap().media.media_id == header.media)
                    .unwrap();
                node.disk().root.join(header.area.path(header.fragment))
            })
            .collect();
        assert!(files.iter().all(|path| path.exists()));
        assert_eq!(
            control
                .reclaim(namespace, operation, Area::Blocks, &pool)
                .await?,
            accepted
        );
        assert!(files.iter().all(|path| path.exists()));
        control.remove(namespace, &path).await?;
        assert!(control.collect_candidates(namespace, 64).await?.is_empty());
        assert!(
            !control
                .reclaim(namespace, operation, Area::Blocks, &pool)
                .await?
        );
        assert!(files.iter().all(|path| path.exists()));
        let mut tx = physical::begin(backend.as_ref()).await?;
        super::super::queries::storage_journal::test_expire(operation)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        assert_eq!(
            control.collect_candidates(namespace, 64).await?,
            [operation]
        );
        assert!(
            control
                .reclaim(Uuid::now_v7(), operation, Area::Blocks, &pool)
                .await
                .is_err()
        );
        assert!(
            control
                .reclaim(namespace, operation, Area::Blocks, &pool)
                .await?
        );
        assert!(files.iter().all(|path| !path.exists()));
        assert!(
            control
                .reclaim(namespace, operation, Area::Blocks, &pool)
                .await?
        );
        assert!(control.collect_candidates(namespace, 64).await?.is_empty());
    }
    Ok(())
}

async fn listing_pages(backend: &Backend, layout: &Intent) -> Result<()> {
    let mut names: Vec<_> = (0..260)
        .rev()
        .map(|index| format!("objects/{index:04}"))
        .collect();
    names.extend(["objects/a", "objects/A", "objects/%c3%a9", "objects/z"].map(str::to_owned));
    for name in &names {
        let mut intent = layout.clone();
        intent.operation = Uuid::now_v7();
        intent.payload = Uuid::now_v7();
        intent.revision = 1;
        intent.total = 1;
        let write = Write {
            recovery: None,
            region: None,
            path: name.clone(),
            owner: Uuid::now_v7(),
            condition: Condition::Create,
            intent,
        };
        let object = backend
            .reserve_object(&write, chrono::Utc::now().timestamp_micros() + 80_000_000)
            .await?;
        received(backend, &object.payload.intent, 0..2, 1).await?;
        backend
            .publish_object(layout.namespace, object.payload.intent.payload, write.owner)
            .await?;
    }
    names.sort();
    let first = backend.list_objects(layout.namespace, None).await?;
    assert_eq!(first.len(), 256);
    assert_eq!(
        first.iter().map(|row| &row.path).collect::<Vec<_>>(),
        names[..256].iter().collect::<Vec<_>>()
    );
    let second = backend
        .list_objects(layout.namespace, Some(&first.last().unwrap().path))
        .await?;
    assert_eq!(
        second.iter().map(|row| &row.path).collect::<Vec<_>>(),
        names[256..].iter().collect::<Vec<_>>()
    );
    assert!(
        backend
            .list_objects(layout.namespace, Some(&second.last().unwrap().path))
            .await?
            .is_empty()
    );
    Ok(())
}

async fn live_fixture() -> Result<(
    std::sync::Arc<Backend>,
    crate::model::storage_cluster::Cluster,
    Vec<crate::node::storage::TestNode>,
    std::sync::Arc<crate::config::Secrets>,
)> {
    use crate::{
        authorization::Principal,
        model::{
            cluster::NodeSessions,
            session::Open,
            storage_cluster::{Create, Members, StorageClusters},
        },
        node::storage::TestNode,
    };
    use std::sync::Arc;
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let config = std::env::var("MOKYU_TEST_PAYLOAD_CONFIG")?;
    let (_, secrets, _) = crate::config::Config::load(std::path::Path::new(&config))?;
    let secrets = Arc::new(secrets);
    let key = &secrets.credential_key;
    let backend = Arc::new(physical::test_backend().await?);
    let principal = Principal::Local;
    let mut nodes = Vec::new();
    for _ in 0..2 {
        let state = backend
            .provision_node(
                &principal,
                &format!("native-{}", Uuid::now_v7()),
                "storage",
                key,
            )
            .await?;
        let run = backend
            .open_session(
                &state,
                key,
                Open {
                    boot: Uuid::now_v7(),
                    connection: Uuid::now_v7(),
                    previous_epoch: 0,
                },
            )
            .await?
            .run;
        let node = TestNode::new(state, run).await?;
        node.apply(
            backend
                .announce(&node.state, key, node.run, node.announcement()?)
                .await?,
        )
        .await?;
        nodes.push(node);
    }
    let mut cluster = backend
        .create(
            &principal,
            key,
            Create {
                name: format!("native-{}", Uuid::now_v7()),
                policy: Default::default(),
            },
        )
        .await?;
    let media: Vec<_> = nodes
        .iter()
        .map(|node| node.announcement().map(|value| value.media.media_id))
        .collect::<Result<_>>()?;
    cluster = backend
        .add_members(
            &principal,
            cluster.id,
            Members {
                revision: cluster.revision,
                media_ids: media.clone(),
            },
        )
        .await?;
    cluster = backend
        .enable_groups(
            &principal,
            cluster.id,
            Members {
                revision: cluster.revision,
                media_ids: media,
            },
        )
        .await?;
    for node in &nodes {
        node.apply(
            backend
                .announce(&node.state, key, node.run, node.announcement()?)
                .await?,
        )
        .await?;
        node.apply(
            backend
                .announce(&node.state, key, node.run, node.announcement()?)
                .await?,
        )
        .await?;
    }
    Ok((backend, cluster, nodes, secrets))
}

#[tokio::test]
#[ignore = "requires an isolated database and MOKYU_TEST_STORAGE_TLS"]
async fn native_control_plans_and_verifies_real_storage_receipts() -> Result<()> {
    use crate::{
        model::{
            cluster::NodeSessions, session::Open, storage_cluster::StorageClusters,
            storage_transfer::Completion,
        },
        storage::native::{
            control::{Control, WriteRequest},
            transport::Pool,
        },
    };
    use bytes::Bytes;
    use futures_util::{StreamExt, TryStreamExt};
    use std::{sync::Arc, time::Duration};
    let (backend, cluster, nodes, secrets) = live_fixture().await?;
    let key = &secrets.credential_key;
    let (namespace, record) = native_backend(&backend, &cluster, true).await?;
    let control = Arc::new(Control::new(
        backend.clone(),
        backend.clone(),
        secrets.clone(),
    ));
    let owner = Uuid::now_v7();
    let bytes = Bytes::from(vec![37; 2 * 1024 * 1024 + 31]);
    let location = Uuid::now_v7();
    let path = crate::storage::location::Layout::S3
        .object(location)
        .to_string();
    let request = || WriteRequest {
        namespace,
        cluster: cluster.id,
        path: &path,
        owner,
        condition: Condition::Create,
        area: Area::Blocks,
        total: bytes.len() as u64,
        region: Some("test"),
    };
    let object = control.reserve(request()).await?;
    assert_eq!(
        control.reserve(request()).await?.payload.intent,
        object.payload.intent
    );
    let operation = object.payload.intent.operation;
    let plan = control.write_window(operation, 0, 1).await?;
    let retried = control.write_window(operation, 0, 1).await?;
    assert_eq!(plan.len(), 2);
    assert_eq!(
        plan.iter()
            .map(|piece| piece.mapping.header.clone())
            .collect::<Vec<_>>(),
        retried
            .iter()
            .map(|piece| piece.mapping.header.clone())
            .collect::<Vec<_>>()
    );
    let pool = Arc::new(Pool::new(Duration::from_secs(60)));
    let mut completed = Vec::new();
    for piece in plan {
        let destination = piece.destination.context("test storage unavailable")?;
        completed.push(
            pool.write(
                destination,
                futures_util::stream::iter([Ok(bytes.clone())]).boxed(),
                control.as_ref(),
            )
            .await?,
        );
    }
    let mut forged = completed.clone();
    forged[1].receipt.tag[0] ^= 1;
    assert!(control.accept(operation, &forged).await.is_err());
    assert!(
        backend
            .write_window(
                operation,
                0,
                1,
                chrono::Utc::now().timestamp_micros() + 30_000_000
            )
            .await?
            .iter()
            .all(|mapping| mapping.receipt.is_none())
    );
    control.accept(operation, &completed).await?;
    control.accept(operation, &completed).await?;
    assert!(control.open(namespace, &path, None).await.is_err());
    backend
        .publish_object(namespace, object.payload.intent.payload, owner)
        .await?;
    assert_eq!(
        control.open(namespace, &path, None).await?.payload.intent,
        object.payload.intent
    );
    let mappings = control.read_window(operation, 0, 1).await?;
    for mapping in &mappings {
        let destination = control.read(mapping, 11, bytes.len() as u64 - 22).await?;
        let read = pool
            .read(destination, control.clone())
            .await?
            .try_fold(Vec::new(), |mut output, bytes| async move {
                output.extend_from_slice(&bytes);
                Ok(output)
            })
            .await?;
        assert_eq!(read, bytes[11..bytes.len() - 11]);
    }
    let plan = control
        .plan_read(
            namespace,
            &path,
            Some(operation),
            None,
            Some(11..bytes.len() as u64 - 11),
            Some("test"),
            chrono::Utc::now().timestamp_micros() + 70_000_000,
        )
        .await?;
    assert_eq!(plan.parts.len(), 5);
    assert_ne!(
        plan.parts[0].copies[0].authorization.grant.media,
        plan.parts[1].copies[0].authorization.grant.media
    );
    let read =
        crate::storage::native::reader::read(pool.clone(), control.clone(), plan.clone(), 2)?
            .try_fold(Vec::new(), |mut output, bytes| async move {
                output.extend_from_slice(&bytes);
                Ok(output)
            })
            .await?;
    assert_eq!(read, bytes[11..bytes.len() - 11]);
    let bad = &mappings[0].header;
    nodes
        .iter()
        .find(|node| node.announcement().unwrap().media.media_id == bad.media)
        .unwrap()
        .corrupt(bad, (3 * fragment::UNIT) as u64)
        .await?;
    let read = crate::storage::native::reader::read(pool.clone(), control.clone(), plan, 2)?
        .try_fold(Vec::new(), |mut output, bytes| async move {
            output.extend_from_slice(&bytes);
            Ok(output)
        })
        .await?;
    assert_eq!(
        read,
        bytes[11..bytes.len() - 11],
        "a corrupt copy must resume without duplicating delivered bytes"
    );
    let path = "objects/stream-test";
    let owner = Uuid::now_v7();
    let stream = control
        .reserve(WriteRequest {
            path,
            owner,
            ..request()
        })
        .await?;
    let (prefix, suffix) = (bytes.slice(..19), bytes.slice(19..));
    crate::storage::native::writer::write(
        control.as_ref(),
        &pool,
        &stream.payload.intent,
        futures_util::stream::iter([Ok(prefix.clone()), Ok(suffix.clone())]).boxed(),
        None,
    )
    .await?;
    backend
        .publish_object(namespace, stream.payload.intent.payload, owner)
        .await?;
    let pieces = control
        .read_window(stream.payload.intent.operation, 0, 1)
        .await?;
    assert_eq!(
        pieces
            .iter()
            .filter(|piece| piece.receipt.is_some())
            .count(),
        2
    );
    assert_eq!(
        pieces[0].receipt.as_ref().unwrap().header.digest,
        pieces[1].receipt.as_ref().unwrap().header.digest
    );
    let short_owner = Uuid::now_v7();
    let short = control
        .reserve(WriteRequest {
            path: "objects/short",
            owner: short_owner,
            ..request()
        })
        .await?;
    assert!(
        crate::storage::native::writer::write(
            control.as_ref(),
            &pool,
            &short.payload.intent,
            futures_util::stream::iter([Ok(prefix)]).boxed(),
            None,
        )
        .await
        .is_err()
    );
    assert!(
        backend
            .publish_object(namespace, short.payload.intent.payload, short_owner)
            .await
            .is_err()
    );
    for excess in [false, true] {
        let owner = Uuid::now_v7();
        let path = format!("objects/invalid-end-{excess}");
        let object = control
            .reserve(WriteRequest {
                path: &path,
                owner,
                ..request()
            })
            .await?;
        let tail = if excess {
            Ok(Bytes::from_static(b"x"))
        } else {
            Err(anyhow::anyhow!("final digest mismatch"))
        };
        assert!(
            crate::storage::native::writer::write(
                control.as_ref(),
                &pool,
                &object.payload.intent,
                futures_util::stream::iter([Ok(bytes.clone()), tail]).boxed(),
                None,
            )
            .await
            .is_err()
        );
        assert!(
            backend
                .publish_object(namespace, object.payload.intent.payload, owner)
                .await
                .is_err(),
            "failed input validation must not leave a publishable candidate"
        );
    }
    marker_publication(
        control.clone(),
        pool.clone(),
        namespace,
        cluster.id,
        &record.identity_marker,
    )
    .await?;
    read_access(&backend, &control, key, namespace, location).await?;
    work_access(&backend, &control, &pool, key, namespace)
        .await
        .context("native work authority")?;
    object_store(
        &backend,
        control.clone(),
        pool.clone(),
        namespace,
        cluster.id,
    )
    .await?;
    chunk_publication(
        &backend,
        control.clone(),
        pool.clone(),
        namespace,
        cluster.id,
    )
    .await?;
    raw_publication(
        &backend,
        control.clone(),
        pool.clone(),
        namespace,
        cluster.id,
    )
    .await?;
    pack_streams(
        &backend,
        control.clone(),
        pool.clone(),
        namespace,
        cluster.id,
    )
    .await
    .context("native pack writers")?;
    raw_writers(backend.clone(), control.clone(), secrets.clone(), &record)
        .await
        .context("native raw writers")?;
    let first = &nodes[0];
    backend.close_session(&first.state, key, first.run).await?;
    backend
        .open_session(
            &first.state,
            key,
            Open {
                boot: Uuid::now_v7(),
                connection: Uuid::now_v7(),
                previous_epoch: 0,
            },
        )
        .await?;
    assert!(control.accept(operation, &completed).await.is_err());
    let resumed = Control::new(backend.clone(), backend.clone(), secrets.clone());
    assert_eq!(
        resumed.reserve(request()).await?.payload.intent,
        object.payload.intent,
        "retry must retain its fixed placement when fresh allocation is unavailable"
    );
    assert!(
        resumed
            .reserve(WriteRequest {
                total: 1,
                ..request()
            })
            .await
            .is_err()
    );
    Ok(())
}

async fn pack_streams(
    backend: &Backend,
    control: std::sync::Arc<crate::storage::native::control::Control>,
    pool: std::sync::Arc<crate::storage::native::transport::Pool>,
    namespace: Uuid,
    cluster: Uuid,
) -> Result<()> {
    use crate::storage::{
        CacheFile, CachePolicy, Fifo,
        file_cache::Files,
        local::{Disk, Usage},
        native::store::{Authority, Store},
        pack_writer::Writer,
    };
    use bytes::Bytes;
    use futures_util::TryStreamExt;
    use object_store::{ObjectStore, ObjectStoreExt, path::Path};
    use std::sync::Arc;
    use tokio::sync::{Mutex, Semaphore};
    let root = std::env::temp_dir().join(format!("mokyu-native-packs-{}", Uuid::now_v7()));
    let limit = 48 * 1024 * 1024;
    let disk = Arc::new(Disk {
        root: root.clone(),
        data: None,
        limits: [None, Some(limit), None],
        floor: 0,
        spaces: Default::default(),
        upload_limit: None,
        usage: std::sync::Mutex::new(Usage {
            used: [0; 3],
            upload: 0,
            upload_by_storage: Default::default(),
        }),
        io: Arc::new(Semaphore::new(1)),
        pending: Default::default(),
    });
    disk.initialize().await?;
    let fifo = Mutex::new(Fifo::default());
    let writes = Mutex::new(());
    let files = || Files {
        disk: &disk,
        root: &root,
        fifo: &fifo,
        cache_writes: &writes,
    };
    let policy = CachePolicy {
        enabled: true,
        cold: false,
        capacity: Some(limit),
        max_entries: 32,
        upload_cache: false,
        upload_capacity: None,
    };
    let store = Arc::new(Store {
        authority: Authority::Control(control.clone()),
        pool,
        namespace,
        cluster,
        prefix: Path::from("nested"),
        region: None,
    });
    let size = crate::content::pack::SINGLE_PUT + 19;
    for stream in [false, true] {
        let owner = Uuid::now_v7();
        let path = crate::storage::location::Layout::S3.pack(owner);
        let mut writer = if stream {
            Writer::stream(store.clone(), path.clone(), size as u64, None)
        } else {
            Writer::spool(store.clone(), path.clone(), size, files(), policy, None).await?
        };
        let mut expected = blake3::Hasher::new();
        let mut offset = 0;
        while offset < size {
            let bytes = Bytes::from(vec![(offset / 131071) as u8; (size - offset).min(131071)]);
            expected.update(&bytes);
            offset += bytes.len();
            writer.put(bytes).await?;
        }
        assert!(store.head(&path).await.is_err());
        writer.finish().await?;
        assert!(
            store.head(&path).await.is_err(),
            "stored bytes must not publish a carrier"
        );
        let object = control
            .open(namespace, &format!("nested/{path}"), Some(owner))
            .await?;
        backend
            .publish_object(namespace, object.payload.intent.payload, owner)
            .await?;
        let actual = store
            .get(&path)
            .await?
            .into_stream()
            .try_fold(blake3::Hasher::new(), |mut hash, bytes| async move {
                hash.update(&bytes);
                Ok(hash)
            })
            .await?;
        assert_eq!(actual.finalize(), expected.finalize());
    }
    let owner = Uuid::now_v7();
    let path = crate::storage::location::Layout::S3.pack(owner);
    let mut writer = Writer::spool(
        store.clone(),
        path.clone(),
        8192,
        files(),
        CachePolicy {
            capacity: Some(4096),
            ..policy
        },
        None,
    )
    .await?;
    writer.put(Bytes::from(vec![7; 4096])).await?;
    assert!(writer.put(Bytes::from_static(b"x")).await.is_err());
    assert!(
        control
            .open(namespace, &format!("nested/{path}"), Some(owner))
            .await
            .is_err()
    );
    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while disk.used()[1] != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(
        !CacheFile::PendingRaw
            .path(&root, owner)
            .with_extension("part")
            .exists()
    );
    let owner = Uuid::now_v7();
    let path = crate::storage::location::Layout::S3.pack(owner);
    let mut writer = Writer::stream(store.clone(), path.clone(), 256 * 1024, None);
    writer.put(Bytes::from(vec![8; 128 * 1024])).await?;
    assert!(writer.finish().await.is_err());
    let candidate = control
        .open(namespace, &format!("nested/{path}"), Some(owner))
        .await?;
    assert!(
        backend
            .publish_object(namespace, candidate.payload.intent.payload, owner)
            .await
            .is_err()
    );
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

async fn raw_writers(
    backend: std::sync::Arc<Backend>,
    control: std::sync::Arc<crate::storage::native::control::Control>,
    secrets: std::sync::Arc<crate::config::Secrets>,
    record: &crate::storage::model::Record,
) -> Result<()> {
    use crate::{
        authorization::{Permit, Principal},
        model::{
            AdministrationStore, StreamStore,
            administration::ProjectInput,
            raw::{PageWrite, RawStore},
        },
        storage::Storage,
    };
    use md5::{Digest, Md5};
    use object_store::ObjectStoreExt;
    use std::sync::Arc;
    let path = std::env::var("MOKYU_TEST_PAYLOAD_CONFIG")?;
    let (mut config, _, _) = crate::config::Config::load(std::path::Path::new(&path))?;
    config.instance.data_path =
        std::env::temp_dir().join(format!("mokyu-native-raw-{}", Uuid::now_v7()));
    config.instance.cache_size = Some("64MiB".into());
    config.instance.min_free_space = "1B".into();
    tokio::fs::create_dir_all(&config.instance.data_path).await?;
    let storage = Arc::new(
        Storage::new(
            &config,
            secrets,
            control,
            &config.budget()?,
            backend.clone(),
            None,
            None,
            None,
            backend.clone(),
            backend.clone(),
            backend.clone(),
            backend.clone(),
            Arc::default(),
            Arc::default(),
            Arc::default(),
            false,
            false,
        )
        .await?,
    );
    // Exercise the writer before the public backend capability gate is enabled.
    let mut enabled = record.clone();
    enabled.enabled = true;
    let mut tx = physical::begin(&backend).await?;
    super::super::queries::update_storage_record(&enabled, None)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    storage.refresh_store(record.id).await?;
    let project = backend
        .save_project(
            &Principal::Local,
            None,
            ProjectInput {
                name: format!("native-writers-{}", Uuid::now_v7()),
                description: String::new(),
                allow_bucket_create: true,
            },
        )
        .await?;
    let mut tx = physical::begin(&backend).await?;
    super::super::queries::update_storage_record(record, None)
        .execute(&mut *tx)
        .await?;
    let bucket = q::test_bucket(&mut tx, project.id, record.id).await?;
    tx.commit().await?;
    for declared in [true, false] {
        let stream = StreamStore::create(
            backend.as_ref(),
            crate::model::StreamCreate {
                authority: Permit::system(),
                bucket,
                key: format!("raw-{declared}"),
                purpose: crate::model::StreamPurpose::Internal,
                metadata: serde_json::json!({}),
                public: false,
                completion: None,
                owner: None,
            },
        )
        .await?;
        let (source, location) =
            RawStore::begin(backend.as_ref(), stream, record.id, 1, "durable").await?;
        let bytes = vec![if declared { 53 } else { 61 }; 2 * 1024 * 1024 + 31];
        let digest = Md5::digest(&bytes);
        let mut pages = PageWrite::new(source);
        let mut writer = storage
            .raw_writer(
                record.id,
                source,
                location,
                declared.then_some(bytes.len() as i64),
                false,
            )
            .await?;
        for part in bytes.chunks(65521) {
            writer.write(part).await?;
            pages.feed(backend.as_ref(), part).await?;
        }
        pages
            .finish(backend.as_ref(), bytes.len() as i64, &digest)
            .await?;
        let written = writer
            .finish(stream, bytes.len() as u64, &digest, None)
            .await?;
        let crate::storage::raw_upload::Written::Final(_, fill) = written else {
            anyhow::bail!("native raw write returned before durable storage");
        };
        RawStore::finish(
            backend.as_ref(),
            stream,
            source,
            bytes.len() as i64,
            &digest,
        )
        .await?;
        if let Some(fill) = fill {
            let _ = fill.send(());
        }
        let source = RawStore::current(backend.as_ref(), source)
            .await?
            .context("raw writer source is missing")?;
        let mut reader = storage.raw_reader(&source, false).await?;
        let mut output = vec![0; 65536];
        let mut received = Vec::new();
        loop {
            let count = reader.file.read(&mut output).await?;
            if count == 0 {
                break;
            }
            received.extend_from_slice(&output[..count]);
        }
        assert_eq!(received, bytes);
        drop(reader);
        storage.remove_raw(&source).await?;
        assert!(
            storage
                .store_backend(record.id)?
                .backend
                .head(&crate::storage::raw::remote_path(location))
                .await
                .is_err()
        );
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while storage.disk.used()[1] != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    tokio::fs::remove_dir_all(&config.instance.data_path).await?;
    Ok(())
}

async fn object_store(
    backend: &Backend,
    control: std::sync::Arc<crate::storage::native::control::Control>,
    pool: std::sync::Arc<crate::storage::native::transport::Pool>,
    namespace: Uuid,
    cluster: Uuid,
) -> Result<()> {
    use futures_util::TryStreamExt;
    use object_store::{GetOptions, ObjectStore, ObjectStoreExt, PutMode, path::Path};
    let store = crate::storage::native::store::Store {
        authority: crate::storage::native::store::Authority::Control(control.clone()),
        pool,
        namespace,
        cluster,
        prefix: Path::from("nested"),
        region: None,
    };
    let owner = Uuid::now_v7();
    let path = crate::storage::location::Layout::S3.object(owner);
    let bytes = bytes::Bytes::from(vec![43; 9 * 1024 * 1024 + 13]);
    let written = store
        .put_opts(&path, bytes.clone().into(), PutMode::Create.into())
        .await?;
    assert!(matches!(
        store.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    let object = control
        .open(namespace, &format!("nested/{path}"), Some(owner))
        .await?;
    backend
        .publish_object(namespace, object.payload.intent.payload, owner)
        .await?;
    let result = store
        .get_opts(
            &path,
            GetOptions {
                if_match: written.e_tag.clone(),
                range: Some((11..133333).into()),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(result.meta.location, path);
    assert_eq!(result.meta.size, bytes.len() as u64);
    assert_eq!(result.bytes().await?, bytes.slice(11..133333));
    assert_eq!(
        store.get(&path).await?.bytes().await?,
        bytes,
        "reads must continue across planning windows"
    );
    assert!(matches!(
        store
            .get_opts(
                &path,
                GetOptions {
                    if_match: Some("incorrect".into()),
                    ..Default::default()
                }
            )
            .await,
        Err(object_store::Error::Precondition { .. })
    ));
    let page = store
        .list(Some(&Path::from("objects")))
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].location, path);
    let pinned = store.get(&path).await?;
    let old = control
        .plan_read(
            namespace,
            &format!("nested/{path}"),
            None,
            None,
            None,
            None,
            i64::MAX,
        )
        .await?;
    let replacement_owner = Uuid::now_v7();
    let replacement = bytes::Bytes::from(vec![67; bytes.len()]);
    let mut options: object_store::PutOptions = PutMode::Update(object_store::UpdateVersion {
        e_tag: written.e_tag,
        version: written.version.clone(),
    })
    .into();
    options
        .extensions
        .insert(crate::storage::native::store::Publication {
            owner: replacement_owner,
            area: Area::Blocks,
        });
    store
        .put_opts(&path, replacement.clone().into(), options)
        .await?;
    let next = control
        .open(
            namespace,
            &format!("nested/{path}"),
            Some(replacement_owner),
        )
        .await?;
    backend
        .publish_object(namespace, next.payload.intent.payload, replacement_owner)
        .await?;
    assert_eq!(
        pinned.bytes().await?,
        bytes,
        "a replaced object must finish its original read across windows"
    );
    assert!(
        store
            .get_opts(
                &path,
                GetOptions {
                    version: written.version,
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    assert!(
        control
            .plan_read(
                namespace,
                "another/path",
                Some(old.object.version),
                Some(&old.lease),
                None,
                None,
                i64::MAX
            )
            .await
            .is_err()
    );
    assert!(
        control
            .plan_read(
                Uuid::now_v7(),
                &format!("nested/{path}"),
                Some(old.object.version),
                Some(&old.lease),
                None,
                None,
                i64::MAX
            )
            .await
            .is_err()
    );
    let mut invalid = old.lease.clone();
    invalid.modified += 1;
    assert!(
        control
            .plan_read(
                namespace,
                &format!("nested/{path}"),
                Some(old.object.version),
                Some(&invalid),
                None,
                None,
                i64::MAX
            )
            .await
            .is_err()
    );
    let pinned = store.get(&path).await?;
    store.delete(&path).await?;
    assert!(matches!(
        store.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert_eq!(
        pinned.bytes().await?,
        replacement,
        "deleting the current name must not truncate an active read"
    );
    let mut tx = physical::begin(backend).await?;
    q::test_expire_protection(old.object.version)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    assert!(
        control
            .plan_read(
                namespace,
                &format!("nested/{path}"),
                Some(old.object.version),
                Some(&old.lease),
                None,
                None,
                i64::MAX
            )
            .await
            .is_err(),
        "a stale lease must not revive an unprotected retired object"
    );
    Ok(())
}

async fn chunk_publication(
    backend: &Backend,
    control: std::sync::Arc<crate::storage::native::control::Control>,
    pool: std::sync::Arc<crate::storage::native::transport::Pool>,
    namespace: Uuid,
    cluster: Uuid,
) -> Result<()> {
    use crate::{
        authorization::{Permit, Principal},
        model::{
            AdministrationStore, ChunkStore, StreamStore, administration::ProjectInput,
            chunk::ChunkCreate,
        },
        storage::native::control::WriteRequest,
    };
    use bytes::Bytes;
    use futures_util::StreamExt;
    let project = backend
        .save_project(
            &Principal::Local,
            None,
            ProjectInput {
                name: format!("native-chunks-{}", Uuid::now_v7()),
                description: String::new(),
                allow_bucket_create: true,
            },
        )
        .await?;
    let mut tx = physical::begin(backend).await?;
    let bucket = q::test_bucket(&mut tx, project.id, namespace).await?;
    tx.commit().await?;
    let stream = StreamStore::create(
        backend,
        crate::model::StreamCreate {
            authority: Permit::system(),
            bucket,
            key: "chunks".into(),
            purpose: crate::model::StreamPurpose::Internal,
            metadata: serde_json::json!({}),
            public: false,
            completion: None,
            owner: None,
        },
    )
    .await?;
    let mut rows = Vec::new();
    let mut frames = Vec::new();
    for index in 0..2 {
        let raw = vec![index + 61; 128 * 1024];
        let (mut chunk, reused) = backend
            .allocate(ChunkCreate {
                storage: namespace,
                stream,
                hash: *blake3::hash(&raw).as_bytes(),
                raw_size: raw.len() as i32,
                algorithm: "none".into(),
                key: None,
            })
            .await?;
        assert!(!reused);
        chunk.stored_size = Some(chunk.raw_size);
        let encoded = Bytes::from(crate::content::codec::seal(
            &chunk,
            raw.clone(),
            &std::collections::BTreeMap::new(),
        )?);
        backend.start_upload(stream, &chunk).await?;
        let path = crate::storage::location::Layout::S3
            .chunk(chunk.address())
            .to_string();
        let object = control
            .reserve(WriteRequest {
                namespace,
                cluster,
                path: &path,
                owner: chunk.address(),
                condition: Condition::Create,
                area: Area::Blocks,
                total: encoded.len() as u64,
                region: None,
            })
            .await?;
        let operation = object.payload.intent.operation;
        let plan = control.write_window(operation, 0, 1).await?;
        for (copy, piece) in plan.into_iter().enumerate() {
            let receipt = pool
                .write(
                    piece.destination.context("chunk storage unavailable")?,
                    futures_util::stream::iter([Ok(encoded.clone())]).boxed(),
                    control.as_ref(),
                )
                .await?;
            control.accept(operation, &[receipt]).await?;
            if copy == 0 {
                assert!(
                    backend
                        .complete(stream, i64::from(index) * i64::from(chunk.raw_size), &chunk)
                        .await
                        .is_err(),
                    "a missing required mirror must keep the business chunk unpublished"
                );
                assert!(backend.ready_chunk(chunk.id).await?.is_none());
                assert!(control.open(namespace, &path, None).await.is_err());
            }
        }
        if index == 1 {
            assert!(
                backend.complete(stream, 0, &chunk).await.is_err(),
                "a conflicting extent must roll back physical publication"
            );
            assert!(control.open(namespace, &path, None).await.is_err());
        }
        backend
            .complete(stream, i64::from(index) * i64::from(chunk.raw_size), &chunk)
            .await?;
        assert!(backend.ready_chunk(chunk.id).await?.is_some());
        assert_eq!(
            control
                .open(namespace, &path, None)
                .await?
                .payload
                .intent
                .operation,
            operation
        );
        rows.push(crate::model::Mapping {
            chunk,
            offset_bytes: i64::from(index) * 128 * 1024,
            pack_id: None,
        });
        frames.push(encoded);
    }
    rewrite_publication(
        backend,
        &control,
        &pool,
        namespace,
        cluster,
        &mut rows[0],
        &mut frames[0],
    )
    .await?;
    pack_publication(backend, &control, &pool, namespace, cluster, &rows, &frames).await?;
    Ok(())
}

async fn finish_task(backend: &Backend, task: crate::model::task::Execution) -> Result<()> {
    crate::model::TaskControlStore::complete(backend, task).await?;
    crate::model::TaskExecutionStore::finish(
        backend,
        crate::model::task::TaskFinish {
            execution: task,
            error: None,
        },
    )
    .await
}

async fn rewrite_publication(
    backend: &Backend,
    control: &crate::storage::native::control::Control,
    pool: &crate::storage::native::transport::Pool,
    namespace: Uuid,
    cluster: Uuid,
    row: &mut crate::model::Mapping,
    encoded: &mut bytes::Bytes,
) -> Result<()> {
    use crate::{
        model::{PackingStore, PhysicalSource, PhysicalStore},
        storage::native::control::WriteRequest,
    };
    use futures_util::StreamExt;
    let mut tx = physical::begin(backend).await?;
    let task = q::test_execution(&mut tx, "rewrite").await?;
    tx.commit().await?;
    backend
        .pin_pack_inputs(task, std::slice::from_ref(row))
        .await?;
    let mut target = row.chunk.clone();
    target.location_id = Uuid::now_v7();
    target.encoding_id = backend.prepare_encoding(&target, task).await?;
    if let crate::model::Id::Uuid(id) = target.encoding_id {
        target.location_id = id;
    }
    backend.encoding_metadata(&target).await?;
    let raw = crate::content::codec::open(
        &row.chunk,
        encoded.to_vec(),
        &std::collections::BTreeMap::new(),
    )?;
    let bytes = bytes::Bytes::from(crate::content::codec::seal(
        &target,
        raw,
        &std::collections::BTreeMap::new(),
    )?);
    let path = crate::storage::location::Layout::S3
        .chunk(target.address())
        .to_string();
    let object = control
        .reserve(WriteRequest {
            namespace,
            cluster,
            path: &path,
            owner: target.address(),
            condition: Condition::Create,
            area: Area::Blocks,
            total: bytes.len() as u64,
            region: None,
        })
        .await?;
    for (copy, piece) in control
        .write_window(object.payload.intent.operation, 0, 1)
        .await?
        .into_iter()
        .enumerate()
    {
        let receipt = pool
            .write(
                piece
                    .destination
                    .context("rewrite destination unavailable")?,
                futures_util::stream::iter([Ok(bytes.clone())]).boxed(),
                control,
            )
            .await?;
        control
            .accept(object.payload.intent.operation, &[receipt])
            .await?;
        if copy == 0 {
            assert!(
                backend
                    .commit_rewrite_chunk(task.id, task.token, &row.chunk, &target)
                    .await
                    .is_err()
            );
            assert!(control.open(namespace, &path, None).await.is_err());
            assert!(
                matches!(backend.source(row.chunk.id, false).await?, PhysicalSource::Chunk(value) if value.address() == row.chunk.address())
            );
        }
    }
    backend
        .commit_rewrite_chunk(task.id, task.token, &row.chunk, &target)
        .await?;
    assert!(control.open(namespace, &path, None).await.is_ok());
    let PhysicalSource::Chunk(current) = backend.source(row.chunk.id, false).await? else {
        anyhow::bail!("rewrite did not publish a chunk");
    };
    assert_eq!(current.address(), target.address());
    row.chunk = current;
    *encoded = bytes;
    finish_task(backend, task).await
}

async fn pack_publication(
    backend: &Backend,
    control: &crate::storage::native::control::Control,
    pool: &crate::storage::native::transport::Pool,
    namespace: Uuid,
    cluster: Uuid,
    rows: &[crate::model::Mapping],
    frames: &[bytes::Bytes],
) -> Result<()> {
    use crate::{
        content::{codec, pack},
        model::{PackingStore, PhysicalSource, PhysicalStore, packing::PackOutput},
        storage::native::control::WriteRequest,
    };
    use futures_util::StreamExt;
    let mut tx = physical::begin(backend).await?;
    let task = q::test_execution(&mut tx, "pack").await?;
    tx.commit().await?;
    backend.pin_pack_inputs(task, rows).await?;
    let raw = rows.iter().map(|row| i64::from(row.chunk.raw_size)).sum();
    let mut packed = backend
        .prepare_pack(namespace, "none", None, raw, rows.len() as i32, task)
        .await?;
    let entries = pack::frame::index(rows.iter().map(|row| &row.chunk))?;
    let mut bytes = pack::frame::HEADER.to_vec();
    for frame in frames {
        bytes.extend_from_slice(&frame[codec::FILE_HEADER.len()..]);
    }
    packed.stored_size = Some(bytes.len() as i64);
    packed.digest = Some(blake3::hash(&bytes).as_bytes().to_vec());
    backend.pack_metadata(&packed).await?;
    backend
        .insert_pack_members(packed.id, rows, &entries)
        .await?;
    let bytes = bytes::Bytes::from(bytes);
    let path = crate::storage::location::Layout::S3
        .pack(packed.address())
        .to_string();
    let object = control
        .reserve(WriteRequest {
            namespace,
            cluster,
            path: &path,
            owner: packed.address(),
            condition: Condition::Create,
            area: Area::Blocks,
            total: bytes.len() as u64,
            region: None,
        })
        .await?;
    let outputs = [PackOutput {
        entries: Vec::new(),
        chunks: rows.iter().map(|row| row.chunk.id).collect(),
        pack: Some(packed.clone()),
        independent: None,
    }];
    for (copy, piece) in control
        .write_window(object.payload.intent.operation, 0, 1)
        .await?
        .into_iter()
        .enumerate()
    {
        let receipt = pool
            .write(
                piece.destination.context("pack destination unavailable")?,
                futures_util::stream::iter([Ok(bytes.clone())]).boxed(),
                control,
            )
            .await?;
        control
            .accept(object.payload.intent.operation, &[receipt])
            .await?;
        if copy == 0 {
            assert!(
                backend
                    .commit_pack_outputs(
                        task,
                        rows,
                        &outputs,
                        false,
                        &pack::Config::default(),
                        60.0
                    )
                    .await
                    .is_err()
            );
            assert!(control.open(namespace, &path, None).await.is_err());
            assert!(matches!(
                backend.source(rows[0].chunk.id, false).await?,
                PhysicalSource::Chunk(_)
            ));
        }
    }
    backend
        .commit_pack_outputs(task, rows, &outputs, false, &pack::Config::default(), 60.0)
        .await?;
    assert!(control.open(namespace, &path, None).await.is_ok());
    for row in rows {
        assert!(
            matches!(backend.source(row.chunk.id, false).await?, PhysicalSource::Pack(value) if value.id == packed.id)
        );
    }
    finish_task(backend, task).await
}

async fn marker_publication(
    control: std::sync::Arc<crate::storage::native::control::Control>,
    pool: std::sync::Arc<crate::storage::native::transport::Pool>,
    namespace: Uuid,
    cluster: Uuid,
    value: &serde_json::Value,
) -> Result<()> {
    use object_store::{ObjectStore, PutMode, path::Path};
    let store = crate::storage::native::store::Store {
        authority: crate::storage::native::store::Authority::Control(control),
        pool,
        namespace,
        cluster,
        prefix: Path::default(),
        region: None,
    };
    assert!(
        crate::storage::marker::Document::read(&store)
            .await?
            .is_none()
    );
    let body = bytes::Bytes::from(serde_json::to_vec(value)?);
    store
        .put_opts(
            &Path::from("mokyu.json"),
            body.clone().into(),
            PutMode::Create.into(),
        )
        .await?;
    crate::storage::marker::verify_store(&store, value).await?;
    assert!(
        store
            .put_opts(
                &Path::from("mokyu.json"),
                body.into(),
                PutMode::Overwrite.into()
            )
            .await
            .is_err()
    );
    let doc = crate::storage::marker::Document::read(&store)
        .await?
        .context("published native marker is missing")?;
    let (e_tag, version) = doc.revision();
    let revision = object_store::UpdateVersion { e_tag, version };
    let mut updated = value.clone();
    updated["created_by"] = "native-test".into();
    store
        .put_opts(
            &Path::from("mokyu.json"),
            bytes::Bytes::from(serde_json::to_vec(&updated)?).into(),
            PutMode::Update(revision.clone()).into(),
        )
        .await?;
    assert!(
        store
            .put_opts(
                &Path::from("mokyu.json"),
                bytes::Bytes::from(serde_json::to_vec(value)?).into(),
                PutMode::Update(revision).into()
            )
            .await
            .is_err()
    );
    crate::storage::marker::verify_store(&store, &updated).await?;
    Ok(())
}

async fn raw_publication(
    backend: &Backend,
    control: std::sync::Arc<crate::storage::native::control::Control>,
    pool: std::sync::Arc<crate::storage::native::transport::Pool>,
    namespace: Uuid,
    cluster: Uuid,
) -> Result<()> {
    use crate::{
        authorization::{Permit, Principal},
        model::{
            AdministrationStore, StreamStore,
            administration::ProjectInput,
            raw::{PageWrite, RawStore},
        },
        storage::native::control::WriteRequest,
    };
    use futures_util::StreamExt;
    use md5::{Digest, Md5};
    let project = backend
        .save_project(
            &Principal::Local,
            None,
            ProjectInput {
                name: format!("native-raw-{}", Uuid::now_v7()),
                description: String::new(),
                allow_bucket_create: true,
            },
        )
        .await?;
    let mut tx = physical::begin(backend).await?;
    q::test_raw_profile(namespace).execute(&mut *tx).await?;
    let bucket = q::test_bucket(&mut tx, project.id, namespace).await?;
    tx.commit().await?;
    let stream = StreamStore::create(
        backend,
        crate::model::StreamCreate {
            authority: Permit::system(),
            bucket,
            key: "raw".into(),
            purpose: crate::model::StreamPurpose::Internal,
            metadata: serde_json::json!({}),
            public: false,
            completion: None,
            owner: None,
        },
    )
    .await?;
    let (source, location) = RawStore::begin(backend, stream, namespace, 1, "durable").await?;
    let bytes = bytes::Bytes::from(vec![71; 1024 * 1024 + 17]);
    let digest = Md5::digest(&bytes);
    let mut pages = PageWrite::new(source);
    pages.feed(backend, &bytes).await?;
    pages.finish(backend, bytes.len() as i64, &digest).await?;
    let path = crate::storage::location::Layout::S3
        .object(location)
        .to_string();
    let object = control
        .reserve(WriteRequest {
            namespace,
            cluster,
            path: &path,
            owner: location,
            condition: Condition::Create,
            area: Area::Blocks,
            total: bytes.len() as u64,
            region: None,
        })
        .await?;
    for (index, piece) in control
        .write_window(object.payload.intent.operation, 0, 1)
        .await?
        .into_iter()
        .enumerate()
    {
        let receipt = pool
            .write(
                piece.destination.context("raw storage unavailable")?,
                futures_util::stream::iter([Ok(bytes.clone())]).boxed(),
                control.as_ref(),
            )
            .await?;
        control
            .accept(object.payload.intent.operation, &[receipt])
            .await?;
        if index == 0 {
            assert!(
                RawStore::finish(backend, stream, source, bytes.len() as i64, &digest)
                    .await
                    .is_err()
            );
            assert!(control.open(namespace, &path, None).await.is_err());
        }
    }
    RawStore::finish(backend, stream, source, bytes.len() as i64, &digest).await?;
    assert_eq!(
        RawStore::current(backend, source)
            .await?
            .context("raw source is missing")?
            .medium,
        "shared"
    );
    assert_eq!(
        control
            .open(namespace, &path, None)
            .await?
            .payload
            .intent
            .operation,
        object.payload.intent.operation
    );
    Ok(())
}

async fn work_access(
    backend: &Backend,
    control: &crate::storage::native::control::Control,
    pool: &crate::storage::native::transport::Pool,
    key: &crate::content::codec::Key,
    namespace: Uuid,
) -> Result<()> {
    use crate::{
        authorization::Principal,
        model::{
            AdministrationStore,
            cluster::NodeSessions,
            node_work::{Deletion, Physical, Plan, RawHash, UploadFile},
            session::{Open, Run},
        },
    };
    use access::{Maintenance, MaintenanceAction};
    use futures_util::StreamExt;
    let node = backend
        .provision_node(
            &Principal::Local,
            &format!("worker-{}", Uuid::now_v7()),
            "worker",
            key,
        )
        .await?;
    let run = backend
        .open_session(
            &node,
            key,
            Open {
                boot: Uuid::now_v7(),
                connection: Uuid::now_v7(),
                previous_epoch: 0,
            },
        )
        .await?
        .run;
    let bytes = bytes::Bytes::from_static(b"native work");
    let source = crate::model::raw::RawSource {
        id: crate::model::Id::Integer(1),
        location_id: Uuid::now_v7(),
        address: None,
        home_storage_id: None,
        storage_id: namespace,
        size: bytes.len() as i64,
        digest: Some(vec![0; 16]),
        state: "ready".into(),
        medium: "shared".into(),
        pending_upload: false,
        revision: 1,
        quarantine_code: None,
    };
    let mut tx = physical::begin(backend).await?;
    let plan = Plan::UploadRaw {
        storage: namespace,
        items: vec![UploadFile {
            task: Uuid::now_v7(),
            source: source.clone(),
            hashing: false,
        }],
    };
    let (operation, _) = q::test_work(&mut tx, &node, run, &plan).await?;
    tx.commit().await?;
    let path = crate::storage::location::Layout::S3
        .object(source.address())
        .to_string();
    let mut input = access::Upload {
        operation,
        work: true,
        batch: None,
        storage: namespace,
        path: path.clone(),
        size: bytes.len() as u64,
    };
    let (intent, _) = control.node_upload(&node, run, &input, None).await?;
    input.size += 1;
    assert!(control.node_upload(&node, run, &input, None).await.is_err());
    input.size -= 1;
    input.work = false;
    assert!(control.node_upload(&node, run, &input, None).await.is_err());
    input.work = true;
    assert!(
        control
            .node_upload(
                &node,
                Run {
                    fence: Uuid::now_v7(),
                    ..run
                },
                &input,
                None
            )
            .await
            .is_err()
    );
    let foreign = access::Upload {
        path: format!("{path}-foreign"),
        ..input.clone()
    };
    assert!(
        control
            .node_upload(&node, run, &foreign, None)
            .await
            .is_err()
    );
    for piece in control
        .node_write_window(&node, run, &input, intent.operation, 0, 1, 0)
        .await?
    {
        let receipt = pool
            .write(
                piece.destination.context("work destination unavailable")?,
                futures_util::stream::iter([Ok(bytes.clone())]).boxed(),
                control,
            )
            .await?;
        control
            .node_accept(&node, run, &input, intent.operation, &[receipt])
            .await?;
    }
    control
        .publish(namespace, intent.payload, source.address())
        .await?;
    let mut read = access::Read {
        windows: Vec::new(),
        upload: None,
        work: Some(operation),
        storage: namespace,
        path: path.clone(),
        range: Some(0..bytes.len() as u64),
    };
    assert!(
        backend
            .authorize_read(&node, key, run, &read)
            .await
            .is_err()
    );
    let mut tx = physical::begin(backend).await?;
    let hash = Plan::HashRaw {
        storage: namespace,
        items: vec![RawHash {
            source: source.clone(),
            hash: None,
        }],
    };
    let (reading, task) = q::test_work(&mut tx, &node, run, &hash).await?;
    tx.commit().await?;
    read.work = Some(reading);
    let plan = control
        .node_read(
            &node,
            run,
            &access::ReadRequest {
                access: read.clone(),
                version: None,
                lease: None,
                region: None,
            },
        )
        .await?;
    let grant = &plan.parts[0].copies[0].authorization;
    control.renew_read(&node, run, &read, grant).await?;
    let request = Maintenance {
        operation: reading,
        storage: namespace,
        action: MaintenanceAction::Delete { path: path.clone() },
    };
    assert!(backend.maintain(&node, key, run, &request).await.is_err());
    let request = Maintenance {
        action: MaintenanceAction::List { after: None },
        ..request
    };
    assert!(backend.maintain(&node, key, run, &request).await.is_err());
    let mut tx = physical::begin(backend).await?;
    q::test_pause_task(task).execute(&mut *tx).await?;
    tx.commit().await?;
    assert!(
        backend
            .authorize_read(&node, key, run, &read)
            .await
            .is_err()
    );
    assert!(control.renew_read(&node, run, &read, grant).await.is_err());
    let scan = Plan::SweepScan {
        storage: namespace,
        after: None,
        limit: 2,
        bytes: None,
    };
    let mut tx = physical::begin(backend).await?;
    let (scanning, _) = q::test_work(&mut tx, &node, run, &scan).await?;
    tx.commit().await?;
    let mut request = Maintenance {
        operation: scanning,
        storage: namespace,
        action: MaintenanceAction::List { after: None },
    };
    let page = backend.maintain(&node, key, run, &request).await?;
    assert_eq!(page.len(), 2);
    assert!(page.windows(2).all(|pair| pair[0].path < pair[1].path));
    let after = page.last().unwrap().path.clone();
    request.action = MaintenanceAction::List {
        after: Some(after.clone()),
    };
    let page = backend.maintain(&node, key, run, &request).await?;
    assert!(page.iter().all(|item| item.path > after));
    request.storage = Uuid::now_v7();
    assert!(backend.maintain(&node, key, run, &request).await.is_err());
    let collect = Plan::Collect {
        storage: namespace,
        delete_before: chrono::Utc::now(),
        retention: 0,
        metadata_count: 0,
        items: vec![Deletion {
            source: Physical::Raw { upload: None },
            identity: source.id,
            location: source.address(),
            inspect_age: true,
        }],
    };
    let mut tx = physical::begin(backend).await?;
    let (deleting, _) = q::test_work(&mut tx, &node, run, &collect).await?;
    tx.commit().await?;
    let mut request = Maintenance {
        operation: deleting,
        storage: namespace,
        action: MaintenanceAction::Delete {
            path: "mokyu.json".into(),
        },
    };
    let physical = control
        .open(namespace, &path, None)
        .await?
        .payload
        .intent
        .operation;
    let collect_request = Maintenance {
        action: MaintenanceAction::Collect { payload: physical },
        ..request.clone()
    };
    assert!(
        backend
            .maintain(&node, key, run, &collect_request)
            .await
            .is_err()
    );
    assert!(backend.maintain(&node, key, run, &request).await.is_err());
    request.action = MaintenanceAction::Delete { path: path.clone() };
    backend.maintain(&node, key, run, &request).await?;
    assert!(control.open(namespace, &path, None).await.is_err());
    backend.maintain(&node, key, run, &request).await?;
    let plan = Plan::Collect {
        storage: namespace,
        delete_before: chrono::Utc::now(),
        retention: 0,
        metadata_count: 0,
        items: vec![Deletion {
            source: Physical::Native {
                operation: physical,
            },
            identity: physical.into(),
            location: physical,
            inspect_age: false,
        }],
    };
    let mut tx = super::super::queries::storage_payload::begin(backend).await?;
    let (collecting, collection_task) = q::test_work(&mut tx, &node, run, &plan).await?;
    tx.commit().await?;
    let mut collect_request = Maintenance {
        operation: collecting,
        ..collect_request
    };
    assert!(
        backend
            .maintain(&node, key, run, &collect_request)
            .await?
            .is_empty()
    );
    collect_request.action = MaintenanceAction::Collect {
        payload: Uuid::now_v7(),
    };
    assert!(
        backend
            .maintain(&node, key, run, &collect_request)
            .await
            .is_err()
    );
    collect_request.action = MaintenanceAction::Collect { payload: physical };
    let mut tx = super::super::queries::storage_payload::begin(backend).await?;
    q::test_pause_task(collection_task)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    assert!(
        backend
            .maintain(&node, key, run, &collect_request)
            .await
            .is_err()
    );
    backend.close_session(&node, key, run).await?;
    assert!(backend.maintain(&node, key, run, &request).await.is_err());
    Ok(())
}

async fn read_access(
    backend: &Backend,
    control: &crate::storage::native::control::Control,
    key: &crate::content::codec::Key,
    namespace: Uuid,
    location: Uuid,
) -> Result<()> {
    use super::super::queries::{self, read_grants};
    use crate::{
        authorization::{Permit, Principal},
        model::{
            AdministrationStore, StreamStore,
            administration::ProjectInput,
            cluster::NodeSessions,
            node_read as read,
            session::{Open, Run},
        },
    };
    let node = backend
        .provision_node(
            &Principal::Local,
            &format!("reader-{}", Uuid::now_v7()),
            "data",
            key,
        )
        .await?;
    let run = backend
        .open_session(
            &node,
            key,
            Open {
                boot: Uuid::now_v7(),
                connection: Uuid::now_v7(),
                previous_epoch: 0,
            },
        )
        .await?
        .run;
    let project = backend
        .save_project(
            &Principal::Local,
            None,
            ProjectInput {
                name: format!("native-{}", Uuid::now_v7()),
                description: String::new(),
                allow_bucket_create: true,
            },
        )
        .await?;
    let mut tx = physical::begin(backend).await?;
    let bucket = q::test_bucket(&mut tx, project.id, namespace).await?;
    tx.commit().await?;
    let stream = StreamStore::create(
        backend,
        crate::model::StreamCreate {
            authority: Permit::system(),
            bucket,
            key: "window".into(),
            purpose: crate::model::StreamPurpose::Internal,
            metadata: serde_json::json!({}),
            public: false,
            completion: None,
            owner: None,
        },
    )
    .await?;
    let page = crate::content::raw_pages::PAGE as i64;
    let mapping = read::Mapping {
        stream,
        revision: 1,
        start: 0,
        end: 1,
        segments: vec![read::Segment {
            page_revision: None,
            offset: 0,
            length: 1,
            source_offset: page + 31,
            source: read::Source::Raw {
                source: crate::model::raw::RawSource {
                    id: stream,
                    location_id: location,
                    address: None,
                    home_storage_id: None,
                    storage_id: namespace,
                    size: 3 * page,
                    digest: Some(vec![0; 16]),
                    state: "ready".into(),
                    medium: "shared".into(),
                    pending_upload: false,
                    revision: 1,
                    quarantine_code: None,
                },
            },
        }],
    };
    let input = read::Query {
        restore_version: false,
        version_id: None,
        run,
        operation: Uuid::now_v7(),
        route: crate::model::ReadRoute::Bucket {
            name: "unused".into(),
            key: "window".into(),
            alias: None,
        },
        access: None,
        head: false,
        web: false,
        mapping: true,
        protection: true,
        policy: false,
        headers: Default::default(),
        overrides: Default::default(),
        cached: Vec::new(),
        max_items: read::MAX_ITEMS,
        lanes: 1,
        permits: Default::default(),
        max_bytes: read::TARGET_BYTES,
        body: None,
    };
    let mut tx = physical::begin(backend).await?;
    let stored = queries::replication_import::stream(stream)
        .fetch_one(&mut *tx)
        .await?;
    let lease = read_grants::create(&mut tx, &node, &input, &stored, 0, 1, &[0; 32]).await?;
    let window = read_grants::insert_window(&mut tx, lease, &mapping).await?;
    tx.commit().await?;
    let mut request = access::Read {
        windows: vec![window],
        upload: None,
        work: None,
        storage: namespace,
        path: crate::storage::location::Layout::S3
            .object(location)
            .to_string(),
        range: Some(page as u64..2 * page as u64),
    };
    assert!(
        backend.authorize_read(&node, key, run, &request).await?
            > chrono::Utc::now().timestamp_micros()
    );
    let planned = control
        .node_read(
            &node,
            run,
            &access::ReadRequest {
                access: request.clone(),
                version: None,
                lease: None,
                region: None,
            },
        )
        .await?;
    assert_eq!(planned.parts.len(), 2);
    let signed = &planned.parts[0].copies[0].authorization;
    control.renew_read(&node, run, &request, signed).await?;
    request.range = Some(0..1);
    assert!(
        backend
            .authorize_read(&node, key, run, &request)
            .await
            .is_err()
    );
    assert!(
        control
            .renew_read(&node, run, &request, signed)
            .await
            .is_err()
    );
    request.range = None;
    backend.authorize_read(&node, key, run, &request).await?;
    let expected = request.path.clone();
    request.path = "mokyu.json".into();
    let marker = control
        .node_read(
            &node,
            run,
            &access::ReadRequest {
                access: request.clone(),
                version: None,
                lease: None,
                region: None,
            },
        )
        .await?;
    assert!(
        marker.object.size > 0 && marker.object.size <= crate::storage::marker::MAX_BYTES as u64
    );
    request.path = expected.clone();
    request.path.push_str("-foreign");
    assert!(
        backend
            .authorize_read(&node, key, run, &request)
            .await
            .is_err()
    );
    request.path = expected;
    assert!(
        backend
            .authorize_read(
                &node,
                key,
                Run {
                    boot: Uuid::now_v7(),
                    ..run
                },
                &request
            )
            .await
            .is_err()
    );
    let stranger = backend
        .provision_node(
            &Principal::Local,
            &format!("stranger-{}", Uuid::now_v7()),
            "data",
            key,
        )
        .await?;
    let other = backend
        .open_session(
            &stranger,
            key,
            Open {
                boot: Uuid::now_v7(),
                connection: Uuid::now_v7(),
                previous_epoch: 0,
            },
        )
        .await?
        .run;
    assert!(
        backend
            .authorize_read(&stranger, key, other, &request)
            .await
            .is_err()
    );
    let mut tx = physical::begin(backend).await?;
    q::test_expire(lease).execute(&mut *tx).await?;
    tx.commit().await?;
    assert!(
        backend
            .authorize_read(&node, key, run, &request)
            .await
            .is_err()
    );
    Ok(())
}
