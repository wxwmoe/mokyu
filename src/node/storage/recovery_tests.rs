use super::{
    files::Files,
    fragments,
    inventory::{EvidenceKind, EvidenceScan, Scan},
    recovery,
};
use crate::{
    content::{codec::Key, fragment::Area},
    model::{
        journal::record::{Attributes, Change, Operation},
        storage_cluster::{
            Acknowledgement, Group, Layout, Manifest, Member, Registration, Topology,
        },
        storage_payload::Intent,
        storage_recovery::{
            self as model, CatalogEntry, Cursor, Part, Physical, Proof, Publication,
            RecoveryCatalog,
        },
    },
    storage::journal::{
        self, Format, FrameType, Sequence,
        batch::{self, Decoder, Record},
    },
};
use anyhow::Result;
use bytes::Bytes;
use fragments::tests::{allowed, fixture, header, input};
use uuid::Uuid;

async fn scan(
    catalog: &dyn RecoveryCatalog,
    job: Uuid,
    files: &Files,
    anchor: &Manifest,
    namespace: Uuid,
) -> Result<()> {
    for kind in [EvidenceKind::Layouts, EvidenceKind::History] {
        let mut cursor = Cursor {
            media: files.media.media_id,
            boot: Uuid::now_v7(),
            fence: Uuid::now_v7(),
            manifest: anchor.digest()?,
            scan: Uuid::now_v7(),
            scope: match kind {
                EvidenceKind::Layouts => "layouts",
                EvidenceKind::History => "history",
            }
            .into(),
            page: 0,
            prefix: 0,
            complete: false,
        };
        let mut scan = EvidenceScan::open(
            files,
            kind,
            match kind {
                EvidenceKind::Layouts => anchor.cluster_id,
                EvidenceKind::History => namespace,
            },
            allowed(),
        )?;
        let mut revision = 0;
        loop {
            let page = scan.page(cursor.page)?;
            assert_eq!(
                serde_json::to_vec(&page)?,
                serde_json::to_vec(&scan.page(cursor.page)?)?
            );
            cursor.complete = page.complete;
            let mut entries = Vec::new();
            for entry in page.entries.into_iter().rev() {
                if entry.pending || entry.error.is_some() {
                    continue;
                }
                if let Some(value) = entry.proof {
                    entries.push(CatalogEntry::Proof(value));
                }
                if let Some(value) = entry.manifest {
                    entries.push(CatalogEntry::Manifest(value));
                }
            }
            let next = catalog
                .catalog_checkpoint(job, revision, &cursor, &entries)
                .await?;
            assert_eq!(
                catalog
                    .catalog_checkpoint(job, revision, &cursor, &entries)
                    .await?,
                next
            );
            revision = next;
            if page.complete {
                break;
            }
            cursor.page += 1;
        }
    }
    let mut scan = Scan::open(files, Area::Recovery, 0, allowed())?;
    let mut cursor = Cursor {
        media: files.media.media_id,
        boot: Uuid::now_v7(),
        fence: Uuid::now_v7(),
        manifest: anchor.digest()?,
        scan: Uuid::now_v7(),
        scope: "recovery".into(),
        page: 0,
        prefix: 0,
        complete: false,
    };
    let mut revision = 0;
    loop {
        let page = scan.page(cursor.page)?;
        cursor.complete = page.complete;
        cursor.prefix = page.restart_prefix;
        let entries: Vec<_> = page
            .entries
            .into_iter()
            .rev()
            .filter(|e| !e.pending && e.error.is_none())
            .filter_map(|e| e.receipt)
            .map(CatalogEntry::Fragment)
            .collect();
        revision = catalog
            .catalog_checkpoint(job, revision, &cursor, &entries)
            .await?;
        if page.complete {
            break;
        }
        cursor.page += 1;
    }
    Ok(())
}

pub(crate) async fn contract(catalog: &dyn RecoveryCatalog) -> Result<()> {
    let (_directory, instance, budget, files) = fixture("32MiB").await?;
    let key = Key::new("aes-256-gcm", [13; 32])?;
    let namespace = Uuid::now_v7();
    let cluster = Uuid::now_v7();
    let group = Uuid::now_v7();
    let member = |media, name: &str| Member {
        media_id: media,
        node_id: Uuid::now_v7(),
        state: "available".into(),
        topology: Topology {
            url: "https://storage.example".into(),
            region: None,
            storage_group: name.into(),
            purpose: "primary".into(),
        },
    };
    let first = Manifest {
        format_version: 1,
        record_id: Uuid::now_v7(),
        cluster_id: cluster,
        deployment_id: Uuid::now_v7(),
        publication_key: model::public_key(&key, cluster)?,
        takeover_epoch: 1,
        revision: 1,
        previous: None,
        members: vec![
            member(Uuid::now_v7(), "lost"),
            member(files.media.media_id, "surviving"),
        ],
        groups: vec![],
    };
    let mut anchor = first.clone();
    anchor.record_id = Uuid::now_v7();
    anchor.revision = 2;
    anchor.previous = Some(first.digest()?);
    anchor.groups.push(Group {
        id: group,
        layout: Layout::default(),
        members: anchor.members.iter().map(|m| m.media_id).collect(),
    });
    files
        .apply(
            Registration {
                manifest: Some(anchor.clone()),
                history: vec![first, anchor.clone()],
            },
            anchor.deployment_id,
        )
        .await?;
    let format = Format {
        storage: namespace,
        journal: Uuid::now_v7(),
        epoch: Uuid::now_v7(),
        key: &key,
        encrypted: true,
    };
    let bucket = Uuid::now_v7();
    let logical = Uuid::now_v7();
    let mut previous = None;
    let mut last_proof = None;
    let mut missing = None;
    for (index, sequence) in [1, 7, 9, 20, 25, 30].into_iter().enumerate() {
        let payload = Uuid::now_v7();
        let deleted = sequence == 30;
        let record = Record::Change {
            value: Change {
                bucket,
                name: "media".into(),
                generation: 1,
                seq: sequence as i64,
                operation: if deleted {
                    Operation::Delete
                } else {
                    Operation::Put
                },
                key: Some("object".into()),
                attributes: (!deleted).then(|| Attributes {
                    size: 0,
                    etag: format!("revision-{sequence}"),
                    metadata: serde_json::json!({}),
                    checksums: serde_json::json!({}),
                    payload: "raw".into(),
                    modified_us: sequence as i64,
                }),
                ranges: vec![],
            },
            range_count: 0,
            range_digest: [0; 32],
        };
        let frame = batch::frames(&[record], 4096, true)?.remove(0);
        let path = format!("metadata/changes/{}/{sequence}", format.epoch.simple());
        let bytes = journal::recovery::encode(
            &format,
            &path,
            &journal::recovery::Envelope {
                payload,
                revision: 1,
                batch: Uuid::now_v7(),
                kind: if index < 2 {
                    FrameType::Snapshot
                } else {
                    FrameType::Changes
                },
                sequence: Sequence {
                    first: sequence,
                    last: sequence,
                    previous: [0; 32],
                },
            },
            &serde_json::to_vec(&vec![Physical::State {
                sequence,
                operation: logical,
                published: !deleted,
            }])?,
            &frame.bytes,
        )?;
        let mut planned = header(&files, bytes.len() as u64);
        planned.area = Area::Recovery;
        planned.namespace = namespace;
        planned.payload = payload;
        planned.cluster = cluster;
        planned.group = group;
        planned.copy = 1;
        planned.layout_revision = 2;
        let intent = Intent {
            operation: planned.operation,
            namespace,
            payload,
            revision: 1,
            cluster,
            layout_revision: 2,
            takeover_epoch: 1,
            group,
            layout: Layout::default(),
            area: Area::Recovery,
            total: bytes.len() as u64,
            acknowledgement: Acknowledgement::All,
        };
        let part = Part {
            intent,
            path,
            digest: *blake3::hash(&bytes).as_bytes(),
        };
        part.inspect(&bytes)?;
        let receipt =
            fragments::write(&files, planned, input(Bytes::from(bytes), true), allowed()).await?;
        let proof = Proof::sign(
            Publication {
                format_version: 1,
                id: Uuid::now_v7(),
                namespace,
                cluster,
                manifest: anchor.digest()?,
                takeover_epoch: 1,
                revision: index as u64 + 1,
                previous,
                through: sequence,
                parts: vec![part],
                retired: vec![],
            },
            &key,
            &anchor,
        )?;
        previous = Some(recovery::commit(&files, proof.clone(), anchor.clone(), allowed()).await?);
        if index == 2 {
            missing = Some((
                files
                    .disk
                    .root
                    .join(receipt.header.area.path(receipt.header.fragment)),
                proof.publication.id,
            ));
        }
        last_proof = Some(proof);
    }
    let last_proof = last_proof.unwrap();
    let proof_path = recovery::history(&files.disk.root, namespace, last_proof.publication.id);
    std::fs::copy(&proof_path, fragments::temporary(&proof_path))?;
    std::fs::remove_file(
        files
            .disk
            .root
            .join("recovery/heads")
            .join(format!("{}.json", namespace.simple())),
    )?;
    let lock = files.lock.clone();
    drop(files);
    let files = Files::open(&instance, &budget, lock).await?;
    let job = Uuid::now_v7();
    catalog.catalog_open(job, namespace, &anchor).await?;
    scan(catalog, job, &files, &anchor, namespace).await?;
    let head = model::verify::chain(catalog, job, &anchor).await?;
    assert_eq!(head, last_proof);
    let mut current = Some(head);
    while let Some(proof) = current {
        for part in &proof.publication.parts {
            let receipt = model::verify::fragments(catalog, job, part, &anchor)
                .await?
                .remove(0);
            let mut stream =
                fragments::read(&files, receipt, 0, part.intent.total, allowed()).await?;
            let mut bytes = Vec::new();
            while let Some(value) = stream.recv().await {
                bytes.extend_from_slice(&value?);
            }
            let physical = model::verify::records(
                part,
                &bytes,
                proof.publication.through,
                None,
                &mut Decoder::default(),
            )?;
            assert_eq!(physical.len(), 1);
            let entries = model::verify::records(
                part,
                &bytes,
                proof.publication.through,
                Some(&format),
                &mut Decoder::default(),
            )?;
            assert_eq!(entries.len(), 2);
            catalog.catalog_stage(job, &entries).await?;
            catalog.catalog_stage(job, &entries).await?;
            assert!(
                model::verify::records(part, &bytes, 0, None, &mut Decoder::default()).is_err()
            );
            bytes[300] ^= 1;
            assert!(part.inspect(&bytes).is_err());
        }
        current = match &proof.publication.previous {
            Some(link) => match catalog
                .catalog_entry(job, 1, link.id, proof.publication.revision - 1)
                .await?
            {
                Some(CatalogEntry::Proof(value)) => Some(value),
                _ => panic!("missing predecessor"),
            },
            None => None,
        };
    }
    let states = catalog.catalog_page(job, 5, Some(logical), None).await?;
    assert_eq!(states.entries.len(), 6);
    assert!(matches!(
        states.entries.last(),
        Some(CatalogEntry::Physical(Physical::State {
            sequence: 30,
            published: false,
            ..
        }))
    ));
    let business = catalog.catalog_page(job, 6, None, None).await?;
    assert_eq!(business.entries.len(), 6);
    let latest = business
        .entries
        .into_iter()
        .filter_map(|entry| match entry {
            CatalogEntry::Business {
                record: Record::Change { value, .. },
                ..
            } => Some(value),
            _ => None,
        })
        .max_by_key(|v| v.seq)
        .unwrap();
    assert!(latest.operation == Operation::Delete);
    assert_eq!(latest.seq, 30);
    let (missing_path, missing_proof) = missing.unwrap();
    std::fs::remove_file(missing_path)?;
    let incomplete = Uuid::now_v7();
    catalog.catalog_open(incomplete, namespace, &anchor).await?;
    scan(catalog, incomplete, &files, &anchor, namespace).await?;
    model::verify::chain(catalog, incomplete, &anchor).await?;
    let page = catalog.catalog_page(incomplete, 1, None, None).await?;
    let proof = page.entries.into_iter().find(|entry| matches!(entry, CatalogEntry::Proof(value) if value.publication.id == missing_proof)).unwrap();
    let CatalogEntry::Proof(proof) = proof else {
        unreachable!()
    };
    assert!(
        model::verify::fragments(catalog, incomplete, &proof.publication.parts[0], &anchor)
            .await
            .is_err()
    );
    let path = recovery::history(&files.disk.root, namespace, proof.publication.id);
    std::fs::remove_file(path)?;
    let broken = Uuid::now_v7();
    catalog.catalog_open(broken, namespace, &anchor).await?;
    scan(catalog, broken, &files, &anchor, namespace).await?;
    assert!(
        model::verify::chain(catalog, broken, &anchor)
            .await
            .is_err()
    );
    Ok(())
}
