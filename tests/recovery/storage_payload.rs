use super::*;
use crate::content::fragment::{self, Area};
use crate::model::storage_cluster::{
    Acknowledgement, Cluster, FORMAT, Group, Layout, Manifest, Member, Policy, Topology,
};

pub(crate) async fn fixture() -> Result<(Backend, Cluster)> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let backend = q::test_backend().await?;
    let mut tx = q::begin(&backend).await?;
    let members: Vec<_> = (0..2)
        .map(|index| Member {
            media_id: Uuid::now_v7(),
            node_id: Uuid::now_v7(),
            state: "available".into(),
            topology: Topology {
                url: format!("https://storage-{index}.example"),
                region: None,
                storage_group: format!("host-{index}"),
                purpose: "primary".into(),
            },
        })
        .collect();
    let group = Group {
        replacing: None,
        id: Uuid::now_v7(),
        layout: Layout::default(),
        members: members.iter().map(|m| m.media_id).collect(),
    };
    let id = Uuid::now_v7();
    let cluster = Cluster {
        id,
        name: format!("payload-{id}"),
        revision: 1,
        policy: Policy::default(),
        state: "layout_enabled".into(),
        manifest: Manifest {
            format_version: FORMAT,
            record_id: Uuid::now_v7(),
            publication_key: [1; 32],
            cluster_id: id,
            deployment_id: super::super::queries::storage_cluster::deployment()
                .fetch_one(&mut *tx)
                .await?,
            takeover_epoch: 1,
            revision: 1,
            previous: None,
            members,
            groups: vec![group.clone()],
        },
    };
    super::super::queries::storage_cluster::insert(&cluster)
        .execute(&mut *tx)
        .await?;
    super::super::queries::storage_cluster::history(&cluster, &cluster.manifest.digest()?)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((backend, cluster))
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_PAYLOAD_CONFIG and an isolated database"]
async fn storage_collection_rotates_failed_batches() -> Result<()> {
    let (backend, cluster) = fixture().await?;
    let group = &cluster.manifest.groups[0];
    let namespace = Uuid::now_v7();
    let mut operations = Vec::new();
    for _ in 0..66 {
        let intent = Intent {
            operation: Uuid::now_v7(),
            namespace,
            payload: Uuid::now_v7(),
            revision: 1,
            cluster: cluster.id,
            layout_revision: 1,
            takeover_epoch: 1,
            group: group.id,
            layout: group.layout.clone(),
            members: Vec::new(),
            area: Area::Blocks,
            total: 1,
            acknowledgement: Acknowledgement::All,
        };
        let until = chrono::Utc::now().timestamp_micros() + 80_000_000;
        backend.prepare(&intent, until).await?;
        backend.write_window(intent.operation, 0, 1, until).await?;
        backend.retire(intent.operation).await?;
        let mut tx = q::begin(&backend).await?;
        super::super::queries::storage_journal::test_expire(intent.operation)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        operations.push(intent.operation);
    }
    operations.sort();
    assert_eq!(
        backend.collect_candidates(namespace, 64).await?,
        operations[..64]
    );
    // Unreachable storage in the first batch must not starve later files.
    assert_eq!(
        backend.collect_candidates(namespace, 64).await?,
        operations[64..]
    );
    assert!(backend.collect_candidates(namespace, 64).await?.is_empty());
    let mut tx = q::begin(&backend).await?;
    q::claim_collection(operations[0], i64::MAX, 0)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    assert_eq!(
        backend.collect_candidates(namespace, 64).await?,
        [operations[0]]
    );
    assert_eq!(backend.reclaim(operations[0], None).await?.len(), 2);
    Ok(())
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_PAYLOAD_CONFIG and an isolated database"]
async fn storage_payload_contract() -> Result<()> {
    let (backend, cluster) = fixture().await?;
    let id = cluster.id;
    let group = &cluster.manifest.groups[0];

    for (index, acknowledgement) in [
        Acknowledgement::All,
        Acknowledgement::DataAndParity,
        Acknowledgement::Data,
    ]
    .into_iter()
    .enumerate()
    {
        let intent = Intent {
            operation: Uuid::now_v7(),
            namespace: Uuid::now_v7(),
            payload: Uuid::now_v7(),
            revision: 1,
            cluster: id,
            layout_revision: 1,
            takeover_epoch: 1,
            group: group.id,
            layout: group.layout.clone(),
            members: if index == 1 {
                group.members.iter().rev().copied().collect()
            } else {
                Vec::new()
            },
            area: Area::Blocks,
            total: if index == 2 { 0 } else { 70_000 },
            acknowledgement,
        };
        let until = chrono::Utc::now().timestamp_micros() + 4_000_000;
        backend.prepare(&intent, until).await?;
        backend.prepare(&intent, until).await?;
        let mut wrong = intent.clone();
        wrong.total += 1;
        assert!(backend.prepare(&wrong, until).await.is_err());
        wrong = intent.clone();
        wrong.operation = Uuid::now_v7();
        assert!(backend.prepare(&wrong, until).await.is_err());
        let bytes = vec![71; intent.total as usize];
        let mut headers = Vec::new();
        let mut receipts = Vec::new();
        for copy in 0..=1 {
            let header = Header {
                area: intent.area,
                namespace: intent.namespace,
                payload: intent.payload,
                fragment: Uuid::now_v7(),
                media: intent.members(group)?[copy as usize],
                cluster: id,
                group: group.id,
                operation: intent.operation,
                layout_revision: 1,
                revision: 1,
                generation: 0,
                takeover_epoch: 1,
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
            headers.push(header.clone());
            let mut header = header;
            header.digest = *blake3::hash(&bytes).as_bytes();
            let leaves = if bytes.is_empty() {
                vec![fragment::leaf(0, &[])]
            } else {
                bytes
                    .chunks(fragment::UNIT)
                    .enumerate()
                    .map(|(i, data)| fragment::leaf(i, data))
                    .collect()
            };
            let root = fragment::tree(&header, &leaves)?[0];
            receipts.push(Receipt {
                commitment: fragment::commitment(&header, &root)?,
                header,
                root,
            });
        }
        let mut wrong = headers.clone();
        wrong[1].media = headers[0].media;
        assert!(backend.stage(intent.operation, &wrong).await.is_err());
        // The first insertion must roll back with the invalid second member.
        let mut tx = q::begin(&backend).await?;
        assert!(
            q::fragment(headers[0].fragment, intent.operation)
                .fetch_optional(&mut *tx)
                .await?
                .is_none()
        );
        tx.commit().await?;
        backend.stage(intent.operation, &headers).await?;
        backend.stage(intent.operation, &headers).await?;
        assert!(backend.publish(intent.operation).await.is_err());
        assert!(backend.read(intent.operation, 0, 1, until).await.is_err());
        let conflicting = if !bytes.is_empty() {
            let bytes = vec![72; bytes.len()];
            let mut value = receipts[1].clone();
            value.header.digest = *blake3::hash(&bytes).as_bytes();
            let leaves = bytes
                .chunks(fragment::UNIT)
                .enumerate()
                .map(|(index, bytes)| fragment::leaf(index, bytes))
                .collect::<Vec<_>>();
            value.root = fragment::tree(&value.header, &leaves)?[0];
            value.commitment = fragment::commitment(&value.header, &value.root)?;
            value.validate()?;
            assert!(
                backend
                    .receipts(intent.operation, &[receipts[0].clone(), value.clone()])
                    .await
                    .is_err()
            );
            let mut tx = q::begin(&backend).await?;
            let row = q::fragment(headers[0].fragment, intent.operation)
                .fetch_one(&mut *tx)
                .await?;
            assert!(mapping(row)?.receipt.is_none());
            tx.commit().await?;
            Some(value)
        } else {
            None
        };
        backend.receipts(intent.operation, &receipts[..1]).await?;
        if index == 0 {
            assert!(backend.publish(intent.operation).await.is_err());
        } else {
            backend.publish(intent.operation).await?;
        }
        if let Some(conflicting) = conflicting {
            assert!(
                backend
                    .receipts(intent.operation, &[conflicting])
                    .await
                    .is_err()
            );
        }
        backend.receipts(intent.operation, &receipts).await?;
        backend.publish(intent.operation).await?;
        backend.publish(intent.operation).await?;
        backend.stage(intent.operation, &headers).await?;
        let mut corrupt = receipts[0].clone();
        corrupt.header.digest[0] ^= 1;
        corrupt.commitment = fragment::commitment(&corrupt.header, &corrupt.root)?;
        assert!(
            backend
                .receipts(intent.operation, &[corrupt])
                .await
                .is_err()
        );
        let mappings = backend.read(intent.operation, 0, 1, until).await?;
        assert_eq!(mappings.len(), 2);
        assert_eq!(mappings[1].receipt, Some(receipts[1].clone()));
        assert!(backend.read(intent.operation, 1, 1, until).await.is_err());
        backend.retire(intent.operation).await?;
        assert!(backend.read(intent.operation, 0, 1, until).await.is_err());
        assert!(backend.publish(intent.operation).await.is_err());
        assert!(backend.prepare(&intent, until).await.is_err());
        assert!(backend.reclaim(intent.operation, None).await.is_err());
        let remaining = (until - chrono::Utc::now().timestamp_micros()).max(0) as u64;
        tokio::time::sleep(std::time::Duration::from_micros(remaining + 20_000)).await;
        let garbage = backend.reclaim(intent.operation, None).await?;
        assert_eq!(garbage.len(), 2);
        let ids: Vec<_> = garbage.iter().map(|m| m.header.fragment).collect();
        backend.removed(intent.operation, &ids).await?;
        backend.removed(intent.operation, &ids).await?;
        assert!(backend.reclaim(intent.operation, None).await?.is_empty());
        assert_eq!(backend.get(intent.operation).await?.state, "retired");
    }
    Ok(())
}
