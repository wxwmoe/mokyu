use super::*;
use crate::{
    content::fragment::Area,
    model::{
        storage_cluster::{self, Acknowledgement, Group, Layout, Member, Topology},
        storage_payload::Intent,
    },
};
use std::fs;

#[tokio::test]
async fn different_copies_cover_different_ranges() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let layout = Layout {
        data_shards: 2,
        parity_shards: 1,
        extra_copies: 1,
        stripe_bytes: UNIT as u32,
    };
    let ids: Vec<_> = (0..6).map(|_| Uuid::now_v7()).collect();
    let group = Uuid::now_v7();
    let manifest = Manifest {
        format_version: 1,
        record_id: Uuid::now_v7(),
        cluster_id: Uuid::now_v7(),
        deployment_id: Uuid::now_v7(),
        publication_key: [1; 32],
        takeover_epoch: 1,
        revision: 1,
        previous: None,
        members: ids
            .iter()
            .enumerate()
            .map(|(i, id)| Member {
                node_id: Uuid::now_v7(),
                media_id: *id,
                state: "available".into(),
                topology: Topology {
                    url: format!("https://disk-{i}.test:9003"),
                    region: None,
                    storage_group: format!("disk-{i}"),
                    purpose: "primary".into(),
                },
            })
            .collect(),
        groups: vec![Group {
            id: group,
            layout: layout.clone(),
            members: ids.clone(),
            replacing: None,
        }],
    };
    let intent = Intent {
        operation: Uuid::now_v7(),
        namespace: Uuid::now_v7(),
        payload: Uuid::now_v7(),
        revision: 1,
        cluster: manifest.cluster_id,
        layout_revision: 1,
        takeover_epoch: 1,
        group,
        layout,
        members: ids.clone(),
        area: Area::Blocks,
        total: 4 * UNIT as u64,
        acknowledgement: Acknowledgement::All,
    };
    let body: Vec<u8> = (0..4 * UNIT)
        .map(|i| ((i + i / UNIT * 13) % 251) as u8)
        .collect();
    let mut slots = vec![
        (body[..UNIT].iter().chain(&body[2 * UNIT..3 * UNIT]))
            .copied()
            .collect::<Vec<_>>(),
        (body[UNIT..2 * UNIT].iter().chain(&body[3 * UNIT..]))
            .copied()
            .collect(),
        vec![0; 2 * UNIT],
    ];
    crate::content::erasure::codec(&intent.layout)?.encode(&mut slots)?;
    let mut media = Media::default();
    let mut candidates = Vec::new();
    for index in [0, 2, 4, 5] {
        let id = ids[index];
        let root = temp.path().join(id.to_string());
        let history = root
            .join("cluster/history")
            .join(manifest.cluster_id.simple().to_string());
        fs::create_dir_all(&history)?;
        fs::write(
            root.join("media.json"),
            serde_json::to_vec(&storage_cluster::Media {
                format_version: 1,
                media_id: id,
            })?,
        )?;
        let marker = serde_json::to_vec(&manifest)?;
        fs::write(root.join("cluster.json"), &marker)?;
        fs::write(
            history.join(format!("{}.json", manifest.record_id.simple())),
            marker,
        )?;
        let slot = index % 3;
        let data = &slots[slot];
        let mut h = intent.header(0, (index / 3) as u16, slot as u16, id, Uuid::now_v7())?;
        h.digest = *blake3::hash(data).as_bytes();
        let leaves: Vec<_> = data
            .chunks(UNIT)
            .enumerate()
            .map(|(i, b)| fragment::leaf(i, b))
            .collect();
        let tree = fragment::tree(&h, &leaves)?;
        let mut file = h.encode()?.to_vec();
        file.extend(data);
        file.extend(tree.iter().flatten());
        file.extend(fragment::footer(&h, &tree[0])?);
        if index == 0 || index == 4 {
            file[fragment::HEADER_SIZE + if index == 0 { UNIT } else { 0 }] ^= 1;
        }
        let path = root.join(h.area.path(h.fragment));
        fs::create_dir_all(path.parent().unwrap())?;
        fs::write(path, file)?;
        let receipt = Receipt {
            commitment: fragment::commitment(&h, &tree[0])?,
            header: h,
            root: tree[0],
        };
        let (_, source) = Source::directory(root).await?;
        media.sources.insert(id, vec![(manifest.clone(), source)]);
        candidates.push((receipt, id, false));
    }
    for (start, end) in [
        (0, 4 * UNIT),
        (UNIT - 13, 2 * UNIT + 7),
        (2 * UNIT + 3, 2 * UNIT + 37),
    ] {
        let mut got = Vec::new();
        let mut at = start as u64;
        while at < end as u64 {
            let (n, mut pieces) = media.window(&intent, &candidates, at, end as u64).await?;
            while let Some(piece) = pieces.try_next().await? {
                got.extend(piece);
            }
            at += n;
        }
        assert_eq!(got, body[start..end]);
    }
    candidates.retain(|(r, _, _)| r.header.shard == 2);
    assert!(
        media
            .window(&intent, &candidates, 0, 2 * UNIT as u64)
            .await
            .is_err()
    );
    Ok(())
}
