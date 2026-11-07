use super::*;
use crate::{
    content::codec::Key,
    model::journal::policy::DEFAULT_LIMIT,
    storage::{journal::source::Source, marker, model},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use object_store::{ObjectStoreExt, path::Path};
use std::collections::BTreeSet;
use std::time::Instant;

#[tokio::test]
#[ignore = "writes a 1 GiB authenticated snapshot to an isolated test volume"]
async fn select_and_verify_a_gibibyte_checkpoint() {
    let root = std::env::var("MOKYU_TEST_SNAPSHOT_VOLUME").unwrap();
    let dir = tempfile::tempdir_in(root).unwrap();
    let store = object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let id = Uuid::new_v4();
    let deployment = Key::new("aes-256-gcm", [9; 32]).unwrap();
    let journal = marker::Journal::create(id, &deployment).unwrap();
    let key = journal.unlock(id, &deployment).unwrap();
    let format = Format {
        storage: Uuid::from_u128(77),
        encrypted: true,
        journal: journal.journal_id,
        epoch: journal.writer_epoch,
        key: &key,
    };
    let owner = Uuid::new_v4();
    let mut pages = Vec::new();
    let mut buckets = Vec::new();
    let mut total = 0u64;
    let start = Instant::now();
    let per_bucket = 21_000usize;
    while total < 1024 * 1024 * 1024 {
        let bucket = Uuid::new_v4();
        let mut rows = Vec::with_capacity(per_bucket);
        let mut rng = blake3::Hasher::new();
        rng.update(bucket.as_bytes());
        let mut rng = rng.finalize_xof();
        for n in 0..per_bucket {
            let mut metadata = [0; 1536];
            rng.fill(&mut metadata);
            rows.push(Row::Object {
                bucket,
                key: format!("asset-{n:06}"),
                generation: 1,
                seq: n as i64 + 1,
                attributes: Attributes {
                    size: 0,
                    etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
                    metadata: serde_json::json!({"sample":URL_SAFE_NO_PAD.encode(metadata)}),
                    checksums: serde_json::json!({}),
                    payload: "chunks".into(),
                    modified_us: 0,
                },
                range_count: 0,
                range_digest: [0; 32],
            });
        }
        let name = format!("bucket-{}", buckets.len());
        buckets.push(BucketCut {
            id: bucket,
            name,
            generation: 1,
            sequence: per_bucket as i64,
            stopped: false,
        });
        let parts = encode_pages(
            &format,
            owner,
            pages.len() as u32,
            &rows,
            DEFAULT_LIMIT as usize,
            Some(bucket),
            Some(bucket),
        )
        .unwrap();
        for (p, data) in parts {
            assert!(p.bytes <= u64::from(DEFAULT_LIMIT));
            total += p.bytes;
            store
                .put(&Path::from(page_path(owner, p.number)), data.into())
                .await
                .unwrap();
            pages.push(p);
        }
    }
    let cut_time = start.elapsed();
    let m = Manifest {
        format_version: 1,
        id: owner,
        parent: None,
        sequence: 1,
        tail: [8; 32],
        pages: pages.clone(),
    };
    let encoded = envelope(&format, FrameType::Manifest, &manifest_path(owner), 1, &m).unwrap();
    let index = Index {
        format_version: 1,
        snapshot: owner,
        operation: owner,
        revision: 1,
        sequence: 1,
        manifest_digest: *blake3::hash(&encoded).as_bytes(),
        bucket_count: buckets.len() as u64,
        buckets: buckets.clone(),
        excluded: Vec::new(),
    };
    store
        .put(&Path::from(manifest_path(owner)), encoded.into())
        .await
        .unwrap();
    let encoded = envelope(&format, FrameType::Index, &index_path(), 1, &index).unwrap();
    store
        .put(
            &Path::from(checkpoint_path(format.epoch, 1)),
            encoded.into(),
        )
        .await
        .unwrap();
    let source = Source {
        connection: model::Connection {
            id,
            r#type: "local".into(),
            path: None,
            endpoint_url: None,
            remote_bucket_name: None,
            region: None,
            prefix: String::new(),
            access: None,
            secret: None,
            identity_marker: marker::create(id, id).unwrap(),
        },
        journal,
        material: key.material,
    };
    let read = Instant::now();
    for b in &buckets {
        let (catalog, _) = source
            .read_selected(&store, None, Some(&BTreeSet::from([b.id])))
            .await
            .unwrap();
        let objects = &catalog.buckets[&b.id].objects;
        assert_eq!(objects.len(), per_bucket);
        let mut rng = blake3::Hasher::new();
        rng.update(b.id.as_bytes());
        let mut rng = rng.finalize_xof();
        for object in objects.values() {
            let mut expected = [0; 1536];
            rng.fill(&mut expected);
            assert_eq!(
                object.attributes.as_ref().unwrap().metadata["sample"],
                URL_SAFE_NO_PAD.encode(expected)
            );
        }
    }
    let verified = read.elapsed();
    let broken = pages.last().unwrap();
    store
        .put(
            &Path::from(page_path(owner, broken.number)),
            vec![10].into(),
        )
        .await
        .unwrap();
    assert!(
        source
            .read_selected(&store, None, Some(&BTreeSet::from([buckets[0].id])))
            .await
            .is_ok()
    );
    assert!(
        source
            .read_selected(
                &store,
                None,
                Some(&BTreeSet::from([broken.first_bucket.unwrap()]))
            )
            .await
            .is_err()
    );
    eprintln!(
        "snapshot_bytes={total} buckets={} objects={} pages={} max_page={} write_ms={} selective_verify_ms={}",
        buckets.len(),
        buckets.len() * per_bucket,
        pages.len(),
        pages.iter().map(|p| p.bytes).max().unwrap(),
        cut_time.as_millis(),
        verified.as_millis()
    );
}
