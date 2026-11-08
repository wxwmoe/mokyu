use super::*;
use crate::{
    model::journal::record::{Change, Operation},
    storage::journal::{
        Sequence,
        batch::{Record, frames},
    },
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use std::collections::BTreeMap;

fn event(bucket: Uuid, seq: i64, operation: Operation, key: Option<String>) -> Record {
    Record::Change {
        value: Change {
            bucket,
            name: "recover-parts".into(),
            generation: 1,
            seq,
            operation,
            key,
            attributes: None,
            ranges: Vec::new(),
        },
        range_count: 0,
        range_digest: [0; 32],
    }
}

#[tokio::test]
#[ignore = "requires an isolated S3 bucket with Multipart permissions"]
async fn recover_three_parts_without_the_source_database() {
    let env = |name: &str| std::env::var(name).unwrap();
    assert_eq!(env("MOKYU_TEST_ALLOW_STATE_CHANGES"), "isolated-only");
    let storage = Uuid::new_v4();
    let key = Key::new("aes-256-gcm", [42; 32]).unwrap();
    let journal = marker::Journal::create(storage, &key).unwrap();
    let material = journal.unlock(storage, &key).unwrap();
    let epoch = journal.writer_epoch;
    let source = Source {
        connection: model::Connection {
            id: storage,
            r#type: "s3".into(),
            path: None,
            endpoint_url: Some(env("MOKYU_TEST_RECOVERY_ENDPOINT")),
            remote_bucket_name: Some(env("MOKYU_TEST_RECOVERY_BUCKET")),
            region: Some("us-east-1".into()),
            prefix: format!("rescue-{}", Uuid::new_v4().simple()),
            access: Some(env("MOKYU_TEST_RECOVERY_ACCESS")),
            secret: Some(env("MOKYU_TEST_RECOVERY_SECRET")),
            identity_marker: marker::create(Uuid::new_v4(), storage).unwrap(),
        },
        journal,
        material: material.material,
    };
    let store = source.connect().unwrap();
    let client = source.client().unwrap();
    let (_, before) = source.read(store.as_ref(), None).await.unwrap();
    let path = segment_path(epoch, 1);
    let business = client
        .start_upload("business-object", &BTreeMap::new(), None)
        .await
        .unwrap();
    let upload = client
        .start_upload(&path, &BTreeMap::new(), None)
        .await
        .unwrap();
    let other = client
        .start_upload(&path, &BTreeMap::new(), None)
        .await
        .unwrap();
    assert!(source.salvage(store.as_ref(), &before).await.is_err());
    client.abort_upload(&path, &other).await.unwrap();
    let bucket = Uuid::new_v4();
    let format = Format {
        storage: source.connection.id,
        encrypted: true,
        journal: source.journal.journal_id,
        epoch,
        key: &material,
    };
    let mut prior = [0; 32];
    let mut first = 1u64;
    let mut seq = 2i64;
    let mut rows = vec![
        event(bucket, 1, Operation::Start, None),
        event(bucket, 2, Operation::Ready, None),
    ];
    for number in 1..=3 {
        let mut part = Vec::new();
        while part.len() < 5 * 1024 * 1024 {
            for _ in 0..256 {
                seq += 1;
                let mut random = [0; 700];
                aws_lc_rs::rand::fill(&mut random).unwrap();
                rows.push(event(
                    bucket,
                    seq,
                    Operation::Delete,
                    Some(URL_SAFE_NO_PAD.encode(random)),
                ));
            }
            for plain in frames(&rows, 1024 * 1024, format.encrypted).unwrap() {
                let last = first + u64::from(plain.records) - 1;
                let bytes = format
                    .encode(
                        FrameType::Changes,
                        &path,
                        Sequence {
                            first,
                            last,
                            previous: prior,
                        },
                        &plain.bytes,
                    )
                    .unwrap();
                prior = *blake3::hash(&bytes).as_bytes();
                part.extend_from_slice(&bytes);
                first = last + 1;
            }
            rows.clear();
        }
        let bytes = format
            .encode(
                FrameType::PartCommit,
                &path,
                Sequence {
                    first: 1,
                    last: first - 1,
                    previous: prior,
                },
                &[],
            )
            .unwrap();
        prior = *blake3::hash(&bytes).as_bytes();
        part.extend_from_slice(&bytes);
        if number == 3 {
            part.extend_from_slice(b"MOKYU\0\0\x06");
        }
        client
            .upload_part(&path, &upload, number, Bytes::from(part))
            .await
            .unwrap();
    }
    assert_eq!(source.read(store.as_ref(), None).await.unwrap().1, before);
    let cut = source.salvage(store.as_ref(), &before).await.unwrap();
    assert!(cut.point.salvaged.is_some());
    assert_eq!(cut.point.sequence, first - 1);
    let (catalog, _) = source.read(store.as_ref(), Some(&cut)).await.unwrap();
    assert_eq!(catalog.buckets[&bucket].sequence, seq);
    assert_eq!(
        client.uploads("business-object").await.unwrap().0,
        [business.clone()]
    );
    assert_eq!(source.salvage(store.as_ref(), &before).await.unwrap(), cut);
    client
        .abort_upload("business-object", &business)
        .await
        .unwrap();
}
