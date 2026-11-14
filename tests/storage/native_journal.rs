use super::*;
use crate::{
    content::{codec::Key, fragment::Area},
    model::{
        storage_cluster::{Acknowledgement, Layout},
        storage_payload::Intent,
        storage_recovery::Part,
    },
    storage::journal::{Sequence, recovery, verify_segment},
};
use bytes::Bytes;
use object_store::{ObjectStoreExt, memory::InMemory};
use uuid::Uuid;

#[tokio::test]
async fn native_snapshot_pages_keep_public_mappings_and_stream_rows() -> Result<()> {
    use crate::model::storage_recovery::{CatalogEntry, Physical, verify};
    use crate::storage::journal::{
        batch::Decoder,
        snapshot::{self, Row},
    };
    let key = Key::new("aes-256-gcm", [8; 32])?;
    for encrypted in [false, true] {
        let format = Format {
            storage: Uuid::now_v7(),
            journal: Uuid::now_v7(),
            epoch: Uuid::now_v7(),
            key: &key,
            encrypted,
        };
        let bucket = Uuid::now_v7();
        let rows: Vec<_> = (1..=30)
            .map(|seq| Row::Delete {
                bucket,
                key: format!("object-{seq}"),
                generation: 1,
                seq,
            })
            .collect();
        let physical: Vec<_> = (0..512)
            .map(|_| Physical::State {
                sequence: 1,
                operation: Uuid::now_v7(),
                published: true,
            })
            .collect();
        let pages = snapshot::encode_pages(
            &format,
            Uuid::now_v7(),
            0,
            &rows,
            4096,
            Some(bucket),
            Some(bucket),
            Some(&physical),
        )?;
        assert!(pages.len() > 1);
        let store = InMemory::new();
        let mut decoders = verify::Decoders::default();
        let mut decoder = Decoder::default();
        let mut sink = Memory::default();
        let mut decoded_rows = Vec::new();
        let mut decoded_mappings = Vec::new();
        for (description, bytes) in &pages {
            assert!(bytes.starts_with(recovery::MAGIC) && bytes.len() <= 4096);
            let public = recovery::inspect_prefix(bytes)?;
            let part = Part {
                intent: Intent {
                    operation: Uuid::now_v7(),
                    namespace: format.storage,
                    payload: public.envelope.payload,
                    revision: 1,
                    cluster: Uuid::now_v7(),
                    layout_revision: 1,
                    takeover_epoch: 1,
                    group: Uuid::now_v7(),
                    layout: Layout::default(),
                    members: vec![],
                    area: Area::Recovery,
                    total: bytes.len() as u64,
                    acknowledgement: Acknowledgement::All,
                },
                path: snapshot::page_path(description.owner, description.number),
                digest: *blake3::hash(bytes).as_bytes(),
            };
            verify::visit(&part, bytes, 1, Some(&format), &mut decoders, |entry| {
                match entry {
                    CatalogEntry::Snapshot { row, .. } => decoded_rows.push(row),
                    CatalogEntry::Physical(_, value) => decoded_mappings.push(value),
                    _ => anyhow::bail!("unexpected snapshot entry"),
                }
                Ok(())
            })?;
            store
                .put(&Path::from(part.path), bytes.clone().into())
                .await?;
            page(&store, &format, description, &mut decoder, &mut sink).await?;
        }
        assert!(decoder.complete() && decoders.complete());
        assert_eq!(decoded_mappings, physical);
        assert_eq!(
            serde_json::to_value(decoded_rows)?,
            serde_json::to_value(rows)?
        );
        let mut decoders = verify::Decoders::default();
        let mut count = 0;
        for (description, bytes) in &pages {
            let public = recovery::inspect_prefix(bytes)?;
            let part = Part {
                intent: Intent {
                    operation: Uuid::now_v7(),
                    namespace: format.storage,
                    payload: public.envelope.payload,
                    revision: 1,
                    cluster: Uuid::now_v7(),
                    layout_revision: 1,
                    takeover_epoch: 1,
                    group: Uuid::now_v7(),
                    layout: Layout::default(),
                    members: vec![],
                    area: Area::Recovery,
                    total: bytes.len() as u64,
                    acknowledgement: Acknowledgement::All,
                },
                path: snapshot::page_path(description.owner, description.number),
                digest: *blake3::hash(bytes).as_bytes(),
            };
            verify::visit(&part, bytes, 1, None, &mut decoders, |entry| {
                assert!(matches!(entry, CatalogEntry::Physical(_, _)));
                count += 1;
                Ok(())
            })?;
        }
        assert_eq!(count, physical.len());
    }
    Ok(())
}

#[tokio::test]
async fn native_metadata_frames_stream_and_authenticate_large_segments() -> Result<()> {
    let key = Key::new("aes-256-gcm", [7; 32])?;
    let wrong_key = Key::new("aes-256-gcm", [8; 32])?;
    let mut content = vec![0; 7 * 1024 * 1024];
    blake3::Hasher::new()
        .update(b"native metadata frame fixture")
        .finalize_xof()
        .fill(&mut content);
    for encrypted in [false, true] {
        let format = Format {
            storage: Uuid::new_v4(),
            journal: Uuid::new_v4(),
            epoch: Uuid::new_v4(),
            key: &key,
            encrypted,
        };
        let path = crate::storage::journal::segment_path(format.epoch, 1);
        let payload = Uuid::new_v4();
        let batch = Uuid::new_v4();
        let mut encoded = Vec::new();
        let mut prior = [0; 32];
        let mut offsets = Vec::new();
        for number in 0..=6 {
            offsets.push(encoded.len());
            let seal = number == 6;
            let bytes = recovery::encode(
                &format,
                &path,
                &recovery::Envelope {
                    payload,
                    revision: 1,
                    batch,
                    kind: if seal {
                        FrameType::Seal
                    } else if number == 0 {
                        FrameType::Mappings
                    } else {
                        FrameType::Changes
                    },
                    sequence: Sequence {
                        first: if seal { 1 } else { number + 1 },
                        last: (number + 1).min(6),
                        previous: prior,
                    },
                },
                b"[]",
                if seal || number == 0 { &[] } else { &content },
            )?;
            prior = *blake3::hash(&bytes).as_bytes();
            encoded.extend(bytes);
        }
        assert!(encoded.len() > 32 * 1024 * 1024);
        let part = Part {
            intent: Intent {
                operation: Uuid::new_v4(),
                namespace: format.storage,
                payload,
                revision: 1,
                cluster: Uuid::new_v4(),
                layout_revision: 1,
                takeover_epoch: 1,
                group: Uuid::new_v4(),
                layout: Layout::default(),
                members: vec![],
                area: Area::Recovery,
                total: encoded.len() as u64,
                acknowledgement: Acknowledgement::All,
            },
            path: path.clone(),
            digest: *blake3::hash(&encoded).as_bytes(),
        };
        let mut count = 0;
        part.visit(&encoded, |frame| {
            assert_eq!(frame.physical, b"[]");
            assert_eq!(frame.encrypted, encrypted);
            assert!(
                frame
                    .open(&Format {
                        key: &wrong_key,
                        ..format
                    })
                    .is_err()
            );
            count += 1;
            Ok(())
        })?;
        assert_eq!(count, 7);
        let mut visited = 0;
        assert_eq!(
            verify_segment(&format, &path, &encoded, 1, [0; 32], |frame| {
                assert_eq!(frame.bytes, content);
                visited += 1;
                Ok(())
            })?,
            (6, prior)
        );
        assert_eq!(visited, 5);
        assert!(
            format
                .decode(FrameType::Changes, "metadata/wrong", &encoded)
                .is_err()
        );
        assert!(format.decode(FrameType::Snapshot, &path, &encoded).is_err());
        assert!(
            verify_segment(&format, &path, &encoded[offsets[1]..], 1, [0; 32], |_| Ok(
                ()
            ))
            .is_err()
        );
        assert!(
            verify_segment(&format, &path, &encoded[..offsets[6]], 1, [0; 32], |_| Ok(
                ()
            ))
            .is_err()
        );
        assert!(recovery::inspect_prefix(&encoded[..offsets[1] - 1]).is_err());
        let mut oversized = encoded[..recovery::HEADER].to_vec();
        oversized[164..168].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(recovery::frame_size(&oversized).is_err());
        let store = InMemory::new();
        let body = Bytes::from(encoded);
        store
            .put(&Path::from(path.as_str()), body.clone().into())
            .await?;
        let mut frames = Frames::open(&store, &path, None).await?;
        for number in 0..=6 {
            let (kind, frame) = frames.next(&format, &path).await?;
            assert_eq!(
                kind,
                if number == 6 {
                    FrameType::Seal
                } else if number == 0 {
                    FrameType::Mappings
                } else {
                    FrameType::Changes
                }
            );
            assert_eq!(
                frame.bytes,
                if number == 6 || number == 0 {
                    &[][..]
                } else {
                    content.as_slice()
                }
            );
        }
        assert_eq!(frames.remaining, 0);
        assert_eq!(frames.digest.finalize().as_bytes(), &part.digest);
        assert!(frames.next(&format, &path).await.is_err());
        store
            .put(
                &Path::from(path.as_str()),
                body.slice(..body.len() - 1).into(),
            )
            .await?;
        let mut frames = Frames::open(&store, &path, None).await?;
        for _ in 0..6 {
            frames.next(&format, &path).await?;
        }
        assert!(frames.next(&format, &path).await.is_err());
    }
    Ok(())
}
