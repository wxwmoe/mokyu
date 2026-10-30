use crate::storage::raw::RawFile;
use crate::{
    content::codec,
    model::transfer::{Checkpoints, Checksum, PROOF_BYTES, Part, Proof, SCAN_BYTES, Scan, Scanned},
};
use anyhow::{Context, Result, ensure};
use md5_checkpoint::Digest;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) struct Resources {
    pub cpu: Arc<Semaphore>,
    pub memory: Arc<OwnedSemaphorePermit>,
}

pub(crate) async fn read_proofs(
    plan: &crate::model::transfer::Copy,
    input: &mut RawFile,
    resources: &Resources,
) -> Result<Vec<(crate::model::Id, bytes::Bytes)>> {
    plan.validate()?;
    let crate::model::transfer::Input::Raw { source } = &plan.input else {
        anyhow::bail!("proof reads require a raw source");
    };
    read_spans(source, &plan.prepared.spans, input, resources).await
}

pub(crate) async fn read_spans(
    source: &crate::model::raw::RawSource,
    spans: &[crate::model::transfer::Span],
    input: &mut RawFile,
    resources: &Resources,
) -> Result<Vec<(crate::model::Id, bytes::Bytes)>> {
    let (start, _) = crate::model::transfer::chunk_span_bounds(source, spans)?;
    input.seek(std::io::SeekFrom::Start(start)).await?;
    let mut result = Vec::with_capacity(spans.len());
    for span in spans {
        let mut data = vec![0; span.size as usize];
        input.read_exact(&mut data).await?;
        let cpu = resources.cpu.clone().acquire_owned().await?;
        let memory = resources.memory.clone();
        let hash = span.hash.clone();
        let (data, _memory) = tokio::task::spawn_blocking(move || {
            let _cpu = cpu;
            ensure!(
                blake3::hash(&data).as_bytes().as_slice() == hash,
                codec::IntegrityError::Hash
            );
            Ok::<_, anyhow::Error>((bytes::Bytes::from(data), memory))
        })
        .await??;
        result.push((span.id, data));
    }
    Ok(result)
}

pub(crate) fn decode(
    input: &crate::model::transfer::Input,
    data: Vec<u8>,
    keys: &dyn codec::KeySource,
    pool: &crate::content::compression::Pool,
) -> Result<Vec<(crate::model::Id, bytes::Bytes)>> {
    use crate::model::transfer::Input;
    input.validate()?;
    match input {
        Input::Chunk { chunk } => {
            codec::validate_metadata(chunk, keys)?;
            let (bytes, _) = codec::decode_read(chunk, data, keys, 0, pool, true)?;
            Ok(vec![(chunk.id, bytes::Bytes::copy_from_slice(&bytes))])
        }
        Input::Pack { pack, members } => {
            pack.validate(keys)?;
            let decoded = crate::content::pack::decode(pack, data, keys, pool)?;
            let index: std::collections::BTreeMap<_, _> = decoded
                .members
                .iter()
                .map(|member| (member.id, member))
                .collect();
            let start = 12 + crate::content::pack::MEMBER_SIZE * decoded.members.len();
            members
                .iter()
                .map(|member| {
                    let stored = index
                        .get(&member.uuid)
                        .context("transfer member missing from pack")?;
                    ensure!(
                        stored.start as u64 == start as u64 + member.offset
                            && stored.len == member.size as usize
                            && stored.hash.as_slice() == member.hash,
                        "transfer member differs from pack"
                    );
                    Ok((
                        member.id,
                        bytes::Bytes::copy_from_slice(
                            &decoded.bytes[stored.start..stored.start + stored.len],
                        ),
                    ))
                })
                .collect()
        }
        Input::Raw { .. } => anyhow::bail!("raw transfers require bounded proof reads"),
    }
}

pub(crate) async fn scan(
    plan: &Scan,
    input: &mut RawFile,
    resources: &Resources,
) -> Result<Scanned> {
    plan.source.validate()?;
    let size = u64::try_from(plan.source.size)?;
    let start = plan.position;
    let replay = plan.checksum.object.offset;
    ensure!(
        plan.revision > 0
            && replay <= start
            && start - replay < 64
            && start <= plan.until
            && plan.until <= size
            && plan.until - start <= SCAN_BYTES
            && (plan.until == size || plan.until > start),
        "invalid transfer scan window"
    );
    let mut digest = plan.checksum.object.resume()?;
    let (part_start, mut part_digest) = if let Some(length) = plan.part_size {
        ensure!(
            plan.chunking.is_none()
                && (5 * 1024 * 1024..=5 * 1024 * 1024 * 1024).contains(&length)
                && length % 64 == 0
                && start / length < 10000,
            "invalid transfer part window"
        );
        let part_start = start / length * length;
        ensure!(
            plan.until <= part_start + length,
            "transfer scan crosses part boundary"
        );
        let saved = plan.checksum.part.clone().unwrap_or_default();
        ensure!(
            part_start.checked_add(saved.offset) == Some(replay),
            "invalid transfer part checkpoint"
        );
        (part_start, Some(saved.resume()?))
    } else {
        ensure!(
            plan.checksum.part.is_none(),
            "unexpected transfer part checkpoint"
        );
        (0, None)
    };
    let mut prefix = [0u8; 64];
    let prefix = &mut prefix[..(start - replay) as usize];
    input.read_exact(prefix).await?;
    digest.update(&*prefix);
    if let Some(part) = &mut part_digest {
        part.update(&*prefix);
    }
    let capacity = if plan.chunking.is_some() {
        codec::MAX
    } else {
        PROOF_BYTES
    };
    let mut buffer = bytes::BytesMut::with_capacity(capacity);
    let mut position = start;
    let mut spans = Vec::new();
    while position < plan.until {
        let wanted = (plan.until - position).min(capacity as u64) as usize;
        let previous = buffer.len();
        buffer.resize(wanted, 0);
        input.read_exact(&mut buffer[previous..]).await?;
        if plan.chunking == Some(true)
            && buffer.len() < codec::MAX
            && position + (buffer.len() as u64) < size
        {
            break;
        }
        let cpu = resources.cpu.clone().acquire_owned().await?;
        let memory = resources.memory.clone();
        let cdc = plan.chunking == Some(true);
        let (remaining, object_hash, part_hash, proof, _memory) =
            tokio::task::spawn_blocking(move || {
                let _cpu = cpu;
                let length = if cdc {
                    codec::cut(&buffer)
                } else {
                    buffer.len()
                };
                ensure!(length > 0, "empty transfer scan segment");
                let bytes = buffer.split_to(length);
                digest.update(&bytes);
                if let Some(part) = &mut part_digest {
                    part.update(&bytes);
                }
                let proof = Proof {
                    offset: position,
                    size: u32::try_from(length)?,
                    hash: blake3::hash(&bytes).as_bytes().to_vec(),
                };
                Ok::<_, anyhow::Error>((buffer, digest, part_digest, proof, memory))
            })
            .await??;
        buffer = remaining;
        digest = object_hash;
        part_digest = part_hash;
        position += u64::from(proof.size);
        spans.push(proof);
    }
    ensure!(
        position > start || size == 0,
        "transfer scan cannot advance"
    );
    let (part, part_checkpoint) = if let Some(digest) = part_digest {
        let length = plan.part_size.context("transfer part size missing")?;
        if size == 0 {
            (None, None)
        } else if position == size || position == part_start + length {
            (
                Some(Part {
                    number: u16::try_from(part_start / length + 1)?,
                    offset: part_start,
                    size: position - part_start,
                    digest: digest.finalize().to_vec(),
                }),
                None,
            )
        } else {
            (
                None,
                Some(Checksum::checkpoint(position - part_start, digest)?),
            )
        }
    } else {
        (None, None)
    };
    let (checksum, digest) = if position == size {
        let actual = digest.finalize().to_vec();
        ensure!(
            plan.source.digest.as_deref() == Some(actual.as_slice()),
            codec::IntegrityError::Hash
        );
        (None, Some(actual))
    } else {
        (
            Some(Checkpoints {
                object: Checksum::checkpoint(position, digest)?,
                part: part_checkpoint,
            }),
            None,
        )
    };
    let result = Scanned {
        unit: plan.unit,
        revision: plan.revision,
        start,
        end: position,
        checksum,
        digest,
        spans,
        part,
    };
    plan.validate(&result)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Id, raw::RawSource};
    use md5_checkpoint::Md5;
    use uuid::Uuid;

    #[test]
    fn selected_pack_members_keep_native_ids_and_validate_the_source() -> Result<()> {
        use crate::content::{compression, pack};
        use crate::model::transfer::{Input, Member};
        let pool = compression::Pool::new(Default::default(), 1)?;
        for algorithm in ["none", "aes-256-gcm", "chacha20-poly1305"] {
            let storage = Uuid::now_v7();
            let key_id = Uuid::now_v7();
            let keys = if algorithm == "none" {
                std::collections::BTreeMap::new()
            } else {
                std::collections::BTreeMap::from([(key_id, codec::Key::new(algorithm, [7; 32])?)])
            };
            let date = chrono::Utc::now();
            let chunks: Vec<_> = (1..=3)
                .map(|id| {
                    let data = bytes::Bytes::from(vec![id as u8; 1000 + id as usize]);
                    (
                        codec::Chunk {
                            id: id.into(),
                            uuid: Uuid::now_v7(),
                            encoding_id: id.into(),
                            location_id: Uuid::now_v7(),
                            storage_id: Some(storage),
                            hash: blake3::hash(&data).as_bytes().to_vec(),
                            raw_size: data.len() as i32,
                            stored_size: None,
                            algorithm: algorithm.into(),
                            block_key_id: (algorithm != "none").then_some(key_id),
                            compressed: false,
                            nonce: None,
                            format: 2,
                            state: "ready".into(),
                            created_at: date,
                        },
                        data,
                    )
                })
                .collect();
            let original = pack::Pack {
                id: 19.into(),
                location_id: Uuid::now_v7(),
                storage_id: Some(storage),
                algorithm: algorithm.into(),
                block_key_id: (algorithm != "none").then_some(key_id),
                raw_size: chunks
                    .iter()
                    .map(|(chunk, _)| i64::from(chunk.raw_size))
                    .sum(),
                stored_size: None,
                compressed: false,
                nonce: (algorithm != "none").then(|| {
                    codec::typed_nonce(date, 19, crate::content::nonce::PACK)
                        .unwrap()
                        .to_vec()
                }),
                digest: None,
                member_count: 3,
                state: "preparing".into(),
                created_at: date,
            };
            let (mut packed, data) = pack::encode(
                original,
                &chunks,
                &keys,
                &pool,
                compression::Strategy::Always,
                true,
                None,
            )?;
            packed.state = "ready".into();
            let mut offset = 0;
            let members: Vec<_> = chunks
                .iter()
                .enumerate()
                .filter_map(|(index, (chunk, _))| {
                    let member = Member {
                        id: chunk.id,
                        uuid: chunk.uuid,
                        offset,
                        size: chunk.raw_size as u32,
                        hash: chunk.hash.clone(),
                    };
                    offset += chunk.raw_size as u64;
                    (index != 1).then_some(member)
                })
                .collect();
            let input = Input::Pack {
                pack: packed.clone(),
                members: members.clone(),
            };
            let copied = decode(&input, data.clone(), &keys, &pool)?;
            assert_eq!(
                copied,
                vec![
                    (chunks[0].0.id, chunks[0].1.clone()),
                    (chunks[2].0.id, chunks[2].1.clone())
                ]
            );
            assert_eq!(
                input.size()?,
                (chunks[0].1.len() + chunks[2].1.len()) as u64
            );
            let mut corrupted = data.clone();
            *corrupted.last_mut().unwrap() ^= 1;
            assert!(decode(&input, corrupted, &keys, &pool).is_err());
            let mut wrong = members.clone();
            wrong[0].uuid = chunks[1].0.uuid;
            assert!(
                decode(
                    &Input::Pack {
                        pack: packed.clone(),
                        members: wrong
                    },
                    data.clone(),
                    &keys,
                    &pool
                )
                .is_err()
            );
            let mut wrong = members;
            wrong[1].offset = 0;
            assert!(
                decode(
                    &Input::Pack {
                        pack: packed,
                        members: wrong
                    },
                    data,
                    &keys,
                    &pool
                )
                .is_err()
            );
        }
        Ok(())
    }

    async fn scan(plan: &Scan, input: &[u8]) -> Result<Scanned> {
        let mut input = RawFile::Memory {
            data: bytes::Bytes::copy_from_slice(input),
            offset: 0,
        };
        let resources = Resources {
            cpu: Arc::new(Semaphore::new(1)),
            memory: Arc::new(Arc::new(Semaphore::new(1)).acquire_owned().await?),
        };
        super::scan(plan, &mut input, &resources).await
    }

    fn plan(bytes: &[u8], until: u64) -> Scan {
        Scan {
            unit: Id::Uuid(Uuid::now_v7()),
            revision: 1,
            source: RawSource {
                id: Id::Uuid(Uuid::now_v7()),
                location_id: Uuid::now_v7(),
                storage_id: Uuid::now_v7(),
                size: bytes.len() as i64,
                digest: Some(<md5::Md5 as md5::Digest>::digest(bytes).to_vec()),
                state: "ready".into(),
                medium: "shared_s3".into(),
                pending_upload: false,
                revision: 1,
                quarantine_code: None,
            },
            checksum: Checkpoints::default(),
            position: 0,
            until,
            chunking: None,
            part_size: None,
        }
    }

    #[tokio::test]
    async fn resumes_with_only_partial_block_replay() -> Result<()> {
        let bytes: Vec<_> = (0..8197).map(|n| (n * 31) as u8).collect();
        let first = scan(&plan(&bytes, 4097), bytes.as_slice()).await?;
        assert_eq!(first.spans[0].hash, blake3::hash(&bytes[..4097]).as_bytes());
        let mut last = plan(&bytes, bytes.len() as u64);
        last.checksum = serde_json::from_slice(&serde_json::to_vec(&first.checksum.unwrap())?)?;
        last.position = 4097;
        assert_eq!(last.checksum.object.offset, 4096);
        let result = scan(&last, &bytes[4096..]).await?;
        assert_eq!(result.start, 4097);
        assert_eq!(result.digest, last.source.digest);
        assert!(result.checksum.is_none());
        assert_eq!(
            result.spans[0].hash,
            blake3::hash(&bytes[4097..]).as_bytes()
        );
        Ok(())
    }

    #[tokio::test]
    async fn rejects_corrupt_or_truncated_source() -> Result<()> {
        let bytes = vec![7u8; 8197];
        let first = scan(&plan(&bytes, 4096), bytes.as_slice()).await?;
        let mut last = plan(&bytes, bytes.len() as u64);
        last.checksum = first.checksum.unwrap();
        last.position = 4096;
        assert!(scan(&last, &bytes[4097..]).await.is_err());
        let mut changed = bytes[4096..].to_vec();
        changed[17] ^= 1;
        assert!(scan(&last, changed.as_slice()).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn rejects_skipped_or_unbounded_checkpoint() -> Result<()> {
        let bytes = vec![0u8; 128];
        let mut skipped = plan(&bytes, 128);
        skipped.position = 64;
        assert!(scan(&skipped, bytes.as_slice()).await.is_err());
        let mut excessive = plan(&bytes, SCAN_BYTES + 64);
        excessive.source.size = (SCAN_BYTES + 64) as i64;
        assert!(scan(&excessive, bytes.as_slice()).await.is_err());
        let mut invalid_part = plan(&bytes, 128);
        invalid_part.part_size = Some(0);
        assert!(scan(&invalid_part, bytes.as_slice()).await.is_err());
        let mut empty = plan(&bytes, 0);
        assert!(scan(&empty, bytes.as_slice()).await.is_err());
        empty.source = plan(&[], 0).source;
        assert!(scan(&empty, &[][..]).await?.digest.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn resumed_content_defined_segments_match_a_complete_scan() -> Result<()> {
        let bytes: Vec<_> = (0..codec::MAX * 3 + 17)
            .map(|n| (n * 31 + n / 37) as u8)
            .collect();
        let mut first = plan(&bytes, (codec::MAX * 2 + 113) as u64);
        first.chunking = Some(true);
        let first = scan(&first, bytes.as_slice()).await?;
        let mut next = plan(&bytes, bytes.len() as u64);
        next.chunking = Some(true);
        next.position = first.end;
        next.checksum = first.checksum.unwrap();
        let last = scan(&next, &bytes[next.checksum.object.offset as usize..]).await?;
        let mut complete = plan(&bytes, bytes.len() as u64);
        complete.chunking = Some(true);
        let complete = scan(&complete, bytes.as_slice()).await?;
        assert_eq!([first.spans, last.spans].concat(), complete.spans);
        assert_eq!(last.digest, complete.digest);
        Ok(())
    }

    #[tokio::test]
    async fn proof_reads_reject_changed_data_and_misaligned_targets() -> Result<()> {
        use crate::model::{
            cache::EncodingPolicy,
            transfer::{Copy, Input, Prepared, Span, Target},
        };
        let bytes: Vec<u8> = (0..8195).map(|n| (n * 17) as u8).collect();
        let storage = Uuid::now_v7();
        let mut targets = Vec::new();
        let mut spans = Vec::new();
        for n in 0..2 {
            let offset = 13 + n * 2048;
            let hash = blake3::hash(&bytes[offset..offset + 2048])
                .as_bytes()
                .to_vec();
            let source = crate::model::Id::from(n as i64 + 1);
            spans.push(Span {
                id: source,
                offset: offset as u64,
                size: 2048,
                hash: hash.clone(),
            });
            targets.push(Target {
                source,
                offset: offset as u64,
                reused: false,
                chunk: codec::Chunk {
                    id: (n as i64 + 101).into(),
                    uuid: Uuid::now_v7(),
                    encoding_id: (n as i64 + 201).into(),
                    location_id: Uuid::now_v7(),
                    storage_id: Some(storage),
                    hash,
                    raw_size: 2048,
                    stored_size: None,
                    algorithm: "none".into(),
                    block_key_id: None,
                    compressed: false,
                    nonce: None,
                    format: 2,
                    state: "preparing".into(),
                    created_at: chrono::Utc::now(),
                },
            });
        }
        let mut copying = Copy {
            unit: 1.into(),
            revision: 1,
            candidate: 10.into(),
            storage,
            input: Input::Raw {
                source: plan(&bytes, bytes.len() as u64).source,
            },
            prepared: Prepared {
                start: 13,
                end: 4109,
                targets,
                spans,
                policy: EncodingPolicy {
                    compression: Default::default(),
                    pack_enabled: true,
                    pack_max_size: "32MiB".into(),
                    pack_flush_timeout: "60s".into(),
                    algorithm: "none".into(),
                    block_key_id: None,
                },
            },
        };
        let resources = Resources {
            cpu: Arc::new(Semaphore::new(1)),
            memory: Arc::new(Arc::new(Semaphore::new(1)).acquire_owned().await?),
        };
        let file = |value: &[u8]| RawFile::Memory {
            data: bytes::Bytes::copy_from_slice(value),
            offset: 0,
        };
        let selected = read_proofs(&copying, &mut file(&bytes), &resources).await?;
        assert_eq!(selected[0].1, bytes[13..2061]);
        assert_eq!(selected[1].1, bytes[2061..4109]);
        let mut changed = bytes.clone();
        changed[3000] ^= 1;
        assert!(
            read_proofs(&copying, &mut file(&changed), &resources)
                .await
                .is_err()
        );
        assert!(
            read_proofs(&copying, &mut file(&bytes[..4108]), &resources)
                .await
                .is_err()
        );
        copying.prepared.targets[1].offset += 1;
        assert!(copying.validate().is_err());
        copying.prepared.targets[1].offset -= 1;
        copying.prepared.spans[1].offset += 1;
        assert!(copying.validate().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn multipart_digests_resume_and_reset_at_part_boundaries() -> Result<()> {
        let part_size = 5 * 1024 * 1024;
        let bytes = vec![0x27; part_size * 2 + 17];
        let mut request = plan(&bytes, 2 * 1024 * 1024 + 31);
        request.part_size = Some(part_size as u64);
        let first = scan(&request, bytes.as_slice()).await?;
        assert!(first.part.is_none());
        request.position = first.end;
        request.until = part_size as u64;
        request.checksum = first.checksum.unwrap();
        let second = scan(&request, &bytes[request.checksum.object.offset as usize..]).await?;
        let part = second.part.unwrap();
        assert_eq!(
            (part.number, part.offset, part.size),
            (1, 0, part_size as u64)
        );
        assert_eq!(part.digest, Md5::digest(&bytes[..part_size]).as_slice());
        request.position = second.end;
        request.until = (part_size * 2) as u64;
        request.checksum = second.checksum.unwrap();
        let third = scan(&request, &bytes[request.checksum.object.offset as usize..]).await?;
        assert_eq!(third.part.unwrap().number, 2);
        request.position = third.end;
        request.until = bytes.len() as u64;
        request.checksum = third.checksum.unwrap();
        let last = scan(&request, &bytes[request.checksum.object.offset as usize..]).await?;
        assert_eq!(
            last.part.unwrap().digest,
            Md5::digest(&bytes[part_size * 2..]).as_slice()
        );
        assert_eq!(last.digest, request.source.digest);
        Ok(())
    }
}
