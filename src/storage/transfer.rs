use super::{raw::RawFile, s3::Streaming};
use crate::model::transfer::{RawCopy, RawProgress};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, stream::BoxStream};
use md5::{Digest, Md5};
use object_store::{ObjectStore, ObjectStoreExt, multipart::MultipartStore};
use std::{collections::BTreeMap, future::Future, path::PathBuf, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

pub(crate) enum Target<'a> {
    Local(PathBuf),
    Remote {
        backend: Arc<dyn ObjectStore>,
        streaming: &'a Streaming,
        multipart: &'a dyn MultipartStore,
    },
}

pub(crate) struct Copied {
    pub bytes: u64,
    pub requests: u64,
}

impl super::Storage {
    pub(crate) async fn transfer_space(
        &self,
        target: &super::StoreBackend,
        size: u64,
        location: uuid::Uuid,
    ) -> Result<Option<super::local::Space>> {
        let Some(root) = &target.raw_root else {
            return Ok(None);
        };
        let path = root.join(super::chunk_name(location));
        let existing = match tokio::fs::metadata(&path).await {
            Ok(value) => value.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match tokio::fs::metadata(path.with_extension("partial")).await {
                    Ok(value) => value.len(),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) => return Err(error.into()),
        };
        self.local_space(target, size.saturating_sub(existing))
    }
}

fn replay<'a>(
    input: &'a Mutex<&mut RawFile>,
    offset: u64,
    length: u64,
    expected: [u8; 16],
) -> BoxStream<'a, Result<Bytes>> {
    Box::pin(async_stream::try_stream! {
        let mut input=input.lock().await;
        input.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut left=length;
        let mut digest=Md5::new();
        while left>0 {
            let mut bytes=vec![0;left.min(256*1024) as usize];
            input.read_exact(&mut bytes).await?;
            left-=bytes.len() as u64;
            digest.update(&bytes);
            yield Bytes::from(bytes);
        }
        if digest.finalize().as_slice()!=expected {
            Err(anyhow::anyhow!("transfer source checksum differs"))?;
        }
    })
}

async fn verify_local(path: &std::path::Path, size: u64, expected: &[u8]) -> Result<bool> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    ensure!(
        metadata.is_file() && metadata.len() == size,
        "raw destination length differs"
    );
    let mut file = tokio::fs::File::open(path).await?;
    let mut hash = Md5::new();
    let mut bytes = vec![0; 256 * 1024];
    loop {
        let count = file.read(&mut bytes).await?;
        if count == 0 {
            break;
        }
        hash.update(&bytes[..count]);
    }
    ensure!(
        hash.finalize().as_slice() == expected,
        "raw destination checksum differs"
    );
    Ok(true)
}

async fn verify_remote(
    backend: &dyn ObjectStore,
    key: &object_store::path::Path,
    size: u64,
    expected: &[u8],
    fill: &mut Option<super::raw_cache::Write>,
) -> Result<bool> {
    let head = match backend.head(key).await {
        Ok(value) => value,
        Err(object_store::Error::NotFound { .. }) => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    ensure!(head.size == size, "raw destination length differs");
    let mut body = backend.get(key).await?.into_stream();
    let mut hash = Md5::new();
    let mut read = 0u64;
    while let Some(bytes) = body.next().await {
        let bytes = bytes?;
        read = read
            .checked_add(bytes.len() as u64)
            .context("raw destination size overflow")?;
        ensure!(read <= size, "raw destination exceeds expected length");
        hash.update(&bytes);
        super::raw_cache::append(fill, &bytes).await;
    }
    ensure!(
        read == size && hash.finalize().as_slice() == expected,
        "raw destination checksum differs"
    );
    Ok(true)
}

pub(crate) async fn copy<F, Fut>(
    target: Target<'_>,
    plan: &RawCopy,
    digest: [u8; 16],
    input: &mut RawFile,
    fill: &mut Option<super::raw_cache::Write>,
    checkpoint: F,
) -> Result<Copied>
where
    F: Fn(RawProgress) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    plan.validate()?;
    ensure!(
        plan.target
            .digest
            .as_ref()
            .is_none_or(|value| value.as_slice() == digest),
        "raw transfer digest differs"
    );
    let size = plan.target.size as u64;
    let input = Mutex::new(input);
    let mut copied = Copied {
        bytes: 0,
        requests: 0,
    };
    match target {
        Target::Local(destination) => {
            if !verify_local(&destination, size, &digest).await? {
                let parent = destination
                    .parent()
                    .context("raw destination has no parent")?;
                tokio::fs::create_dir_all(parent).await?;
                super::raw::sync_directory(parent.parent().context("raw directory has no parent")?)
                    .await?;
                let partial = destination.with_extension("partial");
                let mut options = tokio::fs::OpenOptions::new();
                options.read(true).write(true).create(true).truncate(false);
                #[cfg(unix)]
                options.mode(0o644);
                let mut file = options.open(&partial).await?;
                ensure!(
                    file.metadata().await?.len() <= size,
                    "raw partial exceeds destination length"
                );
                if plan.parts.is_empty() {
                    file.rewind().await?;
                    let mut body = replay(&input, 0, size, digest);
                    while let Some(bytes) = body.next().await {
                        file.write_all(&bytes?).await?;
                    }
                    copied.bytes += size;
                    copied.requests += 1;
                } else {
                    for (part, receipt) in plan.parts.iter().zip(&plan.receipts) {
                        let expected: [u8; 16] = part.digest.as_slice().try_into()?;
                        file.seek(std::io::SeekFrom::Start(part.offset)).await?;
                        if let Some(receipt) = receipt {
                            ensure!(
                                file.metadata().await?.len() >= part.offset + part.size,
                                "confirmed raw part is missing"
                            );
                            let mut remaining = part.size;
                            let mut hash = Md5::new();
                            let mut bytes = vec![0; 256 * 1024];
                            while remaining > 0 {
                                let count = remaining.min(bytes.len() as u64) as usize;
                                file.read_exact(&mut bytes[..count]).await?;
                                hash.update(&bytes[..count]);
                                remaining -= count as u64;
                            }
                            ensure!(
                                hash.finalize().as_slice() == expected
                                    && *receipt
                                        == blake3::hash(hex::encode(expected).as_bytes())
                                            .to_hex()
                                            .as_str(),
                                "confirmed raw part differs"
                            );
                            continue;
                        }
                        let mut body = replay(&input, part.offset, part.size, expected);
                        while let Some(bytes) = body.next().await {
                            file.write_all(&bytes?).await?;
                        }
                        file.sync_data().await?;
                        #[cfg(feature = "fault-injection")]
                        crate::faults::point("raw-transfer-part").await;
                        checkpoint(RawProgress::Part {
                            upload: None,
                            number: part.number,
                            etag: hex::encode(expected),
                        })
                        .await?;
                        copied.bytes += part.size;
                        copied.requests += 1;
                        #[cfg(feature = "fault-injection")]
                        crate::faults::point("raw-transfer-checkpoint").await;
                    }
                }
                file.set_len(size).await?;
                file.sync_all().await?;
                drop(file);
                verify_local(&partial, size, &digest).await?;
                tokio::fs::rename(&partial, &destination).await?;
                super::raw::sync_directory(parent).await?;
            }
        }
        Target::Remote {
            backend,
            streaming,
            multipart,
        } => {
            let key = super::raw::remote_path(plan.target.location_id);
            if !verify_remote(backend.as_ref(), &key, size, &digest, fill).await? {
                let written = if plan.parts.is_empty() {
                    let result = streaming
                        .put(&key, size, &digest, || replay(&input, 0, size, digest))
                        .await;
                    copied.bytes += size;
                    copied.requests += 1;
                    result
                } else {
                    let mut upload = plan.upload.clone();
                    let mut recorded = plan.receipts.clone();
                    let mut listed = BTreeMap::new();
                    if let Some(id) = &upload {
                        let mut after = 0;
                        loop {
                            let Some(page) = streaming.parts(&key, id, after).await? else {
                                ensure!(after == 0, "multipart disappeared while reconciling");
                                checkpoint(RawProgress::Reset { upload: id.clone() }).await?;
                                upload = None;
                                recorded.fill(None);
                                break;
                            };
                            for part in page.items {
                                ensure!(
                                    listed.insert(part.number, part).is_none(),
                                    "duplicate remote part"
                                );
                            }
                            if let Some(next) = page.next {
                                after = next
                            } else {
                                break;
                            }
                        }
                    }
                    if upload.is_none() {
                        let (mut existing, more) = streaming.pending_multipart(&key).await?;
                        ensure!(!more, "too many pending raw uploads");
                        existing.sort();
                        let id = if let Some(id) = existing.first() {
                            id.clone()
                        } else {
                            match multipart.create_multipart(&key).await {
                                Ok(id) => id,
                                Err(error) => {
                                    let (pending, more) = streaming.pending_multipart(&key).await?;
                                    ensure!(!more, "too many pending raw uploads");
                                    existing = pending;
                                    existing.sort();
                                    match existing.first() {
                                        Some(id) => id.clone(),
                                        None => return Err(error.into()),
                                    }
                                }
                            }
                        };
                        #[cfg(feature = "fault-injection")]
                        crate::faults::point("raw-transfer-started").await;
                        checkpoint(RawProgress::Started { upload: id.clone() }).await?;
                        for other in existing.into_iter().filter(|other| *other != id) {
                            multipart.abort_multipart(&key, &other).await?;
                        }
                        upload = Some(id);
                    }
                    let upload = upload.context("raw upload missing")?;
                    let mut parts = Vec::with_capacity(plan.parts.len());
                    for (part, receipt) in plan.parts.iter().zip(&recorded) {
                        let expected: [u8; 16] = part.digest.as_slice().try_into()?;
                        let stored = listed.get(&part.number);
                        let confirmed = if let Some(receipt) = receipt {
                            let stored = stored.context("confirmed remote part is missing")?;
                            ensure!(
                                stored.size == part.size
                                    && *receipt
                                        == blake3::hash(stored.etag.as_bytes()).to_hex().as_str(),
                                "confirmed remote part differs"
                            );
                            Some(stored.etag.clone())
                        } else {
                            stored
                                .filter(|stored| {
                                    stored.size == part.size
                                        && stored.etag.trim_matches('"') == hex::encode(expected)
                                })
                                .map(|stored| stored.etag.clone())
                        };
                        let etag = if let Some(etag) = confirmed {
                            etag
                        } else {
                            let etag = streaming
                                .part(
                                    &key,
                                    &upload,
                                    usize::from(part.number) - 1,
                                    part.size,
                                    &expected,
                                    || replay(&input, part.offset, part.size, expected),
                                )
                                .await?
                                .content_id;
                            copied.bytes += part.size;
                            copied.requests += 1;
                            etag
                        };
                        #[cfg(feature = "fault-injection")]
                        crate::faults::point("raw-transfer-part").await;
                        if receipt.is_none() {
                            checkpoint(RawProgress::Part {
                                upload: Some(upload.clone()),
                                number: part.number,
                                etag: etag.clone(),
                            })
                            .await?;
                        }
                        parts.push((i32::from(part.number), etag));
                        #[cfg(feature = "fault-injection")]
                        crate::faults::point("raw-transfer-checkpoint").await;
                    }
                    streaming
                        .complete(&key, &upload, &parts)
                        .await
                        .and_then(|completed| {
                            ensure!(completed, "raw multipart completion failed");
                            Ok(())
                        })
                };
                // A lost response can follow a durable write; verify its bytes before publishing.
                if !verify_remote(backend.as_ref(), &key, size, &digest, fill).await? {
                    written?;
                    anyhow::bail!("raw destination missing after write");
                }
            }
            streaming
                .abort_pending_multipart(&key, multipart, None)
                .await?;
        }
    }
    #[cfg(feature = "fault-injection")]
    crate::faults::point("raw-transfer-written").await;
    checkpoint(RawProgress::Finished {
        digest: digest.to_vec(),
    })
    .await?;
    Ok(copied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Id,
        raw::RawSource,
        transfer::{Input, Part},
    };
    use uuid::Uuid;

    fn plan(bytes: &[u8]) -> RawCopy {
        let source = RawSource {
            id: Id::Uuid(Uuid::now_v7()),
            location_id: Uuid::now_v7(),
            storage_id: Uuid::new_v4(),
            size: bytes.len() as i64,
            digest: Some(Md5::digest(bytes).to_vec()),
            state: "ready".into(),
            medium: "local".into(),
            pending_upload: false,
            revision: 1,
            quarantine_code: None,
        };
        let mut target = source.clone();
        target.id = Id::Uuid(Uuid::now_v7());
        target.location_id = Uuid::now_v7();
        target.storage_id = Uuid::new_v4();
        target.state = "writing".into();
        let parts: Vec<_> = bytes
            .chunks(5 * 1024 * 1024)
            .enumerate()
            .map(|(index, bytes)| Part {
                number: index as u16 + 1,
                offset: (index * 5 * 1024 * 1024) as u64,
                size: bytes.len() as u64,
                digest: Md5::digest(bytes).to_vec(),
            })
            .collect();
        RawCopy {
            unit: Id::Uuid(Uuid::now_v7()),
            revision: 1,
            input: Input::Raw { source },
            candidate: Id::Uuid(Uuid::now_v7()),
            target,
            upload: None,
            receipts: vec![None; parts.len()],
            parts,
        }
    }

    #[tokio::test]
    async fn local_transfer_resumes_confirmed_parts_and_reconciles_completed_output() -> Result<()>
    {
        let bytes = Bytes::from(vec![29; 6 * 1024 * 1024 + 13]);
        let mut plan = plan(&bytes);
        let digest: [u8; 16] = Md5::digest(&bytes).into();
        let directory = std::env::temp_dir().join(format!("mokyu-transfer-{}", Uuid::new_v4()));
        let destination = directory.join("data");
        let mut source = RawFile::Memory {
            data: bytes.clone(),
            offset: 0,
        };
        let error = copy(
            Target::Local(destination.clone()),
            &plan,
            digest,
            &mut source,
            &mut None,
            |progress| async move {
                ensure!(
                    matches!(progress, RawProgress::Part { number: 1, .. }),
                    "unexpected checkpoint"
                );
                anyhow::bail!("interrupted receipt")
            },
        )
        .await;
        assert!(error.is_err());
        assert!(!destination.exists());
        let mut changed = bytes.to_vec();
        changed[..5 * 1024 * 1024].fill(0);
        source = RawFile::Memory {
            data: Bytes::from(changed),
            offset: 0,
        };
        plan.receipts[0] = Some(
            blake3::hash(hex::encode(&plan.parts[0].digest).as_bytes())
                .to_hex()
                .to_string(),
        );
        let copied = copy(
            Target::Local(destination.clone()),
            &plan,
            digest,
            &mut source,
            &mut None,
            |progress| async move {
                ensure!(
                    matches!(
                        progress,
                        RawProgress::Part { number: 2, .. } | RawProgress::Finished { .. }
                    ),
                    "confirmed part was replayed"
                );
                Ok(())
            },
        )
        .await?;
        assert_eq!(copied.bytes, 1024 * 1024 + 13);
        assert_eq!(tokio::fs::read(&destination).await?, bytes);
        source = RawFile::Memory {
            data: Bytes::new(),
            offset: 0,
        };
        let copied = copy(
            Target::Local(destination.clone()),
            &plan,
            digest,
            &mut source,
            &mut None,
            |progress| async move {
                ensure!(
                    matches!(progress, RawProgress::Finished { .. }),
                    "completed output was replayed"
                );
                Ok(())
            },
        )
        .await?;
        assert_eq!(copied.requests, 0);
        tokio::fs::remove_dir_all(&directory).await?;
        Ok(())
    }

    #[tokio::test]
    async fn transfer_rejects_changed_data_and_noncontiguous_receipts() -> Result<()> {
        let bytes = Bytes::from(vec![31; 6 * 1024 * 1024]);
        let mut plan = plan(&bytes);
        plan.receipts[1] = Some("0".repeat(64));
        assert!(plan.validate().is_err());
        plan.receipts[1] = None;
        plan.parts[1].offset += 1;
        assert!(plan.validate().is_err());
        plan.parts[1].offset -= 1;
        plan.validate()?;
        let mut source = RawFile::Memory {
            data: Bytes::from_static(b"changed"),
            offset: 0,
        };
        let input = Mutex::new(&mut source);
        let mut body = replay(&input, 0, 7, Md5::digest(b"correct").into());
        assert_eq!(body.next().await.unwrap()?, b"changed"[..]);
        assert!(body.next().await.unwrap().is_err());
        assert!(
            RawProgress::Started {
                upload: "bad\nvalue".into()
            }
            .validate()
            .is_err()
        );
        Ok(())
    }
}
