use super::*;

#[test]
fn mappings_reject_gaps_overflow_and_source_range_escape() {
    let id = Uuid::new_v4();
    let segment = read::Segment {
        offset: 10,
        length: 3,
        source_offset: 2,
        source: read::Source::Staged {
            fragment: Uuid::new_v4(),
            storage: Uuid::new_v4(),
            key: "__mokyu/multipart/test".into(),
            size: 5,
            hash: vec![0; 32],
        },
    };
    let original = read::Mapping {
        stream: id,
        revision: 4,
        start: 10,
        end: 13,
        segments: vec![segment],
    };
    validate_mapping(&original, id, 4, 10, 13).unwrap();
    assert!(validate_mapping(&original, Uuid::new_v4(), 4, 10, 13).is_err());
    assert!(validate_mapping(&original, id, 5, 10, 13).is_err());
    for change in 0..5 {
        let mut value = original.clone();
        match change {
            0 => value.segments[0].offset += 1,
            1 => value.segments[0].source_offset += 1,
            2 => value.segments[0].source_offset = -1,
            3 => value.segments[0].length = i64::MAX,
            _ => value.segments.clear(),
        }
        assert!(validate_mapping(&value, id, 4, 10, 13).is_err());
    }
}

#[tokio::test]
async fn returned_slices_hold_buffer_and_request_memory() {
    let buffers = Arc::new(Semaphore::new(2));
    let slots = Arc::new(Semaphore::new(1));
    let output = Bytes::from_owner(Output {
        bytes: Bytes::from_static(b"retained"),
        _memory: buffers.clone().acquire_many_owned(2).await.unwrap(),
        _slot: Arc::new(slots.clone().acquire_owned().await.unwrap()),
    });
    let tail = output.slice(3..);
    drop(output);
    assert_eq!(buffers.available_permits(), 0);
    assert_eq!(slots.available_permits(), 0);
    drop(tail);
    assert_eq!(buffers.available_permits(), 2);
    assert_eq!(slots.available_permits(), 1);
}

#[tokio::test]
async fn dropping_body_cancels_its_window_prefetch() {
    let gate = Arc::new(Semaphore::new(1));
    let permit = gate.clone().acquire_owned().await.unwrap();
    let task = Prefetch(tokio::spawn(async move {
        let _permit = permit;
        std::future::pending::<Result<Prepared>>().await
    }));
    drop(task);
    let _permit = tokio::time::timeout(std::time::Duration::from_secs(1), gate.acquire())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn shared_raw_reader_validates_content_and_pins_remote_etag() -> Result<()> {
    use md5::{Digest, Md5};
    let backend = Arc::new(object_store::memory::InMemory::new());
    let bytes = Bytes::from_static(b"raw range bytes");
    let source = crate::model::raw::RawSource {
        id: Uuid::new_v4().into(),
        location_id: Uuid::new_v4(),
        storage_id: Uuid::new_v4(),
        size: bytes.len() as i64,
        digest: Some(Md5::digest(&bytes).to_vec()),
        state: "ready".into(),
        medium: "shared_s3".into(),
        pending_upload: false,
        revision: 1,
        quarantine_code: None,
    };
    let path = Path::from(format!(
        "raw/{}",
        crate::storage::chunk_name(source.location_id)
    ));
    backend.put(&path, bytes.clone().into()).await?;
    let gets = Arc::new(AtomicU64::new(0));
    let diagnostics = Arc::new(Diagnostics::default());
    let mut file =
        RawFile::remote(backend.clone(), &source, gets.clone(), diagnostics.clone()).await?;
    assert_eq!(file.verify(&source).await?, source.size as u64);
    file.seek(std::io::SeekFrom::Start(4)).await?;
    let mut part = [0; 5];
    assert_eq!(file.read(&mut part).await?, part.len());
    assert_eq!(&part, b"range");
    let mut bad = source.clone();
    bad.digest = Some(vec![0; 16]);
    file.rewind().await?;
    assert!(file.verify(&bad).await.is_err());
    backend
        .put(&path, Bytes::from_static(b"changed content").into())
        .await?;
    file.rewind().await?;
    assert!(file.read(&mut part).await.is_err());
    bad.size += 1;
    assert!(
        RawFile::remote(backend, &bad, gets, diagnostics)
            .await
            .is_err()
    );
    Ok(())
}
