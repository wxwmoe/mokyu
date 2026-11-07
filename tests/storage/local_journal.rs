use super::*;
use crate::content::codec::Key;

#[test]
fn restart_preserves_the_confirmed_prefix_and_rejects_damage() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let reopen = || Local {
        root: dir.path().to_owned(),
        verified: Mutex::new(None),
    };
    let key = Key::new("aes-256-gcm", [42; 32])?;
    let epoch = Uuid::new_v4();
    let format = Format {
        storage: Uuid::from_u128(77),
        encrypted: true,
        journal: Uuid::new_v4(),
        epoch,
        key: &key,
    };
    let path = segment_path(epoch, 1);
    let mut file = Segment {
        encrypt_frames: true,
        id: Uuid::new_v4(),
        storage_id: Uuid::new_v4(),
        epoch,
        first_seq: 1,
        last_seq: 0,
        previous_digest: vec![0; 32],
        digest: vec![0; 32],
        byte_count: 0,
        mode: "put".into(),
        state: "open".into(),
        opened_us: 0,
        oldest_us: 0,
        part_cut: 0,
        part_since_us: None,
        upload_opened_us: None,
        upload_id: None,
    };
    let frame = |file: &mut Segment, kind: FrameType, sequence: u64| -> Result<Vec<u8>> {
        let bytes = format.encode(
            kind,
            &path,
            Sequence {
                first: if kind == FrameType::Changes {
                    sequence
                } else {
                    1
                },
                last: sequence,
                previous: file.digest.as_slice().try_into()?,
            },
            if kind == FrameType::Changes {
                b"change"
            } else {
                &[]
            },
        )?;
        file.byte_count += i64::try_from(bytes.len())?;
        file.last_seq = i64::try_from(sequence)?;
        file.digest = blake3::hash(&bytes).as_bytes().to_vec();
        Ok(bytes)
    };
    let active = dir.path().join(&path).with_extension("part");
    let mut first = frame(&mut file, FrameType::Changes, 1)?;
    first.extend(frame(&mut file, FrameType::PartCommit, 1)?);
    reopen().append(&format, &file, 0, &first)?;
    let start = file.byte_count as u64;
    let mut next = frame(&mut file, FrameType::Changes, 2)?;
    next.extend(frame(&mut file, FrameType::PartCommit, 2)?);
    OpenOptions::new()
        .append(true)
        .open(&active)?
        .write_all(b"torn")?;
    let mut damaged_batch = next.clone();
    *damaged_batch.last_mut().unwrap() ^= 1;
    assert!(
        reopen()
            .append(&format, &file, start, &damaged_batch)
            .is_err()
    );
    reopen().append(&format, &file, start, &next)?;
    reopen().append(&format, &file, start, &next)?;
    let mut expected = first;
    expected.extend(next);
    assert_eq!(fs::read(&active)?, expected);
    let end = file.byte_count as u64;
    let seal = frame(&mut file, FrameType::Seal, 2)?;
    file.state = "sealed".into();
    let mut damaged = expected.clone();
    *damaged.last_mut().unwrap() ^= 1;
    fs::write(&active, &damaged)?;
    assert!(reopen().append(&format, &file, end, &seal).is_err());
    assert_eq!(fs::read(&active)?, damaged);
    fs::write(&active, &expected[..expected.len() - 1])?;
    assert!(reopen().append(&format, &file, end, &seal).is_err());
    fs::write(&active, &expected)?;
    reopen().append(&format, &file, end, &seal)?;
    reopen().append(&format, &file, end, &seal)?;
    expected.extend(seal);
    assert!(!active.exists());
    assert_eq!(fs::read(dir.path().join(&path))?, expected);
    assert_eq!(
        super::super::verify_segment(&format, &path, &expected, 1, [0; 32], |_| Ok(()))?.0,
        2
    );
    Ok(())
}
