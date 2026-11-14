use super::*;
use crate::{
    model::journal::{
        Draft, Ledger,
        policy::{Policy, WriteMode},
        publication,
        record::{Attributes, Change, Operation, Prepared},
    },
    storage::journal::{
        batch::{self, Decoder},
        replay::{Catalog, stage_segment},
    },
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::json;
use sqlx::types::Json;

fn event(bucket: Uuid, seq: i64, operation: Operation, metadata: serde_json::Value) -> Prepared {
    let put = operation == Operation::Put;
    Prepared {
        change: Change {
            bucket,
            name: "journal-test".into(),
            generation: 1,
            seq,
            operation,
            key: put.then(|| format!("key-{seq}")),
            ranges: Vec::new(),
            attributes: put.then(|| Attributes {
                history: None,
                size: 0,
                etag: "empty".into(),
                metadata,
                checksums: json!({}),
                payload: "chunks".into(),
                modified_us: 1,
            }),
        },
        materials: Vec::new(),
        keys: Vec::new(),
    }
}

fn ledger(policy: Policy) -> Ledger {
    Ledger {
        local: false,
        storage_id: Uuid::new_v4(),
        policy: Json(policy),
        revision: 1,
        descriptor: None,
        marker_installed: true,
        allocated_seq: 0,
        allocated_digest: None,
        emitted_seq: 0,
        open_file: None,
        flush_requested: false,
        last_bucket: None,
        now_us: 600_000_000,
    }
}

fn draft(format: &Format<'_>, event: Prepared) -> Result<Draft> {
    let bucket_id = event.change.bucket;
    let seq = event.change.seq;
    let bytes = serde_json::to_vec(&batch::records(vec![event], format)?)?;
    let encrypted = format.encode(
        FrameType::Draft,
        &draft_path(bucket_id, seq, 0),
        Sequence {
            first: 1,
            last: 1,
            previous: [0; 32],
        },
        &bytes,
    )?;
    Ok(Draft {
        bucket_id,
        seq,
        page: 0,
        encrypted,
        final_page: true,
        recorded_us: 600_000_000,
    })
}

#[test]
fn complete_aggregate_and_hard_limit_round_trip() -> Result<()> {
    let key = crate::content::codec::Key::new("aes-256-gcm", [21; 32])?;
    let format = Format {
        storage: Uuid::from_u128(77),
        encrypted: true,
        journal: Uuid::new_v4(),
        epoch: Uuid::new_v4(),
        key: &key,
    };
    let bucket = Uuid::new_v4();
    let mut random = vec![0; 11000];
    blake3::Hasher::new()
        .update(b"journal aggregation fixture")
        .finalize_xof()
        .fill(&mut random);
    for limit in [32 * 1024 * 1024, 4096] {
        let policy = Policy {
            put_target_bytes: 4096,
            segment_bytes: limit,
            ..Default::default()
        };
        let mut ledger = ledger(policy);
        ledger.flush_requested = true;
        let drafts = vec![
            draft(&format, event(bucket, 1, Operation::Start, json!({})))?,
            draft(
                &format,
                event(
                    bucket,
                    2,
                    Operation::Put,
                    json!({"sample":STANDARD.encode(&random)}),
                ),
            )?,
            draft(&format, event(bucket, 3, Operation::Ready, json!({})))?,
        ];
        let (input, ids) = publication::unpack(&format, drafts)?;
        assert!(ids.is_empty());
        let plan = publication::allocate(&ledger, None, input, Vec::new(), &format)?;
        assert_eq!(plan.files.len() == 1, limit > 4096);
        let mut reader = Decoder::default();
        let mut catalog = Catalog::default();
        let mut previous = [0; 32];
        let mut first = 1;
        for file in &plan.files {
            assert_eq!(file.state, "sealed");
            let data: Vec<u8> = plan
                .frames
                .iter()
                .filter(|(id, _, _)| *id == file.id)
                .flat_map(|(_, _, data)| data.iter().copied())
                .collect();
            assert!(data.len() <= limit as usize);
            let (last, digest) = stage_segment(
                &format,
                &file.path()?,
                &data,
                first,
                previous,
                &mut reader,
                |record| catalog.stage(record, &format),
            )?;
            previous = digest;
            first = last + 1;
        }
        assert!(reader.complete());
        catalog.apply()?;
        assert_eq!(catalog.buckets[&bucket].objects.len(), 1);
        assert_eq!(catalog.buckets[&bucket].sequence, 3);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable managed-S3 test namespace"]
async fn compare_single_and_separate_watermark() -> Result<()> {
    use crate::storage::{
        external::{Connection, objects::Condition},
        model::Options,
    };
    use bytes::Bytes;
    use std::{
        collections::BTreeMap,
        fs,
        time::{Duration, Instant},
    };
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let settings: serde_json::Value =
        serde_json::from_slice(&fs::read(std::env::var("MOKYU_TEST_JOURNAL_BACKEND")?)?)?;
    let field = |name: &str| {
        settings[name]
            .as_str()
            .context("missing fixture connection field")
    };
    let connection = Connection {
        endpoint_url: field("endpoint")?.into(),
        region: field("region")?.into(),
        bucket: field("bucket")?.into(),
        prefix: format!("protocol-test/{}", Uuid::new_v4().simple()),
        virtual_hosted: false,
        allow_http: true,
    };
    let options = Options {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(30),
        retry_timeout: Duration::from_secs(30),
        max_retries: 0,
    };
    let remote = connection.connect(field("access_key")?, field("secret_key")?, options)?;
    let key = crate::content::codec::Key::new("aes-256-gcm", [58; 32])?;
    for separate in [false, true] {
        let format = Format {
            storage: Uuid::from_u128(77),
            encrypted: true,
            journal: Uuid::new_v4(),
            epoch: Uuid::new_v4(),
            key: &key,
        };
        let mark = format!("{ROOT}/watermarks/{}", format.epoch.simple());
        let mut previous = [0; 32];
        let mut etag = None;
        let mut elapsed = Vec::new();
        for seq in 1..=32u64 {
            let path = segment_path(format.epoch, seq);
            let seqs = Sequence {
                first: seq,
                last: seq,
                previous,
            };
            let mut body =
                format.encode(FrameType::Changes, &path, seqs, b"protocol comparison")?;
            let seal = format.encode(
                FrameType::Seal,
                &path,
                Sequence {
                    previous: *blake3::hash(&body).as_bytes(),
                    ..seqs
                },
                &[],
            )?;
            previous = *blake3::hash(&seal).as_bytes();
            body.extend(seal);
            let start = Instant::now();
            remote
                .put(
                    &path,
                    &Condition::Absent,
                    &BTreeMap::new(),
                    None,
                    Bytes::from(body),
                )
                .await?;
            if separate {
                let state = super::snapshot::envelope(
                    &format,
                    FrameType::Watermark,
                    &mark,
                    seq,
                    &(seq, previous),
                )?;
                let condition = etag
                    .take()
                    .map(Condition::Match)
                    .unwrap_or(Condition::Absent);
                etag = Some(
                    remote
                        .put(
                            &mark,
                            &condition,
                            &BTreeMap::new(),
                            None,
                            Bytes::from(state),
                        )
                        .await?
                        .etag,
                );
            }
            elapsed.push(start.elapsed().as_micros());
        }
        elapsed.sort_unstable();
        println!(
            "{}",
            json!({"separate_watermark":separate,"segments":32,"put_requests":if separate {64}else{32},
            "median_us":elapsed[16],"p95_us":elapsed[30],"p99_us":elapsed[31]})
        );
    }
    Ok(())
}

#[test]
fn snapshot_pages_bind_their_identity_and_order() -> Result<()> {
    use super::snapshot::{BucketCut, Row, decode_page, encode_pages};
    let key = crate::content::codec::Key::new("aes-256-gcm", [39; 32])?;
    let format = Format {
        storage: Uuid::from_u128(77),
        encrypted: true,
        journal: Uuid::new_v4(),
        epoch: Uuid::new_v4(),
        key: &key,
    };
    let rows: Vec<_> = (0..512)
        .map(|i| {
            Row::Bucket(BucketCut {
                id: Uuid::new_v4(),
                name: format!("bucket-{i}"),
                generation: 1,
                sequence: 2,
                stopped: false,
            })
        })
        .collect();
    let pages = encode_pages(&format, Uuid::new_v4(), 0, &rows, 4096, None, None, None)?;
    assert!(pages.len() > 1);
    let mut decoder = Decoder::default();
    let mut seen = 0;
    for (page, body) in &pages {
        decode_page(&format, page, body, &mut decoder, |_| {
            seen += 1;
            Ok(())
        })?;
        assert!(
            decode_page(
                &format,
                page,
                &body[..body.len() - 1],
                &mut Decoder::default(),
                |_| Ok(())
            )
            .is_err()
        );
    }
    assert!(decoder.complete());
    assert_eq!(seen, rows.len());
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated S3 journal exported into MOKYU_TEST_JOURNAL_DIR"]
async fn replay_published_s3_catalog() -> Result<()> {
    use std::{
        fs,
        path::{Path, PathBuf},
    };
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let root = PathBuf::from(std::env::var("MOKYU_TEST_JOURNAL_DIR")?);
    let (_, secrets, _) =
        crate::config::Config::load(Path::new(&std::env::var("MOKYU_TEST_CONFIG")?))?;
    let marker: serde_json::Value = serde_json::from_slice(&fs::read(root.join("mokyu.json"))?)?;
    let identity = crate::storage::marker::identity(&marker)?;
    let descriptor =
        crate::storage::marker::journal(&marker)?.context("missing fixture descriptor")?;
    let key = descriptor.unlock(identity.storage_id, &secrets.credential_key)?;
    let format = Format {
        storage: Uuid::from_u128(77),
        encrypted: true,
        journal: descriptor.journal_id,
        epoch: descriptor.writer_epoch,
        key: &key,
    };
    let dir = root
        .join(ROOT)
        .join("changes")
        .join(format.epoch.simple().to_string());
    let mut files = fs::read_dir(&dir)?.collect::<std::io::Result<Vec<_>>>()?;
    files.sort_by_key(|file| file.file_name());
    let mut first = 1;
    let mut previous = [0; 32];
    let mut decoder = Decoder::default();
    let mut catalog = Catalog::default();
    for file in files {
        let path = segment_path(format.epoch, first);
        ensure!(
            root.join(&path) == file.path(),
            "unexpected journal sequence or file"
        );
        let (last, digest) = stage_segment(
            &format,
            &path,
            &fs::read(file.path())?,
            first,
            previous,
            &mut decoder,
            |record| catalog.stage(record, &format),
        )?;
        first = last + 1;
        previous = digest;
    }
    ensure!(decoder.complete(), "incomplete final journal record");
    catalog.apply()?;
    let expected: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("expected.json"))?)?;
    for (name, keys) in expected
        .as_object()
        .context("invalid fixture expectations")?
    {
        let bucket = catalog.buckets.values().find(|b| &b.name == name);
        if keys.is_null() {
            ensure!(bucket.is_none(), "disabled bucket was included");
            continue;
        }
        let bucket = bucket.context("expected bucket missing")?;
        let keys: Vec<String> = serde_json::from_value(keys.clone())?;
        ensure!(
            bucket.objects.keys().eq(keys.iter()),
            "journal keys differ from fixture"
        );
    }
    Ok(())
}

#[test]
fn multipart_small_tail_waits_for_its_own_deadline() -> Result<()> {
    let key = crate::content::codec::Key::new("aes-256-gcm", [53; 32])?;
    let format = Format {
        storage: Uuid::from_u128(77),
        encrypted: true,
        journal: Uuid::new_v4(),
        epoch: Uuid::new_v4(),
        key: &key,
    };
    let mut ledger = ledger(Policy {
        mode: WriteMode::Multipart,
        part_max_wait_seconds: Some(10),
        multipart_max_open_seconds: None,
        ..Default::default()
    });
    let draft = draft(
        &format,
        event(Uuid::new_v4(), 1, Operation::Start, json!({})),
    )?;
    let (input, _) = publication::unpack(&format, vec![draft])?;
    let first = publication::allocate(&ledger, None, input, Vec::new(), &format)?;
    assert!(first.open.is_some());
    ledger.allocated_seq = first.sequence;
    ledger.allocated_digest = Some(first.digest.to_vec());
    ledger.open_file = first.open;
    let file = first.files.into_iter().next().unwrap();
    ledger.now_us += 9_999_999;
    let before =
        publication::allocate(&ledger, Some(file.clone()), Vec::new(), Vec::new(), &format)?;
    assert!(before.frames.is_empty());
    ledger.now_us += 1;
    let after = publication::allocate(&ledger, Some(file), Vec::new(), Vec::new(), &format)?;
    assert_eq!(after.files[0].state, "sealed");
    Ok(())
}
#[test]
fn native_allocation_binds_physical_frames_without_s3_waiting() -> Result<()> {
    use crate::{
        content::fragment::Area,
        model::{
            storage_cluster::{Acknowledgement, Layout},
            storage_payload::Intent,
            storage_recovery::{self, CatalogEntry, Part, Physical},
        },
    };
    let key = crate::content::codec::Key::new("aes-256-gcm", [31; 32])?;
    for encrypted in [false, true] {
        let format = Format {
            storage: Uuid::new_v4(),
            journal: Uuid::new_v4(),
            epoch: Uuid::new_v4(),
            key: &key,
            encrypted,
        };
        let mut state = ledger(Policy {
            mode: WriteMode::Multipart,
            segment_bytes: 4096,
            ..Default::default()
        });
        state.storage_id = format.storage;
        let bucket = Uuid::new_v4();
        let mut random = vec![0; 11000];
        blake3::Hasher::new()
            .update(b"mixed native journal")
            .finalize_xof()
            .fill(&mut random);
        let (input, _) = publication::unpack(
            &format,
            vec![
                draft(&format, event(bucket, 1, Operation::Start, json!({})))?,
                draft(
                    &format,
                    event(
                        bucket,
                        2,
                        Operation::Put,
                        json!({"sample": STANDARD.encode(random)}),
                    ),
                )?,
                draft(&format, event(bucket, 3, Operation::Ready, json!({})))?,
            ],
        )?;
        let cluster = Uuid::new_v4();
        let group = Uuid::new_v4();
        let carrier = Intent {
            operation: Uuid::new_v4(),
            namespace: format.storage,
            payload: Uuid::new_v4(),
            revision: 1,
            cluster,
            layout_revision: 1,
            takeover_epoch: 1,
            group,
            layout: Layout {
                data_shards: 128,
                ..Default::default()
            },
            members: (0..256).map(|_| Uuid::new_v4()).collect(),
            area: Area::Blocks,
            total: 8192,
            acknowledgement: Acknowledgement::All,
        };
        let physical = vec![
            Physical::Payload {
                sequence: 1,
                intent: carrier.clone(),
                path: "objects/sample".into(),
            },
            Physical::State {
                sequence: 2,
                operation: carrier.operation,
                published: true,
            },
        ];
        let expected = serde_json::to_value(&physical)?;
        let plan = publication::allocate_native(&state, input, physical, vec![], &format)?;
        assert!(plan.files.len() > 1);
        assert!(plan.open.is_none());
        assert_eq!(plan.pages.len(), 3);
        let mut decoders = storage_recovery::verify::Decoders::default();
        let mut mappings = Vec::new();
        let mut catalog = Catalog::default();
        let mut business = Decoder::default();
        let mut ordered = Catalog::default();
        let (mut first, mut previous) = (1, [0; 32]);
        for file in &plan.files {
            assert_eq!((file.mode.as_str(), file.state.as_str()), ("put", "sealed"));
            let bytes: Vec<_> = plan
                .frames
                .iter()
                .filter(|(id, _, _)| *id == file.id)
                .flat_map(|(_, _, frame)| frame.iter().copied())
                .collect();
            assert!(bytes.len() <= 4096);
            assert_eq!(file.byte_count as usize, bytes.len());
            let part = Part {
                intent: Intent {
                    operation: Uuid::new_v4(),
                    payload: file.id,
                    layout: Layout::default(),
                    members: vec![],
                    area: Area::Recovery,
                    total: bytes.len() as u64,
                    ..carrier.clone()
                },
                path: file.path()?,
                digest: *blake3::hash(&bytes).as_bytes(),
            };
            part.visit(&bytes, |frame| {
                if frame.envelope.kind == FrameType::Mappings {
                    assert!(!frame.encrypted);
                }
                if frame.envelope.kind == FrameType::Changes {
                    assert_eq!(frame.encrypted, encrypted);
                }
                assert!(frame.open(&format)?.consumed <= 4096);
                Ok(())
            })?;
            storage_recovery::verify::visit(
                &part,
                &bytes,
                2,
                Some(&format),
                &mut decoders,
                |entry| {
                    match entry {
                        CatalogEntry::Physical(_, record) => mappings.push(record),
                        CatalogEntry::Business { record, .. } => catalog.stage(record, &format)?,
                        _ => panic!("unexpected recovery entry"),
                    }
                    Ok(())
                },
            )?;
            let (last, digest) = stage_segment(
                &format,
                &file.path()?,
                &bytes,
                first,
                previous,
                &mut business,
                |record| ordered.stage(record, &format),
            )?;
            first = last + 1;
            previous = digest;
        }
        assert!(decoders.complete() && business.complete());
        assert_eq!(serde_json::to_value(mappings)?, expected);
        catalog.apply()?;
        ordered.apply()?;
        assert_eq!(catalog.buckets[&bucket].objects.len(), 1);
        assert_eq!(catalog.buckets[&bucket].sequence, 3);
        assert_eq!(ordered.buckets[&bucket].objects.len(), 1);
        assert_eq!(ordered.buckets[&bucket].sequence, 3);
        let only_physical = publication::allocate_native(
            &state,
            vec![],
            vec![Physical::State {
                sequence: 3,
                operation: carrier.operation,
                published: false,
            }],
            vec![],
            &format,
        )?;
        assert_eq!(only_physical.files.len(), 1);
        assert!(only_physical.members.is_empty() && only_physical.pages.is_empty());
        assert_eq!(only_physical.files[0].state, "sealed");
    }
    Ok(())
}
