use super::{Draft, Ledger, policy::WriteMode};
use crate::storage::journal::{
    Format, FrameType, Sequence,
    batch::{self, PlainFrame, Record},
    draft_path, segment_path,
};
use anyhow::{Context, Result, ensure};
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

#[derive(Clone, sqlx::FromRow)]
pub(crate) struct File {
    pub encrypt_frames: bool,
    pub id: Uuid,
    pub storage_id: Uuid,
    pub epoch: Uuid,
    pub first_seq: i64,
    pub last_seq: i64,
    pub previous_digest: Vec<u8>,
    pub digest: Vec<u8>,
    pub byte_count: i64,
    pub mode: String,
    pub state: String,
    pub opened_us: i64,
    pub oldest_us: i64,
    pub part_cut: i64,
    pub part_since_us: Option<i64>,
    pub upload_opened_us: Option<i64>,
    pub upload_id: Option<String>,
}
impl File {
    pub fn path(&self) -> Result<String> {
        Ok(segment_path(self.epoch, u64::try_from(self.first_seq)?))
    }
}

#[derive(Clone, sqlx::FromRow)]
pub(crate) struct Part {
    pub file_id: Uuid,
    pub number: i32,
    pub byte_offset: i64,
    pub byte_count: i64,
    pub digest: Vec<u8>,
    pub etag: Option<String>,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Page {
    pub bucket: Uuid,
    pub seq: i64,
    pub number: i32,
}

pub(crate) struct Member {
    pub file: Uuid,
    pub page: Page,
    pub offset: i64,
    pub bytes: i64,
    pub final_page: bool,
}

pub(crate) struct Allocation {
    pub revision: i64,
    pub base: i64,
    pub base_digest: Option<Vec<u8>>,
    pub base_open: Option<Uuid>,
    pub files: Vec<File>,
    pub frames: Vec<(Uuid, i64, Vec<u8>)>,
    pub members: Vec<Member>,
    pub pages: Vec<Page>,
    pub dictionary: Vec<([u8; 32], [u8; 32], Uuid)>,
    pub sequence: i64,
    pub digest: [u8; 32],
    pub open: Option<Uuid>,
}

pub(crate) struct Input {
    encrypted: bool,
    record: Record,
    page: Page,
    end: bool,
    time: i64,
    dictionary: Option<([u8; 32], [u8; 32])>,
}

pub(crate) fn unpack(
    format: &Format<'_>,
    drafts: Vec<Draft>,
) -> Result<(Vec<Input>, Vec<[u8; 32]>)> {
    let mut records = Vec::new();
    let mut dictionary = BTreeMap::new();
    for draft in drafts {
        let path = draft_path(draft.bucket_id, draft.seq, draft.page);
        let frame = format.decode(FrameType::Draft, &path, &draft.encrypted)?;
        ensure!(
            frame.consumed == draft.encrypted.len()
                && frame.sequence.first == u64::try_from(draft.page)? + 1
                && frame.sequence.first == frame.sequence.last
                && frame.sequence.previous == [0; 32],
            "invalid staged journal page"
        );
        let page = Page {
            bucket: draft.bucket_id,
            seq: draft.seq,
            number: draft.page,
        };
        let data: Vec<Record> = serde_json::from_slice(&frame.bytes)?;
        ensure!(
            !data.is_empty() && data.len() <= 512,
            "invalid staged journal record count"
        );
        let last = data.len() - 1;
        for (index, record) in data.into_iter().enumerate() {
            let identity = match &record {
                Record::Key(k) => Some(serde_json::to_vec(&("key", k.id))?),
                Record::Material(m) => Some(serde_json::to_vec(&("material", m.reference()))?),
                _ => None,
            };
            let mut mark = None;
            if let Some(identity) = identity {
                let id = *blake3::hash(&identity).as_bytes();
                let hash = match &record {
                    Record::Key(wrapped) => {
                        let key = wrapped.open(format)?;
                        *blake3::hash(&serde_json::to_vec(&(
                            key.id,
                            key.algorithm,
                            blake3::hash(&key.material).as_bytes(),
                        ))?)
                        .as_bytes()
                    }
                    _ => *blake3::hash(&serde_json::to_vec(&record)?).as_bytes(),
                };
                if let Some(old) = dictionary.insert(id, hash) {
                    ensure!(old == hash, "conflicting journal dictionary entry");
                    continue;
                }
                mark = Some((id, hash));
            }
            let end = index == last && draft.final_page;
            ensure!(
                !end || matches!(record, Record::Change { .. }),
                "journal event has no final record"
            );
            records.push(Input {
                encrypted: frame.encrypted,
                record,
                page,
                end,
                time: draft.recorded_us,
                dictionary: mark,
            });
        }
    }
    Ok((records, dictionary.into_keys().collect()))
}

fn trim_known(records: Vec<Input>, known: Vec<(Vec<u8>, Vec<u8>)>) -> Result<Vec<Input>> {
    let known: HashMap<_, _> = known.into_iter().collect();
    let mut kept = Vec::new();
    for input in records {
        if let Some((id, hash)) = input.dictionary
            && let Some(old) = known.get(id.as_slice())
        {
            ensure!(
                old.as_slice() == hash,
                "published journal dictionary disagrees"
            );
            continue;
        }
        kept.push(input);
    }
    Ok(kept)
}

impl Allocation {
    fn append(
        &mut self,
        file: &mut File,
        data: Vec<u8>,
        last: i64,
        digest: [u8; 32],
    ) -> Result<()> {
        let offset = file.byte_count;
        file.byte_count = offset
            .checked_add(i64::try_from(data.len())?)
            .context("journal segment length overflow")?;
        file.last_seq = last;
        file.digest = digest.to_vec();
        self.sequence = last;
        self.digest = digest;
        self.frames.push((file.id, offset, data));
        Ok(())
    }

    fn seal(&mut self, format: &Format<'_>, mut file: File) -> Result<()> {
        let format = Format {
            encrypted: file.encrypt_frames,
            ..*format
        };
        let data = format.encode(
            FrameType::Seal,
            &file.path()?,
            Sequence {
                first: u64::try_from(file.first_seq)?,
                last: u64::try_from(file.last_seq)?,
                previous: file.digest.as_slice().try_into()?,
            },
            &[],
        )?;
        let digest = *blake3::hash(&data).as_bytes();
        let last = file.last_seq;
        self.append(&mut file, data, last, digest)?;
        file.state = "sealed".into();
        self.files.push(file);
        Ok(())
    }

    fn current(&self, ledger: &Ledger, oldest: i64, epoch: Uuid) -> Result<File> {
        Ok(File {
            encrypt_frames: false,
            id: Uuid::new_v4(),
            storage_id: ledger.storage_id,
            epoch,
            first_seq: self
                .sequence
                .checked_add(1)
                .context("journal sequence exhausted")?,
            last_seq: self.sequence,
            previous_digest: self.digest.to_vec(),
            digest: self.digest.to_vec(),
            byte_count: 0,
            mode: if ledger.policy.mode == WriteMode::Put {
                "put"
            } else {
                "multipart"
            }
            .into(),
            state: "open".into(),
            opened_us: ledger.now_us,
            oldest_us: oldest,
            part_cut: 0,
            part_since_us: None,
            upload_opened_us: None,
            upload_id: None,
        })
    }
}

pub(crate) fn allocate(
    ledger: &Ledger,
    current: Option<File>,
    input: Vec<Input>,
    known: Vec<(Vec<u8>, Vec<u8>)>,
    format: &Format<'_>,
) -> Result<Allocation> {
    let policy = &ledger.policy;
    policy.validate()?;
    let mut plan = Allocation {
        revision: ledger.revision,
        base: ledger.allocated_seq,
        base_digest: ledger.allocated_digest.clone(),
        base_open: ledger.open_file,
        files: Vec::new(),
        frames: Vec::new(),
        members: Vec::new(),
        pages: Vec::new(),
        dictionary: Vec::new(),
        sequence: ledger.allocated_seq,
        digest: ledger
            .allocated_digest
            .as_deref()
            .map(<[u8; 32]>::try_from)
            .transpose()?
            .unwrap_or([0; 32]),
        open: ledger.open_file,
    };
    ensure!(
        current.as_ref().map(|f| f.id) == ledger.open_file
            && current
                .as_ref()
                .is_none_or(|f| f.state == "open" && f.epoch == format.epoch),
        "stale open journal file"
    );
    let inputs = trim_known(input, known)?;
    let mut entries = Vec::new();
    let mut owners = Vec::new();
    let mut pages = std::collections::BTreeSet::new();
    for item in inputs {
        pages.insert(item.page);
        owners.push((
            item.page,
            item.time,
            item.end,
            item.dictionary,
            item.encrypted,
        ));
        entries.push(item.record);
    }
    plan.pages.extend(pages);
    let max = usize::try_from(policy.segment_bytes)?;
    let mut active = current;
    let mut start = 0;
    for group in owners.chunk_by(|a, b| a.4 == b.4) {
        let encrypted = group[0].4;
        let frame_format = Format {
            encrypted,
            ..*format
        };
        let seal_bytes = Format::overhead(encrypted);
        for frame in batch::frames(&entries[start..start + group.len()], max, encrypted)? {
            let oldest = frame
                .origins
                .iter()
                .map(|(i, _, _)| group[*i].1)
                .min()
                .context("journal frame has no members")?;
            let mut file = match active.take() {
                Some(f) => f,
                None => plan.current(ledger, oldest, format.epoch)?,
            };
            let encode = |f: &File, part: &PlainFrame| -> Result<_> {
                batch::encode_frame(
                    &frame_format,
                    &f.path()?,
                    u64::try_from(f.last_seq)?
                        .checked_add(1)
                        .context("journal sequence exhausted")?,
                    f.digest.as_slice().try_into()?,
                    part,
                )
            };
            let (mut bytes, mut last, mut hash) = encode(&file, &frame)?;
            if file.byte_count > 0 && file.byte_count as usize + bytes.len() + seal_bytes > max {
                plan.seal(format, file)?;
                file = plan.current(ledger, oldest, format.epoch)?;
                (bytes, last, hash) = encode(&file, &frame)?;
            }
            ensure!(
                file.byte_count as usize + bytes.len() + seal_bytes <= max,
                "journal frame exceeds segment limit"
            );
            file.encrypt_frames = encrypted;
            file.oldest_us = file.oldest_us.min(oldest);
            file.part_since_us = Some(file.part_since_us.unwrap_or(oldest).min(oldest));
            let mut shares = BTreeMap::new();
            for (index, weight, complete) in frame.origins {
                let (page, _, final_event, mark, _) = group[index];
                let share = shares.entry(page).or_insert((0i64, false));
                share.0 += i64::try_from(weight)?;
                share.1 |= complete && final_event;
                if complete && let Some((id, hash)) = mark {
                    plan.dictionary.push((id, hash, file.id));
                }
            }
            for (page, (bytes, final_page)) in shares {
                plan.members.push(Member {
                    file: file.id,
                    page,
                    offset: file.byte_count,
                    bytes,
                    final_page,
                });
            }
            plan.append(&mut file, bytes, i64::try_from(last)?, hash)?;
            active = Some(file);
        }
        start += group.len();
    }
    plan.open = None;
    if let Some(mut file) = active {
        let format = &Format {
            encrypted: file.encrypt_frames,
            ..*format
        };
        let seal_bytes = Format::overhead(file.encrypt_frames);
        let age = ledger.now_us.saturating_sub(file.oldest_us) / 1_000_000;
        let bytes = usize::try_from(file.byte_count)? + seal_bytes;
        let part_age = file
            .part_since_us
            .map(|since| ledger.now_us.saturating_sub(since) / 1_000_000);
        let part_due = part_age.is_some_and(|age| {
            policy
                .part_max_wait_seconds
                .is_some_and(|s| age >= i64::from(s))
        });
        let tail = file.byte_count - file.part_cut;
        let full_part = tail >= i64::from(policy.part_target_bytes)
            || (part_due && tail >= i64::from(super::policy::MIN_PART));
        let due = ledger.flush_requested
            || bytes >= max
            || match policy.mode {
                WriteMode::Put => policy.put_due(bytes, age, false),
                WriteMode::Multipart => {
                    (part_due && tail > 0 && tail < i64::from(super::policy::MIN_PART))
                        || (full_part && bytes + seal_bytes > max)
                        || policy.multipart_max_open_seconds.is_some_and(|s| {
                            ledger.now_us.saturating_sub(file.opened_us) >= i64::from(s) * 1_000_000
                        })
                }
            };
        if !due && policy.mode == WriteMode::Multipart && full_part {
            let data = format.encode(
                FrameType::PartCommit,
                &file.path()?,
                Sequence {
                    first: u64::try_from(file.first_seq)?,
                    last: u64::try_from(file.last_seq)?,
                    previous: file.digest.as_slice().try_into()?,
                },
                &[],
            )?;
            let hash = *blake3::hash(&data).as_bytes();
            let last = file.last_seq;
            plan.append(&mut file, data, last, hash)?;
            file.part_cut = file.byte_count;
            file.part_since_us = None;
        }
        if due {
            plan.seal(format, file)?;
        } else {
            plan.open = Some(file.id);
            if !plan.frames.is_empty() {
                plan.files.push(file);
            }
        }
    }
    Ok(plan)
}
