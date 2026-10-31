use super::*;
use crate::storage::external::objects::{self, Object, Written};
use crate::{content::codec, model::external::Snapshot};
use anyhow::Context;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawRead {
    pub source: super::super::raw::RawSource,
    pub proof: super::super::transfer::Proof,
    pub offset: u64,
    pub length: u32,
}

impl RawRead {
    pub(crate) fn validate(&self) -> Result<()> {
        self.source.validate()?;
        let end = self
            .proof
            .offset
            .checked_add(u64::from(self.proof.size))
            .context("transfer proof overflow")?;
        ensure!(
            self.proof.size > 0
                && self.proof.size as usize <= super::super::transfer::PROOF_BYTES
                && self.proof.hash.len() == 32
                && end <= self.source.size as u64
                && self.offset >= self.proof.offset
                && self.offset < end
                && self.length > 0
                && u64::from(self.length) <= end - self.offset,
            "invalid transfer read proof"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    #[default]
    Inspect,
    Compare,
    Start,
    Transfer,
    Import,
    Project,
    Complete,
    Locate,
    Verify,
    Permissions,
    Finalize,
    Cleanup,
    Done,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct State {
    #[serde(default)]
    pub previous: Option<super::changes::Target>,
    #[serde(default)]
    pub cleanup_pending: bool,
    pub phase: Phase,
    pub source: Option<Object>,
    pub target: Option<Object>,
    pub source_public: Option<bool>,
    pub target_public: Option<bool>,
    pub upload: Option<String>,
    pub part_size: u64,
    pub position: u16,
    pub written: Option<Written>,
    pub output: Option<Object>,
    pub starts: u32,
    pub changed: bool,
    pub finish: Option<Finish>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Finish {
    pub status: String,
    pub error: Option<String>,
}

pub(crate) struct Change {
    pub state: State,
    pub part: Option<Part>,
    pub finish: Option<Finish>,
}

impl Change {
    pub(crate) fn source_changed(&mut self) {
        self.state.finish("conflict", Some("object_changed"));
        self.finish = (self.state.phase == Phase::Done).then(|| self.state.finish.clone().unwrap());
    }
}

impl State {
    pub(crate) fn range(&self, number: i32) -> Result<(u64, u64)> {
        let object = self.source.as_ref().context("transfer source missing")?;
        ensure!(
            self.part_size == objects::part_size(object.size)?,
            "invalid transfer part size"
        );
        ensure!(
            number > 0 && number <= 10000,
            "invalid transfer part number"
        );
        let start = (number as u64 - 1) * self.part_size;
        ensure!(start < object.size, "transfer part exceeds source");
        Ok((start, self.part_size.min(object.size - start)))
    }

    pub(crate) fn parts(&self) -> Result<u16> {
        let source = self.source.as_ref().context("transfer source missing")?;
        ensure!(
            self.part_size == objects::part_size(source.size)?,
            "invalid transfer part size"
        );
        Ok(u16::try_from(source.size.div_ceil(self.part_size))?)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        for object in self
            .source
            .iter()
            .chain(self.target.iter())
            .chain(self.output.iter())
            .chain(self.previous.iter().map(|previous| &previous.object))
        {
            object.validate()?;
        }
        ensure!(
            self.upload
                .as_ref()
                .is_none_or(|value| !value.is_empty() && value.len() <= 4096),
            "invalid transfer upload id"
        );
        if matches!(self.phase, Phase::Inspect) {
            ensure!(
                self == &Self {
                    previous: self.previous.clone(),
                    cleanup_pending: self.cleanup_pending,
                    ..Self::default()
                },
                "invalid initial transfer state"
            );
        } else if !matches!(self.phase, Phase::Cleanup | Phase::Done) {
            ensure!(
                self.position <= self.parts()?,
                "transfer position exceeds source"
            );
        }
        if matches!(self.phase, Phase::Transfer) {
            ensure!(self.upload.is_some(), "transfer upload missing");
        }
        if matches!(
            self.phase,
            Phase::Compare | Phase::Transfer | Phase::Import | Phase::Verify
        ) {
            ensure!(
                self.position < self.parts()?,
                "transfer position is complete"
            );
        }
        if matches!(self.phase, Phase::Verify | Phase::Finalize) {
            let output = self.output.as_ref().context("transfer output missing")?;
            let source = self.source.as_ref().context("transfer source missing")?;
            ensure!(
                output.size == source.size && output.metadata == source.metadata,
                "transfer output differs"
            );
        }
        if let Some(finish) = &self.finish {
            ensure!(
                matches!(self.phase, Phase::Cleanup | Phase::Done)
                    && matches!(
                        finish.status.as_str(),
                        "completed" | "skipped" | "conflict" | "cancelled"
                    )
                    && finish.error.as_ref().is_none_or(|code| code.len() <= 24
                        && code
                            .bytes()
                            .all(|byte| byte.is_ascii_lowercase() || byte == b'_')),
                "invalid transfer result"
            );
        }
        Ok(())
    }

    fn finish(&mut self, status: &str, error: Option<&str>) {
        self.finish = Some(Finish {
            status: status.into(),
            error: error.map(str::to_owned),
        });
        self.phase = if self.upload.is_some() {
            Phase::Cleanup
        } else {
            Phase::Done
        };
    }

    pub(crate) fn cancel(&mut self) {
        self.finish(
            "cancelled",
            (self.cleanup_pending
                || self.starts > 1
                || self.phase == Phase::Start && self.starts > 0)
                .then_some("upload_cleanup_pending"),
        );
    }

    fn write(&mut self, options: &Options) -> Result<()> {
        self.changed = true;
        self.position = 0;
        self.phase = if self.parts()? == 0 {
            Phase::Complete
        } else if options.target_external {
            Phase::Start
        } else {
            Phase::Import
        };
        Ok(())
    }

    fn conflict(&mut self, options: &Options) -> Result<()> {
        if self
            .previous
            .as_ref()
            .is_some_and(|previous| previous.matches(self.target.as_ref(), self.target_public))
        {
            return self.write(options);
        }
        match options.conflict {
            Conflict::Skip => self.finish("skipped", None),
            Conflict::Report => self.finish("conflict", Some("target_exists")),
            Conflict::Overwrite => self.write(options)?,
        }
        Ok(())
    }

    fn matched(&mut self, options: &Options) -> Result<()> {
        let source = self.source.as_ref().context("transfer source missing")?;
        let target = self.target.as_ref().context("transfer target missing")?;
        let permissions_changed = options.permissions != Permissions::Ignore
            && self.target_public
                != Some(
                    options
                        .permissions
                        .public(self.source_public.unwrap_or(false), self.target_public),
                );
        if !options.target_external && (source.metadata != target.metadata || permissions_changed) {
            self.changed = true;
            self.position = 0;
            self.phase = Phase::Project;
            return Ok(());
        }
        if source.metadata != target.metadata {
            return self.write(options);
        }
        self.output = Some(target.clone());
        if permissions_changed {
            if target.version.is_none() {
                self.finish("conflict", Some("target_acl_unfenced"));
            } else {
                self.changed = true;
                self.phase = Phase::Permissions;
            }
        } else {
            self.phase = Phase::Finalize;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, sqlx::FromRow)]
#[serde(deny_unknown_fields)]
pub(crate) struct Part {
    pub number: i32,
    pub size: i64,
    pub etag: String,
    pub digest: Vec<u8>,
}

impl Part {
    pub(crate) fn validate_record(&self) -> Result<()> {
        ensure!(
            (1..=10000).contains(&self.number)
                && (0..=objects::MAX_PART as i64).contains(&self.size)
                && self.digest.len() == 32
                && objects::strong_etag(&self.etag),
            "invalid transfer part receipt"
        );
        Ok(())
    }

    pub(crate) fn validate(&self, state: &State) -> Result<()> {
        self.validate_record()?;
        let (_, size) = state.range(self.number)?;
        ensure!(self.size == size as i64, "invalid transfer part size");
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Plan {
    pub task: Uuid,
    pub generation: i64,
    pub options: Options,
    pub cursor: Option<String>,
    pub item: Option<Id>,
    pub key: Option<String>,
    pub state: State,
    pub parts: Vec<Part>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<Native>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incoming: Option<super::import::Write>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Native {
    pub stream: Id,
    pub object: Object,
    pub public: bool,
}

impl Native {
    pub(crate) fn new(stream: Id, source: &super::Snapshot) -> Result<Self> {
        ensure!(source.size >= 0, "invalid native source size");
        let metadata: crate::app::Metadata = serde_json::from_value(source.metadata.clone())?;
        let headers = crate::http::object::metadata_headers(&metadata)?;
        let mut values = std::collections::BTreeMap::new();
        for (name, value) in &headers {
            values.insert(name.as_str().into(), value.to_str()?.into());
        }
        for (name, value) in metadata.user.unwrap_or_default() {
            values.insert(format!("x-amz-meta-{}", name.to_ascii_lowercase()), value);
        }
        let object = Object {
            size: source.size as u64,
            etag: format!("\"{}\"", source.etag),
            version: Some(stream.to_string()),
            modified: None,
            metadata: values,
        };
        object.validate()?;
        Ok(Self {
            stream,
            object,
            public: source.public,
        })
    }
}

impl Plan {
    pub(crate) fn validate(&self) -> Result<()> {
        self.options.validate()?;
        ensure!(
            !self.task.is_nil() && self.generation > 0 && self.options.external(),
            "invalid external transfer plan"
        );
        ensure!(
            self.item.is_some() == self.key.is_some(),
            "transfer item key missing"
        );
        ensure!(
            self.cursor
                .as_ref()
                .is_none_or(|value| !value.is_empty() && value.len() <= 4096),
            "invalid transfer cursor"
        );
        self.state.validate()?;
        if let Some(native) = &self.native {
            ensure!(
                self.options.source_external != self.options.target_external && self.item.is_some(),
                "unexpected native source"
            );
            native.object.validate()?;
        }
        if let Some(incoming) = &self.incoming {
            incoming.validate(self)?;
        }
        ensure!(
            !matches!(self.state.phase, Phase::Import | Phase::Project)
                || self.options.source_external && !self.options.target_external,
            "unexpected native import phase"
        );
        if let Some(key) = &self.key {
            self.options.target(key)?;
        }
        if self.item.is_none() {
            ensure!(
                self.state == State::default() && self.parts.is_empty(),
                "invalid transfer listing plan"
            );
        }
        let mut previous = 0;
        for part in &self.parts {
            part.validate(&self.state)?;
            ensure!(part.number > previous, "unordered transfer parts");
            previous = part.number;
        }
        match self.state.phase {
            Phase::Transfer | Phase::Import => ensure!(
                self.parts.len() == usize::from(self.state.position > 0)
                    && self
                        .parts
                        .first()
                        .is_none_or(|part| part.number == i32::from(self.state.position)),
                "transfer boundary receipt missing"
            ),
            Phase::Complete => ensure!(
                self.parts.len() == usize::from(self.state.parts()?)
                    && self
                        .parts
                        .iter()
                        .enumerate()
                        .all(|(index, part)| part.number as usize == index + 1),
                "transfer completion receipts missing"
            ),
            Phase::Verify => ensure!(
                self.parts.len() == 1 && self.parts[0].number == i32::from(self.state.position) + 1,
                "transfer verification receipt missing"
            ),
            _ => ensure!(self.parts.is_empty(), "unexpected transfer receipts"),
        }
        ensure!(
            serde_json::to_vec(self)?.len() <= 3 * 1024 * 1024,
            "external transfer plan exceeds limit"
        );
        Ok(())
    }

    pub(crate) fn target(&self) -> Result<String> {
        self.options
            .target(self.key.as_deref().context("transfer key missing")?)
    }

    pub(crate) fn native_key(&self) -> Result<String> {
        if self.options.target_external {
            Ok(self.key.as_ref().context("transfer key missing")?.clone())
        } else {
            self.target()
        }
    }

    pub(crate) fn public(&self) -> Option<bool> {
        (self.options.permissions != Permissions::Ignore).then(|| {
            self.options.permissions.public(
                self.state.source_public.unwrap_or(false),
                self.state.target_public,
            )
        })
    }

    pub(crate) fn listing(&self, keys: &[String], next: Option<&str>) -> Result<()> {
        ensure!(
            self.item.is_none()
                && keys.len() <= 64
                && keys.iter().collect::<BTreeSet<_>>().len() == keys.len(),
            "invalid transfer listing"
        );
        ensure!(
            next.is_none_or(|value| !value.is_empty()
                && value.len() <= 4096
                && Some(value) != self.cursor.as_deref()
                && !value.chars().any(char::is_control)),
            "invalid transfer continuation"
        );
        for key in keys {
            ensure!(valid_key(key, false), "invalid transfer source key");
            self.options.target(key)?;
        }
        if let Scope::Objects { keys: selected } = &self.options.scope {
            let position = self
                .cursor
                .as_deref()
                .map(str::parse::<usize>)
                .transpose()?
                .unwrap_or(0);
            ensure!(
                position <= selected.len(),
                "invalid transfer listing position"
            );
            let end = (position + 64).min(selected.len());
            let expected = (end < selected.len()).then(|| end.to_string());
            ensure!(
                keys == &selected[position..end] && next == expected.as_deref(),
                "transfer selection differs"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Outcome {
    Listed {
        keys: Vec<String>,
        next: Option<String>,
    },
    Inspected {
        source: Option<Object>,
        target: Option<Object>,
        source_public: Option<bool>,
        target_public: Option<bool>,
    },
    Compared {
        equal: bool,
    },
    Started {
        upload: String,
    },
    Transferred(Part),
    Imported,
    Written(Written),
    Located {
        object: Option<Object>,
    },
    Verified {
        digest: Vec<u8>,
    },
    Permissions {
        public: bool,
    },
    Finalized {
        source: Option<Object>,
        source_public: Option<bool>,
        target: Option<Object>,
        public: Option<bool>,
    },
    Aborted,
    Conflict,
}

impl Outcome {
    pub(crate) fn validate(&self) -> Result<()> {
        let opaque = |value: &str| {
            !value.is_empty() && value.len() <= 4096 && !value.chars().any(char::is_control)
        };
        match self {
            Self::Listed { keys, next } => ensure!(
                keys.len() <= 64
                    && keys.iter().all(|key| valid_key(key, false))
                    && next.as_deref().is_none_or(opaque),
                "invalid external listing result"
            ),
            Self::Inspected { source, target, .. } | Self::Finalized { source, target, .. } => {
                for object in source.iter().chain(target.iter()) {
                    object.validate()?;
                }
            }
            Self::Started { upload } => ensure!(opaque(upload), "invalid external upload id"),
            Self::Transferred(part) => part.validate_record()?,
            Self::Written(written) => ensure!(
                objects::strong_etag(&written.etag)
                    && written
                        .version
                        .as_deref()
                        .is_none_or(|value| opaque(value) && value != "null"),
                "invalid external write receipt"
            ),
            Self::Located { object } => {
                if let Some(object) = object {
                    object.validate()?;
                }
            }
            Self::Verified { digest } => ensure!(digest.len() == 32, "invalid external digest"),
            _ => {}
        }
        Ok(())
    }
}

pub(crate) fn apply(plan: &Plan, outcome: &Outcome) -> Result<Change> {
    plan.validate()?;
    outcome.validate()?;
    ensure!(plan.item.is_some(), "transfer item missing");
    let mut state = plan.state.clone();
    let mut part = None;
    match (state.phase, outcome) {
        (_, Outcome::Conflict) => state.finish("conflict", Some("object_changed")),
        (
            Phase::Inspect,
            Outcome::Inspected {
                source,
                target,
                source_public,
                target_public,
            },
        ) => {
            for object in source.iter().chain(target.iter()) {
                object.validate()?;
            }
            state.source = source.clone();
            state.target = target.clone();
            state.source_public = *source_public;
            state.target_public = *target_public;
            if let Some(source) = source {
                ensure!(
                    plan.options.permissions != Permissions::Inherit || source_public.is_some(),
                    "source permissions missing"
                );
                state.part_size = objects::part_size(source.size)?;
                if let Some(target) = target {
                    if source.size != target.size {
                        state.conflict(&plan.options)?;
                    } else if source.size == 0 {
                        state.matched(&plan.options)?;
                    } else {
                        state.phase = Phase::Compare;
                    }
                } else {
                    state.write(&plan.options)?;
                }
            } else {
                state.finish("conflict", Some("source_missing"));
            }
        }
        (Phase::Compare, Outcome::Compared { equal }) => {
            if *equal {
                state.position += 1;
                if state.position == state.parts()? {
                    state.matched(&plan.options)?;
                }
            } else {
                state.conflict(&plan.options)?;
            }
        }
        (Phase::Start, Outcome::Started { upload }) => {
            ensure!(
                !upload.is_empty() && upload.len() <= 4096 && !upload.chars().any(char::is_control),
                "invalid transfer upload id"
            );
            state.upload = Some(upload.clone());
            state.phase = Phase::Transfer;
        }
        (Phase::Transfer | Phase::Import, Outcome::Transferred(receipt)) => {
            receipt.validate(&state)?;
            ensure!(
                receipt.number == i32::from(state.position) + 1,
                "unexpected transfer part"
            );
            state.position += 1;
            if state.phase == Phase::Import {
                state.phase = Phase::Project;
            } else if state.position == state.parts()? {
                state.phase = Phase::Complete;
            }
            part = Some(receipt.clone());
        }
        (Phase::Complete, Outcome::Written(written)) => {
            ensure!(
                objects::strong_etag(&written.etag)
                    && written
                        .version
                        .as_ref()
                        .is_none_or(|value| !value.is_empty()
                            && value.len() <= 4096
                            && value != "null"
                            && !value.chars().any(char::is_control)),
                "invalid transfer write receipt"
            );
            state.written = Some(written.clone());
            state.upload = None;
            state.phase = Phase::Locate;
        }
        (Phase::Complete | Phase::Locate, Outcome::Located { object }) => {
            if let Some(object) = object {
                object.validate()?;
                let original = state.source.as_ref().context("transfer source missing")?;
                if object.size != original.size
                    || object.metadata != original.metadata
                    || state.written.as_ref().is_some_and(|written| {
                        written.etag != object.etag || written.version != object.version
                    })
                {
                    state.finish("conflict", Some("target_changed"));
                } else {
                    state.output = Some(object.clone());
                    state.position = 0;
                    state.phase = if state.parts()? > 0 {
                        Phase::Verify
                    } else if plan.public().is_some() {
                        Phase::Permissions
                    } else {
                        Phase::Finalize
                    };
                }
            } else {
                state.finish("conflict", Some("target_missing"));
            }
        }
        (Phase::Verify, Outcome::Verified { digest }) => {
            if plan.parts[0].digest != *digest {
                state.finish("conflict", Some("target_changed"));
            } else {
                state.position += 1;
                if state.position == state.parts()? {
                    state.phase = if plan.public().is_some() {
                        Phase::Permissions
                    } else {
                        Phase::Finalize
                    };
                }
            }
        }
        (Phase::Permissions, Outcome::Permissions { public }) => {
            ensure!(plan.public() == Some(*public), "target permissions differ");
            state.phase = Phase::Finalize;
        }
        (
            Phase::Finalize,
            Outcome::Finalized {
                source,
                source_public,
                target,
                public,
            },
        ) => {
            if source != &state.source
                || target != &state.output
                || *public != plan.public()
                || plan.options.permissions == Permissions::Inherit
                    && *source_public != state.source_public
            {
                state.finish("conflict", Some("object_changed"));
            } else if plan.options.operation == Operation::Move {
                state.finish("conflict", Some("source_delete_unfenced"));
            } else {
                state.finish(
                    if state.changed
                        || state.previous.as_ref().is_some_and(|previous| {
                            previous.matches(state.output.as_ref(), *public)
                        })
                    {
                        "completed"
                    } else {
                        "skipped"
                    },
                    (state.cleanup_pending || state.starts > 1).then_some("upload_cleanup_pending"),
                );
            }
        }
        (Phase::Cleanup, Outcome::Aborted) => {
            ensure!(state.finish.is_some(), "transfer cleanup result missing");
            state.upload = None;
            state.phase = Phase::Done;
        }
        _ => anyhow::bail!("unexpected external transfer result"),
    }
    state.validate()?;
    let finish = (state.phase == Phase::Done)
        .then(|| state.finish.clone())
        .flatten();
    Ok(Change {
        state,
        part,
        finish,
    })
}

#[derive(Clone, sqlx::FromRow)]
pub(crate) struct Connection {
    pub external_id: Uuid,
    pub revision: i64,
    pub encrypted: Vec<u8>,
}

impl Connection {
    fn binding(&self, task: Uuid) -> String {
        format!(
            "transfer-{task}-external-{}-{}",
            self.external_id, self.revision
        )
    }

    pub(crate) fn validate(&self, task: Uuid) -> Result<()> {
        ensure!(
            !task.is_nil()
                && !self.external_id.is_nil()
                && self.revision > 0
                && !self.encrypted.is_empty()
                && self.encrypted.len() <= 32 * 1024,
            "invalid transfer connection"
        );
        Ok(())
    }

    pub(crate) fn seal(task: Uuid, snapshot: &Snapshot, key: &codec::Key) -> Result<Self> {
        snapshot.validate()?;
        let mut value = Self {
            external_id: snapshot.external_id,
            revision: snapshot.revision,
            encrypted: Vec::new(),
        };
        value.encrypted = codec::protect(
            &serde_json::to_vec(snapshot)?,
            key,
            value.binding(task).as_bytes(),
        )?;
        value.validate(task)?;
        Ok(value)
    }

    pub(crate) fn open(&self, task: Uuid, key: &codec::Key) -> Result<Snapshot> {
        self.validate(task)?;
        let value: Snapshot = serde_json::from_slice(&codec::unprotect_bytes(
            &self.encrypted,
            key,
            self.binding(task).as_bytes(),
        )?)?;
        value.validate()?;
        ensure!(
            value.external_id == self.external_id && value.revision == self.revision,
            "transfer connection differs"
        );
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confirmed_output_replacement_checks_identity_and_permissions() -> Result<()> {
        let mut plan = pending(4)?;
        let previous = plan.state.source.clone().unwrap();
        plan.parts.clear();
        plan.state = State {
            previous: Some(super::super::changes::Target {
                object: previous.clone(),
                public: Some(false),
            }),
            ..State::default()
        };
        let source = Object {
            size: 5,
            etag: "\"next\"".into(),
            ..previous.clone()
        };
        let inspect = |target: Object, public| Outcome::Inspected {
            source: Some(source.clone()),
            target: Some(target),
            source_public: Some(false),
            target_public: Some(public),
        };
        assert_eq!(
            apply(&plan, &inspect(previous.clone(), false))?.state.phase,
            Phase::Start
        );
        assert_eq!(
            apply(&plan, &inspect(previous.clone(), true))?
                .finish
                .unwrap()
                .status,
            "conflict"
        );
        let other = Object {
            etag: "\"independent\"".into(),
            ..previous.clone()
        };
        assert_eq!(
            apply(&plan, &inspect(other, false))?.finish.unwrap().status,
            "conflict"
        );
        plan.state.previous = None;
        assert_eq!(
            apply(&plan, &inspect(previous, false))?
                .finish
                .unwrap()
                .status,
            "conflict"
        );
        Ok(())
    }

    fn pending(size: u64) -> Result<Plan> {
        let source = Object {
            size,
            etag: "\"source\"".into(),
            version: None,
            modified: None,
            metadata: std::collections::BTreeMap::new(),
        };
        let mut state = State {
            phase: Phase::Complete,
            source: Some(source),
            upload: Some("upload".into()),
            part_size: objects::part_size(size)?,
            changed: true,
            starts: 1,
            ..State::default()
        };
        state.position = state.parts()?;
        let parts = (1..=i32::from(state.position))
            .map(|number| {
                Ok(Part {
                    number,
                    size: state.range(number)?.1 as i64,
                    etag: format!("\"{:032x}\"", number),
                    digest: vec![255; 32],
                })
            })
            .collect::<Result<_>>()?;
        Ok(Plan {
            task: Uuid::now_v7(),
            generation: 1,
            item: Some(Id::Integer(1)),
            key: Some("source".into()),
            cursor: None,
            options: Options {
                source_bucket: Uuid::now_v7(),
                target_bucket: Uuid::now_v7(),
                source_external: true,
                target_external: true,
                operation: Operation::Copy,
                scope: Scope::Objects {
                    keys: vec!["source".into()],
                },
                target_prefix: String::new(),
                target_key: Some("target".into()),
                conflict: Conflict::Report,
                permissions: Permissions::Ignore,
                propagate_deletes: false,
            },
            state,
            parts,
            native: None,
            incoming: None,
        })
    }

    #[test]
    fn native_parts_keep_receipts_without_remote_upload_ids() -> Result<()> {
        let mut plan = pending(objects::WINDOW as u64 + 7)?;
        let receipts = plan.parts.clone();
        plan.options.target_external = false;
        plan.state.phase = Phase::Import;
        plan.state.upload = None;
        plan.state.position = 0;
        plan.state.starts = 0;
        plan.parts.clear();
        let change = apply(&plan, &Outcome::Transferred(receipts[0].clone()))?;
        assert_eq!(change.state.phase, Phase::Project);
        assert_eq!(change.state.position, 1);
        assert!(change.state.upload.is_none());
        assert!(change.finish.is_none());
        plan.state = change.state;
        assert!(apply(&plan, &Outcome::Transferred(receipts[1].clone())).is_err());
        plan.state.phase = Phase::Complete;
        plan.state.position = 2;
        plan.parts = receipts;
        let native = super::super::import::snapshot(&plan)?;
        assert!(native.etag.ends_with("-2"));
        assert_eq!(native.size, objects::WINDOW as i64 + 7);
        assert_eq!(native.checksums["type"], "COMPOSITE");
        plan.parts[1].size += 1;
        assert!(super::super::import::snapshot(&plan).is_err());
        Ok(())
    }

    #[test]
    fn native_import_write_is_bound_to_the_current_part() -> Result<()> {
        let mut plan = pending(9)?;
        plan.options.target_external = false;
        plan.state.phase = Phase::Import;
        plan.state.upload = None;
        plan.state.position = 0;
        plan.parts.clear();
        let write = super::super::import::Write {
            stream: Uuid::now_v7().into(),
            storage: Uuid::new_v4(),
            offset: 0,
            size: 9,
            operation: Some(Uuid::now_v7()),
        };
        plan.incoming = Some(write.clone());
        plan.validate()?;
        let mut invalid = write.clone();
        invalid.offset = 1;
        assert!(invalid.validate(&plan).is_err());
        invalid = write.clone();
        invalid.size = 10;
        assert!(invalid.validate(&plan).is_err());
        invalid = write.clone();
        invalid.operation = Some(Uuid::new_v4());
        assert!(invalid.validate(&plan).is_err());
        plan.state.phase = Phase::Project;
        assert!(write.validate(&plan).is_err());
        plan.state.phase = Phase::Import;
        plan.options.source_external = false;
        assert!(write.validate(&plan).is_err());
        Ok(())
    }

    #[test]
    fn matching_native_body_reuses_target_for_permission_changes() -> Result<()> {
        let mut plan = pending(9)?;
        plan.options.target_external = false;
        plan.options.permissions = Permissions::Inherit;
        plan.state.phase = Phase::Compare;
        plan.state.upload = None;
        plan.state.position = 0;
        plan.state.target = plan.state.source.clone();
        plan.state.source_public = Some(true);
        plan.state.target_public = Some(false);
        plan.parts.clear();
        let result = apply(&plan, &Outcome::Compared { equal: true })?;
        assert_eq!(result.state.phase, Phase::Project);
        assert!(result.state.changed);
        assert!(result.part.is_none());
        Ok(())
    }

    #[test]
    fn native_empty_import_uses_content_checksums() -> Result<()> {
        let mut plan = pending(0)?;
        plan.options.target_external = false;
        plan.state.upload = None;
        let result = super::super::import::snapshot(&plan)?;
        assert_eq!(result.etag, "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(
            result.checksums["sha256"],
            "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
        );
        Ok(())
    }

    #[test]
    fn unknown_completion_requires_body_proof_and_cleanup() -> Result<()> {
        let mut plan = pending(17)?;
        let receipt = plan.parts[0].clone();
        let output = plan.state.source.clone().unwrap();
        plan.state = apply(
            &plan,
            &Outcome::Located {
                object: Some(output.clone()),
            },
        )?
        .state;
        assert_eq!(plan.state.phase, Phase::Verify);
        assert!(
            apply(
                &plan,
                &Outcome::Finalized {
                    source: plan.state.source.clone(),
                    source_public: None,
                    target: Some(output.clone()),
                    public: None
                }
            )
            .is_err()
        );
        let rejected = apply(
            &plan,
            &Outcome::Verified {
                digest: vec![0; 32],
            },
        )?;
        assert_eq!(rejected.state.phase, Phase::Cleanup);
        assert_eq!(rejected.state.finish.unwrap().status, "conflict");
        plan.parts = vec![receipt];
        plan.state = apply(
            &plan,
            &Outcome::Verified {
                digest: vec![255; 32],
            },
        )?
        .state;
        plan.parts.clear();
        assert_eq!(plan.state.phase, Phase::Finalize);
        let source = plan.state.source.clone();
        let finalized = Outcome::Finalized {
            source,
            source_public: None,
            target: Some(output),
            public: None,
        };
        plan.state = apply(&plan, &finalized)?.state;
        assert_eq!(plan.state.phase, Phase::Cleanup);
        assert!(
            apply(&plan, &Outcome::Aborted)?
                .finish
                .is_some_and(|finish| finish.status == "completed")
        );
        Ok(())
    }

    #[test]
    fn maximum_object_receipts_fit_the_plan_budget() -> Result<()> {
        let mut plan = pending(5 * 1024 * 1024 * 1024 * 1024)?;
        assert!(plan.parts.len() > 9900);
        plan.validate()?;
        plan.parts[1].number = 1;
        assert!(plan.validate().is_err());
        Ok(())
    }

    #[test]
    fn repeated_confirmation_keeps_a_completed_output() -> Result<()> {
        let mut plan = pending(0)?;
        let object = plan.state.source.clone().unwrap();
        plan.parts.clear();
        for (owned, cleanup_pending) in [(true, false), (false, false), (true, true)] {
            plan.state = State {
                cleanup_pending,
                previous: owned.then(|| super::super::changes::Target {
                    object: object.clone(),
                    public: None,
                }),
                ..State::default()
            };
            plan.state = apply(
                &plan,
                &Outcome::Inspected {
                    source: Some(object.clone()),
                    target: Some(object.clone()),
                    source_public: None,
                    target_public: None,
                },
            )?
            .state;
            let result = apply(
                &plan,
                &Outcome::Finalized {
                    source: Some(object.clone()),
                    target: Some(object.clone()),
                    source_public: None,
                    public: None,
                },
            )?;
            let finish = result.finish.unwrap();
            assert_eq!(
                finish.error.as_deref(),
                cleanup_pending.then_some("upload_cleanup_pending")
            );
            assert_eq!(finish.status, if owned { "completed" } else { "skipped" });
            plan.state.cancel();
            let finish = plan.state.finish.as_ref().unwrap();
            assert_eq!(finish.status, "cancelled");
            assert_eq!(
                finish.error.as_deref(),
                cleanup_pending.then_some("upload_cleanup_pending")
            );
        }
        Ok(())
    }

    #[test]
    fn confirmed_body_cannot_change_an_unversioned_target_acl() -> Result<()> {
        let mut plan = pending(17)?;
        plan.state.phase = Phase::Compare;
        plan.state.position = 0;
        plan.state.target = plan.state.source.clone();
        plan.state.upload = None;
        plan.state.changed = false;
        plan.state.target_public = Some(false);
        plan.options.permissions = Permissions::PublicRead;
        plan.parts.clear();
        let result = apply(&plan, &Outcome::Compared { equal: true })?;
        assert_eq!(
            result.finish.unwrap().error.as_deref(),
            Some("target_acl_unfenced")
        );
        plan.state.target.as_mut().unwrap().version = Some("version".into());
        let result = apply(&plan, &Outcome::Compared { equal: true })?;
        assert_eq!(result.state.phase, Phase::Permissions);
        assert!(result.finish.is_none());
        Ok(())
    }

    #[test]
    fn credentials_are_bound_to_task_resource_and_revision() -> Result<()> {
        let key = codec::Key::new("aes-256-gcm", [7; 32])?;
        let task = Uuid::now_v7();
        let snapshot = Snapshot {
            external_id: Uuid::now_v7(),
            revision: 1,
            connection: crate::storage::external::Connection {
                endpoint_url: "https://example.invalid".into(),
                region: "test".into(),
                bucket: "bucket".into(),
                prefix: "imports".into(),
                virtual_hosted: false,
                allow_http: false,
            },
            access_key: "fixture-access".into(),
            secret_key: "fixture-secret".into(),
        };
        let mut saved = Connection::seal(task, &snapshot, &key)?;
        assert_eq!(saved.open(task, &key)?.secret_key, snapshot.secret_key);
        assert!(
            !saved
                .encrypted
                .windows(snapshot.secret_key.len())
                .any(|bytes| bytes == snapshot.secret_key.as_bytes())
        );
        assert!(saved.open(Uuid::now_v7(), &key).is_err());
        assert!(
            saved
                .open(task, &codec::Key::new("aes-256-gcm", [8; 32])?)
                .is_err()
        );
        saved.revision += 1;
        assert!(saved.open(task, &key).is_err());
        saved.revision = snapshot.revision;
        saved.external_id = Uuid::now_v7();
        assert!(saved.open(task, &key).is_err());
        saved.external_id = snapshot.external_id;
        saved.encrypted[0] ^= 1;
        assert!(saved.open(task, &key).is_err());
        Ok(())
    }

    #[test]
    fn external_aliases_cannot_overwrite_selected_sources() -> Result<()> {
        let mut plan = pending(17)?;
        let source = crate::storage::external::Connection {
            endpoint_url: "https://example.invalid".into(),
            region: "test".into(),
            bucket: "bucket".into(),
            prefix: "shared".into(),
            virtual_hosted: false,
            allow_http: false,
        };
        let mut target = source.clone();
        target.virtual_hosted = true;
        plan.options.external_scopes(&source, &target)?;
        plan.options.target_key = Some("source".into());
        assert!(plan.options.external_scopes(&source, &target).is_err());
        plan.options.target_key = None;
        plan.options.scope = Scope::All;
        assert!(plan.options.external_scopes(&source, &target).is_err());
        target.prefix = "separate".into();
        plan.options.external_scopes(&source, &target)?;
        Ok(())
    }

    #[test]
    fn native_sources_preserve_headers_and_public_state() -> Result<()> {
        let id = Id::from(Uuid::now_v7());
        let source = super::super::Snapshot {
            metadata: serde_json::json!({"content_type":"image/png","cache_control":"public, max-age=60","user":{"author":"test"}}),
            checksums: serde_json::json!({}),
            public: true,
            size: 3,
            etag: "abc".into(),
            payload: "chunks".into(),
        };
        let native = Native::new(id, &source)?;
        assert_eq!(native.object.version, Some(id.to_string()));
        assert_eq!(native.object.metadata["x-amz-meta-author"], "test");
        assert_eq!(native.object.metadata["content-type"], "image/png");
        assert!(native.public);
        let mut invalid = source.clone();
        invalid.size = -1;
        assert!(Native::new(id, &invalid).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn missing_native_source_returns_conflict_before_remote_io() -> Result<()> {
        let mut plan = pending(17)?;
        plan.options.source_external = false;
        plan.state.phase = Phase::Transfer;
        plan.state.position = 0;
        plan.parts.clear();
        let memory = std::sync::Arc::new(
            std::sync::Arc::new(tokio::sync::Semaphore::new(1))
                .acquire_owned()
                .await?,
        );
        let outcome = crate::storage::external::transfer::execute(
            &plan,
            &[],
            &memory,
            None,
            &crate::storage::external::Reads::Node(std::sync::Arc::new(
                tokio::sync::Semaphore::new(1),
            )),
        )
        .await?;
        assert!(matches!(outcome, Outcome::Conflict));
        let change = apply(&plan, &outcome)?;
        assert_eq!(change.state.phase, Phase::Cleanup);
        assert_eq!(change.state.upload.as_deref(), Some("upload"));
        assert!(change.finish.is_none());
        Ok(())
    }

    #[test]
    fn changed_native_source_keeps_new_upload_for_cleanup() -> Result<()> {
        let mut plan = pending(17)?;
        plan.state.phase = Phase::Start;
        plan.state.position = 0;
        plan.state.upload = None;
        plan.parts.clear();
        let mut changed = apply(
            &plan,
            &Outcome::Started {
                upload: "new-upload".into(),
            },
        )?;
        changed.source_changed();
        assert_eq!(changed.state.phase, Phase::Cleanup);
        assert_eq!(changed.state.upload.as_deref(), Some("new-upload"));
        assert!(changed.finish.is_none());
        Ok(())
    }
}
