use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[schema(as = MetadataJournalWriteMode)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WriteMode {
    #[default]
    Put,
    Multipart,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
#[schema(as = MetadataJournalPolicy)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Policy {
    #[schema(required = false)]
    pub mode: WriteMode,
    #[schema(required = false)]
    pub put_target_bytes: u32,
    #[schema(required = false)]
    pub put_max_wait_seconds: Option<u32>,
    #[schema(required = false)]
    pub part_target_bytes: u32,
    #[schema(required = false)]
    pub part_max_wait_seconds: Option<u32>,
    #[schema(required = false)]
    pub multipart_max_open_seconds: Option<u32>,
    pub segment_bytes: u32,
    pub snapshot_page_bytes: u32,
    pub snapshot_trigger_bytes: u64,
}

pub(crate) const MIN_PART: u32 = 5 * 1024 * 1024;
pub(crate) const DEFAULT_LIMIT: u32 = 32 * 1024 * 1024;
pub(crate) const MAX_LIMIT: u32 = 256 * 1024 * 1024;

impl Default for Policy {
    fn default() -> Self {
        Self {
            mode: WriteMode::Put,
            put_target_bytes: 128 * 1024,
            put_max_wait_seconds: Some(600),
            part_target_bytes: MIN_PART,
            part_max_wait_seconds: Some(600),
            multipart_max_open_seconds: Some(3600),
            segment_bytes: DEFAULT_LIMIT,
            snapshot_page_bytes: DEFAULT_LIMIT,
            snapshot_trigger_bytes: u64::from(DEFAULT_LIMIT),
        }
    }
}

impl Policy {
    pub fn validate(&self, local: bool) -> Result<()> {
        ensure!(
            (4096..=MAX_LIMIT).contains(&self.segment_bytes),
            "invalid journal segment limit"
        );
        ensure!(
            (4096..=MAX_LIMIT).contains(&self.snapshot_page_bytes),
            "invalid snapshot page limit"
        );
        ensure!(
            self.snapshot_trigger_bytes > 0 && self.snapshot_trigger_bytes <= 9_007_199_254_740_991,
            "invalid snapshot trigger"
        );
        if local {
            return Ok(());
        }
        ensure!(
            (1..=self.segment_bytes).contains(&self.put_target_bytes),
            "invalid journal PUT target"
        );
        ensure!(
            (MIN_PART..=MAX_LIMIT).contains(&self.part_target_bytes),
            "invalid journal part target"
        );
        if self.mode == WriteMode::Multipart {
            ensure!(
                self.part_target_bytes <= self.segment_bytes,
                "journal part exceeds segment limit"
            );
        }
        for seconds in [
            self.put_max_wait_seconds,
            self.part_max_wait_seconds,
            self.multipart_max_open_seconds,
        ]
        .into_iter()
        .flatten()
        {
            ensure!(seconds > 0, "use null to disable a journal timeout");
        }
        Ok(())
    }

    pub fn put_due(&self, bytes: usize, age_seconds: i64, flush: bool) -> bool {
        bytes > 0
            && (flush
                || bytes >= self.put_target_bytes as usize
                || self
                    .put_max_wait_seconds
                    .is_some_and(|wait| age_seconds >= i64::from(wait)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_size_and_deadline_are_independent() {
        let mut policy = Policy::default();
        assert!(policy.validate(false).is_ok());
        assert!(!policy.put_due(131_071, 599, false));
        assert!(policy.put_due(131_072, 599, false));
        assert!(policy.put_due(1, 600, false));
        assert!(!policy.put_due(0, 600, true));
        policy.put_max_wait_seconds = None;
        assert!(!policy.put_due(128, i64::MAX, false));
        policy.put_target_bytes = 4096;
        assert!(policy.put_due(11 * 1024, -1, false));
        policy.segment_bytes = 4096;
        assert!(policy.validate(false).is_ok());
        policy.mode = WriteMode::Multipart;
        assert!(policy.validate(false).is_err());
    }
}
