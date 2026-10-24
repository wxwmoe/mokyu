use super::{CompressionObservation, session::Run};
use anyhow::{Result, ensure};
use std::collections::HashSet;

pub(crate) const LIMIT: usize = 64;

pub(crate) struct Batch {
    pub(crate) run: Run,
    pub(crate) compression: Vec<CompressionObservation>,
    pub(crate) credentials: Vec<String>,
}

impl Batch {
    pub(crate) fn validate(&self) -> Result<()> {
        let invalid = || s3s::s3_error!(InvalidArgument, "invalid observation batch");
        ensure!(
            (1..=LIMIT).contains(&(self.compression.len() + self.credentials.len())),
            invalid()
        );
        let mut keys = HashSet::new();
        for observation in &self.compression {
            observation.validate()?;
            ensure!(
                matches!(observation.stream, super::Id::Uuid(id) if !id.is_nil())
                    && keys.insert((
                        observation.stream,
                        observation.version,
                        observation.encoding
                    )),
                invalid()
            );
        }
        let mut credentials = HashSet::new();
        for access in &self.credentials {
            ensure!(
                !access.is_empty()
                    && access.len() <= 128
                    && access.bytes().all(|byte| byte.is_ascii_graphic())
                    && credentials.insert(access),
                invalid()
            );
        }
        Ok(())
    }
}
