use super::{node_read, session::Run};
use anyhow::{Result, ensure};
use uuid::Uuid;

pub(crate) struct Prepare {
    pub run: Run,
    pub operation: Uuid,
    pub offset: i64,
    pub size: i32,
    pub hash: [u8; 32],
    pub cached: Vec<node_read::Version>,
}

impl Prepare {
    pub(crate) fn end(&self) -> Result<i64> {
        ensure!(
            self.offset >= 0 && self.size > 0 && (self.size as usize) < crate::content::codec::MAX,
            "invalid multipart tail"
        );
        let end = self
            .offset
            .checked_add(i64::from(self.size))
            .ok_or_else(|| anyhow::anyhow!("tail offset overflow"))?;
        ensure!(
            end <= super::node_write::MAX_PART_BYTES,
            "multipart part exceeds size limit"
        );
        Ok(end)
    }
    pub(crate) fn hash(&self) -> Result<Vec<u8>> {
        self.end()?;
        super::node_chunk::hash(&serde_json::json!([self.offset, self.size, self.hash]))
    }
}

pub(crate) struct Prepared {
    pub fragment: Uuid,
    pub object_key: String,
    pub end: i64,
    pub committed: bool,
    pub materials: Vec<node_read::Material>,
    pub storage: Option<node_read::Version>,
    pub grants: Vec<node_read::Grant>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tails_bind_offsets_and_digests_without_accepting_whole_parts() -> Result<()> {
        let mut input = Prepare {
            run: Run {
                boot: Uuid::new_v4(),
                fence: Uuid::new_v4(),
            },
            operation: Uuid::now_v7(),
            offset: 0,
            size: 3,
            hash: [1; 32],
            cached: vec![],
        };
        let first = input.hash()?;
        input.hash = [2; 32];
        assert_ne!(first, input.hash()?);
        input.size = crate::content::codec::MAX as i32;
        assert!(input.end().is_err());
        input.size = 0;
        assert!(input.end().is_err());
        input.size = 1;
        input.offset = i64::MAX;
        assert!(input.end().is_err());
        Ok(())
    }
}
