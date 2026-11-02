use anyhow::{Result, ensure};

pub(crate) const PAGE: usize = 1024 * 1024;
pub(crate) const GROUP: usize = 256;
pub(crate) const HASH: usize = 32;

pub(crate) fn count(size: i64) -> Result<i64> {
    Ok(u64::try_from(size)?.div_ceil(PAGE as u64).try_into()?)
}

/// A stored group covers consecutive pages of a single physical source.
#[derive(Debug)]
pub(crate) struct Group {
    pub index: i64,
    pub hashes: Vec<u8>,
}

impl Group {
    pub fn validate_size(&self, size: i64) -> Result<()> {
        self.validate()?;
        let pages = count(size)?;
        let first = self
            .index
            .checked_mul(GROUP as i64)
            .ok_or_else(|| anyhow::anyhow!("raw page group overflow"))?;
        ensure!(
            first < pages && self.hashes.len() == (pages - first).min(GROUP as i64) as usize * HASH,
            "raw page group outside source"
        );
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.index >= 0
                && !self.hashes.is_empty()
                && self.hashes.len() <= GROUP * HASH
                && self.hashes.len().is_multiple_of(HASH),
            "invalid raw page group"
        );
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct Builder {
    hasher: blake3::Hasher,
    filled: usize,
    hashes: Vec<u8>,
    next: i64,
}

impl Builder {
    fn finish_page(&mut self) {
        self.hashes
            .extend_from_slice(self.hasher.finalize().as_bytes());
        self.hasher.reset();
        self.filled = 0;
    }

    fn group(&mut self) -> Group {
        let group = Group {
            index: self.next,
            hashes: std::mem::take(&mut self.hashes),
        };
        self.next += 1;
        group
    }

    /// Stop at the next full group, so callers can persist it before consuming more input.
    pub fn feed(&mut self, input: &mut &[u8]) -> Option<Group> {
        while !input.is_empty() {
            let n = input.len().min(PAGE - self.filled);
            self.hasher.update(&input[..n]);
            self.filled += n;
            *input = &input[n..];
            if self.filled == PAGE {
                self.finish_page();
                if self.hashes.len() == GROUP * HASH {
                    return Some(self.group());
                }
            }
        }
        None
    }

    pub fn finish(mut self) -> Option<Group> {
        if self.filled != 0 {
            self.finish_page();
        }
        (!self.hashes.is_empty()).then(|| self.group())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundaries_do_not_depend_on_transport_frames() {
        let input = (0..PAGE * 2 + 17)
            .map(|n| (n % 137) as u8)
            .collect::<Vec<_>>();
        let mut pages = Builder::default();
        for mut frame in input.chunks(157_019) {
            assert!(pages.feed(&mut frame).is_none());
        }
        let group = pages.finish().unwrap();
        let expected: Vec<_> = input
            .chunks(PAGE)
            .flat_map(|p| *blake3::hash(p).as_bytes())
            .collect();
        assert_eq!(group.index, 0);
        assert_eq!(group.hashes, expected);
        assert!(Builder::default().finish().is_none());
    }
}
