use anyhow::{Result, ensure};

pub(crate) const GROUP: usize = 256;
pub(crate) const HASH: usize = 32;

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize, sqlx::Type,
)]
#[serde(try_from = "i64", into = "i64")]
#[repr(i64)]
pub(crate) enum Size {
    #[default]
    KiB256 = 262144,
    MiB1 = 1048576,
}

impl Size {
    pub fn legacy() -> Self {
        Self::MiB1
    }
    pub fn is_legacy(&self) -> bool {
        *self == Self::MiB1
    }
    pub fn count(self, size: i64) -> Result<i64> {
        Ok(u64::try_from(size)?.div_ceil(self as u64).try_into()?)
    }
}

impl TryFrom<i64> for Size {
    type Error = anyhow::Error;
    fn try_from(value: i64) -> Result<Self> {
        match value {
            262144 => Ok(Self::KiB256),
            1048576 => Ok(Self::MiB1),
            _ => anyhow::bail!("invalid raw page size"),
        }
    }
}

impl From<Size> for i64 {
    fn from(value: Size) -> Self {
        value as i64
    }
}

/// A stored group covers consecutive pages of a single physical source.
#[derive(Debug)]
pub(crate) struct Group {
    pub index: i64,
    pub hashes: Vec<u8>,
}

impl Group {
    pub fn validate_size(&self, size: i64, page_size: Size) -> Result<()> {
        self.validate()?;
        let pages = page_size.count(size)?;
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
    page_size: Size,
    hasher: blake3::Hasher,
    filled: usize,
    hashes: Vec<u8>,
    next: i64,
}

impl Builder {
    pub fn new(page_size: Size) -> Self {
        Self {
            page_size,
            ..Self::default()
        }
    }
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
            let n = input.len().min(self.page_size as usize - self.filled);
            self.hasher.update(&input[..n]);
            self.filled += n;
            *input = &input[n..];
            if self.filled == self.page_size as usize {
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
        for size in [Size::KiB256, Size::MiB1] {
            let page = size as usize;
            let input = (0..page * 2 + 17)
                .map(|n| (n % 137) as u8)
                .collect::<Vec<_>>();
            let mut pages = Builder::new(size);
            for mut frame in input.chunks(157_019) {
                assert!(pages.feed(&mut frame).is_none());
            }
            let group = pages.finish().unwrap();
            let expected: Vec<_> = input
                .chunks(page)
                .flat_map(|p| *blake3::hash(p).as_bytes())
                .collect();
            assert_eq!(group.index, 0);
            assert_eq!(group.hashes, expected);
            assert!(Builder::new(size).finish().is_none());
            assert_eq!(size.count(input.len() as i64).unwrap(), 3);
        }
    }
}
