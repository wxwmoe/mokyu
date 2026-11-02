use crate::content::raw_pages::{Builder, GROUP, Group, HASH};
use anyhow::Result;
use std::path::Path;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

/// Keep unpublished hashes off the database until the full-source digest has been verified.
pub(super) struct Spool<'a> {
    builder: Box<Builder>,
    file: Option<File>,
    groups: i64,
    root: &'a Path,
}

impl<'a> Spool<'a> {
    pub(super) fn new(root: &'a Path) -> Self {
        Self {
            builder: Default::default(),
            file: None,
            groups: 0,
            root,
        }
    }

    pub(super) async fn feed(&mut self, mut bytes: &[u8]) -> Result<()> {
        while let Some(group) = self.builder.feed(&mut bytes) {
            if self.file.is_none() {
                let root = self.root.to_owned();
                self.file = Some(File::from_std(
                    tokio::task::spawn_blocking(move || tempfile::tempfile_in(root)).await??,
                ));
            }
            self.file.as_mut().unwrap().write_all(&group.hashes).await?;
            self.groups += 1;
        }
        Ok(())
    }

    pub(super) async fn finish(mut self) -> Result<Verified> {
        if let Some(file) = &mut self.file {
            file.flush().await?;
            file.rewind().await?;
        }
        Ok(Verified {
            file: self.file,
            tail: self.builder.finish(),
            groups: self.groups,
            next: 0,
        })
    }
}

pub(crate) struct Verified {
    file: Option<File>,
    tail: Option<Group>,
    groups: i64,
    next: i64,
}

impl Verified {
    pub(crate) async fn next(&mut self) -> Result<Option<Group>> {
        if self.next == self.groups {
            return Ok(self.tail.take());
        }
        let mut hashes = vec![0; GROUP * HASH];
        self.file.as_mut().unwrap().read_exact(&mut hashes).await?;
        let group = Group {
            index: self.next,
            hashes,
        };
        self.next += 1;
        Ok(Some(group))
    }
}
