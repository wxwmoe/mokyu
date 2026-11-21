use crate::content::raw_pages::{Builder, GROUP, Group, HASH, Size};
use anyhow::Result;
use std::{path::Path, sync::Arc};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

/// Keep unpublished hashes off the database until the full-source digest has been verified.
pub(super) struct Spool<'a> {
    builder: Box<Builder>,
    scratch: Scratch,
    groups: i64,
    root: &'a Path,
    disk: Option<Arc<super::Disk>>,
}

#[derive(Default)]
struct Scratch {
    file: Option<File>,
    reservations: Vec<super::local::Space>,
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let Some(mut file) = self.file.take() else {
            return;
        };
        let reservations = std::mem::take(&mut self.reservations);
        tokio::spawn(async move {
            let _ = file.flush().await;
            drop(file);
            drop(reservations);
        });
    }
}

impl<'a> Spool<'a> {
    pub(super) fn new(root: &'a Path, disk: Option<Arc<super::Disk>>, page_size: Size) -> Self {
        Self {
            builder: Box::new(Builder::new(page_size)),
            scratch: Scratch::default(),
            groups: 0,
            root,
            disk,
        }
    }

    pub(super) async fn feed(&mut self, mut bytes: &[u8]) -> Result<()> {
        while let Some(group) = self.builder.feed(&mut bytes) {
            let mut reservation = self
                .disk
                .as_ref()
                .map(|disk| disk.reserve_space(self.root, disk.floor, group.hashes.len() as u64))
                .transpose()?;
            if let Some(reservation) = &mut reservation {
                reservation.defer();
            }
            if self.scratch.file.is_none() {
                let root = self.root.to_owned();
                self.scratch.file = Some(File::from_std(
                    tokio::task::spawn_blocking(move || tempfile::tempfile_in(root)).await??,
                ));
            }
            if let Some(reservation) = reservation {
                self.scratch.reservations.push(reservation);
            }
            let file = self.scratch.file.as_mut().unwrap();
            file.write_all(&group.hashes).await?;
            if let Some(reservation) = self.scratch.reservations.last_mut() {
                file.flush().await?;
                reservation.materialized();
            }
            self.groups += 1;
        }
        Ok(())
    }

    pub(super) async fn finish(mut self) -> Result<Verified> {
        if let Some(file) = &mut self.scratch.file {
            file.flush().await?;
            file.rewind().await?;
        }
        Ok(Verified {
            scratch: self.scratch,
            tail: self.builder.finish(),
            groups: self.groups,
            next: 0,
        })
    }
}

pub(crate) struct Verified {
    scratch: Scratch,
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
        self.scratch
            .file
            .as_mut()
            .unwrap()
            .read_exact(&mut hashes)
            .await?;
        let group = Group {
            index: self.next,
            hashes,
        };
        self.next += 1;
        Ok(Some(group))
    }
}
