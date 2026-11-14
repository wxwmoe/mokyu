use super::publication::Allocation;
use crate::model::storage_recovery::{Part, Physical, Proof};
use anyhow::Result;
use uuid::Uuid;

pub(crate) struct Capture {
    pub operation: Uuid,
    pub revision: i64,
    pub after: Uuid,
    pub next: Uuid,
    pub complete: bool,
    pub records: Vec<Physical>,
}

#[derive(Clone, Copy)]
pub(crate) enum SnapshotFile {
    Page(u32),
    Manifest,
    Index,
    Checkpoint,
}

impl SnapshotFile {
    pub fn path(self, job: &super::snapshots::Job) -> Result<String> {
        use crate::storage::journal::snapshot;
        Ok(match self {
            Self::Page(number) => snapshot::page_path(job.id, number),
            Self::Manifest => snapshot::manifest_path(job.id),
            Self::Index => snapshot::index_path(),
            Self::Checkpoint => snapshot::checkpoint_path(
                job.epoch
                    .ok_or_else(|| anyhow::anyhow!("snapshot epoch missing"))?,
                job.revision.try_into()?,
            ),
        })
    }
}

#[async_trait::async_trait]
pub(crate) trait NativeJournal: Send + Sync {
    async fn native_restore_publication(
        &self,
        job: Uuid,
        token: Uuid,
        proof: &Proof,
        manifest: &crate::model::storage_cluster::Manifest,
    ) -> Result<()>;
    async fn native_restore_receipt(
        &self,
        job: Uuid,
        token: Uuid,
        media: Uuid,
        head: &Proof,
    ) -> Result<()>;
    async fn native_takeover_prepare(
        &self,
        job: Uuid,
        token: Uuid,
        part: &Part,
        identity: &serde_json::Value,
        key: &crate::content::codec::Key,
    ) -> Result<Proof>;
    async fn native_takeover_acknowledge(
        &self,
        job: Uuid,
        token: Uuid,
        proof: &Proof,
    ) -> Result<()>;
    async fn native_gc_prepare(
        &self,
        token: Uuid,
        file: &super::snapshots::Garbage,
        key: &crate::content::codec::Key,
    ) -> Result<Proof>;
    async fn native_gc_pending(&self, storage: Uuid, token: Uuid) -> Result<Option<Proof>>;
    async fn native_gc_acknowledge(&self, token: Uuid, proof: &Proof) -> Result<()>;
    async fn native_snapshot_material(
        &self,
        job: &super::snapshots::Job,
        token: Uuid,
        hold: &super::record::Hold,
        after: Option<(Uuid, i64, Uuid)>,
    ) -> Result<Capture>;
    async fn native_snapshot_prepare(
        &self,
        job: &super::snapshots::Job,
        token: Uuid,
        file: SnapshotFile,
        part: &Part,
        key: &crate::content::codec::Key,
    ) -> Result<Proof>;
    async fn native_snapshot_acknowledge(
        &self,
        job: &super::snapshots::Job,
        token: Uuid,
        file: SnapshotFile,
        proof: &Proof,
    ) -> Result<()>;
    async fn native_capture(&self, storage: Uuid, token: Uuid) -> Result<Option<Capture>>;
    async fn native_allocate(
        &self,
        storage: Uuid,
        token: Uuid,
        capture: &Capture,
        plan: &Allocation,
    ) -> Result<()>;
    async fn native_prepare(
        &self,
        storage: Uuid,
        token: Uuid,
        file: &super::publication::File,
        part: &Part,
        key: &crate::content::codec::Key,
    ) -> Result<Proof>;
    async fn native_history(
        &self,
        storage: Uuid,
        after: u64,
        through: u64,
    ) -> Result<Vec<(Proof, crate::model::storage_cluster::Manifest)>>;
    async fn native_evidence(&self, storage: Uuid, media: Uuid) -> Result<u64>;
    async fn native_receipt(
        &self,
        storage: Uuid,
        token: Uuid,
        media: Uuid,
        revision: u64,
    ) -> Result<()>;
    async fn native_acknowledge(
        &self,
        storage: Uuid,
        token: Uuid,
        file: &super::publication::File,
        proof: &Proof,
    ) -> Result<()>;
}
