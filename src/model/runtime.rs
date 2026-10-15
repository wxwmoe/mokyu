use anyhow::Result;
use async_trait::async_trait;

pub(crate) struct RuntimeState {
    pub(crate) gc_paused: bool,
    pub(crate) maintenance: bool,
    pub(crate) pack_creation_paused: bool,
    pub(crate) mode: String,
    pub(crate) mode_revision: i64,
}

#[async_trait]
pub(crate) trait RuntimeStore: Send + Sync {
    async fn initialize(&self, maintenance: bool) -> Result<RuntimeState>;
    async fn state(&self) -> Result<RuntimeState>;
    async fn mode(&self) -> Result<String>;
    async fn pending_uploads(&self) -> Result<bool>;
    async fn health(&self) -> Result<()>;
    fn pool(&self) -> (u32, usize);
}

#[async_trait]
pub(crate) trait Owner: Send {
    async fn check(&mut self) -> Result<()>;
}
