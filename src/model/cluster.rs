use crate::model::Id;
use crate::{authorization::Principal, cluster::NodeState, content::codec::Key};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

#[async_trait]
pub(crate) trait ClusterStore: Send + Sync {
    async fn change_objects(
        &self,
        node: &NodeState,
        key: &Key,
        input: super::node_object::Request,
    ) -> Result<super::node_object::Outcome>;
    async fn prepare_raw(
        &self,
        node: &NodeState,
        key: &Key,
        input: super::node_raw::Prepare,
    ) -> Result<super::node_raw::Prepared>;
    async fn commit_raw(
        &self,
        node: &NodeState,
        key: &Key,
        input: super::node_raw::Commit,
    ) -> Result<super::node_raw::Prepared>;
    async fn prepare_request(
        &self,
        node: &NodeState,
        key: &Key,
        input: super::node_request::Prepare,
    ) -> Result<super::node_request::Reply>;
    async fn prepare_chunks(
        &self,
        node: &NodeState,
        key: &Key,
        input: super::node_chunk::Prepare,
    ) -> Result<super::node_chunk::Assembly>;
    async fn activate_chunks(
        &self,
        node: &NodeState,
        key: &Key,
        input: super::node_chunk::Prepare,
        nonces: Vec<super::node_chunk::Nonce>,
    ) -> Result<super::node_chunk::Prepared>;
    async fn commit_chunks(
        &self,
        node: &NodeState,
        key: &Key,
        input: super::node_chunk::Commit,
    ) -> Result<i64>;
    async fn publish_write(
        &self,
        node: &NodeState,
        key: &Key,
        input: super::node_write::Publish,
    ) -> Result<super::node_write::Reply>;
    async fn begin_write(
        &self,
        node: &NodeState,
        key: &Key,
        input: super::node_write::Begin,
    ) -> Result<super::node_write::Reply>;
    async fn write_status(
        &self,
        node: &NodeState,
        key: &Key,
        input: super::node_write::Lookup,
        abort: bool,
    ) -> Result<super::node_write::Reply>;
    async fn report_observations(
        &self,
        node: &NodeState,
        key: &Key,
        input: super::node_observation::Batch,
    ) -> Result<Vec<bool>>;
    async fn report_read(
        &self,
        node: &NodeState,
        key: &Key,
        input: super::node_read::Report,
    ) -> Result<Vec<Uuid>>;
    async fn open_read(
        &self,
        node: &NodeState,
        key: &Key,
        input: super::node_read::Query,
    ) -> Result<super::node_read::Reply>;
    async fn read_window(
        &self,
        node: &NodeState,
        key: &Key,
        input: super::node_read::Window,
    ) -> Result<super::node_read::Reply>;
    async fn provision_node(
        &self,
        principal: &Principal,
        name: &str,
        role: &str,
        key: &Key,
    ) -> Result<NodeState>;
    async fn rotate_node(&self, principal: &Principal, name: &str, key: &Key) -> Result<NodeState>;
    async fn open_session(
        &self,
        node: &NodeState,
        key: &Key,
        input: super::session::Open,
    ) -> Result<super::session::Snapshot>;
    async fn sync_node(
        &self,
        node: &NodeState,
        key: &Key,
        run: super::session::Run,
    ) -> Result<super::session::Snapshot>;
    async fn pulse_session(
        &self,
        node: &NodeState,
        key: &Key,
        run: super::session::Run,
        epoch: i64,
        sequence: i64,
        draining: bool,
        resources: super::node_read::KeepAlive,
    ) -> Result<super::session::Snapshot>;
    async fn remote_upload(&self, upload: Uuid) -> Result<Option<super::multipart::RemoteUpload>>;
    async fn begin_remote(
        &self,
        upload: Uuid,
        storage: Uuid,
        revision: i64,
        node: super::raw::Processor<'_>,
    ) -> Result<super::multipart::RemoteUpload>;
    async fn claim_remote(
        &self,
        upload: Uuid,
        token: Uuid,
        node: super::raw::Processor<'_>,
    ) -> Result<bool>;
    async fn initialized_remote(
        &self,
        upload: Uuid,
        token: Uuid,
        upstream: Option<&str>,
        node: super::raw::Processor<'_>,
    ) -> Result<bool>;
    async fn commit_remote_part(&self, input: super::multipart::RemoteCommit<'_>) -> Result<()>;
    async fn claim_remote_part(
        &self,
        token: Uuid,
        node: super::raw::Processor<'_>,
        stream: Option<Id>,
    ) -> Result<Option<super::multipart::RemotePart>>;
    async fn renew_remote_part(
        &self,
        work: &super::multipart::RemotePart,
        node: super::raw::Processor<'_>,
    ) -> Result<bool>;
    async fn finish_remote_part(
        &self,
        work: &super::multipart::RemotePart,
        etag: &str,
        node: super::raw::Processor<'_>,
    ) -> Result<bool>;
    async fn retry_remote_part(
        &self,
        work: &super::multipart::RemotePart,
        error: &str,
    ) -> Result<()>;
    async fn remote_parts(
        &self,
        upload: Uuid,
        streams: &[Id],
    ) -> Result<Vec<(Id, i32, Option<String>)>>;
    async fn freeze_remote(
        &self,
        upload: Uuid,
        token: Uuid,
        hash: &str,
    ) -> Result<super::multipart::RemoteUpload>;
    async fn resend_remote(&self, upload: Uuid, token: Uuid, streams: &[Id]) -> Result<()>;
    async fn adopt_remote(&self, input: super::multipart::RemoteOutput<'_>) -> Result<Uuid>;
    async fn remote_charges(&self, node: Uuid, after: Id) -> Result<Vec<(Id, Uuid, i64)>>;
    async fn claim_remote_cleanup(
        &self,
        token: Uuid,
        node: super::raw::Processor<'_>,
    ) -> Result<Option<super::multipart::RemoteUpload>>;
    async fn clean_remote(&self, upload: Uuid, token: Uuid) -> Result<()>;
    async fn pending_read(&self, streams: &[Id], node: Uuid) -> Result<bool>;
    async fn prioritize_uploads(&self, streams: &[Id]) -> Result<()>;
    async fn claim_staging(&self, storage: Uuid, key: &str, bytes: u64, limit: u64) -> Result<()>;
    async fn release_staging(&self, storage: Uuid, key: &str) -> Result<()>;
    async fn stale_staging(&self, storage: Option<Uuid>) -> Result<Vec<(Uuid, String)>>;

    async fn claim_task(
        &self,
        input: super::TaskClaim<'_>,
        node: &NodeState,
        role: &str,
        key: &Key,
    ) -> Result<Option<Uuid>>;
    async fn recover_tasks(&self) -> Result<u64>;
    async fn renew_task(&self, id: Uuid, token: Uuid) -> Result<bool>;
    async fn start_usage(&self, node: Option<Uuid>) -> Result<serde_json::Value>;
    async fn complete_usage(&self, task: super::task::Execution) -> Result<bool>;
    async fn acquire_integrity_rate(
        &self,
        storage: Uuid,
        rates: &[(&str, f64, f64)],
    ) -> Result<bool>;
    async fn acquire_storage_permit(
        &self,
        storage: Uuid,
        direction: &str,
        limit: i32,
        holder: Uuid,
    ) -> Result<Option<Uuid>>;
    async fn renew_storage_permit(&self, token: Uuid) -> Result<bool>;
    async fn release_storage_permit(&self, token: Uuid) -> Result<()>;
    async fn current_stream(
        &self,
        bucket: Uuid,
        key: &str,
        ttl_seconds: i64,
    ) -> Result<(crate::app::StoredStream, Uuid)>;
    async fn read_page(
        &self,
        stream: Id,
        lease: Uuid,
        from: i64,
        until: i64,
    ) -> Result<Vec<super::ReadExtent>>;
    async fn create_stream(&self, input: super::StreamCreate, ttl_seconds: i64) -> Result<Id>;
    async fn renew_read(&self, id: Uuid, ttl_seconds: i64) -> Result<()>;
    async fn release_read(&self, id: Uuid) -> Result<()>;
    async fn renew_write(&self, stream: Id, ttl_seconds: i64) -> Result<()>;
    async fn expire_stream_leases(&self) -> Result<()>;
    async fn profile_changes(&self) -> Result<Box<dyn ProfileChanges>>;
    async fn listen_tasks(&self, wake: std::sync::Arc<tokio::sync::Notify>) -> Result<()>;
    async fn mode(&self) -> Result<String>;
    async fn status(&self) -> Result<ClusterStatus>;
    async fn nodes(
        &self,
        filter: &super::node::NodeFilter,
        desired: &serde_json::Value,
    ) -> Result<super::node::NodePage>;
    async fn node(&self, id: Uuid, desired: &serde_json::Value) -> Result<Option<NodeView>>;
    async fn node_counts(&self, desired: &serde_json::Value)
    -> Result<Vec<super::node::NodeCount>>;
    async fn setting_counts(
        &self,
        desired: &serde_json::Value,
    ) -> Result<Vec<super::node::SettingCount>>;
    async fn node_report(&self, id: Uuid) -> Result<Option<super::node::SavedReport>>;
    async fn node_tasks(&self, id: Uuid) -> Result<Vec<super::metrics::WorkSummary>>;
    async fn sample_node_runtime(
        &self,
        state: &NodeState,
        role: &str,
        key: &Key,
        sample: &super::metrics::NodeSample,
    ) -> Result<bool>;
    async fn aggregate_runtime(&self, interval: u64, retention: u64) -> Result<()>;
    async fn cluster_runtime(
        &self,
        query: &super::metrics::RuntimeQuery,
    ) -> Result<Vec<super::metrics::ClusterRuntimePoint>>;
    async fn node_runtime(
        &self,
        id: Uuid,
        query: &super::metrics::RuntimeQuery,
    ) -> Result<Vec<super::metrics::NodeRuntimePoint>>;
    async fn begin_report(
        &self,
        state: &NodeState,
        role: &str,
        key: &Key,
        boot: Uuid,
        started: DateTime<Utc>,
    ) -> Result<()>;
    async fn report(
        &self,
        state: &NodeState,
        role: &str,
        key: &Key,
        report: &super::node::NodeReport,
    ) -> Result<bool>;
    async fn set_mode(&self, principal: &Principal, mode: &str) -> Result<()>;
    async fn release(&self, name: &str) -> Result<()>;
    async fn recover(&self, name: &str, role: &str) -> Result<()>;
    async fn credential(&self, id: Uuid) -> Result<Option<(String, Vec<u8>)>>;
    async fn register(
        &self,
        name: &str,
        role: &str,
        credential: String,
        key: &Key,
    ) -> Result<Option<NodeState>>;
    async fn heartbeat(&self, state: &NodeState, role: &str, key: &Key) -> Result<()>;
    async fn leader(&self, key: i64) -> Result<Option<Box<dyn Leader>>>;
}
#[async_trait]
pub(crate) trait ProfileChanges: Send {
    async fn next(&mut self) -> Result<Uuid>;
}
#[async_trait]
pub(crate) trait Leader: Send {
    async fn finish(self: Box<Self>) -> Result<()>;
}
#[derive(Serialize, ToSchema)]
pub(crate) struct ClusterStatus {
    pub(crate) mode: String,
    pub(crate) enabled_nodes: i64,
    pub(crate) local_storages: i64,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct NodeView {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) role: String,
    pub(crate) enabled: bool,
    pub(crate) last_seen_at: Option<DateTime<Utc>>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) contact: String,
    pub(crate) work: String,
    pub(crate) configuration: String,
    pub(crate) settings: serde_json::Value,
    pub(crate) sampled_at: Option<DateTime<Utc>>,
    pub(crate) received_at: Option<DateTime<Utc>>,
    pub(crate) report_fresh: bool,
}
