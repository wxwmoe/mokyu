use chrono::{DateTime, Utc};
use uuid::Uuid;

pub(crate) const LEASE_SECONDS: i64 = 90;
pub(crate) const BOOT_LIMIT: i64 = 4096;
pub(crate) const SECTIONS: &[&str] = &["requests", "uploads", "metrics", "maintenance", "io"];

#[derive(Clone, Copy, Debug)]
pub(crate) struct Run {
    pub boot: Uuid,
    pub fence: Uuid,
}

#[derive(Clone, Copy)]
pub(crate) struct Open {
    pub boot: Uuid,
    pub connection: Uuid,
    pub previous_epoch: i64,
}

#[derive(sqlx::FromRow)]
pub(crate) struct Session {
    pub credential_generation: i64,
    pub boot_id: Uuid,
    pub fence: Uuid,
    pub epoch: i64,
    pub connection_id: Uuid,
    pub expires_at: DateTime<Utc>,
    pub sequence: i64,
    pub draining: bool,
}

pub(crate) struct Snapshot {
    pub run: Run,
    pub epoch: i64,
    pub valid_for_ms: i64,
    pub maintenance: bool,
    pub draining: bool,
    pub settings: Vec<(String, serde_json::Value, i64)>,
    pub grants: Vec<super::node_read::GrantResult>,
    pub native_pending: bool,
}
