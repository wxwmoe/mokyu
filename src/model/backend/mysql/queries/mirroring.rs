use super::*;
use crate::model::mirroring::{MirroringStore, Record};

pub(in super::super) fn enabled(bucket: Uuid, revision: i64) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM bucket_mirroring WHERE bucket_id=? AND revision=? AND enabled)").bind(bucket).bind(revision)
}

pub(in super::super) fn insert_key(bucket: Uuid, key: &str) -> Statement<'_> {
    sqlx::query("INSERT IGNORE INTO mirroring_keys(bucket_id,object_key) VALUES(?,?)")
        .bind(bucket)
        .bind(key.as_bytes())
}

pub(in super::super) fn key(
    bucket: Uuid,
    key: &str,
) -> Rows<'_, (bool, Option<Uuid>, Option<String>, i64, i64, i64)> {
    sqlx::query_as("SELECT k.blocked,k.task_id,t.state,k.revision,k.missing_until,k.retry_after FROM mirroring_keys k LEFT JOIN tasks t ON t.id=k.task_id WHERE k.bucket_id=? AND k.object_key=?").bind(bucket).bind(key.as_bytes())
}

pub(in super::super) fn assign(
    bucket: Uuid,
    key: &str,
    task: Uuid,
    revision: i64,
) -> Statement<'_> {
    sqlx::query("UPDATE mirroring_keys SET task_id=?,revision=?,external_id=NULL,external_revision=NULL,missing_until=0 WHERE bucket_id=? AND object_key=?").bind(task).bind(revision).bind(bucket).bind(key.as_bytes())
}

pub(in super::super) fn enqueue(input: &crate::model::mirroring::Start) -> Statement<'static> {
    sqlx::query("INSERT INTO tasks(id,task_type,bucket_id,state,detail) VALUES(?,'storage_migration',?,'queued',?)").bind(input.task).bind(input.bucket).bind(serde_json::json!({"operation":"mirroring","revision":input.revision,"rule_id":input.rule,"key":input.key,"storage_id":input.storage}))
}

pub(in super::super) fn record(bucket: Uuid) -> Rows<'static, (i64, Option<Vec<u8>>)> {
    sqlx::query_as("SELECT COALESCE(m.revision,0),m.config_encrypted FROM buckets b LEFT JOIN bucket_mirroring m ON m.bucket_id=b.id WHERE b.id=?").bind(bucket)
}

pub(in super::super) fn save(
    bucket: Uuid,
    revision: i64,
    enabled: bool,
    encrypted: &[u8],
) -> Statement<'_> {
    sqlx::query("INSERT INTO bucket_mirroring(bucket_id,revision,enabled,config_encrypted) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE revision=VALUES(revision),enabled=VALUES(enabled),config_encrypted=VALUES(config_encrypted)").bind(bucket).bind(revision).bind(enabled).bind(encrypted)
}

#[async_trait::async_trait]
impl MirroringStore for Backend {
    async fn bind_source(
        &self,
        task: crate::model::task::Execution,
        source: Option<(Uuid, i64)>,
    ) -> Result<()> {
        super::super::transactions::mirroring::bind_source(self, task, source).await
    }
    async fn failed(
        &self,
        task: crate::model::task::Execution,
        missing_ttl: Option<u32>,
    ) -> Result<()> {
        super::super::transactions::mirroring::failed(self, task, missing_ttl).await
    }
    async fn request(&self, input: crate::model::mirroring::Start) -> Result<Uuid> {
        super::super::transactions::mirroring::request(self, input).await
    }
    async fn get(&self, bucket: Uuid) -> Result<Record> {
        let (revision, encrypted) = record(bucket)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
        Ok(Record {
            revision,
            encrypted,
        })
    }
    async fn save(
        &self,
        principal: &Principal,
        bucket: Uuid,
        revision: i64,
        config: &crate::model::mirroring::Config,
        encrypted: Vec<u8>,
    ) -> Result<i64> {
        super::super::transactions::mirroring::save(
            self, principal, bucket, revision, config, encrypted,
        )
        .await
    }
}

pub(in super::super) fn grant(
    task: crate::model::task::Execution,
) -> Rows<'static, (Option<Uuid>, Option<i64>, Uuid)> {
    sqlx::query_as("SELECT k.external_id,k.external_revision,k.bucket_id FROM mirroring_keys k JOIN bucket_mirroring m ON m.bucket_id=k.bucket_id JOIN tasks t ON t.id=k.task_id WHERE t.id=? AND t.execution_token=? AND t.state='running' AND NOT k.blocked AND m.enabled AND m.revision=k.revision LOCK IN SHARE MODE").bind(task.id).bind(task.token)
}

pub(in super::super) fn origin_owner(stream: Id) -> Rows<'static, (Uuid, Uuid)> {
    sqlx::query_as("SELECT s.owner_task_id,s.owner_execution_token FROM streams s JOIN tasks t ON t.id=s.owner_task_id WHERE s.id=? AND CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation')) AS BINARY)='mirroring'").bind(stream)
}

pub(in super::super) fn source(task: Uuid, source: Option<(Uuid, i64)>) -> Statement<'static> {
    sqlx::query("UPDATE mirroring_keys SET external_id=?,external_revision=? WHERE task_id=?")
        .bind(source.map(|v| v.0))
        .bind(source.map(|v| v.1))
        .bind(task)
}

pub(in super::super) fn external(id: Uuid, revision: i64, bucket: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT e.id FROM external_buckets e JOIN buckets b ON b.project_id=e.project_id WHERE e.id=? AND e.revision=? AND e.enabled AND b.id=? LOCK IN SHARE MODE").bind(id).bind(revision).bind(bucket)
}

pub(in super::super) fn negative(task: Uuid, until: i64, retry: i64) -> Statement<'static> {
    sqlx::query("UPDATE mirroring_keys SET missing_until=?,retry_after=? WHERE task_id=?")
        .bind(until)
        .bind(retry)
        .bind(task)
}

pub(in super::super) fn ensure_claim(bucket: Uuid, key: &str, stream: Id) -> Statement<'_> {
    sqlx::query("INSERT IGNORE INTO objects(bucket_id,object_key,write_token) VALUES(?,?,?)")
        .bind(bucket)
        .bind(key.as_bytes())
        .bind(stream)
}

pub(in super::super) fn claim(bucket: Uuid, key: &str, stream: Id, task: Uuid) -> Statement<'_> {
    sqlx::query("UPDATE objects SET write_token=? WHERE bucket_id=? AND object_key=? AND stream_id IS NULL AND NOT EXISTS(SELECT 1 FROM streams s WHERE s.id=objects.write_token AND s.state='writing' AND (s.owner_task_id IS NULL OR (s.owner_task_id<>? AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=s.owner_task_id AND CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation')) AS BINARY)='mirroring'))))").bind(stream).bind(bucket).bind(key.as_bytes()).bind(task)
}

pub(in super::super) fn block(bucket: Uuid, key: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO mirroring_keys(bucket_id,object_key,blocked) SELECT ?,?,TRUE FROM bucket_mirroring WHERE bucket_id=? AND enabled ON DUPLICATE KEY UPDATE blocked=TRUE,task_id=NULL,missing_until=0").bind(bucket).bind(key.as_bytes()).bind(bucket)
}

pub(in super::super) fn occupied(bucket: Uuid, key: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM objects o LEFT JOIN streams s ON s.id=o.write_token LEFT JOIN mirroring_keys k ON k.bucket_id=o.bucket_id AND k.object_key=o.object_key WHERE o.bucket_id=? AND o.object_key=? AND (o.stream_id IS NOT NULL OR (s.state='writing' AND (s.owner_task_id IS NULL OR NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=s.owner_task_id AND CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation')) AS BINARY)='mirroring')))))").bind(bucket).bind(key.as_bytes())
}
