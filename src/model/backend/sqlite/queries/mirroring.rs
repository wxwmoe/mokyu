use super::*;
use crate::model::mirroring::{MirroringStore, Record};

pub(in super::super) fn enabled(bucket: Uuid, revision: i64) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM bucket_mirroring WHERE bucket_id=? AND revision=? AND enabled)").bind(bucket).bind(revision)
}

pub(in super::super) fn insert_key(bucket: Uuid, key: &str) -> Statement<'_> {
    sqlx::query(
        "INSERT INTO mirroring_keys(bucket_id,object_key) VALUES(?,?) ON CONFLICT DO NOTHING",
    )
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
    sqlx::query("UPDATE mirroring_keys SET task_id=?,revision=?,external_id=NULL,external_revision=NULL,missing_until=0,retry_after=0 WHERE bucket_id=? AND object_key=?").bind(task).bind(revision).bind(bucket).bind(key.as_bytes())
}

pub(in super::super) fn enqueue(input: &crate::model::mirroring::Start) -> Statement<'static> {
    sqlx::query("INSERT INTO tasks(id,task_type,bucket_id,state,detail) VALUES(?,'storage_migration',?,'queued',?)").bind(input.task).bind(input.bucket).bind(serde_json::json!({"operation":"mirroring","policy":"mirroring","revision":input.revision,"rule_id":input.rule,"key":input.key,"storage_id":input.storage}))
}

pub(in super::super) fn record(bucket: Uuid) -> Rows<'static, (i64, Option<Vec<u8>>)> {
    sqlx::query_as("SELECT COALESCE(m.revision,0),m.config_encrypted FROM buckets b LEFT JOIN bucket_mirroring m ON m.bucket_id=b.id WHERE b.id=?").bind(bucket)
}

pub(in super::super) fn save(
    bucket: Uuid,
    revision: i64,
    enabled: bool,
    encrypted: &[u8],
    authority: crate::model::mirroring::Authority,
) -> Statement<'_> {
    sqlx::query("INSERT INTO bucket_mirroring(bucket_id,revision,enabled,config_encrypted,authority) VALUES(?,?,?,?,?) ON CONFLICT(bucket_id) DO UPDATE SET revision=excluded.revision,enabled=excluded.enabled,config_encrypted=excluded.config_encrypted,authority=excluded.authority").bind(bucket).bind(revision).bind(enabled).bind(encrypted).bind(sqlx::types::Json(authority))
}

#[async_trait::async_trait]
impl MirroringStore for Backend {
    async fn range(
        &self,
        bucket: Uuid,
        object: &str,
        key: &crate::content::codec::Key,
    ) -> Result<Option<crate::model::mirroring::Ranged>> {
        super::super::transactions::mirroring::range(self, bucket, object, key).await
    }
    async fn range_result(
        &self,
        task: crate::model::task::Execution,
        bytes: u64,
        conflict: bool,
    ) -> Result<()> {
        super::super::transactions::mirroring::range_result(self, task, bytes, conflict).await
    }
    async fn blocks(
        &self,
        principal: &Principal,
        bucket: Uuid,
        after: Option<&str>,
        limit: usize,
    ) -> Result<crate::model::mirroring::BlockPage> {
        principal.require(self, bucket, Action::Settings).await?;
        let limit = limit.clamp(1, 100);
        let mut keys: Vec<String> = blocked(bucket, after, limit + 1)
            .fetch_all(&self.read)
            .await?;
        let next = (keys.len() > limit).then(|| keys[limit - 1].clone());
        keys.truncate(limit);
        Ok(crate::model::mirroring::BlockPage { keys, next })
    }
    async fn unblock(&self, principal: &Principal, bucket: Uuid, key: &str) -> Result<()> {
        super::super::transactions::mirroring::unblock(self, principal, bucket, key).await
    }
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
            .fetch_optional(&self.read)
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
    sqlx::query_as("SELECT k.external_id,k.external_revision,k.bucket_id FROM mirroring_keys k JOIN bucket_mirroring m ON m.bucket_id=k.bucket_id JOIN tasks t ON t.id=k.task_id WHERE t.id=? AND t.execution_token=? AND t.state='running' AND NOT k.blocked AND m.enabled AND m.revision=k.revision").bind(task.id).bind(task.token)
}

pub(in super::super) fn origin_owner(stream: Id) -> Rows<'static, (Uuid, Uuid)> {
    sqlx::query_as("SELECT s.owner_task_id,s.owner_execution_token FROM streams s JOIN tasks t ON t.id=s.owner_task_id WHERE s.id=? AND json_extract(t.detail,'$.operation')='mirroring'").bind(stream)
}

pub(in super::super) fn source(task: Uuid, source: Option<(Uuid, i64)>) -> Statement<'static> {
    sqlx::query("UPDATE mirroring_keys SET external_id=?,external_revision=? WHERE task_id=?")
        .bind(source.map(|v| v.0))
        .bind(source.map(|v| v.1))
        .bind(task)
}

pub(in super::super) fn external(id: Uuid, revision: i64, bucket: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT e.id FROM external_buckets e JOIN buckets b ON b.project_id=e.project_id WHERE e.id=? AND e.revision=? AND e.enabled AND b.id=?").bind(id).bind(revision).bind(bucket)
}

pub(in super::super) fn negative(task: Uuid, until: i64, retry: i64) -> Statement<'static> {
    sqlx::query("UPDATE mirroring_keys SET missing_until=?,retry_after=? WHERE task_id=?")
        .bind(until)
        .bind(retry)
        .bind(task)
}

pub(in super::super) fn ensure_claim(bucket: Uuid, key: &str, stream: Id) -> Statement<'_> {
    sqlx::query("INSERT INTO objects(bucket_id,object_key,write_token) VALUES(?,?,?) ON CONFLICT DO NOTHING").bind(bucket).bind(key.as_bytes()).bind(stream)
}

pub(in super::super) fn claim(bucket: Uuid, key: &str, stream: Id, task: Uuid) -> Statement<'_> {
    sqlx::query("UPDATE objects SET write_token=? WHERE bucket_id=? AND object_key=? AND stream_id IS NULL AND NOT EXISTS(SELECT 1 FROM streams s WHERE s.id=objects.write_token AND s.state='writing' AND (s.owner_task_id IS NULL OR (s.owner_task_id<>? AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=s.owner_task_id AND json_extract(t.detail,'$.operation')='mirroring'))))").bind(stream).bind(bucket).bind(key.as_bytes()).bind(task)
}

pub(in super::super) fn block(bucket: Uuid, key: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO mirroring_keys(bucket_id,object_key,blocked) SELECT ?,?,TRUE FROM bucket_mirroring WHERE bucket_id=? AND enabled ON CONFLICT(bucket_id,object_key) DO UPDATE SET blocked=TRUE,task_id=NULL,missing_until=0").bind(bucket).bind(key.as_bytes()).bind(bucket)
}

pub(in super::super) fn occupied(bucket: Uuid, key: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM objects o LEFT JOIN streams s ON s.id=o.write_token LEFT JOIN mirroring_keys k ON k.bucket_id=o.bucket_id AND k.object_key=o.object_key WHERE o.bucket_id=? AND o.object_key=? AND (o.stream_id IS NOT NULL OR (s.state='writing' AND (s.owner_task_id IS NULL OR NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=s.owner_task_id AND json_extract(t.detail,'$.operation')='mirroring')))))").bind(bucket).bind(key.as_bytes())
}

pub(in super::super) fn authority(
    bucket: Uuid,
) -> Scalar<'static, Option<sqlx::types::Json<crate::model::mirroring::Authority>>> {
    sqlx::query_scalar("SELECT authority FROM bucket_mirroring WHERE bucket_id=?").bind(bucket)
}
pub(in super::super) fn saved_user(id: Uuid) -> Rows<'static, (String, i64)> {
    sqlx::query_as("SELECT role,authorization_revision FROM web_users WHERE id=? AND enabled AND NOT must_change_password").bind(id)
}
pub(in super::super) fn active(bucket: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT COUNT(*) FROM mirroring_keys k JOIN tasks t ON t.id=k.task_id WHERE k.bucket_id=? AND t.state IN ('queued','running','paused')").bind(bucket)
}
pub(in super::super) fn blocked(
    bucket: Uuid,
    after: Option<&str>,
    limit: usize,
) -> Scalar<'_, String> {
    sqlx::query_scalar("SELECT CAST(object_key AS TEXT) FROM mirroring_keys WHERE bucket_id=? AND blocked AND object_key>? ORDER BY object_key LIMIT ?").bind(bucket).bind(after.unwrap_or("").as_bytes()).bind(limit as i64)
}
pub(in super::super) fn erase(bucket: Uuid, key: &str) -> Statement<'_> {
    sqlx::query("DELETE FROM mirroring_keys WHERE bucket_id=? AND object_key=?")
        .bind(bucket)
        .bind(key.as_bytes())
}
pub(in super::super) fn expired(bucket: Uuid, now: i64) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT CAST(object_key AS TEXT) FROM mirroring_keys WHERE bucket_id=? AND NOT blocked AND missing_until<=? AND retry_after<=? AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=mirroring_keys.task_id AND t.state IN ('queued','running','paused')) ORDER BY object_key LIMIT 64").bind(bucket).bind(now).bind(now)
}

pub(in super::super) fn published(task: crate::model::task::Execution) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT w.next_offset FROM node_writes w JOIN streams s ON s.id=w.stream_id WHERE s.owner_task_id=? AND s.owner_execution_token=? AND w.phase='published' LIMIT 1").bind(task.id).bind(task.token)
}

pub(in super::super) fn invalidate(bucket: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='failed',error='origin_rule_changed',execution_token=NULL,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id IN (SELECT task_id FROM mirroring_keys WHERE bucket_id=?) AND state IN ('queued','running','paused')").bind(bucket)
}

pub(in super::super) fn target(bucket: Uuid) -> Scalar<'static, Option<Uuid>> {
    sqlx::query_scalar("SELECT storage_id FROM buckets WHERE id=?").bind(bucket)
}

pub(in super::super) fn reader(bucket: Uuid, object: &str) -> Rows<'_, (Uuid, Uuid)> {
    sqlx::query_as("SELECT t.id,t.execution_token FROM mirroring_keys k JOIN tasks t ON t.id=k.task_id WHERE k.bucket_id=? AND k.object_key=? AND NOT k.blocked AND t.state='running' AND t.execution_token IS NOT NULL").bind(bucket).bind(object.as_bytes())
}
pub(in super::super) fn range_result(
    task: crate::model::task::Execution,
    bytes: u64,
    conflict: bool,
) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET detail=json_set(detail,'$.range_bytes',COALESCE(json_extract(detail,'$.range_bytes'),0)+?,'$.range_requests',COALESCE(json_extract(detail,'$.range_requests'),0)+1),error=CASE WHEN ? AND state='running' AND execution_token=? THEN 'origin_version_changed' ELSE error END,state=CASE WHEN ? AND state='running' AND execution_token=? THEN 'failed' ELSE state END WHERE id=? AND json_extract(detail,'$.operation')='mirroring' AND (NOT ? OR state='running' AND execution_token=?)").bind(bytes as i64).bind(conflict).bind(task.token).bind(conflict).bind(task.token).bind(task.id).bind(conflict).bind(task.token)
}
