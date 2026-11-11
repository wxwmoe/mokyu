use super::*;
use crate::model::mirroring::{MirroringStore, Record};

pub(in super::super) fn enabled(bucket: Uuid, revision: i64) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM bucket_mirroring WHERE bucket_id=$1 AND revision=$2 AND enabled)").bind(bucket).bind(revision)
}

pub(in super::super) fn insert_key(bucket: Uuid, key: &str) -> Statement<'_> {
    sqlx::query(
        "INSERT INTO mirroring_keys(bucket_id,object_key) VALUES($1,$2) ON CONFLICT DO NOTHING",
    )
    .bind(bucket)
    .bind(key)
}

pub(in super::super) fn key(
    bucket: Uuid,
    key: &str,
) -> Rows<'_, (bool, Option<Uuid>, Option<String>, i64, i64, i64)> {
    sqlx::query_as("SELECT k.blocked,k.task_id,t.state,k.revision,k.missing_until,k.retry_after FROM mirroring_keys k LEFT JOIN tasks t ON t.id=k.task_id WHERE k.bucket_id=$1 AND k.object_key=$2").bind(bucket).bind(key)
}

pub(in super::super) fn assign(
    bucket: Uuid,
    key: &str,
    task: Uuid,
    revision: i64,
) -> Statement<'_> {
    sqlx::query("UPDATE mirroring_keys SET task_id=$1,revision=$2,external_id=NULL,external_revision=NULL,missing_until=0,retry_after=0 WHERE bucket_id=$3 AND object_key=$4").bind(task).bind(revision).bind(bucket).bind(key)
}

pub(in super::super) fn enqueue(input: &crate::model::mirroring::Start) -> Statement<'static> {
    sqlx::query("INSERT INTO tasks(id,task_type,bucket_id,state,detail) VALUES($1,'storage_migration',$2,'queued',$3)").bind(input.task).bind(input.bucket).bind(serde_json::json!({"operation":"mirroring","revision":input.revision,"rule_id":input.rule,"key":input.key,"storage_id":input.storage}))
}

pub(in super::super) fn claim_task(id: Uuid, node: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='running',started_at=COALESCE(started_at,clock_timestamp()),lease_node_id=$2,lease_token=$3,lease_expires_at=clock_timestamp()+interval '60 seconds',updated_at=clock_timestamp() WHERE id=$1 AND state='queued' AND detail->>'operation'='mirroring'").bind(id).bind(node).bind(token)
}

pub(in super::super) fn record(bucket: Uuid) -> Rows<'static, (i64, Option<Vec<u8>>)> {
    sqlx::query_as("SELECT COALESCE(m.revision,0),m.config_encrypted FROM buckets b LEFT JOIN bucket_mirroring m ON m.bucket_id=b.id WHERE b.id=$1").bind(bucket)
}

pub(in super::super) fn save(
    bucket: Uuid,
    revision: i64,
    enabled: bool,
    encrypted: &[u8],
    authority: crate::model::mirroring::Authority,
) -> Statement<'_> {
    sqlx::query("INSERT INTO bucket_mirroring(bucket_id,revision,enabled,config_encrypted,authority) VALUES($1,$2,$3,$4,$5) ON CONFLICT(bucket_id) DO UPDATE SET revision=excluded.revision,enabled=excluded.enabled,config_encrypted=excluded.config_encrypted,authority=excluded.authority").bind(bucket).bind(revision).bind(enabled).bind(encrypted).bind(sqlx::types::Json(authority))
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
            .fetch_all(&self.pool)
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
    sqlx::query_as("SELECT k.external_id,k.external_revision,k.bucket_id FROM mirroring_keys k JOIN bucket_mirroring m ON m.bucket_id=k.bucket_id JOIN tasks t ON t.id=k.task_id WHERE t.id=$1 AND t.lease_token=$2 AND t.state='running' AND NOT k.blocked AND m.enabled AND m.revision=k.revision AND (t.lease_node_id IS NULL OR t.lease_expires_at>clock_timestamp()) FOR SHARE OF k,m,t").bind(task.id).bind(task.token)
}

pub(in super::super) fn origin_owner(stream: Id) -> Rows<'static, (Uuid, Uuid)> {
    sqlx::query_as("SELECT s.owner_task_id,s.owner_lease_token FROM streams s JOIN tasks t ON t.id=s.owner_task_id WHERE s.id=$1 AND t.detail->>'operation'='mirroring'").bind(stream)
}

pub(in super::super) fn source(task: Uuid, source: Option<(Uuid, i64)>) -> Statement<'static> {
    sqlx::query("UPDATE mirroring_keys SET external_id=$1,external_revision=$2 WHERE task_id=$3")
        .bind(source.map(|v| v.0))
        .bind(source.map(|v| v.1))
        .bind(task)
}

pub(in super::super) fn external(id: Uuid, revision: i64, bucket: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT e.id FROM external_buckets e JOIN buckets b ON b.project_id=e.project_id WHERE e.id=$1 AND e.revision=$2 AND e.enabled AND b.id=$3 FOR SHARE OF e").bind(id).bind(revision).bind(bucket)
}

pub(in super::super) fn negative(task: Uuid, until: i64, retry: i64) -> Statement<'static> {
    sqlx::query("UPDATE mirroring_keys SET missing_until=$1,retry_after=$2 WHERE task_id=$3")
        .bind(until)
        .bind(retry)
        .bind(task)
}

pub(in super::super) fn ensure_claim(bucket: Uuid, key: &str, stream: Id) -> Statement<'_> {
    sqlx::query("INSERT INTO objects(bucket_id,object_key,write_token) VALUES($1,$2,$3) ON CONFLICT DO NOTHING").bind(bucket).bind(key).bind(stream)
}

pub(in super::super) fn claim(bucket: Uuid, key: &str, stream: Id, task: Uuid) -> Statement<'_> {
    sqlx::query("UPDATE objects SET write_token=$1 WHERE bucket_id=$2 AND object_key=$3 AND stream_id IS NULL AND NOT EXISTS(SELECT 1 FROM streams s WHERE s.id=objects.write_token AND s.state='writing' AND (s.owner_task_id IS NULL OR (s.owner_task_id<>$4 AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=s.owner_task_id AND t.detail->>'operation'='mirroring'))))").bind(stream).bind(bucket).bind(key).bind(task)
}

pub(in super::super) fn block(bucket: Uuid, key: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO mirroring_keys(bucket_id,object_key,blocked) SELECT $1,$2,TRUE FROM bucket_mirroring WHERE bucket_id=$3 AND enabled ON CONFLICT(bucket_id,object_key) DO UPDATE SET blocked=TRUE,task_id=NULL,missing_until=0").bind(bucket).bind(key).bind(bucket)
}

pub(in super::super) fn occupied(bucket: Uuid, key: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM objects o LEFT JOIN streams s ON s.id=o.write_token LEFT JOIN mirroring_keys k ON k.bucket_id=o.bucket_id AND k.object_key=o.object_key WHERE o.bucket_id=$1 AND o.object_key=$2 AND (o.stream_id IS NOT NULL OR (s.state='writing' AND (s.owner_task_id IS NULL OR NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=s.owner_task_id AND t.detail->>'operation'='mirroring')))))").bind(bucket).bind(key)
}

pub(in super::super) fn authority(
    bucket: Uuid,
) -> Scalar<'static, Option<sqlx::types::Json<crate::model::mirroring::Authority>>> {
    sqlx::query_scalar("SELECT authority FROM bucket_mirroring WHERE bucket_id=$1").bind(bucket)
}
pub(in super::super) fn saved_user(id: Uuid) -> Rows<'static, (String, i64)> {
    sqlx::query_as("SELECT role,authorization_revision FROM web_users WHERE id=$1 AND enabled AND NOT must_change_password FOR SHARE").bind(id)
}
pub(in super::super) fn active(bucket: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT COUNT(*) FROM mirroring_keys k JOIN tasks t ON t.id=k.task_id WHERE k.bucket_id=$1 AND t.state IN ('queued','running','paused')").bind(bucket)
}
pub(in super::super) fn blocked(
    bucket: Uuid,
    after: Option<&str>,
    limit: usize,
) -> Scalar<'_, String> {
    sqlx::query_scalar("SELECT object_key FROM mirroring_keys WHERE bucket_id=$1 AND blocked AND object_key>$2 ORDER BY object_key LIMIT $3").bind(bucket).bind(after.unwrap_or("")).bind(limit as i64)
}
pub(in super::super) fn erase(bucket: Uuid, key: &str) -> Statement<'_> {
    sqlx::query("DELETE FROM mirroring_keys WHERE bucket_id=$1 AND object_key=$2")
        .bind(bucket)
        .bind(key)
}
pub(in super::super) fn expired(bucket: Uuid, now: i64) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT object_key FROM mirroring_keys WHERE bucket_id=$1 AND NOT blocked AND missing_until<=$2 AND retry_after<=$3 AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=mirroring_keys.task_id AND t.state IN ('queued','running','paused')) ORDER BY object_key LIMIT 64").bind(bucket).bind(now).bind(now)
}

pub(in super::super) fn published(task: crate::model::task::Execution) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT w.next_offset FROM node_writes w JOIN streams s ON s.id=w.stream_id WHERE s.owner_task_id=$1 AND s.owner_lease_token=$2 AND w.state='published' LIMIT 1").bind(task.id).bind(task.token)
}

pub(in super::super) fn invalidate(bucket: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='failed',error='origin_rule_changed',lease_token=NULL,updated_at=clock_timestamp() WHERE id IN (SELECT task_id FROM mirroring_keys WHERE bucket_id=$1) AND state IN ('queued','running','paused')").bind(bucket)
}

pub(in super::super) fn target(bucket: Uuid) -> Scalar<'static, Option<Uuid>> {
    sqlx::query_scalar("SELECT storage_id FROM buckets WHERE id=$1 FOR SHARE").bind(bucket)
}

pub(in super::super) fn reader(bucket: Uuid, object: &str) -> Rows<'_, (Uuid, Uuid)> {
    sqlx::query_as("SELECT t.id,t.lease_token FROM mirroring_keys k JOIN tasks t ON t.id=k.task_id WHERE k.bucket_id=$1 AND k.object_key=$2 AND NOT k.blocked AND t.state='running' AND t.lease_token IS NOT NULL").bind(bucket).bind(object)
}
pub(in super::super) fn range_result(
    task: crate::model::task::Execution,
    bytes: u64,
    conflict: bool,
) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET detail=detail||jsonb_build_object('range_bytes',COALESCE((detail->>'range_bytes')::bigint,0)+$3,'range_requests',COALESCE((detail->>'range_requests')::bigint,0)+1),state=CASE WHEN $4 AND state='running' AND lease_token=$2 THEN 'failed' ELSE state END,error=CASE WHEN $4 AND state='running' AND lease_token=$2 THEN 'origin_version_changed' ELSE error END WHERE id=$1 AND detail->>'operation'='mirroring' AND (NOT $4 OR state='running' AND lease_token=$2)").bind(task.id).bind(task.token).bind(bytes as i64).bind(conflict)
}
