use crate::authorization::{Action, Permit, Principal};
use anyhow::{Result, ensure};
use s3s::{dto::ETagCondition, s3_error};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{App, StoredStream};

use super::check_write_conditions;

impl App {
    pub async fn finish_stream(
        &self,
        id: Uuid,
        size: i64,
        etag: &str,
        checksums: Value,
    ) -> Result<()> {
        let changed=sqlx::query("UPDATE streams SET size=$2,etag=$3,checksums=$4,touched_at=now() WHERE id=$1 AND state='writing'").bind(id).bind(size).bind(etag).bind(checksums).execute(&self.db).await?.rows_affected();
        if changed != 1 {
            return Err(s3_error!(OperationAborted).into());
        }
        Ok(())
    }
    pub async fn publish(
        &self,
        id: Uuid,
        if_match: Option<&s3s::dto::ETagCondition>,
        if_none: Option<&s3s::dto::ETagCondition>,
    ) -> Result<()> {
        self.publish_complete(id, if_match, if_none, None).await
    }
    pub async fn publish_complete(
        &self,
        id: Uuid,
        if_match: Option<&s3s::dto::ETagCondition>,
        if_none: Option<&s3s::dto::ETagCondition>,
        completion: Option<(Uuid, String, Value)>,
    ) -> Result<()> {
        self.writable()?;
        let _coord = self.coord.lock().await;
        self.writable()?;
        let mut tx = self.db.begin().await?;
        let authority: sqlx::types::Json<Permit> =
            sqlx::query_scalar("SELECT write_authorization FROM streams WHERE id=$1")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
        authority.lock(&mut tx).await?;
        let s: StoredStream = sqlx::query_as("SELECT * FROM streams WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
        let bucket: String = sqlx::query_scalar("SELECT state FROM buckets WHERE id=$1 FOR SHARE")
            .bind(s.bucket_id)
            .fetch_one(&mut *tx)
            .await?;
        if bucket != "active" {
            return Err(s3_error!(OperationAborted).into());
        }
        let (old, epoch): (Option<Uuid>, Uuid) = sqlx::query_as(
            "SELECT stream_id,write_token FROM objects WHERE bucket_id=$1 AND object_key=$2 FOR UPDATE",
        )
        .bind(s.bucket_id)
        .bind(&s.object_key)
        .fetch_one(&mut *tx)
        .await?;
        if epoch != id || s.state != "writing" {
            return Err(s3_error!(OperationAborted, "write was superseded").into());
        }
        if let Some((upload, _, _)) = &completion {
            let expected: Option<(Option<Uuid>, i64)> = sqlx::query_as(
                "SELECT expected_stream_id,expected_size FROM web_uploads WHERE upload_id=$1",
            )
            .bind(upload)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some((version, size)) = expected {
                if old != version {
                    return Err(s3_error!(
                        PreconditionFailed,
                        "object changed since upload started"
                    )
                    .into());
                }
                if s.size != size {
                    return Err(s3_error!(IncompleteBody).into());
                }
            }
        }
        let previous: Option<String> = if let Some(old) = old {
            Some(
                sqlx::query_scalar("SELECT etag FROM streams WHERE id=$1")
                    .bind(old)
                    .fetch_one(&mut *tx)
                    .await?,
            )
        } else {
            None
        };
        check_write_conditions(previous.as_deref(), if_match, if_none)?;
        let invalid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=$1 AND (e.fragment_id IS NOT NULL OR c.state<>'ready'))").bind(id).fetch_one(&mut *tx).await?;
        ensure!(!invalid, "cannot publish an incomplete generation");
        let (end,contiguous):(i64,bool)=sqlx::query_as("SELECT COALESCE(max(offset_bytes+length),0),COALESCE(bool_and(offset_bytes=previous),true) FROM (SELECT offset_bytes,length,lag(offset_bytes+length,1,0::bigint) OVER (ORDER BY offset_bytes) AS previous FROM extents WHERE stream_id=$1) e").bind(id).fetch_one(&mut *tx).await?;
        ensure!(
            end == s.size && contiguous,
            "cannot publish a mapping with gaps or overlaps"
        );
        sqlx::query("UPDATE objects SET stream_id=$3 WHERE bucket_id=$1 AND object_key=$2")
            .bind(s.bucket_id)
            .bind(&s.object_key)
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE streams SET state='ready',touched_at=now() WHERE id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        if let Some(old) = old {
            sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1")
                .bind(old)
                .execute(&mut *tx)
                .await?;
        }
        if let Some((upload, hash, result)) = completion {
            let changed=sqlx::query("UPDATE uploads SET state='completed',result=$3,output_stream=$4,touched_at=now() WHERE id=$1 AND state='completing' AND manifest_hash=$2").bind(upload).bind(hash).bind(result).bind(id).execute(&mut *tx).await?.rows_affected();
            if changed != 1 {
                return Err(s3_error!(OperationAborted).into());
            }
            sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id IN (SELECT stream_id FROM parts WHERE upload_id=$1)").bind(upload).execute(&mut *tx).await?;
            sqlx::query("DELETE FROM parts WHERE upload_id=$1")
                .bind(upload)
                .execute(&mut *tx)
                .await?;
            crate::control::audit::checkpoint(
                &mut tx,
                "upload.complete",
                &upload.to_string(),
                serde_json::json!({"bucket_id":s.bucket_id,"key":s.object_key,"size":s.size}),
            )
            .await?;
        }
        tx.commit().await?;
        self.wake_gc.notify_one();
        self.seal_uploads(id, true).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("object-published").await;
        Ok(())
    }
    pub async fn delete_object(
        &self,
        principal: &Principal,
        bucket: Uuid,
        key: &str,
    ) -> Result<()> {
        self.change_object(principal, bucket, key, None, None).await
    }
    pub async fn change_object(
        &self,
        principal: &Principal,
        bucket: Uuid,
        key: &str,
        expected: Option<Uuid>,
        public: Option<bool>,
    ) -> Result<()> {
        self.writable()?;
        let _coord = self.coord.lock().await;
        self.writable()?;
        let mut tx = self.db.begin().await?;
        Permit::for_action(
            principal.clone(),
            bucket,
            if public.is_some() {
                Action::Acl
            } else {
                Action::Delete
            },
        )
        .lock(&mut tx)
        .await?;
        let state: String = sqlx::query_scalar("SELECT state FROM buckets WHERE id=$1 FOR SHARE")
            .bind(bucket)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3_error!(NoSuchBucket))?;
        if state != "active" {
            return Err(s3_error!(OperationAborted, "bucket is being purged").into());
        }
        let old: Option<Uuid> = sqlx::query_scalar(
            "SELECT stream_id FROM objects WHERE bucket_id=$1 AND object_key=$2 FOR UPDATE",
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        if expected.is_some() && expected != old {
            return Err(s3_error!(
                PreconditionFailed,
                "object changed; refresh before retrying"
            )
            .into());
        }
        if let Some(public) = public {
            let id = old.ok_or_else(|| s3_error!(NoSuchKey))?;
            sqlx::query("UPDATE streams SET public_read=$2 WHERE id=$1")
                .bind(id)
                .bind(public)
                .execute(&mut *tx)
                .await?;
            crate::control::actions::complete(&mut tx, Some(id)).await?;
            tx.commit().await?;
            return Ok(());
        }
        sqlx::query("INSERT INTO objects(bucket_id,object_key,write_token) VALUES($1,$2,$3) ON CONFLICT(bucket_id,object_key) DO UPDATE SET stream_id=NULL,write_token=excluded.write_token").bind(bucket).bind(key).bind(Uuid::new_v4()).execute(&mut *tx).await?;
        if let Some(old) = old {
            sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1")
                .bind(old)
                .execute(&mut *tx)
                .await?;
        }
        crate::control::actions::complete(&mut tx, None).await?;
        tx.commit().await?;
        self.wake_gc.notify_one();
        Ok(())
    }
}
