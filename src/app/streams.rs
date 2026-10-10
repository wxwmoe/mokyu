use crate::{
    authorization::{Action, Permit},
    config,
};
use anyhow::{Result, ensure};
use s3s::s3_error;
use serde_json::Value;
use uuid::Uuid;

use super::{Active, App, StoredStream};

impl App {
    pub async fn current(&self, bucket: Uuid, key: &str) -> Result<(StoredStream, Active)> {
        let _guard = self.coord.lock().await;
        let ttl = self.read_lease_seconds()?;
        let mut tx = self.db.begin().await?;
        let stream:StoredStream=sqlx::query_as("SELECT s.* FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=$1 AND o.object_key=$2 AND s.state='ready' FOR SHARE").bind(bucket).bind(key).fetch_optional(&mut *tx).await?.ok_or_else(||s3_error!(NoSuchKey))?;
        let lease_id = Uuid::new_v4();
        sqlx::query("INSERT INTO stream_leases(id,stream_id,lease_type,expires_at) VALUES($1,$2,'read',now()+$3*interval '1 second')")
            .bind(lease_id)
            .bind(stream.id)
            .bind(ttl)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        let pin = self.pin_with_lease(
            stream.id,
            Some(super::ReadLease {
                id: lease_id,
                db: self.db.clone(),
                ttl_seconds: ttl,
            }),
        );
        Ok((stream, pin))
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn new_stream(
        &self,
        authority: &Permit,
        bucket: Uuid,
        key: &str,
        kind: &str,
        metadata: Value,
        public: bool,
        claim_object: bool,
        completion: Option<Uuid>,
        owner: Option<(Uuid, Uuid)>,
    ) -> Result<(Uuid, Active)> {
        self.writable()?;
        if key.is_empty() || key.len() > 1024 {
            return Err(s3_error!(InvalidArgument, "invalid object key length").into());
        }
        let _guard = self.coord.lock().await;
        let id = Uuid::new_v4();
        self.writable()?;
        let mut tx = self.db.begin().await?;
        if let Some((task, token)) = owner {
            let owned: Option<Uuid> = sqlx::query_scalar("SELECT id FROM tasks WHERE id=$1 AND state='running' AND lease_token=$2 AND lease_expires_at>clock_timestamp() FOR SHARE")
                .bind(task)
                .bind(token)
                .fetch_optional(&mut *tx)
                .await?;
            ensure!(owned.is_some(), "storage migration lease lost");
        }
        let mut authority = authority.clone();
        authority.add(bucket, Action::Write);
        if public {
            authority.add(bucket, Action::Acl);
        }
        authority.lock(&mut tx).await?;
        let (state, paused): (String, bool) =
            sqlx::query_as("SELECT state,uploads_paused FROM buckets WHERE id=$1 FOR SHARE")
                .bind(bucket)
                .fetch_one(&mut *tx)
                .await?;
        if state != "active" {
            return Err(s3_error!(OperationAborted).into());
        }
        if paused && kind == "object" {
            let finishing: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM uploads WHERE id=$1 AND bucket_id=$2 AND object_key=$3 AND state='completing')")
                .bind(completion).bind(bucket).bind(key).fetch_one(&mut *tx).await?;
            if !finishing {
                return Err(
                    s3_error!(OperationAborted, "new uploads are paused for this bucket").into(),
                );
            }
        }
        sqlx::query("INSERT INTO streams(id,bucket_id,object_key,stream_type,state,metadata,public_read,write_authorization,owner_task_id,owner_lease_token) VALUES($1,$2,$3,$4,'writing',$5,$6,$7,$8,$9)").bind(id).bind(bucket).bind(key).bind(kind).bind(metadata).bind(public).bind(sqlx::types::Json(authority)).bind(owner.map(|value| value.0)).bind(owner.map(|value| value.1)).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO stream_leases(id,stream_id,lease_type,expires_at) VALUES($1,$2,'write',now()+$3*interval '1 second')")
            .bind(Uuid::new_v4())
            .bind(id)
            .bind(self.write_lease_seconds()?)
            .execute(&mut *tx)
            .await?;
        if claim_object {
            sqlx::query("INSERT INTO objects(bucket_id,object_key,write_token) VALUES($1,$2,$3) ON CONFLICT(bucket_id,object_key) DO UPDATE SET write_token=excluded.write_token").bind(bucket).bind(key).bind(id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok((id, self.pin(id)))
    }
    pub(crate) fn write_lease_seconds(&self) -> Result<i64> {
        Ok(config::seconds(&self.config.requests.upload_idle_timeout)?
            .max(30)
            .saturating_mul(2) as i64)
    }
    pub(crate) fn read_lease_seconds(&self) -> Result<i64> {
        Ok(config::seconds(&self.config.requests.read_idle_timeout)?
            .max(30)
            .saturating_mul(2) as i64)
    }
    pub(crate) async fn renew_write_lease(&self, stream: Uuid) -> Result<()> {
        let changed = sqlx::query("UPDATE stream_leases SET expires_at=now()+$2*interval '1 second' WHERE stream_id=$1 AND lease_type='write' AND expires_at>now() AND EXISTS(SELECT 1 FROM streams WHERE id=$1 AND state='writing')")
            .bind(stream)
            .bind(self.write_lease_seconds()?)
            .execute(&self.db)
            .await?
            .rows_affected();
        ensure!(changed == 1, "write lease expired");
        Ok(())
    }
    pub async fn tail(&self, stream: Uuid, offset: i64, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.renew_write_lease(stream).await?;
        let id = Uuid::new_v4();
        let (staging_storage_id, staging_object_key, _lease) =
            self.storage.write_fragment(id, data).await.map_err(|e| {
                tracing::warn!(error=%e,"multipart fragment could not be persisted");
                s3_error!(SlowDown, "local multipart storage unavailable")
            })?;
        sqlx::query("INSERT INTO fragments(id,size,hash,owner_stream,staging_storage_id,staging_object_key) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(id)
            .bind(data.len() as i32)
            .bind(blake3::hash(data).as_bytes().as_slice())
            .bind(stream)
            .bind(staging_storage_id)
            .bind(staging_object_key)
            .execute(&self.db)
            .await?;
        let mut tx = self.db.begin().await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("fragment-persisted").await;
        sqlx::query("UPDATE fragments SET sealed=true WHERE id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO extents(stream_id,offset_bytes,length,fragment_id) VALUES($1,$2,$3,$4)",
        )
        .bind(stream)
        .bind(offset)
        .bind(data.len() as i32)
        .bind(id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE streams SET size=$2,touched_at=now() WHERE id=$1")
            .bind(stream)
            .bind(offset + data.len() as i64)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}
