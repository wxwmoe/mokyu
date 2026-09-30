use crate::app::App;
use anyhow::{Result, ensure};
use uuid::Uuid;

pub(crate) fn database_error(error: &anyhow::Error) -> Option<s3s::S3Error> {
    let code = error
        .downcast_ref::<sqlx::Error>()?
        .as_database_error()?
        .code()?;
    match code.as_ref() {
        "MKQ01" => {
            let mut error = s3s::S3Error::new(s3s::S3ErrorCode::Custom("QuotaExceeded".into()));
            error.set_status_code(hyper::StatusCode::FORBIDDEN);
            Some(error)
        }
        "MKQ02" => Some(s3s::s3_error!(OperationAborted)),
        _ => None,
    }
}

impl App {
    pub(crate) async fn reserve_quota(
        &self,
        stream: Uuid,
        size: i64,
        padding: bool,
        upload: Option<(Uuid, i32)>,
    ) -> Result<i64> {
        Ok(sqlx::query_scalar("SELECT quota_reserve($1,$2,$3,$4,$5)")
            .bind(stream)
            .bind(size)
            .bind(padding)
            .bind(upload.map(|u| u.0))
            .bind(upload.map(|u| u.1))
            .fetch_one(&self.db)
            .await?)
    }

    pub(crate) async fn complete_quota(&self, upload: Uuid, stream: Uuid, size: i64) -> Result<()> {
        let mut tx = self.db.begin().await?;
        let bucket: Uuid = sqlx::query_scalar(
            "SELECT bucket_id FROM uploads WHERE id=$1 AND state='completing' FOR SHARE",
        )
        .bind(upload)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query("SELECT id FROM streams WHERE id=$1 FOR SHARE")
            .bind(stream)
            .execute(&mut *tx)
            .await?;
        sqlx::query("SELECT quota_lock($1)")
            .bind(bucket)
            .execute(&mut *tx)
            .await?;
        // Reuse the accepted parts' reservation, including omitted parts until publication succeeds.
        let changed = sqlx::query(
            "UPDATE quota_reservations SET output_stream=$2 WHERE id=$1 AND logical_bytes>=$3",
        )
        .bind(upload)
        .bind(stream)
        .bind(size)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        ensure!(changed == 1, "multipart quota reservation is missing");
        tx.commit().await?;
        Ok(())
    }
}
