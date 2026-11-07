use super::queries::{journal as q, pack_work::Connection};
use anyhow::{Result, ensure};
use uuid::Uuid;

async fn control(db: &mut Connection, bucket: Uuid, operation: &str) -> Result<()> {
    ensure!(
        q::advance(bucket).execute(&mut *db).await?.rows_affected() == 1,
        "missing journal bucket"
    );
    ensure!(
        q::control(bucket, operation)
            .execute(&mut *db)
            .await?
            .rows_affected()
            == 1,
        "lost journal boundary"
    );
    Ok(())
}

pub(super) async fn configure(db: &mut Connection, bucket: Uuid, enabled: bool) -> Result<()> {
    // Publications take a shared gate until commit; the switch fences them before changing capture.
    let mode = q::lock_mode(bucket).fetch_one(&mut *db).await?;
    if (mode == 1) == enabled {
        return Ok(());
    }
    let operation = if enabled {
        ensure!(
            q::supported(bucket).fetch_one(&mut *db).await?,
            s3s::s3_error!(
                InvalidArgument,
                "metadata journal requires a managed S3 storage"
            )
        );
        if mode == 0 {
            q::create(bucket).execute(&mut *db).await?;
            "start"
        } else if q::restart_needed(bucket).fetch_one(&mut *db).await? {
            q::restart(bucket).execute(&mut *db).await?;
            "start"
        } else {
            "resume"
        }
    } else {
        "pause"
    };
    control(db, bucket, operation).await?;
    q::set_mode(bucket, if enabled { 1 } else { 2 })
        .execute(&mut *db)
        .await?;
    Ok(())
}

pub(super) async fn seed(db: &mut Connection, bucket: Uuid, limit: i64) -> Result<bool> {
    ensure!((1..=1024).contains(&limit), "invalid journal scan limit");
    if q::lock_mode(bucket).fetch_optional(&mut *db).await? != Some(1)
        || !q::scanning(bucket).fetch_one(&mut *db).await?
    {
        return Ok(false);
    }
    let count = i64::try_from(
        q::seed(bucket, limit)
            .execute(&mut *db)
            .await?
            .rows_affected(),
    )?;
    if count > 0 {
        q::scanned(bucket, count).execute(&mut *db).await?;
    }
    if count < limit {
        q::finish(bucket).execute(&mut *db).await?;
        control(db, bucket, "ready").await?;
    }
    Ok(true)
}
