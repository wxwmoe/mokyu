use super::{Backend, migration};
use crate::model::{TaskFinish, TaskLeaseStore, UploadCacheStore, task::Execution};
use anyhow::Result;
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_DATABASE_URL_FILE pointing to a new isolated PostgreSQL database"]
async fn postgres_pending_release_preserves_only_copy() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = PgPool::connect(url.trim()).await?;
    assert!(
        super::queries::metadata_table()
            .fetch_one(&pool)
            .await?
            .is_none()
    );
    migration::run(&pool, &mut *pool.acquire().await?, false).await?;
    let backend = Backend::new(pool.clone());
    let storage = Uuid::new_v4();
    let bucket = Uuid::new_v4();
    let stream = Uuid::new_v4();
    sqlx::query("INSERT INTO storages(id,name,type,path) VALUES($1,'pending-release','local','/data/pending-release')").bind(storage).execute(&pool).await?;
    sqlx::query("INSERT INTO buckets(id,name,storage_id) VALUES($1,'pending-release',$2)")
        .bind(bucket)
        .bind(storage)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO streams(id,bucket_id,object_key,stream_type,state) VALUES($1,$2,'pending','object','writing')").bind(stream).bind(bucket).execute(&pool).await?;
    let mut ids = Vec::new();
    for n in 0..3i64 {
        let id: i64 = sqlx::query_scalar("INSERT INTO chunks(storage_id,hash,raw_size,encryption_algorithm,state) VALUES($1,$2,1,'none','ready') RETURNING id")
            .bind(storage).bind(vec![n as u8;32]).fetch_one(&pool).await?;
        ids.push(id);
        sqlx::query("INSERT INTO pending_uploads(chunk_id,stream_id,offset_bytes,staged_size,staging_storage_id,staging_object_key,next_retry_at) VALUES($1,$2,$3,1,$4,$5,now())")
            .bind(id).bind(stream).bind(n).bind(storage).bind(format!("__mokyu/multipart/pending/{n}")).execute(&pool).await?;
        if n < 2 {
            sqlx::query(
                "INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES($1,$2,1,$3)",
            )
            .bind(stream)
            .bind(n)
            .bind(id)
            .execute(&pool)
            .await?;
        }
    }
    let result = backend.release(&ids, false).await?;
    assert_eq!(result.keys, vec!["__mokyu/multipart/pending/2"]);
    assert_eq!(
        result.staged,
        vec![(storage, "__mokyu/multipart/pending/2".into())]
    );
    let kept: Vec<i64> =
        sqlx::query_scalar("SELECT chunk_id FROM pending_uploads ORDER BY chunk_id")
            .fetch_all(&pool)
            .await?;
    assert_eq!(kept, ids[..2]);
    // One mapping now has a durable encoding; the other has no remaining readers.
    sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,location_id,storage_id,standalone_size,standalone_compressed,state,encryption_algorithm,created_at) SELECT current_encoding_id,id,$2,storage_id,1,false,'ready','none',now() FROM chunks WHERE id=$1")
        .bind(ids[0]).bind(Uuid::new_v4()).execute(&pool).await?;
    sqlx::query("DELETE FROM extents WHERE chunk_id=$1")
        .bind(ids[1])
        .execute(&pool)
        .await?;
    let result = backend.release(&ids, false).await?;
    assert_eq!(result.keys.len(), 2);
    assert_eq!(result.staged.len(), 2);
    assert!(result.staged.iter().all(|(_, name)| !name.ends_with("/2")));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pending_uploads")
            .fetch_one(&pool)
            .await?,
        0
    );
    let task = Uuid::new_v4();
    let execution = Execution {
        id: task,
        token: Uuid::new_v4(),
    };
    sqlx::query("INSERT INTO tasks(id,task_type,state,lease_token,lease_expires_at) VALUES($1,'upload','running',$2,now()+interval '5 minutes')")
        .bind(task).bind(execution.token)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO pack_inputs(task_id,chunk_id) VALUES($1,$2)")
        .bind(task)
        .bind(ids[0])
        .execute(&pool)
        .await?;
    UploadCacheStore::finish(&backend, execution).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pack_inputs WHERE task_id=$1")
            .bind(task)
            .fetch_one(&pool)
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(&pool)
            .await?,
        "completed"
    );
    sqlx::query("UPDATE tasks SET state='running' WHERE id=$1")
        .bind(task)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO pack_inputs(task_id,chunk_id) VALUES($1,$2)")
        .bind(task)
        .bind(ids[0])
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO pending_uploads(chunk_id,stream_id,offset_bytes,staged_size,owner_task,next_retry_at) VALUES($1,$2,0,1,$3,now())").bind(ids[0]).bind(stream).bind(task).execute(&pool).await?;
    TaskLeaseStore::finish(
        &backend,
        TaskFinish {
            execution,
            reuse_seconds: 60,
            error: Some("test failure".into()),
        },
    )
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pack_inputs WHERE task_id=$1")
            .bind(task)
            .fetch_one(&pool)
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(&pool)
            .await?,
        "failed"
    );
    let retry: (i32, Uuid) =
        sqlx::query_as("SELECT attempts,owner_task FROM pending_uploads WHERE chunk_id=$1")
            .bind(ids[0])
            .fetch_one(&pool)
            .await?;
    assert_eq!(retry, (1, task));
    Ok(())
}
