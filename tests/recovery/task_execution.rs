use super::{Backend, migration};
use crate::model::{
    Mapping, PackingStore, PhysicalStore, TaskControlStore, TaskExecutionStore, TaskFinish,
    UploadCacheStore, cluster::ClusterStore, packing::PackOutput, task::Execution,
};
use anyhow::Result;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_DATABASE_URL_FILE pointing to a new isolated PostgreSQL database"]
async fn postgres_task_execution_fences_stale_work() -> Result<()> {
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
    migration::run(&mut *pool.acquire().await?).await?;
    let backend = Backend::new(pool.clone(), pool.acquire().await?.detach(), false);
    let storage = Uuid::new_v4();
    let old = Execution {
        id: Uuid::new_v4(),
        token: Uuid::new_v4(),
    };
    let current = Execution {
        id: old.id,
        token: Uuid::new_v4(),
    };
    let parent = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO storages(id,name,type,path) VALUES($1,'execution','local','/data/execution')",
    )
    .bind(storage)
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO tasks(id,task_type,state,batch_total,detail) VALUES($1,'pack','running',1,$2)",
    )
    .bind(parent)
    .bind(json!({"role":"parent"}))
    .execute(&pool)
    .await?;
    sqlx::query("INSERT INTO tasks(id,task_type,state,lease_token,lease_expires_at,parent_task_id) VALUES($1,'upload','running',$2,now()+interval '5 minutes',$3)")
        .bind(old.id).bind(current.token).bind(parent).execute(&pool).await?;
    let mut chunks = Vec::new();
    for n in 0..2u8 {
        let id: i64 = sqlx::query_scalar("INSERT INTO chunks(storage_id,hash,raw_size,standalone_size,encryption_algorithm,state) VALUES($1,$2,1,1,'none','ready') RETURNING id")
            .bind(storage).bind(vec![n;32]).fetch_one(&pool).await?;
        sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,location_id,storage_id,standalone_size,standalone_compressed,state,encryption_algorithm,created_at) SELECT current_encoding_id,id,$2,storage_id,1,false,'ready','none',now() FROM chunks WHERE id=$1")
            .bind(id).bind(Uuid::new_v4()).execute(&pool).await?;
        chunks.push(id);
    }
    let chunk = chunks[0];
    let sources = backend.ready_chunks(&chunks).await?;
    let source = sources
        .iter()
        .find(|source| source.id == chunk)
        .unwrap()
        .clone();
    let rows = sources
        .into_iter()
        .enumerate()
        .map(|(index, chunk)| Mapping {
            chunk,
            offset_bytes: index as i64,
            pack_id: None,
        })
        .collect::<Vec<_>>();
    backend.pin_pack_inputs(current, &rows).await?;
    let mut prepared = backend
        .prepare_pack(Uuid::new_v4(), storage, "none", None, 2, 2, current)
        .await?;
    prepared.stored_size = Some(2);
    prepared.digest = Some(vec![9; 32]);
    backend.pack_metadata(&prepared).await?;
    backend.insert_pack_members(prepared.id, &rows).await?;
    let mut physical = source.clone();
    physical.location_id = Uuid::new_v4();
    physical.encoding_id = backend.prepare_encoding(&physical, current).await?;
    backend.encoding_metadata(&physical).await?;
    sqlx::query("INSERT INTO pending_uploads(chunk_id,offset_bytes,staged_size,owner_task,next_retry_at) VALUES($1,0,1,$2,now())")
        .bind(chunk).bind(old.id).execute(&pool).await?;

    assert!(!backend.running(old).await?);
    TaskControlStore::pause(&backend, old).await?;
    backend
        .progress(old, "stale", 99, json!({"stale":true}))
        .await?;
    assert!(!backend.complete_usage(old).await?);
    assert!(backend.pin_pack_inputs(old, &rows).await.is_err());
    assert!(
        backend
            .prepare_pack(Uuid::new_v4(), storage, "none", None, 2, 2, old)
            .await
            .is_err()
    );
    assert!(backend.prepare_encoding(&physical, old).await.is_err());
    let output = [PackOutput {
        chunks: chunks.clone(),
        pack: Some(prepared.clone()),
        independent: None,
    }];
    assert!(
        backend
            .commit_pack_outputs(old, &rows, &output, false, &Default::default(), 60.0)
            .await
            .is_err()
    );
    assert!(UploadCacheStore::finish(&backend, old).await.is_err());
    for error in [None, Some("late upload error".into())] {
        TaskExecutionStore::finish(
            &backend,
            TaskFinish {
                execution: old,
                reuse_seconds: 60,
                error,
            },
        )
        .await?;
    }
    let state: (String, Option<String>, i64, Uuid) =
        sqlx::query_as("SELECT state,cursor,processed,lease_token FROM tasks WHERE id=$1")
            .bind(old.id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(state, ("running".into(), None, 0, current.token));
    let physical_state: (String, String, i64, i32) = sqlx::query_as("SELECT (SELECT state FROM packs WHERE id=$1),(SELECT state FROM chunk_encodings WHERE encoding_id=$2),(SELECT count(*) FROM pack_inputs WHERE task_id=$3),(SELECT attempts FROM pending_uploads WHERE chunk_id=$4)")
        .bind(prepared.id).bind(physical.encoding_id).bind(old.id).bind(chunk).fetch_one(&pool).await?;
    assert_eq!(
        physical_state,
        ("preparing".into(), "uploading".into(), 2, 0)
    );
    assert_eq!(
        backend.ready_chunks(&[chunk]).await?[0].encoding_id,
        source.encoding_id
    );

    // Expiry also rejects the current token, before another node has claimed it.
    sqlx::query("UPDATE tasks SET lease_expires_at=now()-interval '1 second' WHERE id=$1")
        .bind(old.id)
        .execute(&pool)
        .await?;
    assert!(!backend.confirm(current.id, current.token).await?);
    assert!(!backend.complete_usage(current).await?);
    assert!(
        backend
            .commit_pack_outputs(current, &rows, &output, false, &Default::default(), 60.0)
            .await
            .is_err()
    );
    sqlx::query("UPDATE tasks SET lease_expires_at=now()+interval '5 minutes' WHERE id=$1")
        .bind(old.id)
        .execute(&pool)
        .await?;
    backend
        .commit_pack_outputs(current, &rows, &output, false, &Default::default(), 60.0)
        .await?;
    assert!(
        matches!(backend.source(chunk, false).await?, crate::model::PhysicalSource::Pack(pack) if pack.id == prepared.id)
    );
    assert!(backend.complete_usage(current).await?);
    TaskExecutionStore::finish(
        &backend,
        TaskFinish {
            execution: old,
            reuse_seconds: 60,
            error: None,
        },
    )
    .await?;
    assert_eq!(
        TaskExecutionStore::load(&backend, parent).await?.state,
        "running"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM chunk_encodings WHERE encoding_id=$1")
            .bind(physical.encoding_id)
            .fetch_one(&pool)
            .await?,
        "uploading"
    );
    assert!(backend.change_state(parent, true, false).await?);
    TaskExecutionStore::finish(
        &backend,
        TaskFinish {
            execution: current,
            reuse_seconds: 60,
            error: None,
        },
    )
    .await?;
    assert_eq!(
        TaskExecutionStore::load(&backend, parent).await?.state,
        "paused"
    );
    assert!(backend.change_state(parent, true, true).await?);
    assert_eq!(
        TaskExecutionStore::load(&backend, parent).await?.state,
        "completed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM chunk_encodings WHERE encoding_id=$1")
            .bind(physical.encoding_id)
            .fetch_one(&pool)
            .await?,
        "retired"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pack_inputs WHERE task_id=$1")
            .bind(old.id)
            .fetch_one(&pool)
            .await?,
        0
    );
    assert_eq!(
        TaskExecutionStore::load(&backend, old.id)
            .await?
            .execution_token,
        None
    );
    assert!(
        matches!(backend.source(chunk, false).await?, crate::model::PhysicalSource::Pack(pack) if pack.id == prepared.id)
    );
    sqlx::query("INSERT INTO storage_profiles(storage_id,revision) VALUES($1,2)")
        .bind(storage)
        .execute(&pool)
        .await?;
    let family = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    for (index, id) in family.iter().enumerate() {
        let detail = json!({"role":if index==0 {"parent"} else {"child"},"storage_id":storage,"profile_revision":1,"target_key":"keep-target","cutoff":"keep-cutoff"});
        sqlx::query("INSERT INTO tasks(id,task_type,state,parent_task_id,detail,cursor,processed) VALUES($1,'rewrite',$2,$3,$4,'keep-cursor',7)")
            .bind(id).bind(if index==2 {"completed"} else {"paused"})
            .bind(if index==0 {None} else {Some(family[0])}).bind(detail).execute(&pool).await?;
    }
    assert!(backend.change_state(family[0], true, true).await?);
    for (index, id) in family.iter().enumerate() {
        let task = TaskExecutionStore::load(&backend, *id).await?;
        assert_eq!(
            task.detail["profile_revision"],
            if index == 2 { 1 } else { 2 }
        );
        assert_eq!(task.detail["target_key"], "keep-target");
        assert_eq!(task.detail["cutoff"], "keep-cutoff");
        assert_eq!(task.cursor.as_deref(), Some("keep-cursor"));
    }
    Ok(())
}
