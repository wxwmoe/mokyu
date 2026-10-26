use super::*;
use crate::{
    cluster::NodeState,
    model::{node_native::Inventory, session::Run},
};

pub(super) async fn check(
    backend: &Backend,
    node: &NodeState,
    key: &Key,
    run: Run,
    storage: Uuid,
    foreign: Uuid,
) -> Result<()> {
    let pool = &backend.pool;
    sqlx::query("UPDATE storage_profiles SET upload_retention='1s',deleted_chunk_retention='1d',multipart_idle_timeout='1s' WHERE storage_id=$1").bind(storage).execute(pool).await?;
    let bucket: Uuid = sqlx::query_scalar(
        "INSERT INTO buckets(name,storage_id) VALUES('work-cleanup',$1) RETURNING id",
    )
    .bind(storage)
    .fetch_one(pool)
    .await?;
    let old: Uuid = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,touched_at) VALUES($1,'old','object','retired',now()-interval '2 days') RETURNING id").bind(bucket).fetch_one(pool).await?;
    let recent: Uuid = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,touched_at) VALUES($1,'recent','object','retired',now()-interval '1 hour') RETURNING id").bind(bucket).fetch_one(pool).await?;
    let file: Uuid = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,touched_at) VALUES($1,'local','object','retired',now()-interval '2 days') RETURNING id").bind(bucket).fetch_one(pool).await?;
    sqlx::query(
        "INSERT INTO multipart_files(id,storage_id,node_id,state) VALUES($1,$2,$3,'sealed')",
    )
    .bind(file)
    .bind(storage)
    .bind(node.node_id)
    .execute(pool)
    .await?;
    let foreign_file: Uuid = sqlx::query_scalar(
        "INSERT INTO multipart_files(storage_id,node_id,state) VALUES($1,$2,'sealed') RETURNING id",
    )
    .bind(storage)
    .bind(foreign)
    .fetch_one(pool)
    .await?;
    let local: Uuid = sqlx::query_scalar("INSERT INTO fragments(size,hash,owner_stream,file_id,file_offset,sealed) VALUES(1,$1,$2,$2,0,true) RETURNING id").bind(vec![1u8;32]).bind(file).fetch_one(pool).await?;
    let staged: Uuid = sqlx::query_scalar("INSERT INTO fragments(size,hash,owner_stream,staging_storage_id,staging_object_key,sealed) VALUES(1,$1,$2,$3,'staging/kept',true) RETURNING id").bind(vec![2u8;32]).bind(file).bind(storage).fetch_one(pool).await?;
    for (offset, fragment) in [(0, local), (1, staged)] {
        sqlx::query(
            "INSERT INTO extents(stream_id,offset_bytes,length,fragment_id) VALUES($1,$2,1,$3)",
        )
        .bind(file)
        .bind(offset as i64)
        .bind(fragment)
        .execute(pool)
        .await?;
    }
    let lease: Uuid = sqlx::query_scalar("INSERT INTO stream_leases(stream_id,lease_type,expires_at) VALUES($1,'read',now()+interval '1 minute') RETURNING id").bind(file).fetch_one(pool).await?;
    let upload: Uuid = sqlx::query_scalar("INSERT INTO uploads(bucket_id,object_key,access_key,touched_at) VALUES($1,'expired','fixture',now()-interval '2 days') RETURNING id").bind(bucket).fetch_one(pool).await?;
    sqlx::query("INSERT INTO staging_objects(storage_id,object_key,bytes,created_at) VALUES($1,'staging/removed',1,now()-interval '10 minutes'),($1,'staging/kept',1,now()-interval '10 minutes')").bind(storage).execute(pool).await?;
    let pack: Uuid = sqlx::query_scalar("INSERT INTO packs(encryption_algorithm,raw_size,stored_size,member_count,state,unreferenced_at,storage_id,pack_fingerprint) VALUES('none',2,2,2,'retired',now()-interval '2 days',$1,$2) RETURNING id").bind(storage).bind(vec![3u8;32]).fetch_one(pool).await?;
    let cleanup: Uuid = sqlx::query_scalar("INSERT INTO tasks(task_type,state,detail) VALUES('cleanup','queued',jsonb_build_object('storage_id',$1::uuid)) RETURNING id").bind(storage).fetch_one(pool).await?;
    let gc: Uuid = sqlx::query_scalar("INSERT INTO tasks(task_type,state,detail) VALUES('gc','queued',jsonb_build_object('storage_id',$1::uuid)) RETURNING id").bind(storage).fetch_one(pool).await?;
    sqlx::query("UPDATE mokyu_meta SET maintenance=true,gc_paused=true")
        .execute(pool)
        .await?;
    sqlx::query("UPDATE maintenance_controls SET paused=true WHERE policy='cleanup'")
        .execute(pool)
        .await?;
    let request = |operation, completion| work::Exchange {
        run,
        operation,
        completion,
        class: 0,
        max_items: 64,
        cached: vec![],
    };
    assert!(!queries::work::pending(node.node_id).fetch_one(pool).await?);
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), None))
            .await?
            .plan
            .is_none()
    );
    sqlx::query("UPDATE maintenance_controls SET paused=false WHERE policy='cleanup'")
        .execute(pool)
        .await?;
    assert!(queries::work::pending(node.node_id).fetch_one(pool).await?);
    let operation = Uuid::now_v7();
    let plan = backend
        .exchange_work(node, key, request(operation, None))
        .await?
        .plan
        .unwrap();
    let work::Plan::Collect {
        items,
        metadata_count,
        ..
    } = plan
    else {
        panic!("expected collection plan")
    };
    assert_eq!(metadata_count, 2);
    assert_eq!(items.len(), 1);
    assert!(matches!(&items[0].source,work::Physical::Staging{key} if key=="staging/removed"));
    assert!(
        backend
            .exchange_work(
                node,
                key,
                request(
                    Uuid::now_v7(),
                    Some(work::Completion {
                        operation,
                        outcomes: vec![work::Outcome::Deleted { stored_at: None }]
                    })
                )
            )
            .await?
            .plan
            .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM streams WHERE id=$1")
            .bind(old)
            .fetch_one(pool)
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM uploads WHERE id=$1")
            .bind(upload)
            .fetch_one(pool)
            .await?,
        "aborted"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=$1")
            .bind(pack)
            .fetch_one(pool)
            .await?,
        "retired"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM streams WHERE id=$1")
            .bind(recent)
            .fetch_one(pool)
            .await?,
        "retired"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM fragments WHERE owner_stream=$1")
            .bind(file)
            .fetch_one(pool)
            .await?,
        2
    );
    assert!(
        backend
            .check_local_files(
                node,
                key,
                Inventory {
                    run,
                    files: vec![file, foreign_file],
                    recover: false
                }
            )
            .await?
            .iter()
            .all(|file| file.state != crate::model::node_raw::FileState::Removable)
    );
    sqlx::query("DELETE FROM stream_leases WHERE id=$1")
        .bind(lease)
        .execute(pool)
        .await?;
    sqlx::query("UPDATE tasks SET state='queued' WHERE id=$1")
        .bind(cleanup)
        .execute(pool)
        .await?;
    let mut completion = None;
    let mut finished = false;
    let mut deletions = Vec::new();
    for _ in 0..5 {
        let operation = Uuid::now_v7();
        let Some(plan) = backend
            .exchange_work(node, key, request(operation, completion.take()))
            .await?
            .plan
        else {
            finished = true;
            break;
        };
        let work::Plan::Collect { items, .. } = plan else {
            panic!("expected collection plan")
        };
        let mut outcomes = Vec::new();
        for item in items {
            let work::Physical::Staging { key } = item.source else {
                panic!("cleanup reclaimed a physical encoding")
            };
            deletions.push(key);
            outcomes.push(work::Outcome::Deleted { stored_at: None });
        }
        completion = Some(work::Completion {
            operation,
            outcomes,
        });
    }
    assert!(finished);
    assert_eq!(deletions, vec!["staging/kept"]);
    assert_eq!(
        backend
            .check_local_files(
                node,
                key,
                Inventory {
                    run,
                    files: vec![file, foreign_file],
                    recover: false
                }
            )
            .await?
            .into_iter()
            .filter(|file| file.state == crate::model::node_raw::FileState::Removable)
            .map(|file| file.id)
            .collect::<Vec<_>>(),
        vec![file]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM multipart_files WHERE id=$1")
            .bind(foreign_file)
            .fetch_one(pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM streams WHERE id=$1")
            .bind(file)
            .fetch_one(pool)
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM streams WHERE id=$1")
            .bind(recent)
            .fetch_one(pool)
            .await?,
        "retired"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM tasks WHERE id=$1")
            .bind(gc)
            .fetch_one(pool)
            .await?,
        "queued"
    );
    sqlx::query("UPDATE tasks SET state='paused' WHERE id=$1")
        .bind(gc)
        .execute(pool)
        .await?;
    sqlx::query("UPDATE mokyu_meta SET maintenance=false,gc_paused=false")
        .execute(pool)
        .await?;
    Ok(())
}
