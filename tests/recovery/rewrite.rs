use super::{Backend, migration};
use crate::model::{PackingStore, packing::RewriteTask, task::Execution};
use anyhow::Result;
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_DATABASE_URL_FILE pointing to a new isolated PostgreSQL database"]
async fn postgres_rewrite_interleaved_members() -> Result<()> {
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
    let task = Uuid::new_v4();
    let token = Uuid::new_v4();
    sqlx::query("INSERT INTO storages(id,name,type,path) VALUES($1,'rewrite-boundary','local','/data/rewrite-boundary')")
        .bind(storage).execute(&pool).await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id) VALUES($1)")
        .bind(storage)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO tasks(id,task_type,state,lease_token,lease_expires_at) VALUES($1,'rewrite','running',$2,now()+interval '5 minutes')")
        .bind(task).bind(token).execute(&pool).await?;
    let mut chunks = Vec::new();
    for n in 0..5u8 {
        let id: i64 = sqlx::query_scalar("INSERT INTO chunks(storage_id,hash,raw_size,standalone_size,encryption_algorithm,state) VALUES($1,$2,1,1,'none','ready') RETURNING id")
            .bind(storage).bind(vec![n; 32]).fetch_one(&pool).await?;
        sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,location_id,storage_id,standalone_size,standalone_compressed,state,encryption_algorithm,created_at) SELECT current_encoding_id,id,$2,storage_id,1,false,'ready','none',now() FROM chunks WHERE id=$1")
            .bind(id).bind(Uuid::new_v4()).execute(&pool).await?;
        chunks.push(id);
    }
    let mut packs = Vec::new();
    for group in [[chunks[4], chunks[0]], [chunks[3], chunks[1]]] {
        let id: i64 = sqlx::query_scalar("INSERT INTO packs(location_id,storage_id,encryption_algorithm,raw_size,stored_size,member_count,state,digest) VALUES($1,$2,'none',2,2,2,'ready',$3) RETURNING id")
            .bind(Uuid::new_v4()).bind(storage).bind(vec![7u8;32]).fetch_one(&pool).await?;
        for (ordinal, chunk) in group.into_iter().enumerate() {
            sqlx::query("INSERT INTO pack_members(pack_id,ordinal,chunk_id,offset_bytes) VALUES($1,$2,$3,$4)")
                .bind(id).bind(ordinal as i32).bind(chunk).bind(ordinal as i64).execute(&pool).await?;
            sqlx::query("UPDATE chunks SET pack_id=$2 WHERE id=$1")
                .bind(chunk)
                .bind(id)
                .execute(&pool)
                .await?;
        }
        packs.push(id);
    }
    sqlx::query("UPDATE storage_profiles SET compression_level=CASE WHEN compression_level=5 THEN 6 ELSE 5 END WHERE storage_id=$1")
        .bind(storage).execute(&pool).await?;
    let mut job = RewriteTask {
        storage_id: storage,
        upper_chunk_id: chunks[4],
        shard: 0,
        shards: 1,
        profile_revision: 0,
        target_key_id: None,
        target_key: String::new(),
        target_algorithm: "none".into(),
    };
    let (loose, candidate) = backend.rewrite_candidates(0, &job).await?;
    assert_eq!(loose.unwrap().id, chunks[2]);
    assert_eq!(candidate, Some((chunks[0], packs[0])));
    let (old, members, current) = backend.rewrite_pack_inputs(packs[0]).await?;
    assert_eq!(
        members.iter().map(|c| c.id).collect::<Vec<_>>(),
        vec![chunks[4], chunks[0]]
    );
    assert_eq!(current, vec![chunks[0], chunks[4]]);

    job.shards = 2;
    job.shard = chunks[0] % 2;
    assert!(
        backend
            .rewrite_candidates(chunks[0], &job)
            .await?
            .1
            .is_none()
    );
    job.shards = 1;
    job.shard = 0;

    let mut new = backend
        .prepare_pack(
            Uuid::new_v4(),
            storage,
            "none",
            None,
            2,
            2,
            Execution { id: task, token },
        )
        .await?;
    new.stored_size = Some(2);
    new.digest = Some(vec![8; 32]);
    backend
        .commit_rewrite_pack(task, token, &old, &new, &current, &members, chunks[0])
        .await?;
    let cursor: String = sqlx::query_scalar("SELECT cursor FROM tasks WHERE id=$1")
        .bind(task)
        .fetch_one(&pool)
        .await?;
    assert_eq!(cursor, chunks[0].to_string());
    let (loose, candidate) = backend.rewrite_candidates(cursor.parse()?, &job).await?;
    assert_eq!(loose.unwrap().id, chunks[2]);
    assert_eq!(candidate, Some((chunks[1], packs[1])));
    assert_eq!(
        backend
            .rewrite_pack_inputs(new.id)
            .await?
            .1
            .iter()
            .map(|c| c.id)
            .collect::<Vec<_>>(),
        vec![chunks[4], chunks[0]]
    );
    Ok(())
}
