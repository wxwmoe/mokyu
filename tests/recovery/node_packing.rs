use super::*;
use crate::{
    cluster::NodeState,
    model::{Id, node_pack as packing, session::Run},
};
use anyhow::Context;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::json;
use std::collections::BTreeSet;

async fn object(
    backend: &Backend,
    storage: Uuid,
    bucket: Uuid,
    count: i32,
    size: i32,
) -> Result<Vec<Id>> {
    let name = Uuid::now_v7().to_string();
    let stream:Id=sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,etag) VALUES($1,$2,'object','ready',$3,'fixture') RETURNING id")
        .bind(bucket).bind(&name).bind(i64::from(count)*i64::from(size)).fetch_one(&backend.pool).await?;
    sqlx::query(
        "INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,$2,$3,uuidv7())",
    )
    .bind(bucket)
    .bind(name)
    .bind(stream)
    .execute(&backend.pool)
    .await?;
    let ids:Vec<Id>=sqlx::query_scalar("INSERT INTO chunks(storage_id,hash,raw_size,standalone_size,encryption_algorithm,state) SELECT $1,digest(uuidv7()::text,'sha256'),$3,$3,'none','ready' FROM generate_series(1,$2) RETURNING id")
        .bind(storage).bind(count).bind(size).fetch_all(&backend.pool).await?;
    sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,storage_id,standalone_size,standalone_compressed,state,encryption_algorithm,created_at) SELECT current_encoding_id,id,storage_id,raw_size,false,'ready','none',clock_timestamp() FROM chunks WHERE id=ANY($1)")
        .bind(&ids).execute(&backend.pool).await?;
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) SELECT $1,(ordinality-1)*$3,$3,id FROM unnest($2::uuid[]) WITH ORDINALITY t(id,ordinality)")
        .bind(stream).bind(&ids).bind(size).execute(&backend.pool).await?;
    Ok(ids)
}

async fn enqueue(backend: &Backend, storage: Uuid, policy: &str, pack: Option<Id>) -> Result<Uuid> {
    Ok(sqlx::query_scalar("INSERT INTO tasks(task_type,state,detail) VALUES($1,'queued',$2) RETURNING id")
        .bind(if policy=="unpack" {"unpack"}else{"pack"})
        .bind(json!({"storage_id":storage,"policy":policy,"pack_id":pack,"upper_time":chrono::Utc::now(),"all":false}))
        .fetch_one(&backend.pool).await?)
}

fn batch(reply: work::Reply) -> packing::Batch {
    let Some(work::Plan::Pack { batch, .. }) = reply.plan else {
        panic!("expected packing plan");
    };
    *batch
}

fn complete(
    operation: Uuid,
    storage: Uuid,
    batch: &packing::Batch,
    packed: bool,
) -> Result<work::Completion> {
    let encoded = if packed {
        let p = batch.pack(storage, 0)?;
        packing::Encoded {
            chunks: vec![0; batch.chunks.len()],
            packs: vec![(
                0,
                packing::encoded(p.raw_size / 2, true)?,
                STANDARD.encode([9; 32]),
            )],
        }
    } else {
        packing::Encoded {
            chunks: batch
                .chunks
                .iter()
                .map(|c| {
                    Ok(c.ready.unwrap_or(packing::encoded(
                        i64::from(c.size) + if batch.algorithm == "none" { 0 } else { 16 },
                        false,
                    )?))
                })
                .collect::<Result<_>>()?,
            packs: vec![],
        }
    };
    Ok(work::Completion {
        operation,
        outcomes: vec![work::Outcome::Packed(encoded)],
    })
}

async fn state(backend: &Backend, task: Uuid) -> Result<(String, i64, Option<String>)> {
    Ok(
        sqlx::query_as("SELECT state,processed,error FROM tasks WHERE id=$1")
            .bind(task)
            .fetch_one(&backend.pool)
            .await?,
    )
}

pub(super) async fn check(
    backend: &Backend,
    peer: &Backend,
    node: &NodeState,
    key: &Key,
    run: Run,
) -> Result<()> {
    let pool = &backend.pool;
    let storage = Uuid::now_v7();
    sqlx::query("INSERT INTO storages(id,name,type,endpoint_url,remote_bucket_name,region,access_key_encrypted,secret_key_encrypted,identity_marker,block_key_id) VALUES($1,'work-packing','s3','http://backend:8333','work-packing','us-east-1',$2,$3,$4,(SELECT id FROM managed_keys WHERE name='work-rewrite-key'))")
        .bind(storage).bind(codec::protect(b"access",key,format!("storage-{storage}-access").as_bytes())?)
        .bind(codec::protect(b"secret",key,format!("storage-{storage}-secret").as_bytes())?)
        .bind(crate::storage::model::identity_marker(node.deployment_id,storage)?).execute(pool).await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id,min_storage_duration,control_concurrency,maintenance_concurrency,pack_enabled) VALUES($1,'0s',1,1,true)").bind(storage).execute(pool).await?;
    let bucket: Uuid = sqlx::query_scalar(
        "INSERT INTO buckets(name,storage_id) VALUES('work-packing',$1) RETURNING id",
    )
    .bind(storage)
    .fetch_one(pool)
    .await?;
    let ids = object(backend, storage, bucket, 6, 65536).await?;
    let request = |operation, completion| work::Exchange {
        run,
        operation,
        completion,
        class: 1,
        max_items: 64,
        cached: vec![],
    };
    let stream: Id = sqlx::query_scalar("SELECT stream_id FROM extents WHERE chunk_id=$1")
        .bind(ids[0])
        .fetch_one(pool)
        .await?;
    sqlx::query("INSERT INTO pack_maintenance(stream_id,generation) VALUES($1,1) ON CONFLICT(stream_id) DO UPDATE SET next_check_at=clock_timestamp(),reason='pack'")
        .bind(stream).execute(pool).await?;
    let task = enqueue(backend, storage, "pack", None).await?;
    sqlx::query("UPDATE tasks SET detail=detail||'{\"automatic\":true}'::jsonb WHERE id=$1")
        .bind(task)
        .execute(pool)
        .await?;
    let operation = Uuid::now_v7();
    let first = batch(
        backend
            .exchange_work(node, key, request(operation, None))
            .await?,
    );
    assert_eq!(first.chunks.len(), 6);
    assert_eq!(first.packs.len(), 3);
    let replay = batch(
        peer.exchange_work(node, key, request(operation, None))
            .await?,
    );
    assert_eq!(serde_json::to_value(&first)?, serde_json::to_value(replay)?);
    assert!(first.chunks.iter().all(|c| c.ready.is_some()));
    assert_eq!(
        first
            .packs
            .iter()
            .map(|p| p.nonce.clone().unwrap())
            .collect::<BTreeSet<_>>()
            .len(),
        3
    );
    let mut invalid = complete(operation, storage, &first, true)?;
    let work::Outcome::Packed(result) = &mut invalid.outcomes[0] else {
        unreachable!()
    };
    result.chunks[0] = first.chunks[0].ready.unwrap();
    assert!(
        peer.exchange_work(node, key, request(Uuid::now_v7(), Some(invalid)))
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM chunks WHERE id=ANY($1) AND pack_id IS NOT NULL"
        )
        .bind(&ids)
        .fetch_one(pool)
        .await?,
        0
    );
    let completion = complete(operation, storage, &first, true)?;
    peer.exchange_work(node, key, request(Uuid::now_v7(), Some(completion.clone())))
        .await?;
    backend
        .exchange_work(node, key, request(Uuid::now_v7(), Some(completion)))
        .await?;
    assert_eq!(state(backend, task).await?, ("completed".into(), 6, None));
    let old = Id::Uuid(first.packs[0].location);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM chunks WHERE id=ANY($1) AND pack_id=$2")
            .bind(&ids)
            .bind(old)
            .fetch_one(pool)
            .await?,
        6
    );
    let task = enqueue(backend, storage, "unpack", Some(old)).await?;
    let operation = Uuid::now_v7();
    let unpack = batch(
        backend
            .exchange_work(node, key, request(operation, None))
            .await?,
    );
    assert!(unpack.packs.is_empty());
    assert_eq!(unpack.chunks.len(), 6);
    assert!(unpack.chunks.iter().all(|c| c.ready.is_none()));
    assert_eq!(
        unpack
            .chunks
            .iter()
            .map(|c| c.nonce.clone().unwrap())
            .collect::<BTreeSet<_>>()
            .len(),
        6
    );
    backend
        .exchange_work(
            node,
            key,
            request(
                Uuid::now_v7(),
                Some(complete(operation, storage, &unpack, false)?),
            ),
        )
        .await?;
    assert_eq!(state(backend, task).await?, ("completed".into(), 6, None));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=$1")
            .bind(old)
            .fetch_one(pool)
            .await?,
        "retired"
    );
    for change in ["retry", "profile", "references"] {
        sqlx::query("UPDATE chunks SET repack_after=NULL,split_at=NULL,range_split_at=NULL WHERE storage_id=$1").bind(storage).execute(pool).await?;
        let task = enqueue(backend, storage, "pack", None).await?;
        let operation = Uuid::now_v7();
        let plan = batch(
            backend
                .exchange_work(node, key, request(operation, None))
                .await?,
        );
        let completion = if change == "retry" {
            work::Completion {
                operation,
                outcomes: vec![work::Outcome::Retry],
            }
        } else {
            complete(operation, storage, &plan, true)?
        };
        if change == "profile" {
            sqlx::query("UPDATE storage_profiles SET revision=revision+1 WHERE storage_id=$1")
                .bind(storage)
                .execute(pool)
                .await?;
        }
        let second = if change == "references" {
            let stream:Id=sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,etag) VALUES($1,'shared','object','ready',65536,'shared') RETURNING id").bind(bucket).fetch_one(pool).await?;
            sqlx::query(
                "INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES($1,0,65536,$2)",
            )
            .bind(stream)
            .bind(ids[0])
            .execute(pool)
            .await?;
            Some(stream)
        } else {
            None
        };
        peer.exchange_work(node, key, request(Uuid::now_v7(), Some(completion)))
            .await?;
        let result = state(backend, task).await?;
        assert_eq!((result.0, result.1), ("failed".into(), 0));
        assert_eq!(
            result.2.as_deref(),
            Some(match change {
                "retry" => "backend_io_failed",
                "profile" => "pack_profile_changed",
                _ => "source_changed",
            })
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT state FROM packs WHERE id=$1")
                .bind(plan.packs[0].location)
                .fetch_one(pool)
                .await?,
            "retired"
        );
        if let Some(stream) = second {
            sqlx::query("DELETE FROM extents WHERE stream_id=$1")
                .bind(stream)
                .execute(pool)
                .await?;
        }
        sqlx::query("UPDATE tasks SET state='queued',error=NULL WHERE id=$1")
            .bind(task)
            .execute(pool)
            .await?;
        let operation = Uuid::now_v7();
        let fresh = batch(
            backend
                .exchange_work(node, key, request(operation, None))
                .await?,
        );
        assert_ne!(fresh.packs[0].location, plan.packs[0].location);
        assert_ne!(fresh.packs[0].nonce, plan.packs[0].nonce);
        backend
            .exchange_work(
                node,
                key,
                request(
                    Uuid::now_v7(),
                    Some(complete(operation, storage, &fresh, false)?),
                ),
            )
            .await?;
        assert_eq!(state(backend, task).await?, ("completed".into(), 0, None));
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pack_inputs WHERE chunk_id=ANY($1)")
            .bind(&ids)
            .fetch_one(pool)
            .await?,
        0
    );
    let large = object(backend, storage, bucket, 4096, 1).await?;
    let old:Id=sqlx::query_scalar("INSERT INTO packs(storage_id,encryption_algorithm,raw_size,stored_size,member_count,state,digest) VALUES($1,'none',4096,$2,4096,'ready',$3) RETURNING id")
        .bind(storage).bind((4096+12+4096*crate::content::pack::MEMBER_SIZE) as i64).bind(vec![8u8;32]).fetch_one(pool).await?;
    sqlx::query("INSERT INTO pack_members(pack_id,ordinal,chunk_id,offset_bytes) SELECT $1,(ordinality-1)::int,id,ordinality-1 FROM unnest($2::uuid[]) WITH ORDINALITY t(id,ordinality)").bind(old).bind(&large).execute(pool).await?;
    sqlx::query("UPDATE chunks SET pack_id=$2 WHERE id=ANY($1)")
        .bind(&large)
        .bind(old)
        .execute(pool)
        .await?;
    sqlx::query("UPDATE chunk_encodings SET state='retired' WHERE chunk_id=ANY($1)")
        .bind(&large)
        .execute(pool)
        .await?;
    let task = enqueue(backend, storage, "unpack", Some(old)).await?;
    let operation = Uuid::now_v7();
    let started = std::time::Instant::now();
    let full = batch(
        tokio::time::timeout(
            std::time::Duration::from_secs(4),
            backend.exchange_work(node, key, request(operation, None)),
        )
        .await
        .context("full pack preparation exceeds RPC budget")??,
    );
    eprintln!("full pack preparation: {:?}", started.elapsed());
    assert_eq!(full.chunks.len(), 4096);
    assert!(full.packs.is_empty());
    assert!(serde_json::to_vec(&full)?.len() < 3 * 1024 * 1024);
    let result = complete(operation, storage, &full, false)?;
    assert!(serde_json::to_vec(&result)?.len() < 64 * 1024);
    assert_eq!(
        full.chunks
            .iter()
            .map(|c| c.nonce.clone().unwrap())
            .collect::<BTreeSet<_>>()
            .len(),
        4096
    );
    let started = std::time::Instant::now();
    tokio::time::timeout(
        std::time::Duration::from_secs(4),
        peer.exchange_work(node, key, request(Uuid::now_v7(), Some(result))),
    )
    .await
    .context("full pack publication exceeds RPC budget")??;
    eprintln!("full pack publication: {:?}", started.elapsed());
    assert_eq!(
        state(backend, task).await?,
        ("completed".into(), 4096, None)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM chunks WHERE id=ANY($1) AND pack_id IS NULL"
        )
        .bind(&large)
        .fetch_one(pool)
        .await?,
        4096
    );
    Ok(())
}
