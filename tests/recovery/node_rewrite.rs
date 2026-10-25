use super::*;
use crate::{
    cluster::NodeState,
    model::{
        Id,
        key::{ManagedKeyCreate, ManagedKeyStore},
        session::Run,
    },
};
use serde_json::json;
use std::collections::BTreeSet;

async fn chunk(backend: &Backend, storage: Uuid, size: i32) -> Result<Id> {
    let hash = blake3::hash(Uuid::new_v4().as_bytes()).as_bytes().to_vec();
    let id: Id = sqlx::query_scalar("INSERT INTO chunks(storage_id,hash,raw_size,standalone_size,encryption_algorithm,state) VALUES($1,$2,$3,$3,'none','ready') RETURNING id")
        .bind(storage).bind(hash).bind(size).fetch_one(&backend.pool).await?;
    sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,storage_id,standalone_size,standalone_compressed,state,encryption_algorithm,created_at) SELECT current_encoding_id,id,storage_id,raw_size,false,'ready','none',clock_timestamp() FROM chunks WHERE id=$1")
        .bind(id).execute(&backend.pool).await?;
    Ok(id)
}

async fn enqueue(backend: &Backend, storage: Uuid, target: Uuid) -> Result<Uuid> {
    let revision: i64 =
        sqlx::query_scalar("SELECT revision FROM storage_profiles WHERE storage_id=$1")
            .bind(storage)
            .fetch_one(&backend.pool)
            .await?;
    Ok(sqlx::query_scalar("INSERT INTO tasks(task_type,state,detail) VALUES('rewrite','queued',$1) RETURNING id")
        .bind(json!({"storage_id":storage,"upper_chunk_id":Uuid::from_u128(u128::MAX),"shard":0,"shards":1,
            "profile_revision":revision,"target_key_id":target,"target_key":"work-rewrite-key","target_algorithm":"aes-256-gcm"}))
        .fetch_one(&backend.pool).await?)
}

fn items(reply: work::Reply) -> Vec<work::Rewrite> {
    let Some(work::Plan::Rewrite { items, .. }) = reply.plan else {
        panic!("expected rewrite plan");
    };
    items
}

fn completion(operation: Uuid, items: &[work::Rewrite]) -> work::Completion {
    work::Completion {
        operation,
        outcomes: items
            .iter()
            .map(|item| match item {
                work::Rewrite::Chunk { target, .. } => work::Outcome::Encoded {
                    size: i64::from(target.raw_size) + 16,
                    compressed: false,
                    digest: None,
                },
                work::Rewrite::Pack { target, .. } => work::Outcome::Encoded {
                    size: target.payload_size().unwrap() as i64 + 16,
                    compressed: false,
                    digest: Some(vec![8; 32]),
                },
            })
            .collect(),
    }
}

async fn result(backend: &Backend, task: Uuid) -> Result<(String, i64, Option<String>)> {
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
    admin: &Principal,
) -> Result<()> {
    let pool = &backend.pool;
    let storage = Uuid::now_v7();
    sqlx::query("INSERT INTO storages(id,name,type,endpoint_url,remote_bucket_name,region,access_key_encrypted,secret_key_encrypted,identity_marker) VALUES($1,'work-rewrite','s3','http://backend:8333','work-rewrite','us-east-1',$2,$3,$4)")
        .bind(storage).bind(codec::protect(b"access", key, format!("storage-{storage}-access").as_bytes())?)
        .bind(codec::protect(b"secret", key, format!("storage-{storage}-secret").as_bytes())?)
        .bind(crate::storage::model::identity_marker(node.deployment_id, storage)?).execute(pool).await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id,min_storage_duration,control_concurrency,maintenance_concurrency) VALUES($1,'0s',1,1)").bind(storage).execute(pool).await?;
    let block = Uuid::now_v7();
    ManagedKeyStore::create(
        backend,
        admin,
        ManagedKeyCreate {
            id: block,
            name: "work-rewrite-key".into(),
            purpose: "block".into(),
            algorithm: "aes-256-gcm".into(),
            protected: codec::protect(&[73; 32], key, format!("managed-key-{block}").as_bytes())?,
            fingerprint: *blake3::hash(&[73; 32]).as_bytes(),
        },
    )
    .await?;
    for _ in 0..67 {
        chunk(backend, storage, 2).await?;
    }
    let request = |operation, completion, max_items| work::Exchange {
        run,
        operation,
        completion,
        class: 2,
        max_items,
        cached: vec![],
    };
    let task = enqueue(backend, storage, block).await?;
    let operation = Uuid::now_v7();
    let first = items(
        backend
            .exchange_work(node, key, request(operation, None, 64))
            .await?,
    );
    assert_eq!(first.len(), 64);
    let replay = items(
        peer.exchange_work(node, key, request(operation, None, 64))
            .await?,
    );
    assert_eq!(serde_json::to_value(&first)?, serde_json::to_value(replay)?);
    let mut nonces = BTreeSet::new();
    for item in &first {
        let work::Rewrite::Chunk {
            location,
            source,
            target,
        } = item
        else {
            panic!("expected chunk");
        };
        assert_ne!(source.encoding_id, target.encoding_id);
        assert_eq!(target.block_key_id, Some(block));
        assert!(nonces.insert(target.nonce.clone().unwrap()));
        assert!(
            queries::work::physical_active(*location)
                .fetch_one(pool)
                .await?
        );
    }
    let mut invalid = completion(operation, &first);
    invalid.outcomes[0] = work::Outcome::Encoded {
        size: 1,
        compressed: false,
        digest: None,
    };
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), Some(invalid), 64))
            .await
            .is_err()
    );
    let receipt = Uuid::now_v7();
    let next = items(
        backend
            .exchange_work(
                node,
                key,
                request(receipt, Some(completion(operation, &first)), 64),
            )
            .await?,
    );
    assert_eq!(next.len(), 3);
    let replay = items(
        peer.exchange_work(
            node,
            key,
            request(receipt, Some(completion(operation, &first)), 64),
        )
        .await?,
    );
    assert_eq!(serde_json::to_value(&next)?, serde_json::to_value(replay)?);
    assert_eq!(result(backend, task).await?.1, 64);
    assert!(
        backend
            .exchange_work(
                node,
                key,
                request(Uuid::now_v7(), Some(completion(receipt, &next)), 64)
            )
            .await?
            .plan
            .is_none()
    );
    assert_eq!(result(backend, task).await?, ("completed".into(), 67, None));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pack_inputs WHERE task_id=$1")
            .bind(task)
            .fetch_one(pool)
            .await?,
        0
    );

    // A whole batch fails without publishing any of its successful uploads.
    for _ in 0..2 {
        chunk(backend, storage, 2).await?;
    }
    let task = enqueue(backend, storage, block).await?;
    let operation = Uuid::now_v7();
    let selected = items(
        backend
            .exchange_work(node, key, request(operation, None, 64))
            .await?,
    );
    assert_eq!(selected.len(), 2);
    let mut failed = completion(operation, &selected);
    failed.outcomes[1] = work::Outcome::Retry;
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), Some(failed), 64))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(
        result(backend, task).await?,
        ("failed".into(), 0, Some("backend_io_failed".into()))
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM chunk_encodings WHERE owner_task=$1 AND state='retired'"
        )
        .bind(task)
        .fetch_one(pool)
        .await?,
        2
    );
    sqlx::query("UPDATE tasks SET state='queued',error=NULL WHERE id=$1")
        .bind(task)
        .execute(pool)
        .await?;
    let operation = Uuid::now_v7();
    let resumed = items(
        backend
            .exchange_work(node, key, request(operation, None, 64))
            .await?,
    );
    for (old, new) in selected.iter().zip(&resumed) {
        let (work::Rewrite::Chunk { target: old, .. }, work::Rewrite::Chunk { target: new, .. }) =
            (old, new)
        else {
            unreachable!()
        };
        assert_ne!(old.encoding_id, new.encoding_id);
        assert_ne!(old.nonce, new.nonce);
    }
    backend
        .exchange_work(
            node,
            key,
            request(Uuid::now_v7(), Some(completion(operation, &resumed)), 64),
        )
        .await?;
    assert_eq!(result(backend, task).await?, ("completed".into(), 2, None));

    // Profile, key and physical-source changes cannot publish a previously issued batch.
    for changed in ["profile", "key", "source"] {
        let chunk = chunk(backend, storage, 2).await?;
        let task = enqueue(backend, storage, block).await?;
        let operation = Uuid::now_v7();
        let selected = items(
            backend
                .exchange_work(node, key, request(operation, None, 64))
                .await?,
        );
        let work::Rewrite::Chunk { source, target, .. } = &selected[0] else {
            unreachable!()
        };
        let (original, prepared) = (source.encoding_id, target.encoding_id);
        match changed {
            "profile" => {
                sqlx::query("UPDATE storage_profiles SET revision=revision+1 WHERE storage_id=$1")
                    .bind(storage)
                    .execute(pool)
                    .await?;
            }
            "key" => {
                sqlx::query("UPDATE managed_keys SET revoked_at=clock_timestamp() WHERE id=$1")
                    .bind(block)
                    .execute(pool)
                    .await?;
            }
            _ => {
                sqlx::query("UPDATE chunk_encodings SET quarantine_revision=quarantine_revision+1 WHERE encoding_id=$1").bind(original).execute(pool).await?;
            }
        }
        backend
            .exchange_work(
                node,
                key,
                request(Uuid::now_v7(), Some(completion(operation, &selected)), 64),
            )
            .await?;
        assert_eq!(result(backend, task).await?.0, "failed");
        assert_eq!(
            sqlx::query_scalar::<_, Id>("SELECT current_encoding_id FROM chunks WHERE id=$1")
                .bind(chunk)
                .fetch_one(pool)
                .await?,
            original
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM chunk_encodings WHERE encoding_id=$1"
            )
            .bind(prepared)
            .fetch_one(pool)
            .await?,
            "retired"
        );
        sqlx::query("UPDATE managed_keys SET revoked_at=NULL WHERE id=$1")
            .bind(block)
            .execute(pool)
            .await?;
        sqlx::query("UPDATE tasks SET state='queued',error=NULL,detail=jsonb_set(detail,'{profile_revision}',to_jsonb((SELECT revision FROM storage_profiles WHERE storage_id=$2))) WHERE id=$1").bind(task).bind(storage).execute(pool).await?;
        let operation = Uuid::now_v7();
        let selected = items(
            backend
                .exchange_work(node, key, request(operation, None, 64))
                .await?,
        );
        backend
            .exchange_work(
                node,
                key,
                request(Uuid::now_v7(), Some(completion(operation, &selected)), 64),
            )
            .await?;
        assert_eq!(result(backend, task).await?.0, "completed");
    }

    let mut ids = Vec::new();
    for _ in 0..5 {
        ids.push(chunk(backend, storage, 2).await?);
    }
    let mut old_packs = Vec::new();
    for group in [[ids[4], ids[0]], [ids[3], ids[1]]] {
        let id: Id = sqlx::query_scalar("INSERT INTO packs(storage_id,encryption_algorithm,raw_size,stored_size,member_count,state,digest) VALUES($1,'none',4,$2,2,'ready',$3) RETURNING id")
            .bind(storage).bind((4+12+2*crate::content::pack::MEMBER_SIZE) as i64).bind(vec![7u8;32]).fetch_one(pool).await?;
        for (ordinal, chunk) in group.into_iter().enumerate() {
            sqlx::query("INSERT INTO pack_members(pack_id,ordinal,chunk_id,offset_bytes) VALUES($1,$2,$3,$4)")
                .bind(id).bind(ordinal as i32).bind(chunk).bind(ordinal as i64*2).execute(pool).await?;
            sqlx::query("UPDATE chunks SET pack_id=$2 WHERE id=$1")
                .bind(chunk)
                .bind(id)
                .execute(pool)
                .await?;
        }
        old_packs.push(id);
    }
    let task = enqueue(backend, storage, block).await?;
    let mut previous = None;
    for (index, cursor) in [ids[0], ids[1], ids[2]].into_iter().enumerate() {
        let operation = Uuid::now_v7();
        let selected = items(
            backend
                .exchange_work(node, key, request(operation, previous, 64))
                .await?,
        );
        assert_eq!(selected.len(), 1);
        if index < 2 {
            let work::Rewrite::Pack {
                source,
                target,
                current,
                cursor: first,
                ..
            } = &selected[0]
            else {
                panic!("expected pack");
            };
            assert_eq!(source.id, old_packs[index]);
            assert_eq!(*first, cursor);
            assert_eq!(current.len(), 2);
            assert_eq!(
                target.nonce.as_ref().unwrap()[0] >> 3,
                crate::content::nonce::PACK
            );
        } else {
            assert!(
                matches!(&selected[0], work::Rewrite::Chunk { source, .. } if source.id == cursor)
            );
        }
        previous = Some(completion(operation, &selected));
    }
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), previous, 64))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(result(backend, task).await?, ("completed".into(), 3, None));

    sqlx::query("UPDATE storage_profiles SET maintenance_byte_budget='1MiB' WHERE storage_id=$1")
        .bind(storage)
        .execute(pool)
        .await?;
    for _ in 0..3 {
        chunk(backend, storage, 700_000).await?;
    }
    let task = enqueue(backend, storage, block).await?;
    let mut previous = None;
    for _ in 0..3 {
        let operation = Uuid::now_v7();
        let selected = items(
            backend
                .exchange_work(node, key, request(operation, previous, 64))
                .await?,
        );
        assert_eq!(selected.len(), 1);
        previous = Some(completion(operation, &selected));
    }
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), previous, 64))
            .await?
            .plan
            .is_none()
    );
    assert_eq!(result(backend, task).await?, ("completed".into(), 3, None));
    Ok(())
}
