use super::*;
use crate::{
    cluster::NodeState,
    model::{Id, session::Run},
};

async fn seed(backend: &Backend, storage: Uuid, bucket: Uuid) -> Result<(Id, Id)> {
    let source = Id::from(Uuid::now_v7());
    let name = Uuid::now_v7().to_string();
    sqlx::query("INSERT INTO raw_sources(id,storage_id,medium,state,size,digest,profile_revision) VALUES($1,$2,'shared_s3','ready',2,$3,1)")
        .bind(source).bind(storage).bind(vec![2u8;16]).execute(&backend.pool).await?;
    let stream: Id=sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,payload_type) VALUES($1,$2,'object','ready',2,'raw') RETURNING id")
        .bind(bucket).bind(&name).fetch_one(&backend.pool).await?;
    sqlx::query("INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_offset,source_id) VALUES($1,0,2,0,$2)").bind(stream).bind(source).execute(&backend.pool).await?;
    sqlx::query(
        "INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,$2,$3,uuidv7())",
    )
    .bind(bucket)
    .bind(name)
    .bind(stream)
    .execute(&backend.pool)
    .await?;
    sqlx::query("INSERT INTO raw_processing(stream_id,source_id,payload_revision) SELECT id,$2,payload_revision FROM streams WHERE id=$1")
        .bind(stream).bind(source).execute(&backend.pool).await?;
    Ok((stream, source))
}

fn items(reply: work::Reply) -> Vec<work::RawHash> {
    let work::Plan::HashRaw { items, .. } = reply.plan.expect("raw plan") else {
        panic!("expected hashing")
    };
    items
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
    sqlx::query("INSERT INTO storages(id,name,type,endpoint_url,remote_bucket_name,region,access_key_encrypted,secret_key_encrypted,identity_marker) VALUES($1,'raw-processing','s3','http://backend:8333','raw-processing','us-east-1',$2,$3,$4)")
        .bind(storage).bind(codec::protect(b"access",key,format!("storage-{storage}-access").as_bytes())?)
        .bind(codec::protect(b"secret",key,format!("storage-{storage}-secret").as_bytes())?)
        .bind(crate::storage::model::identity_marker(node.deployment_id,storage)?).execute(pool).await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id,control_concurrency,maintenance_concurrency) VALUES($1,1,1)")
        .bind(storage).execute(pool).await?;
    let bucket: Uuid = sqlx::query_scalar(
        "INSERT INTO buckets(name,storage_id) VALUES('raw-processing',$1) RETURNING id",
    )
    .bind(storage)
    .fetch_one(pool)
    .await?;
    let request = |operation, completion| work::Exchange {
        run,
        operation,
        completion,
        class: 2,
        max_items: 64,
        cached: vec![],
    };
    let finish = |operation, count, hash: u8| work::Completion {
        operation,
        outcomes: vec![
            work::Outcome::Hashed {
                hash: vec![hash; 32]
            };
            count
        ],
    };
    let mut sources = Vec::new();
    for _ in 0..70 {
        sources.push(seed(backend, storage, bucket).await?);
    }
    let pinned = sources[0].0;
    let lease: Uuid=sqlx::query_scalar("INSERT INTO stream_leases(stream_id,lease_type,expires_at) VALUES($1,'read',clock_timestamp()+interval '2 minutes') RETURNING id").bind(pinned).fetch_one(pool).await?;
    let first = Uuid::now_v7();
    let batch = items(
        backend
            .exchange_work(node, key, request(first, None))
            .await?,
    );
    assert_eq!(batch.len(), 64);
    assert!(batch.iter().all(|i| i.hash.is_none()));
    assert_eq!(
        serde_json::to_value(&batch)?,
        serde_json::to_value(items(
            peer.exchange_work(node, key, request(first, None)).await?
        ))?
    );
    let mut invalid = finish(first, 64, 1);
    invalid.outcomes[0] = work::Outcome::Hashed { hash: vec![1; 31] };
    assert!(
        backend
            .exchange_work(node, key, request(Uuid::now_v7(), Some(invalid)))
            .await
            .is_err()
    );
    let second = Uuid::now_v7();
    let completion = finish(first, 64, 1);
    let batch2 = items(
        peer.exchange_work(node, key, request(second, Some(completion.clone())))
            .await?,
    );
    assert_eq!(batch2.len(), 6);
    assert_eq!(
        serde_json::to_value(&batch2)?,
        serde_json::to_value(items(
            backend
                .exchange_work(node, key, request(second, Some(completion)))
                .await?
        ))?
    );
    let mut submit = request(Uuid::now_v7(), Some(finish(second, 6, 1)));
    submit.class = 0;
    assert!(
        backend
            .exchange_work(node, key, submit)
            .await?
            .plan
            .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raw_processing WHERE stream_id=ANY($1)")
            .bind(sources.iter().map(|v| v.0).collect::<Vec<_>>())
            .fetch_one(pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT content_hash FROM raw_processing WHERE stream_id=$1"
        )
        .bind(pinned)
        .fetch_one(pool)
        .await?,
        vec![1; 32]
    );
    sqlx::query("DELETE FROM stream_leases WHERE id=$1")
        .bind(lease)
        .execute(pool)
        .await?;
    sqlx::query("UPDATE raw_processing SET next_attempt_at=clock_timestamp() WHERE stream_id=$1")
        .bind(pinned)
        .execute(pool)
        .await?;
    let cached = Uuid::now_v7();
    let batch = items(
        backend
            .exchange_work(node, key, request(cached, None))
            .await?,
    );
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].hash, Some(vec![1; 32]));
    assert!(
        backend
            .exchange_work(
                node,
                key,
                request(Uuid::now_v7(), Some(finish(cached, 1, 2)))
            )
            .await
            .is_err()
    );
    assert!(
        backend
            .exchange_work(
                node,
                key,
                request(Uuid::now_v7(), Some(finish(cached, 1, 1)))
            )
            .await?
            .plan
            .is_none()
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(DISTINCT r.source_id) FROM raw_ranges r JOIN streams s ON s.id=r.stream_id WHERE s.bucket_id=$1").bind(bucket).fetch_one(pool).await?,1);

    // A replaced object and changed source cannot publish a stale result.
    let (removed, _) = seed(backend, storage, bucket).await?;
    let (changed, source) = seed(backend, storage, bucket).await?;
    let operation = Uuid::now_v7();
    assert_eq!(
        items(
            backend
                .exchange_work(node, key, request(operation, None))
                .await?
        )
        .len(),
        2
    );
    sqlx::query("DELETE FROM objects WHERE stream_id=$1")
        .bind(removed)
        .execute(pool)
        .await?;
    sqlx::query("UPDATE raw_sources SET revision=revision+1 WHERE id=$1")
        .bind(source)
        .execute(pool)
        .await?;
    assert!(
        peer.exchange_work(
            node,
            key,
            request(Uuid::now_v7(), Some(finish(operation, 2, 3)))
        )
        .await?
        .plan
        .is_none()
    );
    assert!(
        sqlx::query_scalar::<_, Option<Vec<u8>>>(
            "SELECT content_hash FROM raw_processing WHERE stream_id=$1"
        )
        .bind(changed)
        .fetch_one(pool)
        .await?
        .is_none()
    );
    sqlx::query("UPDATE tasks SET updated_at=clock_timestamp()-interval '1 minute' WHERE id=(SELECT token FROM raw_processing WHERE stream_id=$1)").bind(changed).execute(pool).await?;
    assert_eq!(
        sqlx::query_as::<_, (i32, String)>(
            "SELECT attempts,last_error FROM raw_processing WHERE stream_id=$1"
        )
        .bind(changed)
        .fetch_one(pool)
        .await?,
        (1, "source_changed".into())
    );
    sqlx::query("UPDATE raw_processing SET next_attempt_at=clock_timestamp() WHERE stream_id=$1")
        .bind(changed)
        .execute(pool)
        .await?;
    let retry = Uuid::now_v7();
    assert!(
        items(
            backend
                .exchange_work(node, key, request(retry, None))
                .await?
        )[0]
        .hash
        .is_none()
    );
    assert!(
        backend
            .exchange_work(
                node,
                key,
                request(Uuid::now_v7(), Some(finish(retry, 1, 3)))
            )
            .await?
            .plan
            .is_none()
    );

    // Expired work is reclaimed with a different grant; the old result stays fenced.
    let (stream, _) = seed(backend, storage, bucket).await?;
    let expired = Uuid::now_v7();
    assert_eq!(
        items(
            backend
                .exchange_work(node, key, request(expired, None))
                .await?
        )
        .len(),
        1
    );
    sqlx::query("UPDATE node_work SET expires_at=clock_timestamp()-interval '1 second' WHERE node_id=$1 AND operation_id=$2").bind(node.node_id).bind(expired).execute(pool).await?;
    sqlx::query("UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=(SELECT token FROM raw_processing WHERE stream_id=$1)").bind(stream).execute(pool).await?;
    sqlx::query("UPDATE storage_permits SET expires_at=clock_timestamp()-interval '1 second' WHERE storage_id=$1").bind(storage).execute(pool).await?;
    assert!(
        backend
            .exchange_work(
                node,
                key,
                request(Uuid::now_v7(), Some(finish(expired, 1, 4)))
            )
            .await
            .is_err()
    );
    let resumed = Uuid::now_v7();
    assert_eq!(
        items(
            peer.exchange_work(node, key, request(resumed, None))
                .await?
        )
        .len(),
        1
    );
    assert!(
        peer.exchange_work(
            node,
            key,
            request(Uuid::now_v7(), Some(finish(resumed, 1, 4)))
        )
        .await?
        .plan
        .is_none()
    );
    Ok(())
}
