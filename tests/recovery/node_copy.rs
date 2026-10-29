use super::*;
use crate::model::node_read as read;

fn source(run: Run, key: &str) -> read::Query {
    read::Query {
        run,
        operation: Uuid::new_v4(),
        route: model::ReadRoute::Bucket {
            name: "batches".into(),
            key: key.into(),
            alias: None,
        },
        access: Some("writer".into()),
        head: false,
        web: false,
        mapping: false,
        protection: true,
        policy: false,
        headers: Default::default(),
        overrides: Default::default(),
        cached: Vec::new(),
        max_items: read::MAX_ITEMS,
        max_bytes: read::TARGET_BYTES,
        body: None,
    }
}

pub(super) async fn exercise(
    backend: &Backend,
    pool: &sqlx::PgPool,
    node: &crate::cluster::NodeState,
    key: &Key,
    run: Run,
) -> Result<()> {
    sqlx::query("UPDATE s3_credential_grants SET actions=ARRAY['object.read','object.write'] WHERE access_key='writer'")
        .execute(pool).await?;
    let revision: i64 = sqlx::query_scalar(
        "SELECT authorization_revision FROM s3_credentials WHERE access_key_id='writer'",
    )
    .fetch_one(pool)
    .await?;
    let reply = backend.open_read(node, key, source(run, "result")).await?;
    assert!(
        reply.outcome == read::Outcome::Ready && reply.mapping.is_none() && reply.next.is_none()
    );
    assert_eq!(reply.grants.len(), 1);
    assert!(reply.grants[0].r#type == read::GrantType::Read);
    assert!(reply.materials.iter().all(|value| !matches!(
        value.version.r#type,
        read::MaterialType::Storage | read::MaterialType::Key
    )));
    let lease = reply.grants[0].id;
    let object: read::Object = serde_json::from_slice(
        reply
            .materials
            .iter()
            .find(|value| value.version.r#type == read::MaterialType::Object)
            .unwrap()
            .json
            .as_ref()
            .unwrap(),
    )?;
    let intent = |name: &str| {
        let mut input = begin(run, name);
        input.intent.credential_revision = revision;
        input.intent.copy_read = Some(lease);
        input.intent.size = Some(object.size);
        input
    };
    let mut invalid = intent("invalid-copy");
    invalid.intent.copy_read = Some(Uuid::new_v4());
    assert!(backend.begin_write(node, key, invalid).await.is_err());
    let input = intent("copied");
    let operation = input.operation;
    let opened = backend.begin_write(node, key, input).await?;
    assert!(opened.copy_references);
    let batch = || write::CopyBatch {
        run,
        operation,
        batch: 1,
        offset: 0,
    };
    let (first, retry) = tokio::try_join!(
        backend.copy_write_references(node, key, batch()),
        backend.copy_write_references(node, key, batch())
    )?;
    assert_eq!((first, retry), (object.size, object.size));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM extents WHERE stream_id=$1")
        .bind(opened.stream.unwrap())
        .fetch_one(pool)
        .await?;
    assert_eq!(count, 3);
    let mut changed = batch();
    changed.offset = 1;
    assert_eq!(
        model::error(
            &backend
                .copy_write_references(node, key, changed)
                .await
                .err()
                .unwrap()
        ),
        Some(model::Error::NodeOperationConflict)
    );
    let publication = || write::Publish {
        run,
        operation,
        size: object.size,
        etag: object.etag.clone(),
        checksums: serde_json::from_value(object.checksums.clone()).unwrap(),
        if_match: None,
        if_none: None,
    };
    let mut changed = publication();
    changed.etag = "00000000000000000000000000000000".into();
    assert!(backend.publish_write(node, key, changed).await.is_err());
    sqlx::query("UPDATE streams SET state='retired' WHERE id=$1")
        .bind(object.id)
        .execute(pool)
        .await?;
    sqlx::query("UPDATE objects SET stream_id=NULL WHERE stream_id=$1")
        .bind(object.id)
        .execute(pool)
        .await?;
    assert_eq!(
        backend.publish_write(node, key, publication()).await?.phase,
        write::Phase::Published
    );
    let retired = intent("copy-retired-version");
    let retired_operation = retired.operation;
    assert!(
        backend
            .begin_write(node, key, retired)
            .await?
            .copy_references
    );
    assert_eq!(
        backend
            .copy_write_references(
                node,
                key,
                write::CopyBatch {
                    operation: retired_operation,
                    ..batch()
                }
            )
            .await?,
        object.size
    );
    sqlx::query(
        "UPDATE stream_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(lease)
    .execute(pool)
    .await?;
    assert!(
        backend
            .publish_write(
                node,
                key,
                write::Publish {
                    operation: retired_operation,
                    ..publication()
                }
            )
            .await
            .is_err()
    );
    assert_eq!(
        backend.publish_write(node, key, publication()).await?.phase,
        write::Phase::Published
    );
    let mut fenced = batch();
    fenced.run.fence = Uuid::new_v4();
    assert!(
        backend
            .copy_write_references(node, key, fenced)
            .await
            .is_err()
    );
    let empty = backend.open_read(node, key, source(run, "empty")).await?;
    assert!(empty.outcome == read::Outcome::Ready && empty.mapping.is_none());
    assert_eq!(empty.grants.len(), 1);
    let mut input = begin(run, "empty-copy");
    input.intent.credential_revision = revision;
    input.intent.size = Some(0);
    input.intent.copy_read = Some(empty.grants[0].id);
    let empty_operation = input.operation;
    backend.begin_write(node, key, input).await?;
    assert_eq!(
        backend
            .publish_write(
                node,
                key,
                write::Publish {
                    operation: empty_operation,
                    size: 0,
                    ..publication()
                }
            )
            .await?
            .phase,
        write::Phase::Published
    );
    let many: Uuid = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,etag) SELECT bucket_id,'many-copy','object','ready',1503,etag FROM streams WHERE id=$1 RETURNING id")
        .bind(object.id).fetch_one(pool).await?;
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) SELECT $1,n*3,3,e.chunk_id,0 FROM generate_series(0,500) n CROSS JOIN (SELECT chunk_id FROM extents WHERE stream_id=$2 ORDER BY offset_bytes LIMIT 1) e")
        .bind(many).bind(object.id).execute(pool).await?;
    sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) SELECT bucket_id,object_key,id,uuidv7() FROM streams WHERE id=$1")
        .bind(many).execute(pool).await?;
    let protected = backend
        .open_read(node, key, source(run, "many-copy"))
        .await?;
    let mut input = begin(run, "many-copy-result");
    input.intent.credential_revision = revision;
    input.intent.copy_read = Some(protected.grants[0].id);
    input.intent.size = Some(1503);
    let many_operation = input.operation;
    let destination = backend.begin_write(node, key, input).await?.stream.unwrap();
    let part = |batch, offset| write::CopyBatch {
        run,
        operation: many_operation,
        batch,
        offset,
    };
    assert_eq!(
        backend.copy_write_references(node, key, part(1, 0)).await?,
        1500
    );
    assert_eq!(
        backend
            .copy_write_references(node, key, part(2, 1500))
            .await?,
        1503
    );
    assert_eq!(
        backend.copy_write_references(node, key, part(1, 0)).await?,
        1500
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM extents WHERE stream_id=$1")
        .bind(destination)
        .fetch_one(pool)
        .await?;
    assert_eq!(count, 501);
    assert_eq!(
        backend
            .publish_write(
                node,
                key,
                write::Publish {
                    operation: many_operation,
                    size: 1503,
                    ..publication()
                }
            )
            .await?
            .phase,
        write::Phase::Published
    );
    let mut input = begin(run, "copy-revoked");
    input.intent.credential_revision = revision;
    input.intent.copy_read = Some(protected.grants[0].id);
    input.intent.size = Some(1503);
    let revoked = input.operation;
    backend.begin_write(node, key, input).await?;
    backend
        .copy_write_references(
            node,
            key,
            write::CopyBatch {
                operation: revoked,
                ..part(1, 0)
            },
        )
        .await?;
    backend
        .copy_write_references(
            node,
            key,
            write::CopyBatch {
                operation: revoked,
                ..part(2, 1500)
            },
        )
        .await?;
    sqlx::query(
        "UPDATE s3_credential_grants SET actions=ARRAY['object.write'] WHERE access_key='writer'",
    )
    .execute(pool)
    .await?;
    assert!(
        backend
            .publish_write(
                node,
                key,
                write::Publish {
                    operation: revoked,
                    size: 1503,
                    ..publication()
                }
            )
            .await
            .is_err()
    );
    sqlx::query("UPDATE s3_credential_grants SET actions=ARRAY['object.read','object.write'] WHERE access_key='writer'").execute(pool).await?;
    assert!(
        backend
            .publish_write(
                node,
                key,
                write::Publish {
                    operation: revoked,
                    size: 1503,
                    ..publication()
                }
            )
            .await
            .is_err(),
        "restoring permissions revived a stale authorization"
    );
    Ok(())
}
