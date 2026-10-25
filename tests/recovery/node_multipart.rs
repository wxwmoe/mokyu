use super::{Backend, migration, queries};
use crate::{
    authorization::Principal,
    content::codec::{self, Key},
    model::{
        self, RuntimeSettingsStore,
        cluster::ClusterStore,
        node_multipart::{Intent, Operation, Outcome, Request},
        node_request::{Catalog, Prepare, Query},
        session::Open,
    },
};
use anyhow::Result;
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

#[path = "node_completion.rs"]
mod completion;
#[path = "node_parts.rs"]
mod parts;

fn denied(error: anyhow::Error, code: s3s::S3ErrorCode) {
    assert_eq!(
        error
            .downcast_ref::<s3s::S3Error>()
            .map(|error| error.code()),
        Some(&code)
    );
}

async fn state(pool: &PgPool, upload: Uuid) -> Result<String> {
    Ok(sqlx::query_scalar("SELECT state FROM uploads WHERE id=$1")
        .bind(upload)
        .fetch_one(pool)
        .await?)
}

#[tokio::test]
#[ignore = "requires a new isolated PostgreSQL database"]
async fn postgres_node_multipart_replays_admission_and_cancellation() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(url.trim())
        .await?;
    assert!(queries::metadata_table().fetch_one(&pool).await?.is_none());
    migration::run(&mut *pool.acquire().await?).await?;
    sqlx::query("UPDATE mokyu_meta SET deployment_mode='cluster'")
        .execute(&pool)
        .await?;
    let backend = Backend::new(pool.clone(), pool.acquire().await?.detach(), false);
    let key = Key::new("aes-256-gcm", [29; 32])?;
    let config =
        toml::from_str::<crate::config::FileConfig>("[database]\ntype='postgres'\n")?.into_config();
    RuntimeSettingsStore::ensure(&backend, &model::runtime_defaults(&config)).await?;
    let control = backend
        .register(
            "control",
            "control",
            crate::cluster::identity::credential()?,
            &key,
        )
        .await?
        .unwrap();
    let admin = Principal::Node {
        id: control.node_id,
        role: "control".into(),
    };
    let node = backend.provision_node(&admin, "data", "data", &key).await?;
    let session = backend
        .open_session(
            &node,
            &key,
            Open {
                boot: Uuid::new_v4(),
                connection: Uuid::new_v4(),
                previous_epoch: 0,
            },
        )
        .await?;
    let storage = Uuid::now_v7();
    sqlx::query("INSERT INTO storages(id,name,type,endpoint_url,remote_bucket_name,region,access_key_encrypted,secret_key_encrypted,identity_marker) VALUES($1,'multipart-control','s3','http://backend:8333','multipart-control-fixture','us-east-1',$2,$3,$4)")
        .bind(storage).bind(codec::protect(b"source-access", &key, format!("storage-{storage}-access").as_bytes())?)
        .bind(codec::protect(b"source-secret", &key, format!("storage-{storage}-secret").as_bytes())?)
        .bind(crate::storage::model::identity_marker(node.deployment_id, storage)?).execute(&pool).await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id) VALUES($1)")
        .bind(storage)
        .execute(&pool)
        .await?;
    let bucket: Uuid = sqlx::query_scalar(
        "INSERT INTO buckets(name,storage_id) VALUES('multipart-control-a',$1) RETURNING id",
    )
    .bind(storage)
    .fetch_one(&pool)
    .await?;
    sqlx::query("INSERT INTO s3_credentials(access_key_id,secret_encrypted) VALUES('writer',$1)")
        .bind(codec::protect(b"writer-secret", &key, b"writer")?)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO s3_credential_grants(access_key,bucket_id,actions) VALUES('writer',$1,ARRAY['object.write','object.acl','bucket.list'])")
        .bind(bucket).execute(&pool).await?;
    let revision = queries::node_write_credential("writer")
        .fetch_one(&pool)
        .await?;
    let peer_pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(url.trim())
        .await?;
    let peer = Backend::new(
        peer_pool.clone(),
        peer_pool.acquire().await?.detach(),
        false,
    );

    parts::check(
        &backend,
        &peer,
        &node,
        &key,
        &session,
        (bucket, storage),
        revision,
    )
    .await?;

    completion::check(&backend, &peer, &node, &key, &session, bucket, revision).await?;

    let intent = Intent {
        access: "writer".into(),
        credential_revision: revision,
        bucket: "multipart-control-a".into(),
        key: "group/object".into(),
        operation: Operation::Create {
            metadata: serde_json::json!({"content_type":"text/plain"}),
            public: true,
            algorithm: Some("SHA256".into()),
            checksum_type: Some("COMPOSITE".into()),
        },
    };
    let request = |operation, intent: Intent| Request {
        run: session.run,
        operation,
        intent,
    };
    let id = Uuid::now_v7();
    let (a, b) = tokio::join!(
        backend.multipart_operation(&node, &key, request(id, intent.clone()), Some(1)),
        peer.multipart_operation(&node, &key, request(id, intent.clone()), Some(1))
    );
    let first = a?;
    assert_eq!(first, b?);
    let Outcome::Created { upload } = first else {
        panic!("missing upload")
    };
    assert_eq!(queries::active_upload_count().fetch_one(&pool).await?, 1);
    denied(
        backend
            .multipart_operation(
                &node,
                &key,
                request(Uuid::now_v7(), intent.clone()),
                Some(1),
            )
            .await
            .unwrap_err(),
        s3s::S3ErrorCode::SlowDown,
    );
    let mut changed = intent.clone();
    changed.key = "different".into();
    assert!(
        backend
            .multipart_operation(&node, &key, request(id, changed), None)
            .await
            .is_err()
    );
    for number in 1..=3 {
        let stream: Uuid=sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,etag,checksums) VALUES($1,'group/object','part','ready',0,'d41d8cd98f00b204e9800998ecf8427e','{\"sha256\":\"checksum\"}') RETURNING id").bind(bucket).fetch_one(&pool).await?;
        sqlx::query(
            "INSERT INTO parts(upload_id,part_number,stream_id,write_token) VALUES($1,$2,$3,$3)",
        )
        .bind(upload)
        .bind(number)
        .bind(stream)
        .execute(&pool)
        .await?;
    }
    let prepare = |query| Prepare {
        run: session.run,
        access: Some("writer".into()),
        route: Some(model::ReadRoute::Bucket {
            name: "multipart-control-a".into(),
            key: String::new(),
            alias: None,
        }),
        include: vec![
            model::node_read::MaterialType::Credential,
            model::node_read::MaterialType::Bucket,
        ],
        cached: vec![],
        query: Some(query),
    };
    let before: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT touched_at FROM uploads WHERE id=$1")
            .bind(upload)
            .fetch_one(&pool)
            .await?;
    let reply = backend
        .prepare_request(
            &node,
            &key,
            prepare(Query::Parts {
                key: intent.key.clone(),
                upload: upload.to_string(),
                marker: 1,
                limit: 1,
            }),
        )
        .await?;
    let Some(Catalog::Parts {
        rows,
        algorithm,
        checksum_type,
    }) = reply.catalog
    else {
        panic!("missing parts")
    };
    assert_eq!(rows.iter().map(|r| r.0).collect::<Vec<_>>(), [2, 3]);
    assert_eq!(algorithm.as_deref(), Some("SHA256"));
    assert_eq!(checksum_type.as_deref(), Some("COMPOSITE"));
    let reply = backend
        .prepare_request(
            &node,
            &key,
            prepare(Query::Uploads {
                prefix: String::new(),
                delimiter: "/".into(),
                key_marker: None,
                upload_marker: None,
                limit: 1,
            }),
        )
        .await?;
    let Some(Catalog::Uploads(list)) = reply.catalog else {
        panic!("missing listing")
    };
    assert_eq!(list.prefixes, ["group/"]);
    assert!(list.uploads.is_empty() && !list.truncated);
    assert_eq!(
        before,
        sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
            "SELECT touched_at FROM uploads WHERE id=$1"
        )
        .bind(upload)
        .fetch_one(&pool)
        .await?
    );
    let mut abort = intent.clone();
    abort.operation = Operation::Abort {
        upload,
        initiated: Some("Thu, 01 Jan 1970 00:00:00 GMT".into()),
    };
    denied(
        backend
            .multipart_operation(&node, &key, request(Uuid::now_v7(), abort.clone()), None)
            .await
            .unwrap_err(),
        s3s::S3ErrorCode::PreconditionFailed,
    );
    abort.operation = Operation::Abort {
        upload,
        initiated: None,
    };
    let mut wrong = abort.clone();
    wrong.key = "wrong".into();
    denied(
        backend
            .multipart_operation(&node, &key, request(Uuid::now_v7(), wrong), None)
            .await
            .unwrap_err(),
        s3s::S3ErrorCode::NoSuchUpload,
    );
    let cancel = Uuid::now_v7();
    assert_eq!(
        backend
            .multipart_operation(&node, &key, request(cancel, abort.clone()), None)
            .await?,
        Outcome::Aborted
    );
    assert_eq!(state(&pool, upload).await?, "aborted");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM parts WHERE upload_id=$1")
            .bind(upload)
            .fetch_one(&pool)
            .await?,
        0
    );
    let Outcome::Created {
        upload: replacement,
    } = backend
        .multipart_operation(
            &node,
            &key,
            request(Uuid::now_v7(), intent.clone()),
            Some(1),
        )
        .await?
    else {
        panic!("missing replacement")
    };
    assert_eq!(
        peer.multipart_operation(&node, &key, request(cancel, abort), None)
            .await?,
        Outcome::Aborted
    );
    assert_eq!(state(&pool, replacement).await?, "active");
    let missing = backend
        .prepare_request(
            &node,
            &key,
            prepare(Query::Parts {
                key: intent.key.clone(),
                upload: upload.to_string(),
                marker: 0,
                limit: 1000,
            }),
        )
        .await?;
    assert!(matches!(missing.catalog, Some(Catalog::NoSuchUpload)));
    sqlx::query("UPDATE buckets SET uploads_paused=true WHERE id=$1")
        .bind(bucket)
        .execute(&pool)
        .await?;
    denied(
        backend
            .multipart_operation(&node, &key, request(Uuid::now_v7(), intent.clone()), None)
            .await
            .unwrap_err(),
        s3s::S3ErrorCode::OperationAborted,
    );
    let mut abort = intent.clone();
    abort.operation = Operation::Abort {
        upload: replacement,
        initiated: None,
    };
    backend
        .multipart_operation(&node, &key, request(Uuid::now_v7(), abort), None)
        .await?;
    sqlx::query("UPDATE buckets SET uploads_paused=false WHERE id=$1")
        .bind(bucket)
        .execute(&pool)
        .await?;
    sqlx::query(
        "UPDATE s3_credential_grants SET actions=ARRAY['bucket.list'] WHERE access_key='writer'",
    )
    .execute(&pool)
    .await?;
    denied(
        backend
            .multipart_operation(&node, &key, request(Uuid::now_v7(), intent.clone()), None)
            .await
            .unwrap_err(),
        s3s::S3ErrorCode::AccessDenied,
    );
    let mut fenced = request(Uuid::now_v7(), intent);
    fenced.run.fence = Uuid::new_v4();
    assert!(
        backend
            .multipart_operation(&node, &key, fenced, None)
            .await
            .is_err()
    );
    assert_eq!(queries::active_upload_count().fetch_one(&pool).await?, 0);
    Ok(())
}
