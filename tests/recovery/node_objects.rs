use super::{Backend, migration, queries};
use crate::{
    authorization::Principal,
    content::codec::{self, Key},
    model::{
        self, RuntimeSettingsStore,
        cluster::ClusterStore,
        node_object::{Change, Outcome, Request},
        session::{Open, Run},
    },
};
use anyhow::Result;
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

async fn seed(pool: &PgPool, bucket: Uuid, key: &str) -> Result<Uuid> {
    let stream = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,etag) VALUES($1,$2,'object','ready',0,'d41d8cd98f00b204e9800998ecf8427e') RETURNING id")
        .bind(bucket).bind(key).fetch_one(pool).await?;
    sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,$2,$3,uuidv7()) ON CONFLICT(bucket_id,object_key) DO UPDATE SET stream_id=$3,write_token=uuidv7()")
        .bind(bucket).bind(key).bind(stream).execute(pool).await?;
    Ok(stream)
}

fn request(
    run: Run,
    operation: Uuid,
    keys: &[&str],
    public: Option<bool>,
    revision: i64,
) -> Request {
    Request {
        run,
        operation,
        change: Change {
            access: "writer".into(),
            credential_revision: revision,
            bucket: "object-change-a".into(),
            keys: keys.iter().map(|key| (*key).to_owned()).collect(),
            public,
            if_match: None,
        },
    }
}

#[tokio::test]
#[ignore = "requires a new isolated PostgreSQL database"]
async fn postgres_node_objects_replay_without_mutating_replacements() -> Result<()> {
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
    sqlx::query("INSERT INTO storages(id,name,type,endpoint_url,remote_bucket_name,region,access_key_encrypted,secret_key_encrypted,identity_marker) VALUES($1,'object-change','s3','http://backend:8333','object-change-fixture','us-east-1',$2,$3,$4)")
        .bind(storage).bind(codec::protect(b"source-access", &key, format!("storage-{storage}-access").as_bytes())?)
        .bind(codec::protect(b"source-secret", &key, format!("storage-{storage}-secret").as_bytes())?)
        .bind(crate::storage::model::identity_marker(node.deployment_id, storage)?).execute(&pool).await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id) VALUES($1)")
        .bind(storage)
        .execute(&pool)
        .await?;
    let bucket: Uuid = sqlx::query_scalar(
        "INSERT INTO buckets(name,storage_id) VALUES('object-change-a',$1) RETURNING id",
    )
    .bind(storage)
    .fetch_one(&pool)
    .await?;
    sqlx::query("INSERT INTO s3_credentials(access_key_id,secret_encrypted) VALUES('writer',$1)")
        .bind(codec::protect(b"writer-secret", &key, b"writer")?)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO s3_credential_grants(access_key,bucket_id,actions) VALUES('writer',$1,ARRAY['object.delete','object.acl','object.read'])")
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
    let conditional_operation = Uuid::now_v7();
    let conditional = |operation, condition: &str| {
        let mut input = request(session.run, operation, &["conditional"], None, revision);
        input.change.if_match = Some(condition.into());
        input
    };
    assert_eq!(
        backend
            .change_objects(&node, &key, conditional(conditional_operation, "*"))
            .await?,
        Outcome::PreconditionFailed
    );
    let conditional_stream = seed(&pool, bucket, "conditional").await?;
    assert_eq!(
        peer.change_objects(&node, &key, conditional(conditional_operation, "*"))
            .await?,
        Outcome::PreconditionFailed
    );
    let current: Uuid = sqlx::query_scalar(
        "SELECT stream_id FROM objects WHERE bucket_id=$1 AND object_key='conditional'",
    )
    .bind(bucket)
    .fetch_one(&pool)
    .await?;
    assert_eq!(current, conditional_stream);
    assert!(
        backend
            .change_objects(
                &node,
                &key,
                conditional(conditional_operation, "\"different\"")
            )
            .await
            .is_err()
    );
    assert_eq!(
        backend
            .change_objects(&node, &key, conditional(Uuid::now_v7(), "\"different\""))
            .await?,
        Outcome::PreconditionFailed
    );
    let success = Uuid::now_v7();
    assert_eq!(
        backend
            .change_objects(
                &node,
                &key,
                conditional(success, "\"d41d8cd98f00b204e9800998ecf8427e\"")
            )
            .await?,
        Outcome::Applied
    );
    let conditional_replacement = seed(&pool, bucket, "conditional").await?;
    assert_eq!(
        peer.change_objects(
            &node,
            &key,
            conditional(success, "\"d41d8cd98f00b204e9800998ecf8427e\"")
        )
        .await?,
        Outcome::Applied
    );
    let current: Uuid = sqlx::query_scalar(
        "SELECT stream_id FROM objects WHERE bucket_id=$1 AND object_key='conditional'",
    )
    .bind(bucket)
    .fetch_one(&pool)
    .await?;
    assert_eq!(current, conditional_replacement);

    let original = seed(&pool, bucket, "object").await?;
    let operation = Uuid::now_v7();
    let (first, second) = tokio::join!(
        backend.change_objects(
            &node,
            &key,
            request(
                session.run,
                operation,
                &["object", "missing", "object"],
                None,
                revision
            )
        ),
        peer.change_objects(
            &node,
            &key,
            request(
                session.run,
                operation,
                &["object", "missing", "object"],
                None,
                revision
            )
        ),
    );
    assert_eq!(first?, Outcome::Applied);
    assert_eq!(second?, Outcome::Applied);
    let state: String = sqlx::query_scalar("SELECT state FROM streams WHERE id=$1")
        .bind(original)
        .fetch_one(&pool)
        .await?;
    assert_eq!(state, "retired");
    let replacement = seed(&pool, bucket, "object").await?;
    assert_eq!(
        peer.change_objects(
            &node,
            &key,
            request(
                session.run,
                operation,
                &["object", "missing", "object"],
                None,
                revision
            )
        )
        .await?,
        Outcome::Applied
    );
    let current: Uuid = sqlx::query_scalar(
        "SELECT stream_id FROM objects WHERE bucket_id=$1 AND object_key='object'",
    )
    .bind(bucket)
    .fetch_one(&pool)
    .await?;
    assert_eq!(current, replacement);
    let conflict = backend
        .change_objects(
            &node,
            &key,
            request(session.run, operation, &["object"], Some(true), revision),
        )
        .await
        .unwrap_err();
    assert_eq!(
        model::error(&conflict),
        Some(model::Error::NodeOperationConflict)
    );

    let acl = Uuid::now_v7();
    assert_eq!(
        backend
            .change_objects(
                &node,
                &key,
                request(session.run, acl, &["object"], Some(true), revision)
            )
            .await?,
        Outcome::Applied
    );
    let current = seed(&pool, bucket, "object").await?;
    assert_eq!(
        peer.change_objects(
            &node,
            &key,
            request(session.run, acl, &["object"], Some(true), revision)
        )
        .await?,
        Outcome::Applied
    );
    let public: bool = sqlx::query_scalar("SELECT public_read FROM streams WHERE id=$1")
        .bind(current)
        .fetch_one(&pool)
        .await?;
    assert!(!public, "replayed ACL changed a replacement");
    let missing_acl = Uuid::now_v7();
    assert_eq!(
        backend
            .change_objects(
                &node,
                &key,
                request(session.run, missing_acl, &["absent"], Some(true), revision)
            )
            .await?,
        Outcome::NoSuchKey
    );
    let inserted = seed(&pool, bucket, "absent").await?;
    assert_eq!(
        peer.change_objects(
            &node,
            &key,
            request(session.run, missing_acl, &["absent"], Some(true), revision)
        )
        .await?,
        Outcome::NoSuchKey
    );
    let public: bool = sqlx::query_scalar("SELECT public_read FROM streams WHERE id=$1")
        .bind(inserted)
        .fetch_one(&pool)
        .await?;
    assert!(!public);

    let prepare = |object_key: &str| model::node_request::Prepare {
        run: session.run,
        access: Some("writer".into()),
        route: Some(model::ReadRoute::Bucket {
            name: "object-change-a".into(),
            key: String::new(),
            alias: None,
        }),
        include: vec![
            model::node_read::MaterialType::Credential,
            model::node_read::MaterialType::Bucket,
        ],
        cached: vec![],
        query: Some(model::node_request::Query::Acl {
            key: object_key.into(),
        }),
    };
    let reply = backend
        .prepare_request(&node, &key, prepare("object"))
        .await?;
    assert!(matches!(
        reply.catalog,
        Some(model::node_request::Catalog::Acl {
            public: Some(false)
        })
    ));
    assert!(reply.context.grants.is_empty() && reply.context.mapping.is_none());
    assert!(matches!(
        backend
            .prepare_request(&node, &key, prepare("unknown"))
            .await?
            .catalog,
        Some(model::node_request::Catalog::Acl { public: None })
    ));

    sqlx::query(
        "UPDATE s3_credential_grants SET actions=ARRAY['bucket.list'] WHERE access_key='writer'",
    )
    .execute(&pool)
    .await?;
    let denied = backend
        .prepare_request(&node, &key, prepare("object"))
        .await?;
    assert!(
        denied.context.outcome == model::node_read::Outcome::AccessDenied
            && denied.catalog.is_none()
    );
    assert!(
        backend
            .change_objects(
                &node,
                &key,
                request(session.run, Uuid::now_v7(), &["object"], None, revision)
            )
            .await
            .is_err()
    );
    sqlx::query("UPDATE s3_credential_grants SET actions=ARRAY['object.delete','object.acl','object.read'] WHERE access_key='writer'").execute(&pool).await?;
    let updated = queries::node_write_credential("writer")
        .fetch_one(&pool)
        .await?;
    assert!(updated > revision);
    assert!(
        backend
            .change_objects(
                &node,
                &key,
                request(session.run, Uuid::now_v7(), &["object"], None, revision)
            )
            .await
            .is_err()
    );
    let revision = updated;

    let keys: Vec<_> = (0..1000).map(|index| format!("batch/{index:04}")).collect();
    for object_key in &keys {
        seed(&pool, bucket, object_key).await?;
    }
    let borrowed: Vec<_> = keys.iter().map(String::as_str).collect();
    assert_eq!(
        backend
            .change_objects(
                &node,
                &key,
                request(session.run, Uuid::now_v7(), &borrowed, None, revision)
            )
            .await?,
        Outcome::Applied
    );
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM objects WHERE bucket_id=$1 AND starts_with(object_key,'batch/') AND stream_id IS NOT NULL").bind(bucket).fetch_one(&pool).await?;
    assert_eq!(remaining, 0);
    let peer_node = backend
        .provision_node(&admin, "peer-data", "data", &key)
        .await?;
    let peer_session = backend
        .open_session(
            &peer_node,
            &key,
            Open {
                boot: Uuid::new_v4(),
                connection: Uuid::new_v4(),
                previous_epoch: 0,
            },
        )
        .await?;
    for object_key in ["overlap/a", "overlap/b"] {
        seed(&pool, bucket, object_key).await?;
    }
    let (left, right) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(
            backend.change_objects(
                &node,
                &key,
                request(
                    session.run,
                    Uuid::now_v7(),
                    &["overlap/a", "overlap/b"],
                    None,
                    revision
                )
            ),
            peer.change_objects(
                &peer_node,
                &key,
                request(
                    peer_session.run,
                    Uuid::now_v7(),
                    &["overlap/b", "overlap/a"],
                    None,
                    revision
                )
            ),
        )
    })
    .await?;
    assert_eq!(left?, Outcome::Applied);
    assert_eq!(right?, Outcome::Applied);
    let receipts: i64 = sqlx::query_scalar("SELECT count(*) FROM node_object_changes")
        .fetch_one(&pool)
        .await?;
    let mut too_many = request(session.run, Uuid::now_v7(), &borrowed, None, revision);
    too_many.change.keys.push("object".into());
    assert!(backend.change_objects(&node, &key, too_many).await.is_err());

    sqlx::query("UPDATE mokyu_meta SET maintenance=true")
        .execute(&pool)
        .await?;
    assert!(
        backend
            .change_objects(
                &node,
                &key,
                request(session.run, Uuid::now_v7(), &["object"], None, revision)
            )
            .await
            .is_err()
    );
    sqlx::query("UPDATE mokyu_meta SET maintenance=false")
        .execute(&pool)
        .await?;
    let mut stale = request(session.run, Uuid::now_v7(), &["object"], None, revision);
    stale.run.fence = Uuid::new_v4();
    assert!(backend.change_objects(&node, &key, stale).await.is_err());
    let worker = backend
        .provision_node(&admin, "worker", "worker", &key)
        .await?;
    assert!(
        backend
            .change_objects(
                &worker,
                &key,
                request(session.run, Uuid::now_v7(), &["object"], None, revision)
            )
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM node_object_changes")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, receipts, "rejected requests created receipts");

    sqlx::raw_sql("CREATE FUNCTION object_change_pause() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.object_key='concurrent-insert' AND NEW.stream_id IS NULL THEN PERFORM pg_advisory_xact_lock(783421); END IF; RETURN NEW; END $$; CREATE TRIGGER object_change_pause BEFORE INSERT ON objects FOR EACH ROW EXECUTE FUNCTION object_change_pause()")
        .execute(&pool).await?;
    let mut blocker = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(783421)")
        .execute(&mut *blocker)
        .await?;
    let deletion = peer.change_objects(
        &node,
        &key,
        request(
            session.run,
            Uuid::now_v7(),
            &["concurrent-insert"],
            None,
            revision,
        ),
    );
    tokio::pin!(deletion);
    tokio::select! {
        result = &mut deletion => panic!("delete did not wait at insertion: {result:?}"),
        result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory')").fetch_one(&pool).await?;
                if blocked { return Ok::<_, anyhow::Error>(()); }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }) => { result??; },
    }
    let concurrent = seed(&pool, bucket, "concurrent-insert").await?;
    blocker.commit().await?;
    assert_eq!(deletion.await?, Outcome::Applied);
    let state: String = sqlx::query_scalar("SELECT state FROM streams WHERE id=$1")
        .bind(concurrent)
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        state, "retired",
        "delete leaked a concurrently inserted stream"
    );
    sqlx::raw_sql(
        "DROP TRIGGER object_change_pause ON objects; DROP FUNCTION object_change_pause()",
    )
    .execute(&pool)
    .await?;

    let mut bytes = *operation.as_bytes();
    bytes[..6].copy_from_slice(
        &((chrono::Utc::now().timestamp_millis() - 2 * 86400000) as u64).to_be_bytes()[2..],
    );
    let expired = Uuid::from_bytes(bytes);
    sqlx::query("UPDATE node_object_changes SET operation_id=$2,created_at=clock_timestamp()-interval '2 days' WHERE operation_id=$1").bind(operation).bind(expired).execute(&pool).await?;
    backend
        .change_objects(
            &node,
            &key,
            request(session.run, Uuid::now_v7(), &[], None, revision),
        )
        .await?;
    let error = backend
        .change_objects(
            &node,
            &key,
            request(
                session.run,
                expired,
                &["object", "missing", "object"],
                None,
                revision,
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(
        model::error(&error),
        Some(model::Error::NodeOperationExpired)
    );
    let remaining: Uuid = sqlx::query_scalar(
        "SELECT stream_id FROM objects WHERE bucket_id=$1 AND object_key='object'",
    )
    .bind(bucket)
    .fetch_one(&pool)
    .await?;
    assert_eq!(remaining, current);
    Ok(())
}
