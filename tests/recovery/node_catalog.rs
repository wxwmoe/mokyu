use super::{Backend, migration, queries};
use crate::{
    authorization::Principal,
    content::codec::{self, Key},
    model::{
        self, RuntimeSettingsStore,
        cluster::ClusterStore,
        node_read as read,
        node_request::{self as request, Catalog, Query},
        session::Open,
    },
};
use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a new isolated PostgreSQL database"]
async fn postgres_node_catalog_binds_permissions_and_pagination() -> Result<()> {
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
    sqlx::query("INSERT INTO storages(id,name,type,endpoint_url,remote_bucket_name,region,access_key_encrypted,secret_key_encrypted,identity_marker) VALUES($1,'catalog','s3','http://backend:8333','catalog-fixture','us-east-1',$2,$3,$4)")
        .bind(storage).bind(codec::protect(b"source-access", &key, format!("storage-{storage}-access").as_bytes())?)
        .bind(codec::protect(b"source-secret", &key, format!("storage-{storage}-secret").as_bytes())?)
        .bind(crate::storage::model::identity_marker(node.deployment_id, storage)?).execute(&pool).await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id) VALUES($1)")
        .bind(storage)
        .execute(&pool)
        .await?;
    let bucket: Uuid = sqlx::query_scalar(
        "INSERT INTO buckets(name,storage_id) VALUES('catalog-a',$1) RETURNING id",
    )
    .bind(storage)
    .fetch_one(&pool)
    .await?;
    let other: Uuid = sqlx::query_scalar(
        "INSERT INTO buckets(name,storage_id) VALUES('catalog-b',$1) RETURNING id",
    )
    .bind(storage)
    .fetch_one(&pool)
    .await?;
    sqlx::query("INSERT INTO s3_credentials(access_key_id,secret_encrypted) VALUES('catalog',$1)")
        .bind(codec::protect(b"catalog-secret", &key, b"catalog")?)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO s3_credential_grants(access_key,bucket_id,actions) VALUES('catalog',$1,ARRAY['bucket.list']),('catalog',$2,ARRAY['object.read'])")
        .bind(bucket).bind(other).execute(&pool).await?;
    let prepare = |name: Option<&str>, query: Query, cached| request::Prepare {
        run: session.run,
        access: Some("catalog".into()),
        route: name.map(|name| model::ReadRoute::Bucket {
            name: name.into(),
            key: String::new(),
            alias: None,
        }),
        include: if name.is_some() {
            vec![read::MaterialType::Credential, read::MaterialType::Bucket]
        } else {
            vec![read::MaterialType::Credential]
        },
        cached,
        query: Some(query),
    };
    let lists = |token: Option<String>| Query::Objects {
        prefix: String::new(),
        delimiter: "/".into(),
        after: None,
        token,
        limit: 1,
    };
    let buckets = |token: Option<String>| Query::Buckets {
        prefix: "catalog-".into(),
        region: None,
        expected_region: "us-east-1".into(),
        token,
        limit: 1,
    };
    let reply = backend
        .prepare_request(
            &node,
            &key,
            prepare(Some("catalog-a"), Query::Bucket, vec![]),
        )
        .await?;
    assert!(matches!(reply.catalog, Some(Catalog::Bucket)));
    assert!(reply.context.grants.is_empty());
    let cached = reply
        .context
        .materials
        .iter()
        .map(|value| value.version.clone())
        .collect::<Vec<_>>();
    let denied = backend
        .prepare_request(
            &node,
            &key,
            prepare(Some("catalog-b"), Query::Bucket, cached.clone()),
        )
        .await?;
    assert!(denied.context.outcome == read::Outcome::AccessDenied && denied.catalog.is_none());
    let missing = backend
        .prepare_request(
            &node,
            &key,
            prepare(Some("catalog-missing"), Query::Bucket, cached.clone()),
        )
        .await?;
    assert!(missing.context.outcome == read::Outcome::NoSuchBucket && missing.catalog.is_none());
    let listed = backend
        .prepare_request(&node, &key, prepare(None, buckets(None), cached.clone()))
        .await?;
    assert!(
        matches!(listed.catalog, Some(Catalog::Buckets { ref rows, next: None }) if rows.len() == 1 && rows[0].name == "catalog-a")
    );
    sqlx::query("UPDATE s3_credential_grants SET actions=ARRAY['bucket.list'] WHERE bucket_id=$1 AND access_key='catalog'").bind(other).execute(&pool).await?;
    let listed = backend
        .prepare_request(&node, &key, prepare(None, buckets(None), cached.clone()))
        .await?;
    let Some(Catalog::Buckets {
        rows,
        next: Some(token),
    }) = listed.catalog
    else {
        panic!("expected bucket cursor")
    };
    assert_eq!(rows[0].name, "catalog-a");
    let listed = backend
        .prepare_request(
            &node,
            &key,
            prepare(None, buckets(Some(token.clone())), cached.clone()),
        )
        .await?;
    assert!(
        matches!(listed.catalog, Some(Catalog::Buckets { ref rows, next: None }) if rows.len() == 1 && rows[0].name == "catalog-b")
    );
    let mut wrong = buckets(Some(token));
    if let Query::Buckets { prefix, .. } = &mut wrong {
        *prefix = "other-".into();
    }
    let invalid = backend
        .prepare_request(&node, &key, prepare(None, wrong, vec![]))
        .await?;
    assert!(
        matches!(invalid.catalog, Some(Catalog::InvalidArgument))
            && invalid.context.credential.is_some()
    );
    for object_key in
        (0..140)
            .map(|n| format!("a/{n:03}"))
            .chain(["b+%_".into(), "c/item".into(), "z".into()])
    {
        let stream: Uuid = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,etag) VALUES($1,$2,'object','ready',7,'catalog-etag') RETURNING id").bind(bucket).bind(&object_key).fetch_one(&pool).await?;
        sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,$2,$3,uuidv7())").bind(bucket).bind(&object_key).bind(stream).execute(&pool).await?;
    }
    let mut token = None;
    let mut entries = Vec::new();
    for _ in 0..4 {
        let reply = backend
            .prepare_request(
                &node,
                &key,
                prepare(Some("catalog-a"), lists(token.take()), cached.clone()),
            )
            .await?;
        let Some(Catalog::Objects {
            rows,
            prefixes,
            next,
        }) = reply.catalog
        else {
            panic!("expected object page")
        };
        assert_eq!(rows.len() + prefixes.len(), 1);
        entries.extend(rows.into_iter().map(|row| {
            assert_eq!((row.size, row.etag.as_str()), (7, "catalog-etag"));
            row.key
        }));
        entries.extend(prefixes);
        token = next;
    }
    assert_eq!(entries, ["a/", "b+%_", "c/", "z"]);
    assert!(token.is_none());
    let first = backend
        .prepare_request(
            &node,
            &key,
            prepare(Some("catalog-a"), lists(None), cached.clone()),
        )
        .await?;
    let Some(Catalog::Objects {
        next: Some(token), ..
    }) = first.catalog
    else {
        panic!("missing object cursor")
    };
    for name in ["catalog-a", "catalog-b"] {
        let mut query = lists(Some(token.clone()));
        if name == "catalog-a"
            && let Query::Objects { delimiter, .. } = &mut query
        {
            delimiter.clear();
        }
        let invalid = backend
            .prepare_request(&node, &key, prepare(Some(name), query, vec![]))
            .await?;
        assert!(
            matches!(invalid.catalog, Some(Catalog::InvalidArgument))
                && invalid.context.credential.is_some()
        );
    }
    let flat = Query::Objects {
        prefix: "a/".into(),
        delimiter: String::new(),
        after: None,
        token: None,
        limit: 1000,
    };
    let reply = backend
        .prepare_request(
            &node,
            &key,
            prepare(Some("catalog-a"), flat, cached.clone()),
        )
        .await?;
    assert!(
        matches!(reply.catalog, Some(Catalog::Objects { ref rows, next: None, .. }) if rows.len() == 140 && rows[139].key == "a/139")
    );
    let empty = Query::Objects {
        prefix: String::new(),
        delimiter: String::new(),
        after: None,
        token: None,
        limit: 0,
    };
    let reply = backend
        .prepare_request(
            &node,
            &key,
            prepare(Some("catalog-a"), empty, cached.clone()),
        )
        .await?;
    assert!(
        matches!(reply.catalog, Some(Catalog::Objects { ref rows, ref prefixes, next: None }) if rows.is_empty() && prefixes.is_empty())
    );
    sqlx::query("DELETE FROM s3_credential_grants WHERE access_key='catalog'")
        .execute(&pool)
        .await?;
    let denied = backend
        .prepare_request(
            &node,
            &key,
            prepare(Some("catalog-a"), lists(None), cached.clone()),
        )
        .await?;
    assert!(denied.context.outcome == read::Outcome::AccessDenied && denied.catalog.is_none());
    sqlx::query("UPDATE s3_credentials SET enabled=false WHERE access_key_id='catalog'")
        .execute(&pool)
        .await?;
    let denied = backend
        .prepare_request(&node, &key, prepare(None, buckets(None), cached.clone()))
        .await?;
    assert!(
        denied.context.outcome == read::Outcome::AccessDenied
            && denied.context.credential.is_none()
            && denied.catalog.is_none()
    );
    let mut stale = prepare(None, buckets(None), vec![]);
    stale.run.fence = Uuid::new_v4();
    assert!(backend.prepare_request(&node, &key, stale).await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM node_reads)+(SELECT count(*) FROM node_writes)+(SELECT count(*) FROM stream_leases)").fetch_one(&pool).await?;
    assert_eq!(count, 0);
    Ok(())
}
