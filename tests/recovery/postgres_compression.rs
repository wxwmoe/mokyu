use super::{Backend, migration, queries};
use crate::model::Id;
use crate::model::{CompressionObservation, StreamStore};
use anyhow::Result;
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_DATABASE_URL_FILE pointing to a new isolated PostgreSQL database"]
async fn postgres_compression_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = PgPool::connect(url.trim()).await?;
    assert!(queries::metadata_table().fetch_one(&pool).await?.is_none());
    migration::run(&mut *pool.acquire().await?).await?;
    let backend = Backend::new(pool.clone(), pool.acquire().await?.detach(), true);
    let bucket = Uuid::new_v4();
    let stream = Id::from(Uuid::now_v7());
    let storage = Uuid::new_v4();
    sqlx::query("INSERT INTO storages(id,name,type,path) VALUES($1,'compression','local','/data/compression')").bind(storage).execute(&pool).await?;
    sqlx::query("INSERT INTO buckets(id,name,storage_id) VALUES($1,'compression',$2)")
        .bind(bucket)
        .bind(storage)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO streams(id,bucket_id,object_key,stream_type,state,size,etag,http_compression) VALUES($1,$2,'object','object','ready',64,'etag','{\"version\":1,\"estimated_size\":64,\"future\":5}')").bind(stream).bind(bucket).execute(&pool).await?;
    sqlx::query(
        "INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,'object',$2,$3)",
    )
    .bind(bucket)
    .bind(stream)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await?;
    crate::model::stream::compression_tests::check(&backend, bucket, "object").await?;
    sqlx::query("UPDATE mokyu_meta SET maintenance=true")
        .execute(&pool)
        .await?;
    assert!(
        !backend
            .learn_compression(
                CompressionObservation {
                    stream,
                    version: 2,
                    encoding: "zstd",
                    size: 1
                },
                None
            )
            .await?
    );
    sqlx::query("UPDATE mokyu_meta SET maintenance=false,deployment_mode='cluster'")
        .execute(&pool)
        .await?;
    assert!(
        !backend
            .learn_compression(
                CompressionObservation {
                    stream,
                    version: 2,
                    encoding: "zstd",
                    size: 1
                },
                None
            )
            .await?
    );
    assert!(
        !backend
            .learn_compression(
                CompressionObservation {
                    stream,
                    version: 2,
                    encoding: "zstd",
                    size: 1
                },
                Some(Uuid::new_v4())
            )
            .await?
    );
    Ok(())
}
