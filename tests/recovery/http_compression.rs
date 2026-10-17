use crate::model::{CompressionObservation, StreamStore};
use anyhow::Result;
use serde_json::json;
use uuid::Uuid;

pub(crate) async fn check(store: &dyn StreamStore, bucket: Uuid, key: &str) -> Result<()> {
    let before = store.current(bucket, key).await?;
    let observation = |encoding, size, version| CompressionObservation {
        stream: before.id,
        encoding,
        size,
        version,
    };
    let (zstd, br) = tokio::join!(
        store.learn_compression(observation("zstd", 30, 1), None),
        store.learn_compression(observation("br", 0, 1), None)
    );
    assert!(zstd? && br?);
    assert!(
        !store
            .learn_compression(observation("zstd", 31, 1), None)
            .await?
    );
    let learned = store.current(bucket, key).await?;
    assert_eq!(
        learned.http_compression,
        json!({"version":1,"estimated_size":64,"future":5,"zstd":30,"br":0})
    );
    assert_eq!(
        (learned.touched_at, &learned.etag, &learned.metadata),
        (before.touched_at, &before.etag, &before.metadata)
    );
    assert!(
        serde_json::to_value(&learned)?
            .get("http_compression")
            .is_none()
    );
    assert!(
        store
            .learn_compression(observation("gzip", 40, 2), None)
            .await?
    );
    assert!(
        !store
            .learn_compression(observation("zstd", 10, 1), None)
            .await?
    );
    assert_eq!(
        store.current(bucket, key).await?.http_compression,
        json!({"version":2,"estimated_size":64,"gzip":40})
    );
    assert!(
        store
            .learn_compression(observation("unknown", 1, 2), None)
            .await
            .is_err()
    );
    assert!(
        store
            .learn_compression(observation("zstd", -1, 2), None)
            .await
            .is_err()
    );
    Ok(())
}
