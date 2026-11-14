use super::store::{Authority, Publication, Store};
use anyhow::Result;
use bytes::Bytes;
use futures_util::{StreamExt, stream::BoxStream};
use object_store::{ObjectStoreExt, PutMode, PutOptions, path::Path};
use uuid::Uuid;

pub(crate) async fn write_part<'a>(
    store: &Store,
    file: Uuid,
    size: u64,
    mut body: impl FnMut() -> BoxStream<'a, Result<Bytes>>,
) -> Result<String> {
    let path = Path::from(crate::storage::fragment_key(file));
    let mut options: PutOptions = PutMode::Create.into();
    if matches!(store.authority, Authority::Control(_)) {
        options.extensions.insert(Publication {
            owner: file,
            area: crate::content::fragment::Area::Blocks,
        });
    }
    let result = store.write(&path, size, body(), options.clone()).await;
    if result.as_ref().err().and_then(crate::model::error)
        == Some(crate::model::Error::StorageForwardFailed)
    {
        let mut direct = store.clone();
        direct.region = None;
        direct.write(&path, size, body(), options).await?;
    } else {
        result?;
    }
    Ok(file.to_string())
}

pub(crate) async fn complete(
    store: &Store,
    source: Uuid,
    parts: &[(i32, String)],
    size: u64,
) -> Result<crate::model::storage_payload::Intent> {
    let path = crate::storage::raw::remote_path(source);
    let body = || -> BoxStream<'_, Result<Bytes>> {
        Box::pin(async_stream::try_stream! {
            for (_, value) in parts {
                let file = Uuid::parse_str(value)?;
                let mut body = store.get(&Path::from(crate::storage::fragment_key(file))).await?.into_stream();
                while let Some(bytes) = body.next().await { yield bytes?; }
            }
        })
    };
    let result = store
        .write(&path, size, body(), PutMode::Create.into())
        .await;
    if result.as_ref().err().and_then(crate::model::error)
        == Some(crate::model::Error::StorageForwardFailed)
    {
        let mut direct = store.clone();
        direct.region = None;
        direct
            .write(&path, size, body(), PutMode::Create.into())
            .await
    } else {
        result
    }
}
