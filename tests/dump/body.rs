use super::*;
use crate::{
    dump::origin::Location,
    storage::{journal::source::Source, marker, model},
};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use std::sync::Arc;

#[tokio::test]
async fn raw_ranges_are_bound_to_the_verified_source() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let deployment = Uuid::new_v4();
    let id = Uuid::new_v4();
    let key = codec::Key::new("aes-256-gcm", [13; 32])?;
    let origin = Backend {
        deployment,
        location: Location {
            uri: "/source".into(),
            endpoint: None,
            region: None,
        },
        store: store.clone(),
        source: Source {
            connection: model::Connection {
                id,
                r#type: "local".into(),
                path: None,
                region: None,
                prefix: String::new(),
                endpoint_url: None,
                remote_bucket_name: None,
                access: None,
                secret: None,
                identity_marker: marker::create(deployment, id)?,
            },
            journal: marker::Journal::create(id, &key)?,
            material: [13; 32],
        },
    };
    let db = Connection::open_in_memory()?;
    let reader = Reader::new(&db, &origin)?;
    let path = "objects/test";
    let mut bytes = vec![42; RAW_WINDOW as usize + 100];
    let md5 = Md5::digest(&bytes);
    let size = bytes.len() as u64;
    store.put(&Path::from(path), bytes.clone().into()).await?;
    assert_eq!(
        reader
            .raw(path, id, size, &md5, RAW_WINDOW - 4, 8)
            .await?
            .as_ref(),
        [42; 8]
    );
    bytes[RAW_WINDOW as usize + 99] ^= 1;
    store.put(&Path::from(path), bytes.into()).await?;
    assert!(
        reader
            .raw(path, id, size, &md5, RAW_WINDOW + 98, 1)
            .await
            .is_err()
    );
    Ok(())
}
