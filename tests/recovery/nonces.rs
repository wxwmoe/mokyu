use crate::authorization::Principal;
use crate::config::Config;
use crate::content::{codec, nonce};
use crate::model::key::ManagedKeyCreate;
use anyhow::Result;
use std::{collections::HashSet, path::Path, sync::Arc};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires MOKYU_TEST_NONCE_CONFIG pointing to a fresh isolated database"]
async fn nonce_database_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_NONCE_CONFIG")?;
    let (config, mut secrets, budget) = Config::load(Path::new(&path))?;
    let (stores, _owner) = crate::model::connect(&config, &mut secrets, &budget, "single").await?;
    stores.runtime.initialize(false).await?;
    let store = stores.managed_keys;
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let input = |id, material: [u8; 32]| -> Result<ManagedKeyCreate> {
        Ok(ManagedKeyCreate {
            id,
            name: format!("nonce-{id}"),
            purpose: "block".into(),
            algorithm: "aes-256-gcm".into(),
            protected: codec::protect(
                &material,
                &secrets.credential_key,
                format!("managed-key-{id}").as_bytes(),
            )?,
            fingerprint: *blake3::hash(&material).as_bytes(),
        })
    };
    assert!(
        crate::model::audit::scope(
            (-1).into(),
            store.create(&Principal::Local, input(first, [137; 32])?)
        )
        .await
        .is_err()
    );
    assert!(store.reserve_nonce(first, nonce::CHUNK, 1).await.is_err());
    store
        .create(&Principal::Local, input(first, [137; 32])?)
        .await?;
    store
        .create(&Principal::Local, input(second, [138; 32])?)
        .await?;
    assert_eq!(store.reserve_nonce(first, nonce::CHUNK, 1).await?, (1, 64));
    assert_eq!(store.reserve_nonce(first, nonce::PACK, 1).await?, (1, 64));
    assert_eq!(store.reserve_nonce(second, nonce::CHUNK, 1).await?, (1, 64));
    assert_eq!(
        store.reserve_nonce(first, nonce::CHUNK, 3).await?,
        (65, 256)
    );
    assert!(store.reserve_nonce(first, 2, 1).await.is_err());
    assert!(store.reserve_nonce(first, nonce::CHUNK, 0).await.is_err());
    assert!(
        store
            .reserve_nonce(Uuid::new_v4(), nonce::CHUNK, 1)
            .await
            .is_err()
    );

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let store = store.clone();
        tasks.spawn(async move {
            let allocator = Arc::new(nonce::Allocator::default());
            let mut values = Vec::new();
            for _ in 0..257 {
                values.push(
                    allocator
                        .next(first, nonce::CHUNK, |units| {
                            store.reserve_nonce(first, nonce::CHUNK, units)
                        })
                        .await?,
                );
            }
            Ok::<_, anyhow::Error>(values)
        });
    }
    let mut unique = HashSet::new();
    while let Some(result) = tasks.join_next().await {
        for id in result?? {
            assert!(id > 256 && unique.insert(id));
        }
    }
    assert_eq!(unique.len(), 8 * 257);
    let next = store.reserve_nonce(first, nonce::CHUNK, 1).await?.0;
    assert!(next > *unique.iter().max().unwrap());
    let restarted = nonce::Allocator::default()
        .next(first, nonce::CHUNK, |units| {
            store.reserve_nonce(first, nonce::CHUNK, units)
        })
        .await?;
    assert!(restarted > next + 63);

    assert!(
        store
            .create(&Principal::Local, input(Uuid::new_v4(), [137; 32])?)
            .await
            .is_err()
    );
    store.revoke(&Principal::Local, second).await?;
    assert!(store.reserve_nonce(second, nonce::PACK, 1).await.is_err());
    assert!(
        store
            .create(&Principal::Local, input(Uuid::new_v4(), [138; 32])?)
            .await
            .is_err()
    );
    Ok(())
}
