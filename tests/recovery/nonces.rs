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
    let store = stores.managed_keys.clone();
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
    let initial = store.nonce_status().await?;
    assert_eq!(
        initial
            .iter()
            .find(|v| v.key_id == first)
            .unwrap()
            .remaining
            .as_deref(),
        Some("100")
    );
    store.sample_nonces().await?;
    let sampled = store.nonce_observations().await?;
    let observed = sampled.iter().find(|v| v.0 == first).unwrap();
    assert!(observed.6.is_some());
    assert_eq!(observed.7, Some(1));
    store.sample_nonces().await?;
    assert_eq!(
        store
            .nonce_observations()
            .await?
            .iter()
            .find(|v| v.0 == first)
            .unwrap()
            .7,
        Some(1)
    );
    assert_eq!(store.reserve_nonce(first, nonce::CHUNK, 1).await?, (1, 64));
    assert_eq!(
        store
            .nonce_status()
            .await?
            .iter()
            .find(|v| v.key_id == first)
            .unwrap()
            .remaining
            .as_deref(),
        Some("100")
    );
    // A stale collector cannot overwrite a later collector's history.
    store
        .save_nonce_history(first, Some(1), observed.6.as_ref().unwrap().0.clone())
        .await?;
    store
        .save_nonce_history(first, Some(1), Default::default())
        .await?;
    let saved = store.nonce_observations().await?;
    let saved = saved.iter().find(|v| v.0 == first).unwrap();
    assert_eq!(saved.7, Some(2));
    assert!(
        crate::model::key_usage::capacity(saved, chrono::Utc::now())
            .sampled_at
            .is_some()
    );
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
    assert!(
        !store
            .nonce_status()
            .await?
            .iter()
            .any(|v| v.key_id == second)
    );
    drop(store);
    drop(stores);
    drop(_owner);
    let (config, mut secrets, budget) = Config::load(Path::new(&path))?;
    let (stores, _owner) = crate::model::connect(&config, &mut secrets, &budget, "single").await?;
    let restored = stores.managed_keys.nonce_status().await?;
    let restored = restored.iter().find(|v| v.key_id == first).unwrap();
    assert!(restored.sampled_at.is_some());
    assert_eq!(restored.remaining.as_deref(), Some("100"));
    Ok(())
}
