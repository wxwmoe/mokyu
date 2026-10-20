use crate::authorization::Principal;
use crate::config::{Config, Secrets};
use crate::content::codec;
use crate::model::Stores;
use crate::model::key::ManagedKeyCreate;
use crate::model::key_replacement::KeyReplacementInput;
use crate::storage::model::Record;
use anyhow::Result;
use std::path::Path;
use uuid::Uuid;

fn material(secrets: &Secrets, id: Uuid, name: &str, algorithm: &str) -> Result<ManagedKeyCreate> {
    let bytes = *blake3::hash(id.as_bytes()).as_bytes();
    Ok(ManagedKeyCreate {
        id,
        name: name.into(),
        purpose: "block".into(),
        algorithm: algorithm.into(),
        protected: codec::protect(
            &bytes,
            &secrets.credential_key,
            format!("managed-key-{id}").as_bytes(),
        )?,
        fingerprint: *blake3::hash(&bytes).as_bytes(),
    })
}

async fn storage(
    stores: &Stores,
    name: &str,
    key: Uuid,
    remote: bool,
    enabled: bool,
) -> Result<Uuid> {
    let mut creation = stores.storage_records.creation().await?;
    let id = Uuid::new_v4();
    let record = Record {
        id,
        name: name.into(),
        r#type: if remote { "s3" } else { "local" }.into(),
        path: (!remote).then(|| format!("/data/{id}")),
        endpoint_url: remote.then(|| "http://127.0.0.1:9000".into()),
        remote_bucket_name: remote.then(|| "fixture".into()),
        region: remote.then(|| "us-east-1".into()),
        prefix: id.to_string(),
        access_key_encrypted: remote.then(|| vec![1; 32]),
        secret_key_encrypted: remote.then(|| vec![2; 32]),
        identity_marker: crate::storage::model::identity_marker(creation.deployment, id)?,
        enabled,
    };
    creation
        .writer
        .prepare(&record, Some(key), None, None, None, None)
        .await?;
    creation.writer.finish().await?;
    if !enabled {
        let edit = stores.storage_records.edit(name).await?;
        edit.update
            .finish(&record, Some(key), &edit.profile)
            .await?;
    }
    Ok(id)
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_CONFIG pointing to a fresh isolated database"]
async fn key_replacement_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let path = std::env::var("MOKYU_TEST_CONFIG")?;
    let (config, mut secrets, budget) = Config::load(Path::new(&path))?;
    let (stores, owner) = crate::model::connect(&config, &mut secrets, &budget, "single").await?;
    stores.runtime.initialize(false).await?;
    let keys = stores.managed_keys.clone();
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    let revoked = Uuid::new_v4();
    for (id, name, algorithm) in [
        (source, "source", "aes-256-gcm"),
        (target, "existing", "chacha20-poly1305"),
        (revoked, "revoked", "aes-256-gcm"),
    ] {
        keys.create(&Principal::Local, material(&secrets, id, name, algorithm)?)
            .await?;
    }
    keys.reserve_nonce(source, 0, 1).await?;
    keys.reserve_nonce(target, 0, 1).await?;
    let a = storage(&stores, "local", source, false, true).await?;
    let b = storage(&stores, "disabled", source, true, false).await?;
    let existing = || KeyReplacementInput {
        target: Some(target),
        name: None,
    };
    assert!(
        keys.replacement_preview(&Principal::Bootstrap, source, existing())
            .await
            .is_err()
    );
    let stale = keys
        .replacement_preview(&Principal::Local, source, existing())
        .await?;
    assert_eq!(stale.storages.len(), 2);
    assert!(stale.storages.iter().any(|s| s.id == b && !s.enabled));
    assert_eq!(stale.algorithm, "chacha20-poly1305");
    stores
        .storage_records
        .current_key("local", Some("source"))
        .await?;
    assert_eq!(
        crate::model::error(
            &keys
                .replace(&Principal::Local, stale.id, None)
                .await
                .err()
                .unwrap()
        ),
        Some(crate::model::Error::StorageProfileConflict)
    );
    assert!(
        keys.replacement(&Principal::Local, stale.id)
            .await?
            .1
            .is_none()
    );
    let stale = keys
        .replacement_preview(&Principal::Local, source, existing())
        .await?;
    let c = storage(&stores, "added", source, false, true).await?;
    assert!(
        keys.replace(&Principal::Local, stale.id, None)
            .await
            .is_err()
    );
    let stale = keys
        .replacement_preview(
            &Principal::Local,
            source,
            KeyReplacementInput {
                target: Some(revoked),
                name: None,
            },
        )
        .await?;
    keys.revoke(&Principal::Local, revoked).await?;
    assert!(
        keys.replace(&Principal::Local, stale.id, None)
            .await
            .is_err()
    );

    let preview = keys
        .replacement_preview(
            &Principal::Local,
            source,
            KeyReplacementInput {
                target: None,
                name: Some("replacement".into()),
            },
        )
        .await?;
    assert_eq!(preview.storages.len(), 3);
    assert!(preview.create);
    assert_eq!(preview.algorithm, "aes-256-gcm");
    assert!(keys.active_block(preview.target).await?.is_none());
    let input = || {
        material(
            &secrets,
            preview.target,
            &preview.target_name,
            &preview.algorithm,
        )
    };
    // The audit failure occurs after all bindings and counters were changed in the transaction.
    assert!(
        crate::model::audit::scope(
            (-1).into(),
            keys.replace(&Principal::Local, preview.id, Some(input()?))
        )
        .await
        .is_err()
    );
    assert!(keys.active_block(preview.target).await?.is_none());
    assert!(
        keys.replacement(&Principal::Local, preview.id)
            .await?
            .1
            .is_none()
    );
    for id in [a, b, c] {
        assert_eq!(
            stores.storage_records.view(id).await?.block_key_id,
            Some(source)
        );
    }

    let (first, second) = tokio::join!(
        keys.replace(&Principal::Local, preview.id, Some(input()?)),
        keys.replace(&Principal::Local, preview.id, Some(input()?)),
    );
    assert_eq!(
        serde_json::to_value(first?)?,
        serde_json::to_value(second?)?
    );
    for before in &preview.storages {
        let after = stores.storage_records.view(before.id).await?;
        assert_eq!(after.block_key_id, Some(preview.target));
        assert_eq!(after.profile_revision, before.revision + 1);
        assert_eq!(after.enabled, before.enabled);
    }
    let again = keys.replace(&Principal::Local, preview.id, None).await?;
    assert_eq!(again.preview.target, preview.target);
    assert_eq!(keys.reserve_nonce(source, 0, 1).await?.0, 65);
    assert_eq!(keys.reserve_nonce(preview.target, 0, 1).await?.0, 1);
    assert!(
        keys.revoke(&Principal::Local, preview.target)
            .await
            .is_err()
    );
    assert!(
        keys.replacement_preview(&Principal::Local, source, existing())
            .await
            .is_err()
    );
    let wrong = Uuid::new_v4();
    let mut input = material(&secrets, wrong, "wrong", "raw")?;
    input.purpose = "signing".into();
    keys.create(&Principal::Local, input).await?;
    assert!(
        keys.replacement_preview(
            &Principal::Local,
            preview.target,
            KeyReplacementInput {
                target: Some(wrong),
                name: None
            }
        )
        .await
        .is_err()
    );
    let next = keys
        .replacement_preview(&Principal::Local, preview.target, existing())
        .await?;
    keys.replace(&Principal::Local, next.id, None).await?;
    assert_eq!(keys.reserve_nonce(target, 0, 1).await?.0, 65);
    drop(keys);
    drop(stores);
    drop(owner);
    let (config, mut secrets, budget) = Config::load(Path::new(&path))?;
    let (stores, _owner) = crate::model::connect(&config, &mut secrets, &budget, "single").await?;
    let saved = stores
        .managed_keys
        .replacement(&Principal::Local, preview.id)
        .await?
        .1
        .unwrap()
        .0;
    assert_eq!(serde_json::to_value(saved)?, serde_json::to_value(again)?);
    assert!(
        stores
            .managed_keys
            .replace(&Principal::Local, preview.id, None)
            .await
            .is_ok()
    );
    for id in [a, b, c] {
        assert_eq!(
            stores.storage_records.view(id).await?.block_key_id,
            Some(target)
        );
    }
    Ok(())
}
