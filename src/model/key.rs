use crate::authorization::Principal;
use crate::config::Secrets;
use crate::content::codec;
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, HashSet};
use utoipa::ToSchema;
use uuid::Uuid;

pub(crate) struct ManagedKeyRecord {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) algorithm: String,
}

#[async_trait]
pub(crate) trait ManagedKeyStore: Send + Sync {
    async fn reserve_nonce(&self, key: Uuid, type_tag: u8, units: u32) -> Result<(i64, i64)>;
    async fn resolve_block(&self, name: &str) -> Result<Option<ManagedKeyRecord>>;
    async fn active_block(&self, id: Uuid) -> Result<Option<(String, String)>>;
    async fn for_storage(&self, storage_id: Uuid) -> Result<Option<ManagedKeyRecord>>;
    async fn list(&self, principal: &Principal) -> Result<Vec<ManagedKeyView>>;
    async fn create(
        &self,
        principal: &Principal,
        input: ManagedKeyCreate,
    ) -> Result<ManagedKeyView>;
    async fn revoke(&self, principal: &Principal, id: Uuid) -> Result<()>;
    async fn material(&self, name: &str) -> Result<Option<ManagedKeyMaterial>>;
    async fn ensure_loaded(&self, secrets: &Secrets, name: &str) -> Result<()> {
        if secrets.key(name).is_some() {
            return Ok(());
        }
        let row = self
            .material(name)
            .await?
            .context("managed key was not found")?;
        ensure!(row.purpose == "block", "managed key is not a block key");
        let material = codec::unprotect_bytes(
            &row.encrypted,
            &secrets.credential_key,
            format!("managed-key-{}", row.id).as_bytes(),
        )?;
        let material: [u8; 32] = material
            .try_into()
            .map_err(|_| anyhow::anyhow!("managed key material must be 32 bytes"))?;
        ensure!(
            row.fingerprint == blake3::hash(&material).as_bytes(),
            "managed key fingerprint mismatch"
        );
        secrets.publish_key_with_id(
            Some(row.id),
            row.name,
            codec::Key::new(&row.algorithm, material)?,
        )
    }
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ManagedKeyView {
    pub id: Uuid,
    pub name: String,
    pub purpose: String,
    pub algorithm: String,
    pub in_use: bool,
    pub fingerprint: String,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

pub(crate) struct ManagedKeyCreate {
    pub id: Uuid,
    pub name: String,
    pub purpose: String,
    pub algorithm: String,
    pub protected: Vec<u8>,
    pub fingerprint: [u8; 32],
}

pub(crate) struct ManagedKeyMaterial {
    pub id: Uuid,
    pub name: String,
    pub purpose: String,
    pub algorithm: String,
    pub encrypted: Vec<u8>,
    pub fingerprint: Vec<u8>,
}

pub(crate) fn load(secrets: &mut Secrets, rows: Vec<ManagedKeyMaterial>) -> Result<()> {
    let mut keys = BTreeMap::new();
    let mut key_names = BTreeMap::new();
    let mut materials = HashSet::new();
    for ManagedKeyMaterial {
        id,
        name,
        purpose,
        algorithm: key_algorithm,
        encrypted,
        fingerprint,
    } in rows
    {
        let material = crate::content::codec::unprotect_bytes(
            &encrypted,
            &secrets.credential_key,
            format!("managed-key-{id}").as_bytes(),
        )?;
        let material: [u8; 32] = material
            .try_into()
            .map_err(|_| anyhow::anyhow!("managed key material must be 32 bytes"))?;
        ensure!(
            fingerprint == blake3::hash(&material).as_bytes(),
            "managed key fingerprint mismatch"
        );
        ensure!(materials.insert(material), "managed keys must be unique");
        if purpose != "block" {
            continue;
        }
        ensure!(
            matches!(key_algorithm.as_str(), "aes-256-gcm" | "chacha20-poly1305"),
            "invalid managed encryption key algorithm"
        );
        let key = crate::content::codec::Key::new(&key_algorithm, material)?;
        key_names.insert(id, name.clone());
        keys.insert(name, key);
    }
    secrets.keys = keys;
    *secrets
        .key_names
        .write()
        .map_err(|_| anyhow::anyhow!("key registry is unavailable"))? = key_names;
    secrets
        .runtime_keys
        .write()
        .map_err(|_| anyhow::anyhow!("key registry is unavailable"))?
        .clear();
    Ok(())
}

pub(crate) fn nonce_reservation(previous: i64, units: u32) -> Result<(i64, i64)> {
    use crate::content::nonce::{MAX_UNITS, UNIT};
    ensure!(
        previous >= 0 && (1..=MAX_UNITS).contains(&units),
        "invalid nonce reservation request"
    );
    let available = (i64::MAX - previous) / UNIT;
    ensure!(
        available > 0,
        "encryption key cannot reserve more nonce IDs; create and select a new key"
    );
    let size = i64::from(units).min(available) * UNIT;
    Ok((previous + 1, previous + size))
}

#[cfg(test)]
mod nonce_tests {
    use super::*;
    #[test]
    fn reservations_preserve_partial_tail_and_never_wrap() -> Result<()> {
        assert_eq!(nonce_reservation(0, 1)?, (1, 64));
        assert_eq!(nonce_reservation(13, 79)?, (14, 5069));
        for remaining in [0, 63] {
            assert!(nonce_reservation(i64::MAX - remaining, 1).is_err());
        }
        for remaining in [64, 65] {
            let previous = i64::MAX - remaining;
            assert_eq!(
                nonce_reservation(previous, 1024)?,
                (previous + 1, previous + 64)
            );
        }
        assert!(nonce_reservation(-1, 1).is_err());
        assert!(nonce_reservation(0, 0).is_err());
        assert!(nonce_reservation(0, 1025).is_err());
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/recovery/nonces.rs"]
mod database_tests;
