use crate::authorization::{Action, Principal};
use crate::model::administration::BucketGrant;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Serialize, ToSchema)]
pub(crate) struct Credential {
    pub access_key: String,
    pub project_id: Uuid,
    pub label: String,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub created_by: Option<Uuid>,
    #[schema(value_type=Vec<BucketGrant>)]
    pub grants: Vec<BucketGrant>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct CredentialSecret {
    pub access_key: String,
    pub secret_key: String,
    pub credential: Credential,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CredentialInput {
    pub project_id: Uuid,
    pub label: String,
    pub expires_in: Option<String>,
    pub grants: Vec<BucketGrant>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CredentialSettings {
    pub label: String,
    pub enabled: bool,
    pub expires_in: Option<String>,
    #[serde(default)]
    pub keep_expiry: bool,
    pub grants: Option<Vec<BucketGrant>>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct RotateCredential {
    pub overlap: String,
    pub expires_in: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct CredentialRotation {
    #[serde(flatten)]
    pub replacement: CredentialSecret,
    pub old_expires_at: DateTime<Utc>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct Token {
    pub id: Uuid,
    pub user_id: Uuid,
    pub label: String,
    pub prefix: String,
    pub system: bool,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    #[schema(value_type=Vec<BucketGrant>)]
    pub grants: Vec<BucketGrant>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct TokenInput {
    pub label: String,
    #[serde(default)]
    pub system: bool,
    #[serde(default = "default_expiry")]
    pub expires_in: Option<String>,
    #[serde(default)]
    pub keep_expiry: bool,
    #[serde(default)]
    pub grants: Vec<BucketGrant>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct TokenSecret {
    pub token: Token,
    pub secret: String,
}

pub(crate) fn label(value: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    Ok(())
}

pub(crate) fn expiration(value: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    value
        .map(|value| {
            let seconds =
                crate::config::seconds(value).map_err(|_| s3s::s3_error!(InvalidArgument))?;
            if seconds == 0 || seconds > 3650 * 86400 {
                return Err(s3s::s3_error!(InvalidArgument).into());
            }
            Ok(Utc::now() + chrono::Duration::seconds(seconds as i64))
        })
        .transpose()
}

pub(crate) fn validate_grants(grants: &[BucketGrant]) -> Result<()> {
    let mut ids = HashSet::new();
    if grants.len() > 1000
        || grants.iter().any(|g| {
            !ids.insert(g.bucket_id)
                || g.actions.is_empty()
                || g.actions.len() > 7
                || g.actions
                    .iter()
                    .enumerate()
                    .any(|(i, a)| g.actions[..i].contains(a))
        })
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    Ok(())
}

fn default_expiry() -> Option<String> {
    Some("90d".into())
}

pub(crate) struct CredentialMaterial {
    pub access: String,
    pub encrypted: Vec<u8>,
}

pub(crate) struct TokenMaterial {
    pub prefix: String,
    pub hash: [u8; 32],
}

#[async_trait]
pub(crate) trait CredentialStore: Send + Sync {
    async fn token_user(&self, id: Uuid) -> Result<Option<Uuid>>;
    async fn encrypted_secret(&self, access: &str) -> Result<Option<Vec<u8>>>;
    async fn touch_credential(&self, access: &str) -> Result<()>;
    async fn buckets(
        &self,
        access: &str,
        prefix: &str,
        after: &str,
        region: Option<&str>,
        expected_region: &str,
        limit: i64,
    ) -> Result<Vec<crate::app::Bucket>>;
    async fn credential(&self, access: &str) -> Result<Credential>;
    async fn credentials(
        &self,
        project: Option<Uuid>,
        after: &str,
        limit: i64,
    ) -> Result<Vec<Credential>>;
    async fn create_credential(
        &self,
        principal: &Principal,
        input: CredentialInput,
        material: CredentialMaterial,
    ) -> Result<Credential>;
    async fn update_credential(
        &self,
        principal: &Principal,
        access: &str,
        input: CredentialSettings,
    ) -> Result<Credential>;
    async fn set_grants(
        &self,
        principal: &Principal,
        access: &str,
        grants: Vec<BucketGrant>,
    ) -> Result<Credential>;
    async fn revoke_credential(&self, principal: &Principal, access: &str) -> Result<()>;
    async fn enable_credential(
        &self,
        principal: &Principal,
        access: &str,
        enabled: bool,
    ) -> Result<()>;
    async fn change_grant(
        &self,
        principal: &Principal,
        access: &str,
        bucket: Uuid,
        actions: Vec<Action>,
    ) -> Result<()>;
    async fn rotate_credential(
        &self,
        principal: &Principal,
        access: &str,
        input: RotateCredential,
        material: CredentialMaterial,
    ) -> Result<(Credential, DateTime<Utc>)>;
    async fn create_token(
        &self,
        principal: &Principal,
        user: Uuid,
        input: TokenInput,
        material: TokenMaterial,
    ) -> Result<Token>;
    async fn update_token(
        &self,
        principal: &Principal,
        user: Uuid,
        id: Uuid,
        input: TokenInput,
    ) -> Result<Token>;
    async fn revoke_token(&self, principal: &Principal, id: Uuid) -> Result<()>;
    async fn tokens(&self, user: Uuid, after: Option<Uuid>, limit: i64) -> Result<Vec<Token>>;
}
