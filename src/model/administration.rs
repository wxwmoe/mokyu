use crate::authorization::{Action, Principal};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Serialize, ToSchema)]
pub(crate) struct User {
    pub id: Uuid,
    pub username: String,
    pub(crate) display_name: String,
    pub role: String,
    pub enabled: bool,
    pub(crate) must_change_password: bool,
    pub(crate) created_at: DateTime<Utc>,
}

#[derive(Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct UserPatch {
    pub role: Option<String>,
    pub enabled: Option<bool>,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct BucketGrant {
    pub bucket_id: Uuid,
    pub actions: Vec<Action>,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct Membership {
    pub role: String,
    pub scope: String,
    #[serde(default)]
    pub grants: Vec<BucketGrant>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct Member {
    pub(crate) user_id: Uuid,
    pub(crate) username: String,
    pub(crate) display_name: String,
    pub(crate) enabled: bool,
    pub(crate) role: String,
    pub(crate) scope: String,
    #[schema(value_type=Vec<BucketGrant>)]
    pub(crate) grants: Vec<BucketGrant>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct Project {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) builtin: bool,
    pub(crate) allow_bucket_create: bool,
    pub(crate) created_at: DateTime<Utc>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectInput {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub allow_bucket_create: bool,
}

pub(crate) struct UserCreate {
    pub username: String,
    pub password_hash: String,
    pub role: String,
    pub must_change_password: bool,
}

#[async_trait]
pub(crate) trait AdministrationStore: Send + Sync {
    async fn all_users(&self) -> Result<Vec<User>>;
    async fn find_user(&self, name: &str) -> Result<Option<User>>;
    async fn user(&self, id: Uuid) -> Result<Option<User>>;
    async fn users(
        &self,
        search: &str,
        after: &str,
        limit: i64,
        role: Option<&str>,
    ) -> Result<Vec<User>>;
    async fn create_user(&self, principal: &Principal, input: UserCreate) -> Result<User>;
    async fn update_user(&self, principal: &Principal, id: Uuid, input: UserPatch) -> Result<User>;
    async fn delete_user(&self, principal: &Principal, id: Uuid) -> Result<()>;
    async fn reset_password(
        &self,
        principal: &Principal,
        id: Uuid,
        hash: &str,
        require_change: bool,
    ) -> Result<()>;
    async fn save_member(
        &self,
        principal: &Principal,
        project: Uuid,
        user: Uuid,
        input: Option<Membership>,
    ) -> Result<()>;
    async fn members(&self, project: Uuid) -> Result<Vec<Member>>;
    async fn projects(&self) -> Result<Vec<Project>>;
    async fn visible_projects(
        &self,
        user: Uuid,
        admin: bool,
        token: Option<Uuid>,
    ) -> Result<Vec<Project>>;
    async fn save_project(
        &self,
        principal: &Principal,
        id: Option<Uuid>,
        input: ProjectInput,
    ) -> Result<Project>;
    async fn delete_project(&self, principal: &Principal, id: Uuid) -> Result<()>;
    async fn project_mode(&self) -> Result<bool>;
    async fn set_project_mode(&self, principal: &Principal, enabled: bool) -> Result<()>;
}
