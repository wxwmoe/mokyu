use anyhow::Result;
use s3s::s3_error;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use utoipa::ToSchema;
use uuid::Uuid;

pub const DEFAULT_PROJECT: Uuid = Uuid::from_u128(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum Action {
    #[serde(rename = "bucket.list")]
    List,
    #[serde(rename = "object.read")]
    Read,
    #[serde(rename = "object.write")]
    Write,
    #[serde(rename = "object.delete")]
    Delete,
    #[serde(rename = "object.acl")]
    Acl,
    #[serde(rename = "bucket.settings")]
    Settings,
    #[serde(rename = "storage.inspect")]
    Inspect,
}
impl Action {
    pub const ALL: [Self; 7] = [
        Self::List,
        Self::Read,
        Self::Write,
        Self::Delete,
        Self::Acl,
        Self::Settings,
        Self::Inspect,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Self::List => "bucket.list",
            Self::Read => "object.read",
            Self::Write => "object.write",
            Self::Delete => "object.delete",
            Self::Acl => "object.acl",
            Self::Settings => "bucket.settings",
            Self::Inspect => "storage.inspect",
        }
    }
    pub fn role(role: &str) -> Vec<Self> {
        Self::ALL
            .into_iter()
            .filter(|action| match role {
                "reader" => matches!(action, Self::List | Self::Read | Self::Inspect),
                "writer" => !matches!(action, Self::Settings),
                "maintainer" => true,
                _ => false,
            })
            .collect()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "principal_type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Principal {
    System,
    Bootstrap,
    Local,
    Node {
        id: Uuid,
        role: String,
    },
    User {
        id: Uuid,
        session: Uuid,
        revision: i64,
    },
    Service {
        access_key: String,
        revision: i64,
    },
    Token {
        id: Uuid,
        user: Uuid,
        revision: i64,
        user_revision: i64,
    },
}
impl Principal {
    pub fn user_id(&self) -> Option<Uuid> {
        match self {
            Self::User { id, .. } => Some(*id),
            Self::Token { user, .. } => Some(*user),
            _ => None,
        }
    }
    pub fn token_id(&self) -> Option<Uuid> {
        if let Self::Token { id, .. } = self {
            Some(*id)
        } else {
            None
        }
    }
    pub async fn service(
        store: &dyn crate::model::AuthorizationStore,
        access_key: &str,
    ) -> Result<Self> {
        store.service(access_key).await
    }
    pub async fn actions(
        &self,
        store: &dyn crate::model::AuthorizationStore,
        bucket: Uuid,
    ) -> Result<Vec<Action>> {
        store.actions(self, bucket).await
    }
    pub async fn require(
        &self,
        store: &dyn crate::model::AuthorizationStore,
        bucket: Uuid,
        action: Action,
    ) -> Result<Permit> {
        if !self.actions(store, bucket).await?.contains(&action) {
            return Err(s3_error!(AccessDenied).into());
        }
        Ok(Permit {
            principal: self.clone(),
            resources: BTreeMap::from([(bucket, vec![action])]),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Permit {
    pub principal: Principal,
    pub resources: BTreeMap<Uuid, Vec<Action>>,
}
impl Permit {
    pub fn system() -> Self {
        Self {
            principal: Principal::System,
            resources: BTreeMap::new(),
        }
    }
    pub fn for_action(principal: Principal, bucket: Uuid, action: Action) -> Self {
        Self {
            principal,
            resources: BTreeMap::from([(bucket, vec![action])]),
        }
    }
    pub fn add(&mut self, bucket: Uuid, action: Action) {
        self.resources.entry(bucket).or_default().push(action);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roles_form_a_capability_ceiling() {
        assert!(!Action::role("reader").contains(&Action::Write));
        assert!(!Action::role("writer").contains(&Action::Settings));
        assert_eq!(Action::role("maintainer"), Action::ALL);
        assert!(Action::role("unknown").is_empty());
        for action in Action::ALL {
            assert_eq!(serde_json::to_value(action).unwrap(), action.name());
        }
    }
}
