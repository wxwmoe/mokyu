use anyhow::Result;
use s3s::s3_error;
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
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
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Principal {
    Local,
    User {
        id: Uuid,
        session: Uuid,
        revision: i64,
    },
    Service {
        access_key: String,
        revision: i64,
    },
}
impl Principal {
    pub async fn lock_admin(&self, tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        if self.validate(tx, true).await? {
            Ok(())
        } else {
            Err(s3_error!(AccessDenied).into())
        }
    }
    pub async fn service(db: &PgPool, access_key: &str) -> Result<Self> {
        let revision = sqlx::query_scalar(
            "SELECT authorization_revision FROM credentials WHERE access_key=$1 AND enabled",
        )
        .bind(access_key)
        .fetch_optional(db)
        .await?
        .ok_or_else(|| s3_error!(AccessDenied))?;
        Ok(Self::Service {
            access_key: access_key.into(),
            revision,
        })
    }
    async fn validate(&self, connection: &mut PgConnection, lock: bool) -> Result<bool> {
        match self {
            Self::Local => Ok(true),
            Self::User {
                id,
                session,
                revision,
            } => {
                let query = if lock {
                    "SELECT u.role,u.authorization_revision FROM web_users u JOIN sessions s ON s.user_id=u.id WHERE u.id=$1 AND s.id=$2 AND u.enabled AND s.expires_at>now() AND s.auth_revision=u.auth_revision FOR SHARE OF u"
                } else {
                    "SELECT u.role,u.authorization_revision FROM web_users u JOIN sessions s ON s.user_id=u.id WHERE u.id=$1 AND s.id=$2 AND u.enabled AND s.expires_at>now() AND s.auth_revision=u.auth_revision"
                };
                let row: Option<(String, i64)> = sqlx::query_as(query)
                    .bind(id)
                    .bind(session)
                    .fetch_optional(connection)
                    .await?;
                match row {
                    Some((role, current)) if current == *revision => Ok(role == "admin"),
                    _ => Err(s3_error!(AccessDenied).into()),
                }
            }
            Self::Service {
                access_key,
                revision,
            } => {
                let query = if lock {
                    "SELECT authorization_revision FROM credentials WHERE access_key=$1 AND enabled FOR SHARE"
                } else {
                    "SELECT authorization_revision FROM credentials WHERE access_key=$1 AND enabled"
                };
                let current: Option<i64> = sqlx::query_scalar(query)
                    .bind(access_key)
                    .fetch_optional(connection)
                    .await?;
                if current == Some(*revision) {
                    Ok(false)
                } else {
                    Err(s3_error!(AccessDenied).into())
                }
            }
        }
    }
    async fn bucket_actions(
        &self,
        connection: &mut PgConnection,
        bucket: Uuid,
        admin: bool,
    ) -> Result<Vec<Action>> {
        if admin {
            return Ok(Action::ALL.to_vec());
        }
        let actions = match self {
            Self::User { id, .. } => {
                let row: Option<Vec<String>> = sqlx::query_scalar(
                    "SELECT actions FROM user_bucket_access WHERE bucket_id=$1 AND user_id=$2",
                )
                .bind(bucket)
                .bind(id)
                .fetch_optional(connection)
                .await?;
                Action::ALL
                    .into_iter()
                    .filter(|a| {
                        row.as_ref()
                            .is_some_and(|row| row.iter().any(|s| s == a.name()))
                    })
                    .collect()
            }
            Self::Service { access_key, .. } => {
                let row: Option<Vec<String>> = sqlx::query_scalar("SELECT g.actions FROM grants g JOIN buckets b ON b.id=g.bucket_id JOIN credentials c ON c.access_key=g.access_key AND c.project_id=b.project_id WHERE g.access_key=$1 AND g.bucket_id=$2")
                    .bind(access_key).bind(bucket).fetch_optional(connection).await?;
                Action::ALL
                    .into_iter()
                    .filter(|a| {
                        row.as_ref()
                            .is_some_and(|row| row.iter().any(|s| s == a.name()))
                    })
                    .collect()
            }
            Self::Local => Action::ALL.to_vec(),
        };
        Ok(actions)
    }
    pub async fn actions(&self, db: &PgPool, bucket: Uuid) -> Result<Vec<Action>> {
        let mut connection = db.acquire().await?;
        let admin = self.validate(&mut connection, false).await?;
        self.bucket_actions(&mut connection, bucket, admin).await
    }
    pub async fn require(&self, db: &PgPool, bucket: Uuid, action: Action) -> Result<Permit> {
        if !self.actions(db, bucket).await?.contains(&action) {
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
    pub fn local() -> Self {
        Self {
            principal: Principal::Local,
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
    pub async fn lock(&self, tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        let admin = self.principal.validate(tx, true).await?;
        for (bucket, required) in &self.resources {
            let exists: Option<Uuid> =
                sqlx::query_scalar("SELECT id FROM buckets WHERE id=$1 FOR SHARE")
                    .bind(bucket)
                    .fetch_optional(&mut **tx)
                    .await?;
            if exists.is_none() {
                return Err(s3_error!(AccessDenied).into());
            }
            let available = self.principal.bucket_actions(tx, *bucket, admin).await?;
            if required.iter().any(|a| !available.contains(a)) {
                return Err(s3_error!(AccessDenied).into());
            }
        }
        Ok(())
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
