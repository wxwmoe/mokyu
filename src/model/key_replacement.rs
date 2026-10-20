use crate::authorization::Principal;
use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct KeyReplacementInput {
    pub target: Option<Uuid>,
    pub name: Option<String>,
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
pub(crate) struct KeyReplacementPreview {
    pub id: Uuid,
    pub source: Uuid,
    pub source_name: String,
    pub target: Uuid,
    pub target_name: String,
    pub algorithm: String,
    pub create: bool,
    pub expires_at: DateTime<Utc>,
    pub storages: Vec<KeyReplacementStorage>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, sqlx::FromRow, ToSchema)]
pub(crate) struct KeyReplacementStorage {
    pub id: Uuid,
    pub name: String,
    pub enabled: bool,
    pub revision: i64,
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
pub(crate) struct KeyReplacementResult {
    pub preview: KeyReplacementPreview,
    pub committed_at: DateTime<Utc>,
}

pub(crate) type ReplacementRow = (
    sqlx::types::Json<KeyReplacementPreview>,
    Option<sqlx::types::Json<KeyReplacementResult>>,
);

pub(crate) fn actor(principal: &Principal) -> Result<String> {
    Ok(match principal {
        Principal::Local => "local".into(),
        Principal::User { id, .. } => format!("user:{id}"),
        Principal::Token { id, user, .. } => format!("token:{user}:{id}"),
        Principal::Node { id, role } if role == "control" => format!("node:{id}"),
        _ => return Err(s3s::s3_error!(AccessDenied).into()),
    })
}

pub(crate) fn validate(input: &KeyReplacementInput, source: Uuid) -> Result<()> {
    ensure!(
        input.target != Some(source),
        s3s::s3_error!(InvalidArgument, "target key must differ")
    );
    ensure!(
        input.target.is_none() || input.name.is_none(),
        s3s::s3_error!(InvalidArgument, "an existing target cannot have a new name")
    );
    if let Some(name) = &input.name {
        crate::model::key::validate_name(name)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/recovery/key_replacement.rs"]
mod recovery;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replacement_identity_survives_session_refresh_but_not_token_changes() {
        let user = Uuid::new_v4();
        let make = |session| Principal::User {
            id: user,
            session,
            revision: 1,
        };
        assert_eq!(
            actor(&make(Uuid::new_v4())).unwrap(),
            actor(&make(Uuid::new_v4())).unwrap()
        );
        let token = Principal::Token {
            id: Uuid::new_v4(),
            user,
            revision: 1,
            user_revision: 1,
        };
        assert_ne!(
            actor(&make(Uuid::new_v4())).unwrap(),
            actor(&token).unwrap()
        );
        assert!(actor(&Principal::System).is_err());
        assert!(
            validate(
                &KeyReplacementInput {
                    target: Some(user),
                    name: None
                },
                user
            )
            .is_err()
        );
        assert!(
            validate(
                &KeyReplacementInput {
                    target: Some(Uuid::new_v4()),
                    name: Some("new".into())
                },
                user
            )
            .is_err()
        );
    }
}
