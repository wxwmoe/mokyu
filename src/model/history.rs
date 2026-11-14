use crate::{app::StoredStream, authorization::Permit, model::Id};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    utoipa::ToSchema,
    clap::ValueEnum,
)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Versioning {
    #[default]
    Off,
    Enabled,
    Suspended,
}

impl Versioning {
    pub fn from_code(value: i64) -> Result<Self> {
        Ok(match value {
            0 => Self::Off,
            1 => Self::Enabled,
            2 => Self::Suspended,
            _ => anyhow::bail!("invalid bucket versioning state"),
        })
    }
    pub fn code(self) -> i16 {
        match self {
            Self::Off => 0,
            Self::Enabled => 1,
            Self::Suspended => 2,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, sqlx::FromRow, utoipa::ToSchema)]
#[schema(as=ObjectHistoryEntry)]
pub(crate) struct Version {
    pub key: String,
    pub version_id: String,
    pub latest: bool,
    pub modified_us: i64,
    pub size: Option<i64>,
    pub etag: Option<String>,
    #[serde(skip)]
    pub(crate) stream: Option<Id>,
}

#[derive(Default, Clone, Serialize, Deserialize, utoipa::ToSchema, utoipa::IntoParams)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Selection {
    pub prefix: String,
    pub delimiter: Option<String>,
    pub key_marker: Option<String>,
    pub version_marker: Option<String>,
    pub max_keys: Option<i32>,
}

#[derive(Default, Serialize, Deserialize, utoipa::ToSchema)]
#[schema(as=VersionPage)]
pub(crate) struct Page {
    pub rows: Vec<Version>,
    pub prefixes: Vec<String>,
    pub next_key: Option<String>,
    pub next_version: Option<String>,
}

impl Selection {
    pub fn limit(&self) -> Result<usize> {
        ensure!(
            self.delimiter.as_ref().is_none_or(|s| s.len() <= 1024)
                && self.prefix.len() <= 1024
                && self.key_marker.as_ref().is_none_or(|k| k.len() <= 1024)
                && self
                    .version_marker
                    .as_ref()
                    .is_none_or(|id| self.key_marker.is_some() && valid_id(id)),
            s3s::s3_error!(InvalidArgument)
        );
        let n = self.max_keys.unwrap_or(1000);
        ensure!(n >= 0, s3s::s3_error!(InvalidArgument));
        Ok(n.min(1000) as usize)
    }
}

pub(crate) fn valid_id(id: &str) -> bool {
    id == "null"
        || id.len() == 32
            && id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[async_trait::async_trait]
pub(crate) trait History: Send + Sync {
    async fn versioning(
        &self,
        principal: &crate::authorization::Principal,
        bucket: Uuid,
        change: Option<Versioning>,
    ) -> Result<Versioning>;
    async fn versions(
        &self,
        authority: &Permit,
        bucket: Uuid,
        selection: &Selection,
    ) -> Result<Page>;
    async fn version(
        &self,
        authority: &Permit,
        bucket: Uuid,
        key: &str,
        version: Option<&str>,
        ttl: Option<i64>,
    ) -> Result<Option<(Version, Option<StoredStream>, Option<Uuid>)>>;
    async fn published_version(&self, stream: Id) -> Result<Option<String>>;
    async fn delete_version(
        &self,
        authority: &Permit,
        bucket: Uuid,
        key: &str,
        id: &str,
    ) -> Result<Option<Version>>;
}

tokio::task_local! {
    pub(crate) static DELETE_RECEIPT: std::cell::RefCell<Option<Version>>;
}
pub(crate) fn delete_result(row: Option<Version>) {
    let _ = DELETE_RECEIPT.try_with(|cell| *cell.borrow_mut() = row);
}

pub(crate) fn journal_key(key: String) -> String {
    if key.starts_with('~') {
        format!("~{key}")
    } else {
        key
    }
}
pub(crate) enum Subject<'a> {
    Current(&'a str),
    History(&'a str, &'a str),
    Mode,
}
impl Subject<'_> {
    pub(crate) fn key(&self) -> Option<&str> {
        match self {
            Self::Current(k) | Self::History(k, _) => Some(k),
            Self::Mode => None,
        }
    }
}
pub(crate) fn subject(slot: &str) -> Result<Subject<'_>> {
    if slot == "~m" {
        return Ok(Subject::Mode);
    }
    let subject = if let Some(h) = slot.strip_prefix("~h/") {
        let (id, key) = h
            .split_once('/')
            .ok_or_else(|| anyhow::anyhow!("invalid journal history key"))?;
        ensure!(valid_id(id), "invalid journal history version");
        Subject::History(key, id)
    } else {
        Subject::Current(if slot.starts_with("~~") {
            &slot[1..]
        } else {
            slot
        })
    };
    ensure!(
        subject
            .key()
            .is_some_and(|v| !v.is_empty() && v.len() <= 1024),
        "invalid journal key"
    );
    Ok(subject)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn journal_subjects_round_trip_reserved_and_maximum_keys() {
        for key in [
            "~m".to_owned(),
            "~h/null/key".to_owned(),
            "~~".repeat(512),
            "x".repeat(1024),
        ] {
            assert!(
                matches!(subject(&journal_key(key.clone())).unwrap(), Subject::Current(k) if k==key)
            );
            assert!(
                matches!(subject(&format!("~h/null/{key}")).unwrap(), Subject::History(k,"null") if k==key)
            );
        }
        assert!(subject("~h/not-a-version/key").is_err());
        assert!(subject("~h/null/").is_err());
    }
}
