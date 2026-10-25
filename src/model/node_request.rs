use super::{ReadRoute, node_read as read, session::Run};
use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub(crate) enum Query {
    Bucket,
    Buckets {
        prefix: String,
        region: Option<String>,
        expected_region: String,
        token: Option<String>,
        limit: usize,
    },
    Objects {
        prefix: String,
        delimiter: String,
        after: Option<String>,
        token: Option<String>,
        limit: usize,
    },
}

#[derive(Serialize, Deserialize)]
pub(crate) enum Catalog {
    InvalidArgument,
    Bucket,
    Buckets {
        rows: Vec<Bucket>,
        next: Option<String>,
    },
    Objects {
        rows: Vec<Object>,
        prefixes: Vec<String>,
        next: Option<String>,
    },
}

#[derive(Serialize, Deserialize, sqlx::FromRow)]
pub(crate) struct Bucket {
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, sqlx::FromRow)]
pub(crate) struct Object {
    pub key: String,
    pub size: i64,
    pub etag: String,
    pub modified_at: DateTime<Utc>,
}

pub(crate) struct Reply {
    pub context: read::Reply,
    pub catalog: Option<Catalog>,
}

pub(crate) struct Prepare {
    pub run: Run,
    pub access: Option<String>,
    pub route: Option<ReadRoute>,
    pub include: Vec<read::MaterialType>,
    pub cached: Vec<read::Version>,
    pub query: Option<Query>,
}

impl Prepare {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            !self.include.is_empty()
                && self.include.len() <= 3
                && self.include.iter().all(|value| matches!(
                    value,
                    read::MaterialType::Credential
                        | read::MaterialType::Bucket
                        | read::MaterialType::Policy
                ))
                && self
                    .include
                    .iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    == self.include.len(),
            "invalid request includes"
        );
        let text = |value: &str| {
            !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
        };
        ensure!(
            self.access.as_deref().is_none_or(text)
                && (!self.include.contains(&read::MaterialType::Credential)
                    || self.access.is_some()),
            "invalid request credential"
        );
        ensure!(
            self.route.is_some()
                || self
                    .include
                    .iter()
                    .all(|value| *value == read::MaterialType::Credential),
            "request bucket missing"
        );
        if let Some(route) = &self.route {
            ensure!(
                matches!(route, ReadRoute::Bucket { name, key, alias } if text(name) && key.is_empty() && alias.is_none()),
                "invalid request bucket"
            );
        }
        ensure!(
            self.cached.len() <= read::MAX_CACHED,
            "too many cached materials"
        );
        if let Some(query) = &self.query {
            ensure!(
                self.access.is_some() && self.include.contains(&read::MaterialType::Credential),
                "catalog credential missing"
            );
            let text = |value: &str| value.len() <= 1024;
            let token = |value: &str| value.len() <= 16 * 1024;
            let bucket =
                || self.route.is_some() && self.include.contains(&read::MaterialType::Bucket);
            ensure!(
                match query {
                    Query::Bucket => bucket(),
                    Query::Buckets {
                        prefix,
                        region,
                        expected_region,
                        token: cursor,
                        limit,
                    } =>
                        self.route.is_none()
                            && (1..=10000).contains(limit)
                            && text(prefix)
                            && text(expected_region)
                            && region.as_deref().is_none_or(text)
                            && cursor.as_deref().is_none_or(token),
                    Query::Objects {
                        prefix,
                        delimiter,
                        after,
                        token: cursor,
                        limit,
                    } =>
                        bucket()
                            && *limit <= 1000
                            && text(prefix)
                            && text(delimiter)
                            && after.as_deref().is_none_or(text)
                            && cursor.as_deref().is_none_or(token),
                },
                "invalid catalog query"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn request_preparation_bounds_materials_and_routes() -> Result<()> {
        let mut input = Prepare {
            run: Run {
                boot: Uuid::new_v4(),
                fence: Uuid::new_v4(),
            },
            access: Some("writer".into()),
            route: None,
            include: vec![read::MaterialType::Credential],
            cached: vec![],
            query: None,
        };
        input.validate()?;
        input.include.push(read::MaterialType::Credential);
        assert!(input.validate().is_err());
        input.include = vec![read::MaterialType::Bucket];
        assert!(input.validate().is_err());
        input.route = Some(ReadRoute::Bucket {
            name: "bucket".into(),
            key: String::new(),
            alias: None,
        });
        input.validate()?;
        input.include = vec![read::MaterialType::Key];
        assert!(input.validate().is_err());
        input.include = vec![read::MaterialType::Credential];
        input.access = None;
        assert!(input.validate().is_err());
        Ok(())
    }

    #[test]
    fn catalog_requires_current_credentials_and_bounded_pages() -> Result<()> {
        let mut input = Prepare {
            run: Run {
                boot: Uuid::new_v4(),
                fence: Uuid::new_v4(),
            },
            access: Some("reader".into()),
            route: None,
            include: vec![read::MaterialType::Credential],
            cached: vec![],
            query: Some(Query::Buckets {
                prefix: String::new(),
                region: None,
                expected_region: "us-east-1".into(),
                token: None,
                limit: 10000,
            }),
        };
        input.validate()?;
        if let Some(Query::Buckets { limit, .. }) = &mut input.query {
            *limit = 10001;
        }
        assert!(input.validate().is_err());
        input.query = Some(Query::Bucket);
        assert!(input.validate().is_err());
        input.route = Some(ReadRoute::Bucket {
            name: "bucket".into(),
            key: String::new(),
            alias: None,
        });
        input.include.push(read::MaterialType::Bucket);
        input.validate()?;
        input.include.remove(0);
        assert!(input.validate().is_err());
        Ok(())
    }
}
