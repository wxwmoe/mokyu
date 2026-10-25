use super::{ReadRoute, node_read as read, session::Run};
use anyhow::{Result, ensure};

pub(crate) struct Prepare {
    pub run: Run,
    pub access: Option<String>,
    pub route: Option<ReadRoute>,
    pub include: Vec<read::MaterialType>,
    pub cached: Vec<read::Version>,
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
}
