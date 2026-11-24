use anyhow::{Result, ensure};
use serde::Deserialize;
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Sources {
    #[serde(rename = "type")]
    format: String,
    pub namespace: Uuid,
    pub cluster: Option<Uuid>,
    pub manifest_digest: Option<String>,
    pub publication_digest: Option<String>,
    pub backup_group: Option<String>,
    pub sources: Vec<Entry>,
}

#[derive(Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub(super) enum Entry {
    Directory {
        path: PathBuf,
    },
    Remote {
        url: String,
        credential_file: PathBuf,
    },
}

impl Sources {
    pub fn read(path: &Path) -> Result<Self> {
        let mut input = String::new();
        File::open(path)?
            .take(256 * 1024 + 1)
            .read_to_string(&mut input)?;
        ensure!(input.len() <= 256 * 1024, "source manifest is too large");
        let sources: Self = toml::from_str(&input)
            .map_err(|_| anyhow::anyhow!("invalid cluster source manifest"))?;
        ensure!(
            sources.format == "storage_cluster"
                && !sources.namespace.is_nil()
                && sources.cluster.is_none_or(|id| !id.is_nil()),
            "invalid cluster source identity"
        );
        ensure!(
            !sources.sources.is_empty(),
            "expected at least one source entry"
        );
        if let Some(group) = &sources.backup_group {
            crate::config::storage::label(group, "backup_group")?;
        }
        for digest in [&sources.manifest_digest, &sources.publication_digest]
            .into_iter()
            .flatten()
        {
            ensure!(
                digest.len() == 64
                    && digest
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                "invalid recovery digest"
            );
        }
        for entry in &sources.sources {
            match entry {
                Entry::Directory { path } => {
                    ensure!(path.is_absolute(), "media path must be absolute")
                }
                Entry::Remote {
                    url,
                    credential_file,
                } => {
                    crate::config::storage::url(url)?;
                    ensure!(
                        credential_file.is_absolute(),
                        "credential_file must be absolute"
                    );
                }
            }
        }
        Ok(sources)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn accepts_mixed_sources_and_rejects_ambiguous_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().display().to_string().replace('\\', "/");
        let parse = |entries: &str| {
            let mut tmp = tempfile::NamedTempFile::new().unwrap();
            writeln!(
                tmp,
                "type='storage_cluster'\nnamespace='{}'\nsources=[{}]",
                Uuid::now_v7(),
                entries
            )
            .unwrap();
            Sources::read(tmp.path())
        };
        let remote = format!("url='https://node.test:9003', credential_file='{path}/token'");
        assert!(parse(&format!("{{path='{path}'}},{{{remote}}}")).is_ok());
        assert!(parse(&format!("{{path='{path}',{remote}}}")).is_err());
        assert!(parse("{url='http://node.test',credential_file='/token'}").is_err());
        assert!(parse("").is_err());
    }
}
