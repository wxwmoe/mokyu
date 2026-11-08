use crate::{
    config,
    content::codec::Key,
    storage::{external, journal::source::Source, marker, model},
};
use anyhow::{Context, Result, ensure};
use object_store::{
    ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem, prefix::PrefixStore,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

pub(super) fn setting(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(v) => {
            ensure!(!v.is_empty(), "{name} is empty");
            Ok(Some(v))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(_) => anyhow::bail!("{name} must be UTF-8"),
    }
}

pub(super) fn physical(s: &str) -> Result<(&str, &str)> {
    let (bucket, prefix) = s
        .strip_prefix("s3://")
        .context("expected an s3:// URI")?
        .split_once('/')
        .unwrap_or((&s[5..], ""));
    ensure!(
        s3s::path::check_bucket_name(bucket),
        "invalid physical S3 bucket name"
    );
    Ok((bucket, prefix))
}

fn profile_entries(file: &Path, section: &str) -> Result<(bool, HashMap<String, String>)> {
    let input = match fs::read_to_string(file) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Default::default()),
        v => v.with_context(|| format!("cannot read AWS profile file {}", file.display()))?,
    };
    let (mut active, mut found) = (false, false);
    let mut items = HashMap::new();
    for line in input.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            active = line
                .strip_prefix('[')
                .and_then(|v| v.strip_suffix(']'))
                .is_some_and(|v| v.trim() == section);
            found |= active;
        } else if active
            && !line.starts_with([';', '#'])
            && let Some((k, v)) = line.split_once('=')
        {
            items.insert(k.trim().to_ascii_lowercase(), v.trim().to_owned());
        }
    }
    Ok((found, items))
}

pub(super) struct S3 {
    pub connection: external::Connection,
    pub access: String,
    pub secret: String,
    pub token: Option<String>,
}
impl S3 {
    pub fn load(
        side: &str,
        bucket: &str,
        prefix: &str,
        catalog: Option<&Location>,
    ) -> Result<Self> {
        let base = format!("MOKYU_DUMP_{side}");
        let get = |name: &str| setting(&format!("{base}_{name}"));
        let named = get("PROFILE")?;
        let default_profile = setting("AWS_PROFILE")?;
        let profile = named
            .as_ref()
            .or(default_profile.as_ref())
            .map_or("default", String::as_str);
        let home = env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_default()
            .join(".aws");
        let location =
            |var: &str, name: &str| env::var_os(var).map_or_else(|| home.join(name), PathBuf::from);
        let section = if profile == "default" {
            profile.into()
        } else {
            format!("profile {profile}")
        };
        let (has_config, mut cfg) =
            profile_entries(&location("AWS_CONFIG_FILE", "config"), &section)?;
        let (has_keys, keys) = profile_entries(
            &location("AWS_SHARED_CREDENTIALS_FILE", "credentials"),
            profile,
        )?;
        cfg.extend(keys);
        let explicit = [
            get("ACCESS_KEY_ID")?,
            get("SECRET_ACCESS_KEY")?,
            get("SESSION_TOKEN")?,
        ];
        let [access, secret, token] = if explicit.iter().any(Option::is_some) {
            explicit
        } else {
            if named.is_some() || default_profile.is_some() {
                ensure!(has_config || has_keys, "{base}_PROFILE was not found");
            }
            let environment = if named.is_none() {
                [
                    setting("AWS_ACCESS_KEY_ID")?,
                    setting("AWS_SECRET_ACCESS_KEY")?,
                    setting("AWS_SESSION_TOKEN")?,
                ]
            } else {
                Default::default()
            };
            if environment.iter().any(Option::is_some) {
                environment
            } else {
                [
                    cfg.remove("aws_access_key_id"),
                    cfg.remove("aws_secret_access_key"),
                    cfg.remove("aws_session_token"),
                ]
            }
        };
        let region = get("REGION")?
            .or_else(|| catalog.and_then(|l| l.region.clone()))
            .or_else(|| cfg.remove("region"))
            .or(setting("AWS_REGION")?)
            .or(setting("AWS_DEFAULT_REGION")?)
            .unwrap_or_else(|| "us-east-1".into());
        let endpoint_url = get("ENDPOINT")?
            .or_else(|| catalog.and_then(|l| l.endpoint.clone()))
            .or_else(|| cfg.remove("endpoint_url"))
            .or(setting("AWS_ENDPOINT_URL_S3")?)
            .or(setting("AWS_ENDPOINT_URL")?)
            .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
        let connection = external::Connection {
            endpoint_url,
            region,
            bucket: bucket.into(),
            prefix: prefix.into(),
            allow_http: true,
            virtual_hosted: false,
        };
        connection
            .validate()
            .with_context(|| format!("invalid {base}_ENDPOINT, region, bucket or prefix"))?;
        let access = access.with_context(|| format!("missing {base}_ACCESS_KEY_ID"))?;
        let secret = secret.with_context(|| format!("missing {base}_SECRET_ACCESS_KEY"))?;
        external::credentials(&access, &secret)
            .with_context(|| format!("invalid {base} credentials"))?;
        Ok(Self {
            connection,
            access,
            secret,
            token,
        })
    }
    pub fn store(&self) -> Result<Arc<dyn ObjectStore>> {
        let c = &self.connection;
        let mut builder = AmazonS3Builder::new()
            .with_endpoint(&c.endpoint_url)
            .with_region(&c.region)
            .with_allow_http(c.allow_http)
            .with_bucket_name(&c.bucket)
            .with_access_key_id(&self.access)
            .with_secret_access_key(&self.secret);
        if let Some(token) = &self.token {
            builder = builder.with_token(token);
        }
        Ok(Arc::new(PrefixStore::new(
            builder.build()?,
            object_store::path::Path::parse(&c.prefix)?,
        )))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct Location {
    pub uri: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
}
impl Location {
    pub fn normalize(s: &str) -> Result<String> {
        if s.starts_with("s3://") {
            let (b, p) = physical(s)?;
            let p = p.trim_end_matches('/');
            ensure!(
                object_store::path::Path::parse(p)?.as_ref() == p,
                "invalid source prefix"
            );
            Ok(if p.is_empty() {
                format!("s3://{b}")
            } else {
                format!("s3://{b}/{p}")
            })
        } else {
            ensure!(
                !s.contains("://"),
                "MOKYU_DUMP_SOURCE must be a local directory or s3:// URI"
            );
            let path = std::path::absolute(s)?;
            Ok(path
                .to_str()
                .context("source path must be UTF-8")?
                .to_owned())
        }
    }
}

pub(super) struct Backend {
    pub location: Location,
    pub store: Arc<dyn ObjectStore>,
    pub source: Source,
    pub deployment: Uuid,
}
impl Backend {
    pub async fn open(uri: &str, catalog: Option<&Location>) -> Result<Self> {
        let uri = Location::normalize(uri)?;
        let mut c = model::Connection {
            id: Uuid::nil(),
            r#type: "local".into(),
            path: None,
            endpoint_url: None,
            remote_bucket_name: None,
            region: None,
            prefix: String::new(),
            access: None,
            secret: None,
            identity_marker: serde_json::Value::Null,
        };
        let store: Arc<dyn ObjectStore> = if uri.starts_with("s3://") {
            let (bucket, prefix) = physical(&uri)?;
            let s3 = S3::load("SOURCE", bucket, prefix, catalog)?;
            c.r#type = "s3".into();
            c.prefix = prefix.into();
            c.region = Some(s3.connection.region.clone());
            c.remote_bucket_name = Some(bucket.into());
            c.endpoint_url = Some(s3.connection.endpoint_url.clone());
            s3.store()?
        } else {
            c.path = Some(uri.clone());
            Arc::new(
                LocalFileSystem::new_with_prefix(&uri).context("cannot open MOKYU_DUMP_SOURCE")?,
            )
        };
        let location = Location {
            uri,
            endpoint: c.endpoint_url.clone(),
            region: c.region.clone(),
        };
        let doc = marker::Document::read(&*store)
            .await
            .context("reading MOKYU_DUMP_SOURCE marker")?
            .context(
                "missing mokyu.json; select the backend root containing the metadata journal",
            )?;
        let marker = marker::identity(&doc.value)?;
        let journal = marker::journal(&doc.value)?.context("backend has no metadata journal")?;
        let key = match (
            setting("MOKYU_DUMP_DEPLOYMENT_KEY")?,
            setting("MOKYU_DUMP_DEPLOYMENT_KEY_FILE")?,
        ) {
            (Some(s), None) => s,
            (None, Some(file)) => fs::read_to_string(file)
                .context("cannot read MOKYU_DUMP_DEPLOYMENT_KEY_FILE")?
                .trim_end_matches(['\r', '\n'])
                .to_owned(),
            _ => anyhow::bail!(
                "set exactly one of MOKYU_DUMP_DEPLOYMENT_KEY and MOKYU_DUMP_DEPLOYMENT_KEY_FILE"
            ),
        };
        let cipher = Key::new(
            "aes-256-gcm",
            config::file::deployment_key(&key).context("invalid MOKYU_DUMP_DEPLOYMENT_KEY")?,
        )?;
        c.id = marker.storage_id;
        c.identity_marker = doc.value;
        let material = journal
            .unlock(c.id, &cipher)
            .context("MOKYU_DUMP_DEPLOYMENT_KEY cannot unlock this backend")?
            .material;
        Ok(Self {
            location,
            store,
            source: Source {
                connection: c,
                journal,
                material,
            },
            deployment: marker.deployment_id,
        })
    }
    pub async fn verify(&self) -> Result<()> {
        let doc = marker::Document::read(&*self.store)
            .await?
            .context("storage marker disappeared")?;
        marker::verify(&doc.value, &self.source.connection.identity_marker)
    }
}
