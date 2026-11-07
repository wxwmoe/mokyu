use crate::content::codec::{self, Key};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion, path::Path};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

const MAX_BYTES: usize = 16 * 1024;
const VERSION: u32 = 6;

#[derive(Deserialize, Serialize)]
pub(crate) struct Identity {
    format_version: u32,
    pub deployment_id: Uuid,
    pub storage_id: Uuid,
    created_at: Option<DateTime<Utc>>,
    created_by: Option<String>,
}

#[derive(Clone, Deserialize, Serialize, PartialEq)]
pub(crate) struct Journal {
    pub journal_id: Uuid,
    pub writer_epoch: Uuid,
    wrapped_journal_key: String,
}

impl Journal {
    fn binding(&self, storage: Uuid) -> Vec<u8> {
        let mut binding = b"mokyu/journal-key/v6".to_vec();
        for id in [storage, self.journal_id, self.writer_epoch] {
            binding.extend_from_slice(id.as_bytes());
        }
        binding
    }

    pub fn create(storage: Uuid, deployment_key: &Key) -> Result<Self> {
        let mut journal = Self {
            journal_id: Uuid::new_v4(),
            writer_epoch: Uuid::new_v4(),
            wrapped_journal_key: String::new(),
        };
        let mut material = [0; 32];
        aws_lc_rs::rand::fill(&mut material)
            .map_err(|_| anyhow::anyhow!("journal key generation failed"))?;
        journal.wrap(storage, deployment_key, &material)?;
        Ok(journal)
    }

    fn wrap(&mut self, storage: Uuid, key: &Key, material: &[u8; 32]) -> Result<()> {
        self.wrapped_journal_key =
            URL_SAFE_NO_PAD.encode(codec::protect(material, key, &self.binding(storage))?);
        Ok(())
    }

    pub fn unlock(&self, storage: Uuid, key: &Key) -> Result<Key> {
        let bytes = URL_SAFE_NO_PAD.decode(&self.wrapped_journal_key)?;
        let plain = codec::unprotect_bytes(&bytes, key, &self.binding(storage))?;
        Key::new(
            "aes-256-gcm",
            plain
                .try_into()
                .ok()
                .context("invalid journal key length")?,
        )
    }

    fn parse(value: &Value) -> Result<Self> {
        let journal: Self =
            serde_json::from_value(value.clone()).context("invalid metadata journal")?;
        ensure!(
            !journal.journal_id.is_nil() && !journal.writer_epoch.is_nil(),
            "invalid journal identity"
        );
        ensure!(
            URL_SAFE_NO_PAD.decode(&journal.wrapped_journal_key)?.len() == 60,
            "invalid wrapped journal key"
        );
        Ok(journal)
    }
}

pub(crate) fn create(deployment_id: Uuid, storage_id: Uuid) -> Result<Value> {
    Ok(serde_json::to_value(Identity {
        format_version: VERSION,
        deployment_id,
        storage_id,
        created_at: Some(Utc::now()),
        created_by: Some(format!(
            "{}/{}",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION")
        )),
    })?)
}

pub(crate) fn identity(value: &Value) -> Result<Identity> {
    let version = value
        .get("format_version")
        .and_then(Value::as_u64)
        .context("invalid storage marker version")?;
    ensure!(
        version == u64::from(VERSION),
        "unsupported storage marker version {version}"
    );
    let marker: Identity =
        serde_json::from_value(value.clone()).context("invalid storage identity marker")?;
    ensure!(
        !marker.storage_id.is_nil() && !marker.deployment_id.is_nil(),
        "invalid storage identity"
    );
    Ok(marker)
}

pub(crate) fn journal(value: &Value) -> Result<Option<Journal>> {
    value
        .get("metadata_journal")
        .map(Journal::parse)
        .transpose()
}

pub(crate) fn verify(actual: &Value, expected: &Value) -> Result<()> {
    let left = identity(actual)?;
    let right = identity(expected)?;
    ensure!(
        left.deployment_id == right.deployment_id
            && left.storage_id == right.storage_id
            && journal(actual)? == journal(expected)?,
        "storage mokyu.json does not match this store"
    );
    Ok(())
}

pub(crate) struct Document {
    pub value: Value,
    version: UpdateVersion,
}

impl Document {
    pub fn revision(&self) -> (Option<String>, Option<String>) {
        (self.version.e_tag.clone(), self.version.version.clone())
    }

    pub async fn read(backend: &dyn ObjectStore) -> Result<Option<Self>> {
        let object = match backend.get(&Path::from("mokyu.json")).await {
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            other => other.context("cannot read storage mokyu.json")?,
        };
        ensure!(
            object.meta.size <= MAX_BYTES as u64,
            "storage mokyu.json exceeds limit"
        );
        let version = UpdateVersion {
            e_tag: object.meta.e_tag.clone(),
            version: object.meta.version.clone(),
        };
        let mut body = Vec::with_capacity(object.meta.size as usize);
        let mut stream = object.into_stream();
        while let Some(chunk) = stream.try_next().await? {
            ensure!(
                chunk.len() <= MAX_BYTES - body.len(),
                "storage mokyu.json exceeds limit"
            );
            body.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&body).context("invalid storage mokyu.json")?;
        identity(&value)?;
        Ok(Some(Self { value, version }))
    }

    pub async fn probe(backend: &dyn ObjectStore) -> Result<Option<Self>> {
        if let Some(doc) = Self::read(backend).await? {
            if journal(&doc.value)?.is_none() {
                let prefix = Path::from(crate::storage::journal::ROOT);
                ensure!(
                    backend.list(Some(&prefix)).try_next().await?.is_none(),
                    "unclaimed metadata journal"
                );
            }
            return Ok(Some(doc));
        }
        ensure!(
            backend.list(None).try_next().await?.is_none(),
            crate::model::InputDetail::storage(
                "storage namespace is not empty",
                s3s::S3ErrorCode::OperationAborted
            )
        );
        Ok(None)
    }

    pub async fn set_journal(
        mut self,
        backend: &dyn ObjectStore,
        descriptor: &Journal,
    ) -> Result<Value> {
        journal(&self.value)?;
        ensure!(
            self.version.e_tag.is_some() || self.version.version.is_some(),
            "storage marker has no revision"
        );
        self.value["format_version"] = VERSION.into();
        let fields = self
            .value
            .as_object_mut()
            .context("invalid storage mokyu.json")?
            .entry("metadata_journal")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .context("invalid journal descriptor")?;
        let Value::Object(known) = serde_json::to_value(descriptor)? else {
            unreachable!()
        };
        fields.extend(known);
        let body = serde_json::to_vec(&self.value)?;
        ensure!(body.len() <= MAX_BYTES, "storage mokyu.json exceeds limit");
        backend
            .put_opts(
                &Path::from("mokyu.json"),
                Bytes::from(body).into(),
                PutOptions {
                    mode: PutMode::Update(self.version),
                    ..Default::default()
                },
            )
            .await?;
        Ok(self.value)
    }
}

pub(crate) async fn verify_store(backend: &dyn ObjectStore, expected: &Value) -> Result<()> {
    let doc = Document::read(backend)
        .await?
        .context("storage mokyu.json is required; restore it before using this store")?;
    verify(&doc.value, expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use serde_json::json;

    async fn put(store: &InMemory, path: &str, value: &Value) {
        store
            .put(&Path::from(path), serde_json::to_vec(value).unwrap().into())
            .await
            .unwrap();
    }

    #[test]
    fn minimal_marker_accepts_extensions_and_requires_identity() {
        let value = json!({"format_version": 6, "deployment_id": Uuid::new_v4(),
            "storage_id": Uuid::new_v4(), "extension": {"anything": true}});
        assert!(identity(&value).is_ok());
        for field in ["format_version", "deployment_id", "storage_id"] {
            let mut missing = value.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(identity(&missing).is_err());
        }
        for version in [5, 7] {
            let mut other = value.clone();
            other["format_version"] = version.into();
            assert!(identity(&other).is_err());
        }
    }

    #[tokio::test]
    async fn an_old_namespace_is_preserved() {
        let store = InMemory::new();
        let legacy = json!({"format_version": 5, "deployment_id": Uuid::new_v4(), "storage_id": Uuid::new_v4()});
        put(&store, "meta.json", &legacy).await;
        put(
            &store,
            "raw/89/019a111122227abc8def000123456789",
            &json!("preserved"),
        )
        .await;
        assert!(Document::probe(&store).await.is_err());
        assert!(Document::read(&store).await.unwrap().is_none());
        assert_eq!(
            store
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn probe_is_read_only_and_marker_updates_preserve_extensions() {
        let store = InMemory::new();
        let sid = Uuid::from_u128(2);
        let mut marker = create(Uuid::from_u128(1), sid).unwrap();
        assert!(Document::probe(&store).await.unwrap().is_none());
        assert!(store.list(None).try_next().await.unwrap().is_none());
        put(&store, "occupied", &json!(0)).await;
        assert!(Document::probe(&store).await.is_err());
        assert!(Document::read(&store).await.unwrap().is_none());
        marker["future"] = json!({"owner":"test"});
        put(&store, "mokyu.json", &marker).await;
        let key = Key::new("aes-256-gcm", [3; 32]).unwrap();
        let journal = Journal::create(sid, &key).unwrap();
        let original = journal.unlock(sid, &key).unwrap();
        assert!(journal.unlock(Uuid::new_v4(), &key).is_err());
        assert!(
            journal
                .unlock(sid, &Key::new("aes-256-gcm", [4; 32]).unwrap())
                .is_err()
        );
        let stale = Document::probe(&store).await.unwrap().unwrap();
        let current = Document::probe(&store).await.unwrap().unwrap();
        let mut next = current.set_journal(&store, &journal).await.unwrap();
        assert_eq!(next["future"], marker["future"]);
        assert!(stale.set_journal(&store, &journal).await.is_err());
        next["metadata_journal"]["future"] = json!(["retained"]);
        put(&store, "mokyu.json", &next).await;
        let mut rotated = journal;
        rotated.writer_epoch = Uuid::new_v4();
        rotated.wrap(sid, &key, &original.material).unwrap();
        let current = Document::probe(&store).await.unwrap().unwrap();
        let updated = current.set_journal(&store, &rotated).await.unwrap();
        assert_eq!(updated["metadata_journal"]["future"], json!(["retained"]));
        let mut diagnostic = updated.clone();
        diagnostic["created_by"] = json!("extension");
        verify(&diagnostic, &updated).unwrap();
        diagnostic["deployment_id"] = json!(Uuid::new_v4());
        assert!(verify(&diagnostic, &updated).is_err());
        for (field, value) in [
            ("journal_id", json!(Uuid::nil())),
            ("writer_epoch", json!(Uuid::nil())),
            ("wrapped_journal_key", json!("wrong")),
        ] {
            let mut invalid = updated.clone();
            invalid["metadata_journal"][field] = value;
            put(&store, "mokyu.json", &invalid).await;
            assert!(Document::probe(&store).await.is_err());
        }
        marker["format_version"] = json!(7);
        put(&store, "mokyu.json", &marker).await;
        assert!(Document::probe(&store).await.is_err());
        marker["format_version"] = json!(4);
        put(&store, "mokyu.json", &marker).await;
        put(&store, "metadata/index", &json!(false)).await;
        assert!(Document::probe(&store).await.is_err());
    }
}
