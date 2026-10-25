use super::*;
use crate::node::body::Backend;
use futures_util::TryStreamExt;

impl Worker {
    pub(super) async fn scan(
        &self,
        backend: Arc<Backend>,
        after: Option<String>,
        limit: usize,
        bytes: Option<u64>,
    ) -> Result<work::Outcome> {
        ensure!(
            (1..=work::SCAN_ITEMS).contains(&limit),
            "invalid sweep size"
        );
        let _io = self.reader.io.clone().acquire_owned().await?;
        backend.store.verify().await?;
        let after = Path::parse(after.unwrap_or_default())?;
        let mut listing = backend.store.backend.list_with_offset(None, &after);
        let mut items = vec![];
        let mut used = 0u64;
        while items.len() < limit {
            let Some(row) = listing.try_next().await? else {
                break;
            };
            if !items.is_empty() && bytes.is_some_and(|limit| used.saturating_add(row.size) > limit)
            {
                break;
            }
            used = used.checked_add(row.size).context("sweep size overflow")?;
            items.push(work::ListedObject {
                path: row.location.to_string(),
                size: row.size,
                modified: row.last_modified,
            });
        }
        Ok(work::Outcome::Listed { items })
    }

    pub(super) async fn prune(
        &self,
        backend: Arc<Backend>,
        before: chrono::DateTime<chrono::Utc>,
        items: Vec<work::ListedObject>,
    ) -> Result<Vec<work::Outcome>> {
        ensure!(items.len() <= work::SCAN_ITEMS, "invalid sweep size");
        let _io = self.reader.io.clone().acquire_owned().await?;
        backend.store.verify().await?;
        let mut outcomes = Vec::with_capacity(items.len());
        for item in items {
            ensure!(
                self.node.subscribe().borrow().maintenance,
                "sweep requires maintenance"
            );
            ensure!(
                crate::storage::Storage::parse_physical_path(&item.path).is_some(),
                "invalid sweep path"
            );
            let path = Path::parse(&item.path)?;
            let result = async {
                let meta = match backend.store.backend.head(&path).await {
                    Ok(meta) => meta,
                    Err(object_store::Error::NotFound { .. }) => {
                        return Ok(work::Outcome::Deleted { stored_at: None });
                    }
                    Err(error) => return Err(anyhow::Error::from(error)),
                };
                if meta.last_modified > before {
                    return Ok(work::Outcome::Retained {
                        stored_at: meta.last_modified,
                    });
                }
                match backend.store.backend.delete(&path).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                        Ok(work::Outcome::Deleted {
                            stored_at: Some(meta.last_modified),
                        })
                    }
                    Err(error) => Err(error.into()),
                }
            }
            .await;
            outcomes.push(result.unwrap_or(work::Outcome::Retry));
        }
        #[cfg(feature = "fault-injection")]
        crate::faults::point("node-sweep-deleted").await;
        Ok(outcomes)
    }
}
