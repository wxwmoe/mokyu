use super::queries::{storage_cluster::Tx, upload_staging as q};
use crate::model::{Error, multipart::Staging};
use anyhow::{Result, ensure};
use uuid::Uuid;

#[cfg(test)]
#[path = "../../../tests/recovery/upload_staging.rs"]
mod tests;

pub(super) async fn pin(tx: &mut Tx<'_>, upload: Uuid, bucket: Uuid) -> Result<()> {
    let (mode, settings, revision, backend) = q::configuration(bucket).fetch_one(&mut **tx).await?;
    let storage = (mode != "single").then(|| settings.staging_storage_id.unwrap_or(backend));
    if let Some(id) = storage {
        ensure!(
            q::valid(id).fetch_one(&mut **tx).await?,
            Error::SourceUnavailable
        );
    }
    let value = Staging {
        storage_id: storage,
        policy_revision: revision,
        tail_upload: if storage.is_some() {
            settings.tail_upload.clone()
        } else {
            "immediate".into()
        },
        tail_wait_seconds: i64::try_from(crate::config::seconds(&settings.tail_upload_timeout)?)?,
    };
    q::insert(upload, &value).execute(&mut **tx).await?;
    Ok(())
}

pub(super) async fn claim_tail(
    tx: &mut Tx<'_>,
    node: Uuid,
    token: Uuid,
) -> Result<Option<crate::model::node_native::Work>> {
    use crate::model::node_native::{Action, Target, Work};
    let Some((fragment, upload, storage, source, size, hash)) =
        q::due(node, chrono::Utc::now().timestamp_micros())
            .fetch_optional(&mut **tx)
            .await?
    else {
        return Ok(None);
    };
    ensure!(
        q::claim(
            fragment,
            token,
            chrono::Utc::now().timestamp_micros() + 60_000_000
        )
        .execute(&mut **tx)
        .await?
        .rows_affected()
            == 1,
        Error::NodeOperationConflict
    );
    Ok(Some(Work {
        upload: Target {
            upload,
            storage,
            source,
            upstream: None,
        },
        action: Action::Tail {
            fragment,
            size,
            hash,
        },
    }))
}

pub(super) async fn record_owner(
    tx: &mut Tx<'_>,
    value: &crate::model::StreamTail,
    fragment: crate::model::Id,
    owner: Option<(Uuid, Uuid)>,
) -> Result<()> {
    if let Some((upload, node)) = owner {
        ensure!(
            value.staging_storage.is_none() && value.file.is_none(),
            Error::NodeOperationConflict
        );
        let path = crate::storage::fragment_key(value.fragment);
        q::add_tail(
            fragment,
            upload,
            node,
            &path,
            chrono::Utc::now().timestamp_micros(),
        )
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(super) async fn request(
    tx: &mut Tx<'_>,
    upload: Uuid,
    local: Option<Uuid>,
    parts: &[i32],
) -> Result<Vec<Uuid>> {
    let mut owners = std::collections::BTreeSet::new();
    for (id, node) in q::owners(upload, local, parts).fetch_all(&mut **tx).await? {
        q::request(id).execute(&mut **tx).await?;
        owners.insert(node);
    }
    Ok(owners.into_iter().collect())
}

pub(super) async fn finish_tail(
    tx: &mut Tx<'_>,
    work: &crate::model::node_native::Work,
    token: Uuid,
    result: &crate::model::node_native::ResultValue,
) -> Result<u64> {
    let crate::model::node_native::Action::Tail { fragment, .. } = work.action else {
        return Err(Error::NodeOperationConflict.into());
    };
    ensure!(
        result.retry == result.value.is_none(),
        Error::NodeOperationConflict
    );
    if let Some(receipt) = &result.value {
        ensure!(
            Uuid::parse_str(receipt)? == work.upload.source,
            Error::NodeOperationConflict
        );
        let path = crate::storage::fragment_key(work.upload.source);
        ensure!(
            q::synced(work.upload.storage, &path, fragment, token)
                .execute(&mut **tx)
                .await?
                .rows_affected()
                == 1,
            Error::NodeExpired
        );
        Ok(q::finish_tail(fragment, token)
            .execute(&mut **tx)
            .await?
            .rows_affected())
    } else {
        Ok(q::delay_tail(
            result.source_error.map(|e| e.code()),
            chrono::Utc::now().timestamp_micros() + 5_000_000,
            fragment,
            token,
        )
        .execute(&mut **tx)
        .await?
        .rows_affected())
    }
}
