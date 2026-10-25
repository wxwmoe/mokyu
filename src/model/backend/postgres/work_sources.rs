use super::*;
use crate::model::{
    PhysicalSource,
    source::{SourceOperation, SourceUpdate},
};

async fn validate(
    tx: &mut Transaction<'_, Postgres>,
    execution: Execution,
    operation: &SourceOperation,
) -> Result<Option<&'static str>> {
    match source_update_in(
        tx,
        SourceUpdate {
            execution,
            operation,
            apply: false,
        },
    )
    .await
    {
        Ok(()) => Ok(None),
        Err(error)
            if error
                .downcast_ref::<Error>()
                .is_some_and(|e| matches!(e, Error::OperationAborted)) =>
        {
            Ok(Some("source_changed"))
        }
        Err(error)
            if error
                .downcast_ref::<s3s::S3Error>()
                .is_some_and(|e| matches!(e.code(), s3s::S3ErrorCode::AccessDenied)) =>
        {
            Ok(Some("access_denied"))
        }
        Err(error) => Err(error),
    }
}

pub(super) async fn prepare(
    tx: &mut Transaction<'_, Postgres>,
    node: &NodeState,
    execution: Execution,
    detail: &Value,
    profile: &crate::storage::profile::StorageProfile,
    key: &Key,
) -> Result<Option<(work::Plan, Vec<read::BlockKey>)>> {
    let operation: SourceOperation = serde_json::from_value(detail["source_operation"].clone())?;
    ensure!(
        operation.node.is_none_or(|owner| owner == node.node_id),
        Error::OperationAborted
    );
    if let Some(error) = validate(tx, execution, &operation).await? {
        q::finish(execution.id, execution.token, 0, true, Some(error))
            .execute(&mut **tx)
            .await?;
        return Ok(None);
    }
    let snapshot = operation.snapshot;
    ensure!(
        snapshot.scope.storage_id == profile.storage_id,
        Error::OperationAborted
    );
    let inspect = operation.code.is_none() || detail["operation"] == "verify";
    let (bytes, block) = match &snapshot.physical {
        PhysicalSource::Chunk(chunk) => (i64::from(chunk.raw_size), chunk.block_key_id),
        PhysicalSource::Pack(pack) => (pack.raw_size, pack.block_key_id),
        PhysicalSource::Raw(raw) => {
            ensure!(
                raw.medium == "shared_s3" && !raw.pending_upload,
                Error::SourceUnavailable
            );
            (raw.size, None)
        }
    };
    if inspect {
        let mut rates = vec![];
        if let Some(rate) = profile.integrity_rps {
            rates.push(("integrity_rps", rate as f64, 2.0));
        }
        if let Some(rate) = profile
            .integrity_bandwidth
            .as_deref()
            .map(crate::config::bytes)
            .transpose()?
        {
            rates.push(("integrity_bandwidth", rate as f64, bytes.max(0) as f64));
        }
        if !integrity_rate_in(tx, profile.storage_id, &rates).await? {
            return Ok(None);
        }
    }
    let members = if let PhysicalSource::Pack(pack) = &snapshot.physical {
        let members = q::members(pack.id).fetch_all(&mut **tx).await?;
        ensure!(
            Some(crate::model::source::manifest_digest(pack, &members)?)
                == snapshot.manifest_digest,
            Error::OperationAborted
        );
        members
    } else {
        vec![]
    };
    let mut keys = vec![];
    if inspect && let Some(id) = block {
        let record = queries::node_read_keys(&[id]).fetch_one(&mut **tx).await?;
        keys.push(read::BlockKey::unprotect(record, key)?);
    }
    Ok(Some((
        work::Plan::Inspect {
            location: snapshot.location(),
            snapshot: Box::new(snapshot),
            members,
            inspect,
        },
        keys,
    )))
}

pub(super) async fn finish(
    tx: &mut Transaction<'_, Postgres>,
    execution: Execution,
    detail: Value,
    snapshot: crate::model::source::SourceSnapshot,
    inspect: bool,
    outcomes: &[work::Outcome],
) -> Result<Option<&'static str>> {
    ensure!(outcomes.len() == 1, Error::NodeOperationConflict);
    let mut operation: SourceOperation =
        serde_json::from_value(detail["source_operation"].clone())?;
    ensure!(
        operation.snapshot.fingerprint()? == snapshot.fingerprint()?,
        Error::NodeOperationConflict
    );
    if let Some(error) = validate(tx, execution, &operation).await? {
        return Ok(Some(error));
    }
    let code = match &outcomes[0] {
        work::Outcome::Retry => return Ok(Some("backend_io_failed")),
        work::Outcome::Inspected { code } => code,
        _ => return Err(Error::NodeOperationConflict.into()),
    };
    ensure!(
        code.as_deref()
            .is_none_or(crate::model::source::quarantine_code),
        Error::NodeOperationConflict
    );
    if !inspect {
        ensure!(code.is_none(), Error::NodeOperationConflict);
    } else if detail["operation"] == "verify" {
        let Some(code) = code else {
            return Ok(None);
        };
        operation.code = Some(code.clone());
    } else if code.is_some() {
        return Ok(Some("source_verification_failed"));
    }
    source_update_in(
        tx,
        SourceUpdate {
            execution,
            operation: &operation,
            apply: true,
        },
    )
    .await?;
    Ok(None)
}
