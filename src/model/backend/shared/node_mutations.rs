use super::{
    Backend,
    queries::{node_receipts as q, node_registry as registry, node_requests as requests},
};
use crate::{
    authorization::Principal,
    cluster::NodeState,
    content::codec::Key,
    model::{
        Error, ReadRoute,
        node_object::{Outcome, Request},
    },
};
use anyhow::{Result, ensure};
use uuid::Uuid;

async fn start<'a, T: serde::de::DeserializeOwned>(
    b: &'a Backend,
    node: &NodeState,
    key: &Key,
    run: crate::model::session::Run,
    operation: Uuid,
    action: &str,
    hash: &[u8],
) -> Result<(registry::Tx<'a>, Option<T>)> {
    let mut tx = registry::begin(b).await?;
    ensure!(
        matches!(node.role.as_str(), "data" | "node"),
        Error::NodeAuthentication
    );
    super::node_registry::require_work(b, &mut tx, node, key, run).await?;
    if let Some((stored, generation, previous, result)) = q::stored(node.node_id, operation)
        .fetch_optional(&mut *tx)
        .await?
    {
        ensure!(
            stored == action && previous == hash,
            Error::NodeOperationConflict
        );
        ensure!(generation == node.credential_generation, Error::NodeExpired);
        return Ok((tx, Some(serde_json::from_value(result)?)));
    }
    let issued = crate::model::node_write::operation_time(operation)?;
    let now = chrono::Utc::now();
    ensure!(
        issued <= now + chrono::Duration::seconds(30)
            && issued >= now - chrono::Duration::minutes(15),
        Error::NodeOperationExpired
    );
    requests::admit(b, &mut tx, node, key, run).await?;
    Ok((tx, None))
}

pub(super) async fn change_objects(
    b: &Backend,
    node: &NodeState,
    key: &Key,
    input: Request,
) -> Result<Outcome> {
    let hash = input.validate()?;
    let (mut tx, previous) =
        start::<Outcome>(b, node, key, input.run, input.operation, "objects", &hash).await?;
    if let Some(outcome) = previous {
        return Ok(outcome);
    }
    let mut change = input.change;
    let route = ReadRoute::Bucket {
        name: change.bucket,
        key: String::new(),
        alias: None,
    };
    let bucket = requests::node_read_bucket(&route)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    let principal = Principal::Service {
        access_key: change.access,
        revision: change.credential_revision,
    };
    let condition = change
        .if_match
        .as_ref()
        .map(|s| s3s::dto::ETagCondition::parse_http_header(s.as_bytes()))
        .transpose()?;
    change.keys.sort_unstable();
    change.keys.dedup();
    let mut outcome = Outcome::Applied;
    for key in change.keys {
        let item = crate::model::ObjectChange {
            principal: principal.clone(),
            bucket: bucket.id,
            key,
            expected: None,
            public: change.public,
            if_match: condition.clone(),
        };
        match super::transactions::change_object_in(&mut tx, item).await {
            Ok(_) => {}
            Err(e)
                if e.downcast_ref::<s3s::S3Error>()
                    .is_some_and(|s| s.code() == &s3s::S3ErrorCode::NoSuchKey) =>
            {
                outcome = Outcome::NoSuchKey
            }
            Err(e)
                if e.downcast_ref::<s3s::S3Error>()
                    .is_some_and(|s| s.code() == &s3s::S3ErrorCode::PreconditionFailed) =>
            {
                outcome = Outcome::PreconditionFailed
            }
            Err(e) => return Err(e),
        }
    }
    q::save(
        node,
        input.operation,
        "objects",
        &hash,
        serde_json::to_value(outcome)?,
    )
    .execute(&mut *tx)
    .await?;
    super::node_registry::validate_run(&mut tx, node, input.run).await?;
    tx.commit().await?;
    Ok(outcome)
}

pub(super) async fn multipart_operation(
    b: &Backend,
    node: &NodeState,
    key: &Key,
    input: crate::model::node_multipart::Request,
    max_active: Option<i64>,
) -> Result<crate::model::node_multipart::Outcome> {
    use crate::{
        authorization::{Action, Permit},
        model::{
            multipart::MultipartCreate,
            node_multipart::{Operation, Outcome},
        },
    };
    let hash = input.validate()?;
    let (mut tx, previous) =
        start::<Outcome>(b, node, key, input.run, input.operation, "multipart", &hash).await?;
    if let Some(outcome) = previous {
        return Ok(outcome);
    }
    let intent = input.intent;
    let route = ReadRoute::Bucket {
        name: intent.bucket,
        key: String::new(),
        alias: None,
    };
    let bucket = requests::node_read_bucket(&route)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    let authority = Permit::for_action(
        Principal::Service {
            access_key: intent.access.clone(),
            revision: intent.credential_revision,
        },
        bucket.id,
        Action::Write,
    );
    let outcome = match intent.operation {
        Operation::Create {
            metadata,
            public,
            algorithm,
            checksum_type,
        } => {
            let (upload, _) = super::transactions::create_multipart_in(
                &mut tx,
                MultipartCreate {
                    authority: &authority,
                    bucket: bucket.id,
                    key: &intent.key,
                    access: &intent.access,
                    metadata,
                    public,
                    algorithm,
                    checksum_type,
                    web: None,
                    max_active,
                },
            )
            .await?;
            Outcome::Created { upload }
        }
        Operation::Abort { upload, initiated } => {
            super::transactions::lock_permit(&mut tx, &authority).await?;
            let owned = requests::multipart_upload(upload, bucket.id, &intent.key, &intent.access)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| s3s::s3_error!(NoSuchUpload))?;
            if let Some(value) = initiated {
                ensure!(
                    s3s::dto::Timestamp::parse(s3s::dto::TimestampFormat::HttpDate, &value)?
                        == crate::http::gateway::stamp(owned.created_at),
                    s3s::s3_error!(PreconditionFailed)
                );
            }
            super::transactions::abort_multipart_in(&mut tx, &authority, upload, false).await?;
            Outcome::Aborted
        }
    };
    q::save(
        node,
        input.operation,
        "multipart",
        &hash,
        serde_json::to_value(&outcome)?,
    )
    .execute(&mut *tx)
    .await?;
    super::node_registry::validate_run(&mut tx, node, input.run).await?;
    tx.commit().await?;
    Ok(outcome)
}
