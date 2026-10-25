use super::{read, wire};
use crate::model::node_multipart::{Intent, Operation, Outcome, Request};
use tonic::Status;

fn invalid() -> Status {
    Status::invalid_argument("invalid multipart request")
}

pub(super) fn begin_complete(
    input: wire::BeginCompleteRequest,
) -> Result<crate::model::node_write::Begin, Status> {
    if prost::Message::encoded_len(&input) > crate::model::node_complete::REQUEST_LIMIT {
        return Err(Status::resource_exhausted(
            "completion request exceeds message budget",
        ));
    }
    let intent = crate::model::node_write::Intent {
        access: input.access_key,
        credential_revision: input
            .credential_revision
            .ok_or_else(invalid)?
            .try_into()
            .map_err(|_| invalid())?,
        bucket: input.bucket,
        key: input.key,
        metadata: serde_json::json!({}),
        public: false,
        size: None,
        copy_read: None,
        part: None,
        completion: Some(serde_json::from_slice(&input.manifest_json).map_err(|_| invalid())?),
    };
    intent.validate().map_err(|error| {
        if error.downcast_ref::<s3s::S3Error>().is_some() {
            super::server::failure(error)
        } else {
            invalid()
        }
    })?;
    let operation = super::uuid(&input.operation_id)?;
    crate::model::node_write::operation_time(operation).map_err(|_| invalid())?;
    Ok(crate::model::node_write::Begin {
        run: read::run(input.run)?,
        operation,
        intent,
        cached: read::cached(input.cached)?,
    })
}

pub(super) fn sources(
    input: wire::ReadCompleteRequest,
) -> Result<crate::model::node_complete::Sources, Status> {
    if prost::Message::encoded_len(&input) > read::REQUEST_LIMIT
        || input.read_operations.is_empty()
        || input.read_operations.len() > crate::model::node_complete::SOURCES
        || input.shared_permits.len() > 16
        || input.first >= 10000
    {
        return Err(invalid());
    }
    Ok(crate::model::node_complete::Sources {
        run: read::run(input.run)?,
        operation: super::uuid(&input.operation_id)?,
        first: input.first as i32,
        reads: input
            .read_operations
            .iter()
            .map(|value| super::uuid(value))
            .collect::<Result<_, _>>()?,
        cached: read::cached(input.cached)?,
        shared_permits: input
            .shared_permits
            .iter()
            .map(|value| super::uuid(value))
            .collect::<Result<_, _>>()?,
    })
}

pub(super) fn status(
    input: wire::CompleteStatusRequest,
) -> Result<crate::model::node_complete::Lookup, Status> {
    if prost::Message::encoded_len(&input) > read::REQUEST_LIMIT {
        return Err(invalid());
    }
    let value = crate::model::node_complete::Lookup {
        run: read::run(input.run)?,
        access: input.access_key,
        revision: input
            .credential_revision
            .ok_or_else(invalid)?
            .try_into()
            .map_err(|_| invalid())?,
        bucket: input.bucket,
        key: input.key,
        upload: super::uuid(&input.upload_id)?,
        hash: input.manifest_hash,
    };
    value.validate().map_err(|_| invalid())?;
    Ok(value)
}

pub(super) fn source_batch(
    value: crate::model::node_complete::SourceBatch,
) -> Result<wire::CompleteSources, Status> {
    let reply = wire::CompleteSources {
        first: value.first as u32,
        next: value.next.map(|value| value as u32),
        parts: value.parts.into_iter().map(read::reply).collect(),
    };
    if prost::Message::encoded_len(&reply) > super::MESSAGE_LIMIT {
        return Err(Status::resource_exhausted(
            "completion source batch exceeds message budget",
        ));
    }
    Ok(reply)
}

pub(super) fn create(input: wire::CreateMultipartRequest) -> Result<Request, Status> {
    if prost::Message::encoded_len(&input) > read::REQUEST_LIMIT {
        return Err(Status::resource_exhausted(
            "multipart request exceeds message budget",
        ));
    }
    let value = Request {
        run: read::run(input.run)?,
        operation: super::uuid(&input.operation_id)?,
        intent: Intent {
            access: input.access_key,
            credential_revision: input
                .credential_revision
                .ok_or_else(invalid)?
                .try_into()
                .map_err(|_| invalid())?,
            bucket: input.bucket,
            key: input.key,
            operation: Operation::Create {
                metadata: serde_json::from_slice(&input.metadata_json).map_err(|_| invalid())?,
                public: input.public_read,
                algorithm: input.algorithm,
                checksum_type: input.checksum_type,
            },
        },
    };
    value.validate().map_err(|_| invalid())?;
    Ok(value)
}

pub(super) fn abort(input: wire::AbortMultipartRequest) -> Result<Request, Status> {
    if prost::Message::encoded_len(&input) > read::REQUEST_LIMIT {
        return Err(Status::resource_exhausted(
            "multipart request exceeds message budget",
        ));
    }
    let value = Request {
        run: read::run(input.run)?,
        operation: super::uuid(&input.operation_id)?,
        intent: Intent {
            access: input.access_key,
            credential_revision: input
                .credential_revision
                .ok_or_else(invalid)?
                .try_into()
                .map_err(|_| invalid())?,
            bucket: input.bucket,
            key: input.key,
            operation: Operation::Abort {
                upload: super::uuid(&input.upload_id)?,
                initiated: input.initiated,
            },
        },
    };
    value.validate().map_err(|_| invalid())?;
    Ok(value)
}

pub(super) fn reply(value: Outcome) -> wire::MultipartReply {
    wire::MultipartReply {
        upload_id: match value {
            Outcome::Created { upload } => Some(upload.to_string()),
            Outcome::Aborted => None,
        },
    }
}
