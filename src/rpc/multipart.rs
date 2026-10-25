use super::{read, wire};
use crate::model::node_multipart::{Intent, Operation, Outcome, Request};
use tonic::Status;

fn invalid() -> Status {
    Status::invalid_argument("invalid multipart request")
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
