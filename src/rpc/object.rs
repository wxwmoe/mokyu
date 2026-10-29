use super::{read, wire};
use crate::model::node_object;
use tonic::Status;

pub(super) fn change(input: wire::ChangeObjectsRequest) -> Result<node_object::Request, Status> {
    let invalid = || Status::invalid_argument("invalid object change");
    if prost::Message::encoded_len(&input) > node_object::REQUEST_LIMIT {
        return Err(Status::resource_exhausted(
            "object change exceeds message budget",
        ));
    }
    let value = node_object::Request {
        run: read::run(input.run)?,
        operation: super::uuid(&input.operation_id)?,
        change: node_object::Change {
            access: input.access_key,
            credential_revision: input
                .credential_revision
                .ok_or_else(invalid)?
                .try_into()
                .map_err(|_| invalid())?,
            bucket: input.bucket,
            keys: input.keys,
            public: input.public_read,
            if_match: input.if_match,
        },
    };
    value.validate().map_err(|_| invalid())?;
    Ok(value)
}

pub(super) fn reply(outcome: node_object::Outcome) -> wire::ChangeObjectsReply {
    wire::ChangeObjectsReply {
        outcome: match outcome {
            node_object::Outcome::Applied => wire::ObjectChangeOutcome::Applied,
            node_object::Outcome::NoSuchKey => wire::ObjectChangeOutcome::ObjectChangeNoSuchKey,
            node_object::Outcome::PreconditionFailed => {
                wire::ObjectChangeOutcome::ObjectChangePreconditionFailed
            }
        } as i32,
    }
}
