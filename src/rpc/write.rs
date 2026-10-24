use super::{read, wire};
use crate::model::node_write as write;
use tonic::Status;

fn invalid() -> Status {
    Status::invalid_argument("invalid write request")
}

fn operation(value: &str) -> Result<uuid::Uuid, Status> {
    let id = super::uuid(value)?;
    write::operation_time(id).map_err(|_| invalid())?;
    Ok(id)
}

pub(super) fn begin(input: wire::BeginWriteRequest) -> Result<write::Begin, Status> {
    if prost::Message::encoded_len(&input) > write::REQUEST_LIMIT {
        return Err(Status::resource_exhausted(
            "write request exceeds message budget",
        ));
    }
    let intent = write::Intent {
        access: input.access_key,
        bucket: input.bucket,
        key: input.key,
        metadata: serde_json::from_slice(&input.metadata_json).map_err(|_| invalid())?,
        public: input.public_read,
        size: input
            .size
            .map(i64::try_from)
            .transpose()
            .map_err(|_| invalid())?,
    };
    intent.validate().map_err(|_| invalid())?;
    Ok(write::Begin {
        run: read::run(input.run)?,
        operation: operation(&input.operation_id)?,
        intent,
        cached: read::cached(input.cached)?,
    })
}

pub(super) fn lookup(input: wire::WriteRequest) -> Result<write::Lookup, Status> {
    Ok(write::Lookup {
        run: read::run(input.run)?,
        operation: operation(&input.operation_id)?,
    })
}

pub(super) fn reply(value: write::Reply) -> Result<wire::WriteReply, Status> {
    let reply = wire::WriteReply {
        operation_id: value.operation.to_string(),
        stream_id: value.stream.map_or_else(String::new, |id| id.to_string()),
        phase: match value.phase {
            write::Phase::Active => wire::WritePhase::WriteActive,
            write::Phase::Aborted => wire::WritePhase::WriteAborted,
            write::Phase::Expired => wire::WritePhase::WriteExpired,
        } as i32,
        materials: value.materials.into_iter().map(read::material).collect(),
        storage: value.storage.map(read::version),
        grants: value.grants.into_iter().map(read::grant).collect(),
    };
    if prost::Message::encoded_len(&reply) > super::MESSAGE_LIMIT {
        return Err(Status::resource_exhausted(
            "write reply exceeds message budget",
        ));
    }
    Ok(reply)
}
