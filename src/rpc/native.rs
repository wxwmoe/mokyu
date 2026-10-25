use super::{read, wire};
use crate::model::node_native as native;
use tonic::Status;

fn invalid() -> Status {
    Status::invalid_argument("invalid native multipart request")
}

pub(super) fn local(input: wire::PrepareLocalPartRequest) -> Result<native::LocalPart, Status> {
    let value = native::LocalPart {
        run: read::run(input.run)?,
        operation: super::uuid(&input.operation_id)?,
        size: input.size.try_into().map_err(|_| invalid())?,
        hashes: input
            .hashes
            .into_iter()
            .map(|value| value.try_into().map_err(|_| invalid()))
            .collect::<Result<_, _>>()?,
    };
    value.hash().map_err(|_| invalid())?;
    Ok(value)
}

pub(super) fn claim(input: wire::NativeClaimRequest) -> Result<native::Claim, Status> {
    if prost::Message::encoded_len(&input) > read::REQUEST_LIMIT {
        return Err(invalid());
    }
    Ok(native::Claim {
        run: read::run(input.run)?,
        operation: super::uuid(&input.operation_id)?,
        stream: input.stream_id.as_deref().map(super::uuid).transpose()?,
        cached: read::cached(input.cached)?,
    })
}

pub(super) fn finish(input: wire::NativeFinishRequest) -> Result<native::Finish, Status> {
    let result = native::ResultValue {
        value: input.value,
        retry: input.retry,
        source_error: input
            .source_error
            .map(|value| match wire::LocalSourceError::try_from(value) {
                Ok(wire::LocalSourceError::LocalSourceMissing) => Ok(native::SourceError::Missing),
                Ok(wire::LocalSourceError::LocalSourceCorrupt) => Ok(native::SourceError::Corrupt),
                _ => Err(invalid()),
            })
            .transpose()?,
    };
    result.hash().map_err(|_| invalid())?;
    Ok(native::Finish {
        run: read::run(input.run)?,
        operation: super::uuid(&input.operation_id)?,
        result,
    })
}

pub(super) fn inventory(input: wire::LocalFilesRequest) -> Result<native::Inventory, Status> {
    if input.files.len() > 128 {
        return Err(invalid());
    }
    Ok(native::Inventory {
        run: read::run(input.run)?,
        recover: input.recover,
        files: input
            .files
            .iter()
            .map(|value| super::uuid(value))
            .collect::<Result<_, _>>()?,
    })
}

pub(super) fn complete(input: wire::NativeCompleteRequest) -> Result<native::Complete, Status> {
    Ok(native::Complete {
        run: read::run(input.run)?,
        operation: super::uuid(&input.operation_id)?,
        resend: input.resend,
    })
}

pub(super) fn adopt(input: wire::AdoptNativeRequest) -> Result<native::Adopt, Status> {
    Ok(native::Adopt {
        run: read::run(input.run)?,
        operation: super::uuid(&input.operation_id)?,
        digest: input.digest.try_into().map_err(|_| invalid())?,
        hash: input.content_hash.try_into().map_err(|_| invalid())?,
    })
}

pub(super) fn reply(value: native::Reply) -> Result<wire::NativeWorkReply, Status> {
    Ok(wire::NativeWorkReply {
        operation_id: value.operation.to_string(),
        work_json: value
            .work
            .map(|work| serde_json::to_vec(&work))
            .transpose()
            .map_err(|_| Status::internal("native work encoding failed"))?,
        ready: value.ready,
        materials: value.materials.into_iter().map(read::material).collect(),
        storage: value.storage.map(read::version),
        grants: value.grants.into_iter().map(read::grant).collect(),
    })
}

pub(super) fn prepared(
    value: native::PreparedCompletion,
) -> Result<wire::NativeCompleteReply, Status> {
    let reply = wire::NativeCompleteReply {
        completion_json: serde_json::to_vec(&value.value)
            .map_err(|_| Status::internal("native manifest encoding failed"))?,
        grants: value.grants.into_iter().map(read::grant).collect(),
    };
    if prost::Message::encoded_len(&reply) > super::MESSAGE_LIMIT {
        return Err(Status::resource_exhausted(
            "native manifest exceeds message budget",
        ));
    }
    Ok(reply)
}
